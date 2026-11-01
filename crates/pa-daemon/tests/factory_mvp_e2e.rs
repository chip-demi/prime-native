//! Factory qualification: actual daemon, kernel and child workers.
//! A session's RLM
//! session + local-harness dirs in its env (TS `_rlmKernelEnv`), so kernel-side
//! `rlm.harness` writes land in the session-local store. Skips (with a note)
//! without a live kernel install.
#![cfg(unix)]
// Each finite integration scenario keeps its causal wire sequence and assertions together.
#![allow(clippy::too_many_lines)]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc,
};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// The kernel Python with prime-agent-runtime installed; set
/// `PA_E2E_KERNEL_PYTHON` to point at an explicit interpreter instead.
fn kernel_python() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("PA_E2E_KERNEL_PYTHON") {
        let explicit = PathBuf::from(explicit);
        assert!(
            explicit.exists(),
            "PA_E2E_KERNEL_PYTHON {} not found",
            explicit.display()
        );
        return Some(explicit);
    }
    let candidate = PathBuf::from(std::env::var("HOME").map_or_else(
        |_| "/home/ubuntu/.prime/agent/kernel-venv/bin/python".to_string(),
        |home| format!("{home}/.prime/agent/kernel-venv/bin/python"),
    ));
    if candidate.exists() {
        return Some(candidate);
    }
    eprintln!(
        "kernel python {} not found; skipping live kernel-harness-env e2e",
        candidate.display()
    );
    None
}

struct Daemon {
    child: Child,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[allow(clippy::zombie_processes)]
fn spawn_supervisor(socket: &Path, agent_dir: &Path, kernel_python: &Path) -> Daemon {
    std::fs::create_dir_all(agent_dir).expect("agent dir");
    // The same finite fixture can qualify the actual shipped CLI without
    // rebuilding or substituting a different daemon binary.
    let mut command = if let Some(binary) = std::env::var_os("FACTORY_NATIVE_BINARY") {
        let mut command = Command::new(binary);
        command
            .arg("--mode")
            .arg("daemon")
            .arg("--daemon-socket")
            .arg(socket);
        command
    } else {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pa-daemon"));
        command
            .arg("supervisor")
            .arg("--socket")
            .arg(socket)
            .arg("--agent-dir")
            .arg(agent_dir);
        command
    };
    let child = command
        .env("PRIME_AGENT_KERNEL_PYTHON", kernel_python)
        // Hermetic agent dir: point every fallback at the test sandbox.
        .env("PRIME_AGENT_CODING_AGENT_DIR", agent_dir)
        // Hermetic harness resolution: ambient session/harness dirs would
        // mask a missing kernel-env export (the regression this test guards).
        .env_remove("RLM_SESSION_DIR")
        .env_remove("RLM_HARNESS_STATE_DIR")
        .env_remove("RLM_GLOBAL_HARNESS_STATE_DIR")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(agent_dir.join("daemon.log")).unwrap(),
        ))
        .env(
            pa_daemon::worker::WORKER_SUPERVISOR_LOST_EXIT_MS_ENV,
            "15000",
        )
        .spawn()
        .expect("spawn pa-daemon supervisor");
    Daemon { child }
}

fn wait_socket_ready(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if UnixStream::connect(socket).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "supervisor socket never came up");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// JSONL supervisor client (command envelopes, id-matched responses).
struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

impl Client {
    fn connect(socket: &Path) -> (Self, Value) {
        let stream = UnixStream::connect(socket).expect("connect supervisor");
        let writer = stream.try_clone().expect("clone socket");
        let mut client = Client {
            reader: BufReader::new(stream),
            writer,
        };
        let hello = client.read_line();
        (client, hello)
    }

    fn send_command(&mut self, id: &str, command: &Value) {
        self.write_line(&json!({
            "type": "command",
            "id": id,
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "command": command,
        }));
    }

    fn write_line(&mut self, value: &Value) {
        let mut line = serde_json::to_string(value).expect("serialize line");
        line.push('\n');
        self.writer.write_all(line.as_bytes()).expect("send");
        self.writer.flush().expect("flush");
    }

    fn read_line(&mut self) -> Value {
        let mut line = String::new();
        let deadline = Instant::now() + Duration::from_mins(2);
        self.reader
            .get_mut()
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set timeout");
        loop {
            match self.reader.read_line(&mut line) {
                Ok(0) => panic!("supervisor closed the connection"),
                Ok(_) if line.trim().is_empty() => line.clear(),
                Ok(_) => return serde_json::from_str(line.trim()).expect("parse line"),
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for supervisor line: {error}"
                    );
                }
            }
        }
    }

    fn read_response(&mut self, id: &str) -> Value {
        let deadline = Instant::now() + Duration::from_mins(4);
        loop {
            assert!(Instant::now() < deadline, "no response for id {id}");
            let line = self.read_line();
            if line.get("id").and_then(Value::as_str) == Some(id) {
                return line;
            }
        }
    }
}

#[test]
fn factory_mvp_actual_native_local_children() {
    let python = kernel_python().expect("explicit candidate runtime required, no skip");
    let dir = PathBuf::from(std::env::var("FACTORY_FIXTURE_ROOT").expect("owned root required"))
        .join("local-children");
    std::fs::create_dir_all(&dir).unwrap();
    let external_socket = std::env::var_os("FACTORY_NATIVE_SOCKET").map(PathBuf::from);
    let external_agent = std::env::var_os("FACTORY_NATIVE_AGENT_DIR").map(PathBuf::from);
    assert_eq!(
        external_socket.is_some(),
        external_agent.is_some(),
        "attach needs both owned socket and agent directory"
    );
    let agent = external_agent.unwrap_or_else(|| dir.join("agent"));
    std::fs::create_dir_all(&agent).unwrap();
    if external_socket.is_none() {
        std::fs::write(
            agent.join("settings.json"),
            json!({"factory":{"enabled":true}}).to_string(),
        )
        .unwrap();
        std::fs::write(agent.join("models.json"), json!({"providers":{"fixture":{"api":"openai-completions","baseUrl":"http://127.0.0.1:9","apiKey":"fixture-unused","models":[{"id":"local","name":"Local unused descriptor","reasoning":false,"input":["text"],"contextWindow":100_000,"maxTokens":4096}]}}}).to_string()).unwrap();
    }
    let session_dir = if external_socket.is_some() {
        agent.join("sessions")
    } else {
        dir.join("sessions")
    };
    assert!(
        std::env::var_os("FACTORY_FIXTURE_KEEP_OWNER").is_none() || external_socket.is_some(),
        "keeping an owner requires the supplied canary supervisor"
    );
    let parent_script = dir.join("parent-faux.json");
    std::fs::write(
        &parent_script,
        json!({"engine":"faux","responses":std::iter::repeat_n(json!({"text":"local parent notice acknowledged"}),16).collect::<Vec<_>>()})
            .to_string(),
    )
    .unwrap();
    let script = dir.join("child.json");
    std::fs::write(&script, json!({"responses":[{"text":"```json\n{\"verdict\":{\"flag\":true,\"result\":\"fixture-ok\"}}\n```","delayMs":100}]}).to_string()).unwrap();
    let machine = agent.join("machines/qualification/MACHINE.md");
    std::fs::create_dir_all(machine.parent().unwrap()).unwrap();
    let spec = json!({"run":{"max_parallel":1,"max_children":2,"max_transitions":2,"budget_ms":60000,"failure_policy":"fail_fast"},"states":[{"id":"a","entry":true,"subagent":{"prompt":"First local fixture","name":"fixture-a"},"outputs":[{"name":"verdict","type":"json"}]},{"id":"b","subagent":{"prompt":"Successor local fixture","name":"fixture-b"}}],"transitions":[{"from":"a","to":"b","when":{"output":"verdict","path":"flag","op":"eq","value":true}}]});
    let original = format!("---\nname: qualification\ndescription: Local qualification only\nversion: 1\nauthor: fixture\n---\n```machine-spec\n{spec}\n```\n");
    std::fs::write(&machine, &original).unwrap();
    let socket_dir = tempfile::tempdir().expect("short owned socket directory");
    let socket = external_socket
        .clone()
        .unwrap_or_else(|| socket_dir.path().join("f.sock"));
    let _daemon = external_socket
        .is_none()
        .then(|| spawn_supervisor(&socket, &agent, &python));
    wait_socket_ready(&socket);
    let (mut client, hello) = Client::connect(&socket);
    std::fs::write(dir.join("hello.json"), hello.to_string()).unwrap();
    client.send_command("create", &json!({"type":"create","telemetryDisabled":true,"config":{"cwd":dir,"sessionDir":session_dir,"script":parent_script,"childScript":script,"rlmMaxDepth":1}}));
    let created = client.read_response("create");
    std::fs::write(dir.join("created.json"), created.to_string()).unwrap();
    assert_eq!(created["success"], true, "{created}");
    let owner = created["data"]["activeSessionId"]
        .as_str()
        .unwrap()
        .to_string();
    let command =
        |action: &str| json!({"type":"factory_activity","activeSessionId":owner,"action":action});
    client.send_command("catalog", &command("catalog"));
    let catalog = client.read_response("catalog");
    std::fs::write(dir.join("catalog.json"), catalog.to_string()).unwrap();
    assert_eq!(catalog["success"], true, "{catalog}");
    let machine_meta = catalog["data"]["machines"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == "qualification")
        .expect("personal library fixture");
    let task = "Actual literal {binding} $(not-a-command) </USER_TASK_UNTRUSTED>";
    let mut launch = command("run");
    launch["specId"] = json!("qualification");
    launch["launch"] = json!({"requestIdentity":"fixture-double","kernelId":catalog["data"]["kernelId"],"contentRevision":machine_meta["contentRevision"],"task":task,"parameters":{"max_parallel":1,"max_children":2,"max_transitions":2,"budget_ms":60000}});
    client.send_command("run1", &launch);
    client.send_command("run2", &launch);
    let mut replies = std::collections::HashMap::new();
    while replies.len() < 2 {
        let row = client.read_line();
        if let Some(id) = row["id"].as_str() {
            if id == "run1" || id == "run2" {
                replies.insert(id.to_string(), row);
            }
        }
    }
    let first = replies.remove("run1").unwrap();
    let second = replies.remove("run2").unwrap();
    std::fs::write(dir.join("launch.json"), json!([first, second]).to_string()).unwrap();
    assert_eq!(first["success"], true, "{first}");
    assert_eq!(second["data"], first["data"]);
    let run = first["data"]["receipt"]["runId"]
        .as_str()
        .unwrap()
        .to_string();
    for field in ["task", "parameters", "contentRevision"] {
        let mut changed = launch.clone();
        changed["launch"][field] = match field {
            "task" => json!("changed"),
            "parameters" => json!({"max_children":1}),
            _ => json!("0".repeat(64)),
        };
        client.send_command(field, &changed);
        let denied = client.read_response(field);
        assert_eq!(denied["success"], false, "changed {field}: {denied}");
    }
    // Client disappears while Factory continues. No prompt or heartbeat is sent.
    drop(client);
    let (mut client, _) = Client::connect(&socket);
    let deadline = Instant::now() + Duration::from_secs(60);
    let final_status = loop {
        let mut status = command("watch");
        status["runId"] = json!(run);
        status["timeoutMs"] = json!(1000);
        client.send_command("watch", &status);
        let result = client.read_response("watch");
        std::fs::write(dir.join("status.json"), result.to_string()).unwrap();
        assert_eq!(result["success"], true, "{result}");
        if result["data"]["state"] != "running" {
            break result;
        }
        assert!(Instant::now() < deadline, "Factory never settled");
    };
    assert_eq!(final_status["data"]["state"], "done", "{final_status}");
    assert_eq!(final_status["data"]["usage"]["spawns"], 2);
    assert_eq!(final_status["data"]["usage"]["settled"], 2);
    assert_eq!(final_status["data"]["usage"]["transitionsFired"], 1);
    assert!(
        final_status["data"]["elapsedMs"].as_u64().unwrap() < 10000,
        "idle admission stalled: {final_status}"
    );
    client.send_command("lookup",&json!({"type":"factory_activity","activeSessionId":owner,"action":"lookup","launch":launch["launch"]}));
    let lookup = client.read_response("lookup");
    assert_eq!(lookup["data"]["outcome"], "known");
    assert_eq!(lookup["data"]["receipt"], first["data"]);
    client.send_command("receipts", &command("receipts"));
    let receipts = client.read_response("receipts");
    std::fs::write(dir.join("receipts.json"), receipts.to_string()).unwrap();
    assert_eq!(receipts["data"]["receipts"].as_array().unwrap().len(), 1);
    assert!(!receipts.to_string().contains(task));
    assert!(!receipts.to_string().contains("machinePath"));
    client.send_command("roster", &json!({"type":"list"}));
    let roster = client.read_response("roster");
    std::fs::write(dir.join("roster.json"), roster.to_string()).unwrap();
    let children: Vec<_> = roster["data"]["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["runtimeKind"] == "subagent")
        .collect();
    assert_eq!(children.len(), 2, "{roster}");
    let files: Vec<_> = children
        .iter()
        .map(|c| c["sessionFile"].as_str().unwrap())
        .collect();
    for file in files {
        let history = std::fs::read_to_string(file).unwrap();
        assert!(
            history.contains("USER_TASK_UNTRUSTED"),
            "child task absent: {file}"
        );
        assert!(history.contains("$(not-a-command)"));
        assert!(history.contains("fixture-ok"));
    }
    assert_eq!(std::fs::read_to_string(&machine).unwrap(), original);
    let mut stop = command("stop");
    stop["runId"] = json!(run);
    client.send_command("stop", &stop);
    let stopped = client.read_response("stop");
    std::fs::write(dir.join("stop.json"), stopped.to_string()).unwrap();
    assert_eq!(stopped["success"], true, "{stopped}");
    client.send_command("after-stop-roster", &json!({"type":"list"}));
    let after_stop = client.read_response("after-stop-roster");
    std::fs::write(dir.join("after-stop-roster.json"), after_stop.to_string()).unwrap();
    assert!(
        !stopped.to_string().contains("cancel_failed"),
        "cancellation failed: {stopped}"
    );
    // A mismatched/new incarnation is UNKNOWN, not permission to replay.
    client.send_command("unknown",&json!({"type":"factory_activity","activeSessionId":owner,"action":"lookup","launch":{"requestIdentity":"fixture-double","kernelId":"lost-incarnation"}}));
    let unknown = client.read_response("unknown");
    assert_eq!(unknown["data"]["outcome"], "unknown");
    assert_eq!(unknown["data"]["recoverySupported"], false);
    std::fs::write(dir.join("unknown.json"), unknown.to_string()).unwrap();
    // Actual executor milestones cross the kernel bridge into this owner,
    // without a user prompt/heartbeat. First prove the successful run's notice.
    let root_file = roster["data"]["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["activeSessionId"] == owner)
        .and_then(|row| row["sessionFile"].as_str())
        .expect("owning session file");
    let mut expected = vec![(run.clone(), vec!["finished"])];
    // A child with invalid required JSON makes actual finite failed/paused
    // runs; these are executor outcomes, not injected status responses.
    std::fs::write(
        &script,
        json!({"responses":[{"text":"invalid required JSON"}]}).to_string(),
    )
    .unwrap();
    for (name, policy, state) in [
        ("milestone-failed", "fail_fast", "failed"),
        ("milestone-paused", "escalate", "paused"),
    ] {
        let spec = json!({"run":{"max_parallel":1,"max_children":2,"max_transitions":2,
            "budget_ms":60000,"failure_policy":policy},"states":[{"id":"a","entry":true,
            "subagent":{"prompt":"Controlled invalid JSON fixture","name":name},
            "outputs":[{"name":"verdict","type":"json"}]}]});
        let machine_path = agent.join(format!("machines/{name}/MACHINE.md"));
        std::fs::create_dir_all(machine_path.parent().unwrap()).unwrap();
        std::fs::write(&machine_path, format!("---\nname: {name}\ndescription: Owned milestone fixture\nversion: 1\nauthor: fixture\n---\n```machine-spec\n{spec}\n```\n")).unwrap();
        client.send_command("milestone-catalog", &command("catalog"));
        let catalog = client.read_response("milestone-catalog");
        assert_eq!(catalog["success"], true, "{catalog}");
        let machine = catalog["data"]["machines"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["name"] == name)
            .expect("owned milestone machine");
        let mut launch = command("run");
        launch["specId"] = json!(name);
        launch["launch"] = json!({"requestIdentity":format!("fixture-{name}"),
            "kernelId":catalog["data"]["kernelId"],"contentRevision":machine["contentRevision"],
            "task":"Finite local milestone qualification","parameters":{"max_parallel":1,
                "max_children":2,"max_transitions":2,"budget_ms":60000}});
        client.send_command("milestone-run", &launch);
        let launched = client.read_response("milestone-run");
        assert_eq!(launched["success"], true, "{launched}");
        let run_id = launched["data"]["receipt"]["runId"]
            .as_str()
            .unwrap()
            .to_string();
        factory_wait_state(&mut client, &owner, &run_id, state, &dir, name);
        if state == "paused" {
            let mut resume = command("resume");
            resume["runId"] = json!(run_id);
            client.send_command("milestone-resume", &resume);
            assert_eq!(client.read_response("milestone-resume")["success"], true);
            factory_wait_state(
                &mut client,
                &owner,
                &run_id,
                "paused",
                &dir,
                "paused-after-resume",
            );
            expected.push((run_id.clone(), vec!["paused", "paused"]));
        } else {
            expected.push((run_id.clone(), vec!["failed"]));
        }
        let mut stop = command("stop");
        stop["runId"] = json!(run_id);
        client.send_command("milestone-stop", &stop);
        assert_eq!(client.read_response("milestone-stop")["success"], true);
    }
    // Stop only this finite fixture's settled children. Other canary
    // owners and explicit restore fixtures remain outside this scope.
    client.send_command("cleanup-roster", &json!({"type":"list"}));
    let cleanup_roster = client.read_response("cleanup-roster");
    assert_eq!(cleanup_roster["success"], true, "{cleanup_roster}");
    let mut cleaned_children = Vec::new();
    for child in cleanup_roster["data"]["sessions"].as_array().unwrap() {
        if child["parentActiveSessionId"] != owner {
            continue;
        }
        assert_eq!(child["parentSessionId"], created["data"]["sessionId"]);
        assert_eq!(child["parentSessionPath"], root_file);
        assert_eq!(
            child["isStreaming"], false,
            "settled fixture child: {child}"
        );
        assert_eq!(child["sessionActions"]["queuedCount"], 0);
        client.send_command(
            "cleanup-child",
            &json!({"type":"kill","activeSessionId":child["activeSessionId"]}),
        );
        assert_eq!(client.read_response("cleanup-child")["success"], true);
        cleaned_children.push(child["activeSessionId"].clone());
    }
    client.send_command(
        "owner-idle",
        &json!({"type":"wait_for_idle","activeSessionId":owner}),
    );
    assert_eq!(client.read_response("owner-idle")["success"], true);
    let persisted = std::fs::read_to_string(root_file)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|row| {
            row["type"] == "custom_message" && row["customType"] == "factory_progress_notice"
        })
        .collect::<Vec<_>>();
    client.send_command(
        "owner-messages",
        &json!({"type":"get_messages","activeSessionId":owner}),
    );
    let messages = client.read_response("owner-messages");
    assert_eq!(messages["success"], true, "{messages}");
    let rows = messages["data"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["customType"] == "factory_progress_notice")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        rows.iter().map(|row| &row["details"]).collect::<Vec<_>>(),
        persisted
            .iter()
            .map(|row| &row["details"])
            .collect::<Vec<_>>(),
        "actual public messages and durable rows agree"
    );
    for (run_id, kinds) in &expected {
        let delivered = rows
            .iter()
            .filter(|row| row["details"]["runId"].as_str() == Some(run_id.as_str()))
            .map(|row| row["details"]["kind"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            &delivered, kinds,
            "owning session receives exactly the actual milestones"
        );
    }
    assert!(
        rows.iter().all(|row| row["role"] == "custom"),
        "factory notices cannot become human rows"
    );
    // A different actual session/kernel cannot inspect the owning run.
    client.send_command(
        "foreign-create",
        &json!({"type":"create","telemetryDisabled":true,"config":{"cwd":dir,
        "sessionDir":session_dir,"script":parent_script,"rlmMaxDepth":1}}),
    );
    let foreign = client.read_response("foreign-create");
    assert_eq!(foreign["success"], true, "{foreign}");
    let foreign_owner = foreign["data"]["activeSessionId"].as_str().unwrap();
    client.send_command(
        "foreign-catalog",
        &json!({"type":"factory_activity","activeSessionId":foreign_owner,"action":"catalog"}),
    );
    let foreign_catalog = client.read_response("foreign-catalog");
    assert_eq!(foreign_catalog["success"], true, "{foreign_catalog}");
    assert!(foreign_catalog["data"]["kernelId"].is_string());
    assert_ne!(
        foreign_catalog["data"]["kernelId"],
        catalog["data"]["kernelId"]
    );
    client.send_command("foreign-watch", &json!({"type":"factory_activity","activeSessionId":foreign_owner,"action":"watch","runId":run}));
    let foreign_denied = client.read_response("foreign-watch");
    assert_eq!(
        foreign_denied["success"], false,
        "foreign session cannot inspect the owner run: {foreign_denied}"
    );
    let foreign_error = foreign_denied["error"]
        .as_str()
        .expect("specific foreign-run refusal");
    assert!(
        !foreign_error.contains("Kernel is not running"),
        "a missing kernel does not prove owner isolation"
    );
    assert!(
        foreign_error.to_ascii_lowercase().contains("unknown") && foreign_error.contains(&run),
        "{foreign_denied}"
    );
    client.send_command(
        "foreign-kill",
        &json!({"type":"kill","activeSessionId":foreign_owner}),
    );
    assert_eq!(client.read_response("foreign-kill")["success"], true);
    let receipt = dir.join("factory-milestones.json");
    std::fs::write(
        &receipt,
        json!({"owner":owner,"socket":socket,
        "sessionId":created["data"]["sessionId"],"sessionFile":root_file,"cwd":dir,"cleanedChildren":cleaned_children,
        "actualNotices":rows,"persistedNotices":persisted,"expectedMilestones":expected,
        "foreignKernelId":foreign_catalog["data"]["kernelId"],"ownerKernelId":catalog["data"]["kernelId"],"foreignOwnerResponse":foreign_denied,"successfulRun":final_status["data"],
        "unknownLookup":unknown["data"]})
        .to_string(),
    )
    .unwrap();
    println!("FACTORY_FIXTURE_RECEIPT={}", receipt.display());
    if std::env::var_os("FACTORY_FIXTURE_KEEP_OWNER").is_none() {
        client.send_command("kill", &json!({"type":"kill","activeSessionId":owner}));
        assert_eq!(client.read_response("kill")["success"], true);
    }
}

#[test]
fn factory_mvp_actual_http_children_repair() {
    let python = kernel_python().expect("explicit candidate runtime required, no skip");
    let dir = PathBuf::from(std::env::var("FACTORY_FIXTURE_ROOT").expect("owned root required"))
        .join("http-children");
    std::fs::create_dir_all(&dir).unwrap();
    let agent = dir.join("agent");
    std::fs::create_dir_all(&agent).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let count = Arc::new(AtomicUsize::new(0));
    let served = count.clone();
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            let mut length = 0;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse::<usize>().unwrap();
                    }
                }
            }
            let mut body = vec![0; length];
            std::io::Read::read_exact(&mut reader, &mut body).unwrap();
            let n = served.fetch_add(1, Ordering::SeqCst) + 1;
            assert!(n <= 3, "repair LOCAL request budget exhausted");
            if n == 1 {
                held_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }
            let chunk = json!({"id":"local","object":"chat.completion.chunk","model":"local","choices":[{"index":0,"delta":{"role":"assistant","content":"```json\n{\"verdict\":{\"flag\":true,\"result\":\"fixture-ok\"}}\n```"},"finish_reason":"stop"}]});
            let body = format!("data: {chunk}\n\ndata: [DONE]\n\n");
            write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
        }
    });
    std::fs::write(agent.join("settings.json"), json!({"factory":{"enabled":true},"defaultProvider":"fixture","defaultModel":"local","subagentDefaultModel":"fixture/local"}).to_string()).unwrap();
    std::fs::write(agent.join("models.json"), json!({"providers":{"fixture":{"api":"openai-completions","baseUrl":format!("http://127.0.0.1:{port}/v1"),"apiKey":"localhost-only","models":[{"id":"local","name":"Local HTTP descriptor","reasoning":false,"input":["text"],"contextWindow":100_000,"maxTokens":4096}]}}}).to_string()).unwrap();
    let machine = agent.join("machines/qualification/MACHINE.md");
    std::fs::create_dir_all(machine.parent().unwrap()).unwrap();
    let spec = json!({"run":{"max_parallel":1,"max_children":2,"max_transitions":2,"budget_ms":60000,"failure_policy":"fail_fast"},"states":[{"id":"a","entry":true,"subagent":{"prompt":"First local fixture","name":"fixture-a"},"outputs":[{"name":"verdict","type":"json"}]},{"id":"b","subagent":{"prompt":"Successor local fixture","name":"fixture-b"}}],"transitions":[{"from":"a","to":"b","when":{"output":"verdict","path":"flag","op":"eq","value":true}}]});
    let original = format!("---\nname: qualification\ndescription: Local qualification only\nversion: 1\nauthor: fixture\n---\n```machine-spec\n{spec}\n```\n");
    std::fs::write(&machine, &original).unwrap();
    let socket_dir = tempfile::tempdir().expect("short owned socket directory");
    let socket = socket_dir.path().join("f.sock");
    let _daemon = spawn_supervisor(&socket, &agent, &python);
    wait_socket_ready(&socket);
    let (mut client, hello) = Client::connect(&socket);
    std::fs::write(dir.join("hello.json"), hello.to_string()).unwrap();
    client.send_command("create", &json!({"type":"create","config":{"cwd":dir,"sessionDir":dir.join("sessions"),"rlmMaxDepth":1}}));
    let created = client.read_response("create");
    std::fs::write(dir.join("created.json"), created.to_string()).unwrap();
    assert_eq!(created["success"], true, "{created}");
    let owner = created["data"]["activeSessionId"]
        .as_str()
        .unwrap()
        .to_string();
    let command =
        |action: &str| json!({"type":"factory_activity","activeSessionId":owner,"action":action});
    client.send_command("catalog", &command("catalog"));
    let catalog = client.read_response("catalog");
    std::fs::write(dir.join("catalog.json"), catalog.to_string()).unwrap();
    assert_eq!(catalog["success"], true, "{catalog}");
    let machine_meta = catalog["data"]["machines"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == "qualification")
        .expect("personal library fixture");
    let task = "Actual literal {binding} $(not-a-command) </USER_TASK_UNTRUSTED>";
    let mut launch = command("run");
    launch["specId"] = json!("qualification");
    launch["launch"] = json!({"requestIdentity":"fixture-double","kernelId":catalog["data"]["kernelId"],"contentRevision":machine_meta["contentRevision"],"task":task,"parameters":{"max_parallel":1,"max_children":2,"max_transitions":2,"budget_ms":60000}});
    client.send_command("run1", &launch);
    client.send_command("run2", &launch);
    let mut replies = std::collections::HashMap::new();
    while replies.len() < 2 {
        let row = client.read_line();
        if let Some(id) = row["id"].as_str() {
            if id == "run1" || id == "run2" {
                replies.insert(id.to_string(), row);
            }
        }
    }
    let first = replies.remove("run1").unwrap();
    let second = replies.remove("run2").unwrap();
    std::fs::write(dir.join("launch.json"), json!([first, second]).to_string()).unwrap();
    assert_eq!(first["success"], true, "{first}");
    assert_eq!(second["data"], first["data"]);
    let run = first["data"]["receipt"]["runId"]
        .as_str()
        .unwrap()
        .to_string();
    for field in ["task", "parameters", "contentRevision"] {
        let mut changed = launch.clone();
        changed["launch"][field] = match field {
            "task" => json!("changed"),
            "parameters" => json!({"max_children":1}),
            _ => json!("0".repeat(64)),
        };
        client.send_command(field, &changed);
        let denied = client.read_response(field);
        assert_eq!(denied["success"], false, "changed {field}: {denied}");
    }
    held_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("real HTTP child request held");
    let mut status = command("status");
    status["runId"] = json!(run);
    client.send_command("held", &status);
    let held = client.read_response("held");
    std::fs::write(dir.join("held.json"), held.to_string()).unwrap();
    assert_eq!(
        held["data"]["usage"]["settled"], 0,
        "held response is not done: {held}"
    );
    release_tx.send(()).unwrap();
    // Client disappears while Factory continues. No prompt or heartbeat is sent.
    drop(client);
    let (mut client, _) = Client::connect(&socket);
    let deadline = Instant::now() + Duration::from_secs(60);
    let final_status = loop {
        let mut status = command("watch");
        status["runId"] = json!(run);
        status["timeoutMs"] = json!(1000);
        client.send_command("watch", &status);
        let result = client.read_response("watch");
        std::fs::write(dir.join("status.json"), result.to_string()).unwrap();
        assert_eq!(result["success"], true, "{result}");
        if result["data"]["state"] != "running" {
            break result;
        }
        assert!(Instant::now() < deadline, "Factory never settled");
    };
    assert_eq!(final_status["data"]["state"], "done", "{final_status}");
    assert_eq!(final_status["data"]["usage"]["spawns"], 2);
    assert_eq!(final_status["data"]["usage"]["settled"], 2);
    assert_eq!(final_status["data"]["usage"]["transitionsFired"], 1);
    assert!(
        final_status["data"]["elapsedMs"].as_u64().unwrap() < 10000,
        "idle admission stalled: {final_status}"
    );
    client.send_command("lookup",&json!({"type":"factory_activity","activeSessionId":owner,"action":"lookup","launch":launch["launch"]}));
    let lookup = client.read_response("lookup");
    assert_eq!(lookup["data"]["outcome"], "known");
    assert_eq!(lookup["data"]["receipt"], first["data"]);
    client.send_command("receipts", &command("receipts"));
    let receipts = client.read_response("receipts");
    std::fs::write(dir.join("receipts.json"), receipts.to_string()).unwrap();
    assert_eq!(receipts["data"]["receipts"].as_array().unwrap().len(), 1);
    assert!(!receipts.to_string().contains(task));
    assert!(!receipts.to_string().contains("machinePath"));
    client.send_command("roster", &json!({"type":"list"}));
    let roster = client.read_response("roster");
    std::fs::write(dir.join("roster.json"), roster.to_string()).unwrap();
    let children: Vec<_> = roster["data"]["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["runtimeKind"] == "subagent")
        .collect();
    assert_eq!(children.len(), 2, "{roster}");
    let files: Vec<_> = children
        .iter()
        .map(|c| c["sessionFile"].as_str().unwrap())
        .collect();
    for file in files {
        let history = std::fs::read_to_string(file).unwrap();
        assert!(
            history.contains("USER_TASK_UNTRUSTED"),
            "child task absent: {file}"
        );
        assert!(history.contains("$(not-a-command)"));
        assert!(history.contains("fixture-ok"));
    }
    assert_eq!(std::fs::read_to_string(&machine).unwrap(), original);
    let mut stop = command("stop");
    stop["runId"] = json!(run);
    client.send_command("stop", &stop);
    let stopped = client.read_response("stop");
    std::fs::write(dir.join("stop.json"), stopped.to_string()).unwrap();
    assert_eq!(stopped["success"], true, "{stopped}");
    client.send_command("after-stop-roster", &json!({"type":"list"}));
    let after_stop = client.read_response("after-stop-roster");
    std::fs::write(dir.join("after-stop-roster.json"), after_stop.to_string()).unwrap();
    assert!(
        !stopped.to_string().contains("cancel_failed"),
        "cancellation failed: {stopped}"
    );
    // A mismatched/new incarnation is UNKNOWN, not permission to replay.
    client.send_command("unknown",&json!({"type":"factory_activity","activeSessionId":owner,"action":"lookup","launch":{"requestIdentity":"fixture-double","kernelId":"lost-incarnation"}}));
    let unknown = client.read_response("unknown");
    assert_eq!(unknown["data"]["outcome"], "unknown");
    assert_eq!(unknown["data"]["recoverySupported"], false);
    std::fs::write(dir.join("unknown.json"), unknown.to_string()).unwrap();
    std::fs::write(dir.join("local-count.json"), json!({"requests":count.load(Ordering::SeqCst),"repairStageIncludingRepro":count.load(Ordering::SeqCst)+1,"maxRepairStage":4}).to_string()).unwrap();
    client.send_command("kill", &json!({"type":"kill","activeSessionId":owner}));
    assert_eq!(client.read_response("kill")["success"], true);
}

fn factory_wait_state(
    client: &mut Client,
    owner: &str,
    run_id: &str,
    state: &str,
    dir: &Path,
    label: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        client.send_command(
            "milestone-watch",
            &json!({"type":"factory_activity",
            "activeSessionId":owner,"action":"watch","runId":run_id,"timeoutMs":1000}),
        );
        let response = client.read_response("milestone-watch");
        std::fs::write(
            dir.join(format!("{label}-status.json")),
            response.to_string(),
        )
        .unwrap();
        assert_eq!(response["success"], true, "{response}");
        if response["data"]["state"] == state {
            return;
        }
        assert_eq!(
            response["data"]["state"], "running",
            "unexpected executor outcome: {response}"
        );
        if Instant::now() >= deadline {
            // Keep the first real failure's native view beside the Factory
            // snapshot. A completed transcript alone cannot distinguish an
            // admission awaiting acknowledgment from a stalled settle watcher.
            client.send_command(
                "milestone-timeout-children",
                &json!({"type":"get_rlm_children","activeSessionId":owner}),
            );
            let children = client.read_response("milestone-timeout-children");
            std::fs::write(
                dir.join(format!("{label}-native-children.json")),
                children.to_string(),
            )
            .unwrap();
            client.send_command("milestone-timeout-roster", &json!({"type":"list"}));
            let roster = client.read_response("milestone-timeout-roster");
            std::fs::write(
                dir.join(format!("{label}-native-roster.json")),
                roster.to_string(),
            )
            .unwrap();
            for child in roster["data"]["sessions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|row| row["parentActiveSessionId"] == owner)
            {
                let Some(active) = child["activeSessionId"].as_str() else {
                    continue;
                };
                client.send_command(
                    "milestone-timeout-child-state",
                    &json!({"type":"get_state","activeSessionId":active}),
                );
                let child_state = client.read_response("milestone-timeout-child-state");
                std::fs::write(
                    dir.join(format!("{label}-child-{active}-state.json")),
                    child_state.to_string(),
                )
                .unwrap();
            }
        }
        assert!(Instant::now() < deadline, "Factory did not reach {state}");
    }
}
