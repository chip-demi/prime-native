//! RLM child-usage attribution end to end (S2): a spawned child settles, and the
//! `child_usage_attributed` row reaches the PARENT's durable transcript in the TS row
//! shape, so live and resumed usage views fold the child's billable usage. Tests skip
//! without a live kernel install.
// Stack-resident futures by design; boxing for a lint tick is a perf regression.
#![allow(clippy::large_futures)]
// Fn length is a style gate, not correctness.
#![allow(clippy::too_many_lines)]
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Daemon {
    child: Child,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// The timeout panic path cannot wait on the child; the test exits and reaps it.
#[allow(clippy::zombie_processes)]
fn spawn_supervisor(socket: &Path, agent_dir: &Path, kernel_python: &Path) -> Daemon {
    let binary = env!("CARGO_BIN_EXE_pa-daemon");
    let child = Command::new(binary)
        .arg("supervisor")
        .arg("--socket")
        .arg(socket)
        .arg("--agent-dir")
        .arg(agent_dir)
        .env("PRIME_AGENT_KERNEL_PYTHON", kernel_python)
        .env(
            "PYTHONPATH",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../prime-agent-runtime/src"),
        )
        // Hermetic agent dir: the ambient environment exports a real
        // agent dir; point every fallback at the test sandbox instead.
        .env("PRIME_AGENT_CODING_AGENT_DIR", agent_dir)
        .env_remove("PRIME_API_KEY")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env(
            pa_daemon::worker::WORKER_SUPERVISOR_LOST_EXIT_MS_ENV,
            "15000",
        )
        .spawn()
        .expect("spawn pa-daemon supervisor");
    Daemon { child }
}

/// The kernel Python; `PA_E2E_KERNEL_PYTHON` points at an explicit interpreter instead.
fn kernel_python() -> PathBuf {
    let path = PathBuf::from(
        std::env::var("PA_E2E_KERNEL_PYTHON")
            .expect("PA_E2E_KERNEL_PYTHON is required; no silent skip"),
    );
    assert!(
        path.exists(),
        "Kernel interpreter missing: {}",
        path.display()
    );
    path
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
        let write_stream = stream.try_clone().expect("clone socket");
        let mut client = Client {
            reader: BufReader::new(stream),
            writer: write_stream,
        };
        let hello = client.read_line();
        (client, hello)
    }

    fn send_command(&mut self, id: &str, command: &Value) {
        let envelope = json!({
            "type": "command",
            "id": id,
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "command": command,
        });
        let mut line = serde_json::to_string(&envelope).expect("serialize command");
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
                Ok(_) => return serde_json::from_str(line.trim()).expect("parse response line"),
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

/// Run one turn (prompt + idle wait) on the session.
fn run_turn(client: &mut Client, session_id: &str, message: &str, id: &str) {
    client.send_command(
        id,
        &json!({ "type": "prompt", "activeSessionId": session_id, "message": message }),
    );
    let prompted = client.read_response(id);
    assert_eq!(prompted["success"], true, "prompt failed: {prompted}");
    let idle_id = format!("{id}-idle");
    client.send_command(
        &idle_id,
        &json!({ "type": "wait_for_idle", "activeSessionId": session_id }),
    );
    let idle = client.read_response(&idle_id);
    assert_eq!(idle["success"], true, "wait_for_idle failed: {idle}");
}

/// Poll for a kernel cell's receipt content (the cell writes its verdict;
/// an empty read is the create-before-write window, not a verdict yet).
fn await_receipt(receipt: &Path) -> String {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        if let Ok(content) = std::fs::read_to_string(receipt) {
            if !content.is_empty() {
                return content;
            }
        }
        assert!(
            Instant::now() < deadline,
            "kernel cell receipt never appeared at {}",
            receipt.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Create a scripted parent session through the supervisor. The create's `childScript`
/// (the harness seam for the TS inherited `sessionConfig`) makes every child scripted.
fn create_parent(
    client: &mut Client,
    dir: &Path,
    parent_script: &Path,
    child_script: &Path,
    id: &str,
) -> Value {
    let sessions_dir = dir.join("sessions");
    std::fs::create_dir_all(&sessions_dir).expect("sessions dir");
    client.send_command(
        id,
        &json!({
            "type": "create",
            "name": "parent",
            "config": {
                "cwd": dir.to_string_lossy(),
                "sessionDir": sessions_dir.to_string_lossy(),
                "script": parent_script.to_string_lossy(),
                "childScript": child_script.to_string_lossy(),
            },
        }),
    );
    let created = client.read_response(id);
    assert_eq!(created["success"], true, "create parent failed: {created}");
    created["data"].clone()
}

#[test]
fn actual_kernel_binding_start_and_settle_reconciles_before_quiescence() {
    let python = kernel_python();
    let dir = tempfile::TempDir::new().unwrap();
    let agent = dir.path().join("agent");
    std::fs::create_dir_all(&agent).unwrap();
    let socket = dir.path().join("daemon.sock");
    let release = dir.path().join("release");
    std::fs::create_dir_all(&release).unwrap();
    let receipt = dir.path().join("status.json");
    let reaction = dir.path().join("reaction.json");
    let child = dir.path().join("child.json");
    let child_code=format!("import asyncio, pathlib\nbase=pathlib.Path({release:?})\nfor i in range(5):\n    try:\n        (base/('claim-'+str(i))).mkdir()\n        break\n    except FileExistsError: pass\nwhile not (base/'all').exists() and not (base/('release-'+str(i))).exists():\n    await asyncio.sleep(0.02)\nprint('released')",release=release.display().to_string());
    std::fs::write(&child,json!({"engine":"faux","responses":[{"content":[{"type":"toolCall","name":"ipython","arguments":{"code":child_code}}]},{"text":"child done"}]}).to_string()).unwrap();
    let first = format!(
        r"import asyncio,json,pathlib
handles=[]
for i in range(5):
    handles.append(await rlm.spawn('independent fixture lane '+str(i),name='lane-'+str(i)))
s=await rlm.orchestration_status()
assignments=[dict(childId=r['childId'],admissionGeneration=r['admissionGeneration'],incarnation=r['incarnation'],assignmentId='assignment-'+str(i),taskGeneration=1,ownership='lane-'+str(i),acceptanceRef='fixture-acceptance',artifactRef='fixture-result',dependenciesSatisfied=True) for i,r in enumerate(s['assignments'])]
await rlm.configure_orchestration_gate(dict(expectedRevision=s['revision'],policy=s['policy'],task=s['task'],assignments=assignments))
for i in range(500):
    s=await rlm.orchestration_status()
    if s['knownActive']==5: break
    await asyncio.sleep(0.02)
assert s['knownActive']==5,s
pathlib.Path({receipt:?}).write_text(json.dumps(s))
",
        receipt = receipt.display().to_string()
    );
    let correction = format!(
        r"import pathlib,json
s=await rlm.orchestration_status()
assert s['knownActive']==4,s
pathlib.Path({reaction:?}).write_text(json.dumps(s))
e=dict(reason='draining',evidence='four independent fixture lanes remain',dependency='remaining four fixture workers',clearCondition='all workers settled',taskGeneration=1,policyRevision=1)
await rlm.configure_orchestration_gate(dict(expectedRevision=s['revision'],policy=s['policy'],task=s['task'],assignments=[dict(childId=r['childId'],admissionGeneration=r['admissionGeneration'],incarnation=r['incarnation'],assignmentId='assignment-'+str(i),taskGeneration=1,ownership='lane-'+str(i),acceptanceRef='fixture-acceptance',artifactRef='fixture-result',dependenciesSatisfied=True) for i,r in enumerate(s['assignments'])],exception=e))
",
        reaction = reaction.display().to_string()
    );
    let parent_script = dir.path().join("parent.json");
    std::fs::write(
        &parent_script,
        json!({"engine":"faux","responses":[
            {"content":[{"type":"toolCall","name":"ipython","arguments":{"code":first}}]},
            {"text":"root yields with live obligations"},
            {"content":[{"type":"toolCall","name":"ipython","arguments":{"code":correction}}]},
            {"text":"explicit drain exception"}
        ]})
        .to_string(),
    )
    .unwrap();
    let daemon = spawn_supervisor(&socket, &agent, &python);
    wait_socket_ready(&socket);
    let (mut client, hello) = Client::connect(&socket);
    assert_eq!(hello["type"], "daemon_hello");
    let parent = create_parent(&mut client, dir.path(), &parent_script, &child, "create");
    let id = parent["activeSessionId"]
        .as_str()
        .or_else(|| parent["id"].as_str())
        .unwrap()
        .to_string();
    let config = json!({"type":"configure_orchestration_gate","activeSessionId":id,"expectedRevision":0,
        "policy":{"version":1,"enabled":true,"minActive":5,"maxActive":20,"rootWritesCode":false,"correctiveBudget":2},
        "task":{"projectId":"fixture","planId":"p","stepId":"s","taskGeneration":1,"policyRevision":1},"assignments":[]});
    client.send_command("config", &config);
    let result = client.read_response("config");
    assert_eq!(result["success"], true, "{result}");
    run_turn(&mut client, &id, "run fixture", "run");
    let s: Value = serde_json::from_str(&await_receipt(&receipt)).unwrap();
    assert_eq!(s["decision"], "pass");
    assert_eq!(s["knownActive"], 5);
    // Whole-run quiescence is deliberately still outstanding while these workers execute.
    client.send_command(
        "state",
        &json!({"type":"get_orchestration_gate","activeSessionId":id}),
    );
    let status = client.read_response("state");
    assert_eq!(status["data"]["knownActive"], 5, "{status}");
    std::fs::write(release.join("release-0"), "release").unwrap();
    let reacted: Value = serde_json::from_str(&await_receipt(&reaction)).unwrap();
    assert_eq!(reacted["knownActive"], 4, "{reacted}");
    std::fs::write(release.join("all"), "release").unwrap();
    client.send_command(
        "settle",
        &json!({"type":"wait_for_idle","activeSessionId":id,"waitForRlmQuiescence":true}),
    );
    let settled = client.read_response("settle");
    assert_eq!(settled["success"], true, "{settled}");
    client.send_command(
        "final",
        &json!({"type":"get_orchestration_gate","activeSessionId":id}),
    );
    let final_status = client.read_response("final");
    assert_eq!(final_status["data"]["knownActive"], 0, "{final_status}");
    assert!(
        final_status["data"]["correctiveAttempts"].as_u64().unwrap() > 0,
        "{final_status}"
    );
    std::fs::write(&reaction, final_status.to_string()).unwrap();
    drop(daemon);
}
