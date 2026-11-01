//! Actual local supervisor connection authorization, including a reopened promoted resident.
use super::*;
use serde_json::{json, Value};
const ROOT: &str = "01a15340-ce46-73f5-9d29-bf6d4408a1a0";
const OTHER: &str = "01a15e83-70d2-705d-ba6c-1ee0d8790ba9";

fn call(client: &mut Client, id: &str, command: &Value) -> Value {
    client.send_command(id, command);
    client.read_response(id)
}

fn opt_in(client: &mut Client, active: &str, root: &str) {
    let resumed = call(
        client,
        "opt-in",
        &json!({"type":"drone_control","activeSessionId":active,"rootSessionId":root,"action":"resume","expectedRevision":0}),
    );
    assert_eq!(resumed["success"], true, "{resumed}");
    assert_eq!(resumed["data"]["enabled"], true);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status = call(
            client,
            "settled-opt-in",
            &json!({"type":"drone_control","activeSessionId":active,"rootSessionId":root,"action":"status"}),
        );
        assert_eq!(status["success"], true, "{status}");
        if status["data"]["inFlight"] == false {
            assert_eq!(status["data"]["enabled"], true);
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "unbound resume did not settle: {status}"
        );
        std::thread::yield_now();
    }
}

#[test]
fn reopened_attached_root_controls_its_drone_without_create_owner_or_foreign_authority() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("daemon.sock");
    let agent = dir.path().join("agent");
    let sessions = agent.join("sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let script = dir.path().join("script.json");
    std::fs::write(
        &script,
        json!({"responses":[{"text":"owned fixture only"}]}).to_string(),
    )
    .unwrap();
    for root in [ROOT, OTHER] {
        std::fs::write(sessions.join(format!("{root}.jsonl")),format!("{}\n",json!({"type":"session","version":3,"id":root,"timestamp":"2026-10-22T00:00:00Z","cwd":dir.path()}))).unwrap();
    }
    let _daemon = spawn_daemon(&socket, &agent);
    let (mut creator, _) = Client::connect(&socket);
    let created = call(
        &mut creator,
        "create",
        &json!({"type":"create","lifecycle":"client_owned","sessionPath":sessions.join(format!("{ROOT}.jsonl")),"config":{"cwd":dir.path(),"sessionDir":sessions,"script":script}}),
    );
    assert_eq!(created["success"], true, "{created}");
    let active = created["data"]["id"].as_str().unwrap().to_owned();
    let promoted = call(
        &mut creator,
        "promote",
        &json!({"type":"promote_owned_session","activeSessionId":active}),
    );
    assert_eq!(promoted["success"], true, "{promoted}");
    drop(creator);
    let (mut reopened, hello) = Client::connect(&socket);
    assert!(hello["serverCapabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|cap| cap == "drone_control_v1"));
    let denied = call(
        &mut reopened,
        "unattached",
        &json!({"type":"drone_control","activeSessionId":active,"rootSessionId":ROOT,"action":"stop","expectedRevision":0}),
    );
    assert_eq!(denied["success"], false);
    assert_eq!(denied["error"], "drone_control_attached_root_required");
    assert_eq!(
        call(
            &mut reopened,
            "attach",
            &json!({"type":"attach","activeSessionId":active})
        )["success"],
        true
    );
    let status = call(
        &mut reopened,
        "status",
        &json!({"type":"drone_control","activeSessionId":active,"rootSessionId":ROOT,"action":"status"}),
    );
    assert_eq!(status["success"], true, "{status}");
    assert_eq!(status["data"]["rootSessionId"], ROOT);
    assert_eq!(status["data"]["revision"], 0);
    opt_in(&mut reopened, &active, ROOT);
    let stopped = call(
        &mut reopened,
        "stop",
        &json!({"type":"drone_control","activeSessionId":active,"rootSessionId":ROOT,"action":"stop","expectedRevision":1}),
    );
    assert_eq!(stopped["success"], true, "{stopped}");
    assert_eq!(stopped["data"]["state"], "paused");
    let wrong = call(
        &mut reopened,
        "wrong-root",
        &json!({"type":"drone_control","activeSessionId":active,"rootSessionId":OTHER,"action":"resume","expectedRevision":1}),
    );
    assert_eq!(wrong["success"], false);
    let (mut other, _) = Client::connect(&socket);
    let created = call(
        &mut other,
        "create-other",
        &json!({"type":"create","sessionPath":sessions.join(format!("{OTHER}.jsonl")),"config":{"cwd":dir.path(),"sessionDir":sessions,"script":script}}),
    );
    assert_eq!(created["success"], true, "{created}");
    let other_active = created["data"]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            &mut other,
            "attach-other",
            &json!({"type":"attach","activeSessionId":other_active})
        )["success"],
        true
    );
    opt_in(&mut other, &other_active, OTHER);
    let untouched = call(
        &mut other,
        "other-status",
        &json!({"type":"drone_control","activeSessionId":other_active,"rootSessionId":OTHER,"action":"status"}),
    );
    assert_eq!(untouched["success"], true, "{untouched}");
    assert_eq!(untouched["data"]["state"], "enabled");
    assert_eq!(untouched["data"]["revision"], 1);
    let foreign = call(
        &mut reopened,
        "foreign",
        &json!({"type":"drone_control","activeSessionId":other_active,"rootSessionId":OTHER,"action":"stop","expectedRevision":1}),
    );
    assert_eq!(foreign["success"], false);
    let stopped = call(
        &mut reopened,
        "still-stopped",
        &json!({"type":"drone_control","activeSessionId":active,"rootSessionId":ROOT,"action":"status"}),
    );
    assert_eq!(stopped["data"]["state"], "paused");
    assert_eq!(stopped["data"]["revision"], 2);
}

#[test]
fn only_successful_ordinary_direct_transport_authorizes_the_current_control_connection() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("daemon.sock");
    let agent = dir.path().join("agent");
    let sessions = agent.join("sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let saved = sessions.join(format!("{ROOT}.jsonl"));
    std::fs::write(&saved,format!("{}\n",json!({"type":"session","version":3,"id":ROOT,"timestamp":"2026-10-22T00:00:00Z","cwd":dir.path()}))).unwrap();
    let script = dir.path().join("script.json");
    std::fs::write(
        &script,
        json!({"responses":[{"text":"no provider request"}]}).to_string(),
    )
    .unwrap();
    let _daemon = spawn_daemon(&socket, &agent);
    let (mut creator, _) = Client::connect(&socket);
    let created = call(
        &mut creator,
        "create",
        &json!({"type":"create","lifecycle":"client_owned","sessionPath":saved,"config":{"cwd":dir.path(),"sessionDir":sessions,"script":script}}),
    );
    assert_eq!(created["success"], true, "{created}");
    let active = created["data"]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            &mut creator,
            "promote",
            &json!({"type":"promote_owned_session","activeSessionId":active})
        )["success"],
        true
    );
    drop(creator);
    let (mut reopened, _) = Client::connect(&socket);
    let unsigned = call(
        &mut reopened,
        "unsigned-resident",
        &json!({"type":"resident_drone_control","rootSessionId":ROOT,"activeSessionId":active,"workerInstanceId":"forged","supervisorGeneration":"forged","expectedBindingRevision":0,"action":"stop","expectedRevision":0}),
    );
    assert_eq!(
        unsigned["success"], false,
        "the signed resident lane is never a public wire command"
    );
    let status = json!({"type":"drone_control","activeSessionId":active,"rootSessionId":ROOT,"action":"status"});
    assert_eq!(call(&mut reopened, "before", &status)["success"], false);
    assert_eq!(
        call(
            &mut reopened,
            "failed-ticket",
            &json!({"type":"get_direct_worker_transport","activeSessionId":"missing"})
        )["success"],
        false
    );
    assert_eq!(
        call(&mut reopened, "after-failure", &status)["success"],
        false
    );
    let scoped = call(
        &mut reopened,
        "scoped-ticket",
        &json!({"type":"get_direct_worker_transport","activeSessionId":active,"controlScope":{
            "controlVersion":1,"rootSessionId":ROOT,"activeSessionId":active,"sessionFile":saved,"cwd":dir.path(),"operations":["attach","get_state"]
        }}),
    );
    assert_eq!(
        scoped["success"], true,
        "delegated transport fixture must actually be admitted"
    );
    assert_eq!(
        call(&mut reopened, "after-delegated", &status)["success"],
        false
    );
    let ordinary = call(
        &mut reopened,
        "ordinary-ticket",
        &json!({"type":"get_direct_worker_transport","activeSessionId":active}),
    );
    assert_eq!(
        ordinary["success"], true,
        "ordinary transport fixture must actually be admitted"
    );
    let before_opt_in = call(&mut reopened, "before-opt-in", &status);
    assert_eq!(
        before_opt_in["data"]["revision"], 0,
        "unsigned control had no effect"
    );
    assert_eq!(before_opt_in["data"]["enabled"], false);
    opt_in(&mut reopened, &active, ROOT);
    let authorized = call(&mut reopened, "authorized", &status);
    assert_eq!(authorized["success"], true, "{authorized}");
    assert_eq!(authorized["data"]["rootSessionId"], ROOT);
    assert_eq!(
        authorized["data"]["userStopped"], false,
        "unsigned control had no effect"
    );
    assert_eq!(
        authorized["data"]["workerInstanceId"],
        ordinary["data"]["workerInstanceId"]
    );
    let stopped = call(
        &mut reopened,
        "stop",
        &json!({"type":"drone_control","activeSessionId":active,"rootSessionId":ROOT,"action":"stop","expectedRevision":1}),
    );
    assert_eq!(stopped["success"], true, "{stopped}");
    assert_eq!(stopped["data"]["state"], "paused");
    let (mut foreign, _) = Client::connect(&socket);
    assert_eq!(
        call(&mut foreign, "foreign", &status)["success"],
        false,
        "tickets are never a same-UID or another-connection bypass"
    );
}
