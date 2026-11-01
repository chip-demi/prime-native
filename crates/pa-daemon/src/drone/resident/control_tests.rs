use super::*;
use sha2::{Digest, Sha256};

fn payload(operation: &str, generation: &str) -> Value {
    json!({"operation":operation,"rootSessionId":"durable","activeSessionId":"active",
        "workerInstanceId":"incarnation","supervisorGeneration":generation,"expectedBindingRevision":0})
}

#[test]
fn resident_payload_strictly_separates_control_from_bind_and_status() {
    let mut valid = payload("resident_drone_control", "generation");
    valid["action"] = json!("stop");
    valid["expectedRevision"] = json!(0);
    assert!(validate_resident_request(&valid).is_ok());
    for action in ["resume", "stop"] {
        valid["action"] = json!(action);
        assert!(validate_resident_request(&valid).is_ok());
    }
    for field in [
        "droneRoadmapPath",
        "expectedSha256",
        "expectedRoadmapRevision",
        "model",
        "pinnedSource",
    ] {
        let mut wrong = valid.clone();
        wrong[field] = Value::Null;
        assert!(validate_resident_request(&wrong).is_err(), "{field}");
    }
    for action in [json!("status"), json!("pause"), Value::Null, json!(17)] {
        let mut wrong = valid.clone();
        wrong["action"] = action;
        assert!(validate_resident_request(&wrong).is_err());
    }
    for revision in [Value::Null, json!(-1), json!("0")] {
        let mut wrong = valid.clone();
        wrong["expectedRevision"] = revision;
        assert!(validate_resident_request(&wrong).is_err());
    }
    let mut missing = valid.clone();
    missing.as_object_mut().unwrap().remove("expectedRevision");
    assert!(validate_resident_request(&missing).is_err());
    for operation in ["resident_drone_status", "resident_drone_bind"] {
        let legacy = payload(operation, "generation");
        assert!(validate_resident_request(&legacy).is_ok());
        for field in ["action", "expectedRevision"] {
            let mut wrong = legacy.clone();
            wrong[field] = Value::Null;
            assert!(
                validate_resident_request(&wrong).is_err(),
                "{operation}/{field}"
            );
        }
    }
}

async fn fixture() -> (
    tempfile::TempDir,
    Arc<Supervisor>,
    Arc<crate::registry::ResidentWorker>,
) {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = Arc::new(
        Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .unwrap(),
    );
    let descriptor:crate::protocol::DaemonWorkerDescriptor=serde_json::from_value(json!({
        "version":2,"workerId":"worker","pid":0,"socketPath":"/unused","recoveryJournalPath":"/unused",
        "supervisorSocketPath":"/unused","authenticationToken":"fixture","rootActiveSessionId":"active",
        "rootSessionId":"durable","workerInstanceId":"incarnation","createdAt":"t","updatedAt":"t",
        "lifecycle":"ready","createCommand":{"cwd":dir.path(),"goal":{"status":"active"},"threadHeartbeatAdmission":true},"consecutiveFailures":0
    })).unwrap();
    let resident = crate::registry::ResidentWorker::new(
        "worker".into(),
        descriptor,
        dir.path().join("descriptor.json"),
    );
    supervisor.registry.insert(Arc::clone(&resident)).await;
    (dir, supervisor, resident)
}

#[tokio::test]
async fn private_control_shares_durable_stop_status_rebind_and_explicit_resume() {
    let (dir, supervisor, resident) = fixture().await;
    resident
        .descriptor
        .lock()
        .await
        .create_command
        .rest
        .insert("parentSessionPath".into(), json!("/sessions/creator.jsonl"));
    supervisor.drone_controls.resume_for_test("durable");
    let status = payload(
        "resident_drone_status",
        &supervisor.owner_cleanup_generation,
    );
    let response = supervisor
        .resident_drone_request(status.clone())
        .await
        .unwrap();
    assert_eq!(response.command, "resident_drone_status");
    let data = response.data.unwrap();
    assert_eq!(data["binding"]["state"], "missing_binding");
    assert_eq!(data["controls"]["enabled"], true);
    let before = resident.descriptor.lock().await.clone();
    let mut stop = payload(
        "resident_drone_control",
        &supervisor.owner_cleanup_generation,
    );
    stop["action"] = json!("stop");
    stop["expectedRevision"] = json!(1);
    for (field, value) in [
        ("rootSessionId", json!("foreign")),
        ("workerInstanceId", json!("old")),
        ("supervisorGeneration", json!("old")),
        ("expectedBindingRevision", json!(9)),
    ] {
        let mut wrong = stop.clone();
        wrong[field] = value;
        assert!(
            supervisor.resident_drone_request(wrong).await.is_err(),
            "{field}"
        );
    }
    let stopped = supervisor
        .resident_drone_request(stop.clone())
        .await
        .unwrap();
    assert_eq!(stopped.command, "resident_drone_control");
    let stopped = stopped.data.unwrap();
    assert_eq!(stopped["state"], "paused");
    assert_eq!(stopped["revision"], 2);
    assert!(
        stopped.get("binding").is_none(),
        "control reply is the direct shared DTO"
    );
    let identity = supervisor.drone_control_identity("active").await.unwrap();
    assert_eq!(
        stopped,
        supervisor
            .control_drone(&identity, "status", None)
            .await
            .unwrap()
    );
    assert_eq!(
        *resident.descriptor.lock().await,
        before,
        "stop must not alter goal, heartbeat or binding"
    );
    let restored = super::super::controls::DroneControls::new(dir.path().join("agent"));
    assert_eq!(restored.status("durable").unwrap()["state"], "paused");
    let path = dir.path().join("ROADMAP.md");
    std::fs::write(&path, "owned current roadmap").unwrap();
    let evidence = roadmap::read_bound(path.to_str(), dir.path()).unwrap();
    let mut bind = payload("resident_drone_bind", &supervisor.owner_cleanup_generation);
    bind["droneRoadmapPath"] = json!(path);
    bind["expectedSha256"] = evidence["sha256"].clone();
    bind["expectedRoadmapRevision"] = evidence["revision"].clone();
    assert!(
        supervisor
            .resident_drone_request(bind)
            .await
            .unwrap()
            .success
    );
    let mut status = status;
    status["expectedBindingRevision"] = json!(1);
    let rebound = supervisor
        .resident_drone_request(status)
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(rebound["bindingRevision"], 1);
    assert_eq!(rebound["controls"]["revision"], 2);
    assert_eq!(rebound["controls"]["userStopped"], true);
    std::fs::remove_file(&path).unwrap();
    stop["expectedBindingRevision"] = json!(1);
    stop["expectedRevision"] = json!(2);
    assert_eq!(
        supervisor
            .resident_drone_request(stop.clone())
            .await
            .unwrap()
            .data
            .unwrap()["state"],
        "paused",
        "stop remains available with stale or absent roadmap evidence"
    );
    stop["action"] = json!("resume");
    stop["expectedRevision"] = json!(0);
    assert!(supervisor
        .resident_drone_request(stop.clone())
        .await
        .is_err());
    stop["expectedRevision"] = json!(2);
    let resumed = supervisor
        .resident_drone_request(stop)
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(resumed["revision"], 3);
    assert_eq!(resumed["userStopped"], false);
    let descriptor = resident.descriptor.lock().await;
    assert_eq!(
        descriptor.create_command.rest["goal"],
        before.create_command.rest["goal"]
    );
    assert_eq!(
        descriptor.create_command.rest["threadHeartbeatAdmission"],
        true
    );
}

#[tokio::test]
async fn legacy_status_keeps_its_envelope_when_controls_are_unavailable() {
    let (dir, supervisor, resident) = fixture().await;
    let status = payload(
        "resident_drone_status",
        &supervisor.owner_cleanup_generation,
    );
    resident
        .descriptor
        .lock()
        .await
        .create_command
        .rest
        .insert("rlmDepth".into(), json!(1));
    let reply = supervisor
        .resident_drone_request(status.clone())
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(reply["rootSessionId"], "durable");
    assert_eq!(reply["binding"]["state"], "missing_binding");
    assert!(reply["controls"].is_null());
    resident
        .descriptor
        .lock()
        .await
        .create_command
        .rest
        .remove("rlmDepth");
    let controls = dir.path().join("agent/drone-controls");
    std::fs::create_dir_all(&controls).unwrap();
    std::fs::write(
        controls.join(format!("{:x}.json", Sha256::digest(b"durable"))),
        "{bad",
    )
    .unwrap();
    let reply = supervisor
        .resident_drone_request(status)
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(reply["rootSessionId"], "durable");
    assert!(reply["controls"].is_null());
}
