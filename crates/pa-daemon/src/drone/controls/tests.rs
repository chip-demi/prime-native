use super::*;
const ROOT: &str = "01a15340-ce46-73f5-9d29-bf6d4408a1a0";
const OTHER: &str = "01a15e83-70d2-705d-ba6c-1ee0d8790ba9";

fn assessment(root: &str) -> Assessment {
    super::super::registry::DroneRegistry::default()
        .push(root, Value::Null)
        .unwrap()
}

fn opt_in(controls: &DroneControls, root: &str) {
    controls.resume_for_test(root);
    assert!(controls.enabled(root).unwrap());
}

#[test]
fn drone_default_off_requires_explicit_root_opt_in_and_persists_both_choices() {
    let dir = tempfile::tempdir().unwrap();
    let controls = DroneControls::new(dir.path().into());
    for root in [ROOT, OTHER] {
        assert!(!controls.enabled(root).unwrap());
        assert_eq!(controls.status(root).unwrap()["state"], "paused");
        assert!(controls.begin(&assessment(root)).unwrap().is_none());
    }
    assert!(!dir.path().join("drone-controls").exists());
    controls.root(ROOT).unwrap().mutate("resume", 0).unwrap();
    controls.root(OTHER).unwrap().mutate("stop", 0).unwrap();
    let restored = DroneControls::new(dir.path().into());
    assert!(restored.enabled(ROOT).unwrap());
    assert!(!restored.enabled(OTHER).unwrap());
    assert_eq!(restored.status(ROOT).unwrap()["revision"], 1);
    assert_eq!(restored.status(OTHER).unwrap()["revision"], 1);
    restored
        .begin(&assessment(ROOT))
        .unwrap()
        .unwrap()
        .finish(true)
        .unwrap();
    assert!(restored.begin(&assessment(OTHER)).unwrap().is_none());
}

#[test]
fn explicit_stop_is_durable_root_scoped_and_blocks_assessment_admission() {
    let dir = tempfile::tempdir().unwrap();
    let controls = DroneControls::new(dir.path().into());
    assert!(
        !dir.path().join("drone-controls").exists(),
        "status must not create runtime state"
    );
    opt_in(&controls, ROOT);
    opt_in(&controls, OTHER);
    assert!(controls.enabled(ROOT).unwrap() && controls.enabled(OTHER).unwrap());
    let root = controls.root(ROOT).unwrap();
    assert!(root.mutate("stop", 1).unwrap().is_none());
    assert!(controls.begin(&assessment(ROOT)).unwrap().is_none());
    let other = controls.begin(&assessment(OTHER)).unwrap().unwrap();
    assert!(!other.control.is_cancelled());
    assert_eq!(root.status().unwrap()["state"], "paused");
    let restored = DroneControls::new(dir.path().into());
    assert!(!restored.enabled(ROOT).unwrap());
    assert!(restored.enabled(OTHER).unwrap());
    assert!(restored.begin(&assessment(ROOT)).unwrap().is_none());
    assert!(
        root.mutate("resume", 1).is_err(),
        "a stale resume cannot undo the stop"
    );
    assert_eq!(root.status().unwrap()["revision"], 2);
    root.mutate("resume", 2).unwrap();
    assert!(controls.enabled(ROOT).unwrap());
    other.finish(true).unwrap();
}

#[test]
fn corrupt_or_foreign_control_records_fail_closed_and_legacy_ids_are_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let controls = DroneControls::new(dir.path().into());
    let legacy = "imported/session:legacy";
    controls.root(legacy).unwrap().mutate("stop", 0).unwrap();
    let path = control_path(dir.path(), legacy).unwrap();
    assert_eq!(
        path.parent(),
        Some(dir.path().join("drone-controls").as_path())
    );
    assert!(!DroneControls::new(dir.path().into())
        .enabled(legacy)
        .unwrap());
    std::fs::write(&path, b"{invalid").unwrap();
    assert!(DroneControls::new(dir.path().into())
        .enabled(legacy)
        .is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"{invalid");
    let foreign = DurableControl {
        version: 1,
        root_session_id: OTHER.into(),
        revision: 1,
        user_stopped: false,
        pending_cancellation: None,
    };
    std::fs::write(&path, serde_json::to_vec(&foreign).unwrap()).unwrap();
    assert!(DroneControls::new(dir.path().into())
        .enabled(legacy)
        .is_err());
}

#[tokio::test]
async fn stopping_reports_pending_until_own_assessment_settles_and_other_root_is_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let controls = DroneControls::new(dir.path().into());
    opt_in(&controls, ROOT);
    opt_in(&controls, OTHER);
    let run = controls.begin(&assessment(ROOT)).unwrap().unwrap();
    let other = controls.begin(&assessment(OTHER)).unwrap().unwrap();
    let root = controls.root(ROOT).unwrap();
    let mut settled = root.mutate("stop", 1).unwrap().unwrap();
    assert!(run.control.is_cancelled());
    assert!(!other.control.is_cancelled());
    assert_eq!(
        (
            root.status().unwrap()["state"].clone(),
            root.status().unwrap()["inFlight"].clone(),
            root.status().unwrap()["paused"].clone(),
            root.status().unwrap()["pendingCancel"].clone()
        ),
        (json!("stopping"), json!(true), json!(false), json!(true))
    );
    assert!(root.mutate("resume", 2).is_err());
    run.finish(true).unwrap();
    settled.changed().await.unwrap();
    assert!(*settled.borrow());
    assert_eq!(
        (
            root.status().unwrap()["state"].clone(),
            root.status().unwrap()["inFlight"].clone(),
            root.status().unwrap()["pendingCancel"].clone()
        ),
        (json!("paused"), json!(false), json!(false))
    );
    other.finish(true).unwrap();
}

#[test]
fn unconfirmed_cancellation_holds_resume_and_new_assessments() {
    let dir = tempfile::tempdir().unwrap();
    let controls = DroneControls::new(dir.path().into());
    opt_in(&controls, ROOT);
    let run = controls.begin(&assessment(ROOT)).unwrap().unwrap();
    let root = controls.root(ROOT).unwrap();
    root.mutate("stop", 1).unwrap();
    run.finish(false).unwrap();
    let status = root.status().unwrap();
    assert_eq!(
        (
            status["state"].clone(),
            status["inFlight"].clone(),
            status["pendingCancel"].clone(),
            status["paused"].clone()
        ),
        (
            json!("cancellation_unconfirmed"),
            json!(true),
            json!(true),
            json!(false)
        )
    );
    assert!(root.mutate("resume", 2).is_err());
    assert!(controls.begin(&assessment(ROOT)).unwrap().is_none());
}

async fn resident(
    supervisor: &Supervisor,
    dir: &Path,
) -> (
    Arc<crate::registry::ResidentWorker>,
    crate::protocol::DaemonWorkerDescriptor,
) {
    let descriptor:crate::protocol::DaemonWorkerDescriptor=serde_json::from_value(json!({
        "version":2,"workerId":"worker","pid":0,"socketPath":"/unused","recoveryJournalPath":"/unused",
        "supervisorSocketPath":"/unused","authenticationToken":"fixture","rootActiveSessionId":"active","rootSessionId":ROOT,
        "workerInstanceId":"incarnation","createdAt":"t","updatedAt":"t","lifecycle":"ready",
        "createCommand":{"cwd":dir,"rlmDepth":0,"droneBindingRevision":0},"consecutiveFailures":0})).unwrap();
    let resident = crate::registry::ResidentWorker::new(
        "worker".into(),
        descriptor.clone(),
        dir.join("descriptor.json"),
    );
    supervisor.registry.insert(Arc::clone(&resident)).await;
    (resident, descriptor)
}

#[tokio::test]
async fn stop_waits_for_admitted_action_outcome_then_rejects_further_action() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = Arc::new(
        Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .unwrap(),
    );
    let (resident, descriptor) = resident(&supervisor, dir.path()).await;
    opt_in(&supervisor.drone_controls, ROOT);
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    *resident.cmd_tx.lock().await = Some(tx);
    let assessment = supervisor
        .drone
        .lock()
        .unwrap()
        .push(ROOT, Value::Null)
        .unwrap();
    let action = super::super::handlers::Action {
        handler: "notify_root".into(),
        args: json!({"message":"already admitted diagnostic"}),
    };
    let snapshot = json!({"activeSessionId":"active"});
    let decision = supervisor.apply_drone_decision(
        &resident,
        &assessment,
        &descriptor,
        None,
        dir.path(),
        &Value::Null,
        &snapshot,
        None,
        None,
        action.clone(),
    );
    tokio::pin!(decision);
    let snapshot_request = tokio::select! { result=&mut decision=>panic!("premature {result:?}"),request=rx.recv()=>request.unwrap() };
    assert_eq!(snapshot_request.command_type, "drone_snapshot");
    resident.pending.lock().await.remove(&snapshot_request.request_id).unwrap().send(crate::registry::WorkerReply::Typed(crate::protocol::response_success(None,"drone_snapshot",Some(json!({"activeSessionId":"active","sessionId":ROOT,"paused":false,"stopped":false,"goal":{"status":"active"}}))))).unwrap_or_else(|_|panic!("snapshot receiver missing"));
    let admitted = tokio::select! { result=&mut decision=>panic!("premature {result:?}"),request=rx.recv()=>request.unwrap() };
    assert_eq!(admitted.command_type, "follow_up");
    let identity = supervisor.drone_control_identity("active").await.unwrap();
    let stop = supervisor.control_drone(&identity, "stop", Some(1));
    tokio::pin!(stop);
    // Poll the actual stop once: its existing binding/action fence must hold it.
    assert!(futures::poll!(&mut stop).is_pending());
    assert_eq!(
        supervisor.drone_controls.status(ROOT).unwrap()["userStopped"],
        false
    );
    resident
        .pending
        .lock()
        .await
        .remove(&admitted.request_id)
        .unwrap()
        .send(crate::registry::WorkerReply::Typed(
            crate::protocol::response_success(None, "follow_up", Some(json!({"accepted":true}))),
        ))
        .unwrap_or_else(|_| panic!("admitted receiver missing"));
    decision.await.unwrap();
    let stopped = stop.await.unwrap();
    assert_eq!(stopped["state"], "paused");
    assert!(supervisor
        .apply_drone_decision(
            &resident,
            &assessment,
            &descriptor,
            None,
            dir.path(),
            &Value::Null,
            &snapshot,
            None,
            None,
            action
        )
        .await
        .is_err());
    assert!(
        rx.try_recv().is_err(),
        "no action is admitted after stop acknowledgment"
    );
}

#[tokio::test]
async fn a_human_task_prohibition_rejects_a_spawn_notice_before_delivery_despite_free_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = Arc::new(
        Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .unwrap(),
    );
    let (resident, descriptor) = resident(&supervisor, dir.path()).await;
    opt_in(&supervisor.drone_controls, ROOT);
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    *resident.cmd_tx.lock().await = Some(tx);
    let assessment = supervisor
        .drone
        .lock()
        .unwrap()
        .push(ROOT, Value::Null)
        .unwrap();
    let gate = json!({"complete":true,"policy":{"enabled":true,"maxActive":5},
        "nextWork":{"runtime":{"complete":true,"freeSlots":5}}});
    let mut snapshot = json!({"activeSessionId":"active","sessionId":ROOT,"paused":false,
        "stopped":false,"humanInputFingerprint":"same-human","goal":{"status":"active"},
        "subagentPolicy":{"taskAllowsSubagents":false,"instruction":"Do not spawn subagents.","maxDepth":5}});
    for action in [
        super::super::handlers::Action {
            handler: "notify_root".into(),
            args: json!({"message":"Spawn a subagent for the ready task."}),
        },
        super::super::handlers::Action {
            handler: "recommend_parallel_work".into(),
            args: json!({"stepIds":["ready"],"roadmapHash":"hash"}),
        },
    ] {
        let decision = supervisor.apply_drone_decision(
            &resident,
            &assessment,
            &descriptor,
            None,
            dir.path(),
            &Value::Null,
            &snapshot,
            Some(&gate),
            None,
            action,
        );
        tokio::pin!(decision);
        let request = tokio::select! { result=&mut decision=>panic!("premature {result:?}"), request=rx.recv()=>request.unwrap() };
        assert_eq!(request.command_type, "drone_snapshot");
        resident
            .pending
            .lock()
            .await
            .remove(&request.request_id)
            .unwrap()
            .send(crate::registry::WorkerReply::Typed(
                crate::protocol::response_success(None, "drone_snapshot", Some(snapshot.clone())),
            ))
            .unwrap_or_else(|_| panic!("snapshot receiver missing"));
        assert_eq!(
            decision.await.unwrap_err().to_string(),
            "parallel_advice_forbidden"
        );
        assert!(
            rx.try_recv().is_err(),
            "no follow_up or parallel effect after the human prohibition"
        );
    }
    // The observer restriction is advisory; actual root permissions and evidence stay intact.
    snapshot["subagentPolicy"]["taskAllowsSubagents"] = json!(true);
    assert_eq!(
        super::super::parallel::advisory_policy(&snapshot, Some(&gate))["parallelAllowed"],
        true
    );
}

#[tokio::test]
async fn bind_and_descriptor_goal_heartbeat_changes_do_not_clear_explicit_stop() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = Arc::new(
        Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .unwrap(),
    );
    let (resident, _) = resident(&supervisor, dir.path()).await;
    assert_eq!(
        supervisor
            .control_drone(
                &supervisor.drone_control_identity("active").await.unwrap(),
                "stop",
                Some(0)
            )
            .await
            .unwrap()["state"],
        "paused"
    );
    let path = dir.path().join("ROADMAP.md");
    std::fs::write(&path, "current authorized roadmap").unwrap();
    let roadmap = super::super::roadmap::read_bound(path.to_str(), dir.path()).unwrap();
    let bind = json!({"operation":"resident_drone_bind","rootSessionId":ROOT,"activeSessionId":"active","workerInstanceId":"incarnation","supervisorGeneration":supervisor.owner_cleanup_generation,"expectedBindingRevision":0,"droneRoadmapPath":path,"expectedSha256":roadmap["sha256"],"expectedRoadmapRevision":roadmap["revision"]});
    assert!(
        supervisor
            .resident_drone_request(bind)
            .await
            .unwrap()
            .success
    );
    // These are preserved descriptor fields; none is a Drone-control writer.
    {
        let mut descriptor = resident.descriptor.lock().await;
        descriptor
            .create_command
            .rest
            .insert("goal".into(), json!({"status":"active"}));
        descriptor
            .create_command
            .rest
            .insert("threadHeartbeatAdmission".into(), json!(true));
        crate::descriptor::persist_worker(&resident.descriptor_path, &descriptor).unwrap();
    }
    assert!(!supervisor.drone_controls.enabled(ROOT).unwrap());
    assert!(!DroneControls::new(dir.path().join("agent"))
        .enabled(ROOT)
        .unwrap());
    let status = supervisor
        .control_drone(
            &supervisor.drone_control_identity("active").await.unwrap(),
            "status",
            None,
        )
        .await
        .unwrap();
    assert_eq!(status["rootSessionId"], ROOT);
    assert_eq!(status["revision"], 1);
    assert_eq!(status["state"], "paused");
    assert!(supervisor
        .control_drone(
            &ControlIdentity {
                root_session_id: OTHER.into(),
                ..supervisor.drone_control_identity("active").await.unwrap()
            },
            "stop",
            Some(1)
        )
        .await
        .is_err());
}

#[tokio::test]
async fn legacy_root_provenance_is_accepted_but_child_identity_and_mismatches_are_denied() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = Arc::new(
        Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .unwrap(),
    );
    let (resident, _) = resident(&supervisor, dir.path()).await;
    {
        let mut descriptor = resident.descriptor.lock().await;
        descriptor
            .create_command
            .rest
            .insert("parentSessionPath".into(), json!("/creator.jsonl"));
    }
    let identity = supervisor.drone_control_identity("active").await.unwrap();
    let status = supervisor
        .control_drone(&identity, "status", None)
        .await
        .unwrap();
    assert_eq!(status["rootSessionId"], ROOT);
    assert!(supervisor
        .control_drone(
            &ControlIdentity {
                supervisor_generation: "foreign".into(),
                ..identity.clone()
            },
            "status",
            None
        )
        .await
        .is_err());
    assert!(supervisor
        .control_drone(
            &ControlIdentity {
                root_session_id: OTHER.into(),
                ..identity.clone()
            },
            "status",
            None
        )
        .await
        .is_err());
    for (key, value) in [
        ("rlmDepth", json!(1)),
        ("rlmDepth", json!("0")),
        ("rlmChildId", json!("child")),
        (
            "runtimeMetadata",
            json!({"kind":"subagent", "rlmChildId":"child"}),
        ),
        ("runtimeMetadata", json!({"parentActiveSessionId":"parent"})),
        ("runtimeMetadata", json!({"parentSessionId":"parent"})),
        ("runtimeMetadata", json!("invalid")),
    ] {
        let before = {
            let mut descriptor = resident.descriptor.lock().await;
            descriptor.create_command.rest.insert(key.into(), value)
        };
        assert!(
            supervisor.drone_control_identity("active").await.is_err(),
            "{key}"
        );
        assert!(
            supervisor
                .control_drone(&identity, "stop", Some(0))
                .await
                .is_err(),
            "{key}"
        );
        let mut descriptor = resident.descriptor.lock().await;
        match before {
            Some(value) => {
                descriptor.create_command.rest.insert(key.into(), value);
            }
            None => {
                descriptor.create_command.rest.remove(key);
            }
        }
    }
    assert_eq!(
        supervisor
            .control_drone(&identity, "status", None)
            .await
            .unwrap()["revision"],
        0
    );
}

#[tokio::test]
async fn a_child_or_replaced_worker_cannot_reuse_the_root_control_identity() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = Arc::new(
        Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .unwrap(),
    );
    let (resident, _) = resident(&supervisor, dir.path()).await;
    opt_in(&supervisor.drone_controls, ROOT);
    let identity = supervisor.drone_control_identity("active").await.unwrap();
    resident.descriptor.lock().await.worker_instance_id = Some("replacement".into());
    assert!(supervisor
        .control_drone(&identity, "stop", Some(1))
        .await
        .is_err());
    assert!(supervisor.drone_controls.enabled(ROOT).unwrap());
    {
        let mut descriptor = resident.descriptor.lock().await;
        descriptor.worker_instance_id = Some("incarnation".into());
        descriptor
            .create_command
            .rest
            .insert("rlmDepth".into(), json!(1));
    }
    assert!(supervisor.drone_control_identity("active").await.is_err());
    assert!(supervisor
        .control_drone(&identity, "stop", Some(1))
        .await
        .is_err());
    assert!(supervisor.drone_controls.enabled(ROOT).unwrap());
}

#[test]
fn unconfirmed_stop_survives_restart_and_cannot_resume_or_admit_work() {
    let dir = tempfile::tempdir().unwrap();
    let controls = DroneControls::new(dir.path().into());
    opt_in(&controls, ROOT);
    opt_in(&controls, OTHER);
    let run = controls.begin(&assessment(ROOT)).unwrap().unwrap();
    controls.root(ROOT).unwrap().mutate("stop", 1).unwrap();
    run.finish(false).unwrap();
    let reopened = DroneControls::new(dir.path().into());
    let status = reopened.status(ROOT).unwrap();
    assert_eq!(
        (
            status["state"].clone(),
            status["enabled"].clone(),
            status["paused"].clone(),
            status["pendingCancel"].clone(),
            status["cancellationUnconfirmed"].clone()
        ),
        (
            json!("cancellation_unconfirmed"),
            json!(false),
            json!(false),
            json!(true),
            json!(true)
        )
    );
    assert!(!reopened.enabled(ROOT).unwrap());
    assert!(reopened.begin(&assessment(ROOT)).unwrap().is_none());
    assert!(reopened
        .root(ROOT)
        .unwrap()
        .mutate("resume", status["revision"].as_u64().unwrap())
        .is_err());
    assert!(reopened.enabled(OTHER).unwrap());
}

#[tokio::test]
async fn restart_between_durable_stop_and_settlement_remains_held_until_exact_owner_settles() {
    let dir = tempfile::tempdir().unwrap();
    let controls = DroneControls::new(dir.path().into());
    opt_in(&controls, ROOT);
    let run = controls.begin(&assessment(ROOT)).unwrap().unwrap();
    let mut settled = controls
        .root(ROOT)
        .unwrap()
        .mutate("stop", 1)
        .unwrap()
        .unwrap();
    let reopened = DroneControls::new(dir.path().into());
    let before = reopened.status(ROOT).unwrap();
    assert_eq!(before["state"], "cancellation_unconfirmed");
    assert_eq!(before["paused"], false);
    assert_eq!(before["pendingCancel"], true);
    assert!(!reopened.enabled(ROOT).unwrap());
    assert!(reopened.begin(&assessment(ROOT)).unwrap().is_none());
    assert!(reopened
        .root(ROOT)
        .unwrap()
        .mutate("resume", before["revision"].as_u64().unwrap())
        .is_err());
    // The surviving original owner now supplies actual positive settlement, not a restart guess.
    run.finish(true).unwrap();
    settled.changed().await.unwrap();
    assert!(*settled.borrow());
    let after = reopened.status(ROOT).unwrap();
    assert_eq!(after["state"], "paused");
    assert_eq!(after["pendingCancel"], false);
    assert!(
        !reopened.enabled(ROOT).unwrap(),
        "confirmed cancellation does not implicitly resume"
    );
    reopened
        .root(ROOT)
        .unwrap()
        .mutate("resume", after["revision"].as_u64().unwrap())
        .unwrap();
    assert!(reopened.enabled(ROOT).unwrap());
}

#[test]
fn non_user_stop_unknown_reports_disabled_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let controls = DroneControls::new(dir.path().into());
    opt_in(&controls, ROOT);
    controls
        .begin(&assessment(ROOT))
        .unwrap()
        .unwrap()
        .finish(false)
        .unwrap();
    for control in [&controls, &DroneControls::new(dir.path().into())] {
        let status = control.status(ROOT).unwrap();
        assert_eq!(status["userStopped"], false);
        assert_eq!(status["enabled"], false);
        assert_eq!(status["enabled"], control.enabled(ROOT).unwrap());
        assert_eq!(status["state"], "cancellation_unconfirmed");
        assert_eq!(status["pendingCancel"], true);
        assert!(control.begin(&assessment(ROOT)).unwrap().is_none());
        assert!(control
            .root(ROOT)
            .unwrap()
            .mutate("resume", status["revision"].as_u64().unwrap())
            .is_err());
    }
}
