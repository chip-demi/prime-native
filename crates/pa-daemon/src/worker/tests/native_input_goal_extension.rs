use super::*;

#[tokio::test]
async fn native_input_goal_extension_unpersisted_row_never_receipted() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("durable.jsonl");
    {
        let mut core = worker.core.lock().unwrap();
        let store = core.store.as_mut().unwrap();
        store.set_path(path.clone());
        store.rewrite().unwrap();
        store.append_entry(
            "message",
            json!({"queueKey":"unflushed","message":{"role":"user","content":"not committed"}}),
        );
    }
    let result = worker
        .dispatch("get_input_receipts", &json!({"queueKey":"unflushed"}))
        .await;
    assert!(result.success, "{result:?}");
    assert_eq!(result.data.unwrap()["receipts"], json!([]));
    std::fs::remove_file(path).unwrap();
    assert!(
        !worker
            .dispatch("get_input_receipts", &json!({"queueKey":"unflushed"}))
            .await
            .success
    );
}

#[test]
fn native_input_goal_extension_internal_withdrawal_preserves_public_keys() {
    for key in [AUTONOMOUS_QUEUE_KEY, "heartbeat:test"] {
        let mut public = QueuedItem {
            priority: QueuePriority::Human,
            preview: None,
            message: "human".to_string(),
            custom_message: None,
            agent_message: None,
            input_identity: None,
            queue_key: Some(key.to_string()),
            admission_id: None,
            images: Vec::new(),
            done: None,
            queue_visible: true,
            policy: TurnPolicy::Queued,
            forced_batch: false,
        };
        assert!(!is_internal_queue_key(&public, key));
        if key == AUTONOMOUS_QUEUE_KEY {
            public.policy = TurnPolicy::Injected;
            public.queue_visible = false;
        } else {
            public.custom_message = Some(json!({"role":"custom","customType":"heartbeat_prompt"}));
            public.priority = QueuePriority::Background;
        }
        assert!(is_internal_queue_key(&public, key));
        assert!(!is_internal_queue_key(&public, "unrelated"));
    }
}

#[tokio::test]
async fn native_input_goal_extension_receipts_reload_and_ambiguity() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.jsonl");
    {
        let mut core = worker.core.lock().unwrap();
        core.store.as_mut().unwrap().set_path(path.clone());
        core.store.as_ref().unwrap().rewrite().unwrap();
    }
    for key in ["first", "second", "first"] {
        let result = worker
            .dispatch(
                "prompt_and_wait",
                &json!({"message":"same text", "queueKey":key}),
            )
            .await;
        assert!(result.success, "{result:?}");
    }
    // Replace the in-memory store from disk: no cache or text matching can supply this mapping.
    worker.core.lock().unwrap().store = Some(SessionFile::open(&path).unwrap());
    let first = worker
        .dispatch("get_input_receipts", &json!({"queueKey":"first"}))
        .await;
    let value = serde_json::to_value(first).unwrap();
    let receipts = value["data"]["receipts"].as_array().unwrap();
    assert_eq!(receipts.len(), 2);
    assert_ne!(receipts[0]["messageId"], receipts[1]["messageId"]);
    let absent = worker
        .dispatch("get_input_receipts", &json!({"queueKey":"absent"}))
        .await;
    assert_eq!(
        serde_json::to_value(absent).unwrap()["data"]["receipts"],
        json!([])
    );
    let full = SessionFile::open(&path).unwrap();
    let users: Vec<_> = full
        .entries
        .iter()
        .filter(|e| e.fields["message"]["role"] == "user")
        .collect();
    assert_eq!(users.len(), 3);
    for user in users {
        assert_eq!(user.fields["queueKey"], user.fields["message"]["queueKey"]);
    }
}

#[tokio::test]
async fn native_input_goal_extension_queue_keys_and_custom_priority() {
    let worker = created_dispatch_worker().await;
    let journal_dir = tempfile::tempdir().unwrap();
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&journal_dir.path().join("recovery.jsonl")).unwrap());
    // Hold the real admission pause while inspecting/checkpointing admitted queues.
    assert!(worker.dispatch("acquire_session_input_pause", &json!({
        "activeSessionId":"suspension-session", "clientId":"test", "leaseKey":"correlation"
    })).await.success);
    worker.core.lock().unwrap().busy = true;
    assert!(
        worker
            .handle_prompt(&json!({"message":"same", "queueKey":"prompt"}), false)
            .await
            .success
    );
    assert!(
        worker
            .handle_queue(
                &json!({"message":"same", "queueKey":"steer"}),
                Lane::Steering
            )
            .success
    );
    assert!(worker.handle_queue(&json!({"message":"same", "queueKey":"follow", "customMessage":{"role":"custom","customType":"test","content":"same"}}), Lane::FollowUp).success);
    let core = worker.core.lock().unwrap();
    let snapshot = queue_lanes(&core);
    assert!(snapshot
        .steering
        .iter()
        .any(|item| item.queue_key.as_deref() == Some("steer")));
    assert!(snapshot
        .follow_up
        .iter()
        .chain(&snapshot.steering)
        .any(|item| item.queue_key.as_deref() == Some("prompt")));
    let custom = core
        .follow_up
        .iter()
        .find(|item| item.queue_key.as_deref() == Some("follow"))
        .unwrap();
    assert_eq!(custom.priority, QueuePriority::Background);
    drop(core);
    let recovery = worker.recovery.lock().unwrap();
    let (steering, follow_up, _suspended) =
        restore_queue_snapshot(recovery.as_ref().unwrap(), "suspension-session");
    assert!(steering
        .iter()
        .any(|item| item.queue_key.as_deref() == Some("steer")));
    assert!(follow_up
        .iter()
        .chain(&steering)
        .any(|item| item.queue_key.as_deref() == Some("prompt")));
    assert!(follow_up
        .iter()
        .any(|item| item.queue_key.as_deref() == Some("follow")));
    for item in steering.iter().chain(&follow_up) {
        assert_eq!(
            item.priority,
            if item.custom_message.is_some() {
                QueuePriority::Background
            } else {
                QueuePriority::Human
            }
        );
    }
}

#[tokio::test]
async fn native_input_goal_extension_cancelled_owned_queue_has_no_receipt() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    {
        let mut core = worker.core.lock().unwrap();
        let store = core.store.as_mut().unwrap();
        store.set_path(dir.path().join("cancel.jsonl"));
        store.rewrite().unwrap();
    }
    assert!(
        worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId":"suspension-session", "clientId":"test", "leaseKey":"cancel"
                })
            )
            .await
            .success
    );
    let admitted = worker
        .handle_prompt(
            &json!({"message":"not delivered", "queueKey":"cancelled", "admissionId":"admission"}),
            false,
        )
        .await;
    assert!(admitted.success);
    assert!(
        worker
            .handle_cancel_prompt_admission(&json!({"admissionId":"admission", "cancelOwned":true}))
            .success
    );
    let result = worker
        .dispatch("get_input_receipts", &json!({"queueKey":"cancelled"}))
        .await;
    assert_eq!(
        serde_json::to_value(result).unwrap()["data"]["receipts"],
        json!([])
    );
    let core = worker.core.lock().unwrap();
    assert!(core.steering.is_empty() && core.follow_up.is_empty());
}

#[tokio::test]
async fn native_input_withdrawal_exact_admission_drain_restart_and_scope() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&dir.path().join("worker-recovery.jsonl")).unwrap());
    let path = dir.path().join("withdraw.jsonl");
    {
        let mut core = worker.core.lock().unwrap();
        let store = core.store.as_mut().unwrap();
        store.set_path(path.clone());
        store.rewrite().unwrap();
    }
    assert!(
        worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId":"suspension-session","clientId":"test","leaseKey":"withdraw"
                })
            )
            .await
            .success
    );
    let accepted = worker.handle_queue(
        &json!({"extensionVersion":2,"queueKey":"owned",
        "message":"parked"}),
        Lane::FollowUp,
    );
    assert!(accepted.success, "{accepted:?}");
    let identity = accepted.data.unwrap()["inputIdentity"].clone();
    let request = json!({"extensionVersion":2,"queueKey":"owned","inputIdentity":identity});
    assert_eq!(
        worker.handle_get_input_receipts(&request).data.unwrap()["disposition"],
        "unknown"
    );
    let stale = {
        let core = worker.core.lock().unwrap();
        queue_lanes(&core).follow_up
    };
    // Ignore the clearing ACK: restart must recover the same durable proof.
    assert!(
        worker
            .dispatch("abort_and_clear_queue", &json!({}))
            .await
            .success
    );
    {
        let mut core = worker.core.lock().unwrap();
        core.store = Some(SessionFile::open(&path).unwrap());
        core.store.as_ref().unwrap().rewrite().unwrap();
    }
    assert_eq!(
        worker.handle_get_input_receipts(&request).data.unwrap()["disposition"],
        "withdrawn_never_committed"
    );
    let mut foreign = request.clone();
    foreign["inputIdentity"]["sessionFile"] = json!("/foreign");
    assert_eq!(
        worker.handle_get_input_receipts(&foreign).data.unwrap()["disposition"],
        "unknown"
    );
    for (field, value) in [
        ("admissionId", json!("not-a-uuid")),
        ("admissionId", json!(uuid::Uuid::new_v4().to_string())),
        ("sessionId", json!("foreign-session")),
        ("activeSessionId", json!("foreign-worker")),
        ("cwd", json!("/foreign-cwd")),
        ("queueKey", json!("foreign-key")),
    ] {
        let mut foreign = request.clone();
        foreign["inputIdentity"][field] = value;
        assert_eq!(
            worker.handle_get_input_receipts(&foreign).data.unwrap()["disposition"],
            "unknown",
            "{field}"
        );
    }
    assert_eq!(
        worker
            .handle_get_input_receipts(&json!({"extensionVersion":2,"queueKey":"owned"}))
            .data
            .unwrap()["disposition"],
        "unknown"
    );
    // A stale snapshot is filtered at the real pickup lock boundary.
    let mut core = worker.core.lock().unwrap();
    for row in stale {
        core.follow_up.push_back(QueuedItem {
            message: row.message,
            priority: QueuePriority::Human,
            preview: None,
            custom_message: None,
            agent_message: None,
            queue_key: row.queue_key,
            input_identity: row.input_identity,
            admission_id: None,
            images: Vec::new(),
            done: None,
            queue_visible: true,
            policy: TurnPolicy::Queued,
            forced_batch: false,
        });
    }
    assert!(gather_delivery_batch(&mut core, Lane::FollowUp).is_empty());
    assert!(core.follow_up.is_empty());
    // A durable positive row always defeats negative proof; duplicate positives are unknown.
    let fields = json!({"queueKey":"owned","inputIdentity":identity,"message":{"role":"user","content":"committed"}});
    core.store
        .as_mut()
        .unwrap()
        .persist_entry("message", fields.clone())
        .unwrap();
    drop(core);
    assert_eq!(
        worker.handle_get_input_receipts(&request).data.unwrap()["disposition"],
        "committed"
    );
    worker
        .core
        .lock()
        .unwrap()
        .store
        .as_mut()
        .unwrap()
        .persist_entry("message", fields)
        .unwrap();
    assert_eq!(
        worker.handle_get_input_receipts(&request).data.unwrap()["disposition"],
        "unknown"
    );
}

#[tokio::test]
async fn native_input_withdrawal_append_failure_retains_queue_and_suspends() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&dir.path().join("worker-recovery.jsonl")).unwrap());
    let path = dir.path().join("failure.jsonl");
    {
        let mut core = worker.core.lock().unwrap();
        core.store.as_mut().unwrap().set_path(path.clone());
        core.store.as_ref().unwrap().rewrite().unwrap();
    }
    assert!(
        worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId":"suspension-session","clientId":"test","leaseKey":"failure"
                })
            )
            .await
            .success
    );
    let accepted = worker.handle_queue(
        &json!({"extensionVersion":2,"queueKey":"owned",
        "message":"retained"}),
        Lane::FollowUp,
    );
    assert!(accepted.success, "{accepted:?}");
    // Replace the fixture file with a directory: real append fails, no synthetic success.
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(
        !worker
            .dispatch("abort_and_clear_queue", &json!({}))
            .await
            .success
    );
    let core = worker.core.lock().unwrap();
    assert!(core.queued_input_suspended);
    assert_eq!(core.follow_up.len(), 1);
}

#[tokio::test]
async fn native_input_withdrawal_pickup_wins_and_duplicate_keys_are_unknown() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&dir.path().join("worker-recovery.jsonl")).unwrap());
    {
        let mut core = worker.core.lock().unwrap();
        core.store
            .as_mut()
            .unwrap()
            .set_path(dir.path().join("race.jsonl"));
        core.store.as_ref().unwrap().rewrite().unwrap();
    }
    assert!(
        worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId":"suspension-session","clientId":"test","leaseKey":"race"
                })
            )
            .await
            .success
    );
    let admitted = worker.handle_queue(
        &json!({"extensionVersion":2,"queueKey":"race","message":"picked"}),
        Lane::FollowUp,
    );
    let identity = admitted.data.unwrap()["inputIdentity"].clone();
    let picked = gather_delivery_batch(&mut worker.core.lock().unwrap(), Lane::FollowUp);
    assert_eq!(picked.len(), 1);
    assert!(
        worker
            .dispatch("abort_and_clear_queue", &json!({}))
            .await
            .success
    );
    let request = json!({"extensionVersion":2,"queueKey":"race","inputIdentity":identity});
    assert_eq!(
        worker.handle_get_input_receipts(&request).data.unwrap()["disposition"],
        "unknown"
    );
    assert!(
        worker
            .handle_queue(
                &json!({"extensionVersion":2,"queueKey":"race","message":"duplicate"}),
                Lane::FollowUp
            )
            .success
    );
    assert!(worker.dispatch("clear_queue", &json!({})).await.success);
    assert_eq!(
        worker.handle_get_input_receipts(&request).data.unwrap()["disposition"],
        "unknown"
    );
}

#[tokio::test]
async fn native_input_withdrawal_parallel_pickup_clear_is_exclusive() {
    for _ in 0..24 {
        let worker = created_dispatch_worker().await;
        let dir = tempfile::tempdir().unwrap();
        *worker.recovery.lock().unwrap() =
            Some(WorkerRecoveryJournal::open(&dir.path().join("worker-recovery.jsonl")).unwrap());
        {
            let mut core = worker.core.lock().unwrap();
            core.store
                .as_mut()
                .unwrap()
                .set_path(dir.path().join("parallel.jsonl"));
            core.store.as_ref().unwrap().rewrite().unwrap();
        }
        assert!(worker.dispatch("acquire_session_input_pause", &json!({
            "activeSessionId":"suspension-session","clientId":"test","leaseKey":"parallel"
        })).await.success);
        let admitted = worker.handle_queue(
            &json!({"extensionVersion":2,"queueKey":"parallel","message":"exclusive"}),
            Lane::FollowUp,
        );
        assert!(admitted.success, "{admitted:?}");
        let identity = admitted.data.unwrap()["inputIdentity"].clone();
        let barrier = std::sync::Barrier::new(2);
        let runtime = tokio::runtime::Handle::current();
        let (picked, cleared) = std::thread::scope(|scope| {
            let pickup = scope.spawn(|| {
                barrier.wait();
                gather_delivery_batch(&mut worker.core.lock().unwrap(), Lane::FollowUp)
            });
            let clear = scope.spawn(|| {
                barrier.wait();
                runtime.block_on(worker.dispatch("clear_queue", &json!({})))
            });
            (pickup.join().unwrap(), clear.join().unwrap())
        });
        assert!(cleared.success, "{cleared:?}");
        let cleared_count = cleared.data.unwrap()["followUp"].as_array().unwrap().len();
        assert_eq!(picked.len() + cleared_count, 1);
        let proof = worker.handle_get_input_receipts(
            &json!({"extensionVersion":2,"queueKey":"parallel","inputIdentity":identity}),
        );
        assert!(proof.success, "{proof:?}");
        assert_eq!(
            proof.data.unwrap()["disposition"],
            if picked.is_empty() {
                "withdrawn_never_committed"
            } else {
                "unknown"
            }
        );
        let core = worker.core.lock().unwrap();
        assert!(core.follow_up.is_empty());
    }
}

#[tokio::test]
async fn native_input_withdrawal_post_sync_partial_clear_restart_filters_only_persisted() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&dir.path().join("worker-recovery.jsonl")).unwrap());
    let path = dir.path().join("partial.jsonl");
    {
        let mut core = worker.core.lock().unwrap();
        core.store.as_mut().unwrap().set_path(path.clone());
        core.store.as_ref().unwrap().rewrite().unwrap();
    }
    assert!(
        worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId":"suspension-session","clientId":"test","leaseKey":"partial"
                })
            )
            .await
            .success
    );
    let mut identities = Vec::new();
    for key in ["first", "second"] {
        let admitted = worker.handle_queue(
            &json!({"extensionVersion":2,"queueKey":key,"message":key}),
            Lane::FollowUp,
        );
        assert!(admitted.success, "{admitted:?}");
        identities.push(admitted.data.unwrap()["inputIdentity"].clone());
    }
    SessionFile::fail_next_input_withdrawal_after_append();
    let failed = worker.dispatch("clear_queue", &json!({})).await;
    assert!(!failed.success, "{failed:?}");
    assert!(failed.error.unwrap().contains("post-sync withdrawal"));
    {
        let mut core = worker.core.lock().unwrap();
        assert!(core.queued_input_suspended);
        assert_eq!(core.follow_up.len(), 2);
        assert!(!core
            .store
            .as_ref()
            .unwrap()
            .entries
            .iter()
            .any(|entry| entry.fields["customType"] == "input_withdrawal"));
        core.store = Some(SessionFile::open(&path).unwrap());
    }
    for (index, key) in ["first", "second"].into_iter().enumerate() {
        let result = worker.handle_get_input_receipts(
            &json!({"extensionVersion":2,"queueKey":key,"inputIdentity":identities[index]}),
        );
        assert!(result.success, "{result:?}");
        assert_eq!(
            result.data.unwrap()["disposition"],
            if index == 0 {
                "withdrawn_never_committed"
            } else {
                "unknown"
            }
        );
    }
    let picked = gather_delivery_batch(&mut worker.core.lock().unwrap(), Lane::FollowUp);
    assert_eq!(picked.len(), 1);
    assert_eq!(picked[0].queue_key.as_deref(), Some("second"));
    assert!(worker.core.lock().unwrap().follow_up.is_empty());
}

#[tokio::test]
async fn native_input_withdrawal_semantic_compaction_and_replacement_preserve_proof() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&dir.path().join("worker-recovery.jsonl")).unwrap());
    let path = dir.path().join("compact.jsonl");
    {
        let mut core = worker.core.lock().unwrap();
        core.store.as_mut().unwrap().set_path(path.clone());
        core.store.as_ref().unwrap().rewrite().unwrap();
    }
    assert!(
        worker
            .dispatch("prompt_and_wait", &json!({"message":"seed context"}))
            .await
            .success
    );
    assert!(
        worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId":"suspension-session","clientId":"test","leaseKey":"compact"
                })
            )
            .await
            .success
    );
    let admitted = worker.handle_queue(
        &json!({"extensionVersion":2,"queueKey":"compact","message":"never deliver"}),
        Lane::FollowUp,
    );
    assert!(admitted.success, "{admitted:?}");
    let identity = admitted.data.unwrap()["inputIdentity"].clone();
    assert!(worker.dispatch("clear_queue", &json!({})).await.success);
    let compact = worker.dispatch("compact", &json!({})).await;
    assert!(compact.success, "{compact:?}");
    let durable = SessionFile::open(&path).unwrap();
    assert!(durable
        .entries
        .iter()
        .any(|entry| entry.type_ == "compaction"));
    let replacement = created_dispatch_worker().await;
    {
        let mut core = replacement.core.lock().unwrap();
        core.active_session_id = "replacement-worker".into();
        core.store = Some(durable);
    }
    let result = replacement.handle_get_input_receipts(
        &json!({"extensionVersion":2,"queueKey":"compact","inputIdentity":identity}),
    );
    assert!(result.success, "{result:?}");
    assert_eq!(
        result.data.unwrap()["disposition"],
        "withdrawn_never_committed"
    );
}

#[tokio::test]
async fn session_completion_busy_creator_durable_queue_stop_and_duplicate() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&dir.path().join("recovery.jsonl")).unwrap());
    {
        let mut core = worker.core.lock().unwrap();
        let store = core.store.as_mut().unwrap();
        store.set_path(dir.path().join("creator.jsonl"));
        store.rewrite().unwrap();
        core.busy = true;
        core.queued_input_suspended = true;
    }
    let payload = json!({"message":"settled, not accepted", "extensionVersion":2,
        "source":"rlm_session_completion", "completionTargetFile":dir.path().join("creator.jsonl"), "queueKey":"delivery-1"});
    let response = worker.handle_queue(&payload, Lane::FollowUp);
    assert!(response.success, "{response:?}");
    {
        let core = worker.core.lock().unwrap();
        assert!(
            core.queued_input_suspended,
            "notice cannot resume owner stop"
        );
        assert_eq!(core.follow_up.len(), 1);
        let journal = worker.recovery.lock().unwrap();
        let (_, restored, suspended) =
            restore_queue_snapshot(journal.as_ref().unwrap(), &core.active_session_id);
        assert_eq!(restored.len(), 1);
        assert!(suspended);
    }
    assert!(!worker.handle_queue(&payload, Lane::FollowUp).success);
    let receipt = worker.handle_get_input_receipts(
        &json!({"queueKey":"delivery-1", "completionTargetFile":dir.path().join("creator.jsonl")}),
    );
    assert!(receipt.success);
    assert_eq!(receipt.data.unwrap()["durablyQueued"], true);
}
