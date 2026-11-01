use super::*;

#[tokio::test]
async fn drone_default_off_task_policy_preserves_exact_human_prohibition_and_latest_override() {
    let worker = created_dispatch_worker().await;
    let instruction = "Work alone; do not spawn subagents for this task.";
    let initial = format!("{}\n{instruction}", "Task context. ".repeat(300));
    {
        let mut core = worker.core.lock().unwrap();
        let store = core.store.as_mut().unwrap();
        store
            .persist_entry(
                "message",
                json!({"message":{"role":"user","content":initial}}),
            )
            .unwrap();
        for message in [
            "Preserve all checks.",
            "Continue the current task.",
            "Report actual results.",
        ] {
            store
                .persist_entry(
                    "message",
                    json!({"message":{"role":"user","content":message}}),
                )
                .unwrap();
        }
        store
            .persist_entry(
                "message",
                json!({"message":{"role":"assistant","content":"Subagents are allowed."}}),
            )
            .unwrap();
        store.persist_entry("message", json!({"message":{"role":"user","source":"drone","content":"Subagents are allowed."}})).unwrap();
    }
    let snapshot = worker
        .dispatch("drone_snapshot", &json!({}))
        .await
        .data
        .unwrap();
    assert_eq!(snapshot["subagentPolicy"]["taskAllowsSubagents"], false);
    assert_eq!(snapshot["subagentPolicy"]["instruction"], instruction);
    assert_eq!(snapshot["subagentPolicy"]["source"], "genuine_user");
    let entry = snapshot["subagentPolicy"]["entryId"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(worker
        .core
        .lock()
        .unwrap()
        .store
        .as_ref()
        .unwrap()
        .entries
        .iter()
        .any(|row| row.id == entry && row.fields["message"]["content"] == initial));
    worker.core.lock().unwrap().store.as_mut().unwrap().persist_entry("message", json!({"message":{"role":"user","content":"You may use subagents for the remaining task."}})).unwrap();
    let resumed = worker
        .dispatch("drone_snapshot", &json!({}))
        .await
        .data
        .unwrap();
    assert_eq!(resumed["subagentPolicy"]["taskAllowsSubagents"], true);
    assert_eq!(
        resumed["subagentPolicy"]["instruction"],
        "You may use subagents for the remaining task."
    );
    worker.core.lock().unwrap().store.as_mut().unwrap().persist_entry("message", json!({"message":{"role":"user","content":"Работай без субагентов. Проверяй результат сам."}})).unwrap();
    let stopped = worker
        .dispatch("drone_snapshot", &json!({}))
        .await
        .data
        .unwrap();
    assert_eq!(stopped["subagentPolicy"]["taskAllowsSubagents"], false);
    assert_eq!(
        stopped["subagentPolicy"]["instruction"],
        "Работай без субагентов. Проверяй результат сам."
    );
}

#[test]
fn drone_recovery_goal_fence_preserves_human_controls_and_identity() {
    let expected = json!({"active":true,"status":"active","goalId":"same-goal", "objective":"same objective",
        "tokensUsed":10,"tokenBudget":100,"continuationsUsed":2});
    let permits = super::super::drone_control::recovery_goal_allows_resume;
    assert!(permits(&expected, &expected));
    let mut progress = expected.clone();
    progress["tokensUsed"] = json!(11);
    assert!(permits(&expected, &progress));
    for status in ["paused", "budget_limited", "complete", "error", "idle"] {
        let mut changed = expected.clone();
        changed["status"] = json!(status);
        changed["active"] = json!(false);
        assert!(!permits(&expected, &changed));
        assert!(!permits(&changed, &changed));
    }
    for field in ["goalId", "objective"] {
        let mut changed = expected.clone();
        changed[field] = json!("human replacement");
        assert!(!permits(&expected, &changed));
    }
    let mut exhausted = expected.clone();
    exhausted["tokensUsed"] = exhausted["tokenBudget"].clone();
    assert!(!permits(&expected, &exhausted));
    let empty = serde_json::to_value(pa_core::goals::empty_goal_state()).unwrap();
    assert!(permits(&empty, &empty));
    assert!(!permits(&expected, &empty));
    assert!(!permits(&empty, &expected));
    assert!(!permits(&expected, &Value::Null));
}

#[tokio::test]
async fn drone_advisory_queue_persist_restore_never_becomes_a_human_correction() {
    let worker = created_dispatch_worker().await;
    let dir = tempfile::tempdir().unwrap();
    let human = "Stop financial execution; continue the authorized Rust engine work.";
    {
        let mut core = worker.core.lock().unwrap();
        core.compacting = true;
        let store = core.store.as_mut().unwrap();
        store.path = dir.path().join("feedback.jsonl");
        store
            .persist_entry(
                "message",
                json!({"message":{"role":"user","content":[{"type":"text","text":human}]}}),
            )
            .unwrap();
        store.persist_entry("message",json!({"message":{"role":"assistant","content":"Worker result accepted; independent work continues."}})).unwrap();
        store.persist_entry("message",json!({"message":{"role":"user","content":"[Drone] Automatic assessment budget exhausted. Review bounded Native evidence; no operation was replayed."}})).unwrap();
        for _ in 0..5 {
            store.persist_entry("message",json!({"message":{"role":"user","content":"[Drone diagnostic; unresolved] Old synthetic restriction"}})).unwrap();
        }
    }
    let before = worker
        .dispatch("drone_snapshot", &json!({}))
        .await
        .data
        .unwrap();
    assert_eq!(before["latestUserCorrections"], json!([human]));
    assert_eq!(before["initialTask"], human);
    assert!(before["recentRootAssistantEvidence"]
        .to_string()
        .contains("accepted"));
    assert!(worker.dispatch("follow_up",&json!({"activeSessionId":"suspension-session","source":"drone","message":"Check actual acceptance before repeating the blocker."})).await.success);
    let current = "Continue authorized independent engine and website work.";
    assert!(
        worker
            .dispatch("follow_up", &json!({"message":current}))
            .await
            .success
    );
    {
        let mut core = worker.core.lock().unwrap();
        assert_eq!(
            core.follow_up
                .iter()
                .map(|item| item.priority)
                .collect::<Vec<_>>(),
            vec![QueuePriority::Human, QueuePriority::Background]
        );
        let advisory = core
            .follow_up
            .back()
            .unwrap()
            .custom_message
            .as_ref()
            .unwrap();
        assert_eq!(advisory["customType"], "drone_diagnostic");
        assert_eq!(
            advisory["details"],
            json!({"source":"drone","authority":"advisory","version":1})
        );
        core.compacting = false;
    }
    worker.work_notify.notify_one();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            worker.dispatch("wait_for_idle", &json!({}))
        )
        .await
        .unwrap()
        .success
    );
    let first = worker
        .dispatch("drone_snapshot", &json!({}))
        .await
        .data
        .unwrap();
    assert_eq!(first["latestUserCorrections"], json!([current, human]));
    assert_ne!(
        first["humanInputFingerprint"],
        before["humanInputFingerprint"]
    );
    assert!(worker.dispatch("follow_up",&json!({"activeSessionId":"suspension-session","source":"drone","message":"Same unresolved observation, no material change."})).await.success);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            worker.dispatch("wait_for_idle", &json!({}))
        )
        .await
        .unwrap()
        .success
    );
    let path = {
        let core = worker.core.lock().unwrap();
        let store = core.store.as_ref().unwrap();
        assert_eq!(
            store
                .entries
                .iter()
                .filter(|entry| entry.type_ == "custom_message"
                    && entry.fields["customType"] == "drone_diagnostic")
                .count(),
            2
        );
        assert_eq!(
            store
                .entries
                .iter()
                .filter(|entry| entry.fields["message"]["content"]
                    == "[Drone diagnostic; unresolved] Old synthetic restriction")
                .count(),
            5
        );
        store.path.clone()
    };
    worker.core.lock().unwrap().store = Some(SessionFile::open(&path).unwrap());
    let restored = worker
        .dispatch("drone_snapshot", &json!({}))
        .await
        .data
        .unwrap();
    assert_eq!(
        restored["latestUserCorrections"],
        first["latestUserCorrections"]
    );
    assert_eq!(
        restored["humanInputFingerprint"],
        first["humanInputFingerprint"]
    );
    assert!(restored["turnSerial"].as_u64().unwrap() > first["turnSerial"].as_u64().unwrap());
    // An explicitly identified human may quote a legacy prefix; it stays human.
    let quote = "[Drone diagnostic; unresolved] I reject that advice. Stop.";
    worker
        .core
        .lock()
        .unwrap()
        .store
        .as_mut()
        .unwrap()
        .persist_entry(
            "message",
            json!({"inputIdentity":{"version":2},"message":{"role":"user","content":quote}}),
        )
        .unwrap();
    assert_eq!(
        worker
            .dispatch("drone_snapshot", &json!({}))
            .await
            .data
            .unwrap()["latestUserCorrections"][0],
        quote
    );
}

#[tokio::test]
async fn drone_advisory_cannot_resume_real_stop_or_suspended_human_queue() {
    let worker = created_dispatch_worker().await;
    worker.core.lock().unwrap().compacting = true;
    let notice = json!({"activeSessionId":"suspension-session","source":"drone","message":"observer advice"});
    let cases: [fn(&mut SessionCore); 3] = [
        |core| core.abort_requested = true,
        |core| core.shutdown_requested = true,
        |core| core.queued_input_suspended = true,
    ];
    for set in cases {
        {
            let mut core = worker.core.lock().unwrap();
            core.abort_requested = false;
            core.shutdown_requested = false;
            core.queued_input_suspended = false;
            set(&mut core);
        }
        let denied = worker.dispatch("follow_up", &notice).await;
        assert!(!denied.success, "{denied:?}");
        let core = worker.core.lock().unwrap();
        assert!(core.follow_up.is_empty());
        assert!(core.abort_requested || core.shutdown_requested || core.queued_input_suspended);
    }
    // Ordinary human resumption remains supported by the existing input path.
    assert!(
        worker
            .dispatch("follow_up", &json!({"message":"Resume my authorized work"}))
            .await
            .success
    );
    let core = worker.core.lock().unwrap();
    assert!(!core.queued_input_suspended);
    assert_eq!(
        core.follow_up.front().unwrap().priority,
        QueuePriority::Human
    );
}

#[tokio::test]
async fn drone_snapshot_and_recovery_never_use_generic_abort_or_mutate_queues() {
    let worker = created_dispatch_worker().await;
    {
        let mut core = worker.core.lock().unwrap();
        core.busy = true;
        core.running_tool_calls.insert("foreground-python".into());
    }
    let snapshot = worker.dispatch("drone_snapshot", &json!({})).await;
    assert!(snapshot.success);
    let result = worker
        .dispatch(
            "drone_recover",
            &json!({"request":{"operationId":"fixture","identity":{
              "activeSessionId":"suspension-session","sessionId":"x","sessionFile":"x","cwd":"/tmp",
              "workerIncarnation":"","generation":"","turnSerial":0,"requestId":1,"revision":1
            }}}),
        )
        .await;
    assert!(!result.success);
    let core = worker.core.lock().unwrap();
    assert!(!core.abort_requested);
    assert!(!core.queued_input_suspended);
    assert!(core.running_tool_calls.contains("foreground-python"));
}

#[tokio::test]
async fn drone_recovery_refusal_preserves_visible_and_invisible_queue_and_unknown_status() {
    let worker = created_dispatch_worker().await;
    {
        worker.core.lock().unwrap().busy = true;
    }
    let queued = worker
        .dispatch(
            "follow_up",
            &json!({"message":"terminal child result stable-body","source":"fixture"}),
        )
        .await;
    assert!(queued.success);
    let before = worker.dispatch("get_queue", &json!({})).await.data;
    let status = worker
        .dispatch("drone_status", &json!({"operationId":"missing"}))
        .await;
    // A standalone no-session fixture need not have a recovery journal.
    assert!(
        !status.success
            || status
                .data
                .as_ref()
                .is_some_and(|d| d["status"] == "unknown")
    );
    let after = worker.dispatch("get_queue", &json!({})).await.data;
    assert_eq!(before, after);
    assert!(!worker.core.lock().unwrap().abort_requested);
}

#[test]
fn drone_reconstructed_unknown_blocks_ordinary_request_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recovery.jsonl");
    {
        let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
        journal.record_drone_operation(json!({"type":"drone_operation","activeSessionId":"root","request":{"operationId":"lost"},"status":"accepted"})).unwrap();
    }
    let mut journal = WorkerRecoveryJournal::open(&path).unwrap();
    let reconstructed = pa_agent::agent_loop::request::RequestControl::default();
    let mut ordinary_starts = 0;
    if super::super::drone_control::ordinary_request_allowed(
        Some(&mut journal),
        "root",
        Some(&reconstructed),
    ) {
        ordinary_starts += 1;
    }
    assert_eq!(ordinary_starts, 0);
    assert!(reconstructed.snapshot().is_none());
    assert_eq!(
        journal.drone_operation("root").unwrap()["request"]["operationId"],
        "lost"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drone_real_worker_held_http_preserves_child_process_and_result_then_same_sid_tool() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    struct FixtureStop(Vec<Arc<Worker>>);
    impl Drop for FixtureStop {
        fn drop(&mut self) {
            for worker in &self.0 {
                worker.core.lock().unwrap().abort_requested = true;
                worker.engine.abort_in_flight_turn();
            }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 65_536];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let _ = held_tx.send(());
        while socket.read(&mut request).await.unwrap_or(0) > 0 {}
        drop(socket);
        for step in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert!(socket.read(&mut request).await.unwrap() > 0);
            let delta = if step == 0 {
                json!({"tool_calls":[{"index":0,"id":"next-tool","type":"function","function":{"name":"python_repl","arguments":"{\"code\":\"print('DRONE_SUBSTANTIVE_NEXT_TOOL')\"}"}}]})
            } else {
                json!({"content":"verified"})
            };
            let body = format!(
                "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                json!({"id":"fixture","choices":[{"index":0,"delta":delta,"finish_reason":null}]}),
                json!({"id":"fixture","choices":[{"index":0,"delta":{},"finish_reason":if step==0{"tool_calls"}else{"stop"}}]})
            );
            let head=format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());
            socket
                .write_all(format!("{head}{body}").as_bytes())
                .await
                .unwrap();
        }
    });
    std::fs::write(agent_dir.join("models.json"),json!({"providers":{"fixture":{"api":"openai-completions","baseUrl":format!("http://{address}"),"apiKey":"offline-fixture","models":[{"id":"held","name":"held","contextWindow":128_000,"maxTokens":1024}]}}}).to_string()).unwrap();
    std::fs::write(agent_dir.join("settings.json"),json!({"defaultProvider":"fixture","defaultModel":"held","retry":{"provider":{"timeoutMs":1}},"compaction":{"enabled":false},"idleEviction":"off"}).to_string()).unwrap();
    let config = |active: &str, script: Option<Value>| WorkerConfig {
        socket_path: dir.path().join(format!("{active}.sock")),
        supervisor_socket_path: PathBuf::new(),
        token: "fixture".into(),
        worker_instance_id: format!("{active}-incarnation"),
        active_session_id: active.into(),
        agent_dir: agent_dir.clone(),
        recovery_journal_path: dir.path().join(format!("{active}.recovery.jsonl")),
        telemetry_disabled: Some(true),
        script,
    };
    let mut fixture_stop = FixtureStop(Vec::new());
    let worker = Arc::new(Worker::new(config("root", None), None));
    fixture_stop.0.push(worker.clone());
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&worker.config.recovery_journal_path).unwrap());
    let created = worker
        .dispatch(
            "create",
            &json!({"cwd":dir.path(),"provider":"fixture","model":"held","thinking":"off"}),
        )
        .await;
    assert!(created.success, "{created:?}");
    let root_sid = worker
        .core
        .lock()
        .unwrap()
        .store
        .as_ref()
        .unwrap()
        .session_id()
        .to_string();
    let child = Arc::new(Worker::new(
        config(
            "child",
            Some(json!({"responses":[{"text":"child complete","delayMs":60000}]})),
        ),
        None,
    ));
    fixture_stop.0.push(child.clone());
    *child.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&child.config.recovery_journal_path).unwrap());
    assert!(child.dispatch("create",&json!({"cwd":dir.path(),"runtimeMetadata":{"kind":"subagent","rlmChildId":"child-id","parentActiveSessionId":"root","parentSessionId":root_sid}})).await.success);
    let child_generation = child.core.lock().unwrap().generation.clone();
    assert!(
        child
            .dispatch("prompt", &json!({"message":"child work"}))
            .await
            .success
    );
    let mut process = tokio::process::Command::new("/usr/bin/python3")
        .args([
            "-c",
            "import sys; print('READY',flush=True); sys.stdin.read() ",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = process.id().unwrap();
    let start_id = pa_types::platform::process::process_start_id(pid).unwrap();
    let mut output = process.stdout.take().unwrap();
    let mut ready = [0; 6];
    output.read_exact(&mut ready).await.unwrap();
    assert_eq!(&ready, b"READY\n");
    let mut events = worker.events.subscribe();
    assert!(
        worker
            .dispatch("prompt", &json!({"message":"original user request"}))
            .await
            .success
    );
    tokio::time::timeout(Duration::from_secs(15), held_rx)
        .await
        .unwrap()
        .unwrap();
    let parent_file = worker
        .core
        .lock()
        .unwrap()
        .store
        .as_ref()
        .unwrap()
        .path
        .clone();
    let child_file = child
        .core
        .lock()
        .unwrap()
        .store
        .as_ref()
        .unwrap()
        .path
        .clone();
    let ledger =
        crate::rlm_ledger::RlmSpawnLedger::new(&agent_dir, parent_file.parent().unwrap(), |_| {});
    ledger
        .append_spawn(&crate::rlm_ledger::RlmSpawnInput {
            factory: None,

            child_id: "child-id".into(),
            parent: parent_file.display().to_string(),
            child: child_file.display().to_string(),
            depth: 1,
            name: "child fixture".into(),
        })
        .unwrap();
    let terminal_child = Arc::new(Worker::new(
        config(
            "terminal-child",
            Some(json!({"responses":["actual terminal fixture result"]})),
        ),
        None,
    ));
    fixture_stop.0.push(terminal_child.clone());
    *terminal_child.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&terminal_child.config.recovery_journal_path).unwrap());
    assert!(terminal_child.dispatch("create",&json!({"cwd":dir.path(),"runtimeMetadata":{"kind":"subagent","rlmChildId":"completed-child","parentActiveSessionId":"root","parentSessionId":root_sid}})).await.success);
    assert!(
        terminal_child
            .dispatch(
                "prompt_and_wait",
                &json!({"message":"return terminal result"})
            )
            .await
            .success
    );
    let terminal_file = terminal_child
        .core
        .lock()
        .unwrap()
        .store
        .as_ref()
        .unwrap()
        .path
        .clone();
    ledger
        .append_spawn(&crate::rlm_ledger::RlmSpawnInput {
            factory: None,

            child_id: "completed-child".into(),
            parent: parent_file.display().to_string(),
            child: terminal_file.display().to_string(),
            depth: 1,
            name: "completed fixture".into(),
        })
        .unwrap();
    let edge_before = ledger.live_edges().unwrap();
    assert_eq!(edge_before.len(), 2);
    let terminal = json!({"role":"custom","customType":pa_core::session_engine::rlm_notices::RLM_CHILD_TERMINAL_NOTICE_CUSTOM_TYPE,"content":"preserved terminal child result","display":true,"timestamp":crate::util::now_ms(),"details":{"nativeManagedResult":true,"terminalDeliveryId":"fixture-terminal-delivery","childId":"completed-child","status":"completed"}});
    let nonce = crate::child_status_notices::mint();
    assert!(worker.dispatch("follow_up",&json!({"message":"preserved terminal child result","source":"fixture","customMessage":terminal,"rlmNoticeNonce":nonce})).await.success);
    let terminal_before = worker
        .core
        .lock()
        .unwrap()
        .follow_up
        .front()
        .unwrap()
        .custom_message
        .clone();
    let queue = worker.dispatch("get_queue", &json!({})).await.data;
    let snapshot = worker
        .dispatch("drone_snapshot", &json!({}))
        .await
        .data
        .unwrap();
    let mut identity = snapshot.clone();
    let request = identity.as_object_mut().unwrap().remove("request").unwrap();
    for key in ["requestId", "revision"] {
        identity[key] = request[key].clone();
    }
    identity.as_object_mut().unwrap().retain(|k, _| {
        matches!(
            k.as_str(),
            "activeSessionId"
                | "sessionId"
                | "sessionFile"
                | "cwd"
                | "workerIncarnation"
                | "generation"
                | "turnSerial"
                | "requestId"
                | "revision"
        )
    });

    let assessor_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let assessor_address = assessor_listener.local_addr().unwrap();
    let mut models: Value =
        serde_json::from_str(&std::fs::read_to_string(agent_dir.join("models.json")).unwrap())
            .unwrap();
    models["providers"]["h20-keys"] = json!({"api":"openai-completions","baseUrl":format!("http://{assessor_address}"),"apiKey":"offline-fixture","models":[{"id":"h20-mmfast","name":"Drone fixture","reasoning":false,"contextWindow":64000,"maxTokens":1024}]});
    std::fs::write(agent_dir.join("models.json"), models.to_string()).unwrap();
    let chosen_identity = identity.clone();
    let (assessed_tx, mut assessed_rx) = tokio::sync::mpsc::unbounded_channel();
    let assessor = tokio::spawn(async move {
        for step in 0..2 {
            let (mut socket, _) = assessor_listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            let header_end = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            let header = String::from_utf8_lossy(&bytes[..header_end]);
            let length = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|s| s.trim().parse::<usize>().ok())
                })
                .unwrap();
            while bytes.len() < header_end + length {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let request: Value =
                serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
            assert!(request.to_string().contains("Drone"));
            let action = if step == 0 {
                json!({"handler":"run_registered_check","args":{"name":"branch_facts","arguments":{}}})
            } else {
                json!({"handler":"recover_root_request","args":{"operationId":"combined","identity":chosen_identity}})
            };
            let content = action.to_string();
            let body = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"id":"assessment","choices":[{"index":0,"delta":{"content":content},"finish_reason":"stop"}]})
            );
            let response=format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
            assessed_tx.send(step).unwrap();
        }
    });
    let supervisor = Arc::new(
        crate::supervisor::Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.path().join("supervisor.sock"),
            agent_dir: agent_dir.clone(),
        })
        .unwrap(),
    );
    supervisor.drone.lock().unwrap().cooldown = Duration::ZERO;
    supervisor.drone_controls.resume_for_test(&root_sid);
    let roadmap_path = dir.path().join("authoritative-roadmap.json");
    std::fs::write(&roadmap_path,json!({"revision":"fixture-r1","goal":"verify actual user result","acceptance":["same SID next tool and terminal result"],"capacity":2,"userStopped":false,"budgetAvailable":true,"steps":[{"id":"independent-a","status":"ready","dependencies":[],"owner":null,"files":["a.rs"],"acceptance":["test a"]},{"id":"independent-b","status":"ready","dependencies":[],"owner":null,"files":["b.rs"],"acceptance":["test b"]}]}).to_string()).unwrap();
    let descriptor:pa_types::daemon::DaemonWorkerDescriptor=serde_json::from_value(json!({"version":2,"workerId":"root","pid":std::process::id(),"socketPath":"fixture","recoveryJournalPath":worker.config.recovery_journal_path,"supervisorSocketPath":"fixture","authenticationToken":"fixture","rootActiveSessionId":"root","rootSessionId":root_sid,"sessionFile":worker.core.lock().unwrap().store.as_ref().unwrap().path,"createdAt":"fixture","updatedAt":"fixture","lifecycle":"ready","createCommand":{},"consecutiveFailures":0})).unwrap();
    let resident = crate::registry::ResidentWorker::new(
        "root".into(),
        descriptor,
        dir.path().join("descriptor.json"),
    );
    let epoch = resident.note_connection_live();
    let (commands, mut commands_rx) =
        tokio::sync::mpsc::channel::<crate::registry::WorkerRequest>(16);
    resident.install_command_channel(epoch, commands).await;
    supervisor.registry.insert(resident.clone()).await;
    supervisor.roster.lock().unwrap().write_summary(json!({"sessionId":root_sid,"activeSessionId":"root","runtimeKind":"top-level","cwd":dir.path()}),Some("root"),None);
    let route_worker = worker.clone();
    let route_resident = resident.clone();
    let route = tokio::spawn(async move {
        while let Some(command) = commands_rx.recv().await {
            let response = route_worker
                .dispatch(&command.command_type, &command.payload)
                .await;
            if let Some(reply) = route_resident
                .pending
                .lock()
                .await
                .remove(&command.request_id)
            {
                let _ = reply.send(crate::registry::WorkerReply::Typed(response));
            }
        }
    });
    let queue_before_drone = worker.dispatch("get_queue", &json!({})).await.data;
    supervisor
        .observe_drone_event(
            &resident,
            epoch,
            "root",
            json!({"type":"drone_request_progress","request":request}),
        )
        .await;
    assert_eq!(
        supervisor.drone.lock().unwrap().status(&root_sid)["state"],
        "not_observed"
    );
    assert!(assessed_rx.try_recv().is_err());
    assert_eq!(
        worker.dispatch("get_queue", &json!({})).await.data,
        queue_before_drone
    );

    resident
        .descriptor
        .lock()
        .await
        .create_command
        .rest
        .insert("droneRoadmapPath".into(), json!(roadmap_path));
    {
        let mut drone = supervisor.drone.lock().unwrap();
        // A pause must not turn existing bounded provider failures into an owner wake.
        for _ in 0..2 {
            let attempted = drone
                .push(&root_sid, json!({"type":"fixture_failure"}))
                .unwrap();
            drone.failure(&attempted, "model_failed");
            drone.finish(&attempted, false);
        }
        assert_eq!(drone.status(&root_sid)["consecutiveFailures"], 2);
    }
    let pause = worker
        .dispatch(
            "acquire_session_input_pause",
            &json!({
                "activeSessionId":"root", "leaseKey":"drone-paused-fixture", "clientId":"test"
            }),
        )
        .await;
    assert!(pause.success, "pause failed: {pause:?}");
    let denied_notice = worker
        .dispatch(
            "follow_up",
            &json!({
                "activeSessionId":"root", "message":"[Drone] paused notice", "source":"drone"
            }),
        )
        .await;
    assert!(!denied_notice.success);
    assert_eq!(
        worker.dispatch("get_queue", &json!({})).await.data,
        queue_before_drone
    );
    supervisor
        .observe_drone_event(
            &resident,
            epoch,
            "root",
            json!({"type":"drone_request_progress","request":request}),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = supervisor.drone.lock().unwrap().status(&root_sid);
            if status["state"] == "active" && status["inFlight"] == false {
                assert_eq!(status["assessmentCountInBurst"], 2);
                assert_eq!(status["consecutiveFailures"], 2);
                assert_eq!(status["wakeOwned"], false);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(assessed_rx.try_recv().is_err());
    assert_eq!(
        worker.dispatch("get_queue", &json!({})).await.data,
        queue_before_drone
    );
    let released = worker.dispatch("release_session_input_pause", &json!({
        "activeSessionId":"root", "pauseId":pause.data.as_ref().unwrap()["pauseId"], "clientId":"test"
    })).await;
    assert!(released.success, "release failed: {released:?}");
    supervisor
        .observe_drone_event(
            &resident,
            epoch,
            "root",
            json!({"type":"drone_request_progress","request":request}),
        )
        .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), assessed_rx.recv())
            .await
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), assessed_rx.recv())
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        worker
            .core
            .lock()
            .unwrap()
            .store
            .as_ref()
            .unwrap()
            .session_id(),
        root_sid
    );
    assert_eq!(child.core.lock().unwrap().generation, child_generation);
    assert!(child.core.lock().unwrap().busy);
    assert_eq!(
        pa_types::platform::process::process_start_id(pid).as_deref(),
        Some(start_id.as_str())
    );
    assert!(process.try_wait().unwrap().is_none());
    let mut observed = Vec::new();
    let found = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let frame = events.recv().await.unwrap();
            let event: Value = serde_json::from_slice(&frame.payload).unwrap();
            observed.push(event.clone());
            if event["event"]["type"] == "tool_execution_end"
                && event["event"]["toolCallId"] == "next-tool"
            {
                break event;
            }
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "{error}; state={:?}; status={:?}; events={observed:?}",
            worker
                .engine
                .drone_request_control()
                .and_then(|c| c.snapshot()),
            worker
                .engine
                .drone_request_control()
                .and_then(|c| c.operation_status("combined"))
        )
    });
    assert!(
        found.to_string().contains("DRONE_SUBSTANTIVE_NEXT_TOOL"),
        "{found}"
    );
    assert_eq!(
        worker
            .core
            .lock()
            .unwrap()
            .store
            .as_ref()
            .unwrap()
            .session_id(),
        root_sid
    );
    assert!(queue.is_some());
    let idle = worker.dispatch("wait_for_idle", &json!({})).await;
    assert!(idle.success);
    assert_eq!(ledger.live_edges().unwrap(), edge_before);
    assert_eq!(
        terminal_before.as_ref().unwrap()["details"]["terminalDeliveryId"],
        "fixture-terminal-delivery"
    );
    let consumed = worker
        .core
        .lock()
        .unwrap()
        .store
        .as_ref()
        .unwrap()
        .entries
        .iter()
        .filter(|entry| {
            entry
                .fields
                .to_string()
                .contains("fixture-terminal-delivery")
                && entry.type_ == "custom_message"
        })
        .count();
    assert_eq!(
        consumed,
        1,
        "terminal notice must be consumed exactly once; rows={:?}",
        worker.core.lock().unwrap().store.as_ref().unwrap().entries
    );

    // Stop only these fixture consumers; never a live user worker.
    drop(process.stdin.take());
    process.wait().await.unwrap();
    child.engine.abort_in_flight_turn();
    worker.engine.abort_in_flight_turn();
    assessor.await.unwrap();
    route.abort();
    server.abort();
}

#[tokio::test]
async fn drone_unknown_fences_queue_before_pickup_without_reorder_or_receipt_loss() {
    let worker = created_dispatch_worker().await;
    let mut journal = WorkerRecoveryJournal::open(&worker.config.recovery_journal_path).unwrap();
    journal.record_drone_operation(json!({"type":"drone_operation","activeSessionId":worker.core.lock().unwrap().active_session_id,"request":{"operationId":"lost"},"status":"recovery_unconfirmed"})).unwrap();
    *worker.recovery.lock().unwrap() = Some(journal);
    {
        let mut core = worker.core.lock().unwrap();
        core.busy = false;
        for (message, id, visible) in [
            ("reserved child terminal body", "terminal-delivery", true),
            ("ordinary queued body", "ordinary", true),
            ("internal admission body", "internal", false),
        ] {
            core.follow_up.push_back(QueuedItem {
                message: message.into(),
                priority: QueuePriority::Background,
                preview: None,
                custom_message: None,
                agent_message: None,
                input_identity: None,
                queue_key: Some(id.into()),
                admission_id: Some(id.into()),
                images: vec![],
                done: None,
                queue_visible: visible,
                policy: TurnPolicy::Queued,
                forced_batch: false,
            });
        }
    }
    let facts = || {
        let core = worker.core.lock().unwrap();
        (
            core.turn_serial,
            core.follow_up
                .iter()
                .map(|q| {
                    (
                        q.message.clone(),
                        q.queue_key.clone(),
                        q.admission_id.clone(),
                        q.queue_visible,
                    )
                })
                .collect::<Vec<_>>(),
        )
    };
    let before = facts();
    let parked = worker.idle_notify.notified();
    tokio::pin!(parked);
    parked.as_mut().enable();
    worker.work_notify.notify_one();
    tokio::time::timeout(Duration::from_secs(2), parked)
        .await
        .unwrap();
    assert_eq!(facts(), before);
    assert!(worker.core.lock().unwrap().running_admission_ids.is_empty());
}

#[tokio::test]
async fn drone_status_selects_exact_operation_without_aliasing_current() {
    let worker = created_dispatch_worker().await;
    let active = worker.core.lock().unwrap().active_session_id.clone();
    let mut journal = WorkerRecoveryJournal::open(&worker.config.recovery_journal_path).unwrap();
    let old = json!({"request":{"operationId":"old"},"status":"continued"});
    journal.record_drone_operation(json!({"type":"drone_operation","activeSessionId":active,"request":{"operationId":"current"},"status":"recovery_unconfirmed","history":[old]})).unwrap();
    *worker.recovery.lock().unwrap() = Some(journal);
    assert_eq!(
        worker
            .dispatch("drone_status", &json!({"operationId":"old"}))
            .await
            .data
            .unwrap()["status"],
        "continued"
    );
    assert_eq!(
        worker
            .dispatch("drone_status", &json!({"operationId":"missing"}))
            .await
            .data
            .unwrap()["status"],
        "unknown"
    );
    assert_eq!(
        worker
            .dispatch("drone_status", &json!({"operationId":"current"}))
            .await
            .data
            .unwrap()["status"],
        "recovery_unconfirmed"
    );
}
