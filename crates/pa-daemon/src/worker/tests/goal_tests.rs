//! Goal continuation and compact-mint e2e tests.
use super::*;

/// A scripted goal session's dispatch worker.
async fn goal_dispatch_worker(goal: serde_json::Value) -> std::sync::Arc<Worker> {
    let dir = std::env::temp_dir().join(format!("pa-worker-goal-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = WorkerConfig {
        socket_path: dir.join("worker.sock"),
        supervisor_socket_path: PathBuf::new(),
        token: "token".to_string(),
        worker_instance_id: String::new(),
        active_session_id: "goal-session".to_string(),
        agent_dir: dir.join("agent"),
        recovery_journal_path: dir.join("recovery.jsonl"),
        telemetry_disabled: None,
        script: Some(json!({
            "responses": ["ack"],
            "goal": goal,
        })),
    };
    let worker = std::sync::Arc::new(Worker::new(config, None));
    let created = worker
        .dispatch(
            "create",
            &json!({ "noSession": true, "cwd": "/tmp", "name": "goal" }),
        )
        .await;
    assert!(created.success, "create failed: {created:?}");
    worker
}

/// TS `compact()`'s active-goal branch: a successful compact mints the owed continuation.
#[tokio::test]
async fn compact_with_active_goal_schedules_the_continue() {
    let worker = goal_dispatch_worker(json!({
        "status": "active",
        "objective": "land the post-compact continue",
        "message": "[goal: continuation]\n\nkeep pursuing the goal",
    }))
    .await;
    let mut subscription = worker.events.subscribe();
    let compact = worker
        .dispatch("compact", &json!({ "activeSessionId": "goal-session" }))
        .await;
    assert!(compact.success, "scripted compact failed: {compact:?}");
    // The scheduled continue drives the continuation turn; the idle
    // wait settles only after it ran.
    let idle = worker.dispatch("wait_for_idle", &json!({})).await;
    assert!(idle.success, "never went idle: {idle:?}");
    let events = session_events_since(&mut subscription);
    // The mint's `goal_update` surfaces first, then the continuation
    // turn: the goal-context custom row plus its model turn.
    let goal_updates: Vec<&Value> = events
        .iter()
        .filter(|event| event.get("type").and_then(Value::as_str) == Some("goal_update"))
        .collect();
    assert_eq!(goal_updates.len(), 1, "events: {events:?}");
    assert_eq!(goal_updates[0]["goal"]["status"], "active");
    let custom_rows: Vec<&Value> = events
        .iter()
        .filter(|event| {
            event.get("type").and_then(Value::as_str) == Some("message_start")
                && event["message"]["customType"] == "goal_context"
        })
        .collect();
    assert_eq!(custom_rows.len(), 1, "events: {events:?}");
    assert_eq!(
        custom_rows[0]["message"]["content"],
        "[goal: continuation]\n\nkeep pursuing the goal"
    );
    assert!(
        events
            .iter()
            .any(|event| event.get("type").and_then(Value::as_str) == Some("turn_end")),
        "the continuation turn never ran: {events:?}"
    );
    // The resume site crossed the #234 suspension gate: a plain prompt
    // is admitted again.
    let plain = worker
        .dispatch(
            "prompt_and_wait",
            &json!({ "activeSessionId": "goal-session", "message": "after the continue" }),
        )
        .await;
    assert!(plain.success, "still suspended: {plain:?}");
}

#[tokio::test]
async fn compact_with_active_goal_and_parked_work_skips_the_mint() {
    let worker = goal_dispatch_worker(json!({
        "status": "active",
        "objective": "land the post-compact continue",
    }))
    .await;
    let mut subscription = worker.events.subscribe();
    {
        // Park one queued follow-up behind the suspension gate, like a
        // steer that arrived mid-compact-window.
        let mut core = worker.core.lock().unwrap();
        core.queued_input_suspended = true;
        core.follow_up.push_back(QueuedItem {
            priority: QueuePriority::Human,
            preview: None,
            message: "parked queued work".to_string(),
            custom_message: None,
            agent_message: None,
            input_identity: None,
            queue_key: None,
            admission_id: None,
            images: Vec::new(),
            done: None,
            queue_visible: true,
            policy: TurnPolicy::Queued,
            forced_batch: false,
        });
    }
    let compact = worker
        .dispatch("compact", &json!({ "activeSessionId": "goal-session" }))
        .await;
    assert!(compact.success, "scripted compact failed: {compact:?}");
    let idle = worker.dispatch("wait_for_idle", &json!({})).await;
    assert!(idle.success, "never went idle: {idle:?}");
    let events = session_events_since(&mut subscription);
    assert!(
        events.iter().any(|event| {
            event.get("type").and_then(Value::as_str) == Some("message_start")
                && event["message"]["role"] == "user"
                && event["message"]["content"] == "parked queued work"
        }),
        "the parked item never ran: {events:?}"
    );
    assert!(
        !events.iter().any(|event| {
            event.get("type").and_then(Value::as_str) == Some("message_start")
                && event["message"]["customType"] == "goal_context"
        }),
        "a continuation was minted over the parked work: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.get("type").and_then(Value::as_str) == Some("goal_update")),
        "a mint emitted a goal_update: {events:?}"
    );
}

#[tokio::test]
async fn compact_with_paused_goal_never_continues() {
    let worker = goal_dispatch_worker(json!({
        "status": "paused",
        "objective": "land the post-compact continue",
    }))
    .await;
    let mut subscription = worker.events.subscribe();
    let compact = worker
        .dispatch("compact", &json!({ "activeSessionId": "goal-session" }))
        .await;
    assert!(compact.success, "scripted compact failed: {compact:?}");
    let rejected = worker
        .dispatch(
            "prompt_and_wait",
            &json!({ "activeSessionId": "goal-session", "message": "hi" }),
        )
        .await;
    assert_eq!(rejected.error.as_deref(), Some(QUEUED_INPUT_SUSPENDED));
    let events = session_events_since(&mut subscription);
    assert!(
        !events
            .iter()
            .any(|event| event.get("type").and_then(Value::as_str) == Some("goal_update")),
        "a paused goal minted a continuation: {events:?}"
    );
}

/// The goal continuation loop at the natural turn end (TS
/// `_getGoalContinuationMessages` + `_getContinuationMessages`): a
/// multi-continuation goal session driven to completion end to end
/// through the worker. The turn runner's queue drives each minted
/// continuation (the engine consults at every settled boundary, the
/// admission sink queues the follow-up, the runner wakes), the
/// budget-free loop keeps prompting until the kernel's
/// `goal.complete()` (the scripted ipython tool call, the f18
/// completion surface) settles the goal, and the completion's
/// boundary mints nothing more. The completing cell needs a
/// bootable kernel: a gate run needs the kernel venv, bootstrapped
/// or built with uv on PATH (the guard inside names the
/// recipe when the cell fails instead of letting the loop drain
/// the faux script into a misleading count mismatch).
#[allow(clippy::await_holding_lock)] // the faux registry is process-global: the guard must span the async flow
#[tokio::test]
async fn goal_turn_end_loop_runs_to_completion() {
    let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = std::env::temp_dir().join(format!("pa-worker-goal-loop-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = WorkerConfig {
        socket_path: dir.join("worker.sock"),
        supervisor_socket_path: PathBuf::new(),
        token: "token".to_string(),
        worker_instance_id: String::new(),
        active_session_id: "goal-loop-session".to_string(),
        agent_dir: dir.join("agent"),
        recovery_journal_path: dir.join("recovery.jsonl"),
        telemetry_disabled: None,
        script: Some(json!({
            "engine": "faux",
            "responses": [
                "first pursuit turn",
                "second pursuit turn",
                { "content": [
                    { "type": "toolCall", "name": "python_repl",
                      "arguments": { "code": "import goal; await goal.complete()" } },
                ] },
                "wrap-up after the completion",
                "after the loop settled",
            ],
        })),
    };
    let worker = std::sync::Arc::new(Worker::new(config, None));
    let created = worker
        .dispatch(
            "create",
            &json!({ "noSession": true, "cwd": "/tmp", "name": "goal-loop" }),
        )
        .await;
    assert!(created.success, "create failed: {created:?}");
    let mut subscription = worker.events.subscribe();
    let start = worker
        .dispatch(
            "prompt_and_wait",
            &json!({
                "activeSessionId": "goal-loop-session",
                "message": "/goal drive the loop to completion",
            }),
        )
        .await;
    assert!(start.success, "the goal start failed: {start:?}");
    // The loop owns the session until the goal settles: the idle wait
    // returns only when the completion turn's boundary minted nothing.
    let idle = worker.dispatch("wait_for_idle", &json!({})).await;
    assert!(idle.success, "the goal loop never settled: {idle:?}");
    let events = session_events_since(&mut subscription);
    // A gate run without the kernel environment (the kernel venv, or
    // uv on PATH to build it) fails the
    // completing ipython cell: the goal stays active and the loop
    // keeps minting (TS parity: goal continuations are unbounded while
    // the goal is active) until the faux script runs dry. Fail with
    // the diagnosis instead of the misleading continuation-count
    // mismatch.
    let kernel_failure = events.iter().find(|event| {
        event.get("type").and_then(Value::as_str) == Some("message_end")
            && event["message"]["role"] == "toolResult"
            && event["message"]["isError"] == json!(true)
    });
    if let Some(failure) = kernel_failure {
        panic!(
            "the completing ipython cell failed — this test needs the kernel \
                 environment (the bootstrapped kernel venv, or uv on PATH to build it): {failure:?}"
        );
    }
    let goal_rows: Vec<&Value> = events
        .iter()
        .filter(|event| {
            event.get("type").and_then(Value::as_str) == Some("message_start")
                && event["message"]["customType"] == "goal_context"
        })
        .collect();
    assert_eq!(goal_rows.len(), 3, "events: {events:?}");
    assert_eq!(goal_rows[0]["message"]["details"]["kind"], "continuation");
    assert_eq!(goal_rows[1]["message"]["details"]["continuationsUsed"], 1);
    assert_eq!(goal_rows[2]["message"]["details"]["continuationsUsed"], 2);
    let assistant_rows = events
        .iter()
        .filter(|event| {
            event.get("type").and_then(Value::as_str) == Some("message_end")
                && event["message"]["role"] == "assistant"
        })
        .count();
    assert!(assistant_rows >= 4, "events: {events:?}");
    // The goal state settled complete through the worker's host
    // handlers.
    let complete_update = events
        .iter()
        .rev()
        .find(|event| {
            event.get("type").and_then(Value::as_str) == Some("goal_update")
                && event["goal"]["status"] == "complete"
        })
        .expect("the completion surfaced as a goal_update");
    assert_eq!(
        complete_update["goal"]["objective"],
        "drive the loop to completion"
    );
    assert_eq!(complete_update["goal"]["continuationsUsed"], 2);
    assert!(
        complete_update["goal"]["tokensUsed"].as_u64().unwrap_or(0) > 0,
        "usage accounting ran: {complete_update:?}"
    );
    let queue = worker
        .dispatch(
            "get_queue",
            &json!({ "activeSessionId": "goal-loop-session" }),
        )
        .await;
    assert!(queue.success, "queue read failed: {queue:?}");
    let plain = worker
        .dispatch(
            "prompt_and_wait",
            &json!({
                "activeSessionId": "goal-loop-session",
                "message": "after the loop",
            }),
        )
        .await;
    assert!(plain.success, "a post-goal prompt failed: {plain:?}");
}

/// A kernel background Bash handle may outlive the turn that started it.
/// The headless owner must keep the worker alive until its settle hook admits
/// the owed goal continuation and the completing turn has run.
#[allow(clippy::await_holding_lock)] // the faux registry is process-global: the guard must span the async flow
#[tokio::test]
async fn headless_completion_waits_for_background_bash_goal_continuation() {
    let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = std::env::temp_dir().join(format!("pa-headless-goal-bash-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let worker = std::sync::Arc::new(Worker::new(
        WorkerConfig {
            socket_path: dir.join("worker.sock"),
            supervisor_socket_path: PathBuf::new(),
            token: "token".to_string(),
            worker_instance_id: String::new(),
            active_session_id: "headless-goal-bash".to_string(),
            agent_dir: dir.join("agent"),
            recovery_journal_path: dir.join("recovery.jsonl"),
            telemetry_disabled: None,
            script: Some(json!({
                "engine": "faux",
                "responses": [
                    "warm turn",
                    "goal start turn",
                    { "content": [
                        { "type": "toolCall", "name": "python_repl",
                          "arguments": { "code": "import goal; await goal.complete()" } },
                    ] },
                ],
            })),
        },
        None,
    ));
    let created = worker
        .dispatch("create", &json!({ "noSession": true, "cwd": "/tmp" }))
        .await;
    assert!(created.success, "create failed: {created:?}");
    let warm = worker
        .dispatch(
            "prompt_and_wait",
            &json!({ "activeSessionId": "headless-goal-bash", "message": "warm" }),
        )
        .await;
    assert!(warm.success, "warm turn failed: {warm:?}");
    let engine = worker.agent_engine.as_ref().expect("concrete engine");
    let live = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let probe = std::sync::Arc::clone(&live);
    *engine.background_bash_probe.lock().unwrap() = Some(std::sync::Arc::new(move || {
        probe.load(std::sync::atomic::Ordering::SeqCst)
    }));
    let mut events = worker.events.subscribe();
    let start = worker
        .dispatch(
            "prompt_and_wait",
            &json!({
                "activeSessionId": "headless-goal-bash",
                "message": "/goal finish after Bash settles",
            }),
        )
        .await;
    assert!(start.success, "goal start failed: {start:?}");
    let idle = worker.dispatch("wait_for_idle", &json!({})).await;
    assert!(idle.success, "worker did not park: {idle:?}");
    assert_eq!(
        engine.headless_goal_wait_state().await,
        crate::goal_continuation::HeadlessGoalWait::Pending
    );

    let completion_payload = json!({});
    let completion = worker.dispatch("wait_for_headless_completion", &completion_payload);
    tokio::pin!(completion);
    std::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(completion.as_mut(), cx).is_pending(),
            "headless wait returned before Bash settled"
        );
        std::task::Poll::Ready(())
    })
    .await;

    live.store(false, std::sync::atomic::Ordering::SeqCst);
    engine.retry_owed_goal_continuation();
    let completed = tokio::time::timeout(std::time::Duration::from_secs(30), completion)
        .await
        .expect("headless wait did not finish after Bash settled");
    assert!(
        completed.success,
        "headless completion failed: {completed:?}"
    );
    assert_eq!(worker.engine.goal_state_value()["status"], "complete");
    let observed = session_events_since(&mut events);
    assert!(
        observed.iter().any(|event| {
            event["type"] == "message_start" && event["message"]["customType"] == "goal_context"
        }),
        "owed continuation did not run: {observed:?}"
    );
}

#[allow(clippy::await_holding_lock)] // the faux registry is process-global: the guard must span the async flow
#[tokio::test]
async fn idle_abort_releases_headless_goal_wait() {
    let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = std::env::temp_dir().join(format!("pa-headless-goal-abort-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let worker = std::sync::Arc::new(Worker::new(
        WorkerConfig {
            socket_path: dir.join("worker.sock"),
            supervisor_socket_path: PathBuf::new(),
            token: "token".to_string(),
            worker_instance_id: String::new(),
            active_session_id: "headless-goal-abort".to_string(),
            agent_dir: dir.join("agent"),
            recovery_journal_path: dir.join("recovery.jsonl"),
            telemetry_disabled: None,
            script: Some(json!({
                "engine": "faux",
                "responses": ["warm turn", "goal start turn"],
            })),
        },
        None,
    ));
    assert!(
        worker
            .dispatch("create", &json!({ "noSession": true, "cwd": "/tmp" }))
            .await
            .success
    );
    assert!(
        worker
            .dispatch(
                "prompt_and_wait",
                &json!({ "activeSessionId": "headless-goal-abort", "message": "warm" }),
            )
            .await
            .success
    );
    let engine = worker.agent_engine.as_ref().expect("concrete engine");
    let live = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let probe = std::sync::Arc::clone(&live);
    *engine.background_bash_probe.lock().unwrap() = Some(std::sync::Arc::new(move || {
        probe.load(std::sync::atomic::Ordering::SeqCst)
    }));
    assert!(
        worker
            .dispatch(
                "prompt_and_wait",
                &json!({
                    "activeSessionId": "headless-goal-abort",
                    "message": "/goal stop when aborted",
                }),
            )
            .await
            .success
    );
    assert!(worker.dispatch("wait_for_idle", &json!({})).await.success);
    let mut events = worker.events.subscribe();

    let completion_payload = json!({});
    let completion = worker.dispatch("wait_for_headless_completion", &completion_payload);
    tokio::pin!(completion);
    std::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(completion.as_mut(), cx).is_pending(),
            "headless wait returned before abort"
        );
        std::task::Poll::Ready(())
    })
    .await;
    assert!(worker.dispatch("abort", &json!({})).await.success);
    let completed = tokio::time::timeout(std::time::Duration::from_secs(5), completion)
        .await
        .expect("idle abort did not release headless wait");
    assert!(completed.success, "completion failed: {completed:?}");
    assert_eq!(worker.engine.goal_state_value()["status"], "active");
    live.store(false, std::sync::atomic::Ordering::SeqCst);
    let retried = engine.goal_retry_notify.notified();
    tokio::pin!(retried);
    retried.as_mut().enable();
    engine.retry_owed_goal_continuation();
    tokio::time::timeout(std::time::Duration::from_secs(5), retried)
        .await
        .expect("settle retry did not finish");
    let observed = session_events_since(&mut events);
    assert!(
        !observed.iter().any(|event| {
            event["type"] == "message_start" && event["message"]["customType"] == "goal_context"
        }),
        "abort admitted a continuation: {observed:?}"
    );
}

#[allow(clippy::await_holding_lock)] // the faux registry is process-global: the guard must span the async flow
#[tokio::test]
async fn goal_pause_withdraws_the_queued_continuation() {
    let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = std::env::temp_dir().join(format!("pa-worker-goal-pause-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = WorkerConfig {
        socket_path: dir.join("worker.sock"),
        supervisor_socket_path: PathBuf::new(),
        token: "token".to_string(),
        worker_instance_id: String::new(),
        active_session_id: "goal-pause-session".to_string(),
        agent_dir: dir.join("agent"),
        recovery_journal_path: dir.join("recovery.jsonl"),
        telemetry_disabled: None,
        script: Some(json!({
            "engine": "faux",
            "responses": [
                "start turn",
                "one continuation turn at most",
            ],
        })),
    };
    let worker = std::sync::Arc::new(Worker::new(config, None));
    let created = worker
        .dispatch(
            "create",
            &json!({ "noSession": true, "cwd": "/tmp", "name": "goal-pause" }),
        )
        .await;
    assert!(created.success, "create failed: {created:?}");
    let start = worker
        .dispatch(
            "prompt_and_wait",
            &json!({
                "activeSessionId": "goal-pause-session",
                "message": "/goal pause right after the start",
            }),
        )
        .await;
    assert!(start.success, "the goal start failed: {start:?}");
    let pause = worker
        .dispatch(
            "prompt_and_wait",
            &json!({ "activeSessionId": "goal-pause-session", "message": "/goal pause" }),
        )
        .await;
    assert!(pause.success, "the pause never ran: {pause:?}");
    // The loop is quiet: the idle wait settles without consuming
    // further turns (a live continuation would starve it).
    let idle = worker.dispatch("wait_for_idle", &json!({})).await;
    assert!(idle.success, "the loop never went quiet: {idle:?}");
    let goal = worker.engine.goal_state_value();
    assert_eq!(goal["status"], "paused", "goal state: {goal}");
    assert!(
        goal["continuationsUsed"].as_u64().unwrap_or(0) <= 2,
        "the pause never withdrew the loop: {goal}"
    );
}

/// A scripted goal session's dispatch worker with a durable session
/// file, so the `thread_goal_state` mirror is observable.
async fn goal_dispatch_worker_with_store(
    goal: serde_json::Value,
) -> (std::sync::Arc<Worker>, PathBuf) {
    let dir = std::env::temp_dir().join(format!("pa-worker-goal-{}", uuid::Uuid::new_v4()));
    let session_dir = dir.join("sessions");
    std::fs::create_dir_all(&session_dir).unwrap();
    let config = WorkerConfig {
        socket_path: dir.join("worker.sock"),
        supervisor_socket_path: PathBuf::new(),
        token: "token".to_string(),
        worker_instance_id: String::new(),
        active_session_id: "goal-session".to_string(),
        agent_dir: dir.join("agent"),
        recovery_journal_path: dir.join("recovery.jsonl"),
        telemetry_disabled: None,
        script: Some(json!({
            "responses": ["ack"],
            "goal": goal,
        })),
    };
    let worker = std::sync::Arc::new(Worker::new(config, None));
    let created = worker
        .dispatch(
            "create",
            &json!({
                "cwd": dir.to_string_lossy(),
                "name": "goal",
                "sessionDir": session_dir.to_string_lossy(),
            }),
        )
        .await;
    assert!(created.success, "create failed: {created:?}");
    let file = std::fs::read_dir(&session_dir)
        .expect("session dir readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .expect("session file created");
    (worker, file)
}

/// The session file's `thread_goal_state` custom rows, in order.
fn thread_goal_state_rows(path: &std::path::Path) -> Vec<Value> {
    crate::session_store::parse_session_entries(&std::fs::read_to_string(path).expect("read"))
        .into_iter()
        .filter(|entry| {
            entry.get("type").and_then(Value::as_str) == Some("custom")
                && entry.get("customType").and_then(Value::as_str)
                    == Some(pa_core::goals::GOAL_STATE_CUSTOM_TYPE)
        })
        .collect()
}

/// A saved-goal edit must reach the worker-owned JSONL before its response;
/// otherwise the next client-owned create can accept the same stale CAS.
#[allow(clippy::await_holding_lock)] // faux provider registration spans both worker lifetimes
#[tokio::test(flavor = "current_thread")]
async fn objective_edit_is_durable_across_worker_reopen_and_rejects_stale_cas() {
    let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_string_lossy().into_owned();
    let path = dir.path().join("goal.jsonl");
    let mut store = crate::session_store::SessionFile::create(&cwd, None, 0);
    store.set_path(path.clone());
    store.append_entry(
        "custom",
        json!({
            "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data": {
                "active": false,
                "status": "paused",
                "goalId": "goal-smoke-native",
                "objective": "Original synthetic objective",
                "tokenBudget": 9000,
                "tokensUsed": 234,
                "timeUsedSeconds": 12,
                "continuationsUsed": 2,
                "createdAt": 1_780_000_000_000_u64,
                "updatedAt": 1_780_000_000_000_u64,
            },
        }),
    );
    store.rewrite().unwrap();

    let worker_for = |active_session_id: &str| {
        Worker::new(
            WorkerConfig {
                socket_path: dir.path().join(format!("{active_session_id}.sock")),
                supervisor_socket_path: PathBuf::new(),
                token: "token".to_string(),
                worker_instance_id: String::new(),
                active_session_id: active_session_id.to_string(),
                agent_dir: dir.path().join("agent"),
                recovery_journal_path: dir
                    .path()
                    .join(format!("{active_session_id}.recovery.jsonl")),
                telemetry_disabled: Some(true),
                script: Some(
                    json!({ "engine": "faux", "responses": [{ "text": "should not run" }] }),
                ),
            },
            None,
        )
    };
    let first = worker_for("goal-edit-first");
    let created = first
        .dispatch(
            "create",
            &json!({ "sessionPath": path.to_string_lossy(), "cwd": cwd }),
        )
        .await;
    assert!(created.success, "first create failed: {created:?}");
    let edit = json!({
        "goalId": "goal-smoke-native",
        "expectedObjective": "Original synthetic objective",
        "objective": "Edited synthetic objective",
    });
    let edited = first.dispatch("edit_goal_objective", &edit).await;
    assert!(edited.success, "edit failed: {edited:?}");
    let rows = thread_goal_state_rows(&path);
    assert_eq!(rows.len(), 2, "the edit must persist one row: {rows:?}");
    let goal = &rows[1]["data"];
    assert_eq!(goal["objective"], "Edited synthetic objective");
    for key in [
        "goalId",
        "status",
        "tokenBudget",
        "tokensUsed",
        "timeUsedSeconds",
        "continuationsUsed",
        "createdAt",
    ] {
        assert_eq!(goal[key], rows[0]["data"][key], "preserve {key}");
    }
    let unchanged = first
        .dispatch(
            "edit_goal_objective",
            &json!({
                "goalId": "goal-smoke-native",
                "expectedObjective": "Edited synthetic objective",
                "objective": "Edited synthetic objective",
            }),
        )
        .await;
    assert!(unchanged.success, "no-op edit failed: {unchanged:?}");
    assert_eq!(
        thread_goal_state_rows(&path).len(),
        2,
        "no-op wrote a goal row"
    );
    let killed = first.dispatch("kill", &json!({})).await;
    assert!(killed.success, "first worker did not close: {killed:?}");
    drop(first);

    let second = worker_for("goal-edit-second");
    let created = second
        .dispatch(
            "create",
            &json!({ "sessionPath": path.to_string_lossy(), "cwd": cwd }),
        )
        .await;
    assert!(created.success, "reopen failed: {created:?}");
    let stale = second.dispatch("edit_goal_objective", &edit).await;
    assert!(!stale.success, "stale edit was accepted: {stale:?}");
    assert_eq!(thread_goal_state_rows(&path).len(), 2);
    assert_eq!(
        second.engine.goal_state_value()["objective"],
        "Edited synthetic objective"
    );

    // A deterministic pre-append failure leaves the engine's in-memory
    // objective changed. The same-value retry must not acknowledge that
    // unsaved no-op; this worker requires a fresh durable reopen.
    {
        let mut core = second.core.lock().unwrap();
        core.store
            .as_mut()
            .unwrap()
            .set_path(dir.path().to_path_buf());
    }
    let failed_edit = json!({
        "goalId": "goal-smoke-native",
        "expectedObjective": "Edited synthetic objective",
        "objective": "Third synthetic objective",
    });
    let failed = second.dispatch("edit_goal_objective", &failed_edit).await;
    assert!(
        !failed.success,
        "injected persist failure succeeded: {failed:?}"
    );
    {
        let mut core = second.core.lock().unwrap();
        core.store.as_mut().unwrap().set_path(path.clone());
    }
    let retry = second
        .dispatch(
            "edit_goal_objective",
            &json!({
                "goalId": "goal-smoke-native",
                "expectedObjective": "Third synthetic objective",
                "objective": "Third synthetic objective",
            }),
        )
        .await;
    assert!(
        !retry.success,
        "unsaved same-value retry succeeded: {retry:?}"
    );
    assert_eq!(thread_goal_state_rows(&path).len(), 2);
}

/// Direct controls persist before acknowledgement and withdraw pending goal work.
#[allow(clippy::await_holding_lock)] // faux provider registration spans both worker lifetimes
#[tokio::test(flavor = "current_thread")]
async fn direct_goal_control_is_durable_and_purges_queued_continuations() {
    let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_string_lossy().into_owned();
    let path = dir.path().join("goal-control.jsonl");
    let mut store = crate::session_store::SessionFile::create(&cwd, None, 0);
    store.set_path(path.clone());
    store.append_entry(
        "custom",
        json!({
            "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data": {
                "active": true,
                "status": "active",
                "goalId": "controlled-goal",
                "objective": "Original objective",
                "tokenBudget": 9000,
                "tokensUsed": 234,
                "timeUsedSeconds": 12,
                "continuationsUsed": 2,
            },
        }),
    );
    store.rewrite().unwrap();
    let worker_for = |active_session_id: &str| {
        Worker::new(
            WorkerConfig {
                socket_path: dir.path().join(format!("{active_session_id}.sock")),
                supervisor_socket_path: PathBuf::new(),
                token: "token".to_string(),
                worker_instance_id: String::new(),
                active_session_id: active_session_id.to_string(),
                agent_dir: dir.path().join("agent"),
                recovery_journal_path: dir
                    .path()
                    .join(format!("{active_session_id}.recovery.jsonl")),
                telemetry_disabled: Some(true),
                script: Some(
                    json!({ "engine": "faux", "responses": [{ "text": "should not run" }] }),
                ),
            },
            None,
        )
    };
    let first = worker_for("control-first");
    assert!(
        first
            .dispatch(
                "create",
                &json!({ "sessionPath": path.to_string_lossy(), "cwd": cwd })
            )
            .await
            .success
    );
    let agent_engine = first.agent_engine.as_ref().unwrap();
    let model = agent_engine.resolve_model().unwrap();
    agent_engine
        .ensure_core_session_async(&model)
        .await
        .unwrap();
    assert!(first.dispatch("wait_for_idle", &json!({})).await.success);
    *agent_engine.goal_backoff_wake_job_id.lock().unwrap() = Some("armed-before-cas".to_string());
    let stale = first
        .dispatch(
            "control_goal",
            &json!({
                "goalId": "controlled-goal",
                "expectedObjective": "stale",
                "action": "pause",
            }),
        )
        .await;
    assert!(!stale.success);
    assert_eq!(thread_goal_state_rows(&path).len(), 1);
    assert_eq!(
        agent_engine
            .goal_backoff_wake_job_id
            .lock()
            .unwrap()
            .as_deref(),
        Some("armed-before-cas")
    );
    {
        let mut core = first.core.lock().unwrap();
        core.busy = true;
        core.follow_up.push_back(QueuedItem {
            priority: QueuePriority::Background,
            preview: None,
            message: "obsolete continuation".to_string(),
            custom_message: Some(json!({ "customType": pa_core::goals::GOAL_CONTEXT_CUSTOM_TYPE })),
            agent_message: None,
            input_identity: None,
            queue_key: None,
            admission_id: None,
            images: Vec::new(),
            done: None,
            queue_visible: false,
            policy: TurnPolicy::Injected,
            forced_batch: false,
        });
    }
    let control = json!({
        "goalId": "controlled-goal",
        "expectedObjective": "Original objective",
        "action": "pause",
    });
    let paused = first.dispatch("control_goal", &control).await;
    assert!(paused.success, "pause failed: {paused:?}");
    let paused_goal = paused.data.as_ref().unwrap();
    assert_eq!(paused_goal["status"], "paused");
    assert_eq!(paused_goal["objective"], "Original objective");
    assert_eq!(paused_goal["tokenBudget"], 9000);
    assert_eq!(paused_goal["tokensUsed"], 234);
    assert_eq!(paused_goal["continuationsUsed"], 2);
    assert!(first.core.lock().unwrap().follow_up.is_empty());
    assert!(first
        .agent_engine
        .as_ref()
        .unwrap()
        .goal_backoff_wake_job_id
        .lock()
        .unwrap()
        .is_none());
    let rows = thread_goal_state_rows(&path);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["data"]["status"], "paused");
    first.core.lock().unwrap().busy = false;
    assert!(first.dispatch("kill", &json!({})).await.success);
    drop(first);

    let second = worker_for("control-second");
    assert!(
        second
            .dispatch(
                "create",
                &json!({ "sessionPath": path.to_string_lossy(), "cwd": cwd })
            )
            .await
            .success
    );
    let repeated_pause = second.dispatch("control_goal", &control).await;
    assert!(!repeated_pause.success);
    *second
        .agent_engine
        .as_ref()
        .unwrap()
        .goal_backoff_wake_job_id
        .lock()
        .unwrap() = Some("armed-before-clear".to_string());
    let cleared = second
        .dispatch(
            "control_goal",
            &json!({
                "goalId": "controlled-goal",
                "expectedObjective": "Original objective",
                "action": "clear",
            }),
        )
        .await;
    assert!(cleared.success, "clear failed: {cleared:?}");
    assert_eq!(cleared.data, Some(Value::Null));
    assert!(second
        .agent_engine
        .as_ref()
        .unwrap()
        .goal_backoff_wake_job_id
        .lock()
        .unwrap()
        .is_none());
    let rows = thread_goal_state_rows(&path);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2]["data"]["status"], "idle");
    assert!(rows[2]["data"].get("objective").is_none());
    assert!(
        !second
            .dispatch(
                "control_goal",
                &json!({
                    "goalId": "controlled-goal",
                    "expectedObjective": "Original objective",
                    "action": "clear",
                })
            )
            .await
            .success
    );
    assert_eq!(thread_goal_state_rows(&path).len(), 3);
}

/// Live edits remain accepted during a turn and pin the updated objective
/// context behind it; the saved-goal route's idle preflight is separate.
#[allow(clippy::await_holding_lock)] // faux provider registration spans the async flow
#[tokio::test(flavor = "current_thread")]
async fn active_goal_edit_while_busy_queues_updated_objective_context() {
    let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_string_lossy().into_owned();
    let path = dir.path().join("active-goal.jsonl");
    let mut store = crate::session_store::SessionFile::create(&cwd, None, 0);
    store.set_path(path.clone());
    store.append_entry(
        "custom",
        json!({
            "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data": {
                "active": true,
                "status": "active",
                "goalId": "live-goal",
                "objective": "Original live objective",
                "tokensUsed": 9,
                "timeUsedSeconds": 3,
                "continuationsUsed": 1,
            },
        }),
    );
    store.rewrite().unwrap();
    let worker = Worker::new(
        WorkerConfig {
            socket_path: dir.path().join("worker.sock"),
            supervisor_socket_path: PathBuf::new(),
            token: "token".to_string(),
            worker_instance_id: String::new(),
            active_session_id: "live-edit".to_string(),
            agent_dir: dir.path().join("agent"),
            recovery_journal_path: dir.path().join("recovery.jsonl"),
            telemetry_disabled: Some(true),
            script: Some(json!({ "engine": "faux", "responses": [{ "text": "should not run" }] })),
        },
        None,
    );
    let created = worker
        .dispatch(
            "create",
            &json!({ "sessionPath": path.to_string_lossy(), "cwd": cwd }),
        )
        .await;
    assert!(created.success, "create failed: {created:?}");
    // Create acknowledges before its background session build finishes. Join
    // that build and the worker's initial idle pass before simulating a live
    // turn; otherwise the delayed idle pass can clear this test's busy flag.
    let agent_engine = worker.agent_engine.as_ref().unwrap();
    let model = agent_engine.resolve_model().unwrap();
    agent_engine
        .ensure_core_session_async(&model)
        .await
        .unwrap();
    let idle = worker.dispatch("wait_for_idle", &json!({})).await;
    assert!(idle.success, "worker did not settle: {idle:?}");
    worker.core.lock().unwrap().busy = true;
    let edited = worker
        .dispatch(
            "edit_goal_objective",
            &json!({
                "goalId": "live-goal",
                "expectedObjective": "Original live objective",
                "objective": "Updated live objective",
            }),
        )
        .await;
    assert!(edited.success, "busy live edit failed: {edited:?}");
    assert_eq!(edited.data.as_ref().unwrap()["status"], "active");
    let core = worker.core.lock().unwrap();
    let queued = core.follow_up.front().unwrap_or_else(|| {
        panic!(
            "updated context queued; busy={}, status={}",
            core.busy,
            edited.data.as_ref().unwrap()["status"]
        )
    });
    assert_eq!(queued.priority, QueuePriority::Pinned);
    assert!(queued.message.contains("Updated live objective"));
    assert_eq!(
        queued.custom_message.as_ref().unwrap()["details"]["kind"],
        "objective_updated"
    );
    drop(core);
    let rows = thread_goal_state_rows(&path);
    assert_eq!(rows.len(), 2, "live edit must persist before returning");
    assert_eq!(rows[1]["data"]["objective"], "Updated live objective");
}

#[test]
fn stale_turn_goal_update_is_dropped_after_objective_or_counter_advance() {
    let captured = json!({
        "goalId": "live-goal",
        "objective": "Original live objective",
        "status": "active",
        "tokensUsed": 9,
        "timeUsedSeconds": 3,
    });
    let mut current = captured.clone();
    current["timeUsedSeconds"] = json!(4);
    assert!(!super::super::turn::goal_update_is_superseded(
        &captured, &current
    ));
    current["objective"] = json!("Updated live objective");
    assert!(super::super::turn::goal_update_is_superseded(
        &captured, &current
    ));
    current["objective"] = captured["objective"].clone();
    current["tokensUsed"] = json!(10);
    assert!(super::super::turn::goal_update_is_superseded(
        &captured, &current
    ));
}

/// The mirror row is what a recovery rebuild replays.
#[tokio::test]
async fn goal_update_events_mirror_the_durable_goal_row() {
    let (worker, file) = goal_dispatch_worker_with_store(json!({
        "emitUpdateOnPrompt": true,
        "state": {
            "active": true,
            "status": "active",
            "goalId": "goal-1",
            "objective": "ship the port",
            "tokensUsed": 340,
            "timeUsedSeconds": 9,
            "continuationsUsed": 2,
        },
    }))
    .await;
    let prompt = worker
        .dispatch(
            "prompt_and_wait",
            &json!({ "activeSessionId": "goal-session", "message": "work" }),
        )
        .await;
    assert!(prompt.success, "prompt failed: {prompt:?}");
    let rows = thread_goal_state_rows(&file);
    assert_eq!(rows.len(), 1, "rows: {rows:?}");
    assert_eq!(rows[0]["data"]["status"], "active");
    assert_eq!(rows[0]["data"]["objective"], "ship the port");
    assert_eq!(rows[0]["data"]["goalId"], "goal-1");
    assert_eq!(rows[0]["data"]["tokensUsed"], 340);
    assert_eq!(rows[0]["data"]["continuationsUsed"], 2);
}

/// The mint persists its row before the `goal_update`, so the count survives a crash.
#[tokio::test]
async fn compact_mint_persists_the_goal_state_row() {
    let (worker, file) = goal_dispatch_worker_with_store(json!({
        "status": "active",
        "objective": "ship the port",
        "state": {
            "active": true,
            "status": "active",
            "goalId": "goal-1",
            "objective": "ship the port",
            "tokensUsed": 340,
            "timeUsedSeconds": 9,
            "continuationsUsed": 1,
        },
    }))
    .await;
    let compact = worker
        .dispatch("compact", &json!({ "activeSessionId": "goal-session" }))
        .await;
    assert!(compact.success, "scripted compact failed: {compact:?}");
    let idle = worker.dispatch("wait_for_idle", &json!({})).await;
    assert!(idle.success, "never went idle: {idle:?}");
    let rows = thread_goal_state_rows(&file);
    assert_eq!(rows.len(), 1, "rows: {rows:?}");
    assert_eq!(rows[0]["data"]["status"], "active");
    assert_eq!(rows[0]["data"]["continuationsUsed"], 1);
    assert_eq!(rows[0]["data"]["objective"], "ship the port");
}

#[allow(clippy::await_holding_lock)] // faux provider registration spans the async flow
#[tokio::test(flavor = "current_thread")]
async fn versioned_resume_is_durable_before_admission_and_failed_append_latches() {
    let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for failure in [None, Some("missing"), Some("append")] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("goal.jsonl");
        let cwd = dir.path().to_string_lossy().into_owned();
        let mut store = SessionFile::create(&cwd, None, 0);
        store.set_path(path.clone());
        store.append_entry("custom", json!({"customType":pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data":{"active":false,"status":"error","goalId":"resume-test","objective":"resume mission",
            "tokensUsed":123,"timeUsedSeconds":27,"continuationsUsed":4,"createdAt":1}}));
        store.rewrite().unwrap();
        let worker = Worker::new(
            WorkerConfig {
                socket_path: dir.path().join("worker.sock"),
                supervisor_socket_path: PathBuf::new(),
                token: "test".into(),
                worker_instance_id: String::new(),
                active_session_id: "resume-worker".into(),
                agent_dir: dir.path().join("agent"),
                recovery_journal_path: dir.path().join("recovery.jsonl"),
                telemetry_disabled: Some(true),
                script: Some(json!({"engine":"faux","responses":["should not run"]})),
            },
            None,
        );
        assert!(
            worker
                .dispatch("create", &json!({"sessionPath":path,"cwd":cwd}))
                .await
                .success
        );
        let engine = worker.agent_engine.as_ref().unwrap();
        let model = engine.resolve_model().unwrap();
        engine.ensure_core_session_async(&model).await.unwrap();
        assert!(worker.dispatch("wait_for_idle", &json!({})).await.success);
        // Hold actual input admission; a resume may not bypass this pause.
        assert!(
            worker
                .dispatch(
                    "acquire_session_input_pause",
                    &json!({"activeSessionId":"resume-worker",
            "clientId":"test","leaseKey":"hold"})
                )
                .await
                .success
        );
        let request = json!({"extensionVersion":1,"expectedGoalId":"resume-test",
            "expectedObjective":"resume mission","action":"resume"});
        let mut stale = request.clone();
        stale["expectedGoalId"] = json!("wrong");
        assert!(!worker.dispatch("control_goal", &stale).await.success);
        assert_eq!(thread_goal_state_rows(&path).len(), 1);
        if failure == Some("missing") {
            worker.core.lock().unwrap().store = None;
        }
        if failure == Some("append") {
            let blocker = dir.path().join("not-directory");
            std::fs::write(&blocker, "block").unwrap();
            worker
                .core
                .lock()
                .unwrap()
                .store
                .as_mut()
                .unwrap()
                .set_path(blocker.join("session.jsonl"));
        }
        let mut events = worker.events.subscribe();
        let response = worker.dispatch("control_goal", &request).await;
        if failure.is_some() {
            assert!(!response.success, "{response:?}");
            assert!(worker
                .goal_edit_dirty
                .load(std::sync::atomic::Ordering::SeqCst));
            assert!(session_events_since(&mut events).is_empty());
            assert!(!worker.dispatch("control_goal", &request).await.success);
            assert_eq!(thread_goal_state_rows(&path).len(), 1);
        } else {
            assert!(response.success, "{response:?}");
            let data = response.data.unwrap();
            assert_eq!(data["extensionVersion"], 1);
            assert_eq!(data["goal"]["status"], "active");
            assert_eq!(data["goal"]["tokensUsed"], 123);
            assert_eq!(data["goal"]["timeUsedSeconds"], 27);
            assert_eq!(data["goal"]["continuationsUsed"], 4);
            let rows = thread_goal_state_rows(&path);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[1]["data"]["status"], "active");
            assert!(!worker.core.lock().unwrap().busy);
            assert!(!worker.dispatch("control_goal", &request).await.success);
        }
    }
}

#[tokio::test]
async fn factory_progress_admission_preserves_same_active_or_paused_goal_and_budget() {
    for status in ["active", "paused"] {
        let worker = goal_dispatch_worker(json!({"goalId":"same-goal","status":status,
            "active":status == "active","objective":"actual owner scope","tokenBudget":1000,
            "tokensUsed":17,"continuationsUsed":3}))
        .await;
        let before = worker.engine.goal_state_value();
        let row = pa_core::session_engine::messages::create_factory_progress_message(
            "run",
            "finished",
            /*node*/ None,
            "factory outcome needs root acceptance",
            1,
        );
        admit_kernel_notice(
            &worker.recovery,
            &worker.core,
            &Arc::new(Notify::new()),
            crate::engine::KernelNotice {
                row,
                preview_label: pa_core::session_engine::messages::FACTORY_PROGRESS_PREVIEW_LABEL,
            },
            || false,
        );
        assert_eq!(
            worker.engine.goal_state_value(),
            before,
            "a factory milestone is not goal completion/resume or new human authority"
        );
        let core = worker.core.lock().unwrap();
        let notice = core.steering.back().expect("notice queued");
        assert_eq!(notice.priority, QueuePriority::Background);
        assert!(notice.input_identity.is_none());
    }
}
