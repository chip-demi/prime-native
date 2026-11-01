//! The goal tests (recovery, continuation minting, the goal loop's turn-end accounting).
use super::*;

#[test]
fn guarded_recovery_preserves_same_goal_and_continues_only_once_when_allowed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for allowed in [true, false] {
        let dir = tempfile::TempDir::new().unwrap();
        let agent_dir = dir.path().join("agent");
        std::fs::create_dir_all(&agent_dir).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                for step in 0..if allowed { 2 } else { 1 } {
                    let (mut socket, _) = tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept()).await.unwrap().unwrap();
                    let mut bytes = Vec::new();
                    let mut chunk = [0; 4096];
                    let header_end = loop {
                        let n = socket.read(&mut chunk).await.unwrap();
                        assert!(n > 0);
                        bytes.extend_from_slice(&chunk[..n]);
                        assert!(bytes.len() <= 256 * 1024);
                        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") { break i + 4; }
                    };
                    let head = String::from_utf8_lossy(&bytes[..header_end]);
                    let length = head.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").and_then(|s| s.trim().parse::<usize>().ok())).unwrap();
                    while bytes.len() < header_end + length {
                        let n = socket.read(&mut chunk).await.unwrap();
                        assert!(n > 0);
                        bytes.extend_from_slice(&chunk[..n]);
                        assert!(bytes.len() <= 256 * 1024);
                    }
                    if step == 0 {
                        ready_tx.send(()).unwrap();
                        let n = tokio::time::timeout(std::time::Duration::from_secs(10), socket.read(&mut chunk)).await.unwrap().unwrap();
                        assert_eq!(n, 0, "targeted cancellation must close the held HTTP request");
                    } else {
                        let body = format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                            json!({"choices":[{"index":0,"delta":{"content":"verified progress"},"finish_reason":null}]}),
                            json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}));
                        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                        socket.write_all(response.as_bytes()).await.unwrap();
                    }
                }
            });
        });
        std::fs::write(agent_dir.join("models.json"),json!({"providers":{"goal-fixture":{"api":"openai-completions","baseUrl":format!("http://{address}"),"apiKey":"offline-fixture","models":[{"id":"held","name":"held","contextWindow":128_000,"maxTokens":1024}]}}}).to_string()).unwrap();
        std::fs::write(agent_dir.join("settings.json"), json!({"defaultProvider":"goal-fixture","defaultModel":"held","retry":{"provider":{"timeoutMs":1}},"compaction":{"enabled":false}}).to_string()).unwrap();
        let session_path = dir.path().join("session.jsonl");
        let mut store = crate::session_store::SessionFile::create("/tmp", None, 0);
        store.set_path(session_path.clone());
        store.append_entry("custom", json!({"customType":pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data":{"active":true,"status":"active","goalId":"guarded-goal","objective":"ship the owned module",
                "tokenBudget":100_000,"tokensUsed":77,"timeUsedSeconds":9,"continuationsUsed":2}}));
        store.rewrite().unwrap();
        let engine = Arc::new(
            AgentSessionEngine::new(AgentEngineConfig {
                cwd: dir.path().to_path_buf(),
                agent_dir,
                provider: None,
                model: None,
                api_key: None,
                thinking: None,
                session_dir: None,
                session_file: Some(session_path),
                faux_script: None,
                supervisor_link: None,
                telemetry_disabled: Some(true),
                cron_store: None,
                queued_steering_probe: None,
            })
            .unwrap(),
        );
        let work = goal_admission_collector(&engine);
        engine.set_drone_resume_guard(Arc::new(move || allowed));
        let runner = engine.clone();
        let turn = std::thread::spawn(move || {
            let mut events = Vec::new();
            admit(&runner, "one original user request".into(), &mut events);
            events
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        let control = engine.drone_requests.clone();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let snapshot = loop {
            let snapshot = control.snapshot().unwrap();
            if snapshot
                .deadline_ms
                .is_some_and(|limit| snapshot.elapsed_ms >= limit)
            {
                break snapshot;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "Native deadline did not become eligible"
            );
            std::thread::yield_now();
        };
        control
            .recover(snapshot.request_id, snapshot.revision, "goal-recovery")
            .unwrap();
        let events = turn.join().unwrap();
        server.join().unwrap();
        assert_eq!(
            control.operation_status("goal-recovery").as_deref(),
            Some(if allowed { "continued" } else { "held" })
        );
        let goal = engine.goal_state_value();
        assert_eq!(goal["status"], "active");
        assert_eq!(goal["active"], true);
        assert_eq!(goal["goalId"], "guarded-goal");
        assert_eq!(goal["objective"], "ship the owned module");
        assert_eq!(goal["tokenBudget"], 100_000);
        assert_eq!(goal["continuationsUsed"], if allowed { 3 } else { 2 });
        assert_eq!(work.lock().unwrap().len(), usize::from(allowed));
        assert!(!events.iter().any(|event| matches!(event, EngineEvent::AssistantMessage(message) if message["stopReason"]=="error")));
        let entries = engine_session_entries(&engine);
        assert!(pa_core::goals::stale_active_goal_failure(&entries).is_none());
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(
                    entry,
                    pa_types::session::FileEntry::Message {
                        message: pa_types::session::AgentMessage::User(_),
                        ..
                    }
                ))
                .count(),
            1
        );
    }
}

#[tokio::test(flavor = "current_thread")]
// Serializes the global faux provider across independent test runtimes;
// no task in this current-thread runtime acquires this test-only mutex.
#[allow(clippy::await_holding_lock)]
async fn objective_edit_joins_unfinished_create_prewarm_without_a_turn() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    let session_path = dir.path().join("session.jsonl");
    let mut store = crate::session_store::SessionFile::create("/tmp", None, 0);
    store.set_path(session_path.clone());
    store.append_entry(
        "custom",
        json!({
            "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data": {
                "active": false,
                "status": "paused",
                "goalId": "goal-1",
                "objective": "old objective",
                "tokensUsed": 34,
                "timeUsedSeconds": 9,
                "continuationsUsed": 2,
            },
        }),
    );
    store.rewrite().unwrap();
    let engine = AgentSessionEngine::new(AgentEngineConfig {
        cwd: dir.path().to_path_buf(),
        agent_dir,
        provider: None,
        model: None,
        api_key: None,
        thinking: None,
        session_dir: None,
        session_file: Some(session_path),
        faux_script: Some(r#"{"responses": [{"text": "should not run"}]}"#.to_string()),
        supervisor_link: None,
        telemetry_disabled: None,
        cron_store: None,
        queued_steering_probe: None,
    })
    .unwrap();
    assert!(engine.goal_runtime.lock().unwrap().is_none());
    let edited = engine
        .edit_goal_objective("goal-1", "old objective", "new objective")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(edited.status, pa_core::goals::GoalStatus::Paused);
    assert_eq!(edited.tokens_used, 34);
    assert_eq!(edited.continuations_used, 2);
    assert_eq!(engine.goal_state_value()["objective"], "new objective");
    let core = engine.session.lock().await.clone().unwrap();
    assert!(!core.session.agent().state().await.is_streaming);
    assert!(!core.session.agent().has_queued_messages());
}

#[tokio::test(flavor = "current_thread")]
// The same cross-test faux registry guard is deliberately held for this runtime.
#[allow(clippy::await_holding_lock)]
async fn objective_edit_waits_for_published_handles_to_finish_adoption() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    let session_path = dir.path().join("session.jsonl");
    let mut store = crate::session_store::SessionFile::create("/tmp", None, 0);
    store.set_path(session_path.clone());
    store.append_entry(
        "custom",
        json!({
            "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data": {
                "active": false,
                "status": "paused",
                "goalId": "goal-1",
                "objective": "old objective",
                "tokensUsed": 34,
                "timeUsedSeconds": 9,
                "continuationsUsed": 2,
            },
        }),
    );
    store.rewrite().unwrap();
    let engine = AgentSessionEngine::new(AgentEngineConfig {
        cwd: dir.path().to_path_buf(),
        agent_dir,
        provider: None,
        model: None,
        api_key: None,
        thinking: None,
        session_dir: None,
        session_file: Some(session_path),
        faux_script: Some(r#"{"responses": [{"text": "should not run"}]}"#.to_string()),
        supervisor_link: None,
        telemetry_disabled: None,
        cron_store: None,
        queued_steering_probe: None,
    })
    .unwrap();
    let build_guard = engine.session_build.lock().await;
    let model = engine.resolve_model().unwrap();
    let built = engine.build_session(&model).await.unwrap();
    engine.mirror_goal_runtime(&built).await;
    assert!(engine.session.lock().await.is_none());
    assert!(engine.goal_runtime.lock().unwrap().is_some());
    assert!(built.goal_driver.lock().await.state().goal_id.is_none());

    let mut edit = engine.edit_goal_objective("goal-1", "old objective", "new objective");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut edit)
            .await
            .is_err(),
        "the edit must wait for the build gate despite published idle handles"
    );
    drop(build_guard);
    let edited = edit.await.unwrap().unwrap();
    assert_eq!(edited.goal_id.as_deref(), Some("goal-1"));
    assert_eq!(edited.objective.as_deref(), Some("new objective"));
    assert_eq!(edited.status, pa_core::goals::GoalStatus::Paused);
    assert_eq!(edited.tokens_used, 34);
    assert_eq!(edited.time_used_seconds, 9);
    assert_eq!(edited.continuations_used, 2);
    let core = engine.session.lock().await.clone().unwrap();
    let state = core.session.agent().state().await;
    assert!(!state.is_streaming);
    assert!(
        state.messages.is_empty(),
        "objective edit invoked a model turn"
    );
    assert!(!core.session.agent().has_queued_messages());
}

#[test]
fn recovery_rebuild_rehydrates_the_goal_from_the_session_file() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    // The durable store a killed worker leaves behind: an active goal
    // mid-pursuit with usage and continuation counts on the books.
    let mut store = crate::session_store::SessionFile::create("/tmp", None, 0);
    let session_path = dir.path().join("session.jsonl");
    store.set_path(session_path.clone());
    store.append_entry(
        "custom",
        json!({
            "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data": {
                "active": true,
                "status": "active",
                "goalId": "goal-1",
                "objective": "ship the port",
                "tokensUsed": 340,
                "timeUsedSeconds": 9,
                "continuationsUsed": 2,
            },
        }),
    );
    store.rewrite().expect("write session file");
    let engine = std::sync::Arc::new(
        AgentSessionEngine::new(AgentEngineConfig {
            cwd: dir.path().to_path_buf(),
            agent_dir,
            provider: None,
            model: None,
            api_key: None,
            thinking: None,
            session_dir: None,
            session_file: Some(session_path),
            faux_script: Some(r#"{"responses": [{"text": "recovery reply"}]}"#.to_string()),
            supervisor_link: None,
            telemetry_disabled: None,
            cron_store: None,
            queued_steering_probe: None,
        })
        .unwrap(),
    );
    // The turn-end seam: the engine has no worker, so a collector
    // stands in for the queue-lane admission sink.
    let goal_work = goal_admission_collector(&engine);
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(&engine, "keep working".to_string(), &mut events);
    let goal = engine.goal_state_value();
    assert_eq!(goal["status"], "active");
    assert_eq!(goal["objective"], "ship the port");
    assert_eq!(goal["goalId"], "goal-1");
    assert_eq!(goal["continuationsUsed"], 3);
    assert!(goal["tokensUsed"].as_u64().unwrap() >= 340);
    // Usage accounting announced from the rehydrated base: one
    // `goal_update` through the run's emit (the turn-end mint's update
    // surfaces through the admission sink, not the run's emit).
    let goal_updates: Vec<&EngineEvent> = events
        .iter()
        .filter(|event| matches!(event, EngineEvent::GoalUpdate { .. }))
        .collect();
    assert_eq!(goal_updates.len(), 1, "events: {events:?}");
    let EngineEvent::GoalUpdate { goal } = goal_updates[0] else {
        unreachable!();
    };
    assert_eq!(goal["objective"], "ship the port");
    assert_eq!(goal["continuationsUsed"], 2);
    let work = goal_work.lock().unwrap();
    let [crate::engine::GoalTurnEndWork::Continuation(minted)] = work.as_slice() else {
        panic!("unexpected goal work: {work:?}");
    };
    assert!(minted.request.message.contains("[goal: continuation]"));
    assert_eq!(
        minted
            .goal_update
            .as_ref()
            .expect("the mint moved the state")["continuationsUsed"],
        3
    );
    drop(work);
    let minted = engine
        .mint_post_compaction_goal_continuation()
        .expect("the rehydrated goal mints");
    assert_eq!(
        minted.goal_update.expect("mint moved the state")["continuationsUsed"],
        4
    );
}

/// The `/goal` objective start keeps the prompt's attached images on the
/// injected context row (TS `createGoalContextMessage`: text first, images
/// after) — the row is the turn's only image carrier, session commands
/// never admit a user row.
#[test]
fn goal_start_keeps_the_prompt_images_on_the_context_row() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    let engine = std::sync::Arc::new(
        AgentSessionEngine::new(AgentEngineConfig {
            cwd: dir.path().to_path_buf(),
            agent_dir: dir.path().join("agent"),
            provider: None,
            model: None,
            api_key: None,
            thinking: None,
            session_dir: None,
            session_file: None,
            faux_script: Some(r#"{"responses": [{"text": "goal reply"}]}"#.to_string()),
            supervisor_link: None,
            telemetry_disabled: None,
            cron_store: None,
            queued_steering_probe: None,
        })
        .unwrap(),
    );
    let mut events: Vec<EngineEvent> = Vec::new();
    engine.run_prompt(
        0,
        PromptRequest {
            batch: Vec::new(),
            images: vec![pa_agent::types::ImageContent {
                data: "aGVsbG8gZ29hbA==".to_string(),
                mime_type: "image/png".to_string(),
            }],
            message: "/goal describe the attachment".to_string(),
            source: "user".to_string(),
            agent_message_id: None,
            custom_message: None,
        },
        &|| false,
        &mut |event| {
            events.push(event);
            true
        },
    );
    let context = events
        .iter()
        .find_map(|event| match event {
            EngineEvent::CustomMessage(value) if value["customType"] == "goal_context" => {
                Some(value.clone())
            }
            _ => None,
        })
        .expect("the goal start emitted its context row");
    let blocks = context["content"]
        .as_array()
        .expect("the context row carries the attached image as content blocks");
    assert_eq!(blocks[0]["type"], "text");
    let text = blocks[0]["text"].as_str().expect("text block content");
    assert!(
        text.contains("[goal: continuation]") && text.contains("describe the attachment"),
        "the continuation prompt names the objective: {text}"
    );
    assert_eq!(
        blocks[1],
        json!({ "type": "image", "data": "aGVsbG8gZ29hbA==", "mimeType": "image/png" })
    );
}

/// A durable message row in the worker's persisted wire shape.
fn wire_user_message(text: String) -> Value {
    serde_json::to_value(pa_types::session::AgentMessage::User(
        pa_types::ai::UserMessage {
            content: pa_types::ai::UserContent::Text(text),
            timestamp: 1,
            rest: Map::default(),
        },
    ))
    .expect("user message serializes")
}

/// A durable assistant row in the worker's persisted wire shape.
fn wire_assistant_message(text: String) -> Value {
    serde_json::to_value(pa_types::session::AgentMessage::Assistant(
        pa_types::ai::AssistantMessage {
            content: vec![pa_types::ai::AssistantContentBlock::Text(
                pa_types::ai::TextContent {
                    text,
                    text_signature: None,
                    rest: Map::default(),
                },
            )],
            api: "faux".to_string(),
            provider: "faux".to_string(),
            model: "faux-1".to_string(),
            response_model: None,
            response_id: None,
            diagnostics: None,
            usage: pa_types::ai::Usage::default(),
            stop_reason: pa_types::ai::StopReason::Stop,
            stop_reason_raw: None,
            error_message: None,
            timestamp: 2,
            rest: Map::default(),
        },
    ))
    .expect("assistant message serializes")
}

/// One-store recovery: the worker respawns with `--resume <sessionFile>`,
/// so the rebuilt session's branch carries the pre-crash history and a
/// post-recovery compact runs over it — never a skip on the empty branch.
#[test]
fn recovered_engine_compaction_walk_sees_the_durable_history() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    write_compaction_settings(dir.path(), 1);
    // The durable store a killed worker leaves behind: a long
    // conversation the fresh engine never saw in memory.
    let long = "x".repeat(48_000);
    let mut store = crate::session_store::SessionFile::create("/tmp", None, 0);
    let session_path = dir.path().join("session.jsonl");
    store.set_path(session_path.clone());
    store.append_message(&wire_user_message(format!("work turn one {long}")));
    store.append_message(&wire_assistant_message(format!("reply one {long}")));
    store.rewrite().expect("write session file");
    let engine = AgentSessionEngine::new(AgentEngineConfig {
        cwd: dir.path().to_path_buf(),
        agent_dir: dir.path().join("agent"),
        provider: None,
        model: None,
        api_key: None,
        thinking: None,
        session_dir: None,
        session_file: Some(session_path),
        faux_script: Some(
            serde_json::json!({
                "responses": [{"text": "recovery reply"}, {"text": "the summary"}]
            })
            .to_string(),
        ),
        supervisor_link: None,
        telemetry_disabled: None,
        cron_store: None,
        queued_steering_probe: None,
    })
    .unwrap();
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(
        &engine,
        format!("keep working after the crash {}", "y".repeat(2_000)),
        &mut events,
    );
    let entries = engine_session_entries(&engine);
    assert!(
        entries.iter().any(|entry| match entry {
            pa_types::session::FileEntry::Message {
                message: pa_types::session::AgentMessage::User(user),
                ..
            } => user.content.text().contains("work turn one"),
            _ => false,
        }),
        "the recovery build adopted the durable history: {entries:?}"
    );
    let controller = std::sync::Arc::new(pa_agent::abort::AbortController::new());
    let signal = controller.signal();
    let outcome = engine.run_compaction(
        crate::engine::CompactionRequest {
            custom_instructions: None,
        },
        &signal,
    );
    match outcome {
        crate::engine::CompactionOutcome::Compacted { run } => {
            assert_eq!(run.result["summary"], "the summary", "the compact ran");
            assert!(
                run.result["firstKeptEntryId"].is_string(),
                "the cut resolved a kept entry: {run:?}"
            );
        }
        other => panic!("the recovered compact did not run: {other:?}"),
    }
    assert!(compaction_entry_in_entries(&engine));
}

#[test]
fn post_compaction_goal_continuation_mint() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (engine, _engine_dir) = faux_engine_with_settings(
        &serde_json::json!({ "responses": [{"text": "goal turn reply"}] }),
        1,
    );
    let engine = std::sync::Arc::new(engine);
    let goal_work = goal_admission_collector(&engine);
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(
        &engine,
        "/goal ship the post-compact continue".to_string(),
        &mut events,
    );
    // The goal-start turn ran (TS `/goal` start consumes no
    // continuation slot), and its natural end minted the next one.
    assert_eq!(engine.goal_state_value()["status"], "active");
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 1);
    let work = goal_work.lock().unwrap();
    let [crate::engine::GoalTurnEndWork::Continuation(turn_end)] = work.as_slice() else {
        panic!("unexpected goal work: {work:?}");
    };
    assert!(turn_end.request.message.contains("[goal: continuation]"));
    assert_eq!(
        turn_end.goal_update.as_ref().expect("mint moved the state")["continuationsUsed"],
        1
    );
    drop(work);
    let minted = engine
        .mint_post_compaction_goal_continuation()
        .expect("active goal mints the continuation");
    let message = minted.request.message;
    assert!(
        message.contains("[goal: continuation]"),
        "unexpected continuation text: {message}"
    );
    assert!(
        message.contains("ship the post-compact continue"),
        "the continuation context lost the objective: {message}"
    );
    let row = minted
        .request
        .custom_message
        .expect("the goal-context row rides the turn");
    assert_eq!(row["customType"], "goal_context");
    assert_eq!(row["role"], "custom");
    assert_eq!(row["content"], json!(message));
    assert_eq!(row["details"]["kind"], "continuation");
    assert_eq!(row["details"]["continuationsUsed"], 2);
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 2);
    let goal_update = minted.goal_update.expect("the mint moved the state");
    assert_eq!(goal_update["status"], "active");
    assert_eq!(goal_update["continuationsUsed"], 2);
    // Admission releases the pending guard: the item's OWN handle,
    // never the mutable mirror.
    AgentSessionEngine::release_goal_continuation_handle(minted.pending_handle.as_ref());
    // TS #2465: a live background bash handle holds the post-compaction
    // mint the same way: the mint defers (owed, not consumed) until the
    // handle settles.
    let live_probe: std::sync::Arc<dyn Fn() -> bool + Send + Sync> = std::sync::Arc::new(|| true);
    *engine.background_bash_probe.lock().unwrap() = Some(live_probe);
    assert!(
        engine.mint_post_compaction_goal_continuation().is_none(),
        "a live handle minted the post-compaction continuation"
    );
    {
        let handles = engine
            .goal_runtime
            .lock()
            .unwrap()
            .clone()
            .expect("goal runtime");
        assert!(
            engine
                .runtime
                .block_on(async { handles.driver.lock().await.owes_continuation() }),
            "the deferred mint must be owed"
        );
    }
    let settled_probe: std::sync::Arc<dyn Fn() -> bool + Send + Sync> =
        std::sync::Arc::new(|| false);
    *engine.background_bash_probe.lock().unwrap() = Some(settled_probe);
    assert!(
        engine.mint_post_compaction_goal_continuation().is_some(),
        "the settled handle releases the post-compaction mint"
    );
    // A mint over a paused goal produces nothing.
    let mut pause_events: Vec<EngineEvent> = Vec::new();
    admit(&engine, "/goal pause".to_string(), &mut pause_events);
    assert_eq!(engine.goal_state_value()["status"], "paused");
    assert!(
        engine.mint_post_compaction_goal_continuation().is_none(),
        "a paused goal minted a continuation"
    );
}

/// The operator's continuation-spam regression (2026-09-28): a
/// post-compaction mint over an ALREADY-armed deferral delivers the
/// OWED continuation exactly once; a fresh mint would leave the
/// armed flag behind and the settle sites deliver a second one.
#[test]
fn post_compaction_mint_delivers_the_armed_deferral_once() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (engine, _engine_dir) = faux_engine_with_settings(
        &serde_json::json!({ "responses": [{"text": "goal turn reply"}] }),
        1,
    );
    let engine = std::sync::Arc::new(engine);
    let goal_work = goal_admission_collector(&engine);
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(
        &engine,
        "/goal ship the armed deferral".to_string(),
        &mut events,
    );
    assert_eq!(engine.goal_state_value()["status"], "active");
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 1);
    let handles = engine
        .goal_runtime
        .lock()
        .unwrap()
        .clone()
        .expect("goal runtime");
    engine
        .runtime
        .block_on(async { handles.driver.lock().await.mark_continuation_owed() });
    let minted = engine
        .mint_post_compaction_goal_continuation()
        .expect("the armed deferral mints");
    assert!(minted.request.message.contains("[goal: continuation]"));
    assert_eq!(
        minted
            .goal_update
            .as_ref()
            .expect("the mint moved the state")["continuationsUsed"],
        2
    );
    assert!(
        !engine
            .runtime
            .block_on(async { handles.driver.lock().await.owes_continuation() }),
        "the post-compaction delivery consumed the armed deferral"
    );
    engine.retry_owed_goal_continuation();
    for _ in 0..200 {
        if goal_work.lock().unwrap().len() > 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let work = goal_work.lock().unwrap();
    assert_eq!(
        work.len(),
        1,
        "the armed boundary delivered exactly one continuation: {work:?}"
    );
    assert_eq!(
        engine.goal_state_value()["continuationsUsed"],
        2,
        "no second mint after the owed delivery"
    );
}

/// Admit one full turn request, collecting its events.
pub(crate) fn admit_request(
    engine: &AgentSessionEngine,
    request: crate::engine::PromptRequest,
    events: &mut Vec<EngineEvent>,
) {
    engine.run_prompt(0, request, &|| false, &mut |event| {
        events.push(event);
        true
    });
}

#[test]
fn goal_turn_end_mints_the_loop_until_the_goal_completes() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    let engine = std::sync::Arc::new(
        AgentSessionEngine::new(AgentEngineConfig {
            cwd: dir.path().to_path_buf(),
            agent_dir: dir.path().join("agent"),
            provider: None,
            model: None,
            api_key: None,
            thinking: None,
            session_dir: None,
            session_file: None,
            faux_script: Some(
                serde_json::json!({ "responses": [
                    {"text": "first turn"},
                    {"text": "second turn"},
                    {"text": "third turn"},
                    {"text": "final turn"},
                ]})
                .to_string(),
            ),
            supervisor_link: None,
            telemetry_disabled: None,
            cron_store: None,
            queued_steering_probe: None,
        })
        .unwrap(),
    );
    let goal_work = goal_admission_collector(&engine);
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(&engine, "/goal ship the goal loop".to_string(), &mut events);
    assert_eq!(engine.goal_state_value()["status"], "active");
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 1);
    let mut request = {
        let mut work = goal_work.lock().unwrap();
        let crate::engine::GoalTurnEndWork::Continuation(follow_up) =
            work.pop().expect("the start turn minted one continuation")
        else {
            panic!("expected a continuation");
        };
        assert!(follow_up.request.message.contains("[goal: continuation]"));
        assert!(follow_up.request.message.contains("ship the goal loop"));
        follow_up.request
    };
    for expected_count in [2u64, 3] {
        let mut turn_events: Vec<EngineEvent> = Vec::new();
        admit_request(&engine, request, &mut turn_events);
        request = {
            let mut work = goal_work.lock().unwrap();
            assert_eq!(work.len(), 1, "unexpected goal work: {work:?}");
            let crate::engine::GoalTurnEndWork::Continuation(follow_up) = work
                .pop()
                .expect("the settled turn minted the next continuation")
            else {
                panic!("expected a continuation");
            };
            let row = follow_up
                .request
                .custom_message
                .as_ref()
                .expect("the row rides");
            assert_eq!(row["customType"], "goal_context");
            assert_eq!(row["details"]["kind"], "continuation");
            assert_eq!(
                row["details"]["continuationsUsed"],
                serde_json::json!(expected_count)
            );
            assert_eq!(
                follow_up.goal_update.expect("mint moved the state")["continuationsUsed"],
                serde_json::json!(expected_count)
            );
            follow_up.request
        };
        assert_eq!(
            engine.goal_state_value()["continuationsUsed"],
            serde_json::json!(expected_count)
        );
    }
    let handles = engine
        .goal_runtime
        .lock()
        .unwrap()
        .clone()
        .expect("goal runtime");
    engine.runtime.block_on(async {
        let mut driver = handles.driver.lock().await;
        let mut session = handles.session.lock().await;
        driver.complete(&mut session).unwrap();
    });
    let mut turn_events: Vec<EngineEvent> = Vec::new();
    admit_request(&engine, request, &mut turn_events);
    assert_eq!(engine.goal_state_value()["status"], "complete");
    assert_eq!(
        engine.goal_state_value()["continuationsUsed"],
        3,
        "a completed goal mints no continuation at the boundary"
    );
    assert!(goal_work.lock().unwrap().is_empty());
}

#[test]
fn persistent_goal_turns_keep_cadence_and_arm_the_owned_existing_wake() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    std::fs::write(
        agent_dir.join("settings.json"),
        json!({"compaction":{"enabled":false}}).to_string(),
    )
    .unwrap();
    let session_file = dir.path().join("session.jsonl");
    let now = crate::util::now_ms();
    let header = json!({"type":"session","version":3,"id":"cadence-session",
        "timestamp":"2026-01-01T00:00:00Z","cwd":dir.path()});
    let goal = json!({"type":"custom","id":"owned-goal","parentId":null,
        "customType":pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
        "data":{"active":true,"status":"active","goalId":"cadence-goal",
            "objective":"write the full owned explanation","tokenBudget":1_000_000,
            "tokensUsed":0,"timeUsedSeconds":0,"continuationsUsed":0,
            "createdAt":now.saturating_sub(1000),"updatedAt":now.saturating_sub(1000)}});
    assert!(pa_core::goals::is_persisted_goal_state(&goal["data"]));
    std::fs::write(&session_file, format!("{header}\n{goal}\n")).unwrap();
    let restored = crate::goal_state_persist::persisted_goal_state(Some(&session_file))
        .expect("the actual durable reader must accept the goal fixture");
    assert_eq!(restored.goal_id.as_deref(), Some("cadence-goal"));
    assert_eq!(restored.status, pa_core::goals::GoalStatus::Active);
    let store = Arc::new(pa_core::cron::store::AgentCronJobStore::for_session_artifacts());
    store.register_session_artifact("cadence-session", &dir.path().join("artifacts"));
    let engine = Arc::new(
        AgentSessionEngine::new(AgentEngineConfig {
            cwd: dir.path().to_path_buf(),
            agent_dir,
            provider: None,
            model: None,
            api_key: None,
            thinking: None,
            session_dir: None,
            session_file: Some(session_file.clone()),
            faux_script: Some(
                json!({"responses":[
                    {"text":"accepted human work"}, {"text":"novel text one"},
                    {"text":"novel text two"}, {"text":"novel text three"},
                    {"text":"accepted amendment work"}
                ]})
                .to_string(),
            ),
            supervisor_link: Some(SupervisorLinkConfig {
                socket_path: dir.path().join("unused.sock"),
                active_session_id: "cadence-active".into(),
                worker_token: "offline-fixture".into(),
            }),
            telemetry_disabled: Some(true),
            cron_store: Some(pa_core::session_engine::runtime_wiring::KernelCronWiring {
                store: Arc::clone(&store),
                binding: None,
                mutation_hook: None,
            }),
            queued_steering_probe: None,
        })
        .unwrap(),
    );
    let work = goal_admission_collector(&engine);
    let mut initial_events = Vec::new();
    admit(
        &engine,
        "one genuine human input".into(),
        &mut initial_events,
    );
    let core = engine.session.blocking_lock().as_ref().unwrap().clone();
    let driver = engine
        .goal_runtime
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .driver
        .clone();
    let mut next = None;
    for automatic_turn in 0..=3 {
        if automatic_turn != 0 {
            admit_request(&engine, next.take().unwrap(), &mut Vec::new());
        }
        let mut queued = work.lock().unwrap();
        if automatic_turn == 3 {
            assert!(
                queued.is_empty(),
                "the third automatic text turn must wait: {queued:?}"
            );
        } else {
            let [crate::engine::GoalTurnEndWork::Continuation(item)] = queued.as_slice() else {
                panic!("expected one real continuation after turn {automatic_turn}: {queued:?}; goal={:?}; initial_events={initial_events:?}", engine.goal_state_value());
            };
            assert_eq!(
                item.request.custom_message.as_ref().unwrap()["customType"],
                "goal_context"
            );
            let crate::engine::GoalTurnEndWork::Continuation(item) = queued.pop().unwrap() else {
                unreachable!()
            };
            next = Some(item.request);
        }
    }
    assert!(Arc::ptr_eq(
        &core,
        engine.session.blocking_lock().as_ref().unwrap()
    ));
    let handles = engine.goal_runtime.lock().unwrap().clone().unwrap();
    assert!(
        Arc::ptr_eq(&driver, &handles.driver),
        "normal admissions retain the driver"
    );
    let deadline = engine
        .runtime
        .block_on(async { handles.driver.lock().await.backoff_wake_at() })
        .expect("three synthetic text turns arm the existing wake");
    assert!(deadline > crate::util::now_ms());
    let goal = engine.goal_state_value();
    assert_eq!(goal["status"], "active");
    assert_eq!(goal["goalId"], "cadence-goal");
    assert_eq!(goal["tokenBudget"], 1_000_000);
    assert_eq!(goal["continuationsUsed"], 3);
    let jobs = store
        .list()
        .into_iter()
        .filter(|job| job.status == pa_core::cron::JobStatus::Active)
        .collect::<Vec<_>>();
    let [wake] = jobs.as_slice() else {
        panic!("one existing owned wake: {jobs:?}")
    };
    assert_eq!(wake.source.as_deref(), Some("goal_backoff_wake"));
    assert_eq!(
        wake.label.as_deref(),
        Some(pa_core::session_engine::goal_driver::GOAL_BACKOFF_WAKE_CRON_LABEL)
    );
    assert_eq!(wake.active_session_id, "cadence-active");
    assert_eq!(wake.session_id, "cadence-session");
    assert_eq!(wake.session_file, session_file.display().to_string());
    assert_eq!(
        wake.prompt,
        pa_core::session_engine::goal_driver::GOAL_BACKOFF_WAKE_MARKER_TEXT
    );
    assert_eq!(
        wake.next_run_at.as_deref(),
        Some(pa_core::session::manager::format_iso(i64::try_from(deadline).unwrap()).as_str())
    );
    admit(
        &engine,
        "a genuinely new human amendment".into(),
        &mut Vec::new(),
    );
    assert_eq!(
        work.lock().unwrap().len(),
        1,
        "new input immediately admits the next continuation"
    );
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 4);
    assert!(store
        .list()
        .iter()
        .all(|job| job.status != pa_core::cron::JobStatus::Active));
    assert!(Arc::ptr_eq(
        &core,
        engine.session.blocking_lock().as_ref().unwrap()
    ));
    assert_eq!(engine.goal_state_value()["goalId"], "cadence-goal");
    assert_eq!(engine.goal_state_value()["tokenBudget"], 1_000_000);
}

#[test]
fn paused_goal_mints_no_turn_end_continuation() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (engine, _engine_dir) = faux_engine_with_settings(
        &serde_json::json!({ "responses": [
            {"text": "start turn reply"},
            {"text": "paused turn reply"},
        ]}),
        1,
    );
    let engine = std::sync::Arc::new(engine);
    let goal_work = goal_admission_collector(&engine);
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(&engine, "/goal ship while paused".to_string(), &mut events);
    let request = {
        let mut work = goal_work.lock().unwrap();
        let crate::engine::GoalTurnEndWork::Continuation(follow_up) =
            work.pop().expect("the start turn minted")
        else {
            panic!("expected a continuation");
        };
        follow_up.request
    };
    let count_before = engine.goal_state_value()["continuationsUsed"].clone();
    let mut pause_events: Vec<EngineEvent> = Vec::new();
    admit(&engine, "/goal pause".to_string(), &mut pause_events);
    assert_eq!(engine.goal_state_value()["status"], "paused");
    let mut turn_events: Vec<EngineEvent> = Vec::new();
    admit_request(&engine, request, &mut turn_events);
    assert!(goal_work.lock().unwrap().is_empty());
    assert_eq!(engine.goal_state_value()["continuationsUsed"], count_before);
    assert_eq!(turn_events.last(), Some(&EngineEvent::Done(Ok(()))));
    admit(&engine, "/goal clear".to_string(), &mut Vec::new());
    let mut after_clear: Vec<EngineEvent> = Vec::new();
    admit(&engine, "plain turn".to_string(), &mut after_clear);
    assert!(goal_work.lock().unwrap().is_empty());
    assert_eq!(engine.goal_state_value()["status"], "idle");
}

#[test]
fn budget_exhausted_stops_with_the_ts_budget_steer() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (engine, _engine_dir) = faux_engine_with_settings(
        &serde_json::json!({ "responses": [{"text": "crossing turn reply"}] }),
        1,
    );
    let engine = std::sync::Arc::new(engine);
    let goal_work = goal_admission_collector(&engine);
    let mut events: Vec<EngineEvent> = Vec::new();
    // A tiny budget: the goal-start turn's usage crosses it (faux
    // usage is estimated from the context).
    admit(
        &engine,
        "/goal --budget 10 budget the runaway turn".to_string(),
        &mut events,
    );
    let goal = engine.goal_state_value();
    assert_eq!(goal["status"], "budget_limited");
    assert_eq!(
        goal["lastReason"],
        serde_json::json!("Reached 10 token goal budget")
    );
    let work = goal_work.lock().unwrap();
    let [crate::engine::GoalTurnEndWork::BudgetLimitSteer(steer)] = work.as_slice() else {
        panic!("expected exactly the budget steer: {work:?}");
    };
    let steer_text = &steer.request.message;
    assert!(
        steer_text.starts_with("[goal: budget-limit]"),
        "text: {steer_text}"
    );
    assert!(steer_text.contains("budget the runaway turn"));
    assert!(steer_text.contains("status: budget_limited"));
    assert!(steer_text.contains("Do not start new substantive work"));
    let row = steer
        .request
        .custom_message
        .as_ref()
        .expect("the row rides");
    assert_eq!(row["customType"], "goal_context");
    assert_eq!(row["details"]["kind"], "budget_limit");
    assert!(steer.goal_update.is_none());
    drop(work);
    let goal_updates: Vec<&EngineEvent> = events
        .iter()
        .filter(|event| matches!(event, EngineEvent::GoalUpdate { .. }))
        .collect();
    assert!(
        goal_updates
            .iter()
            .any(|event| matches!(event, EngineEvent::GoalUpdate { goal }
                if goal["status"] == serde_json::json!("budget_limited"))),
        "the budget transition never announced: {events:?}"
    );
    assert_eq!(events.last(), Some(&EngineEvent::Done(Ok(()))));
}

#[test]
fn queued_input_defers_the_turn_end_mint() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (engine, _engine_dir) = faux_engine_with_settings(
        &serde_json::json!({ "responses": [{"text": "first"}, {"text": "second"}] }),
        1,
    );
    let engine = std::sync::Arc::new(engine);
    // A probe that reports queued input while the flag is set: the
    // test flips it to simulate the queue draining.
    let queued = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let probe_queued = std::sync::Arc::clone(&queued);
    let goal_work: std::sync::Arc<std::sync::Mutex<Vec<crate::engine::GoalTurnEndWork>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = std::sync::Arc::clone(&goal_work);
    engine.set_goal_admission(
        std::sync::Arc::new(move || probe_queued.load(std::sync::atomic::Ordering::SeqCst)),
        std::sync::Arc::new(move |work| sink.lock().unwrap().push(work)),
        std::sync::Arc::new(|| {}),
    );
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(
        &engine,
        "/goal ship past the queue".to_string(),
        &mut events,
    );
    assert!(goal_work.lock().unwrap().is_empty());
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 0);
    assert_eq!(engine.goal_state_value()["status"], "active");
    queued.store(false, std::sync::atomic::Ordering::SeqCst);
    let mut after_drain: Vec<EngineEvent> = Vec::new();
    admit(&engine, "the queued work ran".to_string(), &mut after_drain);
    let work = goal_work.lock().unwrap();
    let [crate::engine::GoalTurnEndWork::Continuation(follow_up)] = work.as_slice() else {
        panic!("expected exactly one continuation: {work:?}");
    };
    assert_eq!(
        follow_up.request.custom_message.as_ref().unwrap()["details"]["continuationsUsed"],
        serde_json::json!(1)
    );
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 1);
}

#[test]
fn running_children_owe_the_continuation_and_settle_delivers_it() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    let engine = std::sync::Arc::new(
        AgentSessionEngine::new(AgentEngineConfig {
            cwd: dir.path().to_path_buf(),
            agent_dir: dir.path().join("agent"),
            provider: None,
            model: None,
            api_key: None,
            thinking: None,
            session_dir: None,
            session_file: None,
            faux_script: Some(
                serde_json::json!({ "responses": [{"text": "parent turn reply"}] }).to_string(),
            ),
            supervisor_link: Some(crate::agent_engine::SupervisorLinkConfig {
                socket_path: dir.path().join("dead.sock"),
                active_session_id: "parent-session".to_string(),
                worker_token: "token".to_string(),
            }),
            telemetry_disabled: None,
            cron_store: None,
            queued_steering_probe: None,
        })
        .unwrap(),
    );
    let goal_work = goal_admission_collector(&engine);
    let children = engine.children.clone().expect("children registry");
    // A running child (the test seam): the quiescence gate holds.
    engine.runtime.block_on(async {
        children
            .push_test_child(crate::rlm_children::RlmChildIdentity {
                rlm_child_id: "child-1".to_string(),
                active_session_id: "child-session".to_string(),
                session_id: None,
                session_name: "worker-1".to_string(),
            })
            .await;
    });
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(
        &engine,
        "/goal ship behind the children".to_string(),
        &mut events,
    );
    assert!(goal_work.lock().unwrap().is_empty(), "events: {events:?}");
    assert_eq!(events.last(), Some(&EngineEvent::Done(Ok(()))));
    assert_eq!(engine.goal_state_value()["status"], "active");
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 0);
    let handles = engine
        .goal_runtime
        .lock()
        .unwrap()
        .clone()
        .expect("goal runtime");
    assert!(engine
        .runtime
        .block_on(async { handles.driver.lock().await.owes_continuation() }));
    // No supervisor can acknowledge the cancel. It must not prematurely
    // release the previously owed continuation.
    assert!(!engine
        .runtime
        .block_on(async { children.cancel_child_run("child-1").await }));
    assert!(goal_work.lock().unwrap().is_empty());
    assert!(engine
        .runtime
        .block_on(async { handles.driver.lock().await.owes_continuation() }));
    // Settle the child through the settle funnel: a user cancel only settles
    // a child whose abort is confirmed, and this synthetic child has no live
    // supervisor, so the cancel could never confirm (PR #3680 review; the
    // seam change that fires the funnel lives in #3715). The continuation is
    // owed until the child SETTLES, which is what this drives.
    engine
        .runtime
        .block_on(async { children.settle_test_child("child-session").await });
    for _ in 0..200 {
        if !goal_work.lock().unwrap().is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let work = goal_work.lock().unwrap();
    let [crate::engine::GoalTurnEndWork::Continuation(follow_up)] = work.as_slice() else {
        panic!("expected exactly the owed continuation: {work:?}");
    };
    assert!(follow_up.request.message.contains("[goal: continuation]"));
    assert!(follow_up
        .request
        .message
        .contains("ship behind the children"));
    assert_eq!(
        follow_up.request.custom_message.as_ref().unwrap()["details"]["continuationsUsed"],
        serde_json::json!(1)
    );
    drop(work);
    assert!(!engine
        .runtime
        .block_on(async { handles.driver.lock().await.owes_continuation() }));
    assert_eq!(engine.goal_state_value()["continuationsUsed"], 1);
}

/// The engine session's entries as their persisted wire shapes (the
/// hydrating snapshot: a windowed manager holds only the suffix).
fn engine_session_entries(engine: &AgentSessionEngine) -> Vec<pa_types::session::FileEntry> {
    let guard = engine.session.blocking_lock();
    let core = guard.as_deref().expect("session built");
    let persistence = core.session.shared_persistence();
    engine.runtime.block_on(async {
        let snapshot = persistence.lock().await.history_snapshot();
        snapshot.await.expect("history snapshot")
    })
}

#[test]
fn injected_custom_turn_holds_one_representation() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (engine, _dir) = faux_engine_with_settings(
        &serde_json::json!({ "responses": [{"text": "notice acknowledged"}] }),
        1,
    );
    let notice_text = "[child-exited: no-reply child:lane]";
    let notice = serde_json::json!({
        "role": "custom",
        "customType": "rlm_child_terminal_notice",
        "content": notice_text,
        "display": true,
        "details": {
            "kind": "completed_without_reply",
            "childId": "sub-1",
            "sessionName": "lane",
        },
        "timestamp": crate::util::now_ms(),
    });
    let mut events: Vec<EngineEvent> = Vec::new();
    engine.run_prompt(
        0,
        PromptRequest {
            batch: Vec::new(),
            images: Vec::new(),
            message: notice_text.to_string(),
            source: "user".to_string(),
            agent_message_id: None,
            custom_message: Some(notice),
        },
        &|| false,
        &mut |event| {
            events.push(event);
            true
        },
    );
    let custom_rows: Vec<&Value> = events
        .iter()
        .filter_map(|event| match event {
            EngineEvent::CustomMessage(row) if row["customType"] == "rlm_child_terminal_notice" => {
                Some(row)
            }
            _ => None,
        })
        .collect();
    assert_eq!(custom_rows.len(), 1, "events: {events:?}");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EngineEvent::UserMessage(_))),
        "the injected turn must not emit a user row: {events:?}"
    );
    assert_eq!(
        assistant_texts(&events),
        vec!["notice acknowledged".to_string()],
        "the model turn ran on the notice text: {events:?}"
    );
    let entries = engine_session_entries(&engine);
    let notice_rows = entries
        .iter()
        .filter(|entry| {
            matches!(entry, pa_types::session::FileEntry::CustomMessage { payload, .. }
                if payload.custom_type == "rlm_child_terminal_notice")
        })
        .count();
    assert_eq!(notice_rows, 1, "entries: {entries:?}");
    let user_rows = entries
        .iter()
        .filter(|entry| match entry {
            pa_types::session::FileEntry::Message {
                message: pa_types::session::AgentMessage::User(user),
                ..
            } => user.content.text().contains(notice_text),
            _ => false,
        })
        .count();
    assert_eq!(
        user_rows, 0,
        "the injected turn must not persist a user row: {entries:?}"
    );
    let assistant_rows = entries
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                pa_types::session::FileEntry::Message {
                    message: pa_types::session::AgentMessage::Assistant(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(assistant_rows, 1, "entries: {entries:?}");
}

#[test]
fn goal_start_continuation_holds_one_representation() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (engine, _dir) = faux_engine_with_settings(
        &serde_json::json!({ "responses": [{"text": "goal turn reply"}] }),
        1,
    );
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(
        &engine,
        "/goal land the post-compact continue".to_string(),
        &mut events,
    );
    assert_eq!(engine.goal_state_value()["status"], "active");
    let entries = engine_session_entries(&engine);
    let goal_rows: Vec<String> = entries
        .iter()
        .filter_map(|entry| match entry {
            pa_types::session::FileEntry::CustomMessage { payload, .. } => {
                (payload.custom_type == "goal_context").then(|| payload.content.text())
            }
            _ => None,
        })
        .collect();
    assert_eq!(goal_rows.len(), 1, "entries: {entries:?}");
    let goal_prompt = goal_rows[0].clone();
    let user_rows = entries
        .iter()
        .filter(|entry| match entry {
            pa_types::session::FileEntry::Message {
                message: pa_types::session::AgentMessage::User(user),
                ..
            } => user.content.text().contains(&goal_prompt),
            _ => false,
        })
        .count();
    assert_eq!(
        user_rows, 0,
        "the goal continuation must not persist a duplicate user row: {entries:?}"
    );
    let goal_pair_index = events
        .iter()
        .position(|event| {
            matches!(event, EngineEvent::CustomMessage(row) if row["customType"] == "goal_context")
        })
        .expect("the goal-context row rides the wire");
    let assistant_index = events
        .iter()
        .position(|event| {
            matches!(event, EngineEvent::AssistantMessage(message) if message["content"][0]["text"] == "goal turn reply")
        })
        .expect("the continuation turn settled");
    assert!(
        goal_pair_index < assistant_index,
        "the row precedes the turn it drives: {events:?}"
    );
}

#[test]
fn active_goal_aborted_turn_row_broadcasts_and_goal_accounting_skips_it() {
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::TempDir::new().unwrap();
    let engine = AgentSessionEngine::new(AgentEngineConfig {
        cwd: dir.path().to_path_buf(),
        agent_dir: dir.path().join("agent"),
        provider: None,
        model: None,
        api_key: None,
        thinking: None,
        session_dir: None,
        session_file: None,
        faux_script: Some(
            json!({
                "engine": "faux",
                "responses": [
                    { "text": "goal start reply" },
                    { "text": "held reply", "delayMs": 60000 },
                ],
            })
            .to_string(),
        ),
        supervisor_link: None,
        telemetry_disabled: None,
        cron_store: None,
        queued_steering_probe: None,
    })
    .unwrap();
    let engine = std::sync::Arc::new(engine);
    // No worker owns the queue here: minted continuation work collects
    // instead of running, so the held turn below is the only live one.
    let _goal_work = goal_admission_collector(&engine);
    let mut events: Vec<EngineEvent> = Vec::new();
    admit(
        &engine,
        "/goal land the aborted row accounting".to_string(),
        &mut events,
    );
    // The goal-start turn ran inside the command's prompt; its usage was accounted.
    let before = engine.goal_state_value();
    assert_eq!(before["status"], json!("active"), "state: {before:?}");
    assert!(
        before["tokensUsed"].as_u64().unwrap_or(0) > 0,
        "the goal-start turn's usage accounted: {before:?}"
    );
    // The second turn holds mid-provider-wait; the abort cancels the fetch
    // (the eager funnel) and the turn settles on the aborted row.
    let turn_engine = std::sync::Arc::clone(&engine);
    let turn_events: std::sync::Arc<std::sync::Mutex<Vec<EngineEvent>>> = Arc::default();
    let row_events = std::sync::Arc::clone(&turn_events);
    let turn = std::thread::spawn(move || {
        turn_engine.run_prompt(
            0,
            PromptRequest {
                batch: Vec::new(),
                images: Vec::new(),
                message: "held turn".to_string(),
                source: "user".to_string(),
                agent_message_id: None,
                custom_message: None,
            },
            &|| false,
            &mut |event| {
                row_events.lock().unwrap().push(event);
                true
            },
        );
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let agent = engine.turn_agent.lock().expect("turn agent lock").clone();
        if let Some(agent) = agent {
            let state = engine.runtime.block_on(agent.state());
            if state.is_streaming {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the held turn never started streaming"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    engine.abort_in_flight_turn();
    turn.join().expect("the aborted turn settles");
    let events = turn_events.lock().unwrap();
    let aborted_start = events
        .iter()
        .find_map(|event| match event {
            EngineEvent::AssistantUpdate {
                message,
                stream_event,
            } => message.clone().into_wire().filter(|message| {
                message.get("stopReason").and_then(Value::as_str) == Some("aborted")
                    && stream_event
                        .as_ref()
                        .and_then(|event| event.get("type"))
                        .and_then(Value::as_str)
                        == Some("start")
            }),
            _ => None,
        })
        .expect("the aborted row's start frame broadcast");
    assert_eq!(aborted_start["role"], json!("assistant"));
    assert_eq!(aborted_start["errorMessage"], json!("Request was aborted"));
    let aborted_end = events
        .iter()
        .rev()
        .find_map(|event| match event {
            EngineEvent::AssistantMessage(message)
                if message.get("stopReason").and_then(Value::as_str) == Some("aborted") =>
            {
                Some(message.clone())
            }
            _ => None,
        })
        .expect("the aborted row's settled frame broadcast");
    assert_eq!(aborted_end["role"], json!("assistant"));
    assert_eq!(aborted_end["stopReason"], json!("aborted"));
    assert_eq!(aborted_end["errorMessage"], json!("Request was aborted"));
    assert_eq!(
        aborted_end["content"],
        json!([{ "type": "text", "text": "" }]),
        "the no-partial abort carries empty content"
    );
    assert_eq!(aborted_end["usage"]["totalTokens"], json!(0));
    assert_eq!(aborted_end["usage"]["input"], json!(0));
    assert_eq!(aborted_end["usage"]["output"], json!(0));
    // The goal accounting skipped the aborted row: the goal state the
    // goal-start turn left is unchanged (the wall-clock fields are
    // time-based, so the accounting fields compare).
    let after = engine.goal_state_value();
    assert_eq!(after["status"], json!("active"), "state: {after:?}");
    assert_eq!(after["objective"], before["objective"]);
    assert_eq!(after["tokensUsed"], before["tokensUsed"]);
    assert_eq!(after["continuationsUsed"], before["continuationsUsed"]);
}

#[test]
fn shared_window_goal_seed_matches_persisted_goal_state() {
    let header = || {
        json!({
            "type": "session", "version": 3, "id": "s",
            "timestamp": "2026-01-01T00:00:00.000Z", "cwd": "/w",
        })
    };
    // A message row: `parent: Some(_)` links it into the branch chain;
    // `None` terminates the ancestry at this row (the window walk
    // requires a `None` before the header or the open falls back).
    let message = |id: &str, parent: Option<&str>, role: &str| {
        let mut row = json!({
            "type": "message", "id": id, "parentId": parent,
            "timestamp": "2026-01-01T00:00:01.000Z",
            "message": { "role": role, "content": "hi", "timestamp": 0 },
        });
        if parent.is_none() {
            row.as_object_mut().unwrap().remove("parentId");
        }
        // The window walk requires the assistant message's
        // provider/model pair: an assistant row without them falls back.
        if role == "assistant" {
            let message = row["message"].as_object_mut().unwrap();
            message.insert("provider".into(), json!("faux"));
            message.insert("model".into(), json!("faux-1"));
        }
        row
    };
    let goal_row = |id: &str, parent: &str| {
        json!({
            "type": "custom", "id": id, "parentId": parent,
            "timestamp": "2026-01-01T00:00:02.000Z",
            "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
            "data": {
                "active": true, "status": "active", "goalId": "goal-1",
                "objective": "ship the port", "tokensUsed": 340,
                "timeUsedSeconds": 9, "continuationsUsed": 2,
            },
        })
    };
    // (name, rows, terminated tail, the window MUST serve): the `window_serves`
    // flag is the anti-vacuity gate — a fixture regression that silently falls
    // back fails the open-outcome assertion, not just the goal comparison.
    let classes: Vec<(&str, Vec<serde_json::Value>, bool, bool)> = [
        (
            "windowed_with_goal",
            vec![header(), message("m1", None, "user"), goal_row("g1", "m1")],
            true,
            true,
        ),
        (
            "windowed_no_goal",
            vec![header(), message("m1", None, "user")],
            true,
            true,
        ),
        (
            "windowed_off_branch_goal",
            vec![
                header(),
                // The goal links to a row no chain reaches: the
                // active branch never visits it, so the window's
                // on-path capture skips it exactly like the reference.
                goal_row("g1", "ghost-id"),
                message("m1", None, "user"),
                message("m2", Some("m1"), "assistant"),
            ],
            true,
            true,
        ),
        (
            "fallback_with_goal",
            vec![header(), message("m1", None, "user"), goal_row("g1", "m1")],
            false,
            false,
        ),
        (
            "fallback_no_goal",
            vec![header(), message("m1", None, "user")],
            false,
            false,
        ),
    ]
    .into();
    for (name, rows, terminated, window_serves) in classes {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("session.jsonl");
        let mut content = rows
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        if terminated {
            content.push('\n');
        }
        std::fs::write(&path, content).unwrap();
        let opened = pa_core::session::window::WindowedSessionStore::open(&path);
        let window_served = opened.as_ref().is_ok_and(std::option::Option::is_some);
        assert_eq!(
            window_served, window_serves,
            "goal class {name}: the window-served outcome must match the class (the windowed classes must exercise the WINDOW path, the fallback classes the full reader)"
        );
        // The shared open's extraction: the window's snapshot goal when
        // the window serves, else the loaded store's active-branch scan.
        let shared = match opened {
            Ok(Some(window)) => window.goal_state().cloned(),
            _ => crate::session_store::SessionFile::open(&path)
                .ok()
                .as_ref()
                .and_then(crate::goal_state_persist::goal_state_in_session_file),
        };
        assert_eq!(
            shared,
            crate::goal_state_persist::persisted_goal_state(Some(&path)),
            "goal class {name}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::await_holding_lock)]
async fn owner_bootstrap_actual_same_goal_paused_or_complete_during_callback_denied() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _faux = FAUX_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for status in [
        pa_core::goals::GoalStatus::Paused,
        pa_core::goals::GoalStatus::Complete,
    ] {
        let dir = tempfile::TempDir::new().unwrap();
        let agent_dir = dir.path().join("agent");
        std::fs::create_dir_all(&agent_dir).unwrap();
        let file = dir.path().join("root.jsonl");
        std::fs::write(&file, "fixture").unwrap();
        let mut engine = AgentSessionEngine::new(AgentEngineConfig {
            cwd: dir.path().into(),
            agent_dir,
            provider: None,
            model: None,
            api_key: None,
            thinking: None,
            session_dir: None,
            session_file: None,
            faux_script: Some(r#"{"responses":[{"text":"never"}]}"#.into()),
            supervisor_link: None,
            telemetry_disabled: None,
            cron_store: None,
            queued_steering_probe: None,
        })
        .unwrap();
        let model = engine.resolve_model().unwrap();
        let built = engine.build_session(&model).await.unwrap();
        engine.mirror_goal_runtime(&built).await;
        let mut goal = pa_core::goals::empty_goal_state();
        goal.goal_id = Some("same-goal".into());
        goal.objective = Some("fixture".into());
        goal.status = pa_core::goals::GoalStatus::Active;
        built
            .goal_driver
            .lock()
            .await
            .restore_from_persisted(goal.clone());
        let children = std::sync::Arc::new(crate::rlm_children::SupervisorChildSessions::new(
            std::sync::Arc::new(crate::supervisor_link::SupervisorLink::new(
                std::path::PathBuf::new(),
            )),
            dir.path().into(),
            "parent".into(),
            std::sync::Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                dir.path().into(),
                true,
            )),
            String::new(),
        ));
        children.set_identity(crate::rlm_children::ParentIdentity {
            session_id: Some("root".into()),
            session_file: Some(file.to_string_lossy().into()),
            cwd: Some(dir.path().to_string_lossy().into()),
            ..crate::rlm_children::ParentIdentity::with_default_depth()
        });
        children.configure_gate(&json!({"expectedRevision":0,"policy":{"version":1,"enabled":true,"minActive":5,"maxActive":20,"rootWritesCode":false,"correctiveBudget":2},"task":{"projectId":"p","planId":"p","stepId":"s","taskGeneration":1,"policyRevision":1},"assignments":[]})).await.unwrap();
        let gatefile = format!("{}.orchestration-gate.json", file.display());
        let mut gate: Value = serde_json::from_slice(&std::fs::read(&gatefile).unwrap()).unwrap();
        gate["suspended"] = json!(true);
        gate["attempts"] = json!(2);
        std::fs::write(&gatefile, serde_json::to_vec(&gate).unwrap()).unwrap();
        children.restore_gate();
        engine.children = Some(children.clone());
        let identity = json!({"version":1,"operation":"owner_bootstrap_preflight","rootSessionId":"root","sessionFile":file,"cwd":dir.path(),"expectedGoalId":"same-goal","intendedWork":{"planId":"p","stepId":"s"}});
        let before = SessionEngine::owner_bootstrap(&engine, identity.clone())
            .await
            .unwrap();
        let socket = dir.path().join("source.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let envelope = json!({"planIds":["p"],"requiredLeaves":[{"planId":"p","stepId":"s"}],"launchRevision":4});
        let mut request = identity;
        request["operation"] = json!("owner_bootstrap_commit");
        request["preflightId"] = before["preflightId"].clone();
        request["mode"] = json!("source_bootstrap_only");
        request["operationId"] = json!("op");
        request["expectedGateRevision"] = before["gateRevision"].clone();
        request["expectedAccountingEpoch"] = before["accountingEpoch"].clone();
        request["expectedTask"] = before["task"].clone();
        request["expectedPolicy"] = before["policy"].clone();
        request["expectedAssignments"] = before["configuredAssignments"].clone();
        request["envelope"] = envelope.clone();
        request["pinnedSource"] =
            json!({"socketPath":socket,"token":"fixture-secret","instanceId":"source"});
        request["callback"] = json!({"instanceId":"source","operationId":"op","envelope":envelope,"intendedWork":{"planId":"p","stepId":"s"},"expectedSourceSnapshotRevision":4,"expectedSourceEpoch":0});
        let driver = built.goal_driver.clone();
        let source = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0u8; 8192];
            let received = stream.read(&mut bytes).await.unwrap();
            assert!(
                received > 0,
                "source request must arrive before replacing the goal"
            );
            goal.status = status;
            driver.lock().await.restore_from_persisted(goal);
            let body=json!({"operationId":"op","envelope":envelope,"complete":true,"instanceId":"source","sourceRevision":4,"sourceEpoch":0,"snapshot":{"complete":true,"revision":4,"planIds":["p"],"items":[{"planId":"p","stepId":"s","status":"ready"}]}}).to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        });
        let result = SessionEngine::owner_bootstrap(&engine, request).await;
        source.await.unwrap();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Owner goal changed"));
        assert_eq!(
            engine.current_goal_state().unwrap().goal_id.as_deref(),
            Some("same-goal")
        );
        assert_eq!(engine.current_goal_state().unwrap().status, status);
        let after = children.gate_status().await;
        assert!(after["managementScope"].is_null());
        assert!(after["ownerBootstrapReceipt"].is_null());
        assert_eq!(after["correctiveAttempts"], 2);
        let durable: Value = serde_json::from_slice(&std::fs::read(&gatefile).unwrap()).unwrap();
        assert_eq!(durable["suspended"], true);
        assert_eq!(durable["attempts"], 2);
    }
}
