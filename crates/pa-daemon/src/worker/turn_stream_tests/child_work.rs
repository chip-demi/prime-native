//! A queued child consults its owner before the provider is called.
use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
fn managed_kickoff(permit: Value) -> Value {
    let mut rest = serde_json::Map::new();
    rest.insert("nativeWorkPermit".into(), permit);
    crate::session_commands::custom_message_value(&pa_types::session::CustomMessage {
        custom_type: "agent_message".into(),
        content: pa_types::ai::UserContent::Text("child task".into()),
        display: true,
        details: Some(json!({"id":"kickoff"})),
        timestamp: 1,
        rest,
    })
}
struct ProviderProbe(std::sync::atomic::AtomicUsize);
impl SessionEngine for ProviderProbe {
    fn orchestration_permit_is_current(&self, _permit: &Value) -> bool {
        true
    }
    fn orchestration_gate(
        &self,
        _configure: Option<Value>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Value>> + Send + '_>>
    {
        Box::pin(async { Ok(json!({})) })
    }
    fn run_prompt(
        &self,
        _index: usize,
        _request: PromptRequest,
        _aborted: &dyn Fn() -> bool,
        emit: &mut dyn FnMut(EngineEvent) -> bool,
    ) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        emit(EngineEvent::Done(Ok(())));
    }
    fn run_side_question(
        &self,
        _request: SideQuestionRequest,
        _signal: &pa_agent::abort::AbortSignal,
        _sink: &pa_core::session_engine::side_question::SideQuestionSink,
    ) -> SideQuestionOutcome {
        SideQuestionOutcome::Failed {
            answer: String::new(),
            error: "unsupported".into(),
        }
    }
    fn run_compaction(
        &self,
        _request: CompactionRequest,
        _signal: &pa_agent::abort::AbortSignal,
    ) -> CompactionOutcome {
        CompactionOutcome::Skipped {
            message: "unsupported".into(),
        }
    }
    fn run_branch_summary(
        &self,
        _request: crate::engine::BranchSummaryRequest,
        _signal: &pa_agent::abort::AbortSignal,
    ) -> crate::engine::BranchSummaryOutcome {
        crate::engine::BranchSummaryOutcome::Failed {
            error: "unsupported".into(),
        }
    }
    fn rebuild_session_context(
        &self,
        _entries: Vec<pa_types::session::FileEntry>,
        _reload: pa_core::session_engine::goal_driver::GoalBranchReload,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn changed_owner_snapshot_before_queued_child_provider_holds_item_without_model_call() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("owner.sock");
    let listener = pa_types::platform::transport::bind_transport(&socket)
        .await
        .unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.split();
        writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n").await.unwrap();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(request["command"]["type"], "configure_orchestration_gate");
        assert_eq!(request["command"]["activeSessionId"], "owner");
        let permit = &request["command"]["beginWorkStart"];
        assert_eq!(permit["childActiveSessionId"], "burst-session");
        assert_eq!(permit["expectedMessageId"], "kickoff");
        assert_eq!(permit["admissionGeneration"], "generation");
        entered_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let response = crate::protocol::response_failure(
            request["id"].as_str(),
            "configure_orchestration_gate",
            "gate_changed: Work start accounting changed; revision expected=3 current=4, epoch expected=1 current=1; start_permitted=false",
            None,
        );
        writer
            .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
            .await
            .unwrap();
    });
    let engine = Arc::new(ProviderProbe(std::sync::atomic::AtomicUsize::new(0)));
    let mut runner = burst_runner(engine.clone());
    runner.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    runner.core.lock().unwrap().follow_up.push_back(QueuedItem {message:"child task".into(),priority:QueuePriority::Background,preview:None,custom_message:Some(managed_kickoff(json!({"ownerActiveSessionId":"owner","sourceInstanceId":"source","childId":"child","childActiveSessionId":"burst-session","childSessionId":"durable","admissionGeneration":"generation","expectedMessageId":"kickoff","planId":"p","stepId":"d","startToken":"private"}))),agent_message:None,input_identity:None,queue_key:None,admission_id:None,images:Vec::new(),done:None,queue_visible:false,policy:TurnPolicy::Injected,forced_batch:false});
    let core = runner.core.clone();
    let idle = runner.idle_notify.clone();
    let running = tokio::spawn(runner.run());
    entered_rx.await.unwrap();
    let suspended = idle.notified();
    release_tx.send(()).unwrap();
    suspended.await;
    {
        let core = core.lock().unwrap();
        assert!(core.queued_input_suspended);
        assert_eq!(core.follow_up.len(), 1);
    }
    assert_eq!(engine.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    core.lock().unwrap().shutdown_requested = true;
    running.abort();
    server.await.unwrap();
}

#[tokio::test]
async fn validated_managed_start_survives_private_permit_strip_in_real_queue_path() {
    struct InspectProvider(std::sync::atomic::AtomicBool);
    impl SessionEngine for InspectProvider {
        fn run_prompt(
            &self,
            _index: usize,
            request: PromptRequest,
            _aborted: &dyn Fn() -> bool,
            emit: &mut dyn FnMut(EngineEvent) -> bool,
        ) {
            assert!(request
                .custom_message
                .as_ref()
                .unwrap()
                .get("nativeWorkPermit")
                .is_none());
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            emit(EngineEvent::Done(Ok(())));
        }
        fn run_side_question(
            &self,
            _: SideQuestionRequest,
            _: &pa_agent::abort::AbortSignal,
            _: &pa_core::session_engine::side_question::SideQuestionSink,
        ) -> SideQuestionOutcome {
            SideQuestionOutcome::Failed {
                answer: String::new(),
                error: "unsupported".into(),
            }
        }
        fn run_compaction(
            &self,
            _: CompactionRequest,
            _: &pa_agent::abort::AbortSignal,
        ) -> CompactionOutcome {
            CompactionOutcome::Skipped {
                message: "unsupported".into(),
            }
        }
        fn run_branch_summary(
            &self,
            _: crate::engine::BranchSummaryRequest,
            _: &pa_agent::abort::AbortSignal,
        ) -> crate::engine::BranchSummaryOutcome {
            crate::engine::BranchSummaryOutcome::Failed {
                error: "unsupported".into(),
            }
        }
        fn rebuild_session_context(
            &self,
            _: Vec<pa_types::session::FileEntry>,
            _: pa_core::session_engine::goal_driver::GoalBranchReload,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("owner.sock");
    let listener = pa_types::platform::transport::bind_transport(&socket)
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.split();
        writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n").await.unwrap();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            request["command"]["beginWorkStart"]["expectedMessageId"],
            "kickoff"
        );
        let response = crate::protocol::response_success(
            request["id"].as_str(),
            "configure_orchestration_gate",
            Some(json!({"workStartPermitted":true,"managedResultTracking":true})),
        );
        writer
            .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
            .await
            .unwrap();
    });
    let engine = Arc::new(InspectProvider(std::sync::atomic::AtomicBool::new(false)));
    let mut runner = burst_runner(engine.clone());
    runner.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    let path = dir.path().join("worker.jsonl");
    runner.recovery = Arc::new(Mutex::new(Some(
        crate::journal::WorkerRecoveryJournal::open(&path).unwrap(),
    )));
    runner.core.lock().unwrap().follow_up.push_back(QueuedItem{message:"child task".into(),priority:QueuePriority::Background,preview:None,custom_message:Some(managed_kickoff(json!({"ownerActiveSessionId":"owner","childId":"child","childActiveSessionId":"burst-session","admissionGeneration":"generation","expectedMessageId":"kickoff","startToken":"private"}))),agent_message:None,input_identity:None,queue_key:None,admission_id:None,images:Vec::new(),done:None,queue_visible:false,policy:TurnPolicy::Injected,forced_batch:false});
    let core = runner.core.clone();
    let idle = runner.idle_notify.clone();
    let settled = idle.notified();
    tokio::pin!(settled);
    settled.as_mut().enable();
    let running = tokio::spawn(runner.run());
    settled.await;
    assert!(engine.0.load(std::sync::atomic::Ordering::SeqCst));
    let facts = crate::journal::WorkerRecoveryJournal::read_message_facts(&path).unwrap();
    let fact = facts
        .get(&("burst-session".into(), "kickoff".into()))
        .unwrap();
    assert_eq!(fact.phase, "started");
    assert_eq!(fact.admission_generation.as_deref(), Some("generation"));
    core.lock().unwrap().shutdown_requested = true;
    running.abort();
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_managed_notice_never_reaches_provider_or_delivery_claim() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(ProviderProbe(std::sync::atomic::AtomicUsize::new(0)));
    let runner = burst_runner(engine.clone());
    let recovery = runner.recovery.clone();
    *recovery.lock().unwrap() = Some(
        crate::journal::WorkerRecoveryJournal::open(&dir.path().join("worker.jsonl")).unwrap(),
    );
    let item = QueuedItem {
        message: "notice".into(),
        priority: QueuePriority::Background,
        preview: None,
        custom_message: Some(
            json!({"role":"custom","customType":"rlm_child_terminal_notice","content":"notice","display":true,"details":{"childId":"A","terminalDeliveryId":"t","nativeManagedResult":true}}),
        ),
        agent_message: None,
        input_identity: None,
        queue_key: None,
        admission_id: None,
        images: Vec::new(),
        done: None,
        queue_visible: false,
        policy: TurnPolicy::Injected,
        forced_batch: false,
    };
    runner
        .run_turn(
            engine.clone(),
            crate::worker::queue::Lane::FollowUp,
            vec![item],
        )
        .await;
    assert_eq!(engine.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(recovery
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .message_facts("burst-session")
        .is_empty());
}

#[tokio::test]
async fn typed_gate_control_runs_through_real_queue_preflight() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(ProviderProbe(std::sync::atomic::AtomicUsize::new(0)));
    let runner = burst_runner(engine.clone());
    *runner.recovery.lock().unwrap() = Some(
        crate::journal::WorkerRecoveryJournal::open(&dir.path().join("worker.jsonl")).unwrap(),
    );
    crate::worker::queue::admit_gate_follow_up(
        &runner.recovery,
        &runner.core,
        &runner.work_notify,
        "control".into(),
        json!({"revision":1}),
    );
    let idle = runner.idle_notify.clone();
    let core = runner.core.clone();
    let settled = idle.notified();
    tokio::pin!(settled);
    settled.as_mut().enable();
    let running = tokio::spawn(runner.run());
    settled.await;
    assert_eq!(engine.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    core.lock().unwrap().shutdown_requested = true;
    running.abort();
}

#[test]
fn start_denial_diagnostic_preserves_safe_fences_not_untrusted_payload() {
    let reason = "gate_changed: Work start accounting changed; revision expected=3 current=4, epoch expected=1 current=1; start_permitted=false";
    assert_eq!(TurnRunner::start_denial_diagnostic(Some(reason)), reason);
    assert!(
        !TurnRunner::start_denial_diagnostic(Some("token=SECRET rawpayload")).contains("SECRET")
    );
}

/// Interrupt the actual turn boundary without relying on wall-clock races.
struct ShutdownProbe {
    core: Mutex<Option<Arc<Mutex<SessionCore>>>>,
    accept_input: bool,
    persisted: std::sync::atomic::AtomicBool,
}
impl SessionEngine for ShutdownProbe {
    fn run_prompt(
        &self,
        _: usize,
        request: PromptRequest,
        _: &dyn Fn() -> bool,
        emit: &mut dyn FnMut(EngineEvent) -> bool,
    ) {
        let row = request.custom_message.unwrap();
        assert!(row.get("nativeWorkPermit").is_none());
        if self.accept_input {
            self.persisted.store(
                emit(EngineEvent::CustomMessage(row)),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        let held = self.core.lock().unwrap();
        let mut core = held.as_ref().unwrap().lock().unwrap();
        core.shutdown_requested = true;
        core.abort_requested = true;
    }
    fn run_side_question(
        &self,
        request: SideQuestionRequest,
        signal: &pa_agent::abort::AbortSignal,
        sink: &pa_core::session_engine::side_question::SideQuestionSink,
    ) -> SideQuestionOutcome {
        ProviderProbe(std::sync::atomic::AtomicUsize::new(0))
            .run_side_question(request, signal, sink)
    }
    fn run_compaction(
        &self,
        request: CompactionRequest,
        signal: &pa_agent::abort::AbortSignal,
    ) -> CompactionOutcome {
        ProviderProbe(std::sync::atomic::AtomicUsize::new(0)).run_compaction(request, signal)
    }
    fn run_branch_summary(
        &self,
        request: crate::engine::BranchSummaryRequest,
        signal: &pa_agent::abort::AbortSignal,
    ) -> crate::engine::BranchSummaryOutcome {
        ProviderProbe(std::sync::atomic::AtomicUsize::new(0)).run_branch_summary(request, signal)
    }
    fn rebuild_session_context(
        &self,
        _: Vec<pa_types::session::FileEntry>,
        _: pa_core::session_engine::goal_driver::GoalBranchReload,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_preserves_managed_queue_metadata_and_parks_accepted_managed_turns() {
    for accepted in [false, true] {
        for terminal_result in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = Arc::new(ShutdownProbe {
                core: Mutex::new(None),
                accept_input: accepted,
                persisted: std::sync::atomic::AtomicBool::new(false),
            });
            let runner = burst_runner(engine.clone());
            *engine.core.lock().unwrap() = Some(runner.core.clone());
            let journal_path = dir.path().join("worker.jsonl");
            *runner.recovery.lock().unwrap() =
                Some(crate::journal::WorkerRecoveryJournal::open(&journal_path).unwrap());
            let mut store = SessionFile::create("/fixture", None, 0);
            store.path = dir.path().join("session.jsonl");
            store.rewrite().unwrap();
            {
                let mut core = runner.core.lock().unwrap();
                core.store = Some(store);
                core.busy = true;
            }
            let mut custom = managed_kickoff(json!({
                "startToken":"private", "expectedMessageId":"kickoff",
                "admissionGeneration":"generation"
            }));
            if terminal_result {
                custom.as_object_mut().unwrap().remove("nativeWorkPermit");
                custom["details"]["nativeManagedResult"] = json!(true);
            }
            let item = QueuedItem {
                priority: QueuePriority::Background,
                preview: None,
                message: "child task".into(),
                custom_message: Some(custom.clone()),
                agent_message: None,
                input_identity: None,
                queue_key: None,
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: false,
                policy: TurnPolicy::Injected,
                forced_batch: false,
            };
            runner
                .run_turn(
                    engine.clone(),
                    crate::worker::queue::Lane::FollowUp,
                    vec![item],
                )
                .await;
            let core = runner.core.lock().unwrap();
            assert!(
                core.queued_input_suspended,
                "accepted={accepted} result={terminal_result}"
            );
            assert!(!core.turn_completed);
            assert_eq!(core.follow_up.len(), usize::from(!accepted));
            assert_eq!(
                core.follow_up
                    .front()
                    .and_then(|item| item.custom_message.as_ref()),
                (!accepted).then_some(&custom)
            );
            assert!(core.steering.is_empty());
            assert_eq!(
                engine.persisted.load(std::sync::atomic::Ordering::SeqCst),
                accepted
            );
            drop(core);
            let (_, restored, suspended) =
                crate::journal::WorkerRecoveryJournal::read_queue_snapshot(
                    &journal_path,
                    "burst-session",
                )
                .unwrap()
                .unwrap();
            assert!(suspended);
            assert_eq!(restored.len(), usize::from(!accepted));
        }
    }
}

#[tokio::test]
async fn custom_persistence_failure_does_not_consume_shutdown_replay_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(ShutdownProbe {
        core: Mutex::new(None),
        accept_input: true,
        persisted: std::sync::atomic::AtomicBool::new(false),
    });
    let runner = burst_runner(engine.clone());
    *engine.core.lock().unwrap() = Some(runner.core.clone());
    let mut store = SessionFile::create("/fixture", None, 0);
    // A directory cannot accept a durable row; this fails under root as well.
    store.path = dir.path().to_path_buf();
    runner.core.lock().unwrap().store = Some(store);
    let custom = managed_kickoff(json!({"startToken":"private"}));
    let item = QueuedItem {
        priority: QueuePriority::Background,
        preview: None,
        message: "child task".into(),
        custom_message: Some(custom.clone()),
        agent_message: None,
        input_identity: None,
        queue_key: None,
        admission_id: None,
        images: Vec::new(),
        done: None,
        queue_visible: false,
        policy: TurnPolicy::Injected,
        forced_batch: false,
    };
    runner
        .run_turn(
            engine.clone(),
            crate::worker::queue::Lane::FollowUp,
            vec![item],
        )
        .await;
    assert!(!engine.persisted.load(std::sync::atomic::Ordering::SeqCst));
    let core = runner.core.lock().unwrap();
    assert!(
        core.shutdown_requested,
        "probe reached the persistence boundary"
    );
    assert!(core.queued_input_suspended);
    assert_eq!(
        core.follow_up
            .front()
            .and_then(|item| item.custom_message.as_ref()),
        Some(&custom)
    );
}
