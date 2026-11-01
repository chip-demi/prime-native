//! The turn runner's stream tests.
use super::*;
use crate::engine::{
    CompactionOutcome, CompactionRequest, PromptRequest, SessionEngine, SideQuestionOutcome,
    SideQuestionRequest,
};

// The shared fixtures stay here; the child modules reach them
// through `use super::*`.
mod abort_idle_race;
mod broadcast;
mod burst;
mod child_work;
mod feed;
mod interleave;
mod park;
mod provenance;
mod queue;

/// A minimal turn runner over a fresh session core: exactly what `run_turn` touches.
fn burst_runner(engine: Arc<dyn SessionEngine>) -> TurnRunner {
    // Managed admission checkpoints to a real journal in production. Keep the
    // positive stream fixture equally durable; missing-journal denial has its
    // own explicit guard tests.
    let recovery_dir =
        std::env::temp_dir().join(format!("pa-burst-recovery-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&recovery_dir).unwrap();
    let recovery = WorkerRecoveryJournal::open(&recovery_dir.join("recovery.jsonl")).unwrap();
    let core = Arc::new(Mutex::new(SessionCore {
        active_session_id: "burst-session".to_string(),
        generation: "gen".to_string(),
        last_event_sequence: 0,
        store: None,
        cwd: String::new(),
        steering: VecDeque::new(),
        follow_up: VecDeque::new(),
        turn_serial: 0,
        busy: false,
        created: false,
        attached_client_ids: Vec::new(),
        abort_requested: false,
        turn_cancelled: false,
        turn_completed: false,
        suppress_aborted_row: false,
        shutdown_requested: false,
        compacting: false,
        auto_compaction_enabled: true,
        last_activity_ms: 0,
        last_action_snapshot: Some(SessionActionSnapshot::default()),
        rlm_depth: 0,
        runtime_kind: "top-level".to_string(),
        rlm_child_id: None,
        parent_active_session_id: None,
        parent_session_id: None,
        child_script: None,
        service_tier: None,
        active_service_tier: None,
        steering_mode: "all".to_string(),
        follow_up_mode: "one-at-a-time".to_string(),
        forced_all_steering: false,
        scoped_models: Vec::new(),
        retry_abort_requested: false,
        queued_input_suspended: false,
        pending_next_turn: Vec::new(),
        active_action: None,
        running_tool_calls: std::collections::HashSet::new(),
        running_admission_ids: std::collections::HashSet::new(),
        tool_use_count: 0,
    }));
    TurnRunner {
        core,
        input_pauses: crate::session_input_pause::InputPauseTable::new(),
        prompt_admissions: crate::prompt_admission::WorkerAdmissions::new(),
        work_notify: Arc::new(Notify::new()),
        idle_notify: Arc::new(Notify::new()),
        events: Arc::new(EventPump::new()),
        engine,
        recovery: Arc::new(Mutex::new(Some(recovery))),
        active_session_id: "burst-session".to_string(),
        roster_pushes: crate::roster_activity::RosterPushQueue::disabled(),
        user_bash: std::sync::Arc::new(crate::user_bash::UserBash::new()),
        passivation: crate::worker::turn::PassivationContext {
            agent_dir: std::path::PathBuf::from("/tmp"),
            link: std::sync::Arc::new(crate::supervisor_link::SupervisorLink::new(
                std::path::PathBuf::from("/nonexistent-supervisor.sock"),
            )),
            worker_token: String::new(),
        },
        herdr: std::sync::Arc::new(std::sync::Mutex::new(crate::herdr::HerdrReporter::default())),
    }
}

#[tokio::test]
async fn session_quarantine_denies_already_queued_goal_or_cron_turn_start() {
    let dir = tempfile::TempDir::new().unwrap();
    let file = dir.path().join("s.jsonl");
    let engine: Arc<dyn SessionEngine> = Arc::new(GateProbeEngine {
        frames: Vec::new(),
        boundaries: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let mut runner = burst_runner(engine.clone());
    runner.passivation.agent_dir = dir.path().join("agent");
    let mut store = SessionFile::create("/fixture", None, 0);
    store.path = file.clone();
    runner.core.lock().unwrap().store = Some(store);
    std::fs::write(&file, "preserved").unwrap();
    crate::lease::persist_session_quarantine(
        &runner.passivation.agent_dir,
        &crate::lease::SessionQuarantine {
            version: 1,
            parent_session_file: dir.path().join("p.jsonl").to_string_lossy().into(),
            child_id: "c".into(),
            child_session_file: file.to_string_lossy().into(),
            task: json!({}),
            assignment: json!({}),
            current_admission_generation: None,
            current_expected_message_id: None,
        },
    )
    .unwrap();
    let mut events = runner.events.subscribe();
    runner
        .run_turn(
            engine,
            crate::worker::queue::Lane::FollowUp,
            vec![QueuedItem {
                priority: QueuePriority::Background,
                preview: None,
                message: "scheduled".into(),
                custom_message: None,
                agent_message: None,
                input_identity: None,
                queue_key: Some("goal-or-cron".into()),
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: TurnPolicy::Queued,
                forced_batch: false,
            }],
        )
        .await;
    assert!(
        events.try_recv().is_err(),
        "No agent_start/provider events under quarantine"
    );
    let core = runner.core.lock().unwrap();
    assert!(core.queued_input_suspended && !core.busy);
    assert_eq!(core.follow_up.len(), 1);
    assert_eq!(std::fs::read_to_string(file).unwrap(), "preserved");
}

async fn turn_session_events(engine: Arc<dyn SessionEngine>) -> Vec<Value> {
    let runner = burst_runner(Arc::clone(&engine));
    let mut subscription = runner.events.subscribe();
    runner
        .run_turn(
            engine,
            Lane::Steering,
            vec![QueuedItem {
                priority: QueuePriority::Human,
                preview: None,
                message: "burst".to_string(),
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
            }],
        )
        .await;
    let mut events = Vec::new();
    while let Ok(frame) = subscription.try_recv() {
        if frame.outbound_type == "session_event" {
            if let Ok(outbound) = serde_json::from_slice::<Value>(&frame.payload) {
                events.push(outbound["event"].clone());
            }
        }
    }
    events
}

fn positions_of(events: &[Value], frame_type: &str) -> Vec<usize> {
    events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.get("type").and_then(Value::as_str) == Some(frame_type))
        .map(|(index, _)| index)
        .collect()
}

// The abort gate's arm semantics, pinned on the wire: `run_turn` driven
// directly. Admission starts un-aborted; the probe arms the late sighting
// only inside run_prompt, so the final-emit race is deterministic.
struct ProbeAbort {
    core: Arc<Mutex<SessionCore>>,
    abort_requested: bool,
    suppress_aborted_row: bool,
}

#[derive(Default)]
struct GateProbeEngine {
    frames: Vec<EngineEvent>,
    background: std::sync::atomic::AtomicBool,
    continuation_pending: std::sync::atomic::AtomicBool,
    boundaries: Mutex<Vec<Option<String>>>,
    abort_on_start: Mutex<Option<ProbeAbort>>,
    turns_done: std::sync::atomic::AtomicUsize,
}

impl SessionEngine for GateProbeEngine {
    fn settlement_continuation_pending(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(std::future::ready(
            self.continuation_pending
                .load(std::sync::atomic::Ordering::SeqCst),
        ))
    }

    fn has_background_bash(&self) -> bool {
        self.background.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn set_prompt_context_boundary(&self, leaf_id: Option<String>) {
        self.boundaries.lock().unwrap().push(leaf_id);
    }

    fn on_turn_done(&self) {
        self.turns_done
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn run_prompt(
        &self,
        _prompt_index: usize,
        _request: PromptRequest,
        aborted: &dyn Fn() -> bool,
        emit: &mut dyn FnMut(EngineEvent) -> bool,
    ) {
        if let Some(arm) = self.abort_on_start.lock().unwrap().take() {
            assert!(
                !aborted(),
                "the scripted sighting occurs after positive admission"
            );
            let mut core = arm.core.lock().unwrap();
            core.abort_requested = arm.abort_requested;
            core.suppress_aborted_row = arm.suppress_aborted_row;
        }
        for frame in &self.frames {
            if !emit(frame.clone()) {
                return;
            }
        }
    }

    fn run_side_question(
        &self,
        _request: SideQuestionRequest,
        _signal: &pa_agent::abort::AbortSignal,
        _sink: &pa_core::session_engine::side_question::SideQuestionSink,
    ) -> SideQuestionOutcome {
        SideQuestionOutcome::Failed {
            answer: String::new(),
            error: "unsupported".to_string(),
        }
    }

    fn run_compaction(
        &self,
        _request: CompactionRequest,
        _signal: &pa_agent::abort::AbortSignal,
    ) -> CompactionOutcome {
        CompactionOutcome::Skipped {
            message: "nothing to compact".to_string(),
        }
    }

    fn run_branch_summary(
        &self,
        _request: crate::engine::BranchSummaryRequest,
        _signal: &pa_agent::abort::AbortSignal,
    ) -> crate::engine::BranchSummaryOutcome {
        crate::engine::BranchSummaryOutcome::Failed {
            error: "unsupported".to_string(),
        }
    }

    fn rebuild_session_context(
        &self,
        _branch_entries: Vec<pa_types::session::FileEntry>,
        _goal_reload: pa_core::session_engine::goal_driver::GoalBranchReload,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// One gated sighting battery: the scripted frames through the worker's turn with
/// the abort flag (and suppressed-row class) armed after positive admission.
async fn gate_sighting_events(
    frames: Vec<EngineEvent>,
    abort_requested: bool,
    suppress_aborted_row: bool,
) -> Vec<Value> {
    let probe = Arc::new(GateProbeEngine {
        frames,
        boundaries: Mutex::new(Vec::new()),
        ..Default::default()
    });
    let engine: Arc<dyn SessionEngine> = probe.clone();
    let runner = burst_runner(Arc::clone(&engine));
    *probe.abort_on_start.lock().unwrap() = Some(ProbeAbort {
        core: Arc::clone(&runner.core),
        abort_requested,
        suppress_aborted_row,
    });
    let mut subscription = runner.events.subscribe();
    runner
        .run_turn(
            engine,
            Lane::Steering,
            vec![QueuedItem {
                priority: QueuePriority::Human,
                preview: None,
                message: "gate".to_string(),
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
            }],
        )
        .await;
    assert!(
        probe.abort_on_start.lock().unwrap().is_none(),
        "the scripted engine must be entered after admission"
    );
    let mut events = Vec::new();
    while let Ok(frame) = subscription.try_recv() {
        if frame.outbound_type == "session_event" {
            if let Ok(outbound) = serde_json::from_slice::<Value>(&frame.payload) {
                events.push(outbound["event"].clone());
            }
        }
    }
    events
}

/// A sighting on the trailing `Done` of a self-completed run must not arm the
/// fallback's silence — the closer still pairs the run's `agent_start`.
#[tokio::test]
async fn a_late_abort_sighting_on_a_completed_runs_done_keeps_the_fallback_closer() {
    let events = gate_sighting_events(vec![EngineEvent::Done(Ok(()))], true, false).await;
    let starts = positions_of(&events, "agent_start");
    let ends = positions_of(&events, "agent_end");
    assert_eq!(starts.len(), 1, "the run opens one agent_start: {events:?}");
    assert_eq!(
        ends.len(),
        1,
        "the fallback closer pairs the opening agent_start: {events:?}"
    );
    assert!(
        ends[0] > starts[0],
        "the closer lands after the open: {events:?}"
    );
}

/// `DoneAborted` with no engine `agent_end`: the aborted-outcome carrier arms the
/// silence.
#[tokio::test]
async fn done_aborted_arms_the_fallback_silence() {
    let events = gate_sighting_events(vec![EngineEvent::DoneAborted], true, false).await;
    assert!(
        positions_of(&events, "agent_end").is_empty(),
        "the aborted settle keeps the suppressed-run silence: {events:?}"
    );
}

/// A turn aborted under row suppression (a manual compaction or tree move)
/// still reaches the engine's turn boundary at its swallowed `Done`, so
/// child prompts held for that turn are released.
#[tokio::test]
async fn a_suppressed_abort_still_ends_the_engine_turn() {
    for done in [EngineEvent::Done(Ok(())), EngineEvent::DoneAborted] {
        let engine = Arc::new(GateProbeEngine {
            frames: vec![done],
            ..GateProbeEngine::default()
        });
        let dynamic: Arc<dyn SessionEngine> = engine.clone();
        let runner = burst_runner(Arc::clone(&dynamic));
        {
            let mut core = runner.core.lock().unwrap();
            core.abort_requested = true;
            core.suppress_aborted_row = true;
        }
        runner
            .run_turn(
                dynamic,
                Lane::Steering,
                vec![QueuedItem {
                    priority: QueuePriority::Human,
                    preview: None,
                    message: "aborted".to_string(),
                    input_identity: None,
                    custom_message: None,
                    agent_message: None,
                    queue_key: None,
                    admission_id: None,
                    images: Vec::new(),
                    done: None,
                    queue_visible: true,
                    policy: TurnPolicy::Queued,
                    forced_batch: false,
                }],
            )
            .await;
        assert_eq!(
            engine.turns_done.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}

/// #4: the turn's tool executions count onto the session's `toolUseCount`
/// (TS bumps the child's counter per `tool_execution_start`), the value the
/// worker's roster deltas carry.
#[tokio::test]
async fn a_turns_tool_executions_count_onto_the_summary() {
    let engine: Arc<dyn SessionEngine> = Arc::new(GateProbeEngine {
        frames: vec![
            EngineEvent::ToolExecutionStart {
                tool_call_id: "t1".to_string(),
                tool_name: "bash".to_string(),
                args: Value::Null,
            },
            EngineEvent::ToolExecutionEnd {
                tool_call_id: "t1".to_string(),
                result: Value::Null,
                is_error: false,
            },
            EngineEvent::ToolExecutionStart {
                tool_call_id: "t2".to_string(),
                tool_name: "edit".to_string(),
                args: Value::Null,
            },
            EngineEvent::Done(Ok(())),
        ],
        ..GateProbeEngine::default()
    });
    let runner = burst_runner(Arc::clone(&engine));
    runner
        .run_turn(
            engine,
            Lane::Steering,
            vec![QueuedItem {
                priority: QueuePriority::Human,
                preview: None,
                message: "tools".to_string(),
                custom_message: None,
                agent_message: None,
                queue_key: None,
                admission_id: None,
                input_identity: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: TurnPolicy::Queued,
                forced_batch: false,
            }],
        )
        .await;
    let summary = {
        let core = runner.core.lock().unwrap();
        session_summary(
            &core,
            SummaryInputs {
                thinking_level: "default".to_string(),
                model: None,
                model_fallback_message: None,
                bash_running: false,
                background_bash: false,
                quota_parked: false,
                subagents_running: false,
                progress_note: None,
            },
        )
    };
    let summary = serde_json::to_value(&summary).unwrap();
    assert_eq!(
        summary["toolUseCount"],
        json!(2),
        "the summary: {summary:?}"
    );
}

/// A settle frame that would forward on a plain flag sighting drops when
/// `suppress_aborted_row` is set.
#[tokio::test]
async fn the_suppressed_row_sighting_drops_and_arms_the_silence() {
    let events = gate_sighting_events(
        vec![
            EngineEvent::ToolResultMessage(json!({
                "role": "toolResult",
                "text": "the aborted tool's error result",
            })),
            EngineEvent::Done(Ok(())),
        ],
        true,
        true,
    )
    .await;
    assert!(
        positions_of(&events, "message_start").is_empty()
            && positions_of(&events, "message_end").is_empty(),
        "the suppressed settle frame never reaches the wire: {events:?}"
    );
    assert!(
        positions_of(&events, "agent_end").is_empty(),
        "the suppressed-row sighting arms the silence: {events:?}"
    );
}

/// Engine-only stores cannot persist these transitions: the worker must
/// commit both rows in the same stream order as the assistant/tool rows.
#[tokio::test]
async fn model_transitions_reach_the_worker_owned_session_file() {
    for abort_suppressed in [None, Some(false), Some(true)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        let expected = vec![
            json!({"provider": "backup", "modelId": "model/with/slashes"}),
            json!({"provider": "primary", "modelId": "model/with/slashes"}),
        ];
        let mut frames = vec![
            EngineEvent::ModelChange(pa_types::session::ModelChangeEntry {
                provider: "backup".to_string(),
                model_id: "model/with/slashes".to_string(),
            }),
            EngineEvent::AssistantMessage(
                json!({"role": "assistant", "content": [], "stopReason": "toolUse"}),
            ),
            EngineEvent::ModelChange(pa_types::session::ModelChangeEntry {
                provider: "primary".to_string(),
                model_id: "model/with/slashes".to_string(),
            }),
            EngineEvent::ToolResultMessage(
                json!({"role": "toolResult", "toolCallId": "probe", "content": []}),
            ),
            EngineEvent::Done(Ok(())),
        ];
        if abort_suppressed.is_some() {
            frames.retain(|event| {
                matches!(event, EngineEvent::ModelChange(_) | EngineEvent::Done(_))
            });
        }
        let probe = Arc::new(GateProbeEngine {
            frames,
            boundaries: Mutex::new(Vec::new()),
            ..Default::default()
        });
        let engine: Arc<dyn SessionEngine> = probe.clone();
        let runner = burst_runner(Arc::clone(&engine));
        let mut store = crate::session_store::SessionFile::create("/fixture", None, 0);
        store.set_path(path.clone());
        store.rewrite().unwrap();
        {
            let mut core = runner.core.lock().unwrap();
            core.store = Some(store);
        }
        if let Some(suppress_aborted_row) = abort_suppressed {
            *probe.abort_on_start.lock().unwrap() = Some(ProbeAbort {
                core: Arc::clone(&runner.core),
                abort_requested: true,
                suppress_aborted_row,
            });
        }
        runner
            .run_turn(
                engine,
                Lane::Steering,
                vec![QueuedItem {
                    priority: QueuePriority::Human,
                    preview: None,
                    message: "probe".to_string(),
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
                }],
            )
            .await;
        assert!(
            probe.abort_on_start.lock().unwrap().is_none(),
            "model frames must be emitted after positive admission"
        );
        let rows: Vec<Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let transitions: Vec<Value> = rows
            .iter()
            .filter(|row| row["type"] == "model_change")
            .map(|row| json!({"provider": row["provider"], "modelId": row["modelId"]}))
            .collect();
        assert_eq!(transitions, expected);
        let relevant: Vec<&str> = rows
            .iter()
            .filter_map(|row| match row["type"].as_str() {
                Some("model_change") => Some("model"),
                Some("message") => row["message"]["role"].as_str(),
                _ => None,
            })
            .collect();
        assert_eq!(
            relevant,
            if abort_suppressed.is_some() {
                vec!["model", "model"]
            } else {
                vec!["model", "assistant", "model", "toolResult"]
            }
        );
    }
}

/// noSession keeps its prefix IDs in the worker's in-memory store; they
/// cannot fence the engine's durable restore. File-backed prompts capture the
/// leaf after next-turn prefix rows have persisted.
#[tokio::test]
async fn prompt_restore_boundary_is_supplied_only_for_file_backed_stores() {
    let dir = tempfile::tempdir().unwrap();
    for file_backed in [false, true] {
        let engine = Arc::new(GateProbeEngine {
            frames: vec![EngineEvent::Done(Ok(()))],
            boundaries: Mutex::new(Vec::new()),
            ..Default::default()
        });
        let dynamic: Arc<dyn SessionEngine> = engine.clone();
        let runner = burst_runner(Arc::clone(&dynamic));
        let mut store = crate::session_store::SessionFile::create("/tmp", None, 0);
        store.append_session_info("prefix");
        assert!(store.leaf_id().is_some(), "in-memory prefixes have IDs too");
        if file_backed {
            store.set_path(dir.path().join("backed.jsonl"));
            store.rewrite().unwrap();
        }
        {
            let mut core = runner.core.lock().unwrap();
            core.store = Some(store);
            core.pending_next_turn.push(json!({
                "role":"custom", "customType":"prior_context", "content":"keep this prefix",
                "display":false,
            }));
        }
        runner
            .run_turn(
                dynamic,
                Lane::Steering,
                vec![QueuedItem {
                    priority: QueuePriority::Human,
                    preview: None,
                    message: "go".to_string(),
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
                }],
            )
            .await;
        let expected = if file_backed {
            let path = dir.path().join("backed.jsonl");
            let persisted = crate::session_store::SessionFile::open(&path).unwrap();
            assert!(
                std::fs::read_to_string(&path)
                    .unwrap()
                    .contains("prior_context"),
                "the captured boundary follows the prefix on disk"
            );
            vec![persisted.leaf_id().map(str::to_string)]
        } else {
            assert!(!dir.path().join("backed.jsonl").exists());
            Vec::new()
        };
        assert_eq!(
            *engine.boundaries.lock().unwrap(),
            expected,
            "boundary capture must match durable ownership (file_backed={file_backed})"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn session_completion_settle_duplicate_recovery_and_retask() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("supervisor.sock");
    let (commands, server) = super::tests::fake_supervisor(&socket);
    let engine = Arc::new(GateProbeEngine::default());
    let mut runner = burst_runner(engine.clone());
    runner.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    let mut store = SessionFile::create("/fixture", None, 0);
    store.set_path(dir.path().join("worker.jsonl"));
    store.rewrite().unwrap();
    let assignment = json!({"assignmentId":"task-1", "artifactRef":"result.md",
        "creatorSessionId":"creator", "creatorSessionFile":"creator.jsonl", "deliveryId":"delivery-1"});
    store
        .persist_entry(
            "custom_message",
            json!({"customType":"rlm_session_assignment", "details":assignment}),
        )
        .unwrap();
    store.persist_entry("message", json!({"message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"result"}]}})).unwrap();
    runner.core.lock().unwrap().store = Some(store);
    // A persisted assistant/turn boundary without full settle is not completion.
    runner.deliver_session_completion().await;
    assert!(commands.lock().unwrap().is_empty());
    {
        let mut core = runner.core.lock().unwrap();
        core.turn_completed = true;
        core.queued_input_suspended = true;
    }
    runner.deliver_session_completion().await;
    assert!(commands.lock().unwrap().is_empty());
    runner.core.lock().unwrap().queued_input_suspended = false;
    runner.record_session_settlement();
    engine
        .background
        .store(true, std::sync::atomic::Ordering::SeqCst);
    runner.deliver_session_completion().await;
    assert!(
        commands.lock().unwrap().is_empty(),
        "background work is not settled"
    );
    engine
        .background
        .store(false, std::sync::atomic::Ordering::SeqCst);
    engine
        .continuation_pending
        .store(true, std::sync::atomic::Ordering::SeqCst);
    runner.deliver_session_completion().await;
    assert!(
        commands.lock().unwrap().is_empty(),
        "retry/backoff owns continuation"
    );
    engine
        .continuation_pending
        .store(false, std::sync::atomic::Ordering::SeqCst);
    runner.deliver_session_completion().await;
    runner.deliver_session_completion().await;
    let delivered = commands.lock().unwrap().clone();
    let sends: Vec<_> = delivered
        .iter()
        .filter(|row| row["type"] == "follow_up")
        .collect();
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0]["activeSessionId"], "creator");
    assert_eq!(sends[0]["queueKey"], "delivery-1");
    assert!(sends[0]["message"]
        .as_str()
        .unwrap()
        .contains("\"taskAccepted\":false"));
    // A lost ACK or crash after intent writes can only query, never resend.
    {
        let mut core = runner.core.lock().unwrap();
        core.turn_completed = false;
        let store = core.store.as_mut().unwrap();
        store.persist_entry("custom", json!({"customType":"rlm_session_completion_delivery", "data":{"deliveryId":"delivery-1","state":"unknown"}})).unwrap();
        *store = SessionFile::open(&store.path).unwrap();
    }
    runner.deliver_session_completion().await;
    assert_eq!(
        commands
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["type"] == "follow_up")
            .count(),
        1
    );
    assert_eq!(
        commands
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["type"] == "get_input_receipts")
            .count(),
        1
    );
    // Accepted retask invalidates old assignment; idle/restart cannot relabel it.
    {
        let mut core = runner.core.lock().unwrap();
        core.turn_completed = true;
        core.store
            .as_mut()
            .unwrap()
            .persist_entry(
                "message",
                json!({"message":{"role":"user","content":"new task"}}),
            )
            .unwrap();
    }
    runner.deliver_session_completion().await;
    assert_eq!(
        commands
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["type"] == "get_input_receipts")
            .count(),
        1
    );
    server.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn session_completion_actual_turn_automatically_delivers_opt_in_only() {
    for subscribed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("supervisor.sock");
        let (commands, server) = super::tests::fake_supervisor(&socket);
        let input = if subscribed {
            EngineEvent::CustomMessage(
                json!({"role":"custom","customType":"rlm_session_assignment","content":"work",
                "details":{"assignmentId":"actual-task","artifactRef":"result.md","creatorSessionId":"creator",
                "creatorSessionFile":"creator.jsonl","deliveryId":"actual-delivery"}}),
            )
        } else {
            EngineEvent::UserMessage(json!({"role":"user","content":"work"}))
        };
        let engine: Arc<dyn SessionEngine> = Arc::new(GateProbeEngine {
            frames: vec![
                input,
                EngineEvent::AssistantMessage(json!({"role":"assistant","stopReason":"stop",
                "content":[{"type":"text","text":"finished result"}]})),
                EngineEvent::Done(Ok(())),
            ],
            ..Default::default()
        });
        let mut runner = burst_runner(engine.clone());
        runner.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
        let mut store = SessionFile::create("/fixture", None, 0);
        store.set_path(dir.path().join("worker.jsonl"));
        store.rewrite().unwrap();
        runner.core.lock().unwrap().store = Some(store);
        runner
            .run_turn(
                engine,
                Lane::Steering,
                vec![QueuedItem {
                    message: "work".into(),
                    priority: QueuePriority::Human,
                    preview: None,
                    custom_message: None,
                    agent_message: None,
                    queue_key: None,
                    input_identity: None,
                    admission_id: None,
                    images: Vec::new(),
                    done: None,
                    queue_visible: true,
                    policy: TurnPolicy::Queued,
                    forced_batch: false,
                }],
            )
            .await;
        assert_eq!(
            commands
                .lock()
                .unwrap()
                .iter()
                .filter(|row| row["type"] == "follow_up")
                .count(),
            usize::from(subscribed)
        );
        server.abort();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn session_completion_lost_ack_never_resends_after_reopen() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("supervisor.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let sink = seen.clone();
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n").await.unwrap();
            let mut lines = BufReader::new(reader).lines();
            while let Some(line) = lines.next_line().await.unwrap() {
                let request: Value = serde_json::from_str(&line).unwrap();
                let command = &request["command"];
                sink.lock().unwrap().push(command.clone());
                if command["type"] == "follow_up" {
                    break;
                } // admitted by target, response lost
                let response = crate::protocol::response_line(&crate::protocol::response_success(
                    request["id"].as_str(),
                    "get_input_receipts",
                    Some(json!({"durablyQueued":true,"receipts":[]})),
                ));
                writer
                    .write_all(
                        format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        }
    });
    let mut runner = burst_runner(Arc::new(GateProbeEngine::default()));
    runner.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    let mut store = SessionFile::create("/fixture", None, 0);
    store.set_path(dir.path().join("worker.jsonl"));
    store.rewrite().unwrap();
    store.persist_entry("custom_message", json!({"customType":"rlm_session_assignment","details":{
        "assignmentId":"lost","artifactRef":"result.md","creatorSessionId":"creator","creatorSessionFile":"creator.jsonl","deliveryId":"lost-delivery"}})).unwrap();
    store.persist_entry("message", json!({"message":{"role":"assistant","stopReason":"stop","content":[{"text":"result"}]}})).unwrap();
    runner.core.lock().unwrap().store = Some(store);
    runner.record_session_settlement();
    runner.deliver_session_completion().await;
    {
        let mut core = runner.core.lock().unwrap();
        let path = core.store.as_ref().unwrap().path.clone();
        core.store = Some(SessionFile::open(&path).unwrap());
    }
    runner.deliver_session_completion().await;
    runner.deliver_session_completion().await;
    let commands = seen.lock().unwrap();
    assert_eq!(
        commands
            .iter()
            .filter(|row| row["type"] == "follow_up")
            .count(),
        1
    );
    assert_eq!(
        commands
            .iter()
            .filter(|row| row["type"] == "get_input_receipts")
            .count(),
        1
    );
    server.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn session_completion_background_turn_waits_for_continuation() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("supervisor.sock");
    let (commands, server) = super::tests::fake_supervisor(&socket);
    let assignment = json!({"customType":"rlm_session_assignment", "role":"custom", "content":"work",
        "details":{"assignmentId":"background-task","artifactRef":"result.md",
        "creatorSessionId":"creator","creatorSessionFile":"creator.jsonl","deliveryId":"background-delivery"}});
    let engine = Arc::new(GateProbeEngine {
        frames: vec![
            EngineEvent::CustomMessage(assignment),
            EngineEvent::AssistantMessage(json!({"role":"assistant","stopReason":"stop",
                "content":[{"type":"text","text":"intermediate result"}]})),
            EngineEvent::Done(Ok(())),
        ],
        background: true.into(),
        ..Default::default()
    });
    let mut runner = burst_runner(engine.clone());
    runner.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    let mut store = SessionFile::create("/fixture", None, 0);
    let path = dir.path().join("worker.jsonl");
    store.set_path(path.clone());
    store.rewrite().unwrap();
    let session_id = store.session_id().to_string();
    runner.core.lock().unwrap().store = Some(store);
    runner
        .run_turn(
            engine.clone(),
            Lane::Steering,
            vec![QueuedItem {
                message: "work".into(),
                priority: QueuePriority::Human,
                preview: None,
                custom_message: None,
                agent_message: None,
                queue_key: None,
                input_identity: None,
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: TurnPolicy::Queued,
                forced_batch: false,
            }],
        )
        .await;
    assert_eq!(engine.turns_done.load(Ordering::SeqCst), 1);
    assert!(commands.lock().unwrap().is_empty());
    engine.continuation_pending.store(true, Ordering::SeqCst);
    engine.background.store(false, Ordering::SeqCst);
    runner.deliver_session_completion().await;
    assert!(commands.lock().unwrap().is_empty());
    // A second scripted engine supplies the continuation on the same runner/session.
    // No new assignment or user row is emitted by this continuation.
    let continuation = Arc::new(GateProbeEngine {
        frames: vec![
            EngineEvent::AssistantMessage(json!({"role":"assistant","stopReason":"stop",
            "content":[{"type":"text","text":"latest final result"}]})),
            EngineEvent::Done(Ok(())),
        ],
        ..Default::default()
    });
    engine.continuation_pending.store(false, Ordering::SeqCst);
    runner.engine = continuation.clone();
    runner
        .run_turn(
            continuation.clone(),
            Lane::Steering,
            vec![QueuedItem {
                message: "work".into(),
                priority: QueuePriority::Human,
                preview: None,
                custom_message: None,
                agent_message: None,
                queue_key: None,
                input_identity: None,
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: TurnPolicy::Queued,
                forced_batch: false,
            }],
        )
        .await;
    assert_eq!(continuation.turns_done.load(Ordering::SeqCst), 1);
    runner.deliver_session_completion().await;
    let rows = commands.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["type"], "follow_up");
    assert_eq!(rows[0]["activeSessionId"], "creator");
    assert_eq!(rows[0]["queueKey"], "background-delivery");
    let notice: Value = serde_json::from_str(
        rows[0]["message"]
            .as_str()
            .unwrap()
            .split_once('\n')
            .unwrap()
            .1,
    )
    .unwrap();
    assert_eq!(
        notice,
        json!({"assignmentId":"background-task","artifactRef":"result.md",
        "deliveryId":"background-delivery","sessionId":session_id,"sessionFile":path.to_string_lossy(),
        "status":"settled","stopReason":"stop","resultPreview":"latest final result","taskAccepted":false})
    );
    server.abort();
    let _ = server.await;
}

#[cfg(unix)]
#[tokio::test]
async fn session_completion_error_turn_preserves_failure_and_never_accepts() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("supervisor.sock");
    let (commands, server) = super::tests::fake_supervisor(&socket);
    let engine = Arc::new(GateProbeEngine {
        frames: vec![
            EngineEvent::AssistantMessage(json!({"role":"assistant","stopReason":"error",
            "content":[{"type":"text","text":"scripted failure"}]})),
            EngineEvent::Done(Err("scripted failure".into())),
        ],
        ..Default::default()
    });
    let mut runner = burst_runner(engine.clone());
    runner.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    let path = dir.path().join("worker.jsonl");
    let mut store = SessionFile::create("/fixture", None, 0);
    store.set_path(path.clone());
    store.rewrite().unwrap();
    let session_id = store.session_id().to_string();
    store
        .persist_entry(
            "custom_message",
            json!({"customType":"rlm_session_assignment","details":{
        "assignmentId":"error-task","artifactRef":"result.md","creatorSessionId":"creator",
        "creatorSessionFile":"creator.jsonl","deliveryId":"error-delivery"}}),
        )
        .unwrap();
    runner.core.lock().unwrap().store = Some(store);
    // No assistant or settlement: no synthetic notice, even for a subscribed task.
    runner.deliver_session_completion().await;
    assert!(commands.lock().unwrap().is_empty());
    runner.core.lock().unwrap().queued_input_suspended = true;
    runner
        .run_turn(
            engine,
            Lane::Steering,
            vec![QueuedItem {
                message: "work".into(),
                priority: QueuePriority::Human,
                preview: None,
                custom_message: None,
                agent_message: None,
                queue_key: None,
                input_identity: None,
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: TurnPolicy::Queued,
                forced_batch: false,
            }],
        )
        .await;
    let durable = SessionFile::open(&path).unwrap();
    let assistant = durable
        .entries
        .iter()
        .rev()
        .find(|row| row.type_ == "message" && row.fields["message"]["role"] == "assistant")
        .unwrap();
    assert_eq!(assistant.fields["message"]["stopReason"], "error");
    assert!(durable
        .entries
        .iter()
        .any(|row| row.fields["customType"] == "rlm_session_settlement"
            && row.fields["data"]["assistantId"] == assistant.id));
    assert!(
        commands.lock().unwrap().is_empty(),
        "error cannot bypass input suspension"
    );
    runner.core.lock().unwrap().queued_input_suspended = false;
    runner.core.lock().unwrap().abort_requested = true;
    runner.deliver_session_completion().await;
    assert!(
        commands.lock().unwrap().is_empty(),
        "error cannot bypass owner stop"
    );
    runner.core.lock().unwrap().abort_requested = false;
    runner.deliver_session_completion().await;
    runner.deliver_session_completion().await;
    let rows = commands.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["type"], "follow_up");
    assert_eq!(rows[0]["activeSessionId"], "creator");
    assert_eq!(rows[0]["queueKey"], "error-delivery");
    let notice: Value = serde_json::from_str(
        rows[0]["message"]
            .as_str()
            .unwrap()
            .split_once('\n')
            .unwrap()
            .1,
    )
    .unwrap();
    assert_eq!(
        notice,
        json!({"assignmentId":"error-task","artifactRef":"result.md",
        "deliveryId":"error-delivery","sessionId":session_id,"sessionFile":path.to_string_lossy(),
        "status":"settled","stopReason":"error","resultPreview":"scripted failure","taskAccepted":false})
    );
    server.abort();
    let _ = server.await;
}

#[cfg(unix)]
#[tokio::test]
async fn session_completion_fresh_runner_recovery_queries_only() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("supervisor.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let sink = seen.clone();
    let server = tokio::spawn(async move {
        let mut queries = 0;
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n").await.unwrap();
            let mut lines = BufReader::new(reader).lines();
            while let Some(line) = lines.next_line().await.unwrap() {
                let request: Value = serde_json::from_str(&line).unwrap();
                let command = &request["command"];
                sink.lock().unwrap().push(command.clone());
                if command["type"] == "follow_up" {
                    // Response lost; this fixture does not claim recipient admission.
                    break;
                }
                assert_eq!(command["type"], "get_input_receipts");
                queries += 1;
                let response = crate::protocol::response_line(&crate::protocol::response_success(
                    request["id"].as_str(),
                    "get_input_receipts",
                    Some(json!({"durablyQueued":queries > 1,"receipts":[]})),
                ));
                writer
                    .write_all(
                        format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        }
    });
    let path = dir.path().join("worker.jsonl");
    let mut store = SessionFile::create("/fixture", None, 0);
    store.set_path(path.clone());
    store.rewrite().unwrap();
    let session_id = store.session_id().to_string();
    store
        .persist_entry(
            "custom_message",
            json!({"customType":"rlm_session_assignment","details":{
        "assignmentId":"fresh-task","artifactRef":"result.md","creatorSessionId":"creator",
        "creatorSessionFile":"creator.jsonl","deliveryId":"fresh-delivery"}}),
        )
        .unwrap();
    store.persist_entry("message", json!({"message":{"role":"assistant","stopReason":"stop","content":[{"text":"result"}]}})).unwrap();
    let mut old = burst_runner(Arc::new(GateProbeEngine::default()));
    old.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket.clone()));
    old.core.lock().unwrap().store = Some(store);
    old.record_session_settlement();
    old.deliver_session_completion().await;
    let durable = SessionFile::open(&path).unwrap();
    assert_eq!(
        durable.entries.last().unwrap().fields["data"],
        json!({"deliveryId":"fresh-delivery","state":"unknown"})
    );
    let unknown_len = durable.entries.len();
    drop(old);
    // A new runner, link and recovery journal, reopening only the durable session.
    let mut fresh = burst_runner(Arc::new(GateProbeEngine::default()));
    fresh.passivation.link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    {
        let mut core = fresh.core.lock().unwrap();
        core.store = Some(SessionFile::open(&path).unwrap());
        core.queued_input_suspended = true;
    }
    fresh.deliver_session_completion().await;
    assert_eq!(seen.lock().unwrap().len(), 1);
    fresh.core.lock().unwrap().queued_input_suspended = false;
    fresh.core.lock().unwrap().abort_requested = true;
    fresh.deliver_session_completion().await;
    assert_eq!(seen.lock().unwrap().len(), 1);
    fresh.core.lock().unwrap().abort_requested = false;
    fresh.deliver_session_completion().await;
    let unresolved = SessionFile::open(&path).unwrap();
    assert_eq!(unresolved.entries.len(), unknown_len);
    assert_eq!(
        unresolved.entries.last().unwrap().fields["data"],
        json!({"deliveryId":"fresh-delivery","state":"unknown"})
    );
    assert_eq!(
        seen.lock().unwrap().len(),
        2,
        "empty negative receipt is query-only"
    );
    fresh.deliver_session_completion().await;
    let resolved = SessionFile::open(&path).unwrap();
    assert_eq!(resolved.session_id(), session_id);
    assert_eq!(resolved.entries.len(), unknown_len + 1);
    assert_eq!(
        resolved.entries.last().unwrap().fields["data"],
        json!({"deliveryId":"fresh-delivery","state":"durably_queued"})
    );
    assert_eq!(
        resolved
            .entries
            .iter()
            .filter(|row| row.fields["customType"] == "rlm_session_assignment")
            .count(),
        1
    );
    fresh.deliver_session_completion().await;
    let commands = seen.lock().unwrap().clone();
    assert_eq!(commands.len(), 3);
    assert_eq!(commands[0]["type"], "follow_up");
    for command in &commands[1..] {
        assert_eq!(command["type"], "get_input_receipts");
        assert_eq!(command["activeSessionId"], "creator");
        assert_eq!(command["completionTargetFile"], "creator.jsonl");
        assert_eq!(command["queueKey"], "fresh-delivery");
    }
    drop(fresh);
    server.abort();
    let _ = server.await;
}
