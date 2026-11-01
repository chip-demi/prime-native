use super::*;
use crate::protocol::{response_failure, response_success};
use pa_core::session_engine::semantic_edges::{
    ChildCancelledReason, SemanticEdgeIdentity, SemanticEdgeLedgerEvent, SemanticEdgeRecorder,
};
use pa_types::platform::transport::bind_transport;
use pa_types::session::AgentMessage;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

/// How the fake supervisor answers a child `kill`.
enum FakeKill {
    Success,
    /// The kill succeeds, but only after `ms` (the delete race window: the
    /// watcher wakes and tries to settle while the kill is in flight).
    SuccessAfterMs(u64),
    /// The child session is gone (the route failure a supervisor answers for a non-resident child).
    UnknownSession,
    /// The kill fails for a real reason (a stuck worker).
    Failure,
    /// The kill fails for a real reason, but only after `ms` (the delete
    /// race window with a refused kill: the watcher wakes and settles while
    /// the kill is still pending).
    FailureAfterMs(u64),
}

/// How the fake answers the child link: healthy, a failing `prompt`,
/// or a failing `get_state` (unreachable).
#[derive(Clone, Copy)]
enum FakeChild {
    Healthy,
    GatedAnswer,
    GatedFailureAnswer,
    GatedNotice,
    ProgressSummary,
    HeldProgressSummary,
    /// A state read never answers; collect must enforce its own deadline.
    StateHangs,
    PromptFails,
    Unreachable,
    /// The first `get_last_assistant_text` reads no text (the settle raced
    /// the worker's answer hand-off); later reads answer it.
    TextLate,
    /// The worker leaves right after the settle answer is captured:
    /// every later child read fails.
    LeavesAfterSettle,
    /// Unreachable; the watcher's give-up poll parks until the test
    /// lands a reader's verdict.
    UnreachableUntilVerdict,
    /// Every model call of the task run fails (#3366): the admitted
    /// prompt's run leaves a failed assistant row and the retry
    /// episode's failed outcome row in the child's session file.
    FinalTurnFails,
}

/// A scripted JSONL supervisor for the watcher tests: creates one child
/// session, reports it idle with a final answer, and captures the
/// `follow_up` commands routed to the parent (the terminal-notice
/// deliveries). `idle_delay_ms` paces `wait_for_idle` so a test can act
/// while the child is still "running". `child` scripts the child link
/// (`FakeChild`).
static HELD_PROGRESS_WAIT_STARTED: tokio::sync::Notify = tokio::sync::Notify::const_new();
static HELD_PROGRESS_WAIT_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();
static HELD_PROGRESS_READY: AtomicBool = AtomicBool::new(false);
static HELD_PROGRESS_STATE_READS: AtomicU32 = AtomicU32::new(0);

static RETASK_NOTICE_STARTED: tokio::sync::Notify = tokio::sync::Notify::const_new();
static RETASK_NOTICE_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();

static RETASK_FAILURE_ANSWER_STARTED: tokio::sync::Notify = tokio::sync::Notify::const_new();
static RETASK_FAILURE_ANSWER_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();

static RETASK_ANSWER_STARTED: tokio::sync::Notify = tokio::sync::Notify::const_new();
static RETASK_ANSWER_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();
/// The fake supervisor's scripted answers: the kill and child behaviors
/// plus the pushed worker roster `list_agent_peers` serves.
struct FakeSupervisorScript {
    kill: FakeKill,
    child: FakeChild,
    child_subagents: Arc<FakeChildSubagents>,
    peers: Arc<std::sync::Mutex<Vec<Value>>>,
}

async fn spawn_fake_supervisor(
    socket: std::path::PathBuf,
    follow_up_tx: mpsc::UnboundedSender<Value>,
    idle_delay_ms: u64,
    kill_tx: mpsc::UnboundedSender<Value>,
    script: FakeSupervisorScript,
) {
    let FakeSupervisorScript {
        kill: kill_behavior,
        child,
        child_subagents,
        peers,
    } = script;
    let kill_behavior = std::sync::Arc::new(kill_behavior);
    // Per-fake child session file: a fixed path would let a leftover file
    // from another test (or run) carry the prompt text, flip the
    // prompt-retry arbitration to `landed`, and turn an expected failure
    // row into a no-reply notice.
    let child_session_file = std::sync::Arc::new(
        std::env::temp_dir()
            .join(format!(
                "pa-rlm-watch-child-{}.jsonl",
                uuid::Uuid::new_v4().simple()
            ))
            .to_string_lossy()
            .to_string(),
    );
    let listener = bind_transport(&socket).await.unwrap();
    tokio::spawn(async move {
        // Shared across link connections: a left worker fails every child
        // read on whichever connection carries it.
        let gone = Arc::new(AtomicBool::new(false));
        // Shared across link connections: the give-up gate counts the
        // child's state reads on whichever connection carries them.
        let state_reads = Arc::new(AtomicU32::new(0));
        loop {
            let Ok(stream) = listener.accept().await else {
                return;
            };
            let follow_up_tx = follow_up_tx.clone();
            let kill_tx = kill_tx.clone();
            let kill_behavior = std::sync::Arc::clone(&kill_behavior);
            let gone = Arc::clone(&gone);
            let state_reads = Arc::clone(&state_reads);
            let child_session_file = std::sync::Arc::clone(&child_session_file);
            let child_subagents = Arc::clone(&child_subagents);
            let peers = Arc::clone(&peers);
            tokio::spawn(async move {
                let (reader, mut writer) = stream.split();
                let mut reader = BufReader::new(reader);
                writer
                    .write_all(
                        b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n",
                    )
                    .await
                    .unwrap();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap() == 0 {
                        return;
                    }
                    let value: Value = serde_json::from_str(line.trim()).unwrap();
                    let id = value["id"].as_str().unwrap_or_default().to_string();
                    let command = value["command"].clone();
                    let command_type: &str = command["type"].as_str().unwrap_or_default();
                    let response = match command_type {
                        "get_state" if matches!(child, FakeChild::StateHangs) => {
                            std::future::pending::<()>().await;
                            unreachable!("the scripted state read never returns");
                        }
                        _ if matches!(
                            (child, command_type),
                            (FakeChild::PromptFails, "prompt")
                                | (
                                    FakeChild::Unreachable | FakeChild::UnreachableUntilVerdict,
                                    "get_state",
                                )
                        ) =>
                        {
                            if matches!(child, FakeChild::PromptFails) {
                                PROMPT_FAILURE_STARTED.notify_one();
                                PROMPT_FAILURE_RELEASE.notified().await;
                            }
                            // Two get_state reads per watcher pass for the
                            // parked variant (the refresh read, then the
                            // liveness poll): the last one is the give-up
                            // poll. Only that variant counts, so a plain
                            // unreachable child keeps its instant refusal.
                            if matches!(child, FakeChild::UnreachableUntilVerdict)
                                && state_reads.fetch_add(1, Ordering::SeqCst) + 1
                                    == 2 * WATCH_MAX_UNREACHABLE_POLLS
                            {
                                GIVE_UP_POLL.notify_one();
                                VERDICT_LANDED.notified().await;
                            }
                            response_failure(
                                Some(&id),
                                command_type,
                                "refused by the fake supervisor",
                                None,
                            )
                        }
                        "get_state" | "wait_for_idle" if gone.load(Ordering::SeqCst) => {
                            response_failure(
                                Some(&id),
                                command_type,
                                "Unknown active session: child-live",
                                None,
                            )
                        }
                        "create" => response_success(
                            Some(&id),
                            command_type,
                            Some(json!({
                                "activeSessionId": "child-live",
                                "sessionId": "child-file",
                                "sessionFile": *child_session_file,
                                "sessionName": "f20-worker",
                            })),
                        ),
                        "prompt" => {
                            if matches!(child, FakeChild::FinalTurnFails) {
                                use super::lifecycle::exit_capture_tests::{
                                    assistant_row_ending, retry_outcome_row,
                                };
                                let failed_call = assistant_row_ending(
                                    "",
                                    pa_types::ai::StopReason::Error,
                                    Some("429 Provider returned error"),
                                );
                                // Persist the admitted agent_message before its terminal rows.
                                let kickoff = &command["customMessage"];
                                assert_eq!(kickoff["customType"], "agent_message");
                                assert!(kickoff["details"]["id"].as_str().is_some());
                                let admitted = json!({
                                    "type": "custom_message", "id": "kickoff-row",
                                    "customType": kickoff["customType"],
                                    "content": kickoff["content"],
                                    "display": kickoff["display"],
                                    "details": kickoff["details"],
                                });
                                std::fs::write(
                                    &*child_session_file,
                                    format!("{admitted}\n{failed_call}\n{}\n", retry_outcome_row(false)),
                                )
                                .unwrap();
                            }
                            if let Some(gate) = child_subagents
                                .prompt_gate
                                .as_ref()
                                .filter(|gate| gate.armed.swap(false, Ordering::SeqCst))
                            {
                                gate.copied.notify_one();
                                gate.release.notified().await;
                            }
                            response_success(Some(&id), command_type, None)
                        }
                        "wait_for_idle" => {
                            if matches!(child, FakeChild::HeldProgressSummary) {
                                HELD_PROGRESS_WAIT_STARTED.notify_one();
                                HELD_PROGRESS_WAIT_RELEASE.notified().await;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(idle_delay_ms))
                                .await;
                            // The quiescence arm also waits out the
                            // child's own running subagents.
                            if command["waitForRlmQuiescence"] == true {
                                child_subagents
                                    .quiescent_waits
                                    .fetch_add(1, Ordering::SeqCst);
                                while child_subagents.running.load(Ordering::SeqCst) {
                                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                }
                            }
                            response_success(Some(&id), command_type, None)
                        }
                        "get_state" if matches!(child, FakeChild::HeldProgressSummary) => {
                            HELD_PROGRESS_STATE_READS.fetch_add(1, Ordering::SeqCst);
                            let mut summary = json!({"isStreaming": true});
                            if HELD_PROGRESS_READY.load(Ordering::SeqCst) {
                                summary["thinkingLevel"] = json!("medium");
                                summary["progressNote"] = json!("note emitted after initial state");
                            }
                            response_success(Some(&id), command_type, Some(summary))
                        }
                        "get_state" if matches!(child, FakeChild::ProgressSummary) => {
                            let notes =
                                pa_core::session_engine::rlm_host::RlmProgressNotes::default();
                            assert!(notes.note("checking the consumer", 7).accepted);
                            let mut summary =
                                json!({"isStreaming": true, "thinkingLevel": "medium"});
                            if state_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                                summary["progressNote"] = json!(notes.latest_note().unwrap().0);
                            }
                            response_success(Some(&id), command_type, Some(summary))
                        }
                        "get_state" => response_success(
                            Some(&id),
                            command_type,
                            Some(child_subagents.live_state.lock().unwrap().clone().unwrap_or_else(|| {
                                json!({
                                    "isStreaming": false,
                                    "hasRunningSubagents": child_subagents.running.load(Ordering::SeqCst),
                                    "hasBackgroundBash": child_subagents.background_bash.load(Ordering::SeqCst),
                                    "sessionActions": { "queuedCount": 0 },
                                    "progressNote": "halfway through the task",
                                    "toolUseCount": 3,
                                })
                            })),
                        ),
                        "list_agent_peers" => {
                            let mut snapshot = peers.lock().unwrap().clone();
                            // This variant holds the same active turn in get_state and the
                            // supervisor roster; an empty roster would contradict that fixture.
                            if matches!(child, FakeChild::HeldProgressSummary) {
                                let mut summary = json!({"activeSessionId":"child-live", "rlmChildId":"held-progress", "isStreaming":true});
                                if HELD_PROGRESS_READY.load(Ordering::SeqCst) {
                                    summary["thinkingLevel"] = json!("medium");
                                    summary["progressNote"] = json!("note emitted after initial state");
                                }
                                snapshot.push(summary);
                            }
                            if let Some(gate) = child_subagents
                                .peer_gate
                                .as_ref()
                                .filter(|gate| gate.armed.swap(false, Ordering::SeqCst))
                            {
                                gate.copied.notify_one();
                                gate.release.notified().await;
                            }
                            if snapshot.iter().any(|row| row["_testRosterUnavailable"] == true) {
                                response_failure(Some(&id), command_type, "test roster unavailable", None)
                            } else {
                            response_success(
                                Some(&id),
                                command_type,
                                Some(json!({ "peers": snapshot })),
                            )
                            }
                        }
                        "get_last_assistant_text" => {
                            if matches!(child, FakeChild::GatedAnswer) {
                                RETASK_ANSWER_STARTED.notify_one();
                                RETASK_ANSWER_RELEASE.notified().await;
                            }
                            if matches!(child, FakeChild::GatedFailureAnswer) {
                                RETASK_FAILURE_ANSWER_STARTED.notify_one();
                                RETASK_FAILURE_ANSWER_RELEASE.notified().await;
                            }
                            // The settle capture: with the knob set, the
                            // worker leaves right after its final answer.
                            if matches!(child, FakeChild::LeavesAfterSettle) {
                                gone.store(true, Ordering::SeqCst);
                            }
                            // Every answer read counts; the late-text
                            // variant's FIRST read races the worker's
                            // answer hand-off and reads no text (a None
                            // capture), later reads answer.
                            let reads = child_subagents.answer_reads.fetch_add(1, Ordering::SeqCst);
                            if matches!(child, FakeChild::TextLate) && reads == 0 {
                                response_success(Some(&id), command_type, Some(json!({})))
                            } else {
                                response_success(
                                    Some(&id),
                                    command_type,
                                    Some(json!({ "text": "the child final answer" })),
                                )
                            }
                        }
                        "abort" => {
                            if child_subagents.abort_fails.load(Ordering::SeqCst) {
                                response_failure(
                                    Some(&id),
                                    command_type,
                                    "abort refused by the fake supervisor",
                                    None,
                                )
                            } else {
                                response_success(Some(&id), command_type, None)
                            }
                        }
                        "kill" => {
                            let _ = kill_tx.send(command.clone());
                            match *kill_behavior {
                                FakeKill::Success => {
                                    response_success(Some(&id), command_type, None)
                                }
                                FakeKill::SuccessAfterMs(ms) => {
                                    tokio::time::sleep(std::time::Duration::from_millis(ms))
                                        .await;
                                    response_success(Some(&id), command_type, None)
                                }
                                FakeKill::UnknownSession => response_failure(
                                    Some(&id),
                                    command_type,
                                    "Unknown active session: child-live",
                                    None,
                                ),
                                FakeKill::Failure | FakeKill::FailureAfterMs(_) => {
                                    if let FakeKill::FailureAfterMs(ms) = *kill_behavior {
                                        tokio::time::sleep(std::time::Duration::from_millis(ms))
                                            .await;
                                    }
                                    response_failure(
                                        Some(&id),
                                        command_type,
                                        "kill refused by the fake supervisor",
                                        None,
                                    )
                                }
                            }
                        }
                        "follow_up" => {
                            if matches!(child, FakeChild::GatedNotice) {
                                RETASK_NOTICE_STARTED.notify_one();
                                RETASK_NOTICE_RELEASE.notified().await;
                            }
                            let _ = follow_up_tx.send(command.clone());
                            response_success(
                                Some(&id),
                                command_type,
                                Some(json!({ "queued": true })),
                            )
                        }
                        other => response_failure(Some(&id), other, "unexpected command", None),
                    };
                    let mut line = serde_json::to_string(&response).unwrap();
                    line.push('\n');
                    if writer.write_all(line.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
}

async fn sessions_with_fake_supervisor(
    follow_up_tx: mpsc::UnboundedSender<Value>,
    idle_delay_ms: u64,
    kill_behavior: FakeKill,
    child: FakeChild,
) -> (SupervisorChildSessions, mpsc::UnboundedReceiver<Value>) {
    sessions_with_fake_child_subagents(
        follow_up_tx,
        idle_delay_ms,
        kill_behavior,
        child,
        Arc::new(FakeChildSubagents::default()),
    )
    .await
}

/// The fake child's own subagents: whether one still runs (the child
/// reports `hasRunningSubagents`, and a `waitForRlmQuiescence` idle wait
/// holds until it finishes), how many such waits started, and how many
/// `get_last_assistant_text` reads the child link answered (the
/// late-text variant scripts its first read empty off the count).
#[derive(Default)]
struct FakeChildSubagents {
    running: AtomicBool,
    background_bash: AtomicBool,
    quiescent_waits: std::sync::atomic::AtomicUsize,
    answer_reads: AtomicU32,
    /// The fake answers `abort` with a real refusal (the cancel path's
    /// failed-abort lane).
    abort_fails: AtomicBool,
    live_state: std::sync::Mutex<Option<Value>>,
    peers: Arc<std::sync::Mutex<Vec<Value>>>,
    peer_gate: Option<Arc<FakePeerGate>>,
    /// Holds the task `prompt` in flight: the worker has not accepted it yet.
    prompt_gate: Option<Arc<FakePeerGate>>,
}

#[derive(Default)]
struct FakePeerGate {
    armed: AtomicBool,
    copied: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

/// [`sessions_with_fake_supervisor`] whose child reports its own running
/// subagents from `child_subagents` (a grandchild still running).
async fn sessions_with_fake_child_subagents(
    follow_up_tx: mpsc::UnboundedSender<Value>,
    idle_delay_ms: u64,
    kill_behavior: FakeKill,
    child: FakeChild,
    child_subagents: Arc<FakeChildSubagents>,
) -> (SupervisorChildSessions, mpsc::UnboundedReceiver<Value>) {
    let socket = std::env::temp_dir().join(format!(
        "pa-rlm-watch-{}.sock",
        uuid::Uuid::new_v4().simple()
    ));
    let (kill_tx, kill_rx) = mpsc::unbounded_channel();
    spawn_fake_supervisor(
        socket.clone(),
        follow_up_tx,
        idle_delay_ms,
        kill_tx,
        FakeSupervisorScript {
            kill: kill_behavior,
            child,
            peers: Arc::clone(&child_subagents.peers),
            child_subagents,
        },
    )
    .await;
    let link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    let sessions = SupervisorChildSessions::new(
        link,
        std::env::temp_dir(),
        "parent-live".to_string(),
        std::sync::Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
            std::env::temp_dir(),
            /*telemetry_disabled*/ true,
        )),
        String::new(),
    );
    // A live parent carries its resolved model on the identity; the
    // spawn path resolves the child's model from it.
    sessions.set_identity(ParentIdentity {
        model: Some("mock/mock-1".to_string()),
        cwd: Some(std::env::temp_dir().to_string_lossy().to_string()),
        ..ParentIdentity::with_default_depth()
    });
    (sessions, kill_rx)
}

async fn spawn_child(sessions: &SupervisorChildSessions) -> RlmSpawnHandle {
    sessions
        .spawn(RlmSpawnRequest {
            prompt: "f20 child task".to_string(),
            name: Some("f20-worker".to_string()),
            model: None,
            thinking: None,
            cwd: None,
            spawned_by_request_id: None,
            work: None,
            factory: None,
        })
        .await
        .expect("spawn must succeed against the fake supervisor")
}

async fn begin_failed_spawn(
    sessions: &SupervisorChildSessions,
) -> (
    tokio::task::JoinHandle<anyhow::Result<RlmSpawnHandle>>,
    String,
) {
    let task = tokio::spawn(sessions.spawn(RlmSpawnRequest {
        prompt: "f20 child task".into(),
        cwd: None,
        factory: None,
        name: Some("f20-worker".into()),
        model: None,
        thinking: None,
        spawned_by_request_id: None,
        work: None,
    }));
    PROMPT_FAILURE_STARTED.notified().await;
    let child_id = sessions.inner.children.lock().await[0]
        .lock()
        .await
        .rlm_child_id
        .clone();
    (task, child_id)
}

async fn refuse_failed_spawn(task: tokio::task::JoinHandle<anyhow::Result<RlmSpawnHandle>>) {
    PROMPT_FAILURE_RELEASE.notify_one();
    PROMPT_FAILURE_STARTED.notified().await;
    PROMPT_FAILURE_RELEASE.notify_one();
    let error = task
        .await
        .unwrap()
        .expect_err("initial prompt failure must not return a usable handle");
    assert!(format!("{error:#}").contains("refused by the fake supervisor"));
}

#[tokio::test]
async fn a_factory_child_settles_without_a_parent_notice() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    sessions
        .spawn(RlmSpawnRequest {
            cwd: None,
            work: None,

            prompt: "f20 child task".to_string(),
            name: Some("f20-worker".to_string()),
            model: None,
            thinking: None,
            spawned_by_request_id: None,
            factory: Some(pa_core::session_engine::rlm_host::RlmFactoryTag {
                run_id: "run".to_string(),
                spec_id: "spec".to_string(),
                state_id: "state".to_string(),
            }),
        })
        .await
        .expect("spawn must succeed against the fake supervisor");
    sessions.notify_turn_done();

    // The factory's own signal still reports the settle.
    let results = sessions.collect(Vec::new(), 10_000).await.expect("collect");
    assert_eq!(results[0].status, "done");
    let notice =
        tokio::time::timeout(std::time::Duration::from_millis(500), follow_up_rx.recv()).await;
    assert!(
        notice.is_err(),
        "a factory child must not notify the parent: {notice:?}"
    );
}

/// [`sessions_with_fake_supervisor`] whose fake answers `list_agent_peers`
/// from `peers`: the supervisor's pushed worker roster that the live roster
/// read joins each child row onto.
async fn sessions_with_roster_peers(
    peers: Arc<std::sync::Mutex<Vec<Value>>>,
    idle_delay_ms: u64,
) -> (SupervisorChildSessions, mpsc::UnboundedReceiver<Value>) {
    let socket = std::env::temp_dir().join(format!(
        "pa-rlm-peers-{}.sock",
        uuid::Uuid::new_v4().simple()
    ));
    let (kill_tx, kill_rx) = mpsc::unbounded_channel();
    spawn_fake_supervisor(
        socket.clone(),
        mpsc::unbounded_channel().0,
        idle_delay_ms,
        kill_tx,
        FakeSupervisorScript {
            kill: FakeKill::Success,
            child: FakeChild::Healthy,
            child_subagents: Arc::new(FakeChildSubagents::default()),
            peers,
        },
    )
    .await;
    let link = Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
    let sessions = SupervisorChildSessions::new(
        link,
        std::env::temp_dir(),
        "parent-live".to_string(),
        std::sync::Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
            std::env::temp_dir(),
            /*telemetry_disabled*/ true,
        )),
        String::new(),
    );
    sessions.set_identity(ParentIdentity {
        model: Some("mock/mock-1".to_string()),
        cwd: Some(std::env::temp_dir().to_string_lossy().to_string()),
        ..ParentIdentity::with_default_depth()
    });
    (sessions, kill_rx)
}

#[tokio::test]
async fn roster_snapshot_does_not_wait_for_a_slow_child_worker() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 1_000, FakeKill::Success, FakeChild::Healthy)
            .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".to_string(),
            active_session_id: "child-live".to_string(),
            session_id: Some("child-file".to_string()),
            session_name: "slow-child".to_string(),
        })
        .await;
    let roster = tokio::time::timeout(Duration::from_millis(250), sessions.list_subagents())
        .await
        .expect("the roster read never waits on the slow child worker")
        .expect("roster snapshot");
    assert_eq!(roster.len(), 1);
    assert_eq!(roster[0].assignment_status, "running");
    assert_eq!(roster[0].status, "unknown");
}

#[tokio::test]
async fn collect_zero_timeout_returns_a_cached_snapshot_without_worker_rpc() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::StateHangs)
            .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".to_string(),
            active_session_id: "child-live".to_string(),
            session_id: Some("child-file".to_string()),
            session_name: "slow-child".to_string(),
        })
        .await;
    let results = tokio::time::timeout(
        Duration::from_millis(100),
        sessions.collect(vec!["child-id".to_string()], 0),
    )
    .await
    .expect("zero-timeout collect must not wait for a worker")
    .expect("collect cached child");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, "running");
}

#[tokio::test]
async fn collect_positive_timeout_bounds_worker_refresh() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::StateHangs)
            .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".to_string(),
            active_session_id: "child-live".to_string(),
            session_id: Some("child-file".to_string()),
            session_name: "slow-child".to_string(),
        })
        .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-two".to_string(),
            active_session_id: "child-two-live".to_string(),
            session_id: Some("child-two-file".to_string()),
            session_name: "second-slow-child".to_string(),
        })
        .await;
    let results =
        tokio::time::timeout(Duration::from_millis(250), sessions.collect(Vec::new(), 10))
            .await
            .expect("collect deadline must include worker refresh")
            .expect("collect timed-out child snapshot");
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.status == "running"));
}

#[tokio::test]
async fn retask_admission_waits_for_unreachable_exit_capture_and_tail() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _) = sessions_with_fake_supervisor(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::GatedFailureAnswer,
    )
    .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "failed-gated".into(),
            active_session_id: "child-live".into(),
            session_id: Some("child-file".into()),
            session_name: "failed-gated".into(),
        })
        .await;
    let dir = tempfile::TempDir::new().unwrap();
    sessions
        .prepare_admission_display_for_test("failed-gated", dir.path())
        .await;
    let record = sessions.inner.find_record("failed-gated").await.unwrap();
    let inner = Arc::clone(&sessions.inner);
    let row = Arc::clone(&record);
    let failure = tokio::spawn(async move {
        inner
            .settle_failed(
                &row,
                "unreachable".into(),
                super::lifecycle::FailedArm::Unreachable,
            )
            .await
    });
    RETASK_FAILURE_ANSWER_STARTED.notified().await;
    let mut begin = Box::pin(sessions.begin_child_admission("failed-gated"));
    assert!(matches!(
        futures::poll!(begin.as_mut()),
        std::task::Poll::Pending
    ));
    RETASK_FAILURE_ANSWER_RELEASE.notify_one();
    assert!(failure.await.unwrap());
    let admission = begin.await.unwrap().unwrap();
    let current = sessions.collect(vec![], 0).await.unwrap();
    assert!(!current[0].settled);
    assert_eq!(current[0].answer_text, None);
    assert_eq!(current[0].error, None);
    drop(admission);
}

#[tokio::test]
async fn retask_unreachable_failure_cannot_capture_or_settle_an_unresolved_admission() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "unknown".into(),
            active_session_id: "child-live".into(),
            session_id: Some("child-file".into()),
            session_name: "unknown".into(),
        })
        .await;
    let dir = tempfile::TempDir::new().unwrap();
    sessions
        .prepare_admission_display_for_test("unknown", dir.path())
        .await;
    let admission = sessions
        .begin_child_admission("unknown")
        .await
        .unwrap()
        .unwrap();
    let record = sessions.inner.find_record("unknown").await.unwrap();
    let inner = Arc::clone(&sessions.inner);
    let row = Arc::clone(&record);
    let mut failure = tokio::spawn(async move {
        inner
            .settle_failed(
                &row,
                "unreachable".into(),
                super::lifecycle::FailedArm::Unreachable,
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut failure)
            .await
            .is_err()
    );
    drop(admission);
    assert!(!failure.await.unwrap());
    let row = record.lock().await;
    assert!(!row.settled);
    assert_eq!(row.settled_status, None);
    assert_eq!(row.answer_text, None);
    assert!(!row.notice_delivered);
    assert!(follow_up_rx.try_recv().is_err());
}

#[tokio::test]
async fn retask_admission_waits_for_old_answer_capture_then_invalidates_it() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::GatedAnswer)
            .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "gated-child".to_string(),
            active_session_id: "child-live".to_string(),
            session_id: Some("child-file".to_string()),
            session_name: "gated-child".to_string(),
        })
        .await;
    let display_dir = tempfile::TempDir::new().unwrap();
    sessions
        .prepare_admission_display_for_test("gated-child", display_dir.path())
        .await;
    let observer = sessions.clone();
    let collecting = tokio::spawn(async move { observer.collect(vec![], 1_000).await.unwrap() });
    RETASK_ANSWER_STARTED.notified().await;
    let mut begin = Box::pin(sessions.begin_child_admission("gated-child"));
    assert!(matches!(
        futures::poll!(begin.as_mut()),
        std::task::Poll::Pending
    ));
    RETASK_ANSWER_RELEASE.notify_one();
    collecting.await.unwrap();
    let admission = begin.await.unwrap().unwrap();
    let current = sessions.collect(vec![], 0).await.unwrap();
    assert_eq!(current[0].status, "running");
    assert_eq!(current[0].answer_text, None);
    drop(admission);
    let unresolved = sessions.collect(vec![], 10).await.unwrap();
    assert!(
        !unresolved[0].settled,
        "cancelled admission cannot close from idle old answer"
    );
}

#[tokio::test]
async fn retask_admission_waits_for_old_notice_tail_before_reopening() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::GatedNotice)
            .await;
    let child = spawn_child(&sessions).await;
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    sessions.set_settle_hook(Arc::new(move || {
        let _ = hook_tx.send(());
    }));
    sessions.notify_turn_done();
    RETASK_NOTICE_STARTED.notified().await;
    let display_dir = tempfile::TempDir::new().unwrap();
    sessions
        .prepare_admission_display_for_test(&child.rlm_child_id, display_dir.path())
        .await;
    let mut begin = Box::pin(sessions.begin_child_admission(&child.rlm_child_id));
    assert!(matches!(
        futures::poll!(begin.as_mut()),
        std::task::Poll::Pending
    ));
    RETASK_NOTICE_RELEASE.notify_one();
    let admission = begin.await.unwrap().unwrap();
    hook_rx.recv().await.unwrap();
    let current = sessions.collect(vec![], 0).await.unwrap();
    assert_eq!(current[0].status, "running");
    assert!(!current[0].settled);
    assert_eq!(current[0].answer_text, None);
    drop(admission);
    assert!(matches!(
        hook_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

/// #4 A1/A2: a running child's row carries its latest accepted note and its
/// tool-call count within one roster read — the supervisor's pushed worker
/// roster joined onto the registry row (the watcher's 60s slice is too
/// coarse to be the source; TS bumps the child's counter per
/// `tool_execution_start` and omits a zero count).
#[tokio::test]
async fn a_running_childs_roster_row_carries_its_live_note_and_tool_count() {
    let peers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (sessions, _kill_rx) = sessions_with_roster_peers(peers.clone(), 5_000).await;
    let handle = spawn_child(&sessions).await;
    *peers.lock().unwrap() = vec![json!({
        "activeSessionId": "child-live",
        "isStreaming": true,
        "rlmChildId": handle.rlm_child_id.clone(),
        "progressNote": "halfway through the task",
        "toolUseCount": 3,
    })];
    let roster = sessions.list_subagents().await.expect("child roster");
    assert_eq!(roster[0].status, "running");
    assert_eq!(
        roster[0].progress_note.as_deref(),
        Some("halfway through the task"),
        "the row joins the pushed roster's live note: {roster:?}"
    );
    assert_eq!(
        roster[0].tool_use_count,
        Some(3),
        "the row joins the pushed roster's live count: {roster:?}"
    );
}

/// #4: `collect` rows carry the same count: the refresh's `get_state`
/// round trip (already in flight for the busy verdict) observes the child's
/// live summary onto the record.
#[tokio::test]
async fn a_collect_row_counts_the_childs_tool_calls() {
    let peers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (sessions, _kill_rx) = sessions_with_roster_peers(peers, 5_000).await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "sub-collect".to_string(),
            active_session_id: "child-live".to_string(),
            session_id: Some("child-file".to_string()),
            session_name: "counted-child".to_string(),
        })
        .await;
    let results = sessions.collect(Vec::new(), 1_000).await.expect("collect");
    assert_eq!(results[0].status, "done");
    assert_eq!(
        results[0].tool_use_count,
        Some(3),
        "the collect row reads the observed count: {results:?}"
    );
}

/// An old list reply must not undo a newer direct child observation, even
/// when that direct read settled the child before the list reply arrived.
#[tokio::test]
async fn a_delayed_roster_reply_keeps_the_newer_settled_observation() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("sup.sock");
    let gate = Arc::new(FakePeerGate::default());
    gate.armed.store(true, Ordering::SeqCst);
    let state = Arc::new(FakeChildSubagents {
        peer_gate: Some(Arc::clone(&gate)),
        ..FakeChildSubagents::default()
    });
    let peers = Arc::new(std::sync::Mutex::new(vec![json!({
        "activeSessionId": "child-live",
        "workerInstanceId": "instance-1",
        "rosterDeltaSequence": 3,
        "progressNote": "older note",
        "toolUseCount": 3,
    })]));
    *state.live_state.lock().unwrap() = Some(json!({
        "workerInstanceId": "instance-1",
        "rosterDeltaSequence": 4,
        "isStreaming": false,
        "progressNote": "newer note",
        "toolUseCount": 4,
    }));
    spawn_fake_supervisor(
        socket.clone(),
        mpsc::unbounded_channel().0,
        0,
        mpsc::unbounded_channel().0,
        FakeSupervisorScript {
            kill: FakeKill::Success,
            child: FakeChild::Healthy,
            child_subagents: state,
            peers: Arc::clone(&peers),
        },
    )
    .await;
    let sessions = SupervisorChildSessions::new(
        Arc::new(crate::supervisor_link::SupervisorLink::new(socket)),
        dir.path().to_path_buf(),
        "parent-live".to_string(),
        Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
            dir.path().to_path_buf(),
            /*telemetry_disabled*/ true,
        )),
        String::new(),
    );
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".to_string(),
            active_session_id: "child-live".to_string(),
            session_id: Some("child-file".to_string()),
            session_name: "worker".to_string(),
        })
        .await;
    let listing = {
        let sessions = sessions.clone();
        tokio::spawn(async move { sessions.list_subagents().await.unwrap() })
    };
    gate.copied.notified().await;
    let collected = sessions.collect(Vec::new(), 1_000).await.unwrap();
    assert_eq!(collected[0].tool_use_count, Some(4));
    gate.release.notify_one();
    let roster = listing.await.unwrap();
    assert_eq!(
        (roster[0].tool_use_count, roster[0].progress_note.as_deref()),
        (Some(4), Some("newer note")),
    );
    // Completed rows do not read peers again: the accepted observation
    // must survive even when no further direct refresh is requested.
    let roster = sessions.list_subagents().await.unwrap();
    assert_eq!(roster[0].tool_use_count, Some(4));
}

/// A later roster request may still read older pushed state. Sequence
/// wins over request order; a replacement worker may legitimately reset.
#[tokio::test]
async fn child_observations_order_worker_sequences_and_allow_recovery_resets() {
    let peers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (sessions, _kill_rx) = sessions_with_roster_peers(peers, 0).await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".to_string(),
            active_session_id: "child-live".to_string(),
            session_id: Some("child-file".to_string()),
            session_name: "worker".to_string(),
        })
        .await;
    let record = sessions.inner.children.lock().await[0].clone();
    let direct_observation = Arc::clone(&record.lock().await.direct_observation);
    let direct = direct_observation.lock().await;
    sessions
        .inner
        .observe_live_state(
            &record,
            &json!({
                "workerInstanceId": "initial-roster",
                "rosterDeltaSequence": 99,
                "toolUseCount": 99,
            }),
            0,
            ChildObservationSource::Roster,
        )
        .await;
    assert!(record.lock().await.live_observation.is_none());
    drop(direct);
    for (request_order, instance, sequence, count, note, source, expected) in [
        (
            1,
            "i1",
            4,
            4,
            "new",
            ChildObservationSource::Direct,
            (Some(4), Some("new")),
        ),
        (
            2,
            "i1",
            3,
            3,
            "old",
            ChildObservationSource::Roster,
            (Some(4), Some("new")),
        ),
        (
            3,
            "i1",
            4,
            5,
            "pushed",
            ChildObservationSource::Roster,
            (Some(4), Some("new")),
        ),
        (
            4,
            "i1",
            4,
            4,
            "equal",
            ChildObservationSource::Direct,
            (Some(4), Some("equal")),
        ),
        (
            5,
            "i1",
            5,
            6,
            "advanced",
            ChildObservationSource::Roster,
            (Some(6), Some("advanced")),
        ),
        (
            6,
            "i2",
            0,
            1,
            "recovered",
            ChildObservationSource::Direct,
            (Some(1), Some("recovered")),
        ),
        (
            8,
            "i1",
            99,
            99,
            "retired",
            ChildObservationSource::Roster,
            (Some(1), Some("recovered")),
        ),
    ] {
        sessions
            .inner
            .observe_live_state(
                &record,
                &json!({
                    "workerInstanceId": instance,
                    "rosterDeltaSequence": sequence,
                    "toolUseCount": count,
                    "progressNote": note,
                }),
                request_order,
                source,
            )
            .await;
        let record = record.lock().await;
        assert_eq!(
            (record.tool_use_count, record.progress_note.as_deref()),
            expected,
        );
    }
    sessions
        .inner
        .observe_live_state(
            &record,
            &json!({ "workerInstanceId": "i3", "rosterDeltaSequence": 0 }),
            7,
            ChildObservationSource::Direct,
        )
        .await;
    let record = record.lock().await;
    assert_eq!(
        (record.tool_use_count, &record.progress_note),
        (None, &None)
    );
}

#[tokio::test]
async fn a_settled_child_without_a_reply_delivers_the_terminal_notice() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let handle = spawn_child(&sessions).await;
    // The worker releases the detached prompt at its turn boundary.
    sessions.notify_turn_done();

    let follow_up = tokio::time::timeout(std::time::Duration::from_secs(10), follow_up_rx.recv())
        .await
        .expect("the watcher must deliver the notice")
        .expect("the follow_up channel stays open");
    assert_eq!(follow_up["type"], "follow_up");
    assert_eq!(follow_up["activeSessionId"], "parent-live");
    let custom = &follow_up["customMessage"];
    assert_eq!(custom["role"], "custom");
    assert_eq!(custom["customType"], "rlm_child_terminal_notice");
    assert!(
        follow_up["rlmNoticeNonce"].as_str().is_some(),
        "the notice carries the one-shot capability the parent's queue admission consumes"
    );
    assert_eq!(
        custom["content"],
        "[child-exited: no-reply child:f20-worker]\n\nLast assistant text: the child final answer"
    );
    assert_eq!(custom["details"]["childId"], handle.rlm_child_id);
    assert_eq!(custom["details"]["sessionName"], "f20-worker");
    // Exactly one notice lands: the watcher delivers once.
    let extra =
        tokio::time::timeout(std::time::Duration::from_millis(300), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "no second notice may arrive");
}

/// A spawn admitted while the parent is idle (a background kernel task: a
/// factory run resumed or advanced between turns) has no turn to wait
/// for: the task prompt goes out at once and the run settles.
#[tokio::test]
async fn a_child_spawned_while_the_parent_is_idle_runs_without_a_turn_boundary() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    spawn_child(&sessions).await;

    let notice = tokio::time::timeout(Duration::from_secs(10), follow_up_rx.recv())
        .await
        .expect("an idle parent's child must not wait for a parent turn boundary")
        .expect("the follow_up channel stays open");
    assert_eq!(
        notice["customMessage"]["customType"],
        "rlm_child_terminal_notice"
    );
}

/// A spawn admitted mid-turn holds its task prompt until that parent turn
/// completes, so the parent's continuation request reaches the provider
/// before the child's first turn.
#[tokio::test]
async fn a_child_spawned_mid_turn_prompts_after_the_parent_turn_completes() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let gate = Arc::new(FakePeerGate {
        armed: AtomicBool::new(true),
        ..FakePeerGate::default()
    });
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::new(FakeChildSubagents {
            prompt_gate: Some(Arc::clone(&gate)),
            ..FakeChildSubagents::default()
        }),
    )
    .await;
    sessions.notify_turn_started();
    spawn_child(&sessions).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), gate.copied.notified())
            .await
            .is_err(),
        "the task prompt must wait for the spawning turn"
    );

    sessions.notify_turn_done();
    tokio::time::timeout(Duration::from_secs(10), gate.copied.notified())
        .await
        .expect("the turn boundary releases the task prompt");
    gate.release.notify_one();
}

/// A `collect` that lands while the task prompt is still in flight reads
/// the child's pre-prompt idle worker: the child is still running, never a
/// `done` settle with no answer (the factory settled its verifier before
/// the verifier's turn ran).
#[tokio::test]
async fn a_collect_while_the_task_prompt_is_in_flight_does_not_settle_the_child() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let gate = Arc::new(FakePeerGate {
        armed: AtomicBool::new(true),
        ..FakePeerGate::default()
    });
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::new(FakeChildSubagents {
            prompt_gate: Some(Arc::clone(&gate)),
            ..FakeChildSubagents::default()
        }),
    )
    .await;
    let admitting = {
        let sessions = sessions.clone();
        tokio::spawn(async move { spawn_child(&sessions).await })
    };
    gate.copied.notified().await;
    assert!(
        !admitting.is_finished(),
        "a handle cannot escape before task admission"
    );
    let child_id = sessions.inner.children.lock().await[0]
        .lock()
        .await
        .rlm_child_id
        .clone();

    let in_flight = sessions
        .collect(vec![child_id], 0)
        .await
        .expect("collect while the prompt is in flight");
    assert!(
        !in_flight[0].settled,
        "a child whose task prompt is not accepted yet must not settle: {:?}",
        in_flight[0].status
    );

    gate.release.notify_one();
    let handle = admitting.await.expect("spawn admission task");
    tokio::time::timeout(Duration::from_secs(10), follow_up_rx.recv())
        .await
        .expect("the run settles once the prompt lands")
        .expect("follow_up channel open");
    let settled = sessions
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .expect("collect after the settle");
    assert_eq!(settled[0].status, "done");
    assert_eq!(
        settled[0].answer_text.as_deref(),
        Some("the child final answer")
    );
}

/// The settle grace re-marks only a BUSY child as running: a worker that
/// leaves inside the grace (the idle passivation's stop, a crash) keeps
/// the settled verdict, and the settle tail (notice, funnel) still runs.
#[tokio::test]
async fn a_worker_leaving_inside_the_settle_grace_keeps_the_verdict() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) = sessions_with_fake_supervisor(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::LeavesAfterSettle,
    )
    .await;
    let settled = sessions.settle_notified();
    spawn_child(&sessions).await;
    sessions.notify_turn_done();

    tokio::time::timeout(Duration::from_secs(10), settled)
        .await
        .expect("the settle funnel fires although the worker left");
    assert!(!sessions.any_running().await);
    let roster = sessions.list_subagents().await.expect("child roster");
    assert_eq!(roster[0].status, "completed");
    let notice = follow_up_rx
        .try_recv()
        .expect("the no-reply notice is still owed");
    assert!(notice["customMessage"]["content"]
        .as_str()
        .is_some_and(|content| content.contains("the child final answer")));
}

#[tokio::test]
async fn child_updates_surface_through_the_sink() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let rows: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink_rows = Arc::clone(&rows);
    sessions.set_child_update_sink(Arc::new(move |child| {
        sink_rows.lock().unwrap().push(child);
    }));
    let settled = sessions.settle_notified();
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    tokio::time::timeout(Duration::from_secs(10), settled)
        .await
        .expect("the child settles");
    let result = sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect("the settled child deletes");
    assert_eq!(result.outcome, Some("deleted"));
    let rows = rows.lock().unwrap().clone();
    assert!(
        rows.iter().any(|row| {
            row["id"] == json!(handle.rlm_child_id)
                && row["status"] == json!("running")
                && row["sessionName"] == json!("f20-worker")
                && row["model"] == json!("mock/mock-1")
                && row["sessionDir"].is_string()
        }),
        "the admission row carries the snapshot: {rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            row["id"] == json!(handle.rlm_child_id)
                && row["status"] == json!("done")
                && row["answerPreview"] == json!("the child final answer")
        }),
        "the settle row carries the terminal status: {rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            row["id"] == json!(handle.rlm_child_id)
                && row["status"] == json!("cancelled")
                && row["error"] == json!("Deleted by parent orchestrator")
        }),
        "the delete surfaces the removal row: {rows:?}"
    );
}

/// A cancelled run's watcher settle leaves the display `running`, so a
/// restart relists the child as `error` instead of `completed`.
#[tokio::test]
async fn a_cancelled_child_keeps_its_display_running_through_the_watcher_settle() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 500, FakeKill::Success, FakeChild::Healthy)
            .await;
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    sessions.set_settle_hook(Arc::new(move || {
        let _ = hook_tx.send(());
    }));
    let handle = spawn_child(&sessions).await;
    let display_file = Path::new(&handle.session_dir).join("rlm-subagent.json");
    std::fs::write(
        &display_file,
        json!({ "type": "rlm_subagent", "childId": handle.rlm_child_id,
                "sessionDir": handle.session_dir, "status": "running" })
        .to_string(),
    )
    .unwrap();
    sessions.notify_turn_done();
    assert!(sessions.cancel_child_run(&handle.rlm_child_id).await);
    // One settle hook from the cancel, one from the watcher's settle tail.
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(10), hook_rx.recv())
            .await
            .expect("both settle hooks fire")
            .expect("hook channel open");
    }
    let display = crate::rlm_ledger::read_rlm_subagent_display(Path::new(&handle.session_dir))
        .expect("display entry stays readable");
    assert_eq!(display.status, "running");
}

/// The `follow_up` carries exactly the TS failure row (`createRlmChildFailureMessage`).
fn assert_failure_row(follow_up: &Value, child_id: &str, error: &str) {
    let custom = &follow_up["customMessage"];
    let timestamp = custom["timestamp"].as_u64().expect("failure row timestamp");
    let expected = create_rlm_child_failure_message(child_id, "f20-worker", error, timestamp);
    let mut expected = serde_json::to_value(AgentMessage::Custom(expected)).unwrap();
    let delivery_id = custom["details"]["terminalDeliveryId"]
        .as_str()
        .expect("durable terminal delivery identity");
    uuid::Uuid::parse_str(delivery_id).expect("terminal delivery UUID");
    expected["details"]["terminalDeliveryId"] = json!(delivery_id);
    assert_eq!(*custom, expected);
}

/// A child whose task prompt cannot be routed settles `error`, delivers
/// the TS failure row and releases the owed continuation.
#[tokio::test]
async fn a_child_whose_task_prompt_cannot_be_routed_delivers_the_failure_notice() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, mut kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::PromptFails)
            .await;
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    sessions.set_settle_hook(Arc::new(move || {
        let _ = hook_tx.send(());
    }));
    let (spawned, child_id) = begin_failed_spawn(&sessions).await;
    // The worker releases the detached prompt at its turn boundary.
    refuse_failed_spawn(spawned).await;

    let follow_up = tokio::time::timeout(Duration::from_secs(10), follow_up_rx.recv())
        .await
        .expect("the failed child must deliver its failure notice")
        .expect("the follow_up channel stays open");
    assert_failure_row(
        &follow_up,
        &child_id,
        "prompt RLM child session child-live: refused by the fake supervisor",
    );
    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(entries[0].status, "error");
    tokio::time::timeout(Duration::from_secs(10), hook_rx.recv())
        .await
        .expect("the failed child releases the owed continuation")
        .expect("hook channel open");
    assert!(
        !sessions.any_running().await,
        "the failed run must mark settled: an unsettled run parks the quiescence barrier"
    );
    let kill = kill_rx.try_recv().expect("the unadmitted child is killed");
    assert_eq!(kill["activeSessionId"], "child-live");
    // Exactly one failure row lands: the claim admits one writer.
    let extra = tokio::time::timeout(Duration::from_millis(300), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "no second notice may arrive");
}

/// #3366: a child whose model calls all failed settles `error`, not
/// `done`: the parent receives the failure row carrying the provider
/// error (not the no-reply notice), and the roster and the collect
/// envelope report the error with its text.
#[tokio::test]
async fn a_child_whose_final_turn_failed_settles_as_an_error() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) = sessions_with_fake_supervisor(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::FinalTurnFails,
    )
    .await;
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();

    let follow_up = tokio::time::timeout(Duration::from_secs(10), follow_up_rx.recv())
        .await
        .expect("the failed child must deliver its failure notice")
        .expect("the follow_up channel stays open");
    let error = "\u{26a0} Error: Retry failed after 3 attempts: 429 Provider returned error";
    assert_failure_row(&follow_up, &handle.rlm_child_id, error);
    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(entries[0].status, "error");
    let results = sessions
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .expect("collect the failed child");
    assert_eq!(
        (results[0].status, results[0].error.as_deref()),
        ("error", Some(error))
    );
    // Exactly one notice lands: the watcher delivers once.
    let extra = tokio::time::timeout(Duration::from_millis(300), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "no second notice may arrive");
}

/// A child that replied before its final model call failed (the cleanup
/// call after the reply, #3366) sends the parent no failure row on top of
/// its answer; the roster still reports the error.
#[tokio::test]
async fn a_replied_child_whose_final_turn_failed_sends_no_failure_row() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) = sessions_with_fake_supervisor(
        follow_up_tx,
        250,
        FakeKill::Success,
        FakeChild::FinalTurnFails,
    )
    .await;
    spawn_child(&sessions).await;
    sessions.mark_replied("child-live").await;
    sessions.notify_turn_done();

    let extra = tokio::time::timeout(Duration::from_secs(2), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "a replied child must not deliver a notice");
    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(entries[0].status, "error");
}

/// An unreachable child delivers the failure row, not the no-reply notice.
/// A short tick keeps the paused clock's auto-advance from firing link
/// deadlines while the real-socket round trips are in flight.
#[tokio::test(start_paused = true)]
async fn an_unreachable_child_delivers_the_failure_notice_instead_of_no_reply() {
    tokio::spawn(async {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
        }
    });
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Unreachable)
            .await;
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();

    let follow_up = tokio::time::timeout(Duration::from_secs(3_600), follow_up_rx.recv())
        .await
        .expect("the unreachable child must deliver its failure notice")
        .expect("follow_up channel open");
    assert_failure_row(&follow_up, &handle.rlm_child_id, "Child worker unreachable");
    // Exactly one failure row lands: the give-up claims once.
    let extra = tokio::time::timeout(Duration::from_millis(300), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "no second notice may arrive");
}

/// Hand-off into the unreachable give-up's poll: the fake parks the
/// give-up poll, the test lands the reader's verdict, the fake releases
/// the poll. Only the lost-claim test uses these.
static PROMPT_FAILURE_STARTED: tokio::sync::Notify = tokio::sync::Notify::const_new();
static PROMPT_FAILURE_RELEASE: tokio::sync::Notify = tokio::sync::Notify::const_new();

static GIVE_UP_POLL: tokio::sync::Notify = tokio::sync::Notify::const_new();
static VERDICT_LANDED: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// A reader's refresh can settle the child between the watcher's settle
/// read and its unreachable give-up claim: the claim keeps that verdict,
/// and the watcher still runs its settle tail.
#[tokio::test(start_paused = true)]
async fn a_verdict_landing_inside_the_unreachable_give_up_still_runs_the_settle_tail() {
    tokio::spawn(async {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
        }
    });
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) = sessions_with_fake_supervisor(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::UnreachableUntilVerdict,
    )
    .await;
    let settled = sessions.settle_notified();
    spawn_child(&sessions).await;
    sessions.notify_turn_done();

    GIVE_UP_POLL.notified().await;
    {
        // What a `collect`'s `refresh_record` writes when the worker
        // answers idle.
        let record = Arc::clone(&sessions.inner.children.lock().await[0]);
        let mut record = record.lock().await;
        record.settled_status = Some("done");
    }
    VERDICT_LANDED.notify_one();

    tokio::time::timeout(Duration::from_secs(3_600), settled)
        .await
        .expect("the settle funnel fires for the reader's verdict");
    assert!(!sessions.any_running().await, "the run must mark settled");
    let notice = follow_up_rx
        .try_recv()
        .expect("the verdict's no-reply notice is still owed");
    assert_eq!(
        notice["customMessage"]["customType"],
        "rlm_child_terminal_notice"
    );
}

/// The review's C1 interleaving: a `collect` that lands inside the
/// prompt-failure window (after the detached task flips
/// `prompt_admitted`, before the retry verdict) reads the alive-but-idle
/// worker as `done` — `refresh_record`'s admission-window misread, not a
/// settle verdict. The prompt arm's failure claim must ignore it: the
/// failure row still lands, the roster reports `error`, the hook fires,
/// and the run marks settled (post-#3171 an unsettled failed run parks
/// `waitForRlmQuiescence` forever).
#[tokio::test]
async fn a_collect_inside_the_prompt_failure_window_does_not_swallow_the_failure_row() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::PromptFails)
            .await;
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    sessions.set_settle_hook(Arc::new(move || {
        let _ = hook_tx.send(());
    }));
    let (spawned, child_id) = begin_failed_spawn(&sessions).await;
    // The window's entry condition, set deterministically: the detached
    // prompt task flips `prompt_admitted` BEFORE its first
    // `prompt_child` (host.rs), and the collect below races that task in
    // production. With the flag set and the worker alive-but-idle, the
    // real collect path scores the misread.
    sessions.inner.children.lock().await[0]
        .lock()
        .await
        .prompt_admitted = true;
    let misread = sessions
        .collect(vec![child_id.clone()], 1_000)
        .await
        .expect("collect inside the window");
    assert_eq!(
        misread[0].status, "done",
        "the premise: the collect misreads the pre-prompt idle worker as done"
    );

    // The task prompt now runs and fails (prompt + retry) inside the
    // window the collect already scored.
    refuse_failed_spawn(spawned).await;

    let follow_up = tokio::time::timeout(Duration::from_secs(10), follow_up_rx.recv())
        .await
        .expect("the failure row must land despite the collect's misread")
        .expect("follow_up channel open");
    assert_failure_row(
        &follow_up,
        &child_id,
        "prompt RLM child session child-live: refused by the fake supervisor",
    );
    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(entries[0].status, "error");
    tokio::time::timeout(Duration::from_secs(10), hook_rx.recv())
        .await
        .expect("the settle funnel fires despite the misread")
        .expect("hook channel open");
    assert!(
        !sessions.any_running().await,
        "the failed run must mark settled: an unsettled run parks the quiescence barrier"
    );
    let extra = tokio::time::timeout(Duration::from_millis(300), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "no second notice may arrive");
}

/// A child that already replied still delivers the failure row when its
/// task prompt fails: TS sends the catch-arm row regardless of reply
/// count (`agent-session.ts:13026-13045`).
#[tokio::test]
async fn a_replied_child_whose_prompt_fails_still_delivers_the_failure_row() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::PromptFails)
            .await;
    let (spawned, child_id) = begin_failed_spawn(&sessions).await;
    sessions.mark_replied("child-live").await;
    refuse_failed_spawn(spawned).await;

    let follow_up = tokio::time::timeout(Duration::from_secs(10), follow_up_rx.recv())
        .await
        .expect("the replied child must still deliver the failure row")
        .expect("follow_up channel open");
    assert_failure_row(
        &follow_up,
        &child_id,
        "prompt RLM child session child-live: refused by the fake supervisor",
    );
    let extra = tokio::time::timeout(Duration::from_millis(300), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "no second notice may arrive");
}

/// A parent counts as running while any descendant runs: the child's own
/// turn is done, but while it reports a running grandchild the parent's
/// row stays `running` and the registry keeps the parent's summary busy;
/// once the grandchild finishes, the child settles and the parent is
/// quiet again.
#[tokio::test]
async fn a_child_with_a_running_grandchild_keeps_the_parent_running() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let grandchild = Arc::new(FakeChildSubagents {
        running: AtomicBool::new(true),
        ..FakeChildSubagents::default()
    });
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::clone(&grandchild),
    )
    .await;
    assert!(!sessions.has_running_children());
    let mut running = sessions.subscribe_running();
    let settled = sessions.settle_notified();
    spawn_child(&sessions).await;
    assert!(running.has_changed().unwrap());
    assert!(*running.borrow_and_update());
    sessions.notify_turn_done();

    // The child is idle on its own, but its grandchild still runs: the
    // watcher's idle wait holds for the child's whole subtree.
    wait_for_quiescent_wait(&grandchild).await;
    let roster = sessions.list_subagents().await.expect("child roster");
    assert_eq!(roster[0].assignment_status, "running");
    assert_ne!(roster[0].status, "running");
    assert!(sessions.has_running_children());

    grandchild.running.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), settled)
        .await
        .expect("the child settles once its grandchild finished");
    let roster = sessions.list_subagents().await.expect("child roster");
    assert_eq!(roster[0].status, "completed");
    assert!(!sessions.has_running_children());
    assert!(running.has_changed().unwrap());
}

/// A `collect` with a timeout waits out a child whose own turn ended but
/// whose grandchild still runs, instead of answering `running` at once.
#[tokio::test]
async fn collect_with_a_timeout_waits_for_a_running_grandchild() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let grandchild = Arc::new(FakeChildSubagents {
        running: AtomicBool::new(true),
        ..FakeChildSubagents::default()
    });
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::clone(&grandchild),
    )
    .await;
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    wait_for_quiescent_wait(&grandchild).await;

    let mut collecting = Box::pin(sessions.collect(vec![handle.rlm_child_id.clone()], 10_000));
    std::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(collecting.as_mut(), cx).is_pending(),
            "collect returned while the grandchild still ran"
        );
        std::task::Poll::Ready(())
    })
    .await;
    grandchild.running.store(false, Ordering::SeqCst);
    let results = collecting.await.expect("collect the child");
    assert_eq!(results[0].status, "done");
    assert!(!grandchild.running.load(Ordering::SeqCst));
}

/// Wait until the settle watcher parks in the child's subtree idle wait.
async fn wait_for_quiescent_wait(child_subagents: &FakeChildSubagents) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while child_subagents.quiescent_waits.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < deadline,
            "the watcher never waited for the child's subtree"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// One child row exists and is running before the close tests run.
async fn one_running_child(sessions: &SupervisorChildSessions) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let entries = sessions.list_subagents().await.expect("child roster");
        if let Some(row) = entries.first() {
            assert_eq!(row.assignment_status, "running");
            return;
        }
        assert!(Instant::now() < deadline, "child row never appeared");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `close_children`: every tracked child is stopped through the supervisor — a plain stop, no
/// delete marker; the registry empties, and no terminal notice is owed.
#[tokio::test]
async fn close_children_stops_the_child_and_clears_the_roster() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    // A long idle keeps the child mid-run while the close fires, so the
    // settle watcher is parked instead of raced.
    let (sessions, mut kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 10_000, FakeKill::Success, FakeChild::Healthy)
            .await;
    spawn_child(&sessions).await;
    sessions.notify_turn_done();
    one_running_child(&sessions).await;

    sessions
        .close_children(ChildCloseReason::Killed)
        .await
        .expect("close children");

    // The stop carried no delete marker: the spawn edge survives (TS
    // `closeSessionOnce` archives; only `recordRlmSubagentDeletion`
    // tombstones).
    let kill = kill_rx
        .recv()
        .await
        .expect("the close must stop the child through the supervisor");
    assert_eq!(kill["type"], "kill");
    assert!(
        !kill.to_string().contains("rlmLedgerDelete"),
        "the replacement close is a stop, not a delete"
    );
    let entries = sessions.list_subagents().await.expect("child roster");
    assert!(
        entries.is_empty(),
        "the closed child stays listed: {entries:?}"
    );
    let extra = tokio::time::timeout(Duration::from_millis(300), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "a closed child must not deliver a notice");
}

#[tokio::test]
async fn close_children_treats_an_already_gone_child_as_a_no_op() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) = sessions_with_fake_supervisor(
        follow_up_tx,
        10_000,
        FakeKill::UnknownSession,
        FakeChild::Healthy,
    )
    .await;
    spawn_child(&sessions).await;
    sessions.notify_turn_done();
    one_running_child(&sessions).await;

    sessions
        .close_children(ChildCloseReason::Killed)
        .await
        .expect("an already-gone child must not fail the close");

    let entries = sessions.list_subagents().await.expect("child roster");
    assert!(
        entries.is_empty(),
        "the gone child stays listed: {entries:?}"
    );
}

#[tokio::test]
async fn close_children_keeps_a_failed_child_tracked() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 10_000, FakeKill::Failure, FakeChild::Healthy)
            .await;
    spawn_child(&sessions).await;
    sessions.notify_turn_done();
    one_running_child(&sessions).await;

    let error = sessions
        .close_children(ChildCloseReason::Killed)
        .await
        .expect_err("a real close failure must propagate");
    assert!(
        format!("{error:#}").contains("kill refused"),
        "the close error must surface the kill failure: {error:#}"
    );

    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(entries.len(), 1, "the failed child stays tracked for retry");
}

/// A failed close must not poison the cancel record (PR #3680 review
/// finding): a real kill failure undoes the pre-kill settle, so the
/// still-live child stays `running` on the roster and a later successful
/// stop still records the parent -> child cancel edge (the failed attempt
/// never reached the recorder's dedup).
#[tokio::test]
async fn a_failed_close_leaves_the_child_running_and_can_still_record_its_cancel() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    *child_subagents.live_state.lock().unwrap() = Some(json!({"isStreaming": true}));
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        10_000,
        FakeKill::Failure,
        FakeChild::Healthy,
        Arc::clone(&child_subagents),
    )
    .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    one_running_child(&sessions).await;

    let error = sessions
        .close_children(ChildCloseReason::Killed)
        .await
        .expect_err("a real close failure must propagate");
    assert!(
        format!("{error:#}").contains("kill refused"),
        "the close error must surface the kill failure: {error:#}"
    );

    // The kill never landed, so the pre-kill settle is undone: the
    // still-live child is not misreported cancelled.
    // Without an observation, assignment remains running but execution is unknown.
    let unobserved = sessions.list_subagents().await.expect("unobserved roster");
    assert_eq!(unobserved[0].assignment_status, "running");
    assert_eq!(unobserved[0].status, "unknown");
    assert_eq!(
        unobserved[0].observation.as_ref().unwrap()["available"],
        false
    );
    // Both the direct worker and the supervisor roster now report this live turn.
    child_subagents.peers.lock().unwrap().push(json!({
        "activeSessionId": "child-live", "isStreaming": true
    }));
    sessions
        .inner
        .child_state("child-live")
        .await
        .expect("live worker observation");
    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(entries.len(), 1, "the failed child stays tracked for retry");
    assert_eq!(entries[0].assignment_status, "running");
    assert_eq!(entries[0].observation.as_ref().unwrap()["available"], true);
    assert_eq!(
        entries[0].status, "running",
        "a failed close must not mark the still-live child cancelled: {entries:?}"
    );
    assert!(
        cancelled_lines(&ledger_path).is_empty(),
        "a failed close records no cancel edge: {:?}",
        cancelled_lines(&ledger_path)
    );

    // A later successful stop still records the edge: the failed attempt
    // did not burn the at-most-once claim.
    assert!(sessions.cancel_child_run(&handle.rlm_child_id).await);
    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::UserCancel
        )],
        "the retry records the cancel edge the failed close could not"
    );
}

/// A delete-killed live run never records a clean return beside its cancel
/// line (PR #3680 review finding, the delete twin of the close case): the
/// delete settles the row `cancelled` before its kill, so a settle watcher
/// that wakes inside the delete's in-flight window cannot refresh the
/// killed run back to `done` and append a `child_returned` after the cancel
/// line.
#[tokio::test]
async fn a_deleted_live_child_never_records_a_clean_return() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    // A live grandchild holds the watcher's wait slice open until the test
    // releases it inside the delete's in-flight window.
    child_subagents.running.store(true, Ordering::SeqCst);
    let (sessions, mut kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        100,
        FakeKill::SuccessAfterMs(2_000),
        FakeChild::Healthy,
        Arc::clone(&child_subagents),
    )
    .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    sessions.set_settle_hook(Arc::new(move || {
        let _ = hook_tx.send(());
    }));
    let handle = spawn_child(&sessions).await;
    // The child committed one request before the delete cut it short (its
    // own ledger carries the line the parent's return recording reads).
    std::fs::create_dir_all(&handle.session_dir).expect("child session dir");
    let mut child_ledger = String::new();
    child_ledger.push_str("{\"type\":\"session_registered\",\"session_id\":\"child-file\"}");
    child_ledger.push('\n');
    child_ledger.push_str("{\"type\":\"request_finished\",\"request_id\":\"child-request-1\"}");
    child_ledger.push('\n');
    std::fs::write(
        Path::new(&handle.session_dir).join("semantic-edges.jsonl"),
        child_ledger,
    )
    .expect("write the child ledger");
    sessions.notify_turn_done();
    // Wait until the watcher is provably parked inside its held quiescence
    // wait (the fake counts them), never on one still starting.
    let parked = Instant::now() + Duration::from_secs(10);
    while child_subagents.quiescent_waits.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < parked,
            "the watcher never parked in its wait slice"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The delete reads the live row (and settles it `cancelled`) before it
    // sends the kill; the kill answers only after its delay. Waiting for
    // the kill therefore proves the pre-kill settle landed, and releasing
    // the parked watcher now puts its refresh inside the delete's window.
    let delete_sessions = sessions.clone();
    let child_id = handle.rlm_child_id.clone();
    let delete = tokio::spawn(async move { delete_sessions.delete_subagent(child_id).await });
    let kill = kill_rx.recv().await.expect("the delete reaches the kill");
    assert_eq!(kill["type"], "kill");
    child_subagents.running.store(false, Ordering::SeqCst);
    // The watcher parks while the delete owns the terminal edge (the kill
    // window holds the `retiring` mark), so the hook fires exactly once:
    // the delete's own `finish_retirement`. A watcher that ran its settle
    // tail into the window would fire here too and deliver the terminal
    // notice before the delete's cancel overwrites the verdict.
    tokio::time::timeout(Duration::from_secs(15), hook_rx.recv())
        .await
        .expect("the delete fires the settle hook")
        .expect("hook channel open");
    assert!(
        hook_rx.try_recv().is_err(),
        "the watcher must not fire the settle hook inside the delete's kill window"
    );
    delete
        .await
        .expect("the delete task joins")
        .expect("delete the running child");

    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::ParentDelete
        )],
        "the delete's kill records the cancel line"
    );
    assert!(
        returned_children(&ledger_path).is_empty(),
        "a deleted live child never records a child_returned line: {:?}",
        returned_children(&ledger_path)
    );
}

/// A failed delete must not poison the cancel record (PR #3680 review
/// finding): the pre-kill settle is undone on a real kill failure, so the
/// still-live child stays `running` and a later confirmed cancel still
/// records the parent -> child edge.
#[tokio::test]
async fn a_failed_delete_leaves_the_child_running_and_can_still_record_its_cancel() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    *child_subagents.live_state.lock().unwrap() = Some(json!({"isStreaming": true}));
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        10_000,
        FakeKill::Failure,
        FakeChild::Healthy,
        Arc::clone(&child_subagents),
    )
    .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let handle = spawn_child(&sessions).await;

    let error = sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect_err("a real delete failure must propagate");
    assert!(
        format!("{error:#}").contains("kill refused"),
        "the delete error must surface the kill failure: {error:#}"
    );

    // The kill never landed, so the pre-kill settle is undone: the
    // still-live child is not misreported cancelled.
    // Without an observation, assignment remains running but execution is unknown.
    let unobserved = sessions.list_subagents().await.expect("unobserved roster");
    assert_eq!(unobserved[0].assignment_status, "running");
    assert_eq!(unobserved[0].status, "unknown");
    assert_eq!(
        unobserved[0].observation.as_ref().unwrap()["available"],
        false
    );
    // Both the direct worker and the supervisor roster now report this live turn.
    child_subagents.peers.lock().unwrap().push(json!({
        "activeSessionId": "child-live", "isStreaming": true
    }));
    sessions
        .inner
        .child_state("child-live")
        .await
        .expect("live worker observation");
    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(entries.len(), 1, "the failed child stays tracked for retry");
    assert_eq!(entries[0].assignment_status, "running");
    assert_eq!(entries[0].observation.as_ref().unwrap()["available"], true);
    assert_eq!(
        entries[0].status, "running",
        "a failed delete must not mark the still-live child cancelled: {entries:?}"
    );
    assert!(
        cancelled_lines(&ledger_path).is_empty(),
        "a failed delete records no cancel edge: {:?}",
        cancelled_lines(&ledger_path)
    );

    // A later confirmed stop still records the edge: the failed attempt
    // did not burn the at-most-once claim.
    assert!(sessions.cancel_child_run(&handle.rlm_child_id).await);
    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::UserCancel
        )],
        "the retry records the cancel edge the failed delete could not"
    );
}

/// A failed delete leaves the child watched (PR #3680 review finding): the
/// delete mark is cleared, so the watcher survives and the still-live run's
/// later natural settle still lands its return edge.
#[tokio::test]
async fn a_failed_delete_leaves_the_child_watched_and_its_later_return_able_to_land() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    // A live grandchild keeps the run unsettled (and so its watcher alive)
    // until the test releases it.
    child_subagents.running.store(true, Ordering::SeqCst);
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        100,
        FakeKill::Failure,
        FakeChild::Healthy,
        Arc::clone(&child_subagents),
    )
    .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let handle = spawn_child(&sessions).await;
    // The child committed one request: the return recording reads it from
    // the child's own ledger.
    std::fs::create_dir_all(&handle.session_dir).expect("child session dir");
    let mut child_ledger = String::new();
    child_ledger.push_str("{\"type\":\"session_registered\",\"session_id\":\"child-file\"}");
    child_ledger.push('\n');
    child_ledger.push_str("{\"type\":\"request_finished\",\"request_id\":\"child-request-1\"}");
    child_ledger.push('\n');
    std::fs::write(
        Path::new(&handle.session_dir).join("semantic-edges.jsonl"),
        child_ledger,
    )
    .expect("write the child ledger");
    sessions.notify_turn_done();
    one_running_child(&sessions).await;

    let error = sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect_err("a real delete failure must propagate");
    assert!(
        format!("{error:#}").contains("kill refused"),
        "the delete error must surface the kill failure: {error:#}"
    );

    // The child finishes naturally afterwards: its watcher is still live,
    // so the return edge lands instead of being dropped by a stale mark.
    child_subagents.running.store(false, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(10);
    while returned_children(&ledger_path).is_empty() {
        assert!(
            Instant::now() < deadline,
            "the settled child never recorded its return: {:?}",
            returned_children(&ledger_path)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        returned_children(&ledger_path),
        vec!["child-file".to_string()],
        "the failed delete must not drop the child's terminal edge"
    );
}

/// A run that settles inside a failed delete's kill window must not lose
/// its return edge (PR #3680 review finding): the watcher parks while the
/// kill is pending, so the refused kill leaves it alive to settle the run
/// normally once the mark clears. A watcher that ran its settle tail into
/// the window would exit before the failure lands, stranding a settled row
/// with no watcher and no ledger line.
#[tokio::test]
async fn a_child_settling_inside_a_failed_deletes_kill_window_still_lands_its_return() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    // A live grandchild holds the watcher's wait slice open until the test
    // releases it inside the delete's in-flight kill window.
    child_subagents.running.store(true, Ordering::SeqCst);
    let (sessions, mut kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        100,
        FakeKill::FailureAfterMs(1_500),
        FakeChild::Healthy,
        Arc::clone(&child_subagents),
    )
    .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let handle = spawn_child(&sessions).await;
    // The child committed one request: the return recording reads it from
    // the child's own ledger.
    std::fs::create_dir_all(&handle.session_dir).expect("child session dir");
    let mut child_ledger = String::new();
    child_ledger.push_str("{\"type\":\"session_registered\",\"session_id\":\"child-file\"}");
    child_ledger.push('\n');
    child_ledger.push_str("{\"type\":\"request_finished\",\"request_id\":\"child-request-1\"}");
    child_ledger.push('\n');
    std::fs::write(
        Path::new(&handle.session_dir).join("semantic-edges.jsonl"),
        child_ledger,
    )
    .expect("write the child ledger");
    sessions.notify_turn_done();
    // Wait until the watcher is provably parked inside its held quiescence
    // wait (the fake counts them), never on one still starting.
    let parked = Instant::now() + Duration::from_secs(10);
    while child_subagents.quiescent_waits.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < parked,
            "the watcher never parked in its wait slice"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The delete sets `retiring` and sends the kill; the fake holds its
    // refusal, so releasing the watcher now puts its refresh inside the
    // delete's kill window: the run settles while the kill is pending.
    let delete_sessions = sessions.clone();
    let child_id = handle.rlm_child_id.clone();
    let delete = tokio::spawn(async move { delete_sessions.delete_subagent(child_id).await });
    let kill = kill_rx.recv().await.expect("the delete reaches the kill");
    assert_eq!(kill["type"], "kill");
    child_subagents.running.store(false, Ordering::SeqCst);

    let error = delete
        .await
        .expect("the delete task joins")
        .expect_err("the refused kill must fail the delete");
    assert!(
        format!("{error:#}").contains("kill refused"),
        "the delete error must surface the kill failure: {error:#}"
    );
    assert!(
        cancelled_lines(&ledger_path).is_empty(),
        "a failed delete records no cancel edge: {:?}",
        cancelled_lines(&ledger_path)
    );

    // The watcher survived the window (it parked instead of running its
    // tail into it), so the run's natural settle still lands its return
    // edge. A watcher that exited inside the window would leave this line
    // forever absent.
    let deadline = Instant::now() + Duration::from_secs(10);
    while returned_children(&ledger_path).is_empty() {
        assert!(
            Instant::now() < deadline,
            "a run settling inside a failed delete's kill window never recorded its return: {:?}",
            returned_children(&ledger_path)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        returned_children(&ledger_path),
        vec!["child-file".to_string()],
        "the parked watcher settles the run once the delete fails"
    );
}

#[tokio::test]
async fn a_replied_child_gets_no_terminal_notice() {
    let (follow_up_tx, mut follow_up_rx) = mpsc::unbounded_channel();
    // A slow idle wait keeps the child "running" while the test marks
    // the reply.
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 250, FakeKill::Success, FakeChild::Healthy)
            .await;
    let handle = spawn_child(&sessions).await;
    assert!(!handle.rlm_child_id.is_empty());
    sessions.mark_replied("child-live").await;
    sessions.notify_turn_done();

    let extra = tokio::time::timeout(std::time::Duration::from_secs(2), follow_up_rx.recv()).await;
    assert!(extra.is_err(), "a replied child must not deliver a notice");
}

/// A parent recorder over a fresh ledger seeded with one started request:
/// the replay restores the in-flight turn, so the registry's cancel sites
/// anchor the `child_cancelled` line to it (the same wire a live parent
/// session would carry mid-turn).
fn seeded_parent_recorder() -> (
    std::sync::Arc<SemanticEdgeRecorder>,
    PathBuf,
    tempfile::TempDir,
) {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let ledger_path = dir.path().join("semantic-edges.jsonl");
    let registered = SemanticEdgeLedgerEvent::SessionRegistered {
        session_id: "parent-lead".to_string(),
        parent_session_id: None,
        spawned_by_request_id: None,
    };
    let started = SemanticEdgeLedgerEvent::RequestStarted {
        request_id: "parent-request-1".to_string(),
        session_id: "parent-lead".to_string(),
        compaction_id: None,
    };
    std::fs::write(
        &ledger_path,
        format!(
            "{}\n{}\n",
            serde_json::to_string(&registered).expect("serialize the registered line"),
            serde_json::to_string(&started).expect("serialize the started line")
        ),
    )
    .expect("write the seeded ledger");
    let recorder = std::sync::Arc::new(SemanticEdgeRecorder::open(SemanticEdgeIdentity {
        session_id: "parent-lead".to_string(),
        ledger_path: Some(ledger_path.clone()),
        parent_session_id: None,
        spawned_by_request_id: None,
    }));
    (recorder, ledger_path, dir)
}

/// The `child_cancelled` lines of one parent ledger: (parent, child,
/// cancelling request, which parent-side action cancelled the run).
fn cancelled_lines(path: &Path) -> Vec<(String, String, String, ChildCancelledReason)> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<SemanticEdgeLedgerEvent>(line.trim()).ok())
        .filter_map(|event| match event {
            SemanticEdgeLedgerEvent::ChildCancelled {
                session_id,
                child_session_id,
                request_id,
                reason,
            } => Some((session_id, child_session_id, request_id, reason)),
            _ => None,
        })
        .collect()
}

/// A user-cancelled run records the cancel edge (nano-rlm #202's
/// `agent.cancel` gap): one parent -> child line anchored to the
/// parent's in-flight request, never a clean disappearance.
#[tokio::test]
async fn a_cancelled_run_records_the_cancel_edge_on_the_parent_ledger() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 10_000, FakeKill::Success, FakeChild::Healthy)
            .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let handle = spawn_child(&sessions).await;

    assert!(sessions.cancel_child_run(&handle.rlm_child_id).await);

    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::UserCancel
        )],
        "the cancel lands as a parent -> child edge anchored to the cancelling request"
    );
}

/// A delete that kills a live run records the cancel edge (nano-rlm
/// #202's delete gap): a deleted child leaves a cancel line, not a clean
/// disappearance.
#[tokio::test]
async fn a_deleted_running_child_records_the_cancel_edge_on_the_parent_ledger() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 10_000, FakeKill::Success, FakeChild::Healthy)
            .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let handle = spawn_child(&sessions).await;

    let deleted = sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect("delete the running child");
    assert_eq!(deleted.outcome, Some("deleted"));

    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::ParentDelete
        )],
        "the running child's delete lands as a parent -> child cancel edge"
    );
}

/// A settled child's delete leaves its return alone (PR #3680 review
/// finding): the delete marks only a LIVE run, so a `done` child torn down
/// inside its settle grace still records its `child_returned` edge, and no
/// `child_cancelled` line is written for it.
#[tokio::test]
async fn a_deleted_settled_child_still_records_its_return() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let handle = spawn_child(&sessions).await;
    // The child committed one request: the return recording reads it.
    std::fs::create_dir_all(&handle.session_dir).expect("child session dir");
    let mut child_ledger = String::new();
    child_ledger.push_str("{\"type\":\"session_registered\",\"session_id\":\"child-file\"}");
    child_ledger.push('\n');
    child_ledger.push_str("{\"type\":\"request_finished\",\"request_id\":\"child-request-1\"}");
    child_ledger.push('\n');
    std::fs::write(
        Path::new(&handle.session_dir).join("semantic-edges.jsonl"),
        child_ledger,
    )
    .expect("write the child ledger");
    sessions.notify_turn_done();
    // The watcher's refresh settles the row `done`; the tail that records
    // the return follows the settle grace.
    let settled = Instant::now() + Duration::from_secs(10);
    loop {
        let entries = sessions.list_subagents().await.expect("child roster");
        if entries.iter().any(|entry| entry.status == "completed") {
            break;
        }
        assert!(Instant::now() < settled, "child never settled: {entries:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Delete inside the grace window: the run already settled, so no cancel
    // is owed and the return must still land.
    sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect("delete the settled child");

    let deadline = Instant::now() + Duration::from_secs(10);
    while returned_children(&ledger_path).is_empty() {
        assert!(
            Instant::now() < deadline,
            "the deleted settled child never recorded its return"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        returned_children(&ledger_path),
        vec!["child-file".to_string()],
        "a settled child's delete leaves its return edge alone"
    );
    assert!(
        cancelled_lines(&ledger_path).is_empty(),
        "a settled child's delete records no cancel line: {:?}",
        cancelled_lines(&ledger_path)
    );
}

/// Only a cut-short run cancels: a delete of an already-settled child
/// records no cancel line (its return line already stands).
#[tokio::test]
async fn a_deleted_settled_child_records_no_cancel_edge() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let entries = sessions.list_subagents().await.expect("child roster");
        if entries.iter().any(|entry| entry.status == "completed") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "child never settled: {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect("delete the settled child");

    assert_eq!(
        cancelled_lines(&ledger_path),
        Vec::new(),
        "a settled child's delete records no cancel edge"
    );
}

/// A close that kills a live run records the cancel edge (nano-rlm
/// #202's killed-child gap): a killed child leaves a cancel line on the
/// parent's ledger, not a clean disappearance.
#[tokio::test]
async fn a_closed_running_child_records_the_cancel_edge_on_the_parent_ledger() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 10_000, FakeKill::Success, FakeChild::Healthy)
            .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let _handle = spawn_child(&sessions).await;

    sessions
        .close_children(ChildCloseReason::Killed)
        .await
        .expect("close the running child");

    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::CloseKilled
        )],
        "the close's kill lands as a parent -> child cancel edge"
    );
}

/// A daemon lifecycle close (Shutdown: the daemon stops; Replaced: an
/// upgrade) kills running children but records NO `child_cancelled` line
/// (PR #3680 review finding): the close walk is not a parent action, so a
/// permanent cancel line anchored to the parent's stale last request would
/// mislabel every restart or upgrade as a parent cancel — and the
/// at-most-once dedup would make it permanent. Only the session-kill
/// close (`Killed`) is the parent's cancel.
#[tokio::test]
async fn a_lifecycle_close_records_no_cancel_line() {
    for reason in [ChildCloseReason::Shutdown, ChildCloseReason::Replaced] {
        let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
        let (sessions, _kill_rx) = sessions_with_fake_supervisor(
            follow_up_tx,
            10_000,
            FakeKill::Success,
            FakeChild::Healthy,
        )
        .await;
        let (recorder, ledger_path, _dir) = seeded_parent_recorder();
        sessions.set_semantic_edges(Some(recorder));
        let _handle = spawn_child(&sessions).await;

        sessions
            .close_children(reason)
            .await
            .expect("close the running child");

        assert!(
            cancelled_lines(&ledger_path).is_empty(),
            "a lifecycle close records no cancel line: {:?}",
            cancelled_lines(&ledger_path)
        );
        assert!(
            returned_children(&ledger_path).is_empty(),
            "a lifecycle close records no return line either: {:?}",
            returned_children(&ledger_path)
        );
        // The close still tore the live child down.
        let entries = sessions.list_subagents().await.expect("child roster");
        assert!(
            entries.is_empty() || entries.iter().all(|entry| entry.status == "cancelled"),
            "the lifecycle close tore the running child down: {entries:?}"
        );
    }
}

/// The `child_returned` lines of one parent ledger: the child session ids.
fn returned_children(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<SemanticEdgeLedgerEvent>(line.trim()).ok())
        .filter_map(|event| match event {
            SemanticEdgeLedgerEvent::ChildReturned {
                child_session_id, ..
            } => Some(child_session_id),
            _ => None,
        })
        .collect()
}

/// A refused abort leaves the child live and records no cancel edge (PR
/// #3680 review finding): the ledger mirrors the confirmed effect only, and
/// the row is left live so a later confirmed cancel can still record the
/// parent -> child edge.
#[tokio::test]
async fn a_refused_abort_leaves_the_child_running_and_a_later_cancel_records_the_edge() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    child_subagents.abort_fails.store(true, Ordering::SeqCst);
    // Running is an observed execution fact in the private runtime, not an
    // inference from an unsettled assignment. The fake child is genuinely busy.
    *child_subagents.live_state.lock().unwrap() = Some(json!({"isStreaming": true}));
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        10_000,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::clone(&child_subagents),
    )
    .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    sessions.set_settle_hook(Arc::new(move || {
        let _ = hook_tx.send(());
    }));
    let rows: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink_rows = Arc::clone(&rows);
    sessions.set_child_update_sink(Arc::new(move |child| {
        sink_rows.lock().unwrap().push(child);
    }));
    let handle = spawn_child(&sessions).await;
    *child_subagents.peers.lock().unwrap() = vec![json!({
        "activeSessionId":"child-live", "rlmChildId":handle.rlm_child_id, "isStreaming":true
    })];
    sessions.inner.child_state("child-live").await.unwrap();

    assert!(
        !sessions.cancel_child_run(&handle.rlm_child_id).await,
        "a refused abort answers the confirmed effect: the cancel did not happen"
    );

    // The refused abort never reached the child: the row stays live, so a
    // still-running child is not misreported cancelled.
    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(
        entries[0].status, "running",
        "a refused abort must leave the live child running: {entries:?}"
    );
    assert_eq!(
        cancelled_lines(&ledger_path),
        Vec::new(),
        "a refused abort records no cancel line"
    );
    // And nothing is released: the quiescence barrier still sees the live
    // child, so no settle hook may fire.
    assert!(
        sessions.any_running().await,
        "a refused abort must leave the run unsettled"
    );
    // The restored row is republished: subscribers must not keep showing a
    // live child as cancelled.
    {
        let published = rows.lock().unwrap();
        assert_eq!(
            published.last().and_then(|row| row["status"].as_str()),
            Some("running"),
            "the refused abort must republish the live row: {published:?}"
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), hook_rx.recv())
            .await
            .is_err(),
        "a refused abort must not fire the settle hook"
    );

    // A later confirmed cancel records the edge the refused attempt could
    // not: the settle undo left the row live for the retry.
    child_subagents.abort_fails.store(false, Ordering::SeqCst);
    assert!(
        sessions.cancel_child_run(&handle.rlm_child_id).await,
        "the retry claims the still-live row"
    );
    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::UserCancel
        )],
        "the confirmed retry records the cancel edge"
    );
    tokio::time::timeout(Duration::from_secs(5), hook_rx.recv())
        .await
        .expect("the confirmed cancel fires the settle hook")
        .expect("hook channel open");
}

/// A close-killed live run never records a clean return beside its cancel
/// line (PR #3680 review finding): the settle watcher is parked inside
/// its wait slice when the close kills the child, and its post-kill
/// refresh used to re-mark the killed run `done` — appending a
/// `child_returned` line after the cancel line. The close now settles
/// the row `cancelled`, so the parked watcher's tail records nothing.
/// A registry-set `cancelled` survives the watcher's grace re-check (PR
/// #3680 review finding): the watcher parks mid-slice while a user cancel
/// settles the row with its abort in flight; a busy child at the grace
/// re-check used to wipe the registry's settle, refresh the still-live run
/// back to `done`, and record a clean `child_returned` beside the cancel
/// line. The re-check now wipes only a watcher-set `done` (the admission
/// race it exists for), so the registry's cancelled stands: the cut-short
/// run never records a return, and the roster keeps reporting it
/// cancelled.
#[tokio::test]
async fn a_cancelled_rows_settle_survives_the_watcher_grace_recheck() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    // The child reads busy for the whole cancel window: the parked watcher
    // provably hits its grace re-check with the row already settled
    // `cancelled` by the registry and the child still busy — the exact
    // wipe window.
    child_subagents.live_state.lock().unwrap().replace(json!({
        "isStreaming": true,
        "hasRunningSubagents": false,
        "sessionActions": { "queuedCount": 0 },
    }));
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        100,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::clone(&child_subagents),
    )
    .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    sessions.set_settle_hook(Arc::new(move || {
        let _ = hook_tx.send(());
    }));
    let handle = spawn_child(&sessions).await;
    // The child committed one request before the cancel cut it short (the
    // wrongly-recorded return would read this line).
    std::fs::create_dir_all(&handle.session_dir).expect("child session dir");
    let mut child_ledger = String::new();
    child_ledger.push_str("{\"type\":\"session_registered\",\"session_id\":\"child-file\"}");
    child_ledger.push('\n');
    child_ledger.push_str("{\"type\":\"request_finished\",\"request_id\":\"child-request-1\"}");
    child_ledger.push('\n');
    std::fs::write(
        Path::new(&handle.session_dir).join("semantic-edges.jsonl"),
        child_ledger,
    )
    .expect("write the child ledger");
    sessions.notify_turn_done();

    assert!(
        sessions.cancel_child_run(&handle.rlm_child_id).await,
        "the confirmed cancel claims the running child"
    );

    // Hold the busy state through at least two post-cancel watcher slices
    // (each wait_for_idle is one slice): every post-cancel slice observes
    // the registry-set `cancelled` while the child still reads busy — the
    // exact grace re-check window — so the buggy unconditional wipe fires
    // here deterministically. Only then let the run read idle.
    let slices_before_cancel = child_subagents.quiescent_waits.load(Ordering::SeqCst);
    let cancelled_and_busy = Instant::now() + Duration::from_secs(15);
    while child_subagents.quiescent_waits.load(Ordering::SeqCst) < slices_before_cancel + 2 {
        assert!(
            Instant::now() < cancelled_and_busy,
            "the watcher never re-checked the cancelled row while busy"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    child_subagents.live_state.lock().unwrap().take();

    // The cancel path fires the hook, the watcher's settle tail fires it
    // again once the child reads idle; the second fire bounds the tail.
    let mut hook_fires = 0;
    while hook_fires < 2 {
        tokio::time::timeout(Duration::from_secs(15), hook_rx.recv())
            .await
            .expect("the cancel and the watcher's tail each fire the hook")
            .expect("hook channel open");
        hook_fires += 1;
    }

    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::UserCancel
        )],
        "the confirmed cancel records exactly one cancel line"
    );
    assert!(
        returned_children(&ledger_path).is_empty(),
        "a cancel-cut run never records a child_returned line: {:?}",
        returned_children(&ledger_path)
    );
    let entries = sessions.list_subagents().await.expect("child roster");
    assert_eq!(
        entries[0].status, "cancelled",
        "the cancelled row stays cancelled after the watcher's pass: {entries:?}"
    );
}

#[tokio::test]
async fn a_close_killed_child_never_records_a_clean_return() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    // A live grandchild holds the watcher's wait slice open indefinitely,
    // so the close provably lands on a watcher parked mid-wait.
    child_subagents.running.store(true, Ordering::SeqCst);
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        100,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::clone(&child_subagents),
    )
    .await;
    let (recorder, ledger_path, _dir) = seeded_parent_recorder();
    sessions.set_semantic_edges(Some(recorder));
    let (hook_tx, mut hook_rx) = mpsc::unbounded_channel();
    sessions.set_settle_hook(Arc::new(move || {
        let _ = hook_tx.send(());
    }));
    let handle = spawn_child(&sessions).await;
    // The child committed one request before the close cut it short (its
    // own ledger carries the line the parent's return recording reads).
    std::fs::create_dir_all(&handle.session_dir).expect("child session dir");
    let mut child_ledger = String::new();
    child_ledger.push_str("{\"type\":\"session_registered\",\"session_id\":\"child-file\"}");
    child_ledger.push('\n');
    child_ledger.push_str("{\"type\":\"request_finished\",\"request_id\":\"child-request-1\"}");
    child_ledger.push('\n');
    std::fs::write(
        Path::new(&handle.session_dir).join("semantic-edges.jsonl"),
        child_ledger,
    )
    .expect("write the child ledger");
    sessions.notify_turn_done();
    // Wait until the watcher is provably parked inside its held quiescence
    // wait (the fake counts them), never on one still starting.
    let parked = Instant::now() + Duration::from_secs(10);
    while child_subagents.quiescent_waits.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < parked,
            "the watcher never parked in its wait slice"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    sessions
        .close_children(ChildCloseReason::Killed)
        .await
        .expect("close the running child");
    // Release the parked watcher: its wait slice answers, its refresh runs,
    // and its settle tail fires the hook — the return recording precedes
    // the hook in that tail, so the hook bounds the assertion.
    child_subagents.running.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(15), hook_rx.recv())
        .await
        .expect("the parked watcher runs its settle tail")
        .expect("hook channel open");

    assert_eq!(
        cancelled_lines(&ledger_path),
        vec![(
            "parent-lead".to_string(),
            "child-file".to_string(),
            "parent-request-1".to_string(),
            ChildCancelledReason::CloseKilled
        )],
        "the close's kill records the cancel line"
    );
    assert!(
        returned_children(&ledger_path).is_empty(),
        "a close-killed run never records a child_returned line: {:?}",
        returned_children(&ledger_path)
    );
}

/// A target whose delete receipt already returned resolves immediately to the settled cancelled
/// envelope without spending the timeout budget; unknown selectors keep erroring.
#[tokio::test]
async fn collect_answers_a_just_deleted_target_with_the_cancelled_envelope() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    // The child settles with its final answer before the delete.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let entries = sessions.list_subagents().await.expect("child roster");
        if entries.iter().any(|entry| entry.status == "completed") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "child never settled: {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect("delete the settled child");

    let results = sessions
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .expect("collect the deleted child by id");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].rlm_child_id, handle.rlm_child_id);
    assert_eq!(results[0].session_name.as_deref(), Some("f20-worker"));
    assert_eq!(results[0].status, "cancelled");
    assert!(results[0].settled);
    assert_eq!(
        results[0].error.as_deref(),
        Some("Deleted by parent orchestrator")
    );
    let results = sessions
        .collect(vec!["f20-worker".to_string()], 0)
        .await
        .expect("collect the deleted child by name");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].rlm_child_id, handle.rlm_child_id);
    assert_eq!(results[0].status, "cancelled");
    assert!(results[0].settled);
    let missing = sessions
        .collect(vec!["ghost".to_string()], 0)
        .await
        .expect_err("an unknown selector still errors");
    assert_eq!(
        missing.to_string(),
        "No direct RLM child matches \"ghost\" in the current parent session"
    );
    // Retirement is idempotent (the M5 class): a re-delete of the same
    // selector answers from the tombstone with the row's status at its
    // delete, never the old "no longer resolves" miss.
    let redeleted = sessions
        .delete_subagent("f20-worker".to_string())
        .await
        .expect("the re-delete of a retired child is idempotent");
    assert_eq!(redeleted.outcome, Some("deleted"));
    assert_eq!(redeleted.subagent.status, "completed");
    assert_eq!(redeleted.subagent.session_name, "f20-worker");
    // A genuinely unknown selector keeps its miss.
    let gone = sessions
        .delete_subagent("ghost".to_string())
        .await
        .expect_err("an unknown selector still errors");
    assert_eq!(
        gone.to_string(),
        "No direct RLM subagent matches \"ghost\" in the current parent session"
    );
}

/// The M5 pin: a settled (terminal) child's delete releases its name slot —
/// a same-name spawn admits again — and the delete of a settled child
/// returns the terminal row instead of erroring.
#[tokio::test]
async fn a_deleted_settled_child_releases_its_name_slot_and_deletes_idempotently() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    // The child settles before the delete (a terminal-status row).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let entries = sessions.list_subagents().await.expect("child roster");
        if entries.iter().any(|entry| entry.status == "completed") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "child never settled: {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let deleted = sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect("delete the settled child");
    assert_eq!(deleted.subagent.status, "completed");

    // The name slot is released: the supervisor admits a same-name spawn.
    let respawned = spawn_child(&sessions).await;
    assert_eq!(respawned.name, "f20-worker");

    // Deleting a child whose run was cancelled (the factory's stop pass)
    // reports the cancelled row, and a second delete of it is idempotent.
    // The cancel lands before the turn-done notification, so the respawned
    // child is provably still running when it is cut short.
    assert!(
        sessions.cancel_child_run(&respawned.rlm_child_id).await,
        "cancel the respawned child"
    );
    let cancelled_delete = sessions
        .delete_subagent(respawned.rlm_child_id.clone())
        .await
        .expect("delete the cancelled child");
    assert_eq!(cancelled_delete.subagent.status, "cancelled");
    let again = sessions
        .delete_subagent(respawned.rlm_child_id.clone())
        .await
        .expect("the second delete of the cancelled child is idempotent");
    assert_eq!(again.subagent.status, "cancelled");
}

/// The exit capture (the M4 seam): an unreachable child settles `error`
/// through the give-up, but its last assistant text still lands on the row —
/// the live read first, the durable session file second — so the parent-side
/// reader (the factory's provisional answer) sees the child's final say, not
/// a bare failure row.
#[tokio::test(start_paused = true)]
async fn a_failed_settle_captures_the_childs_last_assistant_text() {
    tokio::spawn(async {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
        }
    });
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Unreachable)
            .await;
    let handle = spawn_child(&sessions).await;
    let settled = sessions.settle_notified();
    sessions.notify_turn_done();
    tokio::time::timeout(Duration::from_secs(3_600), settled)
        .await
        .expect("the unreachable give-up settles the child as failed");
    let results = sessions
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .expect("collect the failed child");
    assert_eq!(results[0].status, "error");
    assert_eq!(
        results[0].answer_preview.as_deref(),
        Some("the child final answer"),
        "the exit capture preserved the last assistant text on the failed row"
    );
    assert_eq!(
        results[0].answer_text.as_deref(),
        Some("the child final answer"),
        "the binding lane carries the exit capture"
    );
}

/// Review finding (PR #3462): a `None` first answer capture (the settle
/// raced the worker's answer hand-off) FROZE the row empty — the capture fill
/// only ran while the settle verdict was unset, so no later refresh could
/// deliver the text and every re-collect answered the same empty envelope. The
/// fill now runs on every refresh: it writes only when the fresh round trip
/// produced a text AND the row has no answer yet.
#[tokio::test]
async fn a_none_first_answer_capture_recovers_on_a_later_collect() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::TextLate)
            .await;
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    // The watcher's settle refresh consumed the empty first answer read and
    // settled the child with no captured answer.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let entries = sessions.list_subagents().await.expect("child roster");
        if entries.iter().any(|entry| entry.status == "completed") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "child never settled: {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A positive-budget collect refreshes the row and the answer lands;
    // zero-budget collection only reads the cached snapshot.
    let results = sessions
        .collect(vec![handle.rlm_child_id.clone()], 1_000)
        .await
        .expect("collect the settled child");
    assert_eq!(results[0].status, "done");
    assert_eq!(
        results[0].answer_text.as_deref(),
        Some("the child final answer"),
        "the late answer must land after the empty first capture"
    );
    assert_eq!(
        results[0].answer_preview.as_deref(),
        Some("the child final answer")
    );
}

/// The collect envelope carries the settled child's full final answer as its
/// binding lane (the factory's output capture binds the whole fenced JSON from
/// it), while the roster preview stays the compact form; the deleted envelope's
/// binding lane is empty (the tombstone only keeps the preview).
#[tokio::test]
async fn collect_carries_the_full_answer_text_of_a_settled_child() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let entries = sessions.list_subagents().await.expect("child roster");
        if entries.iter().any(|entry| entry.status == "completed") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "child never settled: {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let results = sessions
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .expect("collect the settled child");
    assert_eq!(
        results[0].answer_preview.as_deref(),
        Some("the child final answer")
    );
    assert_eq!(
        results[0].answer_text.as_deref(),
        Some("the child final answer"),
        "the binding lane carries the full final answer"
    );
    sessions
        .delete_subagent(handle.rlm_child_id.clone())
        .await
        .expect("delete the settled child");
    let deleted = sessions
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .expect("collect the deleted child");
    assert_eq!(
        deleted[0].answer_preview.as_deref(),
        Some("the child final answer")
    );
    assert_eq!(
        deleted[0].answer_text, None,
        "the tombstone carries no binding lane"
    );
}

/// The inactive delete (a settled retained child) leaves the same tombstone as the live delete, so
/// `collect` answers a just-deleted selector with the settled cancelled envelope.
#[tokio::test]
async fn collect_answers_the_cancelled_envelope_after_an_inactive_delete() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _kill_rx) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let handle = spawn_child(&sessions).await;
    sessions.notify_turn_done();
    // The inactive delete requires a settled child (a running child
    // answers "running").
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let entries = sessions.list_subagents().await.expect("child roster");
        if entries.iter().any(|entry| entry.status == "completed") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "child never settled: {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let outcome = sessions
        .delete_inactive_subagent(&handle.rlm_child_id)
        .await
        .expect("inactive delete");
    assert_eq!(outcome, "deleted");

    let results = sessions
        .collect(vec![handle.rlm_child_id.clone()], 0)
        .await
        .expect("collect the inactive-deleted child");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].rlm_child_id, handle.rlm_child_id);
    assert_eq!(results[0].status, "cancelled");
    assert!(results[0].settled);
    assert_eq!(
        results[0].error.as_deref(),
        Some("Deleted by parent orchestrator")
    );
}

/// A child that already settled never re-scores as an error when its worker leaves afterward; a
/// still-RUNNING child does (the crash class the error verdict exists for).
#[test]
fn an_already_settled_child_never_re_scores_as_an_unreachable_error() {
    let base = || ChildRecord {
        rlm_child_id: "child-id".to_string(),
        session_name: "lane".to_string(),
        active_session_id: "child-live".to_string(),
        session_id: Some("child-file".to_string()),
        session_dir: "/tmp".to_string(),
        model: String::new(),
        thinking_level: None,
        progress_note: None,
        label: "task".to_string(),
        started_at_ms: 0,
        owner_cancelled: false,
        settled_status: None,
        settled: false,
        admission_blocked: false,
        admission_lock: Arc::new(Mutex::new(())),
        settle_watch_lock: Arc::new(Mutex::new(())),
        answer_preview: None,
        answer_text: None,
        answer_captured: false,
        replied_since_task: false,
        tool_use_count: None,
        live_observation: None,
        direct_observation: Arc::new(Mutex::new(())),
        notice_delivered: false,
        factory: false,
        prompt_admitted: true,
        admission_generation: None,
        expected_message_id: None,
        error: None,
        closed_by_parent: false,
        retiring: false,
        retirement_finished: false,
        retirement_notified: false,
        retirement_roster_emitted: false,
        usage_registration_forgotten: false,
        session_file: None,
        attributed_rows: Some(0),
        pending_usage_report: None,
        usage_watch_live: false,
        usage_rearm: false,
        emit_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        last_emitted_status: None,
        observation: None,
        rename_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    };
    // A running child that goes unreachable is the error class.
    assert!(super::lifecycle::should_mark_unreachable_error(&base()));
    // A settled child keeps its positive verdict.
    let mut settled = base();
    settled.settled_status = Some("done");
    assert!(
        !super::lifecycle::should_mark_unreachable_error(&settled),
        "an idle-passivated (or post-settle crashed) child keeps its settled verdict"
    );
    // A parent-closed child and a noticed child never re-score.
    let mut closed = base();
    closed.closed_by_parent = true;
    assert!(!super::lifecycle::should_mark_unreachable_error(&closed));
    let mut noticed = base();
    noticed.notice_delivered = true;
    assert!(!super::lifecycle::should_mark_unreachable_error(&noticed));
}

/// The unreachable give-up's exit capture is a wasted round trip when a
/// captured answer already stands (a child that settled, went busy again
/// on a queued continuation, then lost its worker): the record keeps its
/// capture, so the fetched text could never land. The give-up must skip
/// the capture entirely — zero `get_last_assistant_text` reads, the
/// standing capture unchanged.
#[tokio::test]
async fn the_unreachable_give_up_skips_the_exit_capture_when_a_capture_stands() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let child_subagents = Arc::new(FakeChildSubagents::default());
    let (sessions, _kill_rx) = sessions_with_fake_child_subagents(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::Unreachable,
        Arc::clone(&child_subagents),
    )
    .await;
    let record = Arc::new(tokio::sync::Mutex::new(ChildRecord {
        rlm_child_id: "child-id".to_string(),
        session_name: "lane".to_string(),
        active_session_id: "child-live".to_string(),
        session_id: Some("child-file".to_string()),
        session_dir: "/tmp".to_string(),
        model: String::new(),
        thinking_level: None,
        progress_note: None,
        label: "task".to_string(),
        started_at_ms: 0,
        owner_cancelled: false,
        settled_status: None,
        settled: false,
        admission_blocked: false,
        admission_lock: Arc::new(Mutex::new(())),
        settle_watch_lock: Arc::new(Mutex::new(())),
        answer_preview: Some("the captured say".to_string()),
        answer_text: Some("the captured say".to_string()),
        answer_captured: true,
        replied_since_task: false,
        tool_use_count: None,
        live_observation: None,
        direct_observation: Arc::new(Mutex::new(())),
        notice_delivered: false,
        factory: false,
        prompt_admitted: true,
        admission_generation: None,
        expected_message_id: None,
        error: None,
        closed_by_parent: false,
        retiring: false,
        retirement_finished: false,
        retirement_notified: false,
        retirement_roster_emitted: false,
        usage_registration_forgotten: false,
        session_file: Some(
            std::env::temp_dir()
                .join(format!(
                    "pa-rlm-watch-skip-{}.jsonl",
                    uuid::Uuid::new_v4().simple()
                ))
                .to_string_lossy()
                .to_string(),
        ),
        attributed_rows: Some(0),
        pending_usage_report: None,
        usage_watch_live: false,
        usage_rearm: false,
        emit_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        last_emitted_status: None,
        observation: None,
        rename_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    }));
    let claimed = sessions
        .inner
        .settle_failed(
            &record,
            "Child worker unreachable".to_string(),
            super::lifecycle::FailedArm::Unreachable,
        )
        .await;
    assert!(claimed, "the give-up claims the settle");
    assert_eq!(
        child_subagents.answer_reads.load(Ordering::SeqCst),
        0,
        "no exit-capture round trip may run while a captured answer stands"
    );
    let record = record.lock().await;
    assert_eq!(record.settled_status, Some("error"));
    assert_eq!(record.answer_preview.as_deref(), Some("the captured say"));
    assert_eq!(record.answer_text.as_deref(), Some("the captured say"));
    assert!(record.answer_captured);
}

#[tokio::test]
async fn watcher_projects_progress_while_its_existing_idle_wait_is_held() {
    HELD_PROGRESS_READY.store(false, Ordering::SeqCst);
    HELD_PROGRESS_STATE_READS.store(0, Ordering::SeqCst);
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _) = sessions_with_fake_supervisor(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::HeldProgressSummary,
    )
    .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "held-progress".into(),
            active_session_id: "child-live".into(),
            session_id: Some("child-file".into()),
            session_name: "held-progress".into(),
        })
        .await;
    let record = sessions.inner.find_record("held-progress").await.unwrap();
    let inner = Arc::clone(&sessions.inner);
    let row = Arc::clone(&record);
    let watcher = tokio::spawn(async move {
        inner.watch_child_settle(&row).await;
    });
    HELD_PROGRESS_WAIT_STARTED.notified().await;
    let first_observation = tokio::time::timeout(Duration::from_secs(5), async {
        while HELD_PROGRESS_STATE_READS.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if first_observation.is_err() {
        watcher.abort();
        panic!("watcher must observe running state before its held idle wait completes");
    }
    assert!(sessions.child_snapshots().await[0]
        .get("progressNote")
        .is_none());
    HELD_PROGRESS_READY.store(true, Ordering::SeqCst);
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshots = sessions.child_snapshots().await;
            if snapshots[0]["progressNote"] == "note emitted after initial state" {
                assert_eq!(snapshots[0]["thinkingLevel"], "medium");
                assert_eq!(snapshots[0]["status"], "running");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    watcher.abort();
    observed.expect("later progress must refresh while the same idle wait stays held");
    let roster = sessions.list_subagents().await.unwrap();
    assert_eq!(roster[0].assignment_status, "running");
    assert_eq!(roster[0].status, "running");
    assert_eq!(
        roster[0].progress_note.as_deref(),
        Some("note emitted after initial state")
    );
    assert!(!record.lock().await.settled);
    assert!(record.lock().await.answer_text.is_none());
}

#[tokio::test]
async fn direct_refresh_projects_progress_while_owning_observation_lock() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _) = sessions_with_fake_supervisor(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::ProgressSummary,
    )
    .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".into(),
            active_session_id: "child-live".into(),
            session_id: Some("child-file".into()),
            session_name: "note-child".into(),
        })
        .await;
    let record = sessions.inner.find_record("child-id").await.unwrap();
    sessions.inner.refresh_record(&record).await;
    assert_eq!(
        record.lock().await.progress_note.as_deref(),
        Some("checking the consumer")
    );
}

#[tokio::test]
async fn child_progress_projection_retains_real_note_and_clears_on_fresh_summary() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _) = sessions_with_fake_supervisor(
        follow_up_tx,
        0,
        FakeKill::Success,
        FakeChild::ProgressSummary,
    )
    .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".into(),
            active_session_id: "child-live".into(),
            session_id: Some("child-file".into()),
            session_name: "note-child".into(),
        })
        .await;
    assert!(sessions.inner.child_busy("child-live").await.unwrap());
    let wire = sessions.child_snapshots().await;
    assert_eq!(wire[0]["thinkingLevel"], "medium");
    assert_eq!(wire[0]["progressNote"], "checking the consumer");
    assert_eq!(
        sessions.list_subagents().await.unwrap()[0]
            .progress_note
            .as_deref(),
        Some("checking the consumer")
    );
    assert!(sessions.inner.child_busy("child-live").await.unwrap());
    assert!(sessions.child_snapshots().await[0]
        .get("progressNote")
        .is_none());
    assert!(sessions.list_subagents().await.unwrap()[0]
        .progress_note
        .is_none());
}

#[tokio::test]
async fn failed_child_summary_retains_cached_progress_without_new_freshness() {
    let (follow_up_tx, _follow_up_rx) = mpsc::unbounded_channel();
    let (sessions, _) =
        sessions_with_fake_supervisor(follow_up_tx, 0, FakeKill::Success, FakeChild::Unreachable)
            .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".into(),
            active_session_id: "child-live".into(),
            session_id: Some("child-file".into()),
            session_name: "note-child".into(),
        })
        .await;
    let record = sessions.inner.find_record("child-live").await.unwrap();
    record
        .lock()
        .await
        .observe_summary(&json!({"thinkingLevel":"high", "progressNote":"last observation"}));
    assert!(sessions.inner.child_busy("child-live").await.is_err());
    let wire = sessions.child_snapshots().await;
    assert_eq!(wire[0]["progressNote"], "last observation");
    assert_eq!(wire[0]["thinkingLevel"], "high");
    assert!(wire[0].get("lastActivityAt").is_none());
}

#[tokio::test]
async fn background_bash_state_keeps_child_busy_before_answer_capture() {
    let state = Arc::new(FakeChildSubagents::default());
    state.background_bash.store(true, Ordering::SeqCst);
    let (tx, _rx) = mpsc::unbounded_channel();
    let (sessions, _) = sessions_with_fake_child_subagents(
        tx,
        0,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::clone(&state),
    )
    .await;
    assert!(sessions.inner.child_busy("child-live").await.unwrap());
    assert_eq!(state.answer_reads.load(Ordering::SeqCst), 0);
    state.background_bash.store(false, Ordering::SeqCst);
    assert!(!sessions.inner.child_busy("child-live").await.unwrap());
}

#[tokio::test]
async fn orchestration_max_admission_serializes_concurrent_twentieth_and_twenty_first() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let (sessions, _) =
        sessions_with_fake_supervisor(tx, 0, FakeKill::Success, FakeChild::StateHangs).await;
    sessions.configure_gate(&json!({"expectedRevision":0,
        "policy":{"version":1,"enabled":true,"minActive":5,"maxActive":20,"rootWritesCode":false,"correctiveBudget":2},
        "task":{"projectId":"fixture","planId":"p","stepId":"s","taskGeneration":1,"policyRevision":1},"assignments":[]})).await.unwrap();
    for i in 0..19 {
        sessions
            .push_test_child(RlmChildIdentity {
                rlm_child_id: format!("existing-{i}"),
                active_session_id: format!("live-{i}"),
                session_id: None,
                session_name: format!("existing-{i}"),
            })
            .await;
    }
    // Fake supervisor doesn't own a display file: seed the real create display seam below.
    // Capacity refusal happens before create for the losing contender.
    let requests = (0..2).map(|i| {
        sessions.spawn(RlmSpawnRequest {
            cwd: None,
            factory: None,

            prompt: "independent capacity fixture".into(),
            name: Some(format!("contender-{i}")),
            model: None,
            thinking: None,
            spawned_by_request_id: None,
            work: None,
        })
    });
    let results = futures::future::join_all(requests).await;
    let capacity_errors = results
        .iter()
        .filter(|r| {
            r.as_ref()
                .err()
                .is_some_and(|e| format!("{e:#}").contains("max admission capacity"))
        })
        .count();
    assert_eq!(capacity_errors, 1, "{results:?}");
    assert_eq!(sessions.inner.children.lock().await.len(), 20);
}

fn observed_status_test_record() -> ChildRecord {
    ChildRecord {
        factory: false,

        rlm_child_id: "child-id".to_string(),
        session_name: "lane".to_string(),
        active_session_id: "child-live".to_string(),
        session_id: Some("child-file".to_string()),
        session_dir: "/tmp".to_string(),
        model: String::new(),
        thinking_level: None,
        progress_note: None,
        label: "task".to_string(),
        started_at_ms: 0,
        owner_cancelled: false,
        settled_status: None,
        settled: false,
        admission_blocked: false,
        admission_lock: Arc::new(Mutex::new(())),
        settle_watch_lock: Arc::new(Mutex::new(())),
        answer_preview: None,
        answer_text: None,
        answer_captured: false,
        replied_since_task: false,
        notice_delivered: false,
        prompt_admitted: true,
        admission_generation: None,
        expected_message_id: None,
        error: None,
        closed_by_parent: false,
        retiring: false,
        retirement_finished: false,
        retirement_notified: false,
        retirement_roster_emitted: false,
        usage_registration_forgotten: false,
        session_file: None,
        attributed_rows: Some(0),
        pending_usage_report: None,
        usage_watch_live: false,
        usage_rearm: false,
        emit_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        last_emitted_status: None,
        observation: None,
        rename_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        tool_use_count: None,
        live_observation: None,
        direct_observation: Arc::new(Mutex::new(())),
    }
}

#[test]
fn observed_own_turn_is_separate_from_unsettled_jobs_queue_and_descendants() {
    let mut row = observed_status_test_record();
    assert_eq!(SupervisorChildSessions::entry(&row).status, "unknown");
    for (summary, expected) in [
        (json!({"isStreaming":true}), "running"),
        (
            json!({"isStreaming":false,"hasBackgroundBash":true}),
            "idle",
        ),
        (
            json!({"isStreaming":false,"sessionActions":{"queuedCount":1}}),
            "queued",
        ),
        (
            json!({"isStreaming":false,"hasRunningSubagents":true}),
            "idle",
        ),
    ] {
        row.observe_summary(&summary);
        let entry = SupervisorChildSessions::entry(&row);
        assert_eq!(entry.status, expected);
        assert_eq!(entry.assignment_status, "running");
        assert_eq!(entry.activity.is_some(), expected == "running");
        assert!(!SupervisorChildSessions::collect_result(&row).settled);
    }
    for (terminal, expected) in [
        ("done", "completed"),
        ("error", "error"),
        ("cancelled", "cancelled"),
    ] {
        row.settled_status = Some(terminal);
        assert_eq!(SupervisorChildSessions::entry(&row).status, expected);
    }
}

#[test]
fn observed_execution_is_invalidated_by_failure_age_retask_and_restart() {
    let mut row = observed_status_test_record();
    row.observe_summary(&json!({"isStreaming":true,"workerInstanceId":"old"}));
    assert_eq!(row.execution_status(), "running");
    row.observation.as_mut().unwrap()["available"] = json!(false);
    assert_eq!(row.execution_status(), "unknown");
    row.observe_summary(&json!({"isStreaming":true,"workerInstanceId":"new"}));
    assert_eq!(row.observation_value()["workerInstanceId"], "new");
    row.observation.as_mut().unwrap()["observedAt"] = json!(0);
    assert_eq!(row.execution_status(), "unknown");
    row.observe_summary(&json!({"isStreaming":true}));
    row.admission_generation = Some("next".into());
    assert_eq!(row.execution_status(), "unknown");
    row.observation = None;
    assert_eq!(row.execution_status(), "unknown");
}

#[tokio::test]
async fn execution_update_emits_while_assignment_remains_running() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let (sessions, _) =
        sessions_with_fake_supervisor(tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&rows);
    sessions.set_child_update_sink(Arc::new(move |row| sink.lock().unwrap().push(row)));
    let record = Arc::new(Mutex::new(observed_status_test_record()));
    record
        .lock()
        .await
        .observe_summary(&json!({"isStreaming":true}));
    sessions.inner.emit_child_update(&record).await;
    record
        .lock()
        .await
        .observe_summary(&json!({"isStreaming":false,"hasBackgroundBash":true}));
    sessions.inner.emit_child_update(&record).await;
    let rows = rows.lock().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["executionStatus"], "running");
    assert_eq!(rows[1]["executionStatus"], "idle");
    assert_eq!(rows[1]["status"], "running");
    assert_eq!(rows[1]["observation"]["hasBackgroundBash"], true);
}

#[test]
fn native_roster_serialization_preserves_observation_contract() {
    let mut row = observed_status_test_record();
    row.admission_generation = Some("generation".into());
    row.expected_message_id = Some("message".into());
    row.observe_summary(
        &json!({"isStreaming":false,"hasBackgroundBash":true,"workerInstanceId":"worker"}),
    );
    let wire = serde_json::to_value(SupervisorChildSessions::entry(&row)).unwrap();
    assert_eq!(wire["status"], "idle");
    assert_eq!(wire["assignment_status"], "running");
    assert_eq!(wire["observation"]["admissionGeneration"], "generation");
    assert_eq!(wire["observation"]["expectedMessageId"], "message");
    assert_eq!(wire["observation"]["workerInstanceId"], "worker");
    assert!(wire["observation"]["observedAt"].is_u64());
    assert!(wire["observation"]["ageMs"].is_u64());
    let runtime = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../prime-agent-runtime/src");
    let output = std::process::Command::new("python3")
        .env("PYTHONPATH", runtime)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .arg("-c")
        .arg("import json,sys,rlm; row=rlm._subagent_from_payload(json.loads(sys.argv[1])); assert row.status=='idle'; assert row.assignment_status=='running'; assert row.observation['hasBackgroundBash'] is True; assert row.observation['workerInstanceId']=='worker'; assert row.observation['ageMs'] >= 0; assert row.observation['admissionGeneration']=='generation'")
        .arg(wire.to_string())
        .output().expect("Python parser on serialized Native roster");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn watcher_observation_failure_invalidates_running_without_settling_assignment() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let (sessions, _) =
        sessions_with_fake_supervisor(tx, 0, FakeKill::Success, FakeChild::Unreachable).await;
    let mut row = observed_status_test_record();
    row.observe_summary(&json!({"isStreaming":true}));
    let record = Arc::new(Mutex::new(row));
    sessions
        .inner
        .children
        .lock()
        .await
        .push(Arc::clone(&record));
    assert!(sessions.inner.child_busy("child-live").await.is_err());
    let row = record.lock().await;
    assert_eq!(row.execution_status(), "unknown");
    assert_eq!(row.observation_value()["available"], false);
    assert_eq!(row.status(), "running");
    assert!(!row.settled);
}

#[tokio::test]
async fn successful_observation_timestamp_refresh_emits_without_assignment_change() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let (sessions, _) =
        sessions_with_fake_supervisor(tx, 0, FakeKill::Success, FakeChild::Healthy).await;
    let rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&rows);
    sessions.set_child_update_sink(Arc::new(move |row| sink.lock().unwrap().push(row)));
    let mut row = observed_status_test_record();
    row.observe_summary(&json!({"isStreaming":true}));
    let observed_at = row.observation.as_ref().unwrap()["observedAt"]
        .as_u64()
        .unwrap();
    row.observation.as_mut().unwrap()["observedAt"] = json!(observed_at - 1);
    let record = Arc::new(Mutex::new(row));
    sessions.inner.emit_child_update(&record).await;
    record.lock().await.observation.as_mut().unwrap()["observedAt"] = json!(observed_at);
    sessions.inner.emit_child_update(&record).await;
    sessions.inner.emit_child_update(&record).await;
    let rows = rows.lock().unwrap();
    assert_eq!(
        rows.len(),
        2,
        "fresh successful observations emit; identical snapshots deduplicate"
    );
    assert_eq!(rows[0]["status"], "running");
    assert_eq!(rows[1]["status"], "running");
    assert_eq!(rows[0]["executionStatus"], "running");
    assert_eq!(rows[1]["executionStatus"], "running");
    assert_eq!(rows[0]["observation"]["observedAt"], observed_at - 1);
    assert_eq!(rows[1]["observation"]["observedAt"], observed_at);
}

/// Exercise the existing refresh/roster seam, not just the durable-row parser.
#[tokio::test]
async fn terminal_assistant_verdict_survives_idle_refresh() {
    for (reasons, expected, error) in [
        (vec!["error"], "error", Some("controlled terminal failure")),
        (vec!["stop"], "done", None),
        (
            vec!["aborted"],
            "cancelled",
            Some("controlled terminal failure"),
        ),
        (vec!["error", "stop"], "done", None),
    ] {
        let dir = tempfile::TempDir::new().unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let (sessions, _kill) =
            sessions_with_fake_supervisor(tx, 0, FakeKill::Success, FakeChild::Healthy).await;
        sessions
            .push_test_child(RlmChildIdentity {
                rlm_child_id: "child".into(),
                active_session_id: "child-live".into(),
                session_id: None,
                session_name: "child".into(),
            })
            .await;
        let file = dir.path().join("child.jsonl");
        let mut entries = vec![
            json!({"type":"message","id":"old", "message":{"role":"assistant","stopReason":"error","errorMessage":"previous task"}}),
            json!({"type":"custom_message","id":"row", "details":{"id":"current"}}),
        ];
        entries.extend(reasons.into_iter().map(|reason| json!({"type":"message","message":{"role":"assistant","stopReason":reason,"errorMessage":"controlled terminal failure"}})));
        std::fs::write(
            &file,
            entries
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let record = sessions.inner.find_record("child-live").await.unwrap();
        {
            let mut row = record.lock().await;
            row.session_file = Some(file.to_string_lossy().into_owned());
            row.expected_message_id = Some("current".into());
        }
        sessions.inner.refresh_record(&record).await;
        let row = record.lock().await;
        assert_eq!(
            (row.settled_status, row.error.as_deref()),
            (Some(expected), error)
        );
        assert_eq!(
            row.snapshot_value()["executionStatus"],
            match expected {
                "done" => "completed",
                "error" => "error",
                "cancelled" => "cancelled",
                _ => unreachable!(),
            }
        );
    }
}

#[tokio::test]
async fn terminal_failure_does_not_settle_busy_or_unavailable_child() {
    for (child, background, execution) in [
        (FakeChild::Unreachable, false, "unknown"),
        (FakeChild::Healthy, true, "idle"),
    ] {
        let dir = tempfile::TempDir::new().unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let flags = Arc::new(FakeChildSubagents::default());
        flags.background_bash.store(background, Ordering::SeqCst);
        let (sessions, _kill) =
            sessions_with_fake_child_subagents(tx, 0, FakeKill::Success, child, flags).await;
        sessions
            .push_test_child(RlmChildIdentity {
                rlm_child_id: "child".into(),
                active_session_id: "child-live".into(),
                session_id: None,
                session_name: "child".into(),
            })
            .await;
        let file = dir.path().join("child.jsonl");
        std::fs::write(&file, concat!(
            "{\"type\":\"custom_message\",\"details\":{\"id\":\"current\"}}\n",
            "{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"error\",\"errorMessage\":\"failed\"}}\n"
        )).unwrap();
        let record = sessions.inner.find_record("child-live").await.unwrap();
        {
            let mut row = record.lock().await;
            row.session_file = Some(file.to_string_lossy().into_owned());
            row.expected_message_id = Some("current".into());
        }
        sessions.inner.refresh_record(&record).await;
        let row = record.lock().await;
        assert_eq!((row.settled_status, row.error.as_deref()), (None, None));
        assert_eq!(row.snapshot_value()["executionStatus"], execution);
    }
}

/// Every direct read must publish the same generation/sequence watermark as
/// collect, including clearing old notes and accepting a replacement worker.
#[tokio::test]
async fn child_busy_orders_direct_state_before_stale_roster_and_worker_replacement() {
    let state = Arc::new(FakeChildSubagents::default());
    let (sessions, _) = sessions_with_fake_child_subagents(
        mpsc::unbounded_channel().0,
        0,
        FakeKill::Success,
        FakeChild::Healthy,
        Arc::clone(&state),
    )
    .await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "child-id".into(),
            active_session_id: "child-live".into(),
            session_id: Some("child-file".into()),
            session_name: "worker".into(),
        })
        .await;
    let record = sessions.inner.find_record("child-live").await.unwrap();
    *state.live_state.lock().unwrap() = Some(json!({
        "workerInstanceId":"i1", "rosterDeltaSequence":4,
        "isStreaming":true, "progressNote":"old note", "toolUseCount":4,
    }));
    assert!(sessions.inner.child_busy("child-live").await.unwrap());
    *state.live_state.lock().unwrap() = Some(json!({
        "workerInstanceId":"i1", "rosterDeltaSequence":6,
        "isStreaming":false, "toolUseCount":6,
    }));
    assert!(!sessions.inner.child_busy("child-live").await.unwrap());
    sessions
        .inner
        .observe_live_state(
            &record,
            &json!({
                "workerInstanceId":"i1", "rosterDeltaSequence":5,
                "isStreaming":true, "progressNote":"stale note", "toolUseCount":5,
            }),
            100,
            ChildObservationSource::Roster,
        )
        .await;
    {
        let row = record.lock().await;
        assert_eq!(row.tool_use_count, Some(6));
        assert_eq!(row.progress_note, None);
        assert_eq!(row.execution_status(), "idle");
        assert_eq!(row.live_observation.as_ref().unwrap().sequence, Some(6));
    }
    *state.live_state.lock().unwrap() = Some(json!({
        "workerInstanceId":"i2", "rosterDeltaSequence":0, "isStreaming":true,
    }));
    assert!(sessions.inner.child_busy("child-live").await.unwrap());
    sessions
        .inner
        .observe_live_state(
            &record,
            &json!({
                "workerInstanceId":"i1", "rosterDeltaSequence":99,
                "isStreaming":false, "progressNote":"retired note", "toolUseCount":99,
            }),
            101,
            ChildObservationSource::Roster,
        )
        .await;
    let row = record.lock().await;
    assert_eq!(row.tool_use_count, None);
    assert_eq!(row.progress_note, None);
    assert_eq!(row.execution_status(), "running");
    assert_eq!(
        row.live_observation
            .as_ref()
            .unwrap()
            .worker_instance_id
            .as_deref(),
        Some("i2")
    );
}

#[test]
fn settled_assignment_preserves_fresh_runtime_and_expires_to_unknown() {
    let mut row = observed_status_test_record();
    row.settled_status = Some("done");
    row.settled = true;
    row.observe_summary(&json!({"isStreaming":true,"workerInstanceId":"same"}));
    let snapshot = row.snapshot_value();
    assert_eq!(snapshot["assignmentStatus"], "done");
    assert_eq!(snapshot["executionStatus"], "completed");
    assert_eq!(snapshot["runtimeStatus"], "running");
    assert_eq!(snapshot["runtimeObservationAvailable"], true);
    row.observation.as_mut().unwrap()["observedAt"] = json!(0);
    let expired = row.snapshot_value();
    assert_eq!(expired["assignmentStatus"], "done");
    assert_eq!(expired["runtimeStatus"], "unknown");
    assert_eq!(expired["runtimeObservationAvailable"], false);
}

#[tokio::test]
async fn settled_retained_child_fetches_roster_and_missing_or_failed_source_is_unknown() {
    let peers = Arc::new(std::sync::Mutex::new(vec![
        json!({"rlmChildId":"retained","activeSessionId":"retained-live","workerInstanceId":"worker","rosterDeltaSequence":1,"isStreaming":true,"progressNote":"fresh roster busy"}),
    ]));
    let (sessions, _) = sessions_with_roster_peers(Arc::clone(&peers), 0).await;
    sessions
        .push_test_child(RlmChildIdentity {
            rlm_child_id: "retained".into(),
            active_session_id: "retained-live".into(),
            session_id: Some("retained-file".into()),
            session_name: "retained".into(),
        })
        .await;
    sessions.settle_test_child("retained-live").await;
    assert!(
        !sessions.any_running().await,
        "assignment barrier remains settled"
    );
    sessions.list_subagents().await.unwrap();
    let row = &sessions.child_snapshots().await[0];
    assert_eq!(row["assignmentStatus"], "done");
    assert_eq!(row["runtimeStatus"], "running");
    assert_eq!(
        row["progressNote"], "fresh roster busy",
        "all-settled list must actually fetch supervisor"
    );
    peers.lock().unwrap().clear();
    sessions.list_subagents().await.unwrap();
    assert_eq!(
        sessions.child_snapshots().await[0]["runtimeStatus"],
        "unknown"
    );
    *peers.lock().unwrap() = vec![
        json!({"rlmChildId":"retained","workerInstanceId":"worker","rosterDeltaSequence":2,"isStreaming":true}),
    ];
    sessions.list_subagents().await.unwrap();
    assert_eq!(
        sessions.child_snapshots().await[0]["runtimeStatus"],
        "running"
    );
    *peers.lock().unwrap() = vec![json!({"_testRosterUnavailable":true})];
    sessions.list_subagents().await.unwrap();
    let row = &sessions.child_snapshots().await[0];
    assert_eq!(row["runtimeStatus"], "unknown");
    assert_eq!(row["runtimeObservationAvailable"], false);
    assert!(
        !sessions.any_running().await,
        "runtime truth must not reopen assignment barrier"
    );
}
