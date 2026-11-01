//! Request-scoped control. This token is never passed to tools or the run controller.
use crate::abort::AbortController;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestSnapshot {
    pub request_id: u64,
    pub revision: u64,
    pub phase: String,
    pub elapsed_ms: u64,
    pub last_progress_ms: u64,
    pub deadline_ms: Option<u64>,
    pub operation_id: Option<String>,
}
struct ActiveRequest {
    id: u64,
    revision: u64,
    phase: &'static str,
    started: Instant,
    progress: Instant,
    controller: AbortController,
    operation: Option<String>,
    deadline: Option<Duration>,
}
#[derive(Default)]
struct State {
    next_id: u64,
    active: Option<ActiveRequest>,
    settled_operation: Option<String>,
    last_outcome: Option<(String, &'static str)>,
    deadline: Option<Duration>,
}
type ResumeGuard = Arc<dyn Fn() -> bool + Send + Sync>;
type RequestTelemetry = Arc<dyn Fn(RequestSnapshot) + Send + Sync>;
type RequestAdmission = Arc<dyn Fn(&mut dyn FnMut()) -> bool + Send + Sync>;

/// Shared control for one agent. All evidence validation and token cancellation share one lock.
#[derive(Clone, Default)]
pub struct RequestControl {
    state: Arc<Mutex<State>>,
    resume_allowed: Arc<Mutex<Option<ResumeGuard>>>,
    telemetry: Arc<Mutex<Option<RequestTelemetry>>>,
    admission: Arc<Mutex<Option<RequestAdmission>>>,
}
#[derive(Debug, thiserror::Error)]
#[error("model request interrupted by guarded recovery")]
pub(crate) struct RequestInterrupted;
impl RequestControl {
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    pub fn set_telemetry(&self, sink: Arc<dyn Fn(RequestSnapshot) + Send + Sync>) {
        *self.telemetry.lock().unwrap() = Some(sink);
    }
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    pub fn publish_snapshot(&self) {
        let snapshot = self.snapshot();
        let sink = self.telemetry.lock().unwrap().clone();
        if let (Some(snapshot), Some(sink)) = (snapshot, sink) {
            sink(snapshot);
        }
    }
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    pub fn set_admission(&self, admission: RequestAdmission) {
        *self.admission.lock().unwrap() = Some(admission);
    }
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    pub fn block_unconfirmed(&self, operation: String) {
        self.state.lock().unwrap().last_outcome = Some((operation, "recovery_unconfirmed"));
    }
    pub(crate) fn admit(&self) -> Option<RequestLease> {
        if self.unresolved() {
            return None;
        }
        let admission = self.admission.lock().unwrap().clone();
        let mut lease = None;
        let mut register = || lease = self.begin().ok();
        match admission {
            Some(admission) => {
                if !admission(&mut register) {
                    return None;
                }
            }
            None => register(),
        }
        lease
    }
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    pub fn set_resume_guard(&self, guard: Arc<dyn Fn() -> bool + Send + Sync>) {
        *self.resume_allowed.lock().unwrap() = Some(guard);
    }
    /// Set the provider request deadline supplied by the Native host policy.
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    pub fn set_deadline(&self, deadline: Option<Duration>) {
        self.state.lock().unwrap().deadline = deadline;
    }
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    #[must_use]
    pub fn operation_status(&self, operation: &str) -> Option<String> {
        let state = self.state.lock().unwrap();
        state
            .last_outcome
            .as_ref()
            .filter(|(id, _)| id == operation)
            .map(|(_, status)| (*status).to_string())
    }
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    #[must_use]
    pub fn snapshot(&self) -> Option<RequestSnapshot> {
        self.state
            .lock()
            .unwrap()
            .active
            .as_ref()
            .map(|a| RequestSnapshot {
                request_id: a.id,
                revision: a.revision,
                phase: a.phase.into(),
                elapsed_ms: u64::try_from(a.started.elapsed().as_millis()).unwrap_or(u64::MAX),
                last_progress_ms: u64::try_from(a.progress.elapsed().as_millis())
                    .unwrap_or(u64::MAX),
                deadline_ms: a
                    .deadline
                    .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
                operation_id: a.operation.clone(),
            })
    }
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    ///
    /// # Errors
    ///
    /// Returns a refusal for missing, stale, already-owned, or insufficient request evidence.
    pub fn recover(
        &self,
        request_id: u64,
        revision: u64,
        operation: &str,
    ) -> Result<(), &'static str> {
        let mut state = self.state.lock().unwrap();
        let a = state.active.as_mut().ok_or("not_model_request")?;
        if a.id != request_id || a.revision != revision {
            return Err("stale_request");
        }
        if a.operation.as_deref() == Some(operation) {
            return Ok(());
        }
        if a.operation.is_some() {
            return Err("recovery_unconfirmed");
        }
        if a.phase != "transport" || a.deadline.is_none_or(|d| a.started.elapsed() < d) {
            return Err("native_evidence_insufficient");
        }
        a.operation = Some(operation.into());
        a.controller.abort();
        Ok(())
    }
    pub(crate) fn continuation_allowed(&self) -> bool {
        let continued = self
            .state
            .lock()
            .unwrap()
            .last_outcome
            .as_ref()
            .is_some_and(|(_, s)| *s == "continued");
        !continued
            || self
                .resume_allowed
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|g| g())
    }
    pub(crate) fn unresolved(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .last_outcome
            .as_ref()
            .is_some_and(|(_, status)| *status == "recovery_unconfirmed")
    }
    /// # Panics
    ///
    /// Panics if an internal request-control mutex is poisoned.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state.lock().unwrap().active.is_some()
    }
    pub(crate) fn begin(&self) -> Result<RequestLease, &'static str> {
        let mut state = self.state.lock().unwrap();
        if state.active.is_some() {
            return Err("request_control_busy");
        }
        state.next_id += 1;
        let id = state.next_id;
        let controller = AbortController::new();
        let deadline = state.deadline;
        state.active = Some(ActiveRequest {
            id,
            revision: 1,
            phase: "transport",
            started: Instant::now(),
            progress: Instant::now(),
            controller: controller.clone(),
            operation: None,
            deadline,
        });
        drop(state);
        self.publish_snapshot();
        Ok(RequestLease {
            control: self.clone(),
            id,
            controller,
        })
    }
    pub(crate) fn resume(&self) -> bool {
        let operation = self.state.lock().unwrap().settled_operation.take();
        let allowed = operation.is_some()
            && self
                .resume_allowed
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|g| g());
        if let Some(operation) = operation {
            self.state.lock().unwrap().last_outcome =
                Some((operation, if allowed { "continued" } else { "held" }));
        }
        allowed
    }
}
pub(crate) struct RequestLease {
    control: RequestControl,
    id: u64,
    controller: AbortController,
}
impl RequestLease {
    pub(crate) fn signal(&self) -> crate::abort::AbortSignal {
        self.controller.signal()
    }
    pub(crate) fn enter_emitter(&self) -> bool {
        let mut state = self.control.state.lock().unwrap();
        let Some(a) = state.active.as_mut().filter(|a| a.id == self.id) else {
            return false;
        };
        if a.operation.is_some() {
            return false;
        }
        a.phase = "emitter";
        a.revision += 1;
        a.progress = Instant::now();
        true
    }
    pub(crate) fn confirmed(&self) {
        let mut state = self.control.state.lock().unwrap();
        let State {
            active,
            settled_operation,
            last_outcome,
            ..
        } = &mut *state;
        if let Some(a) = active.as_ref().filter(|a| a.id == self.id) {
            settled_operation.clone_from(&a.operation);
            *last_outcome = settled_operation.clone().map(|op| (op, "settled"));
        }
    }
    pub(crate) fn phase(&self, phase: &'static str) {
        {
            if let Some(a) = self
                .control
                .state
                .lock()
                .unwrap()
                .active
                .as_mut()
                .filter(|a| a.id == self.id)
            {
                a.phase = phase;
                a.revision += 1;
                a.progress = Instant::now();
            }
        }
        self.control.publish_snapshot();
    }
}
impl Drop for RequestLease {
    fn drop(&mut self) {
        self.controller.abort();
        let mut state = self.control.state.lock().unwrap();
        if state.active.as_ref().is_some_and(|a| a.id == self.id) {
            if let Some(operation) = state.active.take().and_then(|a| a.operation) {
                if state
                    .last_outcome
                    .as_ref()
                    .is_none_or(|(op, _)| op != &operation)
                {
                    state.last_outcome = Some((operation, "recovery_unconfirmed"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_deadline_is_not_a_hang_and_emitter_is_protected() {
        let control = RequestControl::default();
        let lease = control.begin().unwrap();
        let s = control.snapshot().unwrap();
        assert_eq!(
            control.recover(s.request_id, s.revision, "op"),
            Err("native_evidence_insufficient")
        );
        assert!(!lease.signal().is_aborted());
        drop(lease);
        control.set_deadline(Some(Duration::ZERO));
        let lease = control.begin().unwrap();
        lease.phase("emitter");
        let s = control.snapshot().unwrap();
        assert_eq!(
            control.recover(s.request_id, s.revision, "op"),
            Err("native_evidence_insufficient")
        );
    }
    #[test]
    fn exact_request_settlement_and_one_resume() {
        let control = RequestControl::default();
        control.set_deadline(Some(Duration::ZERO));
        control.set_resume_guard(Arc::new(|| true));
        let lease = control.begin().unwrap();
        let s = control.snapshot().unwrap();
        assert_eq!(
            control.recover(s.request_id + 1, s.revision, "op"),
            Err("stale_request")
        );
        assert_eq!(control.recover(s.request_id, s.revision, "op"), Ok(()));
        assert_eq!(control.recover(s.request_id, s.revision, "op"), Ok(()));
        assert_eq!(
            control.recover(s.request_id, s.revision, "other"),
            Err("recovery_unconfirmed")
        );
        assert!(lease.signal().is_aborted());
        assert!(!control.resume());
        lease.confirmed();
        drop(lease);
        assert!(control.resume());
        assert!(!control.resume());
    }
    #[test]
    fn active_host_control_cannot_be_overwritten_by_another_registration() {
        let host = RequestControl::default();
        let first = host.begin().unwrap();
        let before = host.snapshot().unwrap();
        assert!(matches!(host.begin(), Err("request_control_busy")));
        let fresh = crate::agent::Agent::new(crate::agent::AgentOptions::default());
        assert!(fresh.set_request_control(host.clone()).is_err());
        assert!(host.admit().is_none());
        assert_eq!(host.snapshot().unwrap().request_id, before.request_id);
        assert!(!first.signal().is_aborted());
    }
    #[test]
    fn request_identity_is_not_reused_by_host_control_rebinding() {
        let host = RequestControl::default();
        let first = host.begin().unwrap();
        let old = host.snapshot().unwrap();
        drop(first);
        let rebuilt = host.clone();
        let next = rebuilt.begin().unwrap();
        let current = rebuilt.snapshot().unwrap();
        assert_eq!(host.snapshot().unwrap().request_id, current.request_id);
        assert!(current.request_id > old.request_id);
        assert_eq!(
            rebuilt.recover(old.request_id, old.revision, "stale"),
            Err("stale_request")
        );
        assert!(!next.signal().is_aborted());
    }
    #[test]
    fn native_admission_rechecks_pause_and_registers_atomically() {
        let control = RequestControl::default();
        let pause = Arc::new(Mutex::new(false));
        let gate = pause.clone();
        control.set_admission(Arc::new(move |register| {
            let held = gate.lock().unwrap();
            if *held {
                return false;
            }
            register();
            true
        }));
        *pause.lock().unwrap() = true;
        assert!(control.admit().is_none());
        assert!(control.snapshot().is_none());
        *pause.lock().unwrap() = false;
        let request = control.admit().unwrap();
        assert!(control.snapshot().is_some());
        drop(request);
    }
    #[test]
    fn stopped_root_and_new_root_do_not_share_recovery() {
        let stopped = RequestControl::default();
        stopped.set_deadline(Some(Duration::ZERO));
        stopped.set_resume_guard(Arc::new(|| false));
        let old = stopped.begin().unwrap();
        let snapshot = stopped.snapshot().unwrap();
        stopped
            .recover(snapshot.request_id, snapshot.revision, "old")
            .unwrap();
        old.confirmed();
        drop(old);
        let replacement = RequestControl::default();
        replacement.set_resume_guard(Arc::new(|| true));
        assert!(!stopped.resume());
        assert!(!replacement.resume());
        assert_eq!(replacement.operation_status("old"), None);
    }
    #[test]
    fn cancellation_without_confirmed_producer_exit_does_not_resume() {
        let control = RequestControl::default();
        control.set_deadline(Some(Duration::ZERO));
        control.set_resume_guard(Arc::new(|| true));
        let lease = control.begin().unwrap();
        let s = control.snapshot().unwrap();
        control
            .recover(s.request_id, s.revision, "unknown")
            .unwrap();
        drop(lease);
        assert!(!control.resume());
        assert_eq!(
            control.operation_status("unknown"),
            Some("recovery_unconfirmed".into())
        );
    }
    #[test]
    fn pause_after_cancel_prevents_resume() {
        let control = RequestControl::default();
        control.set_deadline(Some(Duration::ZERO));
        control.set_resume_guard(Arc::new(|| false));
        let lease = control.begin().unwrap();
        let s = control.snapshot().unwrap();
        control.recover(s.request_id, s.revision, "op").unwrap();
        drop(lease);
        assert!(!control.resume());
    }
}

#[cfg(test)]
mod loop_tests {
    use super::*;
    use crate::stream::{AssistantMessageEvent, ModelStream};
    use crate::types::{AgentContext, AgentMessage, AssistantMessage, Model};
    struct VerifyTool {
        schema: serde_json::Value,
        executed: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl crate::types::AgentTool for VerifyTool {
        fn name(&self) -> &'static str {
            "verify_artifact"
        }
        fn description(&self) -> &'static str {
            "Verify one exact local artifact"
        }
        fn parameters(&self) -> &serde_json::Value {
            &self.schema
        }
        fn execute(
            self: Arc<Self>,
            _: String,
            _: serde_json::Value,
            signal: crate::abort::AbortSignal,
            _: crate::types::AgentToolUpdateCallback,
        ) -> crate::BoxFut<'static, anyhow::Result<crate::types::AgentToolResult>> {
            Box::pin(async move {
                assert!(!signal.is_aborted());
                self.executed
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(crate::types::AgentToolResult::text("artifact_verified"))
            })
        }
    }
    struct Held {
        settled: bool,
        ready: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    }
    impl ModelStream for Held {
        fn next_event(&mut self) -> crate::BoxFut<'_, Option<AssistantMessageEvent>> {
            if let Some(ready) = self.ready.take() {
                let _ = ready.send(());
            }
            Box::pin(std::future::pending())
        }
        fn result(&mut self) -> crate::BoxFut<'_, anyhow::Result<AssistantMessage>> {
            Box::pin(std::future::pending())
        }
        fn settle(&mut self) -> crate::BoxFut<'_, bool> {
            let settled = self.settled;
            Box::pin(async move { settled })
        }
    }

    #[tokio::test]
    async fn guarded_recovery_hold_and_unknown_settle_as_aborted_without_replay() {
        for settled in [true, false] {
            let config = super::super::AgentLoopConfig::new(
                Model::unknown(),
                super::super::AgentLoopConfig::default_convert_to_llm(),
            );
            config.request_control.set_deadline(Some(Duration::ZERO));
            config.request_control.set_resume_guard(Arc::new(|| false));
            let control = config.request_control.clone();
            let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let seen = calls.clone();
            let stream: crate::stream::StreamFn = Arc::new(move |_, _, _| {
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let ready = ready_tx.clone();
                Box::pin(async move {
                    Ok(Box::new(Held {
                        settled,
                        ready: Some(ready),
                    }) as Box<dyn ModelStream>)
                })
            });
            let turn = tokio::spawn(async move {
                super::super::run_agent_loop(
                    vec![AgentMessage::user("one original request")],
                    AgentContext {
                        system_prompt: String::new(),
                        messages: vec![],
                        tools: vec![],
                    },
                    &config,
                    Arc::new(|_| Box::pin(async { Ok(()) })),
                    None,
                    Some(&stream),
                )
                .await
            });
            ready_rx.recv().await.unwrap();
            let snapshot = control.snapshot().unwrap();
            control
                .recover(snapshot.request_id, snapshot.revision, "guarded")
                .unwrap();
            let messages = tokio::time::timeout(Duration::from_secs(2), turn)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let row = serde_json::to_value(messages.last().unwrap()).unwrap();
            assert_eq!(row["stopReason"], "aborted");
            assert_eq!(
                row["errorMessage"],
                "model request interrupted by guarded recovery"
            );
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(
                control.operation_status("guarded").as_deref(),
                Some(if settled {
                    "held"
                } else {
                    "recovery_unconfirmed"
                })
            );
            assert!(!control.resume());
        }
    }

    #[tokio::test]
    async fn held_update_emitter_rejects_request_recovery() {
        let model = Model::unknown();
        let provider = Arc::new(crate::scripted::ScriptedProvider::new(model.clone()));
        provider.push_text_turn("legitimate output");
        let config = super::super::AgentLoopConfig::new(
            model,
            super::super::AgentLoopConfig::default_convert_to_llm(),
        );
        config.request_control.set_deadline(Some(Duration::ZERO));
        let control = config.request_control.clone();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let sink_release = release.clone();
        let held = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sink: super::super::AgentEventSink = Arc::new(move |event| {
            let tx = tx.clone();
            let release = sink_release.clone();
            let held = held.clone();
            Box::pin(async move {
                if matches!(event, crate::types::AgentEvent::MessageUpdate { .. })
                    && !held.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    let _ = tx.send(());
                    release.notified().await;
                }
                Ok(())
            })
        });
        let session = tokio::spawn(async move {
            let prompt: AgentMessage = serde_json::from_value(
                serde_json::json!({"role":"user","content":"verify","timestamp":0}),
            )
            .unwrap();
            super::super::run_agent_loop(
                vec![prompt],
                AgentContext::default(),
                &config,
                sink,
                None,
                Some(&provider.stream_fn()),
            )
            .await
        });
        rx.recv().await.unwrap();
        let snapshot = control.snapshot().unwrap();
        assert_eq!(snapshot.phase, "emitter");
        assert_eq!(
            control.recover(snapshot.request_id, snapshot.revision, "forbidden"),
            Err("native_evidence_insufficient")
        );
        release.notify_waiters();
        assert!(session.await.unwrap().is_ok());
    }
    #[tokio::test]
    async fn truncated_provider_partial_tool_arguments_never_reach_executor() {
        let model = Model::unknown();
        let provider = Arc::new(crate::scripted::ScriptedProvider::new(model.clone()));
        let mut partial:AssistantMessage=serde_json::from_value(serde_json::json!({"role":"assistant","content":[{"type":"toolCall","id":"partial","name":"verify_artifact","arguments":{"unexpected":"unclosed provider stream"}}],"api":"fixture","provider":"fixture","model":"fixture","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"error","errorMessage":"Failed to read provider response body: error decoding response body","timestamp":0})).unwrap();
        partial.stop_reason = crate::types::StopReason::Error;
        provider.push_turn(crate::scripted::ScriptedTurn::Events(vec![
            crate::scripted::ScriptStep::Event(Box::new(AssistantMessageEvent::Start {
                partial: partial.clone(),
            })),
            crate::scripted::ScriptStep::Event(Box::new(AssistantMessageEvent::Error {
                reason: crate::types::StopReason::Error,
                error: partial,
            })),
        ]));
        let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let tool: Arc<dyn crate::types::AgentTool> = Arc::new(VerifyTool {
            schema: serde_json::json!({"type":"object","properties":{}}),
            executed: executed.clone(),
        });
        let config = super::super::AgentLoopConfig::new(
            model,
            super::super::AgentLoopConfig::default_convert_to_llm(),
        );
        let prompt: AgentMessage = serde_json::from_value(
            serde_json::json!({"role":"user","content":"verify","timestamp":0}),
        )
        .unwrap();
        let messages = super::super::run_agent_loop(
            vec![prompt],
            AgentContext {
                system_prompt: String::new(),
                messages: vec![],
                tools: vec![tool],
            },
            &config,
            Arc::new(|_| Box::pin(async { Ok(()) })),
            None,
            Some(&provider.stream_fn()),
        )
        .await
        .unwrap();
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(!messages.iter().any(|m| m.role() == "toolResult"));
        assert_eq!(provider.calls().len(), 1);
    }
    #[tokio::test]
    async fn held_before_tool_hook_is_not_a_model_request() {
        let model = Model::unknown();
        let provider = Arc::new(crate::scripted::ScriptedProvider::new(model.clone()));
        provider.push_tool_call_turn(
            None,
            vec![("held-hook", "verify_artifact", serde_json::json!({}))],
        );
        provider.push_text_turn("verified");
        let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let tool: Arc<dyn crate::types::AgentTool> = Arc::new(VerifyTool {
            schema: serde_json::json!({"type":"object","properties":{}}),
            executed: executed.clone(),
        });
        let mut config = super::super::AgentLoopConfig::new(
            model,
            super::super::AgentLoopConfig::default_convert_to_llm(),
        );
        config.request_control.set_deadline(Some(Duration::ZERO));
        let control = config.request_control.clone();
        let (held_tx, mut held_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let hook_release = release.clone();
        config.before_tool_call = Some(Arc::new(move |_, _| {
            let release = hook_release.clone();
            let held = held_tx.clone();
            Box::pin(async move {
                let _ = held.send(());
                release.notified().await;
                Ok(None)
            })
        }));
        let session = tokio::spawn(async move {
            let prompt: AgentMessage = serde_json::from_value(
                serde_json::json!({"role":"user","content":"verify","timestamp":0}),
            )
            .unwrap();
            super::super::run_agent_loop(
                vec![prompt],
                AgentContext {
                    system_prompt: String::new(),
                    messages: vec![],
                    tools: vec![tool],
                },
                &config,
                Arc::new(|_| Box::pin(async { Ok(()) })),
                None,
                Some(&provider.stream_fn()),
            )
            .await
        });
        held_rx.recv().await.unwrap();
        assert!(control.snapshot().is_none());
        assert_eq!(control.recover(1, 1, "forbidden"), Err("not_model_request"));
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 0);
        release.notify_one();
        assert!(session.await.unwrap().is_ok());
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn malformed_tool_arguments_emit_error_then_valid_next_tool_without_executor_replay() {
        let model = Model::unknown();
        let provider = Arc::new(crate::scripted::ScriptedProvider::new(model.clone()));
        provider.push_tool_call_turn(
            None,
            vec![(
                "bad",
                "verify_artifact",
                serde_json::json!({"unexpected":"partial malformed"}),
            )],
        );
        provider.push_tool_call_turn(
            None,
            vec![("good", "verify_artifact", serde_json::json!({}))],
        );
        provider.push_text_turn("verified");
        let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let tool: Arc<dyn crate::types::AgentTool> = Arc::new(VerifyTool {
            schema: serde_json::json!({"type":"object","additionalProperties":false,"properties":{}}),
            executed: executed.clone(),
        });
        let config = super::super::AgentLoopConfig::new(
            model,
            super::super::AgentLoopConfig::default_convert_to_llm(),
        );
        let prompt: AgentMessage = serde_json::from_value(
            serde_json::json!({"role":"user","content":"verify","timestamp":0}),
        )
        .unwrap();
        let messages = super::super::run_agent_loop(
            vec![prompt],
            AgentContext {
                system_prompt: String::new(),
                messages: vec![],
                tools: vec![tool],
            },
            &config,
            Arc::new(|_| Box::pin(async { Ok(()) })),
            None,
            Some(&provider.stream_fn()),
        )
        .await
        .unwrap();
        let results = messages
            .iter()
            .filter_map(|message| match message {
                AgentMessage::Standard(crate::types::Message::ToolResult(result)) => Some(result),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].tool_call_id, "bad");
        assert!(results[0].is_error);
        assert_eq!(results[1].tool_call_id, "good");
        assert!(!results[1].is_error);
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn busy_provider_recovers_independently_without_replaying_user_prompt() {
        let config = super::super::AgentLoopConfig::new(
            Model::unknown(),
            super::super::AgentLoopConfig::default_convert_to_llm(),
        );
        config.request_control.set_deadline(Some(Duration::ZERO));
        config.request_control.set_resume_guard(Arc::new(|| true));
        let control = config.request_control.clone();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
        let stream: crate::stream::StreamFn = Arc::new(move |_, context, _| {
            let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(
                context
                    .messages
                    .iter()
                    .filter(|m| matches!(m, crate::types::Message::User(_)))
                    .count(),
                1
            );
            let _ = ready_tx.send(n);
            Box::pin(async move {
                if n == 0 {
                    return Ok(Box::new(Held {
                        settled: true,
                        ready: None,
                    }) as Box<dyn ModelStream>);
                }
                let (writer, consumer) = crate::stream::event_stream();
                let message: AssistantMessage = serde_json::from_value(serde_json::json!({
                    "role":"assistant","content":if n==1 {serde_json::json!([{"type":"toolCall","id":"verified-call","name":"verify_artifact","arguments":{}}])} else {serde_json::json!([])},"api":"test","provider":"test","model":"test",
                    "usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},
                    "stopReason":if n==1 {"toolUse"} else {"stop"},"timestamp":0
                }))?;
                writer.push(AssistantMessageEvent::Done {
                    reason: crate::types::StopReason::Stop,
                    message: message.clone(),
                });
                writer.end(Some(message));
                Ok(Box::new(consumer) as Box<dyn ModelStream>)
            })
        });
        let prompt: AgentMessage = serde_json::from_value(
            serde_json::json!({"role":"user","content":"one original request","timestamp":0}),
        )
        .unwrap();
        let executed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let tool: Arc<dyn crate::types::AgentTool> = Arc::new(VerifyTool {
            schema: serde_json::json!({"type":"object","additionalProperties":false,"properties":{}}),
            executed: executed.clone(),
        });
        let session = tokio::spawn(async move {
            super::super::run_agent_loop(
                vec![prompt],
                AgentContext {
                    system_prompt: String::new(),
                    messages: vec![],
                    tools: vec![tool],
                },
                &config,
                Arc::new(|_| Box::pin(async { Ok(()) })),
                None,
                Some(&stream),
            )
            .await
        });
        assert_eq!(ready_rx.recv().await, Some(0));
        let snapshot = control.snapshot().unwrap();
        control
            .recover(snapshot.request_id, snapshot.revision, "one")
            .unwrap();
        assert_eq!(ready_rx.recv().await, Some(1));
        let messages = session.await.unwrap().unwrap();
        assert!(messages.iter().any(|m| m.role() == "toolResult"));
        assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(control.operation_status("one"), Some("continued".into()));
    }
}
