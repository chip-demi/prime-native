//! Private turn admission policy: drone fences, owner controls and managed child permits.
//!
//! The upstream runner calls this after queue pickup and before durable input acceptance.
//! Keep admission ordering and UNKNOWN parking semantics here; do not add a second runner,
//! transport, retry policy, or public API. The caller retains queue settlement ownership.
use super::{json, Arc, Mutex, QueuedItem, SessionEngine, TurnRunner};
#[cfg(test)]
use super::{EventPump, OutboundFrame, Value};
use std::time::{Duration, Instant};

/// Bound public request-progress notifications at the same cadence as message
/// updates. `RequestControl` still records every revision before calling this sink;
/// request, phase, deadline and recovery-operation changes publish immediately.
#[derive(Default)]
struct RequestProgressCadence {
    last: Option<(pa_agent::agent_loop::request::RequestSnapshot, Instant)>,
}

impl RequestProgressCadence {
    fn should_publish(
        &mut self,
        snapshot: &pa_agent::agent_loop::request::RequestSnapshot,
        now: Instant,
    ) -> bool {
        if self.last.as_ref().is_some_and(|(last, published)| {
            last.request_id == snapshot.request_id
                && last.phase == snapshot.phase
                && last.operation_id == snapshot.operation_id
                && last.deadline_ms == snapshot.deadline_ms
                && now.saturating_duration_since(*published)
                    < crate::streaming::UPDATE_FLUSH_INTERVAL
        }) {
            return false;
        }
        self.last = Some((snapshot.clone(), now));
        true
    }
}

#[cfg(test)]
#[path = "request_progress_tests.rs"]
mod request_progress_tests;

impl TurnRunner {
    /// Refresh owner policy and validate start permits before any model delivery.
    /// False means the caller must park the original records without retrying them.
    pub(super) async fn prepare_owned_delivery(
        &self,
        engine: &Arc<dyn SessionEngine>,
        items: &mut Vec<QueuedItem>,
    ) -> bool {
        let resume_core = Arc::clone(&self.core);
        let resume_pauses = self.input_pauses.clone();
        let (resume_generation, resume_turn) = {
            let core = self.core.lock().unwrap();
            (core.generation.clone(), core.turn_serial)
        };
        let admission_core = Arc::clone(&self.core);
        let admission_pauses = self.input_pauses.clone();
        let admission_recovery = Arc::clone(&self.recovery);
        let admission_generation = resume_generation.clone();
        let admission_engine = Arc::downgrade(&self.engine);
        let telemetry_events = Arc::clone(&self.events);
        let telemetry_core = Arc::clone(&self.core);
        self.engine.set_drone_admission(Arc::new(move |register| {
            let core = admission_core.lock().unwrap();
            if core.generation != admission_generation
                || core.turn_serial != resume_turn
                || core.abort_requested
                || core.shutdown_requested
                || core.queued_input_suspended
                || core.compacting
                || !core.running_tool_calls.is_empty()
            {
                return false;
            }
            admission_pauses
                .with_unpaused(|| {
                    let mut recovery = admission_recovery.lock().unwrap();
                    let control = admission_engine
                        .upgrade()
                        .and_then(|e| e.drone_request_control());
                    if !super::super::drone_control::ordinary_request_allowed(
                        recovery.as_mut(),
                        &core.active_session_id,
                        control.as_ref(),
                    ) {
                        return false;
                    }
                    if let Some(control) = control {
                        let events = telemetry_events.clone();
                        let core = telemetry_core.clone();
                        let progress = Mutex::new(RequestProgressCadence::default());
                        control.set_telemetry(Arc::new(move |snapshot| {
                            if !progress
                                .lock()
                                .unwrap()
                                .should_publish(&snapshot, Instant::now())
                            {
                                return;
                            }
                            let events = events.clone();
                            let core = core.clone();
                            tokio::spawn(async move {
                                super::super::emit_worker_event_with(
                                    &core,
                                    &events,
                                    json!({"type":"drone_request_progress","request":snapshot}),
                                );
                            });
                        }));
                    }
                    register();
                    true
                })
                .unwrap_or(false)
        }));
        self.engine.set_drone_resume_guard(Arc::new(move || {
            let core = resume_core.lock().unwrap();
            core.generation == resume_generation
                && core.turn_serial == resume_turn
                && !core.abort_requested
                && !core.shutdown_requested
                && !core.queued_input_suspended
                && !core.compacting
                && core.running_tool_calls.is_empty()
                && !resume_pauses.paused()
        }));
        if items.iter().any(|item| {
            item.custom_message.as_ref().is_some_and(|row| {
                row["customType"] == super::super::queue::ORCHESTRATION_CONTROL_TYPE
            })
        }) {
            // Pull the owner projection again at pickup, not only at mint time.
            let refreshed = engine.orchestration_gate(None).await.is_ok();
            let core = self.core.lock().unwrap();
            if !refreshed
                || core.shutdown_requested
                || core.abort_requested
                || !core.steering.is_empty()
            {
                items.retain(|item| {
                    !item.custom_message.as_ref().is_some_and(|row| {
                        row["customType"] == super::super::queue::ORCHESTRATION_CONTROL_TYPE
                    })
                });
            }
        }
        let mut work_allowed = true;
        for item in items.iter_mut() {
            let Some(row) = item.custom_message.as_mut() else {
                continue;
            };
            let Some(mut permit) = row.get("nativeWorkPermit").cloned() else {
                continue;
            };
            let _ = row["nativeWorkPermit"]
                .as_object_mut()
                .map(|permit| permit.remove("managedResultTracking"));
            let message_id = row["details"]["id"].as_str();
            if message_id.is_none()
                || message_id != permit["expectedMessageId"].as_str()
                || permit["childActiveSessionId"] != self.active_session_id
            {
                work_allowed = false;
                continue;
            }
            permit["incarnation"] =
                json!(std::env::var(super::super::WORKER_INSTANCE_ID_ENV).unwrap_or_default());
            let response = self.passivation.link.request(json!({
            "type":"configure_orchestration_gate","activeSessionId":permit["ownerActiveSessionId"],"beginWorkStart":permit
        }), Duration::from_millis(2_000)).await;
            let status = match response {
                Ok(response) if response.success => response.data,
                Ok(response) => {
                    let reason = Self::start_denial_diagnostic(response.error.as_deref());
                    eprintln!(
                        "pa-daemon: beginWorkStart denied child={} message={}: {reason}",
                        permit["childId"].as_str().unwrap_or("missing"),
                        permit["expectedMessageId"].as_str().unwrap_or("missing")
                    );
                    None
                }
                Err(_) => {
                    eprintln!(
                        "pa-daemon: beginWorkStart transport_unknown child={} message={}; no retry",
                        permit["childId"].as_str().unwrap_or("missing"),
                        permit["expectedMessageId"].as_str().unwrap_or("missing")
                    );
                    None
                }
            };
            work_allowed &= status
                .as_ref()
                .is_some_and(|status| status["workStartPermitted"] == true);
            if status
                .as_ref()
                .is_some_and(|status| status["managedResultTracking"] == true)
            {
                row["nativeWorkPermit"]["managedResultTracking"] = json!(true);
            }
        }
        if items.iter().any(|item| {
            item.custom_message
                .as_ref()
                .is_some_and(|row| row.get("nativeWorkPermit").is_some())
        }) {
            let core = self.core.lock().unwrap();
            work_allowed &= !core.shutdown_requested
                && !core.abort_requested
                && !core.queued_input_suspended
                && core.steering.is_empty();
            drop(core);
            work_allowed &= !engine.is_quota_parked()
                && engine.goal_state_value()["status"] != "budget_limited";
        }
        work_allowed
    }

    /// One delivery: the first item anchors the turn and the rest ride
    /// as co-delivered user rows of the same run.
    pub(in crate::worker) fn start_denial_diagnostic(error: Option<&str>) -> String {
        let reason = error.unwrap_or("missing denial reason");
        // These start-gate errors contain only runtime-owned constants and numeric fences.
        if ["gate_changed:", "accounting_unknown:", "identity_invalid:"]
            .iter()
            .any(|prefix| reason.starts_with(prefix))
        {
            reason.chars().take(512).collect()
        } else if reason.starts_with("Work start")
            || reason.starts_with("Stale work start")
            || reason.starts_with("Work no longer ready")
        {
            "identity_invalid: start permit or owner state rejected; no retry".into()
        } else {
            "identity_invalid: unclassified owner denial; no retry".into()
        }
    }

    pub(super) fn take_managed_start_identities(
        items: &mut [QueuedItem],
    ) -> Vec<(String, Option<String>)> {
        items
            .iter()
            .filter_map(|item| {
                item.custom_message
                    .as_ref()
                    .filter(|row| row["nativeWorkPermit"]["managedResultTracking"] == true)
                    .and_then(|row| {
                        row["details"]["id"].as_str().map(|id| {
                            (
                                id.to_string(),
                                row["nativeWorkPermit"]["admissionGeneration"]
                                    .as_str()
                                    .map(str::to_string),
                            )
                        })
                    })
            })
            .collect()
    }
}
