//! Private update admission. Keep downstream drain requirements here,
//! separate from upstream session lifecycle; both operations use the same guard.
use super::{json, response_failure, DaemonResponse, Worker};

/// The old roster only projects text lanes, so these native-owned records must
/// drain before an update can safely retire their writer.
pub(super) fn snapshot_blocker(
    core: &super::SessionCore,
    recovery: Option<&crate::journal::WorkerRecoveryJournal>,
) -> Option<&'static str> {
    if core
        .steering
        .iter()
        .chain(core.follow_up.iter())
        .any(|item| {
            item.custom_message
                .as_ref()
                .is_some_and(crate::child_status_notices::is_reserved_child_status_custom_type)
        })
    {
        return Some("update_requires_child_notice_drain");
    }
    if recovery
        .and_then(|journal| journal.drone_operation(&core.active_session_id))
        .is_some_and(|op| {
            !matches!(
                op["status"].as_str(),
                Some("refused" | "continued" | "held")
            )
        })
    {
        return Some("update_requires_drone_operation_resolution");
    }
    None
}

impl Worker {
    /// Graceful stop: the connection loop exits the process after
    /// replying. The session's telemetry finalizes first.
    pub(crate) async fn handle_update_shutdown(&self) -> DaemonResponse {
        {
            let mut core = self.core.lock().unwrap();
            let recovery = self.recovery.lock().unwrap();
            if let Some(reason) = snapshot_blocker(&core, recovery.as_ref()) {
                return response_failure(None, "shutdown", reason, None);
            }
            // Admission observes the same core lock: nothing can queue a notice
            // between the final safety check and the stop gate.
            core.shutdown_requested = true;
            core.abort_requested = true;
        }
        self.handle_shutdown(&json!({})).await
    }
}
