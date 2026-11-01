//! Versioned no-turn controls for the existing Native scheduler.
use super::{
    json, response_failure, response_success, CreateAgentCronJobInput, DaemonResponse, Value,
    Worker,
};
impl Worker {
    pub(crate) fn handle_thread_heartbeat(&self, payload: &Value, mutate: bool) -> DaemonResponse {
        let command = if mutate {
            "heartbeat_update"
        } else {
            "heartbeat_get"
        };
        if let Err(response) = self.require_created(command) {
            return response;
        }
        let core = self.core.lock().unwrap();
        self.bind_store_artifact(&core);
        let failure = |code: &str| response_failure(None, command, code, None);
        let Some(session) = core
            .store
            .as_ref()
            .filter(|s| !s.path.as_os_str().is_empty())
        else {
            return failure("identity_conflict");
        };
        for (key, expected) in [
            ("activeSessionId", core.active_session_id.as_str()),
            ("sessionId", session.session_id()),
            ("sessionFile", session.path.to_str().unwrap_or("")),
            ("cwd", core.cwd.as_str()),
        ] {
            if payload.get(key).and_then(Value::as_str) != Some(expected) {
                return failure("identity_conflict");
            }
        }
        let input = CreateAgentCronJobInput {
            active_session_id: core.active_session_id.clone(),
            session_id: session.session_id().into(),
            session_file: session.path.to_string_lossy().into(),
            cwd: core.cwd.clone(),
            runtime_kind: Some(core.runtime_kind.clone()),
            ..Default::default()
        };
        let mut authorized_payload = payload.clone();
        authorized_payload["supervisorSocket"] =
            json!(self.config.supervisor_socket_path.to_string_lossy());
        let result = if mutate {
            self.scheduled
                .store()
                .mutate_thread_heartbeat(&input, &authorized_payload)
        } else {
            self.scheduled.store().thread_heartbeat_snapshot(&input)
        };
        let mut data = match result {
            Ok(data) => data,
            Err(error) => {
                let error = error.to_string();
                let refusal = match error.as_str() {
                    "identity_conflict"
                    | "revision_conflict"
                    | "binding_conflict"
                    | "invalid_input"
                    | "dispatch_unconfirmed" => error.as_str(),
                    _ => "store_error",
                };
                return failure(refusal);
            }
        };
        let scheduler_started = self
            .scheduled
            .scheduler
            .try_lock()
            .is_ok_and(|guard| guard.is_some());
        data["runtime"] = json!({"health":if core.shutdown_requested {"stopping"} else if scheduler_started {"running"} else {"unconfirmed"},"waitingState":if core.busy {"active_turn"} else if core.compacting {"compacting"} else if !core.pending_next_turn.is_empty()||!core.follow_up.is_empty() {"waiting_turn"} else {"idle"}});
        let withdrawn = if mutate
            && matches!(
                payload.get("action").and_then(Value::as_str),
                Some("pause" | "delete")
            ) {
            self.scheduled
                .store()
                .list()
                .into_iter()
                .filter(|job| {
                    job.session_id == input.session_id
                        && job.source.as_deref() == Some("rlm_heartbeat")
                        && job.label.as_deref() == Some("studio-roadmap-progress")
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        drop(core);
        for job in withdrawn {
            self.scheduled.remove_queued_heartbeat_follow_up(&job);
        }
        response_success(None, command, Some(data))
    }
}
