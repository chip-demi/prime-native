//! Versioned Native extension. Correlation is not an idempotency/replay contract.
use super::{json, response_failure, response_success, Value, Worker};
use crate::protocol::DaemonResponse;

impl Worker {
    pub(crate) fn handle_get_input_receipts(&self, payload: &Value) -> DaemonResponse {
        let command = "get_input_receipts";
        if let Err(response) = self.require_created(command) {
            return response;
        }
        let Some(key) = payload.get("queueKey").and_then(Value::as_str) else {
            return response_failure(None, command, "queueKey is required", None);
        };
        let v2 = payload.get("extensionVersion").and_then(Value::as_u64) == Some(2);
        let core = self.core.lock().unwrap();
        let Some(store) = core.store.as_ref() else {
            return response_failure(None, command, "Session store unavailable", None);
        };
        if let Some(path) = payload.get("completionTargetFile") {
            if *path != store.path.to_string_lossy().as_ref() {
                return response_failure(None, command, "completion target identity changed", None);
            }
        }
        // Receipts come only from durable bytes, never an unflushed in-memory index.
        if store.path.as_os_str().is_empty() {
            return response_failure(
                None,
                command,
                "Input receipts require a durable session file",
                None,
            );
        }
        let durable = match std::fs::File::open(&store.path).and_then(|file| file.sync_all()) {
            Ok(()) => match crate::session_store::SessionFile::open(&store.path) {
                Ok(durable) if durable.session_id() == store.session_id() => durable,
                Ok(_) => {
                    return response_failure(None, command, "Session file identity changed", None);
                }
                Err(error) => return response_failure(None, command, &error.to_string(), None),
            },
            Err(error) => return response_failure(None, command, &error.to_string(), None),
        };
        let store = &durable;
        let receipts: Vec<_> = store
            .entries
            .iter()
            .filter_map(|entry| {
                let message = entry.fields.get("message")?;
                (entry.type_ == "message"
                    && message.get("role").and_then(Value::as_str) == Some("user")
                    && entry.fields.get("queueKey").and_then(Value::as_str) == Some(key))
                .then(|| json!({ "messageId": entry.id, "queueKey": key }))
            })
            .collect();
        if !v2 {
            return response_success(
                None,
                command,
                Some(if payload.get("completionTargetFile").is_some() {
                    let active_session_id = core.active_session_id.clone();
                    drop(core);
                    let queued = self
                        .recovery
                        .lock()
                        .unwrap()
                        .as_ref()
                        .and_then(|journal| journal.latest_queue_snapshot(&active_session_id))
                        .is_some_and(|(steering, follow_up, _)| {
                            steering
                                .iter()
                                .chain(&follow_up)
                                .any(|row| row.queue_key.as_deref() == Some(key))
                        });
                    json!({"extensionVersion":1,"receipts":receipts,"durablyQueued":queued})
                } else {
                    json!({"extensionVersion":1,"receipts":receipts})
                }),
            );
        }
        let identity = payload.get("inputIdentity");
        let mut disposition = "unknown";
        if let Some(identity) = identity.filter(|identity| {
            identity["version"] == 2
                && identity["queueKey"] == key
                && identity["sessionId"] == store.session_id()
                && identity["sessionFile"] == store.path.to_string_lossy().as_ref()
                && identity["cwd"] == core.cwd
                && identity["activeSessionId"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty())
                && identity["admissionId"]
                    .as_str()
                    .is_some_and(|s| uuid::Uuid::parse_str(s).is_ok())
        }) {
            let admissions: Vec<_> = store
                .entries
                .iter()
                .filter(|entry| {
                    entry.type_ == "custom"
                        && entry.fields["customType"] == "input_admission"
                        && (entry.fields["data"]["queueKey"] == key
                            || entry.fields["data"]["admissionId"] == identity["admissionId"])
                })
                .collect();
            let withdrawals: Vec<_> = store
                .entries
                .iter()
                .filter(|entry| {
                    entry.type_ == "custom"
                        && entry.fields["customType"] == "input_withdrawal"
                        && (entry.fields["data"]["queueKey"] == key
                            || entry.fields["data"]["admissionId"] == identity["admissionId"])
                })
                .collect();
            let positive: Vec<_> = store
                .entries
                .iter()
                .filter(|entry| {
                    entry.type_ == "message"
                        && entry.fields["message"]["role"] == "user"
                        && (entry.fields["queueKey"] == key
                            || entry.fields["inputIdentity"]["admissionId"]
                                == identity["admissionId"])
                })
                .collect();
            if admissions.len() == 1 && admissions[0].fields["data"] == *identity {
                if positive.len() == 1
                    && positive[0].fields["inputIdentity"] == *identity
                    && positive[0].fields["queueKey"] == key
                {
                    disposition = "committed";
                } else if positive.is_empty()
                    && withdrawals.len() == 1
                    && withdrawals[0].fields["data"] == *identity
                {
                    disposition = "withdrawn_never_committed";
                }
            }
        }
        response_success(
            None,
            command,
            Some(json!({"extensionVersion":2,
            "supportsInputWithdrawal":true,"receipts":receipts,"disposition":disposition,
            "inputIdentity":identity})),
        )
    }
}
