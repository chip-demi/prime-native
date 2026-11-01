//! The `factory_activity` worker arm: the `/factory` view's daemon lane.

//! One session-addressed command reaches this session's kernel factory
//! executor (the bridge registered by the pa-core session engine): the
//! payload's `action` rides the out-of-band kernel frame, and the kernel's
//! reply returns verbatim (the run registry stays kernel-owned). The
//! `run` action's model preflight (the allowlist pin, request auth)
//! happens in the session engine before the frame — a doomed run fails
//! before any child spawns.

use std::path::Path;

use serde_json::{json, Value};

use crate::protocol::{response_failure, response_success, DaemonResponse};
use crate::worker::Worker;

/// The `factory_activity` capability's advertisement gate: the factory is
/// opt-in (`factory.enabled` in the shared settings file, default off),
/// so a disabled factory never surfaces its lane — no capability, no
/// dock group, no page. Both hello surfaces (the supervisor's and the
/// worker's) advertise the lane only while the setting reads enabled.
pub(crate) fn factory_lane_enabled(agent_dir: &Path) -> bool {
    pa_core::settings::SettingsManager::create(
        std::env::current_dir().unwrap_or_default(),
        agent_dir,
    )
    .get_factory_enabled()
}

/// The capabilities a hello advertises: the default set, minus the
/// `factory_activity` lane while the factory stays disabled (the opt-in
/// gate; the settings read is fresh per connection, so a `/factory on`
/// toggle surfaces on the next client start).
pub(crate) fn advertised_server_capabilities(agent_dir: &Path) -> Vec<String> {
    let mut capabilities = crate::protocol::default_server_capabilities();
    if factory_lane_enabled(agent_dir) {
        capabilities.push("factory_mvp_v1".to_string());
    } else {
        capabilities.retain(|capability| capability != "factory_activity");
    }
    capabilities
}

impl Worker {
    /// `factory_activity`: one factory action over this session's kernel.
    /// The arm mirrors `handle_kernel_bash_activity`: the engine owns the
    /// kernel scope, and a missing session answers with the same "Kernel
    /// is not running" refusal.
    pub(crate) async fn handle_factory_activity(&self, payload: &Value) -> DaemonResponse {
        if let Err(response) = self.require_created("factory_activity") {
            return response;
        }
        let action = payload
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if action.is_empty() {
            return response_failure(None, "factory_activity", "action is required", None);
        }
        if action == "receipts" {
            // Read the canonical ordinary-session journal, not executor state.
            // A recorded ACK is historical evidence only after kernel loss.
            let core = self.core.lock().unwrap();
            let Some(store) = core.store.as_ref() else {
                return response_failure(
                    None,
                    "factory_activity",
                    "Factory receipt history unavailable",
                    None,
                );
            };
            let receipts: Vec<Value> = store
                .entries
                .iter()
                .rev()
                .filter(|entry| {
                    entry.fields.get("customType").and_then(Value::as_str)
                        == Some("factory_run_receipt")
                })
                .filter_map(|entry| entry.fields.get("details").cloned())
                .take(100)
                .collect();
            return response_success(
                None,
                "factory_activity",
                Some(json!({
                    "schemaVersion": 1, "recoverySupported": false, "receipts": receipts,
                })),
            );
        }
        let run_id = payload
            .get("runId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let spec_id = payload
            .get("specId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let timeout_ms = payload.get("timeoutMs").and_then(Value::as_u64);
        let Some(engine) = &self.agent_engine else {
            return response_failure(
                None,
                "factory_activity",
                pa_types::daemon::KERNEL_NOT_RUNNING_MESSAGE,
                None,
            );
        };
        match engine
            .factory_activity_with_launch(
                &action,
                run_id.as_deref(),
                spec_id.as_deref(),
                timeout_ms,
                payload.get("launch"),
            )
            .await
        {
            Ok(mut result) => {
                // Preserve a real ACK in the ordinary session journal. This is
                // historical provenance, never a durable executor/replay store.
                let receipt = if action == "run" && payload.get("launch").is_some() {
                    Some(&result)
                } else if action == "lookup"
                    && result.get("outcome").and_then(Value::as_str) == Some("known")
                {
                    result.get("receipt")
                } else {
                    None
                };
                if let Some(receipt) = receipt {
                    let public_receipt = public_factory_receipt(receipt);
                    let details = json!({
                        "schemaVersion": 1,
                        "kernelId": public_receipt.get("kernelId"),
                        "requestIdentity": public_receipt.get("requestIdentity"),
                        "specId": public_receipt.pointer("/receipt/specId"),
                        "contentRevision": public_receipt.pointer("/receipt/contentRevision"),
                        "receipt": public_receipt,
                    });
                    let persisted = {
                        let mut core = self.core.lock().unwrap();
                        let Some(store) = core.store.as_mut() else {
                            return response_failure(None, "factory_activity", "Factory ran but receipt history unavailable; outcome unknown; do not replay", None);
                        };
                        let persisted = persist_factory_receipt(store, &details);
                        if matches!(persisted, Ok(true)) {
                            crate::worker::broadcast_custom_row_locked(
                                &mut core,
                                &self.events,
                                &json!({
                                    "role": "custom", "customType": "factory_run_receipt",
                                    "content": "Factory run instantiated", "display": true, "details": details,
                                }),
                            );
                        }
                        persisted
                    };
                    match persisted {
                        Ok(true | false) => {},
                        Err(error) => return response_failure(None, "factory_activity", &format!("Factory ran but receipt history failed: {error}; outcome unknown; do not replay"), None),
                    }
                    // The public ACK must not leak configured local library paths.
                    result = if action == "run" {
                        public_receipt
                    } else {
                        let mut lookup = result;
                        lookup["receipt"] = public_receipt;
                        lookup
                    };
                }
                response_success(None, "factory_activity", Some(result))
            }
            Err(error) => response_failure(None, "factory_activity", &format!("{error:#}"), None),
        }
    }
}

fn public_factory_receipt(receipt: &Value) -> Value {
    let mut public = receipt.clone();
    if let Some(run) = public.get_mut("receipt").and_then(Value::as_object_mut) {
        run.remove("machinePath");
    }
    public
}

fn persist_factory_receipt(
    store: &mut crate::session_store::SessionFile,
    details: &Value,
) -> anyhow::Result<bool> {
    let duplicate = store.entries.iter().any(|entry| {
        entry.fields.get("customType").and_then(Value::as_str) == Some("factory_run_receipt")
            && entry.fields.get("details") == Some(details)
    });
    if duplicate {
        return Ok(false);
    }
    store
        .persist_entry(
            "custom_message",
            json!({
                "customType": "factory_run_receipt", "content": "Factory run instantiated",
                "display": true, "details": details,
            }),
        )
        .map(|_| true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_records_one_real_ack_and_strips_library_path() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let mut store = crate::session_store::SessionFile::create("/fixture", None, 0);
        store.set_path(dir.path().join("session.jsonl"));
        store.rewrite().expect("persist header");
        let receipt = public_factory_receipt(&json!({
            "kernelId": "kernel", "requestIdentity": "request",
            "receipt": {"runId": "actual-run", "specId": "fixture", "machinePath": "/private/MACHINE.md"},
        }));
        let details = json!({"schemaVersion":1, "receipt":receipt});
        assert!(persist_factory_receipt(&mut store, &details).expect("first ack"));
        assert!(!persist_factory_receipt(&mut store, &details).expect("same ack"));
        let reopened = crate::session_store::SessionFile::open(&store.path).expect("read history");
        let rows: Vec<_> = reopened
            .entries
            .iter()
            .filter(|entry| entry.fields["customType"] == "factory_run_receipt")
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fields["details"], details);
        assert!(!serde_json::to_string(&rows[0].fields)
            .unwrap()
            .contains("/private"));
        assert!(rows[0].fields["details"]["receipt"].get("task").is_none());
    }

    #[test]
    fn history_persistence_failure_is_not_an_ack() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let blocker = dir.path().join("not-a-directory");
        std::fs::write(&blocker, "block").unwrap();
        let mut store = crate::session_store::SessionFile::create("/fixture", None, 0);
        store.set_path(blocker.join("session.jsonl"));
        assert!(
            persist_factory_receipt(&mut store, &json!({"receipt":{"runId":"already-ran"}}))
                .is_err()
        );
    }

    /// The opt-in default: no settings file, no factory lane.
    #[test]
    fn the_factory_lane_stays_unadvertised_until_enabled() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let agent_dir = dir.path().join("agent");
        std::fs::create_dir_all(&agent_dir).expect("agent dir");

        let disabled = advertised_server_capabilities(&agent_dir);
        assert!(
            !disabled
                .iter()
                .any(|capability| capability == "factory_activity"),
            "the default factory gate stays off: {disabled:?}"
        );

        // `/factory on` persists `factory.enabled` in the settings file —
        // the exact shared key the lane advertisement reads.
        let settings = serde_json::json!({ "factory": { "enabled": true } });
        std::fs::write(
            agent_dir.join("settings.json"),
            serde_json::to_string(&settings).expect("serialize"),
        )
        .expect("write settings");
        let enabled = advertised_server_capabilities(&agent_dir);
        assert!(enabled
            .iter()
            .any(|capability| capability == "factory_mvp_v1"));
        assert!(
            enabled
                .iter()
                .any(|capability| capability == "factory_activity"),
            "the enabled factory advertises its lane: {enabled:?}"
        );

        // `/factory off` re-reads as disabled: the lane leaves the
        // advertisement again.
        let off = serde_json::json!({ "factory": { "enabled": false } });
        std::fs::write(
            agent_dir.join("settings.json"),
            serde_json::to_string(&off).expect("serialize"),
        )
        .expect("write settings");
        let disabled_again = advertised_server_capabilities(&agent_dir);
        assert!(
            !disabled_again
                .iter()
                .any(|capability| capability == "factory_activity"),
            "the disabled factory lane leaves the advertisement: {disabled_again:?}"
        );
    }
}
