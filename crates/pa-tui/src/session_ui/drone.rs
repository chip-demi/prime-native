//! A branch-scoped user command. Its network work never occupies the paint path.
use super::{AgentView, SessionUi, UI_REQUEST_TIMEOUT_MS};
use anyhow::{ensure, Result};
use pa_types::daemon::DaemonCommand;
use serde_json::{Map, Value};
use std::future::Future;

#[derive(Clone)]
pub(super) struct PendingDroneControl {
    root: String,
    active: String,
    token: std::sync::Arc<()>,
}

pub(crate) struct DroneUpdate {
    request: PendingDroneControl,
    outcome: std::result::Result<Value, String>,
}

fn accept_update(
    pending: &mut Option<PendingDroneControl>,
    active: &str,
    root: &str,
    update: DroneUpdate,
) -> Option<std::result::Result<Value, String>> {
    let expected = pending.as_ref()?;
    if expected.root != update.request.root
        || expected.active != update.request.active
        || !std::sync::Arc::ptr_eq(&expected.token, &update.request.token)
    {
        return None;
    }
    *pending = None;
    if update.request.active != active || update.request.root != root {
        return None;
    }
    Some(update.outcome)
}

fn action(args: &str) -> Result<&'static str> {
    match args.trim() {
        "" | "status" => Ok("status"),
        "stop" => Ok("stop"),
        "resume" => Ok("resume"),
        _ => anyhow::bail!("Usage: /drone [status|stop|resume]"),
    }
}

fn verify_status(data: &Value, active: &str, root: &str) -> Result<u64> {
    ensure!(
        data["rootSessionId"] == root
            && data["activeSessionId"] == active
            && data["userStopped"].is_boolean()
            && data["inFlight"].is_boolean()
            && data["pendingCancel"].is_boolean()
            && data["cancellationUnconfirmed"].is_boolean(),
        "Drone control returned a different or incomplete conversation identity"
    );
    data["revision"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("Drone control revision is unavailable"))
}

async fn request_control<F, R>(
    mut request: F,
    active: String,
    root: String,
    action: &str,
) -> Result<Value>
where
    F: FnMut(DaemonCommand) -> R,
    R: Future<Output = Result<Value>>,
{
    let command = |action: &str, expected_revision| DaemonCommand::ControlDrone {
        id: None,
        active_session_id: active.clone(),
        root_session_id: root.clone(),
        action: action.into(),
        expected_revision,
        rest: Map::default(),
    };
    let status = request(command("status", None)).await?;
    let revision = verify_status(&status, &active, &root)?;
    if action == "status" {
        return Ok(status);
    }
    // A conflict is returned to the human. Never retry an old resume over a newer stop.
    let changed = request(command(action, Some(revision))).await?;
    verify_status(&changed, &active, &root)?;
    ensure!(
        changed["userStopped"] == (action == "stop"),
        "Drone did not confirm the requested control state; inspect /drone status"
    );
    Ok(changed)
}

fn describe(data: &Value) -> Result<&'static str> {
    match data["state"].as_str() {
        Some("paused") if data["inFlight"] == false && data["pendingCancel"] == false =>
            Ok("Drone stopped for this conversation. Use /drone resume to enable it."),
        Some("stopping") => Ok("Drone stopping: its admitted assessment is still settling. Use /drone status to inspect it."),
        Some("cancellation_unconfirmed") => Ok("Drone cancellation is unconfirmed. No new assessment or resume is admitted; inspect /drone status."),
        Some("enabled") if data["inFlight"] == true => Ok("Drone enabled for this conversation; one assessment is in flight."),
        Some("enabled") => Ok("Drone enabled for this conversation; no assessment is in flight."),
        _ => anyhow::bail!("Drone control status is unavailable"),
    }
}

impl SessionUi {
    pub(super) fn start_drone_command(&mut self, args: &str, view: &mut AgentView) {
        let action = match action(args) {
            Ok(action) => action,
            Err(error) => {
                self.error_row(&error.to_string(), view);
                return;
            }
        };
        let supported = self
            .client
            .hello()
            .get("serverCapabilities")
            .and_then(Value::as_array)
            .is_some_and(|caps| caps.iter().any(|cap| cap == "drone_control_v1"));
        if !supported {
            self.note(
                "/drone requires a Native daemon with conversation Drone controls.",
                view,
            );
            return;
        }
        if self.drone_pending.is_some() {
            self.note(
                "A Drone control request is still pending; it has not been replayed.",
                view,
            );
            return;
        }
        let client = self.client.clone();
        let active = self.active_session_id.clone();
        let root = self.session_id.clone();
        let pending = PendingDroneControl {
            root: root.clone(),
            active: active.clone(),
            token: std::sync::Arc::new(()),
        };
        self.drone_pending = Some(pending.clone());
        let updates = self.drone_updates.clone();
        self.note("Reading this conversation's Drone control…", view);
        tokio::spawn(async move {
            let outcome = tokio::time::timeout(
                std::time::Duration::from_millis(UI_REQUEST_TIMEOUT_MS),
                request_control(
                    move |command| {
                        let client = client.clone();
                        async move { client.request_ok(command).await }
                    },
                    active,
                    root.clone(),
                    action,
                ),
            )
            .await
            .map_err(|_| {
                "Drone control timed out; use /drone status. The command was not replayed.".into()
            })
            .and_then(|result| result.map_err(|error| error.to_string()));
            let _ = updates.send(DroneUpdate {
                request: pending,
                outcome,
            });
        });
    }

    pub(crate) fn apply_drone_update(&mut self, update: DroneUpdate, view: &mut AgentView) {
        let Some(outcome) = accept_update(
            &mut self.drone_pending,
            &self.active_session_id,
            &self.session_id,
            update,
        ) else {
            return;
        };
        match outcome.and_then(|data| {
            describe(&data)
                .map(str::to_owned)
                .map_err(|error| error.to_string())
        }) {
            Ok(note) => self.note(&note, view),
            Err(error) => self.error_row(&error, view),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[tokio::test]
    async fn slash_control_uses_only_exact_root_control_and_one_cas_without_prompt() {
        let registry = pa_types::slash_commands::SlashCommandRegistry::builtin();
        let parsed = registry.parse("/drone stop").unwrap();
        assert_eq!(action(&parsed.args).unwrap(), "stop");
        assert_eq!(
            registry.get(parsed.name).unwrap().execution,
            pa_types::slash_commands::SlashCommandExecution::Client
        );
        for invalid in ["stop all", "pause", "resume stop", "stop\nresume"] {
            assert!(action(invalid).is_err());
        }
        let mut calls = Vec::new();
        let result = request_control(|command| {
            calls.push(command);
            std::future::ready(Ok(json!({"rootSessionId":"root","activeSessionId":"active","revision":7,
                "userStopped":true,"inFlight":false,"pendingCancel":false,"cancellationUnconfirmed":false,"state":"paused"})))
        }, "active".into(), "root".into(), "stop").await.unwrap();
        assert_eq!(result["revision"], 7);
        assert_eq!(
            serde_json::to_value(&calls[1]).unwrap(),
            json!({"type":"drone_control","activeSessionId":"active","rootSessionId":"root","action":"stop","expectedRevision":7})
        );
        assert_eq!(
            calls,
            vec![
                DaemonCommand::ControlDrone {
                    id: None,
                    active_session_id: "active".into(),
                    root_session_id: "root".into(),
                    action: "status".into(),
                    expected_revision: None,
                    rest: Map::default()
                },
                DaemonCommand::ControlDrone {
                    id: None,
                    active_session_id: "active".into(),
                    root_session_id: "root".into(),
                    action: "stop".into(),
                    expected_revision: Some(7),
                    rest: Map::default()
                }
            ]
        );
    }
    #[tokio::test]
    async fn foreign_status_and_cas_error_cannot_send_or_retry_a_resume() {
        let mut calls = 0;
        assert!(request_control(
            |_| {
                calls += 1;
                std::future::ready(Ok(json!({"rootSessionId":"foreign"})))
            },
            "active".into(),
            "root".into(),
            "resume"
        )
        .await
        .is_err());
        assert_eq!(calls, 1);
        calls = 0;
        assert!(request_control(|_| {
            calls += 1;
            std::future::ready(if calls == 1 { Ok(json!({"rootSessionId":"root","activeSessionId":"active","revision":2,"userStopped":true,"inFlight":false,"pendingCancel":false,"cancellationUnconfirmed":false})) } else { Err(anyhow::anyhow!("drone_control_conflict")) })
        }, "active".into(), "root".into(), "resume").await.is_err());
        assert_eq!(calls, 2);
    }
    #[test]
    fn stopping_or_unknown_is_never_rendered_as_stopped() {
        assert!(describe(&json!({"state":"stopping","inFlight":true}))
            .unwrap()
            .contains("still settling"));
        assert!(
            describe(&json!({"state":"cancellation_unconfirmed","inFlight":true}))
                .unwrap()
                .contains("unconfirmed")
        );
        assert!(describe(&json!({"state":"paused","inFlight":true})).is_err());
    }
    #[test]
    fn old_callback_after_same_root_reopen_never_clears_or_renders_the_new_request() {
        let old = PendingDroneControl {
            root: "same-root".into(),
            active: "same-active".into(),
            token: std::sync::Arc::new(()),
        };
        let new = PendingDroneControl {
            root: "same-root".into(),
            active: "same-active".into(),
            token: std::sync::Arc::new(()),
        };
        let mut pending = Some(new.clone());
        assert!(accept_update(
            &mut pending,
            "same-active",
            "same-root",
            DroneUpdate {
                request: old,
                outcome: Ok(serde_json::json!({"state":"paused"}))
            }
        )
        .is_none());
        assert!(std::sync::Arc::ptr_eq(
            &pending.as_ref().unwrap().token,
            &new.token
        ));
        let current = serde_json::json!({"state":"enabled"});
        assert_eq!(
            accept_update(
                &mut pending,
                "same-active",
                "same-root",
                DroneUpdate {
                    request: new,
                    outcome: Ok(current.clone())
                }
            ),
            Some(Ok(current))
        );
        assert!(pending.is_none());
    }
}
