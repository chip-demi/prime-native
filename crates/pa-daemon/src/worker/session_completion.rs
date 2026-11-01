//! Opt-in top-level settlement delivery. A settlement is evidence, not task acceptance.
use super::{json, TurnRunner};
use anyhow::{Context, Result};
use std::time::Duration;

const ASSIGNMENT: &str = "rlm_session_assignment";
const DELIVERY: &str = "rlm_session_completion_delivery";

impl TurnRunner {
    pub(super) fn record_session_settlement(&self) {
        let mut core = self.core.lock().unwrap();
        if core.abort_requested || core.turn_cancelled || core.shutdown_requested {
            return;
        }
        let Some(store) = core.store.as_mut() else {
            return;
        };
        if !store
            .entries
            .iter()
            .any(|row| row.type_ == "custom_message" && row.fields["customType"] == ASSIGNMENT)
        {
            return;
        }
        if let Some(id) = store
            .entries
            .iter()
            .rev()
            .find(|row| row.type_ == "message" && row.fields["message"]["role"] == "assistant")
            .map(|row| row.id.clone())
        {
            if let Err(error) = store.persist_entry(
                "custom",
                json!({"customType":"rlm_session_settlement", "data":{"assistantId":id}}),
            ) {
                eprintln!("pa-daemon: session settlement persist failed: {error:#}");
            }
        }
    }

    pub(super) async fn deliver_session_completion(&self) {
        if let Err(error) = self.try_session_completion().await {
            eprintln!("pa-daemon: session completion remains unresolved: {error:#}");
        }
    }

    async fn try_session_completion(&self) -> Result<()> {
        let subscribed = self
            .core
            .lock()
            .unwrap()
            .store
            .as_ref()
            .is_some_and(|store| {
                store.entries.iter().any(|row| {
                    row.type_ == "custom_message" && row.fields["customType"] == ASSIGNMENT
                })
            });
        if !subscribed {
            return Ok(());
        }
        if self.engine.settlement_continuation_pending().await {
            return Ok(());
        }
        let pending = {
            let mut core = self.core.lock().unwrap();
            if core.busy
                || core.abort_requested
                || core.turn_cancelled
                || core.shutdown_requested
                || core.queued_input_suspended
                || core.compacting
                || !core.steering.is_empty()
                || !core.follow_up.is_empty()
                || self.input_pauses.paused()
                || self.user_bash.is_running()
                || self.engine.has_background_bash()
                || self.engine.has_running_subagents()
            {
                return Ok(());
            }
            let Some(store) = core.store.as_mut() else {
                return Ok(());
            };
            if store.path.as_os_str().is_empty() {
                return Ok(());
            }
            if !store
                .entries
                .iter()
                .any(|row| row.type_ == "custom_message" && row.fields["customType"] == ASSIGNMENT)
            {
                return Ok(());
            }
            let durable = crate::session_store::SessionFile::open(&store.path)?;
            // Only the latest assignment can settle. Any later accepted user/custom
            // input invalidates the old result; bash completion is continuation.
            let Some(index) = durable.entries.iter().rposition(|row| {
                row.type_ == "custom_message" && row.fields["customType"] == ASSIGNMENT
            }) else {
                return Ok(());
            };
            let assignment = durable.entries[index].fields["details"].clone();
            let tail = &durable.entries[index + 1..];
            if tail.iter().any(|row| {
                (row.type_ == "message" && row.fields["message"]["role"] == "user")
                    || (row.type_ == "custom_message"
                        && row.fields["customType"] == "agent_message")
            }) {
                return Ok(());
            }
            let Some(answer_row) = tail
                .iter()
                .rev()
                .find(|row| row.type_ == "message" && row.fields["message"]["role"] == "assistant")
            else {
                return Ok(());
            };
            let answer = &answer_row.fields["message"];
            if !matches!(answer["stopReason"].as_str(), None | Some("stop" | "error")) {
                return Ok(());
            }
            let id = assignment["deliveryId"]
                .as_str()
                .context("missing completion delivery id")?;
            let prior = tail.iter().rev().find(|row| {
                row.type_ == "custom"
                    && row.fields["customType"] == DELIVERY
                    && row.fields["data"]["deliveryId"] == id
            });
            if prior.is_some_and(|row| row.fields["data"]["state"] == "durably_queued") {
                return Ok(());
            }
            let attempted = prior.is_some();
            // A crash-recovered assistant row alone is not a fully settled turn.
            if !attempted
                && !tail.iter().any(|row| {
                    row.type_ == "custom"
                        && row.fields["customType"] == "rlm_session_settlement"
                        && row.fields["data"]["assistantId"] == answer_row.id
                })
            {
                return Ok(());
            }
            let text = answer["content"].as_array().map_or_else(
                || answer["content"].as_str().unwrap_or_default().to_string(),
                |blocks| {
                    blocks
                        .iter()
                        .filter_map(|block| block["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                },
            );
            let result = json!({"assignmentId":assignment["assignmentId"], "artifactRef":assignment["artifactRef"],
                "deliveryId":id,"sessionId":store.session_id(),"sessionFile":store.path.to_string_lossy(),
                "status":"settled", "stopReason":answer["stopReason"],
                "resultPreview":text.chars().take(4096).collect::<String>(), "taskAccepted":false});
            if !attempted {
                store.persist_entry(
                    "custom",
                    json!({"customType":DELIVERY,"data":{"deliveryId":id,"state":"unknown"}}),
                )?;
            }
            Some((assignment, result, attempted))
        };
        let Some((assignment, result, attempted)) = pending else {
            return Ok(());
        };
        let target = assignment["creatorSessionId"]
            .as_str()
            .context("missing durable creator")?;
        let id = assignment["deliveryId"]
            .as_str()
            .context("missing delivery id")?;
        if attempted {
            // Query only. A missing ACK/receipt is UNKNOWN, never permission to resend.
            let response = self
                .passivation
                .link
                .request(
                    json!({"type":"get_input_receipts", "activeSessionId":target,
                "queueKey":id,"completionTargetFile":assignment["creatorSessionFile"]}),
                    Duration::from_secs(30),
                )
                .await?;
            if !response.success
                || !response.data.as_ref().is_some_and(|data| {
                    data["receipts"]
                        .as_array()
                        .is_some_and(|rows| !rows.is_empty())
                        || data["durablyQueued"] == true
                })
            {
                return Ok(());
            }
        } else {
            let message = format!(
                "[session-settled; not task acceptance]\n{}",
                serde_json::to_string(&result)?
            );
            let response = self.passivation.link.request(json!({"type":"follow_up", "activeSessionId":target,
                "source":"rlm_session_completion", "completionTargetFile":assignment["creatorSessionFile"], "extensionVersion":2,"queueKey":id,"message":message}),
                Duration::from_secs(30)).await?;
            anyhow::ensure!(
                response.success,
                "completion queue refused: {:?}",
                response.error
            );
        }
        let mut core = self.core.lock().unwrap();
        let store = core
            .store
            .as_mut()
            .context("completion store unavailable")?;
        anyhow::ensure!(
            result["sessionId"] == store.session_id(),
            "completion session changed"
        );
        store.persist_entry(
            "custom",
            json!({"customType":DELIVERY,"data":{"deliveryId":id,"state":"durably_queued"}}),
        )?;
        Ok(())
    }
}
