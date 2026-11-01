//! Exact request control, independent of the busy turn queue.
use super::{
    json, response_failure, response_success, Arc, Context, DaemonResponse, QueuePriority, Result,
    SessionFile, Value, Worker, WorkerRecoveryJournal,
};
use sha2::{Digest, Sha256};
const RECOVERY_BUDGET: u64 = 2;
const RECOVERY_COOLDOWN_MS: u64 = 30_000;

pub(super) fn recovery_goal_allows_resume(expected: &Value, current: &Value) -> bool {
    use pa_core::goals::{GoalState, GoalStatus};
    let (Ok(expected), Ok(current)) = (
        serde_json::from_value::<GoalState>(expected.clone()),
        serde_json::from_value::<GoalState>(current.clone()),
    ) else {
        return false;
    };
    expected.goal_id == current.goal_id
        && expected.objective == current.objective
        && match expected.status {
            GoalStatus::Active => {
                current.active
                    && current.status == GoalStatus::Active
                    && current
                        .token_budget
                        .is_none_or(|budget| current.tokens_used < budget)
            }
            GoalStatus::Idle => {
                expected.goal_id.is_none()
                    && expected.objective.is_none()
                    && !current.active
                    && current.status == GoalStatus::Idle
            }
            GoalStatus::Paused
            | GoalStatus::BudgetLimited
            | GoalStatus::Complete
            | GoalStatus::Error => false,
        }
}

fn message_text(content: &Value) -> String {
    if let Some(text) = content.as_str() {
        return text.to_owned();
    }
    content
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|part| part["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

// This conservative recognition restricts Drone advice only. It never grants or
// changes the root's real RLM permissions. Only genuine user entries call it.
fn subagent_task_instruction(text: &str) -> Option<Value> {
    const PROHIBITIONS: &[&str] = &[
        "no subagents",
        "no sub-agents",
        "no sub agents",
        "without subagents",
        "do not spawn subagents",
        "do not use subagents",
        "don't spawn subagents",
        "don't use subagents",
        "must not spawn subagents",
        "must not use subagents",
        "work alone",
        "без субагентов",
        "не используй субагентов",
        "не запускай субагентов",
        "субагенты запрещены",
    ];
    const PERMISSIONS: &[&str] = &[
        "you may use subagents",
        "you may spawn subagents",
        "subagents are allowed",
        "you can now use subagents",
        "разрешаю использовать субагентов",
        "разрешаю запуск субагентов",
        "теперь используй субагентов",
        "можно использовать субагентов",
    ];
    text.lines().rev().find_map(|line| {
        let lower = line.to_lowercase();
        let matched = PROHIBITIONS
            .iter()
            .map(|p| (*p, false))
            .chain(PERMISSIONS.iter().map(|p| (*p, true)))
            .filter_map(|(pattern, allowed)| lower.rfind(pattern).map(|at| (at, allowed)))
            .max_by_key(|(at, _)| *at)?;
        // Preserve the exact source instruction, including a prohibition beyond the
        // old initialTask prefix. Long lines get an explicitly accounted exact window.
        let at = lower[..matched.0].chars().count();
        let start = at.saturating_sub(256);
        let instruction = line.chars().skip(start).take(2048).collect::<String>();
        Some(
            json!({"taskAllowsSubagents":matched.1,"instruction":instruction,
            "source":"genuine_user","omittedPrefixCharacters":start,
            "omittedSuffixCharacters":line.chars().count().saturating_sub(start + 2048)}),
        )
    })
}

fn history_evidence(store: Option<&SessionFile>) -> Value {
    let entries = store
        .map(|store| store.entries.as_slice())
        .unwrap_or_default();
    let humans = entries.iter().filter(|entry| {
        let message = &entry.fields["message"];
        if message["role"] != "user" || message["source"] == "drone"
            || message["origin"] == "drone" || entry.fields["source"] == "drone"
            || entry.fields["origin"] == "drone" {
            return false;
        }
        // The old queue discarded Drone provenance. Recognize only its exact
        // generated prefixes; retain raw history and explicitly identified users.
        let text = message_text(&message["content"]);
        entry.fields["inputIdentity"]["version"] == 2
            || !(text.starts_with("[Drone diagnostic; unresolved] ")
                || text.starts_with("[Drone] Repeated assessment failures:")
                || text == "[Drone] Automatic assessment budget exhausted. Review bounded Native evidence; no operation was replayed.")
    }).collect::<Vec<_>>();
    let latest = humans.iter().rev().take(3).collect::<Vec<_>>();
    let subagent_policy = humans
        .iter()
        .rev()
        .find_map(|entry| {
            subagent_task_instruction(&message_text(&entry.fields["message"]["content"])).map(
                |mut policy| {
                    policy["entryId"] = json!(entry.id);
                    policy
                },
            )
        })
        .unwrap_or_else(|| json!({"taskAllowsSubagents":null,"source":"unspecified"}));
    let identity = json!({
        "initial":humans.first().map(|entry|json!({"id":entry.id,"content":entry.fields["message"]["content"]})),
        "latest":latest.iter().map(|entry|json!({"id":entry.id,"content":entry.fields["message"]["content"]})).collect::<Vec<_>>()
    });
    json!({
        "initialTask":humans.first().map(|entry|message_text(&entry.fields["message"]["content"]).chars().take(2048).collect::<String>()),
        "subagentPolicy":subagent_policy,
        "latestUserCorrections":latest.iter().map(|entry|message_text(&entry.fields["message"]["content"]).chars().take(2048).collect::<String>()).collect::<Vec<_>>(),
        "humanInputFingerprint":format!("{:x}",Sha256::digest(identity.to_string().as_bytes())),
        "recentRootAssistantEvidence":entries.iter().rev().filter(|entry|entry.fields["message"]["role"]=="assistant")
            .filter_map(|entry| {
                let text = message_text(&entry.fields["message"]["content"]);
                (!text.is_empty()).then(||json!({"entryId":entry.id,"text":text.chars().take(768).collect::<String>()}))
            }).take(3).collect::<Vec<_>>()
    })
}

impl Worker {
    pub(crate) fn handle_drone_control(&self, command: &str, payload: &Value) -> DaemonResponse {
        let result = (|| -> Result<Value> {
            let core = self.core.lock().unwrap();
            let mut history = history_evidence(core.store.as_ref());
            history["subagentPolicy"]["maxDepth"] =
                self.engine.rlm_max_depth_status()["maxDepth"].clone();
            let identity = |request: Option<pa_agent::agent_loop::request::RequestSnapshot>| {
                json!({
                    "activeSessionId":core.active_session_id,
                    "sessionId":core.store.as_ref().map(super::super::session_store::SessionFile::session_id),
                    "sessionFile":core.store.as_ref().map(|s|s.path.display().to_string()),
                    "cwd":core.cwd, "workerIncarnation":self.config.worker_instance_id,
                    "generation":core.generation,"turnSerial":core.turn_serial,"request":request,
                    "tools":core.running_tool_calls,"compacting":core.compacting,
                    "queuedInputs":core.steering.len()+core.follow_up.len(),"hasBackgroundBash":self.engine.has_background_bash(),"hasRunningChildren":self.engine.has_running_subagents(),"goal":self.engine.goal_state_value(),
                    "initialTask":history["initialTask"],
                    "subagentPolicy":history["subagentPolicy"],
                    "roadmapEvidence":"capability_missing",
                    "latestUserCorrections":history["latestUserCorrections"],
                    "humanInputFingerprint":history["humanInputFingerprint"],
                    "recentRootAssistantEvidence":history["recentRootAssistantEvidence"],
                    "pendingHumanInputs":core.steering.iter().chain(core.follow_up.iter()).filter(|item|item.priority==QueuePriority::Human && item.custom_message.is_none()).count(),
                    "paused":self.input_pauses.paused(),"stopped":core.abort_requested || core.shutdown_requested,
                })
            };
            let control = self.engine.drone_request_control();
            if command == "drone_snapshot" {
                return Ok(identity(control.and_then(|c| c.snapshot())));
            }
            let mut recovery = self.recovery.lock().unwrap();
            let journal = recovery.as_mut().context("recovery_journal_unavailable")?;
            if command == "drone_status" {
                let mut record = journal
                    .drone_operation(&core.active_session_id)
                    .cloned()
                    .unwrap_or(json!({"status":"unknown"}));
                if let Some(id) = payload
                    .get("operationId")
                    .filter(|id| *id != &record["request"]["operationId"])
                {
                    return Ok(record["history"]
                        .as_array()
                        .and_then(|history| {
                            history
                                .iter()
                                .find(|op| &op["request"]["operationId"] == id)
                        })
                        .cloned()
                        .unwrap_or(json!({"status":"unknown"})));
                }
                if let Some(operation) = record["request"]["operationId"].as_str() {
                    if let Some(status) =
                        control.as_ref().and_then(|c| c.operation_status(operation))
                    {
                        record["status"] = json!(status);
                        journal.record_drone_operation(record.clone())?;
                    }
                }
                return Ok(record);
            }
            let request: pa_types::daemon::drone::DroneRecoverRequest =
                serde_json::from_value(payload["request"].clone())?;
            anyhow::ensure!(
                !request.operation_id.is_empty() && request.operation_id.len() <= 128,
                "invalid_operation_id"
            );
            if let Some(previous) = journal.drone_operation(&core.active_session_id) {
                if let Some(known) = previous["history"].as_array().and_then(|history| {
                    history
                        .iter()
                        .find(|op| op["request"]["operationId"] == request.operation_id)
                }) {
                    anyhow::ensure!(known["request"] == payload["request"], "operation_conflict");
                    return Ok(known.clone());
                }
                if previous["request"]["operationId"] == request.operation_id {
                    anyhow::ensure!(
                        previous["request"] == payload["request"],
                        "operation_conflict"
                    );
                    let mut previous = previous.clone();
                    if let Some(status) = control
                        .as_ref()
                        .and_then(|c| c.operation_status(&request.operation_id))
                    {
                        previous["status"] = json!(status);
                        journal.record_drone_operation(previous.clone())?;
                    }
                    return Ok(previous);
                }
                anyhow::ensure!(
                    matches!(
                        previous["status"].as_str(),
                        Some("refused" | "continued" | "held")
                    ),
                    "recovery_unconfirmed"
                );
                anyhow::ensure!(
                    previous["recoveryCount"].as_u64().unwrap_or(1) < RECOVERY_BUDGET,
                    "recovery_budget_exhausted"
                );
                let previous_at = previous["recordedAtMs"].as_u64().unwrap_or(0);
                anyhow::ensure!(
                    crate::util::now_ms().saturating_sub(previous_at) >= RECOVERY_COOLDOWN_MS,
                    "recovery_cooldown"
                );
            }
            anyhow::ensure!(
                core.runtime_kind == "top-level" && core.parent_active_session_id.is_none(),
                "authority_denied"
            );
            let expected = &request.identity;
            anyhow::ensure!(
                expected.active_session_id == core.active_session_id
                    && expected.worker_incarnation == self.config.worker_instance_id
                    && expected.generation == core.generation
                    && expected.turn_serial == core.turn_serial
                    && expected.cwd == core.cwd
                    && core
                        .store
                        .as_ref()
                        .is_some_and(|s| s.session_id() == expected.session_id
                            && s.path.display().to_string() == expected.session_file),
                "stale_identity"
            );
            anyhow::ensure!(
                !core.abort_requested
                    && !core.shutdown_requested
                    && !core.queued_input_suspended
                    && !self.input_pauses.paused(),
                "authority_denied"
            );
            anyhow::ensure!(
                !core.compacting && core.running_tool_calls.is_empty(),
                "not_model_request"
            );
            let control = control.context("not_model_request")?;
            let goal_engine = self
                .agent_engine
                .as_ref()
                .context("goal_state_unavailable")?;
            let recovery_goal = goal_engine
                .current_goal_state()
                .and_then(|goal| serde_json::to_value(goal).ok())
                .context("goal_state_unavailable")?;
            anyhow::ensure!(
                recovery_goal_allows_resume(&recovery_goal, &recovery_goal),
                "authority_denied"
            );
            let snapshot = control.snapshot().context("not_model_request")?;
            anyhow::ensure!(
                snapshot.request_id == expected.request_id
                    && snapshot.revision == expected.revision,
                "stale_request"
            );
            anyhow::ensure!(
                snapshot.phase == "transport"
                    && snapshot
                        .deadline_ms
                        .is_some_and(|deadline| snapshot.elapsed_ms >= deadline),
                "native_evidence_insufficient"
            );
            // Persist intent BEFORE cancellation; a lost reply never authorizes replay.
            let count = journal
                .drone_operation(&core.active_session_id)
                .map_or(0, |op| op["recoveryCount"].as_u64().unwrap_or(1));
            let history = journal
                .drone_operation(&core.active_session_id)
                .map(|op| {
                    let mut history = op["history"].as_array().cloned().unwrap_or_default();
                    let mut last = op.clone();
                    last.as_object_mut().unwrap().remove("history");
                    history.push(last);
                    history
                })
                .unwrap_or_default();
            anyhow::ensure!(history.len() < 16, "operation_history_budget_exhausted");
            let mut record = json!({"type":"drone_operation","activeSessionId":core.active_session_id,
                "request":request,"status":"recovery_unconfirmed","recoveryCount":count+1,"recordedAtMs":crate::util::now_ms(),"history":history});
            journal.record_drone_operation(record.clone())?;
            // Capture the actual goal at cancellation, not at turn creation:
            // a goal may have started during this run. The core lock also
            // fences continuation until the targeted cancel is registered.
            let resume_core = Arc::clone(&self.core);
            let resume_pauses = self.input_pauses.clone();
            let resume_engine = Arc::downgrade(goal_engine);
            let generation = core.generation.clone();
            let turn = core.turn_serial;
            control.set_resume_guard(Arc::new(move || {
                let core = resume_core.lock().unwrap();
                core.generation == generation
                    && core.turn_serial == turn
                    && !core.abort_requested
                    && !core.shutdown_requested
                    && !core.queued_input_suspended
                    && !core.compacting
                    && core.running_tool_calls.is_empty()
                    && !resume_pauses.paused()
                    && resume_engine
                        .upgrade()
                        .and_then(|engine| engine.current_goal_state())
                        .and_then(|goal| serde_json::to_value(goal).ok())
                        .is_some_and(|goal| recovery_goal_allows_resume(&recovery_goal, &goal))
            }));
            match self
                .input_pauses
                .with_unpaused(|| {
                    control.recover(
                        expected.request_id,
                        expected.revision,
                        &request.operation_id,
                    )
                })
                .unwrap_or(Err("authority_denied"))
            {
                Ok(()) => {
                    record["status"] = json!("accepted");
                }
                Err(reason) => {
                    record["status"] = json!("refused");
                    record["reason"] = json!(reason);
                    record["recoveryCount"] = json!(count);
                }
            }
            journal.record_drone_operation(record.clone())?;
            Ok(record)
        })();
        match result {
            Ok(value) => response_success(None, command, Some(value)),
            Err(error) => response_failure(None, command, &error.to_string(), None),
        }
    }
}

/// Called only while core and pause admission locks are held.
pub(super) fn ordinary_request_allowed(
    journal: Option<&mut WorkerRecoveryJournal>,
    session: &str,
    control: Option<&pa_agent::agent_loop::request::RequestControl>,
) -> bool {
    let Some(journal) = journal else { return true };
    let Some(mut op) = journal.drone_operation(session).cloned().filter(|op| {
        matches!(
            op["status"].as_str(),
            Some("accepted" | "recovery_unconfirmed")
        )
    }) else {
        return true;
    };
    let status = control
        .and_then(|c| c.operation_status(op["request"]["operationId"].as_str().unwrap_or("")));
    if status.as_deref() != Some("continued") {
        return false;
    }
    op["status"] = json!("continued");
    journal.record_drone_operation(op).is_ok()
}
