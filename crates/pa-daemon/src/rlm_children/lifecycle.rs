//! The child-lifecycle concern: session creation (create/launch), the
//! prompt/kill/close routing, settle watching with its notices, and the
//! spawn-admission outbox types (`CreatedSessionIds`, `CreatedChild`).
use pa_core::session_engine::semantic_edges::ChildCancelledReason;

use super::{
    anyhow, cap_text, compact_rlm_text, create_rlm_child_failure_message,
    create_rlm_child_terminal_notice, json, now_ms, Arc, ChildCloseReason, ChildObservationSource,
    ChildRecord, Context, CustomMessage, DaemonCommand, DaemonSessionLifecycle, Duration, Map,
    Mutex, ParentIdentity, Path, PromptInput, Result, RlmChildTerminalNotice,
    SupervisorChildSessionsInner, Value, ANSWER_TEXT_MAX_CHARS, CREATE_TIMEOUT_MS,
    IDLE_WAIT_GRACE_MS, KILL_TIMEOUT_MS, NOTICE_DELIVERY_TIMEOUT_MS, PROMPT_TIMEOUT_MS,
    RUNTIME_METADATA_PROMPT_MAX, STATE_TIMEOUT_MS, WATCH_MAX_UNREACHABLE_POLLS,
    WATCH_POLL_INTERVAL_MS, WATCH_SETTLE_GRACE_MS, WATCH_WAIT_SLICE_MS,
};

fn set_child_parent(config: &mut Value, depth: u32, parent: Option<&str>) {
    if depth > 0 {
        if let Some(parent) = parent {
            config["parentSessionPath"] = json!(parent);
        }
    }
}

#[cfg(test)]
mod root_creation_tests {
    use super::*;

    #[test]
    fn only_children_inherit_parent_path() {
        let mut root = json!({"rlmDepth": 0});
        set_child_parent(&mut root, 0, Some("/sessions/creator.jsonl"));
        assert!(root.get("parentSessionPath").is_none());
        let mut child = json!({"rlmDepth": 1});
        set_child_parent(&mut child, 1, Some("/sessions/creator.jsonl"));
        assert_eq!(child["parentSessionPath"], "/sessions/creator.jsonl");
    }
}

/// Parsed ids of one created child session.
struct CreatedSessionIds {
    active_session_id: String,
    session_id: Option<String>,
    session_file: Option<String>,
    session_name: Option<String>,
}

pub(super) struct CreatedChild {
    pub(super) active_session_id: String,
    pub(super) session_id: Option<String>,
    pub(super) session_file: Option<String>,
    pub(super) session_name: Option<String>,
    pub(super) session_dir: String,
    pub(super) summary_rlm_depth: Option<u64>,
}

impl CreatedChild {
    fn from_summary(summary: &Value, session_dir: &Path) -> Result<Self> {
        let ids = SupervisorChildSessionsInner::created_summary_ids(summary)?;
        Ok(Self {
            active_session_id: ids.active_session_id,
            session_id: ids.session_id,
            session_file: ids.session_file,
            session_name: ids.session_name,
            session_dir: session_dir.to_string_lossy().to_string(),
            summary_rlm_depth: summary.get("rlmDepth").and_then(Value::as_u64),
        })
    }
}

/// The plain text of a custom row's content (the notice turn's model
/// prompt); `None` for non-text content shapes.
fn custom_message_text(message: &CustomMessage) -> Option<String> {
    match &message.content {
        pa_types::ai::UserContent::Text(text) => Some(text.clone()),
        pa_types::ai::UserContent::Blocks(_) => None,
    }
}

impl super::SupervisorChildSessions {
    pub(crate) async fn restore_owner_cancellations(
        &self,
        facts: &[crate::journal::OwnerCancelFact],
    ) -> Result<()> {
        let parent = self
            .inner
            .identity
            .lock()
            .expect("identity lock")
            .session_file
            .clone();
        for fact in facts {
            if parent.as_deref() != Some(fact.parent_session_file.as_str()) {
                continue;
            }
            if let Some(record) = self.inner.find_record(&fact.child_id).await {
                let lock = record.lock().await.admission_lock.clone();
                let Ok(_guard) = lock.try_lock() else {
                    continue;
                };
                let _ = self.inner.apply_owner_cancel(&record, fact).await;
            }
        }
        Ok(())
    }

    pub(crate) async fn owner_cancel_child(&self, request: &Value) -> Result<Value> {
        anyhow::ensure!(
            request
                .as_object()
                .is_some_and(|object| object.len() == 1 && object.contains_key("childId")),
            "Invalid owner cancellation request"
        );
        let child_id = request["childId"]
            .as_str()
            .ok_or_else(|| anyhow!("Child identity missing"))?;
        let record = self
            .inner
            .find_record(child_id)
            .await
            .ok_or_else(|| anyhow!("Direct child unavailable"))?;
        let lock = record.lock().await.admission_lock.clone();
        let _guard = lock.lock().await;
        let fact = {
            let row = record.lock().await;
            let parent = self
                .inner
                .identity
                .lock()
                .expect("identity lock")
                .session_file
                .clone()
                .ok_or_else(|| anyhow!("Parent file unavailable"))?;
            crate::journal::OwnerCancelFact {
                parent_session_file: parent,
                child_id: row.rlm_child_id.clone(),
                child_session_file: row
                    .session_file
                    .clone()
                    .ok_or_else(|| anyhow!("Child file unavailable"))?,
                admission_generation: row.admission_generation.clone(),
                expected_message_id: row.expected_message_id.clone(),
            }
        };
        let state = self
            .inner
            .command(
                &DaemonCommand::GetState {
                    id: None,
                    active_session_id: self.inner.parent_active_session_id.clone(),
                    rest: Map::default(),
                },
                STATE_TIMEOUT_MS,
            )
            .await?;
        if state["ownerCancelFacts"].as_array().is_some_and(|facts| {
            facts.iter().any(|value| {
                serde_json::from_value::<crate::journal::OwnerCancelFact>(value.clone())
                    .ok()
                    .as_ref()
                    == Some(&fact)
            })
        }) {
            self.inner.apply_owner_cancel(&record, &fact).await?;
            return Ok(
                json!({"ownerCancel":fact,"kind":"owner_cancel","processExitObserved":false,"alreadyRecorded":true}),
            );
        }
        let token = std::env::var("PRIME_AGENT_INTERNAL_DAEMON_WORKER_TOKEN")
            .map_err(|_| anyhow!("Native owner authentication unavailable"))?;
        let command = DaemonCommand::Kill {
            id: None,
            active_session_id: record.lock().await.active_session_id.clone(),
            rest: Map::from_iter([(
                "ownerCancel".into(),
                json!({"workerToken":token,"fact":fact}),
            )]),
        };
        let response = self.inner.command(&command, KILL_TIMEOUT_MS).await?;
        self.inner.apply_owner_cancel(&record, &fact).await?;
        Ok(response)
    }
}

impl SupervisorChildSessionsInner {
    async fn apply_owner_cancel(
        &self,
        record: &Arc<Mutex<ChildRecord>>,
        fact: &crate::journal::OwnerCancelFact,
    ) -> Result<()> {
        {
            let mut row = record.lock().await;
            anyhow::ensure!(
                self.identity
                    .lock()
                    .expect("identity lock")
                    .session_file
                    .as_deref()
                    == Some(fact.parent_session_file.as_str())
                    && row.rlm_child_id == fact.child_id
                    && row.session_file.as_deref() == Some(fact.child_session_file.as_str())
                    && row.admission_generation == fact.admission_generation
                    && row.expected_message_id == fact.expected_message_id,
                "Owner cancellation no longer matches child admission"
            );
            let mut gate = self.gate.lock().expect("gate lock");
            let changed = !row.owner_cancelled
                || !row.settled
                || row.admission_blocked
                || !gate.authenticated_owner_cancellations.contains(fact);
            if !gate.authenticated_owner_cancellations.contains(fact) {
                gate.authenticated_owner_cancellations.push(fact.clone());
            }
            if changed {
                self.accounting_epoch
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            row.owner_cancelled = true;
            row.admission_blocked = false;
            row.settled_status = Some("cancelled");
            row.settled = true;
            row.error = Some("Cancelled by owner under exclusive session lease; historical process exit not observed".into());
        }
        self.refresh_running().await;
        Ok(())
    }

    /// Create one child session over the supervisor link (no prompt yet);
    /// `model` is the resolved `provider/id` selector.
    ///
    /// `launch_env` is the environment the child's worker process runs
    /// in: the parent worker's own env (filtered) for an `rlm.spawn`
    /// child — TS runs those in-process inside the parent worker, so
    /// parity means the child inherits the parent's environment — or
    /// the minimal TS set for a depth-0 `create_session` root.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn create_child(
        &self,
        child_id: &str,
        name: Option<&str>,
        // The spawned task's prompt, mirrored into the create runtime
        // metadata (`None` for a depth-0 resident session).
        prompt: Option<&str>,
        depth: u32,
        model: &str,
        thinking: Option<&str>,
        cwd: &str,
        session_dir: &Path,
        // The parent's in-flight turn request the child anchors to (TS
        // `spawnedByRequestId` rides the child's session options, never
        // the runtime metadata).
        spawned_by_request_id: Option<&str>,
        runtime_metadata: Option<Value>,
        identity: &ParentIdentity,
        launch_env: Option<std::collections::BTreeMap<String, String>>,
    ) -> Result<CreatedChild> {
        let mut config = json!({
            "cwd": cwd,
            "sessionDir": session_dir.to_string_lossy(),
            "rlmDepth": depth,
            "rlmMaxDepth": identity.rlm_max_depth,
        });
        if let Some((provider, id)) = model.split_once('/') {
            config["provider"] = json!(provider);
            config["model"] = json!(id);
        } else {
            config["model"] = json!(model);
        }
        if let Some(thinking) = thinking {
            config["thinking"] = json!(thinking);
        }
        set_child_parent(&mut config, depth, identity.session_file.as_deref());
        if let Some(request_id) = spawned_by_request_id {
            config["spawnedByRequestId"] = json!(request_id);
        }
        if let Some(script) = &identity.child_script {
            config["script"] = json!(script);
            // The scripted engine rides the identity down the recursion (TS
            // inherits the parent's sessionConfig, so harness children spawn
            // the same way).
            config["childScript"] = json!(script);
        }
        // Runtime metadata mirrors the TS subagent runtime identity.
        let runtime_metadata = runtime_metadata.map(|mut metadata| {
            if let Some(session_id) = &identity.session_id {
                metadata["parentSessionId"] = json!(session_id);
            }
            if let Some(parent_file) = &identity.session_file {
                metadata["parentSessionFile"] = json!(parent_file);
            }
            if let Some(prompt) =
                prompt.filter(|prompt| prompt.len() <= RUNTIME_METADATA_PROMPT_MAX)
            {
                metadata["prompt"] = json!(prompt);
            }
            // The resolved model rides the metadata so the supervisor's
            // display entry carries it for passive hydration.
            if let Some((provider, model_id)) = model.split_once('/') {
                metadata["model"] = json!({ "provider": provider, "modelId": model_id });
            }
            metadata
        });
        let create = DaemonCommand::Create {
            id: None,
            session_path: None,
            continue_recent: None,
            no_session: None,
            name: name.map(str::to_string),
            config: Some(config),
            // RLM children never report telemetry (the depth-0 gate installs
            // nothing); the worker's own opt-out stays process-level.
            telemetry_disabled: None,
            runtime_metadata,
            lifecycle: Some(DaemonSessionLifecycle::Resident),
            env: None,
            launch_env,
            rest: Map::default(),
        };
        let response = self
            .link
            .request(
                serde_json::to_value(&create)?,
                Duration::from_millis(CREATE_TIMEOUT_MS),
            )
            .await
            .with_context(|| format!("spawn RLM child session {child_id}"))?;
        if !response.success {
            let message = response
                .error
                .unwrap_or_else(|| "Child create refused".into());
            if matches!(
                response.error_info,
                Some(
                    pa_types::daemon::DaemonErrorInfo::UpdateRestarting
                        | pa_types::daemon::DaemonErrorInfo::WorkerOverloaded
                )
            ) {
                return Err(super::next_work::CreateNotSent(message).into());
            }
            return Err(super::host::create_refused_error(message));
        }
        let summary = response.data.unwrap_or(Value::Null);
        let created = CreatedChild::from_summary(&summary, session_dir)?;
        Ok(created)
    }

    /// Create and promptly admit one child's task (the depth-0 resident
    /// session path: the prompt is part of the awaited admission).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn launch_child(
        &self,
        child_id: &str,
        name: Option<&str>,
        prompt: &str,
        depth: u32,
        model: &str,
        thinking: Option<&str>,
        cwd: &str,
        session_dir: &Path,
        runtime_metadata: Option<Value>,
        identity: &ParentIdentity,
        launch_env: Option<std::collections::BTreeMap<String, String>>,
    ) -> Result<CreatedChild> {
        let created = self
            .create_child(
                child_id,
                name,
                None,
                depth,
                model,
                thinking,
                cwd,
                session_dir,
                /*spawned_by_request_id*/ None,
                runtime_metadata,
                identity,
                launch_env,
            )
            .await?;
        // A failed prompt tears the just-created session down (TS kills the
        // created session in the create-path catch block).
        if let Err(error) = self
            .prompt_child(&created.active_session_id, prompt, None)
            .await
        {
            let _ = self
                .kill_child(&created.active_session_id, ChildCloseReason::Killed)
                .await;
            return Err(error);
        }
        Ok(created)
    }

    /// Parse a created-session summary into its ids.
    fn created_summary_ids(summary: &Value) -> Result<CreatedSessionIds> {
        let active_session_id = summary
            .get("activeSessionId")
            .or_else(|| summary.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow!("supervisor returned a session summary without an id"))?;
        Ok(CreatedSessionIds {
            active_session_id: active_session_id.to_string(),
            session_id: summary
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string),
            session_file: summary
                .get("sessionFile")
                .and_then(Value::as_str)
                .map(str::to_string),
            session_name: summary
                .get("sessionName")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    /// Send one child-addressed daemon command and retry it ONCE by the
    /// child's durable selector when its routing id no longer resolves
    /// (the passivation-aware wake, TS's tier-2 revival — shared by the
    /// prompt and the parent-directed rename): the child's worker was
    /// idle-evicted, or a prior wake re-keyed the supervisor roster's
    /// child row to a fresh worker id, so the routing id the caller
    /// holds (the record's spawn-time active id) resolves nowhere. The
    /// retry addresses the child by its DURABLE selector (its session id
    /// — the session-file stem the supervisor's wake resolves through
    /// the spawn ledger, then the child id) — the daemon's wake arm
    /// launches a fresh worker over the child's session file and the
    /// command lands on the replay. `subject` renders the failure
    /// contexts; the retry's context appends the wake provenance. A
    /// non-miss failure and a child with no record keep the original
    /// error.
    pub(super) async fn command_with_durable_wake(
        &self,
        make_command: impl Fn(&str) -> DaemonCommand,
        active_session_id: &str,
        timeout_ms: u64,
        subject: &str,
    ) -> Result<()> {
        let command = make_command(active_session_id);
        match self.command(&command, timeout_ms).await {
            Ok(_) => Ok(()),
            Err(error) => {
                let rendered = format!("{error:#}");
                if !rendered.starts_with("Unknown active session:") {
                    return Err(error).with_context(|| subject.to_string());
                }
                let durable = self.durable_child_selector_for(active_session_id).await;
                let Some(durable) = durable else {
                    return Err(error).with_context(|| subject.to_string());
                };
                let retry = make_command(&durable);
                self.command(&retry, timeout_ms)
                    .await
                    .with_context(|| format!("{subject} (woken by {durable})"))?;
                Ok(())
            }
        }
    }

    /// The durable selector of the child whose routing id is
    /// `active_session_id`: the address that reaches a passivated child
    /// through the supervisor's ledger wake.
    async fn durable_child_selector_for(&self, active_session_id: &str) -> Option<String> {
        let children = self.children.lock().await;
        for record in children.iter() {
            let record = record.lock().await;
            if record.active_session_id == active_session_id {
                return Some(crate::rlm_children::durable_child_selector(
                    record.session_id.as_deref(),
                    &record.rlm_child_id,
                ));
            }
        }
        None
    }

    /// Prompt one child over the supervisor link (TS `promptAndWait`):
    /// `custom_message` replaces the persisted user row (TS
    /// `customMessage`).
    pub(super) async fn prompt_child(
        &self,
        active_session_id: &str,
        prompt: &str,
        custom_message: Option<&Value>,
    ) -> Result<()> {
        let make_command = |selector: &str| DaemonCommand::Prompt {
            id: None,
            active_session_id: selector.to_string(),
            message: prompt.to_string(),
            input: PromptInput {
                content: None,
                images: None,
                streaming_behavior: None,
                queue_if_busy: None,
                expand_prompt_templates: None,
                source: Some(json!("rpc")),
                agent_message_id: None,
                custom_message: custom_message.cloned(),
                queue_key: None,
                prefix_messages: None,
                admission_id: None,
                rlm_notice_nonce: None,
            },
            rest: Map::default(),
        };
        self.command_with_durable_wake(
            make_command,
            active_session_id,
            PROMPT_TIMEOUT_MS,
            &format!("prompt RLM child session {active_session_id}"),
        )
        .await
    }

    pub(super) async fn kill_child(
        &self,
        active_session_id: &str,
        reason: ChildCloseReason,
    ) -> Result<()> {
        let mut rest = serde_json::Map::new();
        if let Some(marker) = reason.wire_marker() {
            rest.insert("rlmCloseReason".to_string(), json!(marker));
        }
        let command = DaemonCommand::Kill {
            id: None,
            active_session_id: active_session_id.to_string(),
            rest,
        };
        self.command(&command, KILL_TIMEOUT_MS)
            .await
            .with_context(|| format!("kill RLM child session {active_session_id}"))?;
        Ok(())
    }

    /// The child's live session summary (`get_state`): the busy verdict,
    /// the note, and the tool count read one round trip.
    pub(super) async fn child_state(&self, active_session_id: &str) -> Result<Value> {
        let record = self.find_record(active_session_id).await;
        let direct_observation = if let Some(record) = &record {
            Some(Arc::clone(&record.lock().await.direct_observation))
        } else {
            None
        };
        // Every direct read, including child_busy/usage/collect watchers, owns
        // one ordering slot through network capture and record application.
        let direct_guard = if let Some(lock) = &direct_observation {
            Some(lock.lock().await)
        } else {
            None
        };
        let identity = if let Some(record) = &record {
            let row = record.lock().await;
            Some((
                row.admission_generation.clone(),
                row.expected_message_id.clone(),
            ))
        } else {
            None
        };
        let request_order = self
            .observation_requests
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let command = DaemonCommand::GetState {
            id: None,
            active_session_id: active_session_id.to_string(),
            rest: Map::default(),
        };
        let result = self.command(&command, STATE_TIMEOUT_MS).await;
        if let Some(record) = &record {
            let mut row = record.lock().await;
            if identity
                != Some((
                    row.admission_generation.clone(),
                    row.expected_message_id.clone(),
                ))
            {
                return Err(anyhow!("Child assignment changed during observation"));
            }
            match &result {
                Ok(state) => Self::apply_live_state(
                    &mut row,
                    state,
                    request_order,
                    ChildObservationSource::Direct,
                ),
                Err(_) => {
                    if let Some(observation) = row.observation.as_mut() {
                        observation["available"] = json!(false);
                        observation["status"] = json!("unknown");
                    }
                }
            }
        }
        drop(direct_guard);
        if let Some(record) = &record {
            self.emit_child_update(record).await;
        }
        result
    }

    pub(super) async fn child_busy(&self, active_session_id: &str) -> Result<bool> {
        let state = self.child_state(active_session_id).await?;
        Ok(child_busy_flags(&state))
    }

    /// One `GetLastAssistantText` round trip: the raw text the roster
    /// preview and the collect envelope's binding lane both derive from
    /// (the preview compacts it; the binding lane caps it at
    /// [`ANSWER_TEXT_MAX_CHARS`] instead, so the whole fenced JSON a
    /// settle binds from survives).
    async fn child_answer_raw(&self, active_session_id: &str) -> Result<Option<String>> {
        let command = DaemonCommand::GetLastAssistantText {
            id: None,
            active_session_id: active_session_id.to_string(),
            rest: Map::default(),
        };
        let answer = self.command(&command, STATE_TIMEOUT_MS).await?;
        Ok(answer
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string))
    }

    /// Best-effort bounded wait for one child and its descendants to go
    /// idle. The wait is a
    /// snapshot helper, not a gate: its timeout is not an error, and the
    /// caller re-reads the child's state afterwards (TS collect: "a
    /// timeout returns current snapshots, never an error").
    pub(super) async fn wait_for_child(&self, active_session_id: &str, budget: Duration) {
        if budget.is_zero() {
            return;
        }
        // The child's own descendants keep it busy (`child_busy`), so the
        // wait holds until they settle too.
        let command = DaemonCommand::WaitForIdle {
            id: None,
            active_session_id: active_session_id.to_string(),
            wait_for_rlm_quiescence: Some(true),
            rest: Map::default(),
        };
        let _ = self
            .command(&command, budget.as_millis() as u64 + IDLE_WAIT_GRACE_MS)
            .await;
    }

    /// Refresh one record against its worker: settle a child whose worker ran out of work and
    /// capture its answer once; an unreachable child keeps its last known state. The same
    /// round trip observes the child's live note and tool count onto the record.
    pub(super) async fn refresh_record(&self, record: &Arc<Mutex<ChildRecord>>) {
        let admission_lock = record.lock().await.admission_lock.clone();
        let Ok(_observation) = admission_lock.try_lock() else {
            return;
        };
        {
            let record = record.lock().await;
            if !record.prompt_admitted || record.admission_blocked {
                return;
            }
        }
        // child_state serializes direct observations and verifies admission identity.
        let active_session_id = record.lock().await.active_session_id.clone();
        let state = self.child_state(&active_session_id).await;
        if let Ok(state) = &state {
            let row = record.lock().await;
            let mut gate = self.gate.lock().expect("gate lock");
            let mut next = gate.clone();
            if let Some(binding) = next
                .work_bindings
                .iter_mut()
                .find(|binding| binding.child_id == row.rlm_child_id && binding.start_permitted)
            {
                if let Some(fact) = state["messageLifecycleFacts"].as_array().and_then(|facts| {
                    facts.iter().find(|fact| {
                        fact["phase"] == "started"
                            && fact["messageId"].as_str() == row.expected_message_id.as_deref()
                            && fact["incarnation"] == state["workerInstanceId"]
                            && fact["admissionGeneration"].as_str()
                                == row.admission_generation.as_deref()
                    })
                }) {
                    binding.started_result = Some(
                        json!({"childId":row.rlm_child_id,"admissionGeneration":row.admission_generation,
                        "expectedMessageId":row.expected_message_id,"incarnation":fact["incarnation"]}),
                    );
                    match self.persist_gate(&next) {
                        Ok(()) => *gate = next,
                        Err(error) => {
                            eprintln!("pa-daemon: started result persist failed: {error:#}");
                        }
                    }
                }
            }
        }
        let idle = matches!(&state, Ok(state) if !child_busy_flags(state));
        if !idle {
            return;
        }
        // Capture the answer before taking the record lock (the capture is
        // a link round trip): one fetch yields the raw text both lanes
        // derive from — the compact preview for the roster rows and the
        // full text for the collect envelope's binding lane.
        let raw_answer = self
            .child_answer_raw(&active_session_id)
            .await
            .ok()
            .flatten();
        let (session_file, expected_message_id) = {
            let row = record.lock().await;
            (row.session_file.clone(), row.expected_message_id.clone())
        };
        // Idle is not a success verdict: the worker persists failed/aborted
        // assistant rows too. Read only this admission, never an earlier task.
        let terminal = match (session_file, expected_message_id) {
            (Some(path), Some(message_id)) => tokio::task::spawn_blocking(move || {
                final_turn_error_after_admission(&path, &message_id)
                    .map(|e| ("error", Some(compact_rlm_text(&e))))
                    .or_else(|| terminal_assistant_from_file(&path, &message_id))
            })
            .await
            .ok()
            .flatten(),
            _ => None,
        };
        let answer = raw_answer.as_deref().map(compact_rlm_text);
        let answer_text = raw_answer
            .as_deref()
            .map(|text| cap_text(text, ANSWER_TEXT_MAX_CHARS));
        let mut record = record.lock().await;
        if record.admission_blocked {
            return;
        }
        // The settle verdict is one-shot, the answer capture is not: a
        // `None` first capture (the settle raced the worker's answer
        // hand-off) froze the row empty before — the capture fill only
        // ran while `settled_status` was unset, so a later refresh could
        // never deliver the text and a re-collect answered the same empty
        // envelope forever. The fill now runs on every refresh: it only
        // writes when the fresh round trip produced a text AND the row has
        // no answer yet, so a settled preview is still never overwritten
        // with a later miss.
        if record.settled_status.is_none() {
            let (status, error) = terminal.unwrap_or(("done", None));
            record.settled_status = Some(status);
            record.error = error;
        }
        if (!record.answer_captured || record.answer_preview.is_none()) && answer.is_some() {
            record.answer_preview = answer;
            record.answer_text = answer_text;
            record.answer_captured = true;
        }
    }

    /// Watch one admitted child until its run settles, then deliver the parent's terminal notice
    /// (the failure row or the no-reply notice) when the child has not replied since its task;
    /// the watcher stops as soon as the record is removed.
    pub(super) async fn watch_child_settle(&self, record: &Arc<Mutex<ChildRecord>>) {
        let watch_lock = record.lock().await.settle_watch_lock.clone();
        let Ok(_watch) = watch_lock.try_lock() else {
            return;
        };
        let mut unreachable_polls: u32 = 0;
        loop {
            // The parent closed with this child running: the child dies
            // with it and no notice is owed — end without polling it.
            if record.lock().await.closed_by_parent {
                return;
            }
            if record.lock().await.admission_blocked {
                tokio::time::sleep(Duration::from_millis(WATCH_POLL_INTERVAL_MS)).await;
                continue;
            }
            // Slice-top usage flush: rows of turns that completed since the
            // last slice land here (crash loss is bounded to one slice).
            self.emit_child_usage(record).await;
            let active_session_id = record.lock().await.active_session_id.clone();
            // One bounded idle-wait slice: a timeout while the child still
            // runs re-slices; a returned slice means the queue drained.
            let idle_wait = self.wait_for_child(
                &active_session_id,
                Duration::from_millis(WATCH_WAIT_SLICE_MS),
            );
            tokio::pin!(idle_wait);
            // Keep live summaries fresh without restarting the outstanding idle wait.
            loop {
                tokio::select! {
                    () = &mut idle_wait => break,
                    () = tokio::time::sleep(Duration::from_millis(WATCH_POLL_INTERVAL_MS)) => {
                        self.refresh_record(record).await;
                    }
                }
            }
            self.refresh_record(record).await;
            let settled = record.lock().await.settled_status.is_some();
            if settled {
                // A delete in flight (`retiring`) owns this run's terminal
                // edge: the watcher must not run the settle tail into the
                // delete's kill window. The tail would record nothing
                // (`record_child_return` holds the mark), deliver the
                // terminal notice and exit, so a failed kill would strand
                // a settled row with no watcher and no ledger line, and a
                // confirmed kill would race its cancel line with the
                // notice, the goal hook and the display flip. Park
                // instead: a failed kill clears the mark and the loop
                // settles the run normally, and a confirmed kill closes
                // the record (`closed_by_parent` ends the watch at the
                // loop top).
                if record.lock().await.retiring {
                    tokio::time::sleep(Duration::from_millis(WATCH_POLL_INTERVAL_MS)).await;
                    continue;
                }
                // Stability re-check: a prompt admitted to an idle worker
                // can read idle once between the admission and the turn
                // pop (the queue snapshot and the busy flag change under
                // different locks on the far side of a socket). A short
                // grace closes that window; a child that went busy again
                // (a queued continuation) keeps watching. An unreachable
                // worker (an idle passivation, a crash) leaves the settled
                // verdict standing, as in `refresh_record`.
                //
                // The re-check wipes only a WATCHER-set settle (the
                // premature `done` of the admission race, or a refresh's
                // final-turn `error`, which claimed no notice yet): a
                // registry-set `cancelled` (a user abort or a parent kill
                // in flight, `cancel_child_run`/`close_children_inner`)
                // stands — wiping it would let this watcher refresh the
                // still-live run back to `done` and record a clean
                // `child_returned` beside the cancel line, so the row
                // keeps its registry settle and this loop keeps watching
                // until the abort lands (the cancel path owns the
                // terminal handling).
                tokio::time::sleep(Duration::from_millis(WATCH_SETTLE_GRACE_MS)).await;
                if matches!(self.child_busy(&active_session_id).await, Ok(true)) {
                    let mut record = record.lock().await;
                    let refresh_verdict = match record.settled_status {
                        Some("done") => true,
                        Some("error") => !record.notice_delivered,
                        _ => false,
                    };
                    if refresh_verdict {
                        record.settled_status = None;
                        record.error = None;
                    }
                    drop(record);
                    continue;
                }
                self.refresh_record(record).await;
                let admission_lock = record.lock().await.admission_lock.clone();
                let Ok(_settling) = admission_lock.try_lock() else {
                    continue;
                };
                {
                    let row = record.lock().await;
                    if row.admission_blocked || row.settled_status.is_none() {
                        continue;
                    }
                }
                // The run's final rows are attributed before the terminal
                // notice rides the parent's follow-up route.
                self.emit_final_child_usage(record).await;
                // TS records the return before delivering the notice: the
                // pending edge must be on the parent's ledger before the
                // notice's follow-up turn mints its own request.
                self.record_child_return(record).await;
                self.emit_child_update(record).await;
                // Only a successful run completes the display (TS
                // `completeRlmSubagentRuntime`); a cancelled or failed run
                // stays `running`, which a restart relists as `error`.
                let completed = {
                    let record = record.lock().await;
                    (record.settled_status == Some("done"))
                        .then(|| (record.session_dir.clone(), record.rlm_child_id.clone()))
                };
                if let Some((session_dir, child_id)) = completed {
                    if let Err(error) = tokio::task::spawn_blocking(move || {
                        let Some(mut display) =
                            crate::rlm_ledger::read_rlm_subagent_display(Path::new(&session_dir))
                        else {
                            return Ok(());
                        };
                        if display.child_id != child_id || display.status == "deleted" {
                            return Ok(());
                        }
                        display.status = "completed".to_string();
                        display.admission_unresolved = None;
                        crate::rlm_ledger::write_rlm_subagent_display(&display).map(|_| ())
                    })
                    .await
                    .unwrap_or_else(|error| Err(anyhow!(error)))
                    {
                        eprintln!("pa-daemon: RLM child display completion failed: {error:#}");
                    }
                }
                self.deliver_settle_notice(record).await;
                // A settled child releases an owed goal continuation.
                self.fire_settle_hook(record).await;
                return;
            }
            // Still running (a timed-out slice or a re-queued continuation):
            // re-check liveness so a dead worker cannot spin the watch.
            let busy = self.child_busy(&active_session_id).await;
            if busy.is_ok() {
                unreachable_polls = 0;
            } else {
                unreachable_polls += 1;
                if unreachable_polls >= WATCH_MAX_UNREACHABLE_POLLS {
                    if self
                        .settle_failed(
                            record,
                            "Child worker unreachable".to_string(),
                            FailedArm::Unreachable,
                        )
                        .await
                    {
                        return;
                    }
                    // The claim lost to a verdict a reader's refresh landed
                    // after this pass's settle read (the worker answered
                    // idle, so the dead streak is broken): the settled
                    // branch owes that verdict its notice and funnel. With
                    // no verdict left, a cancel or a parent close owns the
                    // tail.
                    if record.lock().await.settled_status.is_none() {
                        return;
                    }
                    unreachable_polls = 0;
                    continue;
                }
            }
            tokio::time::sleep(Duration::from_millis(WATCH_POLL_INTERVAL_MS)).await;
        }
    }

    /// Record a settled child's return in the parent's semantic-edge ledger
    /// (TS `recordChildReturned` at the run-settle sites): the child's last
    /// committed request, read from the child's own ledger file (the
    /// cross-process form of TS's in-process
    /// `child.semanticEdges.lastCommittedRequestId`). Only a `done` or
    /// `error` settle returns — a cancelled run never returns (it is the
    /// parent's action, so it records through
    /// [`Self::record_child_cancelled`] instead, nano-rlm #202) — and a
    /// delete in flight ([`ChildRecord::retiring`]) returns nothing either.
    /// A child without a durable session id, or a parent without a
    /// recorder, records nothing.
    async fn record_child_return(&self, record: &Arc<Mutex<ChildRecord>>) {
        let recorder = self
            .semantic_edges
            .lock()
            .expect("semantic edges lock")
            .clone();
        let Some(recorder) = recorder else {
            return;
        };
        let (child_session_id, child_dir) = {
            let record = record.lock().await;
            // A delete in flight owns this child's terminal edge: the
            // delete records a cancel line, so a watcher that wakes inside
            // the kill window must not append a clean return beside it.
            if record.retiring || !matches!(record.settled_status, Some("done" | "error")) {
                return;
            }
            (record.session_id.clone(), record.session_dir.clone())
        };
        let Some(child_session_id) = child_session_id else {
            return;
        };
        let ledger_path = pa_core::session_engine::semantic_edges::semantic_edge_ledger_path(
            Some(Path::new(&child_dir)),
            None,
        );
        // The file read stays off the async workers (the usage walk's
        // discipline).
        let last_committed = tokio::task::spawn_blocking(move || {
            ledger_path
                .as_deref()
                .and_then(pa_core::session_engine::semantic_edges::last_committed_request_id)
        })
        .await
        .ok()
        .flatten();
        recorder.record_child_returned(&child_session_id, last_committed);
    }

    /// Record a parent-cancelled child in the parent's semantic-edge
    /// ledger (nano-rlm #202's `recordSubagentCancel`): the cancel twin
    /// of a spawn — an action the PARENT took, anchored to the parent's
    /// in-flight (cancelling) request, never a passive child return.
    /// `reason` names the parent-side action (a delete, a user cancel,
    /// the session-kill close walk) on the line; a daemon lifecycle
    /// close never reaches here. A child without a durable session id,
    /// or a parent without a recorder, records nothing.
    pub(super) async fn record_child_cancelled(
        &self,
        record: &Arc<Mutex<ChildRecord>>,
        reason: ChildCancelledReason,
    ) {
        let recorder = self
            .semantic_edges
            .lock()
            .expect("semantic edges lock")
            .clone();
        let Some(recorder) = recorder else {
            return;
        };
        let child_session_id = record.lock().await.session_id.clone();
        let Some(child_session_id) = child_session_id else {
            return;
        };
        recorder.record_child_cancelled(&child_session_id, reason);
    }

    /// Deliver the settled child's terminal notice when it has not replied
    /// since its task: the failure row for a run whose final turn ended in
    /// an error, else the no-reply notice. A child that already replied
    /// gets neither (its answer is the parent's notice; the roster and
    /// collect still report the error). Exactly-once: the record's notice claim collapses the races between
    /// the watcher, a natural settle during `delete_subagent`, and the
    /// delete path itself.
    async fn deliver_settle_notice(&self, record: &Arc<Mutex<ChildRecord>>) {
        let message = {
            let mut record = record.lock().await;
            if record.notice_delivered || record.replied_since_task {
                // A prior reply is not a terminal delivery receipt. Keep the managed
                // binding for explicit acceptance; it never earns a result consult here.
                return;
            }
            record.notice_delivered = true;
            let failure = match (record.settled_status, &record.error) {
                (Some("error"), Some(error)) => Some(error.clone()),
                _ => None,
            };
            if let Some(error) = failure {
                create_rlm_child_failure_message(
                    &record.rlm_child_id,
                    &record.session_name,
                    &error,
                    now_ms(),
                )
            } else {
                let notice = RlmChildTerminalNotice::CompletedWithoutReply {
                    child_id: record.rlm_child_id.clone(),
                    session_name: record.session_name.clone(),
                    last_assistant_text_preview: record.answer_preview.clone(),
                };
                create_rlm_child_terminal_notice(&notice, now_ms())
            }
        };
        self.deliver_terminal_notice(record, message).await;
    }

    /// Settle one child as failed (TS's thrown-run arm): the record
    /// settles `error` with its failure text and the parent receives the
    /// `rlm_child_failure` row — whether or not the child replied — then
    /// the settle funnel fires like TS's `finally` resume. Exactly-once:
    /// a child that already holds a verdict keeps it
    /// (`should_mark_error`; #3104's passivation semantics: a verdict a
    /// reader's refresh lands inside the unreachable give-up keeps
    /// standing, and the claim reports the loss so the watcher runs that
    /// verdict's settle tail). Returns whether this call claimed the
    /// settle.
    pub(super) async fn settle_failed(
        &self,
        record: &Arc<Mutex<ChildRecord>>,
        error: String,
        arm: FailedArm,
    ) -> bool {
        let admission_lock = record.lock().await.admission_lock.clone();
        let _failure = admission_lock.lock().await;
        if record.lock().await.admission_blocked {
            return false;
        }
        self.emit_final_child_usage(record).await;
        // The exit capture (the M4 seam): a child that dies without
        // reporting still said something — the live worker's last
        // assistant text when it is still reachable, else the durable
        // session file's last assistant row. It lands on the record only
        // when no answer was captured, so a positive verdict's capture
        // stands and the round trip never runs when one does; the
        // parent-side reader (the factory's provisional answer) sees
        // what the child last said instead of a bare failure row.
        let live_or_file = {
            let record = record.lock().await;
            if record.answer_captured && record.answer_preview.is_some() {
                None
            } else {
                Some((
                    record.active_session_id.clone(),
                    record.session_file.clone(),
                ))
            }
        };
        let exit_text = match live_or_file {
            Some((active_session_id, session_file)) => {
                let live = self
                    .child_answer_raw(&active_session_id)
                    .await
                    .ok()
                    .flatten();
                if live.is_some() {
                    live
                } else {
                    match session_file.filter(|path| !path.is_empty()) {
                        Some(path) => tokio::task::spawn_blocking(move || {
                            last_assistant_text_from_file(&path)
                        })
                        .await
                        .ok()
                        .flatten(),
                        None => None,
                    }
                }
            }
            None => None,
        };
        let message = {
            let mut record = record.lock().await;
            let keep_verdict = match arm {
                FailedArm::Unreachable => !should_mark_unreachable_error(&record),
                FailedArm::Prompt => {
                    record.closed_by_parent
                        || record.notice_delivered
                        || matches!(record.settled_status, Some("error" | "cancelled"))
                }
            };
            if keep_verdict {
                return false;
            }
            record.settled_status = Some("error");
            record.notice_delivered = true;
            if let Some(text) = exit_text {
                if !record.answer_captured || record.answer_preview.is_none() {
                    record.answer_preview = Some(compact_rlm_text(&text));
                    record.answer_text = Some(cap_text(&text, ANSWER_TEXT_MAX_CHARS));
                    record.answer_captured = true;
                }
            }
            let message = create_rlm_child_failure_message(
                &record.rlm_child_id,
                &record.session_name,
                &error,
                now_ms(),
            );
            record.error = Some(error);
            message
        };
        self.emit_child_update(record).await;
        // TS records a failed child's return too (`recordChildReturned` in
        // the thrown-run arm): a child that committed requests before
        // failing still returns them; a zero-commit child records nothing.
        self.record_child_return(record).await;
        self.deliver_terminal_notice(record, message).await;
        self.fire_settle_hook(record).await;
        true
    }

    /// Deliver one terminal notice into the parent session: the notice rides
    /// the supervisor's `follow_up` route as an injected custom turn (the
    /// row renders in the parent transcript and the turn runs on the
    /// notice content, the TS `followUp` notice action). The reserved
    /// custom kinds are daemon provenance, so the command carries the
    /// one-shot notice capability minted in this same worker process
    /// (`child_status_notices`): the parent's queue admission accepts a
    /// reserved-kind row exclusively with a live mint, and answers
    /// anything a caller sends — with or without a guessed nonce —
    /// loudly instead. A factory child's notice is dropped: the factory
    /// reads the settle through collect and posts its own milestones.
    pub(super) async fn deliver_terminal_notice(
        &self,
        record: &Arc<Mutex<ChildRecord>>,
        message: CustomMessage,
    ) -> Option<String> {
        let (factory, child_id) = {
            let record = record.lock().await;
            (record.factory, record.rlm_child_id.clone())
        };
        // Managed work still owes its owner the durable terminal receipt.
        let managed = self
            .gate
            .lock()
            .expect("gate lock")
            .work_bindings
            .iter()
            .any(|binding| binding.child_id == child_id);
        if factory && !managed {
            return None;
        }
        let Some(content) = custom_message_text(&message) else {
            eprintln!("pa-daemon: RLM child notice carried no text content");
            return None;
        };
        let mut wire = serde_json::to_value(pa_types::session::AgentMessage::Custom(message))
            .unwrap_or(Value::Null);
        let terminal_id = uuid::Uuid::new_v4().to_string();
        wire["details"]["terminalDeliveryId"] = json!(terminal_id);
        if let Some(child_id) = wire["details"]["childId"].as_str() {
            let mut gate = self.gate.lock().expect("gate lock");
            let mut next = gate.clone();
            if let Some(binding) = next
                .work_bindings
                .iter_mut()
                .find(|binding| binding.child_id == child_id)
            {
                binding.terminal_delivery = Some(terminal_id.clone());
                wire["details"]["nativeManagedResult"] =
                    json!(next
                        .policy
                        .as_ref()
                        .is_some_and(|policy| policy.managed_pass_policy.as_deref()
                            == Some("causal_results_v1")));
                if let Err(error) = self.persist_gate(&next) {
                    eprintln!("pa-daemon: terminal result intent persist failed: {error:#}");
                    return None;
                }
                *gate = next;
            }
        }
        let nonce = crate::child_status_notices::mint();
        let command = DaemonCommand::FollowUp {
            id: None,
            active_session_id: self.parent_active_session_id.clone(),
            message: content,
            input: PromptInput {
                content: None,
                images: None,
                streaming_behavior: None,
                queue_if_busy: None,
                expand_prompt_templates: None,
                source: None,
                agent_message_id: None,
                custom_message: Some(wire),
                queue_key: None,
                prefix_messages: None,
                admission_id: None,
                rlm_notice_nonce: Some(nonce),
            },
            rest: Map::default(),
        };
        match self.command(&command, NOTICE_DELIVERY_TIMEOUT_MS).await {
            Ok(receipt) => receipt["terminalDeliveryId"].as_str().map(str::to_string),
            Err(error) => {
                eprintln!("pa-daemon: RLM child terminal notice delivery ambiguous: {error:#}");
                None
            }
        }
    }
}

/// The prompt arm can reject a pre-admission idle misread; unreachable
/// residency churn never changes a real terminal verdict.
pub(super) enum FailedArm {
    Prompt,
    Unreachable,
}

/// Whether a child may still settle as an error (the failed task prompt's
/// or the unreachable give-up's claim): a parent-closed child, a noticed child, or an
/// ALREADY-SETTLED child keeps its POSITIVE verdict — its worker leaving
/// afterward (the idle passivation's graceful stop, a crash after
/// settle, or a give-up) is residency churn, not a settle verdict
/// change. The settle state is durable and the passive roster row
/// stays the representation.
pub(super) fn should_mark_unreachable_error(state: &ChildRecord) -> bool {
    !(state.admission_blocked
        || state.closed_by_parent
        || state.notice_delivered
        || state.settled_status.is_some())
}

/// The child's last assistant text from its durable session file (the exit
/// capture's fallback when the worker is already gone): the file is the
/// record a worker replacement replays from, so its last assistant row is
/// the child's final say even after a crash. The newest non-empty text row
/// wins; a file with no assistant rows (the prompt arm's never-started
/// turn) answers nothing.
pub(super) fn last_assistant_text_from_file(session_file: &str) -> Option<String> {
    let content = std::fs::read_to_string(session_file).ok()?;
    for line in content.lines().rev() {
        let Ok(pa_types::session::FileEntry::Message {
            message: pa_types::session::AgentMessage::Assistant(assistant),
            ..
        }) = serde_json::from_str::<pa_types::session::FileEntry>(line.trim())
        else {
            continue;
        };
        let text = assistant
            .content
            .iter()
            .filter_map(|block| match block {
                pa_types::ai::AssistantContentBlock::Text(text) => Some(text.text.clone()),
                _ => None,
            })
            .collect::<String>();
        if !text.is_empty() {
            return Some(text);
        }
    }
    None
}

/// The newest assistant verdict after the current admitted message. A retry's
/// later success replaces its earlier error; diagnostic/custom rows do not.
fn terminal_assistant_from_file(
    session_file: &str,
    expected_message_id: &str,
) -> Option<(&'static str, Option<String>)> {
    let content = std::fs::read_to_string(session_file).ok()?;
    let mut admitted = false;
    let mut terminal = None;
    for line in content.lines() {
        let Ok(entry) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if entry["id"].as_str() == Some(expected_message_id)
            || entry["details"]["id"].as_str() == Some(expected_message_id)
        {
            admitted = true;
            terminal = None;
        }
        if !admitted || entry["type"] != "message" || entry["message"]["role"] != "assistant" {
            continue;
        }
        let message = &entry["message"];
        let error = message["errorMessage"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        terminal = match message["stopReason"].as_str() {
            Some("error") => Some((
                "error",
                Some(error.unwrap_or_else(|| "Assistant response failed".into())),
            )),
            Some("aborted") => Some(("cancelled", error)),
            Some("stop" | "length") => Some(("done", None)),
            _ => None,
        };
    }
    terminal
}

fn final_turn_error_after_admission(
    session_file: &str,
    expected_message_id: &str,
) -> Option<String> {
    let content = std::fs::read_to_string(session_file).ok()?;
    let mut admitted = false;
    let mut suffix = String::new();
    for line in content.lines() {
        let entry = serde_json::from_str::<Value>(line).ok();
        if entry.as_ref().is_some_and(|entry| {
            entry["id"].as_str() == Some(expected_message_id)
                || entry["details"]["id"].as_str() == Some(expected_message_id)
        }) {
            admitted = true;
            suffix.clear();
        }
        if admitted {
            suffix.push_str(line);
            suffix.push('\n');
        }
    }
    if !admitted {
        return None;
    }
    final_turn_error_from_content(&suffix)
}

/// The error a child's final turn ended in, read from its durable session
/// file (#3366), scanning back from the newest row. A failed
/// `provider_retry_outcome` row (the retry episode's one durable outcome)
/// answers its text, and an assistant row that stopped on `error` answers
/// its error message. The scan answers `None` at the first other row that
/// bounds the final turn: any other message row (a successful or aborted
/// assistant reply, a tool result, a user prompt) or an `agent_message`
/// row (a task delivered after an older failure). A missing file answers
/// `None`. Non-message entries and other custom rows (notices, a recovered
/// retry's outcome) are skipped.
#[cfg(test)]
pub(super) fn final_turn_error_from_file(session_file: &str) -> Option<String> {
    let content = std::fs::read_to_string(session_file).ok()?;
    final_turn_error_from_content(&content)
}

fn final_turn_error_from_content(content: &str) -> Option<String> {
    use pa_types::session::{AgentMessage, FileEntry};
    for line in content.lines().rev() {
        let Ok(entry) = serde_json::from_str::<FileEntry>(line.trim()) else {
            continue;
        };
        match entry {
            FileEntry::CustomMessage { payload, .. } => {
                if payload.custom_type
                    == pa_core::session_engine::agent_messaging::AGENT_MESSAGE_CUSTOM_TYPE
                {
                    return None;
                }
                if payload.custom_type
                    != pa_core::session_engine::messages::PROVIDER_RETRY_OUTCOME_CUSTOM_TYPE
                {
                    continue;
                }
                let failed = payload
                    .details
                    .as_ref()
                    .and_then(|details| details.get("success"))
                    .and_then(Value::as_bool)
                    == Some(false);
                if !failed {
                    continue;
                }
                // The outcome row always carries the exhausted-retry text.
                if let pa_types::ai::UserContent::Text(text) = payload.content {
                    return Some(text);
                }
            }
            FileEntry::Message {
                message: AgentMessage::Assistant(assistant),
                ..
            } => {
                if assistant.stop_reason != pa_types::ai::StopReason::Error {
                    return None;
                }
                let error = assistant
                    .error_message
                    .filter(|message| !message.trim().is_empty())
                    .unwrap_or_else(|| "The final model call failed".to_string());
                return Some(format!("Error: {error}"));
            }
            FileEntry::Message { .. } => return None,
            FileEntry::Header { .. }
            | FileEntry::ToolStarted { .. }
            | FileEntry::ToolProgress { .. }
            | FileEntry::ThinkingLevelChange { .. }
            | FileEntry::ServiceTierChange { .. }
            | FileEntry::ModelChange { .. }
            | FileEntry::Compaction { .. }
            | FileEntry::BranchSummary { .. }
            | FileEntry::Custom { .. }
            | FileEntry::ChildUsageAttributed { .. }
            | FileEntry::Label { .. }
            | FileEntry::SessionInfo { .. }
            | FileEntry::SessionState { .. }
            | FileEntry::GitState { .. }
            | FileEntry::Unknown { .. } => {}
        }
    }
    None
}

/// The busy flags of one child `get_state` summary (streaming, its own
/// descendants, or queued work in flight).
fn child_busy_flags(state: &Value) -> bool {
    let flag = |name: &str| state.get(name).and_then(Value::as_bool).unwrap_or(false);
    flag("hasBackgroundBash")
        || flag("isStreaming")
        || flag("hasRunningSubagents")
        || state
            .get("sessionActions")
            .and_then(|actions| actions.get("queuedCount"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0
}

#[cfg(test)]
pub(super) mod exit_capture_tests {
    use super::*;

    #[test]
    fn terminal_reader_binds_current_admission_and_ignores_partial_tail() {
        let dir = tempfile::TempDir::new().unwrap();
        let file = dir.path().join("child.jsonl");
        std::fs::write(&file, concat!(
            "{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"error\"}}\n",
            "{\"type\":\"custom_message\",\"id\":\"current\"}\n",
            "{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"stopReason\":\"stop\"}}\n",
            "{partial"
        )).unwrap();
        assert_eq!(
            terminal_assistant_from_file(&file.to_string_lossy(), "current"),
            Some(("done", None))
        );
        assert_eq!(
            terminal_assistant_from_file(&file.to_string_lossy(), "missing"),
            None
        );
    }

    /// One serialized assistant row with the given text.
    fn assistant_row(text: &str) -> String {
        assistant_row_ending(text, pa_types::ai::StopReason::Stop, None)
    }

    /// One serialized assistant row with the given text, stop reason and
    /// error message (a failed model call's row).
    pub(crate) fn assistant_row_ending(
        text: &str,
        stop_reason: pa_types::ai::StopReason,
        error_message: Option<&str>,
    ) -> String {
        let message = pa_types::session::AgentMessage::Assistant(pa_types::ai::AssistantMessage {
            content: vec![pa_types::ai::AssistantContentBlock::Text(
                pa_types::ai::TextContent {
                    text: text.to_string(),
                    text_signature: None,
                    rest: Map::default(),
                },
            )],
            api: "faux".to_string(),
            provider: "faux".to_string(),
            model: "faux-1".to_string(),
            response_model: None,
            response_id: None,
            diagnostics: None,
            usage: pa_types::ai::Usage::default(),
            stop_reason,
            stop_reason_raw: None,
            error_message: error_message.map(str::to_string),
            timestamp: 2,
            rest: Map::default(),
        });
        let entry = pa_types::session::FileEntry::Message {
            message,
            base: pa_types::session::EntryBase {
                id: Some("row-1".to_string()),
                parent_id: None,
                timestamp: None,
                rest: Map::default(),
            },
        };
        serde_json::to_string(&entry).expect("serialize the assistant row")
    }

    /// The exit capture reads the child's last say from its durable file
    /// after the worker is gone: the newest non-empty assistant text wins.
    #[test]
    fn the_exit_capture_reader_takes_the_last_assistant_row() {
        let file = std::env::temp_dir().join(format!(
            "pa-rlm-exit-capture-{}.jsonl",
            uuid::Uuid::new_v4().simple()
        ));
        let user_row = r#"{"type":"message","role":"user","content":"the task"}"#;
        std::fs::write(
            &file,
            format!(
                "{user_row}\n{}\n{}\n",
                assistant_row("an earlier turn's say"),
                assistant_row("the final say")
            ),
        )
        .expect("write the session file");
        let text = last_assistant_text_from_file(&file.to_string_lossy())
            .expect("the last assistant row is readable");
        assert_eq!(text, "the final say");
        // A file with no assistant rows (the prompt arm's never-started
        // turn) answers nothing.
        let bare = std::env::temp_dir().join(format!(
            "pa-rlm-exit-capture-bare-{}.jsonl",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::write(&bare, format!("{user_row}\n")).expect("write the bare file");
        assert_eq!(last_assistant_text_from_file(&bare.to_string_lossy()), None);
        let _ = std::fs::remove_file(&file);
        let _ = std::fs::remove_file(&bare);
    }

    /// The retry episode's durable outcome row, as the worker persists it.
    pub(crate) fn retry_outcome_row(success: bool) -> String {
        let outcome = pa_core::session_engine::messages::create_provider_retry_outcome_message(
            success,
            3,
            "429 Provider returned error",
        );
        let pa_types::ai::UserContent::Text(content) = outcome.content else {
            unreachable!("the outcome row carries text content");
        };
        json!({
            "type": "custom_message",
            "id": "row-outcome",
            "parentId": null,
            "customType": outcome.custom_type,
            "content": content,
            "display": true,
            "details": outcome.details,
        })
        .to_string()
    }

    /// A stored row of `kind` (`message` or `custom_message`) with `fields`,
    /// in the shape the session file holds.
    fn stored_row(kind: &str, fields: &Value) -> String {
        let mut row = json!({"type": kind, "id": "row-0", "parentId": null});
        row.as_object_mut()
            .expect("row object")
            .extend(fields.as_object().expect("fields object").clone());
        row.to_string()
    }

    /// A user prompt row.
    fn user_row(text: &str) -> String {
        stored_row(
            "message",
            &json!({"message": {"role": "user", "content": text, "timestamp": 1}}),
        )
    }

    /// An `rlm.spawn` task row: an `agent_message` custom row.
    fn task_row() -> String {
        stored_row(
            "custom_message",
            &json!({
                "customType": pa_core::session_engine::agent_messaging::AGENT_MESSAGE_CUSTOM_TYPE,
                "content": "the task",
                "display": true,
                "details": {},
            }),
        )
    }

    fn final_turn_error_of(rows: &[String]) -> Option<String> {
        let file = std::env::temp_dir().join(format!(
            "pa-rlm-final-turn-{}.jsonl",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::write(&file, format!("{}\n{}\n", task_row(), rows.join("\n")))
            .expect("write the session file");
        let error = final_turn_error_from_file(&file.to_string_lossy());
        let _ = std::fs::remove_file(&file);
        error
    }

    #[test]
    fn retry_outcome_is_scoped_to_the_current_admission() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy();
        let old_failure = retry_outcome_row(false);
        let current = task_row();
        std::fs::write(file.path(), format!("{old_failure}\n{current}\n")).unwrap();
        assert_eq!(final_turn_error_after_admission(&path, "row-0"), None);
        std::fs::write(file.path(), format!("{current}\n{old_failure}\n")).unwrap();
        assert!(final_turn_error_after_admission(&path, "row-0").is_some());
        assert_eq!(final_turn_error_after_admission(&path, "missing"), None);
        std::fs::write(
            file.path(),
            format!("{}\n{}\n{}\n", current, old_failure, assistant_row("ok")),
        )
        .unwrap();
        assert_eq!(final_turn_error_after_admission(&path, "row-0"), None);
    }

    /// #3366: a run whose model calls all failed reads as failed, with the
    /// provider error text; a run whose last turn answered reads clean, and
    /// a failure older than a later turn-bounding row does not stand.
    #[test]
    fn the_final_turn_reader_reports_how_the_run_ended() {
        use pa_types::ai::StopReason;
        let failed_call =
            assistant_row_ending("", StopReason::Error, Some("429 Provider returned error"));
        let failed_run = [failed_call.clone(), retry_outcome_row(false)];
        let with_later = |later: String| {
            let mut rows = failed_run.to_vec();
            rows.push(later);
            final_turn_error_of(&rows)
        };
        assert_eq!(
            [
                final_turn_error_of(&failed_run),
                final_turn_error_of(std::slice::from_ref(&failed_call)),
                final_turn_error_of(&[assistant_row_ending("", StopReason::Aborted, None)]),
                // An earlier failure the run recovered from does not stand.
                final_turn_error_of(&[
                    failed_call.clone(),
                    retry_outcome_row(true),
                    assistant_row("the final say"),
                ]),
                final_turn_error_of(&[assistant_row("the final say"), retry_outcome_row(true)]),
                final_turn_error_of(&[]),
                // An older failure, then a later row that bounds the turn.
                with_later(user_row("try again")),
                with_later(task_row()),
                with_later(assistant_row("the final say")),
            ],
            [
                Some(
                    "\u{26a0} Error: Retry failed after 3 attempts: 429 Provider returned error"
                        .to_string()
                ),
                Some("Error: 429 Provider returned error".to_string()),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ]
        );
        assert_eq!(
            final_turn_error_from_file("/nonexistent/pa-rlm-final-turn.jsonl"),
            None
        );
    }
}

#[cfg(test)]
mod owner_cancel_tests {
    use super::*;
    #[tokio::test]
    async fn owner_cancel_settles_only_matching_admission_without_deadlock_or_credit() {
        let dir = tempfile::TempDir::new().unwrap();
        let sessions = super::super::SupervisorChildSessions::new(
            Arc::new(crate::supervisor_link::SupervisorLink::new(
                std::path::PathBuf::new(),
            )),
            dir.path().to_path_buf(),
            "parent".into(),
            Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                dir.path().to_path_buf(),
                true,
            )),
            String::new(),
        );
        sessions.set_identity(ParentIdentity {
            session_file: Some("parent-file".into()),
            ..ParentIdentity::with_default_depth()
        });
        let record = Arc::new(Mutex::new(ChildRecord {
            factory: false,

            rlm_child_id: "child".into(),
            session_name: "child".into(),
            active_session_id: "child-active".into(),
            session_id: None,
            session_dir: dir.path().to_string_lossy().into_owned(),
            model: String::new(),
            thinking_level: None,
            progress_note: None,
            label: String::new(),
            started_at_ms: 0,
            owner_cancelled: false,
            settled_status: None,
            settled: false,
            admission_blocked: true,
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
            session_file: Some("child-file".into()),
            attributed_rows: None,
            pending_usage_report: None,
            usage_watch_live: false,
            usage_rearm: false,
            emit_lock: Arc::new(Mutex::new(())),
            last_emitted_status: None,
            observation: None,
            rename_lock: Arc::new(Mutex::new(())),

            tool_use_count: None,

            live_observation: None,

            direct_observation: Arc::new(Mutex::new(())),
        }));
        sessions
            .inner
            .children
            .lock()
            .await
            .push(Arc::clone(&record));
        let fact = crate::journal::OwnerCancelFact {
            parent_session_file: "parent-file".into(),
            child_id: "child".into(),
            child_session_file: "child-file".into(),
            admission_generation: None,
            expected_message_id: None,
        };
        sessions.inner.refresh_running().await;
        assert!(*sessions.inner.running.borrow());
        tokio::time::timeout(
            Duration::from_secs(1),
            sessions.restore_owner_cancellations(std::slice::from_ref(&fact)),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!*sessions.inner.running.borrow());
        assert_eq!(record.lock().await.settled_status, Some("cancelled"));
        let rows = sessions.accounting_rows().await;
        assert_eq!(rows[0]["executionKind"], "idle");
        assert_eq!(rows[0]["obligation"], false);
        {
            let mut row = record.lock().await;
            row.owner_cancelled = false;
        }
        assert_eq!(
            sessions.accounting_rows().await[0]["executionKind"],
            "unknown"
        );
        sessions
            .restore_owner_cancellations(std::slice::from_ref(&fact))
            .await
            .unwrap();
        assert!(sessions
            .inner
            .gate
            .lock()
            .unwrap()
            .consumed_managed_results
            .is_empty());
        sessions
            .restore_owner_cancellations(std::slice::from_ref(&fact))
            .await
            .unwrap();
        {
            let mut row = record.lock().await;
            row.owner_cancelled = false;
            row.admission_generation = Some("new-generation".into());
            row.admission_blocked = true;
            row.settled = false;
            row.settled_status = None;
        }
        sessions
            .restore_owner_cancellations(std::slice::from_ref(&fact))
            .await
            .unwrap();
        assert!(record.lock().await.admission_blocked);
        assert!(record.lock().await.settled_status.is_none());
        // Exact legacy binding cancellation is separate from current incarnation matching.
        let parent = dir.path().join("parent.jsonl").display().to_string();
        sessions.set_identity(ParentIdentity {
            session_file: Some(parent.clone()),
            ..ParentIdentity::with_default_depth()
        });
        {
            let mut row = record.lock().await;
            row.admission_generation = Some("generation".into());
            row.expected_message_id = Some("message".into());
            row.owner_cancelled = false;
            row.settled = true; // Idle alone is not owner evidence.
        }
        let task = json!({"projectId":"p","planId":"plan","stepId":"step","taskGeneration":1,"policyRevision":1});
        let assignment = json!({"childId":"child","admissionGeneration":"generation","incarnation":"old-worker",
            "assignmentId":"assignment","taskGeneration":1,"ownership":"lane","acceptanceRef":"accept","artifactRef":"result","dependenciesSatisfied":true});
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            let mut value = serde_json::to_value(&*gate).unwrap();
            value["revision"] = json!(7);
            value["task"] = task.clone();
            value["policy"] = json!({"version":1,"enabled":true,"minActive":5,"maxActive":20,"rootWritesCode":false,"correctiveBudget":2});
            value["assignments"] = json!([assignment.clone()]);
            value["attempts"] = json!(2);
            value["suspended"] = json!(true);
            *gate = serde_json::from_value(value).unwrap();
        }
        sessions.retain_management_obligation("goal").unwrap();
        let mut request = json!({"expectedRevision":7,"task":task,"assignment":assignment});
        assert!(sessions
            .reconcile_owner_cancellation(&request)
            .await
            .is_err());
        request["fact"] = json!({"forged":true});
        assert!(sessions
            .reconcile_owner_cancellation(&request)
            .await
            .is_err());
        request.as_object_mut().unwrap().remove("fact");
        let exact_fact = crate::journal::OwnerCancelFact {
            parent_session_file: parent,
            admission_generation: Some("generation".into()),
            expected_message_id: Some("message".into()),
            ..fact.clone()
        };
        let mut wrong_fact = exact_fact.clone();
        wrong_fact.expected_message_id = Some("wrong".into());
        sessions
            .restore_owner_cancellations(&[wrong_fact])
            .await
            .unwrap();
        assert!(sessions
            .reconcile_owner_cancellation(&request)
            .await
            .is_err());
        let epoch = sessions
            .inner
            .accounting_epoch
            .load(std::sync::atomic::Ordering::SeqCst);
        sessions
            .restore_owner_cancellations(std::slice::from_ref(&exact_fact))
            .await
            .unwrap();
        assert!(
            sessions
                .inner
                .accounting_epoch
                .load(std::sync::atomic::Ordering::SeqCst)
                > epoch
        );
        let epoch = sessions
            .inner
            .accounting_epoch
            .load(std::sync::atomic::Ordering::SeqCst);
        sessions
            .restore_owner_cancellations(std::slice::from_ref(&exact_fact))
            .await
            .unwrap();
        assert_eq!(
            sessions
                .inner
                .accounting_epoch
                .load(std::sync::atomic::Ordering::SeqCst),
            epoch
        );
        for (field, value) in [
            ("incarnation", json!("other")),
            ("admissionGeneration", json!("other")),
            ("assignmentId", json!("other")),
        ] {
            let mut wrong = request.clone();
            wrong["assignment"][field] = value;
            assert!(sessions.reconcile_owner_cancellation(&wrong).await.is_err());
        }
        let mut wrong = request.clone();
        wrong["task"]["policyRevision"] = json!(2);
        assert!(sessions.reconcile_owner_cancellation(&wrong).await.is_err());
        let receipt = sessions
            .reconcile_owner_cancellation(&request)
            .await
            .unwrap();
        assert_eq!(receipt["kind"], "owner_cancel");
        assert_eq!(receipt["processExitObserved"], false);
        assert_eq!(receipt["revision"], 8);
        assert!(sessions
            .reconcile_owner_cancellation(&request)
            .await
            .is_err());
        request["expectedRevision"] = json!(8);
        assert_eq!(
            sessions
                .reconcile_owner_cancellation(&request)
                .await
                .unwrap(),
            receipt
        );
        let status = sessions.gate_status().await;
        assert_eq!(status["unknown"], 0);
        assert_eq!(status["knownActive"], 0);
        assert_eq!(status["decision"], "suspended");
        assert_eq!(status["correctiveAttempts"], 2);
        assert_eq!(status["managementObligation"]["goalId"], "goal");
        assert!(sessions
            .inner
            .gate
            .lock()
            .unwrap()
            .consumed_managed_results
            .is_empty());
        let bytes = std::fs::read(sessions.inner.gate_path().unwrap()).unwrap();
        sessions.restore_gate();
        assert_eq!(sessions.gate_status().await["unknown"], 1); // Receipt alone is not evidence.
        sessions
            .restore_owner_cancellations(&[exact_fact])
            .await
            .unwrap();
        let restored = sessions.gate_status().await;
        assert_eq!(restored["unknown"], 0);
        assert_eq!(
            restored["ownerCancellationReceipts"],
            status["ownerCancellationReceipts"]
        );
        assert_eq!(
            std::fs::read(sessions.inner.gate_path().unwrap()).unwrap(),
            bytes
        );
        record.lock().await.admission_generation = Some("changed".into());
        assert!(sessions
            .reconcile_owner_cancellation(&request)
            .await
            .is_err());
        assert!(sessions.gate_status().await["unknown"].as_u64().unwrap() > 0);
        // A legitimate task change through generic configure invalidates retained receipts.
        let mut reconfigure = json!({"expectedRevision":8,"task":request["task"].clone(),
            "policy":{"version":1,"enabled":true,"minActive":5,"maxActive":20,"rootWritesCode":false,"correctiveBudget":2},"assignments":[]});
        reconfigure["task"]["taskGeneration"] = json!(2);
        sessions.configure_gate(&reconfigure).await.unwrap();
        assert_eq!(
            sessions.gate_status().await["ownerCancellationReceipts"],
            json!([])
        );
    }
}
