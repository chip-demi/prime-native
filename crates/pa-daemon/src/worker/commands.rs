//! The dispatch surface: command routing, the command handlers,
//! and the abort family.
use super::{
    checkpoint_queue_recovery, clear_withdrawn_progress_note, enqueue_priority, json,
    persist_custom_row, response_failure, response_success, KillCloseReason, Lane, QueueCheckpoint,
    QueuePriority, QueuedItem, Result, SessionFile, TurnPolicy, TurnSettle, VecDeque, Worker,
    PROMPT_ABORTED_BEFORE_DELIVERY, SIDE_QUESTION_SETTLE_TIMEOUT,
};

use serde_json::Value;

use crate::protocol::DaemonResponse;
use crate::session_store::SessionMessagesQuery;

impl Worker {
    pub(crate) async fn dispatch_scoped(
        &self,
        command: &str,
        payload: &Value,
        grant: &pa_types::daemon::DaemonWorkerPeerGrant,
    ) -> DaemonResponse {
        let _admission = if matches!(
            command,
            "control_goal"
                | "edit_goal_objective"
                | "heartbeat_update"
                | "steer"
                | "follow_up"
                | "mutate_queued_message"
        ) {
            Some(self.control_admission.read().await)
        } else {
            None
        };
        if !self.peer_grants.scope_current(grant, &self.grant_context())
            || !crate::peer::peer_command_allowed(command, payload, grant)
        {
            return response_failure(None, command, "execution_authority_unavailable", None);
        }
        if matches!(command, "steer" | "follow_up" | "mutate_queued_message") {
            let path = self
                .core
                .lock()
                .unwrap()
                .store
                .as_ref()
                .map(|store| store.path.clone());
            if let Some(path) = path {
                if let Err(error) =
                    crate::lease::assert_session_not_quarantined(&self.config.agent_dir, &path)
                {
                    return response_failure(None, command, &format!("{error:#}"), None);
                }
            }
        }
        match command {
            "steer" => return self.handle_queue_scoped(payload, Lane::Steering, true),
            "follow_up" => return self.handle_queue_scoped(payload, Lane::FollowUp, true),
            "mutate_queued_message" => {
                return self.handle_mutate_queued_message_scoped(payload, true)
            }
            _ => {}
        }
        if command == "control_goal" {
            return self.handle_atomic_goal_stop(payload, grant).await;
        }
        self.dispatch(command, payload).await
    }

    pub(crate) async fn dispatch(&self, command_type: &str, payload: &Value) -> DaemonResponse {
        if command_type == "private_owner_bootstrap" {
            return self.handle_private_owner(payload).await;
        }
        if command_type == "configure_orchestration_gate"
            && payload.get("acceptLegacyClosure").is_some()
        {
            let result = async {
                anyhow::ensure!(
                    payload
                        .as_object()
                        .is_some_and(|o| o.keys().all(|k| matches!(
                            k.as_str(),
                            "acceptLegacyClosure" | "type" | "id" | "activeSessionId" | "clientId"
                        ))),
                    "Unexpected legacy closure fields"
                );
                let children = self
                    .agent_engine
                    .as_ref()
                    .and_then(|e| e.children.as_ref())
                    .ok_or_else(|| anyhow::anyhow!("Orchestration children unavailable"))?;
                children
                    .accept_legacy_closure(&payload["acceptLegacyClosure"])
                    .await
            }
            .await;
            return match result {
                Ok(value) => response_success(None, command_type, Some(value)),
                Err(error) => response_failure(None, command_type, &format!("{error:#}"), None),
            };
        }
        if command_type == "configure_orchestration_gate"
            && payload.get("quarantineSessions").is_some()
        {
            let result = async {
                anyhow::ensure!(
                    payload
                        .as_object()
                        .is_some_and(|object| object.keys().all(|key| matches!(
                            key.as_str(),
                            "quarantineSessions" | "type" | "id" | "activeSessionId" | "clientId"
                        ))),
                    "Unexpected quarantine fields"
                );
                let children = self
                    .agent_engine
                    .as_ref()
                    .and_then(|engine| engine.children.as_ref())
                    .ok_or_else(|| anyhow::anyhow!("Orchestration children unavailable"))?;
                children
                    .quarantine_sessions(&payload["quarantineSessions"])
                    .await
            }
            .await;
            return match result {
                Ok(value) => response_success(None, command_type, Some(value)),
                Err(error) => response_failure(None, command_type, &format!("{error:#}"), None),
            };
        }
        if !crate::lease::quarantine_command_is_control(command_type) {
            let path = self
                .core
                .lock()
                .unwrap()
                .store
                .as_ref()
                .map(|store| store.path.clone());
            if let Some(path) = path.filter(|path| !path.as_os_str().is_empty()) {
                if let Err(error) =
                    crate::lease::assert_session_not_quarantined(&self.config.agent_dir, &path)
                {
                    return response_failure(None, command_type, &format!("{error:#}"), None);
                }
            }
        }
        // Only operations that inspect or change historical branches need hydration.
        // Cancellation is deliberately excluded: it must reach the live turn immediately.
        let history_page = command_type == "get_messages"
            && transcript_page(payload).is_some_and(|page| page.reads_history());
        if history_page
            || matches!(
                command_type,
                "get_session_tree"
                    | "get_context_tree"
                    | "get_user_messages_for_forking"
                    | "set_session_entry_label"
                    | "navigate_tree"
                    | "fork"
                    | "export_html"
                    | "export_jsonl"
            )
        {
            if let Err(error) = self.hydrate_history().await {
                return response_failure(None, command_type, &error.to_string(), None);
            }
        }
        match command_type {
            "drone_snapshot" | "drone_recover" | "drone_status" => {
                self.handle_drone_control(command_type, payload)
            }
            "create" => self.handle_create(payload).await,
            "attach" => self.handle_attach(payload),
            "detach" => self.handle_detach(payload),
            "prompt" => self.handle_prompt(payload, false).await,
            "prompt_and_wait" => self.handle_prompt(payload, true).await,
            "steer" => self.handle_queue(payload, Lane::Steering),
            "follow_up" => self.handle_queue(payload, Lane::FollowUp),
            "abort" => self.handle_abort(),
            "abort_and_send_queued" => self.handle_abort_and_send_queued(),
            "start_side_question" => {
                if let Err(response) = self.require_created("start_side_question") {
                    return response;
                }
                self.side_questions.start(payload)
            }
            "abort_side_question" => {
                if let Err(response) = self.require_created("abort_side_question") {
                    return response;
                }
                self.side_questions.abort(payload)
            }
            "compact" => self.handle_compaction(payload).await,
            "abort_compaction" => {
                self.compaction.abort();
                response_success(None, "abort_compaction", None)
            }
            "set_auto_compaction" => self.handle_set_auto_compaction(payload),
            "wait_for_idle" => self.handle_wait_for_idle(payload).await,
            "wait_for_headless_completion" => {
                self.handle_wait_for_headless_completion(payload).await
            }
            "persist_owner_cancel" => {
                let result = (|| -> Result<Value> {
                    let fact: crate::journal::OwnerCancelFact =
                        serde_json::from_value(payload["fact"].clone())?;
                    let proof: crate::lease::LeaseProof =
                        serde_json::from_value(payload["leaseProof"].clone())?;
                    let parent = self
                        .core
                        .lock()
                        .unwrap()
                        .store
                        .as_ref()
                        .map(|store| store.path.clone())
                        .ok_or_else(|| anyhow::anyhow!("Parent session unavailable"))?;
                    anyhow::ensure!(
                        crate::lease::canonical_session_path(&parent)
                            == crate::lease::canonical_session_path(std::path::Path::new(
                                &fact.parent_session_file
                            )),
                        "Owner cancellation parent mismatch"
                    );
                    let mut recovery = self.recovery.lock().unwrap();
                    let journal = recovery
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("Parent recovery journal unavailable"))?;
                    crate::lease::with_lease_proof(
                        &self.config.agent_dir,
                        std::path::Path::new(&fact.child_session_file),
                        &proof,
                        || journal.record_owner_cancel(fact.clone()),
                    )?;
                    Ok(
                        json!({"ownerCancel":fact,"kind":"owner_cancel","processExitObserved":false}),
                    )
                })();
                match result {
                    Ok(value) => response_success(None, command_type, Some(value)),
                    Err(error) => response_failure(None, command_type, &format!("{error:#}"), None),
                }
            }
            "get_input_receipts" => self.handle_get_input_receipts(payload),
            "get_state" => self.handle_get_state(),
            "get_orchestration_gate" if payload.get("historicalOwnerCancellation").is_some() => {
                let result = async {
                    anyhow::ensure!(
                        payload.as_object().is_some_and(|object| object.keys().all(
                            |key| matches!(
                                key.as_str(),
                                "historicalOwnerCancellation"
                                    | "id"
                                    | "type"
                                    | "activeSessionId"
                                    | "clientId"
                            )
                        )),
                        "Unexpected historical lookup fields"
                    );
                    if let Err(response) = self.require_created(command_type) {
                        anyhow::bail!("{}", response.error.unwrap_or_default());
                    }
                    let children = self
                        .agent_engine
                        .as_ref()
                        .and_then(|engine| engine.children.as_ref())
                        .ok_or_else(|| anyhow::anyhow!("Orchestration children unavailable"))?;
                    let facts = self
                        .recovery
                        .lock()
                        .unwrap()
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Parent recovery journal unavailable"))?
                        .owner_cancel_facts()
                        .to_vec();
                    children
                        .historical_owner_cancellation(
                            &payload["historicalOwnerCancellation"],
                            &facts,
                        )
                        .await
                }
                .await;
                match result {
                    Ok(value) => response_success(None, command_type, Some(value)),
                    Err(error) => response_failure(None, command_type, &format!("{error:#}"), None),
                }
            }
            "configure_orchestration_gate" | "get_orchestration_gate" => {
                let facts = self
                    .recovery
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|journal| journal.owner_cancel_facts().to_vec())
                    .unwrap_or_default();
                if let Some(children) = self
                    .agent_engine
                    .as_ref()
                    .and_then(|engine| engine.children.as_ref())
                {
                    if let Err(error) = children.restore_owner_cancellations(&facts).await {
                        return response_failure(None, command_type, &format!("{error:#}"), None);
                    }
                }
                if let Err(response) = self.require_created(command_type) {
                    return response;
                }
                match self
                    .engine
                    .orchestration_gate((command_type == "configure_orchestration_gate").then(
                        || {
                            let mut configure = payload.clone();
                            if configure.get("ownerCancelChild").is_some()
                                || configure.get("reconcileOwnerCancellation").is_some()
                            {
                                if let Some(object) = configure.as_object_mut() {
                                    object.remove("id");
                                    object.remove("type");
                                    object.remove("activeSessionId");
                                    object.remove("clientId");
                                }
                            }
                            configure
                        },
                    ))
                    .await
                {
                    Ok(status) => response_success(None, command_type, Some(status)),
                    Err(error) => response_failure(None, command_type, &format!("{error:#}"), None),
                }
            }
            "edit_goal_objective" => self.handle_edit_goal_objective(payload).await,
            "control_goal" => self.handle_control_goal(payload).await,
            "get_messages" => self.handle_get_messages(payload),
            "get_session_messages" => self.handle_get_session_messages(payload).await,
            "get_session_header" => self.handle_get_session_header(),
            "get_session_stats" => self.handle_get_session_stats(),
            "get_model_catalog" => self.handle_get_model_catalog(),
            "get_queue" => self.handle_get_queue(),
            "clear_queue" => self.handle_clear_queue(),
            "abort_and_clear_queue" => self.handle_abort_and_clear_queue(),
            "get_last_assistant_text" => self.handle_get_last_assistant_text(),
            "get_connection_state" => self.handle_get_connection_state(),
            "get_mcp_connections" => self.handle_get_mcp_connections().await,
            "set_mcp_static_token" => self.handle_set_mcp_static_token(payload).await,
            "remove_mcp_connection" => self.handle_remove_mcp_connection(payload).await,
            "get_rlm_children" => {
                let facts = self
                    .recovery
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|journal| journal.owner_cancel_facts().to_vec())
                    .unwrap_or_default();
                if let Some(children) = self
                    .agent_engine
                    .as_ref()
                    .and_then(|engine| engine.children.as_ref())
                {
                    if let Err(error) = children.restore_owner_cancellations(&facts).await {
                        return response_failure(None, command_type, &format!("{error:#}"), None);
                    }
                }
                self.handle_get_rlm_children().await
            }
            "get_context_tree" => self.handle_get_context_tree().await,
            "get_commands" => self.handle_get_commands().await,
            "get_resource_snapshot" => self.handle_get_resource_snapshot().await,
            "get_session_context" => self.handle_get_session_context(),
            "get_system_prompt" => self.handle_get_system_prompt().await,
            "get_tool_definition" => self.handle_get_tool_definition(payload).await,
            "get_rlm_max_depth_status" => self.handle_get_rlm_max_depth_status(),
            "get_available_models" => self.handle_get_available_models(),
            "worker_deliver_message" => self.handle_worker_deliver_message(payload),
            "update_snapshot" => self.handle_update_snapshot(),
            "kill" => self.handle_kill(payload).await,
            "shutdown" if payload["updateRestart"] == true => self.handle_update_shutdown().await,
            "shutdown" => self.handle_shutdown(payload).await,
            "rename" => self.handle_rename("rename", payload),
            "set_session_name" => self.handle_rename("set_session_name", payload),
            "mark_anthropic_warning_shown" => self.handle_mark_anthropic_warning_shown(),
            "rename_saved_session" => self.handle_rename_saved_session(payload),
            "delete_saved_session" => self.handle_delete_saved_session(payload).await,
            "replace_acp_mcp_servers" => self.handle_replace_acp_mcp_servers(payload),
            "set_model" => self.handle_set_model(payload).await,
            "set_thinking_level" => self.handle_set_thinking_level(payload).await,
            "cycle_model" => self.handle_cycle_model(payload).await,
            "set_scoped_models" => self.handle_set_scoped_models(payload),
            "cycle_thinking_level" => self.handle_cycle_thinking_level().await,
            "set_service_tier" => self.handle_set_service_tier(payload).await,
            "set_transport" => self.handle_set_transport(payload),
            "set_steering_mode" => self.handle_set_queue_mode("set_steering_mode", payload),
            "set_follow_up_mode" => self.handle_set_queue_mode("set_follow_up_mode", payload),
            "set_auto_retry" => self.handle_set_auto_retry(payload),
            "abort_retry" => self.handle_abort_retry(),
            "get_session_tree" => self.tree_navigation.get_session_tree(),
            "get_user_messages_for_forking" => self.tree_navigation.get_user_messages_for_forking(),
            "set_session_entry_label" => self.tree_navigation.set_session_entry_label(payload),
            "navigate_tree" => self.handle_navigate_tree(payload).await,
            "fork" => self.handle_fork(payload).await,
            "abort_branch_summary" => {
                self.tree_navigation.abort();
                response_success(None, "abort_branch_summary", None)
            }
            "export_html" => self.exports.export_html(payload).await,
            "export_jsonl" => self.exports.export_jsonl(payload),
            "mutate_queued_message" => self.handle_mutate_queued_message(payload),
            "resume_queue" => self.handle_resume_queue(),
            "factory_activity" => self.handle_factory_activity(payload).await,
            "execute_bash" => self.handle_execute_bash(payload),
            "execute_bash_and_wait" => self.handle_execute_bash_and_wait(payload).await,
            "abort_bash" => self.handle_abort_bash(),
            "list_kernel_bash" | "tail_kernel_bash" | "kill_kernel_bash" => {
                self.handle_kernel_bash_activity(command_type, payload)
                    .await
            }
            "append_custom_message" => self.handle_append_custom_message(payload),
            "restore_next_turn" => self.handle_restore_next_turn(payload),
            "restore_actions" => self.handle_restore_actions(payload),
            "refine" => self.handle_refine(payload).await,
            "reload" => self.handle_reload().await,
            "cancel_rlm_child" => self.handle_cancel_rlm_child(payload).await,
            "delete_rlm_subagent" => self.handle_delete_rlm_subagent(payload).await,
            // The engine call blocks on the engine runtime, so it
            // runs on a blocking thread like every other engine call.
            "set_rlm_max_depth" => self.handle_set_rlm_max_depth(payload).await,
            "acquire_session_input_pause" => self.handle_acquire_session_input_pause(payload),
            "release_session_input_pause" => self.handle_release_session_input_pause(payload),
            "cancel_prompt_admission" => self.handle_cancel_prompt_admission(payload),
            "new_session" => self.handle_new_session(payload).await,
            "switch_session" => self.handle_switch_session(payload).await,
            "import_jsonl" => self.handle_import_jsonl(payload).await,
            "agent_messages_status" => self.handle_agent_messages_status(),
            "agent_messages_pause" => self.handle_agent_messages_pause(),
            "agent_messages_resume" => self.handle_agent_messages_resume(),
            "agent_messages_clear" => self.handle_agent_messages_clear(),
            "cron_list" => self.handle_cron_list(payload),
            "heartbeats_list" => self.handle_heartbeats_list(),
            "heartbeat_manage" => self.handle_heartbeat_manage(payload).await,
            "cron_add" => self.handle_cron_add(payload).await,
            "cron_cancel" => self.handle_cron_cancel(payload).await,
            "heartbeat_get" => self.handle_heartbeat_get(payload),
            "heartbeat_set" => self.handle_heartbeat_set(payload).await,
            "heartbeat_update" => self.handle_heartbeat_update(payload).await,
            other => response_failure(
                None,
                command_type,
                &format!("Unknown worker command: {other}"),
                None,
            ),
        }
    }

    // DaemonResponse is the wire response struct and is deliberately wide; the
    // error channel here carries the whole response, so allow the large-err lint.
    #[allow(clippy::result_large_err)]
    pub(crate) fn require_created(&self, command_type: &str) -> Result<(), DaemonResponse> {
        let core = self.core.lock().unwrap();
        if !core.created {
            return Err(response_failure(
                None,
                command_type,
                "Session is still initializing",
                None,
            ));
        }
        // The shutdown admission gate: a command dispatched while the graceful stop
        // is closing must not start new work the exit would orphan (an execute_bash
        // racing the shutdown would spawn a child the exit leaves running).
        if core.shutdown_requested {
            return Err(response_failure(
                None,
                command_type,
                "Session is shutting down",
                None,
            ));
        }
        Ok(())
    }

    /// `navigate_tree` with the reload's announcement: announced before the
    /// response reaches the client, so surfaces never show the pre-navigation goal.
    /// The whole navigation runs under the queued-work pause (TS
    /// `_navigateTree`'s `acquireQueuedWorkPause` + `waitForIdle`).
    async fn handle_navigate_tree(&self, payload: &Value) -> DaemonResponse {
        let _queued_work = self.hold_queued_work_pause("navigate");
        let response = self.tree_navigation.navigate_tree(payload).await;
        if response.success {
            if let Some(goal) = self.engine.goal_update_after_rebuild() {
                self.emit_worker_event(json!({
                    "type": "goal_update",
                    "goal": goal,
                }));
            }
        }
        response
    }

    /// The session's owner-fenced ACP MCP store: the ACP transport resolves and
    /// validates the servers before sending them; the worker fences ownership,
    /// guards the busy turn, and rolls back a failed replacement.
    fn handle_replace_acp_mcp_servers(&self, payload: &Value) -> DaemonResponse {
        let owner_id = payload
            .get("ownerId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if owner_id.is_empty() {
            return response_failure(
                None,
                "replace_acp_mcp_servers",
                "ACP MCP owner id is required",
                None,
            );
        }
        let servers: Vec<pa_core::mcp::AcpMcpServerConfig> = payload
            .get("servers")
            .cloned()
            .map(|servers| serde_json::from_value(servers).unwrap_or_default())
            .unwrap_or_default();
        // The agent cannot adopt a different MCP tool list mid-turn (TS
        // `session.isStreaming` guard).
        if !servers.is_empty() && self.core.lock().unwrap().busy {
            return response_failure(
                None,
                "replace_acp_mcp_servers",
                "Cannot replace ACP MCP servers while the agent is running",
                None,
            );
        }
        // The real agent engine owns the session's MCP store (one store for admission
        // and prompt gating); scripted harness engines fall back to the worker-level store.
        let manager = self
            .engine
            .acp_mcp_manager()
            .unwrap_or_else(|| std::sync::Arc::clone(&self.acp_mcp));
        let manager = manager.lock().unwrap();
        match manager.replace_acp_servers(&servers, owner_id) {
            // An unchanged list (same owner, identical servers) is a
            // no-op success.
            Ok(_) => response_success(None, "replace_acp_mcp_servers", None),
            Err(error) => {
                // Roll back any partially applied configuration with
                // the owner-scoped clear before surfacing the failure.
                if manager.can_release_acp_servers(owner_id) {
                    let _ = manager.replace_acp_servers(&[], owner_id);
                }
                response_failure(None, "replace_acp_mcp_servers", &error.to_string(), None)
            }
        }
    }

    /// Arm the one-shot forced steering batch: the visible plain-user steering
    /// items deliver as ONE batched turn at the next boundary, even under queue
    /// mode "one-at-a-time". Returns whether anything armed.
    pub(crate) fn arm_forced_all_steering(&self) -> bool {
        let mut core = self.core.lock().unwrap();
        let armable = |item: &QueuedItem| {
            item.queue_visible && item.agent_message.is_none() && item.custom_message.is_none()
        };
        if !core.steering.iter().any(armable) {
            return false;
        }
        core.forced_all_steering = true;
        for item in &mut core.steering {
            if armable(item) {
                item.forced_batch = true;
            }
        }
        true
    }

    /// `abort_and_send_queued` (schema 29): abort the active run, keep the queue
    /// flowing — the armed steering rows co-deliver, then the follow-up drains.
    ///
    /// SANCTIONED DIVERGENCE (operator ruling 2026-09-25): TS parks the queue
    /// whenever nothing is armable; here the abort resumes. The same divergence
    /// covers a held admission pause: the abort clears the suspension and the
    /// pump's own pause gate holds the delivery until the release re-notifies
    /// it. Returns whether the queue was resumed.
    pub(crate) fn abort_and_send_queued(&self) -> bool {
        // TS `canResume` also parks under an admission pause; the pump's own
        // pause gate is the one hold that matters, so only a shutting-down
        // worker (whose queue survives via the restart journal) stays parked.
        let can_resume = !self.core.lock().unwrap().shutdown_requested;
        if !can_resume {
            self.request_abort();
            return false;
        }
        self.arm_forced_all_steering();
        // The cancel sweep keeps only the queue-visible rows, so the emptiness
        // read below measures exactly the work the abort leaves behind.
        self.request_abort();
        // The resumed pump — not this funnel — owns the delivery at
        // the settled turn's boundary.
        let queued_work = {
            let core = self.core.lock().unwrap();
            !core.steering.is_empty() || !core.follow_up.is_empty()
        };
        if !queued_work {
            return false;
        }
        self.resume_queued_input();
        true
    }

    /// The abort funnel behind both abort commands: suspend queued-input
    /// admission, cancel the queue-invisible turn actions, abort the in-flight
    /// compaction, and cancel the running turn.
    fn request_abort(&self) {
        {
            let mut core = self.core.lock().unwrap();
            core.abort_requested = true;
            core.turn_cancelled = true;
            // The queue parks and a plain prompt is rejected until a
            // resume site fires.
            core.queued_input_suspended = true;
        }
        // Queue-INVISIBLE turn actions cancel: a direct prompt admitted on an idle
        // session never became a queue row, so the abort must resolve its waiting
        // response, not park it behind the suspension forever. Queue-visible lanes
        // survive parked — the suspension defers the pump, it never drops the queue.
        {
            let mut core = self.core.lock().unwrap();
            let cancel = |lane: &mut VecDeque<QueuedItem>| {
                let mut kept = VecDeque::new();
                while let Some(item) = lane.pop_front() {
                    if item.queue_visible {
                        kept.push_back(item);
                    } else {
                        if let Some(id) = &item.admission_id {
                            // A withdrawn prompt clears its admission (TS clearAdmission).
                            self.prompt_admissions.clear(id);
                        }
                        if let Some(done) = item.done {
                            let _ = done.send(TurnSettle::Withdrawn(
                                PROMPT_ABORTED_BEFORE_DELIVERY.to_string(),
                            ));
                        }
                    }
                }
                *lane = kept;
            };
            cancel(&mut core.steering);
            cancel(&mut core.follow_up);
            clear_withdrawn_progress_note(&core, self.engine.as_ref());
        }
        // A withdrawn continuation's pending guard already released at ITS admission,
        // so the withdraw clears nothing (a mirror clear could drop an unrelated guard).
        self.compaction.abort();
        // The in-flight turn's fetch cancels now, not at its next
        // streamed event.
        self.engine.abort_in_flight_turn();
        // An idle withdrawal has no turn-end event to refresh the roster.
        self.push_roster_delta();
        // An idle headless goal may be waiting behind background Bash or a
        // backoff wake; no turn boundary will signal its abort.
        self.idle_notify.notify_waiters();
    }

    fn handle_abort(&self) -> DaemonResponse {
        self.request_abort();
        response_success(None, "abort", None)
    }

    fn handle_abort_and_send_queued(&self) -> DaemonResponse {
        // The response acknowledges the abort itself, never the
        // async deliveries the resumed pump runs at the settle.
        self.abort_and_send_queued();
        response_success(None, "abort_and_send_queued", None)
    }

    fn handle_get_state(&self) -> DaemonResponse {
        if let Err(response) = self.require_created("get_state") {
            return response;
        }
        let message_facts = self
            .recovery
            .lock()
            .unwrap()
            .as_ref()
            .map(|journal| {
                journal
                    .message_facts(&self.core.lock().unwrap().active_session_id)
                    .iter()
                    .map(|fact| serde_json::to_value(fact).unwrap_or(Value::Null))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let owner_cancel_facts = self
            .recovery
            .lock()
            .unwrap()
            .as_ref()
            .map_or_else(|| json!([]), |journal| json!(journal.owner_cancel_facts()));
        let (core, inputs) = self.summary_inputs();
        let summary = self.summary_locked(&core, inputs);
        response_success(
            None,
            "get_state",
            Some({
                let mut value = serde_json::to_value(&summary).unwrap_or(Value::Null);
                value["rootRetaskCompletionVersion"] = json!(1);
                value["rootRetaskInputFence"] = super::input::root_retask_input_fence(&core);
                value["rootRetaskGoalFence"] =
                    super::input::root_retask_goal_fence(self.engine.as_ref());
                value["messageLifecycleFacts"] = json!(message_facts);
                value["ownerCancelFacts"] = owner_cancel_facts;
                value["ownExecutionStarted"] = json!(
                    core.busy
                        && core
                            .active_action
                            .as_ref()
                            .is_some_and(|action| action.phase == "running")
                );
                value["runningMessageIds"] = json!(core
                    .running_admission_ids
                    .iter()
                    .filter_map(|id| id.strip_prefix("message:"))
                    .collect::<Vec<_>>());
                value
            }),
        )
    }

    async fn handle_edit_goal_objective(&self, payload: &Value) -> DaemonResponse {
        const COMMAND: &str = "edit_goal_objective";
        // Connection tasks dispatch concurrently. Keep CAS, the worker-owned
        // append, and publication in one mutation lane, including no-ops.
        let _edit_gate = self.replacement_gate.lock().await;
        if let Err(response) = self.require_created(COMMAND) {
            return response;
        }
        if self
            .goal_edit_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return response_failure(
                None,
                COMMAND,
                "Goal objective persistence is uncertain; close and reopen this session before editing.",
                None,
            );
        }
        {
            let core = self.core.lock().unwrap();
            if core.compacting {
                return response_failure(
                    None,
                    COMMAND,
                    "Goal objective cannot be edited during compaction.",
                    None,
                );
            }
        }
        let (Some(goal_id), Some(expected_objective), Some(objective)) = (
            payload
                .get("goalId")
                .or_else(|| payload.get("expectedGoalId"))
                .and_then(Value::as_str),
            payload.get("expectedObjective").and_then(Value::as_str),
            payload.get("objective").and_then(Value::as_str),
        ) else {
            return response_failure(
                None,
                COMMAND,
                "Goal identity, expected objective, and objective must be strings.",
                None,
            );
        };
        let changed = match self
            .engine
            .edit_goal_objective(goal_id, expected_objective, objective)
            .await
        {
            Ok(changed) => changed,
            Err(error) => return response_failure(None, COMMAND, &format!("{error:#}"), None),
        };
        let Some(goal) = changed else {
            return response_success(
                None,
                COMMAND,
                Some(goal_control_response(
                    payload,
                    self.engine.goal_state_value(),
                )),
            );
        };
        // The core session manager is intentionally in-memory. The worker's
        // leased SessionFile is the only durable owner of this JSONL, just as
        // it is for goal updates emitted by model turns. Persist before the
        // edit is announced or acknowledged so a fresh owned create sees it.
        let (persisted, goal) = {
            let mut core = self.core.lock().unwrap();
            // A model turn may advance counters after the CAS and before this
            // append. Read the actual driver while holding the store lane so
            // that an earlier turn row cannot be followed by stale counters.
            // If the driver is mid-mutation, that mutation's goal event will
            // follow this row through the same store lock.
            let latest = self
                .agent_engine
                .as_ref()
                .and_then(|engine| engine.current_goal_state())
                .unwrap_or(goal);
            let persisted = core.store.as_mut().map(|store| {
                store.persist_entry(
                    "custom",
                    json!({
                        "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
                        "data": &latest,
                    }),
                )
            });
            if persisted.as_ref().is_some_and(Result::is_ok) {
                // Publish before releasing the store lock: a delayed model
                // event then sees this objective as the latest baseline.
                self.engine.mark_goal_edit_published(&latest);
            }
            (persisted, latest)
        };
        match persisted {
            Some(Ok(_)) => {}
            Some(Err(error)) => {
                self.goal_edit_dirty
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return response_failure(
                    None,
                    COMMAND,
                    &format!(
                        "Goal objective changed in memory but could not be persisted: {error:#}"
                    ),
                    None,
                );
            }
            None => {
                self.goal_edit_dirty
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return response_failure(
                    None,
                    COMMAND,
                    "Goal objective changed in memory but this thread has no durable session file.",
                    None,
                );
            }
        }
        self.emit_worker_event(json!({ "type": "goal_update", "goal": goal }));
        if goal.status == pa_core::goals::GoalStatus::Active {
            let was_busy = self.core.lock().unwrap().busy;
            if was_busy {
                self.engine.purge_queued_goal_contexts();
                if let Ok(message) = pa_core::goals::create_goal_context_message(
                    &goal,
                    pa_core::goals::GoalContextKind::ObjectiveUpdated,
                    &[],
                ) {
                    let mut core = self.core.lock().unwrap();
                    enqueue_priority(
                        &mut core.follow_up,
                        QueuedItem {
                            priority: QueuePriority::Pinned,
                            preview: None,
                            message: message.content.text(),
                            custom_message: Some(crate::session_commands::custom_message_value(
                                &message,
                            )),
                            agent_message: None,
                            input_identity: None,
                            queue_key: None,
                            admission_id: None,
                            images: Vec::new(),
                            done: None,
                            queue_visible: false,
                            policy: TurnPolicy::Injected,
                            forced_batch: false,
                        },
                    );
                    drop(core);
                    checkpoint_queue_recovery(
                        &self.recovery,
                        &self.core,
                        QueueCheckpoint::Admitted {
                            operation: "follow_up_queued",
                        },
                    );
                    self.work_notify.notify_one();
                }
            }
        }
        response_success(
            None,
            COMMAND,
            Some(goal_control_response(payload, json!(goal))),
        )
    }

    async fn handle_atomic_goal_stop(
        &self,
        payload: &Value,
        grant: &pa_types::daemon::DaemonWorkerPeerGrant,
    ) -> DaemonResponse {
        use pa_core::session_engine::goal_driver::GoalControlAction;
        const COMMAND: &str = "control_goal";
        let _mutation = self.replacement_gate.lock().await;
        if !self.peer_grants.scope_current(grant, &self.grant_context()) {
            return response_failure(None, COMMAND, "execution_authority_unavailable", None);
        }
        if let Err(response) = self.require_created(COMMAND) {
            return response;
        }
        if self
            .goal_edit_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return response_failure(
                None,
                COMMAND,
                "UNKNOWN: goal persistence uncertain; reopen session",
                None,
            );
        }
        if payload.get("controlVersion").and_then(Value::as_u64) != Some(1) {
            return response_failure(
                None,
                COMMAND,
                "atomic_goal_stop_unsupported_operation",
                None,
            );
        }
        let action = match payload.get("action").and_then(Value::as_str) {
            Some("pause_and_stop") => GoalControlAction::Pause,
            Some("clear_and_stop") => GoalControlAction::Clear,
            _ => {
                return response_failure(
                    None,
                    COMMAND,
                    "atomic_goal_stop_unsupported_operation",
                    None,
                );
            }
        };
        let (Some(id), Some(objective)) = (
            payload
                .get("goalId")
                .or_else(|| payload.get("expectedGoalId"))
                .and_then(Value::as_str),
            payload.get("expectedObjective").and_then(Value::as_str),
        ) else {
            return response_failure(
                None,
                COMMAND,
                "Goal identity and expected objective required",
                None,
            );
        };
        let (serial, generation) = {
            let core = self.core.lock().unwrap();
            if let Some(store) = core.store.as_ref() {
                if let Err(error) = crate::lease::assert_session_not_quarantined(
                    &self.config.agent_dir,
                    &store.path,
                ) {
                    return response_failure(None, COMMAND, &format!("{error:#}"), None);
                }
            }
            if core.compacting || core.shutdown_requested {
                return response_failure(
                    None,
                    COMMAND,
                    "Session cannot stop goal during compaction or shutdown",
                    None,
                );
            }
            (core.turn_serial, core.generation.clone())
        };
        let mut commit = |goal: &pa_core::goals::GoalState| -> Result<()> {
            // Issuer changes require control_admission write; the caller holds its read permit.
            anyhow::ensure!(
                self.peer_grants.scope_current(grant, &self.grant_context()),
                "execution_authority_unavailable"
            );
            let mut recovery = self.recovery.lock().unwrap();
            let mut core = self.core.lock().unwrap();
            anyhow::ensure!(
                core.turn_serial == serial
                    && core.generation == generation
                    && !core.shutdown_requested
                    && !core.compacting,
                "Captured turn changed; replacement not cancelled"
            );
            let store = core
                .store
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Session store unavailable"))?;
            anyhow::ensure!(
                !store.path.as_os_str().is_empty(),
                "Durable session file required"
            );
            store.persist_entry(
                "custom",
                json!({"customType":pa_core::goals::GOAL_STATE_CUSTOM_TYPE,"data":goal}),
            )?;
            let journal = recovery
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Durable queue journal unavailable"))?;
            journal.record_queue_checkpoint(
                &core.active_session_id,
                core.store.as_ref().unwrap().session_id(),
                core.store
                    .as_ref()
                    .map(|store| store.path.to_string_lossy().to_string())
                    .as_deref(),
                core.busy,
                "atomic_goal_stop",
                &[],
                &[],
                core.queued_input_suspended,
            )?;
            self.engine.mark_goal_edit_published(goal);
            let mut withdrawn: Vec<_> = core.steering.drain(..).collect();
            withdrawn.extend(core.follow_up.drain(..));
            for mut item in withdrawn {
                if let Some(id) = &item.admission_id {
                    self.prompt_admissions.clear(id);
                }
                if let Some(done) = item.done.take() {
                    let _ = done.send(TurnSettle::Withdrawn(PROMPT_ABORTED_BEFORE_DELIVERY.into()));
                }
            }
            core.pending_next_turn.clear();
            core.forced_all_steering = false;
            core.queued_input_suspended = true;
            core.abort_requested = true;
            self.engine.abort_in_flight_turn();
            Ok(())
        };
        let goal = match self
            .engine
            .atomic_goal_stop(id, objective, action, &mut commit)
            .await
        {
            Ok(goal) => goal,
            Err(error) => {
                let message = format!("{error:#}");
                if message.starts_with("UNKNOWN:") {
                    self.goal_edit_dirty
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                return response_failure(None, COMMAND, &message, None);
            }
        };
        if let Some(engine) = &self.agent_engine {
            engine.cancel_goal_backoff_wake();
        }
        self.emit_worker_event(json!({"type":"goal_update","goal":goal}));
        self.idle_notify.notify_waiters();
        response_success(
            None,
            COMMAND,
            Some(json!({"controlVersion":1,"cancellationRequested":true,
            "goal":if action == GoalControlAction::Clear { Value::Null } else { json!(goal) }})),
        )
    }

    async fn handle_control_goal(&self, payload: &Value) -> DaemonResponse {
        use pa_core::session_engine::goal_driver::GoalControlAction;

        const COMMAND: &str = "control_goal";
        // Serializes the CAS, durable append, and announcement against edits
        // and other lifecycle requests arriving on separate connections.
        let _control_gate = self.replacement_gate.lock().await;
        if let Err(response) = self.require_created(COMMAND) {
            return response;
        }
        if self
            .goal_edit_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return response_failure(
                None,
                COMMAND,
                "Goal persistence is uncertain; close and reopen this session before controlling it.",
                None,
            );
        }
        if self.core.lock().unwrap().compacting {
            return response_failure(
                None,
                COMMAND,
                "Goal cannot be controlled during compaction.",
                None,
            );
        }
        let (Some(goal_id), Some(expected_objective), Some(action)) = (
            payload
                .get("goalId")
                .or_else(|| payload.get("expectedGoalId"))
                .and_then(Value::as_str),
            payload.get("expectedObjective").and_then(Value::as_str),
            payload.get("action").and_then(Value::as_str),
        ) else {
            return response_failure(
                None,
                COMMAND,
                "Goal identity, expected objective, and action must be strings.",
                None,
            );
        };
        let action = match action {
            "pause" => GoalControlAction::Pause,
            "resume" => GoalControlAction::Resume,
            "clear" => GoalControlAction::Clear,
            _ => {
                return response_failure(None, COMMAND, "Unknown goal control action.", None);
            }
        };
        let changed = match self
            .engine
            .control_goal(goal_id, expected_objective, action)
            .await
        {
            Ok(changed) => changed,
            Err(error) => return response_failure(None, COMMAND, &format!("{error:#}"), None),
        };
        let (persisted, goal) = {
            let mut core = self.core.lock().unwrap();
            let goal = self
                .agent_engine
                .as_ref()
                .and_then(|engine| engine.current_goal_state())
                .unwrap_or_else(|| {
                    changed
                        .clone()
                        .unwrap_or_else(pa_core::goals::empty_goal_state)
                });
            let persisted = core.store.as_mut().map(|store| {
                store.persist_entry(
                    "custom",
                    json!({
                        "customType": pa_core::goals::GOAL_STATE_CUSTOM_TYPE,
                        "data": &goal,
                    }),
                )
            });
            if persisted.as_ref().is_some_and(Result::is_ok) {
                self.engine.mark_goal_edit_published(&goal);
            }
            (persisted, goal)
        };
        match persisted {
            Some(Ok(_)) => {}
            Some(Err(error)) => {
                self.goal_edit_dirty
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return response_failure(
                    None,
                    COMMAND,
                    &format!("Goal changed in memory but could not be persisted: {error:#}"),
                    None,
                );
            }
            None => {
                self.goal_edit_dirty
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return response_failure(
                    None,
                    COMMAND,
                    "Goal changed in memory but this thread has no durable session file.",
                    None,
                );
            }
        }
        if let Some(engine) = &self.agent_engine {
            engine.cancel_goal_backoff_wake();
        }
        self.engine.purge_queued_goal_contexts();
        self.emit_worker_event(json!({ "type": "goal_update", "goal": goal }));
        if action == GoalControlAction::Resume {
            // Resume reaches admission only after the durable append and publication.
            self.resume_queued_input();
            if let Some(engine) = &self.agent_engine {
                let handles = engine
                    .goal_runtime
                    .lock()
                    .expect("goal runtime lock")
                    .clone();
                if let Some(handles) = handles {
                    let mut driver = handles.driver.lock().await;
                    driver.continuation_consumed();
                    if driver.state().status == pa_core::goals::GoalStatus::Active {
                        driver.mark_continuation_owed();
                    }
                }
                engine.retry_owed_goal_continuation();
            }
        }
        let result = changed.map_or(Value::Null, |_| json!(goal));
        response_success(None, COMMAND, Some(goal_control_response(payload, result)))
    }

    /// `get_session_header`: the persisted session header line (TS wraps it
    /// in `{ header: ... }`).
    fn handle_get_session_header(&self) -> DaemonResponse {
        if let Err(response) = self.require_created("get_session_header") {
            return response;
        }
        let core = self.core.lock().unwrap();
        let Some(store) = core.store.as_ref() else {
            return response_failure(
                None,
                "get_session_header",
                "Session is still initializing",
                None,
            );
        };
        response_success(
            None,
            "get_session_header",
            Some(json!({ "header": crate::session_store::session_header_line(&store.header) })),
        )
    }

    /// `get_session_stats`: counts, token totals, and the context-usage
    /// estimate over the persisted branch (TS `getSessionStats`).
    fn handle_get_session_stats(&self) -> DaemonResponse {
        if let Err(response) = self.require_created("get_session_stats") {
            return response;
        }
        let core = self.core.lock().unwrap();
        let Some(store) = core.store.as_ref() else {
            return response_failure(
                None,
                "get_session_stats",
                "Session is still initializing",
                None,
            );
        };
        let stats = crate::session_stats::session_stats(store, self.engine.model_context_window());
        response_success(None, "get_session_stats", Some(stats))
    }

    fn handle_get_messages(&self, payload: &Value) -> DaemonResponse {
        if let Err(response) = self.require_created("get_messages") {
            return response;
        }
        let page = transcript_page(payload);
        let capabilities = payload
            .get("capabilities")
            .and_then(Value::as_array)
            .map(|capabilities| {
                capabilities
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let core = self.core.lock().unwrap();
        let Some(page) = page else {
            let mut messages = core
                .store
                .as_ref()
                .map(SessionFile::messages)
                .unwrap_or_default();
            if crate::snapshot_stream::wants_image_elision(&capabilities) {
                crate::snapshot_stream::elide_snapshot_image_payloads(&mut messages);
            }
            return response_success(None, "get_messages", Some(json!({ "messages": messages })));
        };
        let Some(store) = core.store.as_ref() else {
            return response_failure(None, "get_messages", "History branch changed", None);
        };
        // Validate ancestry atomically with the page read. Appends keep the
        // attached leaf on this branch; navigation to another branch does not.
        let history_leaf = match payload.get("historyLeafId").map(Value::as_str) {
            Some(Some(leaf)) if store.branch_contains(leaf) => leaf.to_string(),
            Some(_) => {
                return response_failure(None, "get_messages", "History branch changed", None);
            }
            None => store.leaf_id.clone().unwrap_or_default(),
        };
        let Some(mut rows) = store.transcript_page(&page) else {
            return response_failure(None, "get_messages", "History branch changed", None);
        };
        if crate::snapshot_stream::wants_image_elision(&capabilities) {
            crate::snapshot_stream::elide_snapshot_image_payloads(&mut rows.messages);
        }
        response_success(
            None,
            "get_messages",
            Some(json!({
                "messages": rows.messages,
                "firstEntryId": rows.first_entry_id,
                "lastEntryId": rows.last_entry_id,
                "more": rows.more,
                "historyLeafId": history_leaf,
            })),
        )
    }

    /// `get_session_messages`: one slice of the session's display transcript, filtered by
    /// role globs; a slice into a windowed store's unloaded prefix hydrates the store first.
    async fn handle_get_session_messages(&self, payload: &Value) -> DaemonResponse {
        const NAME: &str = "get_session_messages";
        if let Err(response) = self.require_created(NAME) {
            return response;
        }
        let roles: Vec<String> = payload
            .get("roles")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        let query = match SessionMessagesQuery::new(
            payload.get("start").and_then(Value::as_i64).unwrap_or(0),
            payload.get("limit").and_then(Value::as_u64).unwrap_or(0),
            &roles,
        ) {
            Ok(query) => query,
            Err(error) => return response_failure(None, NAME, &error.to_string(), None),
        };
        let read = || {
            let core = self.core.lock().unwrap();
            let slice = core
                .store
                .as_ref()
                .and_then(|store| query.read(store, &core.running_tool_calls));
            slice
        };
        let mut slice = read();
        if slice.is_none() {
            if let Err(error) = self.hydrate_history().await {
                return response_failure(None, NAME, &error.to_string(), None);
            }
            slice = read();
        }
        let Some(slice) = slice else {
            return response_failure(None, NAME, "Session history is unavailable", None);
        };
        response_success(None, NAME, Some(slice.into_reply()))
    }

    /// Load a windowed store's full history in place; a full store stays as it is.
    async fn hydrate_history(&self) -> Result<()> {
        let Some(path) = self
            .core
            .lock()
            .unwrap()
            .store
            .as_ref()
            .filter(|store| store.window.is_some())
            .map(|store| store.path.clone())
        else {
            return Ok(());
        };
        let load_path = path.clone();
        let full = tokio::task::spawn_blocking(move || SessionFile::open(&load_path)).await??;
        if let Some(store) = self
            .core
            .lock()
            .unwrap()
            .store
            .as_mut()
            .filter(|store| store.path == path)
        {
            store.install_full_history(full);
        }
        // The hydrated full file was a transient whole-file copy on
        // top of the installed history: release its freed heap.
        pa_types::memory_release::trim_freed_heap();
        Ok(())
    }

    fn handle_get_queue(&self) -> DaemonResponse {
        if let Err(response) = self.require_created("get_queue") {
            return response;
        }
        let core = self.core.lock().unwrap();
        // The labeled preview when the delivery carries one, else the
        // message text.
        response_success(
            None,
            "get_queue",
            Some(json!({
                "steering": core.steering.iter().map(|item| item.preview.clone().unwrap_or_else(|| item.message.clone())).collect::<Vec<_>>(),
                "followUp": core.follow_up.iter().map(|item| item.preview.clone().unwrap_or_else(|| item.message.clone())).collect::<Vec<_>>(),
            })),
        )
    }

    fn handle_clear_queue(&self) -> DaemonResponse {
        if let Err(response) = self.require_created("clear_queue") {
            return response;
        }
        let mut core = self.core.lock().unwrap();
        // Pickup and withdrawal share the core lock. A failed append leaves the
        // rows parked and suspended: neither disposal nor a clearing ACK is safe.
        let identities: Vec<Value> = core
            .steering
            .iter()
            .chain(&core.follow_up)
            .filter(|item| {
                item.queue_key.as_ref().is_some_and(|key| {
                    core.steering
                        .iter()
                        .chain(&core.follow_up)
                        .filter(|row| row.queue_key.as_ref() == Some(key))
                        .count()
                        == 1
                })
            })
            .filter_map(|item| item.input_identity.clone())
            .collect();
        for identity in identities {
            let result = core
                .store
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Session store unavailable"))
                .and_then(|store| {
                    store.persist_entry(
                        "custom",
                        json!({"customType":"input_withdrawal","data":identity}),
                    )
                });
            if let Err(error) = result {
                core.queued_input_suspended = true;
                return response_failure(
                    None,
                    "clear_queue",
                    &format!("Input withdrawal unresolved: {error:#}"),
                    None,
                );
            }
        }
        // The dropped agent-message rows bounce to their senders: a queued
        // row's sender otherwise never learns its message will not deliver.
        let mut dropped_agent_rows = Vec::new();
        let drain_lane =
            |lane: &mut VecDeque<QueuedItem>, dropped_agent_rows: &mut Vec<Value>| -> Vec<String> {
                lane.drain(..)
                    .map(|item| {
                        if let Some(id) = item.admission_id.as_deref() {
                            self.prompt_admissions.clear(id);
                        }
                        if item.agent_message.is_some() {
                            dropped_agent_rows.extend(item.custom_message);
                        }
                        item.message
                    })
                    .collect()
            };
        let steering: Vec<String> = drain_lane(&mut core.steering, &mut dropped_agent_rows);
        let follow_up: Vec<String> = drain_lane(&mut core.follow_up, &mut dropped_agent_rows);
        clear_withdrawn_progress_note(&core, self.engine.as_ref());
        let snapshot = Self::snapshot_locked(&core);
        drop(core);
        // The verdict refresh rides the same checkpoint as the snapshot: a stale
        // busy=true from the cleared items' admission must not revive an empty session.
        self.checkpoint_queue(QueueCheckpoint::Settle {
            operation: "queue_cleared",
        });
        let _ = self.emit_action_update(&snapshot);
        for row in &dropped_agent_rows {
            self.bounce_cleared_agent_message(row);
        }
        response_success(
            None,
            "clear_queue",
            Some(json!({ "steering": steering, "followUp": follow_up })),
        )
    }

    /// One dropped agent-message row's sender notice: the row's own
    /// `details` name the sender and the body, and the bounce rides the
    /// supervisor `send_message` route from this session. Spawned off the
    /// clear's response path — a failed bounce is logged, never retried.
    fn bounce_cleared_agent_message(&self, row: &Value) {
        let Some(details) = row.get("details") else {
            return;
        };
        let sender = details
            .get("from")
            .and_then(|from| from.get("activeSessionId"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let Some(sender) = sender else {
            // CLI-origin senders carry no session to bounce to.
            return;
        };
        let message = details
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // The cleared queue is this worker's own; its identity — not the
        // row's delivery-time copy — names the receiver.
        let receiver = {
            let core = self.core.lock().unwrap();
            core.store
                .as_ref()
                .and_then(|store| store.session_name().map(str::to_string))
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| self.config.active_session_id.clone())
        };
        let command = json!({
            "type": "send_message",
            "targetActiveSessionId": sender,
            "message": cleared_agent_message_bounce_text(&receiver, message),
            "fromActiveSessionId": self.config.active_session_id,
            "agentOrigin": true,
        });
        let link = std::sync::Arc::clone(&self.roster_link);
        let sender = sender.to_string();
        let timeout = std::time::Duration::from_millis(crate::messaging::WORKER_REQUEST_TIMEOUT_MS);
        tokio::spawn(async move {
            if let Err(error) = link.request_success(command, timeout).await {
                eprintln!("pa-daemon: cleared agent-message bounce to {sender} failed: {error:#}");
            }
        });
    }

    fn handle_abort_and_clear_queue(&self) -> DaemonResponse {
        let cleared = self.handle_clear_queue();
        if !cleared.success {
            return cleared;
        }
        let mut core = self.core.lock().unwrap();
        core.abort_requested = true;
        core.turn_cancelled = true;
        // The same TS `requestAbort()` suspension as the bare `abort`.
        core.queued_input_suspended = true;
        drop(core);
        // And the same eager agent abort.
        self.engine.abort_in_flight_turn();
        self.idle_notify.notify_waiters();
        response_success(None, "abort_and_clear_queue", cleared.data)
    }

    fn handle_get_last_assistant_text(&self) -> DaemonResponse {
        if let Err(response) = self.require_created("get_last_assistant_text") {
            return response;
        }
        let core = self.core.lock().unwrap();
        let text = core.store.as_ref().and_then(|store| {
            store
                .messages()
                .into_iter()
                .rev()
                .find(|message| crate::types::message_role(message) == Some("assistant"))
                .map(|message| crate::types::message_text(&message))
        });
        response_success(
            None,
            "get_last_assistant_text",
            Some(json!({ "text": text })),
        )
    }

    async fn handle_kill(&self, payload: &Value) -> DaemonResponse {
        let reason = KillCloseReason::from_payload(payload);
        // The session is closing: the continuation mint sites and
        // their settle-hook retries bail from here on.
        if let Some(agent_engine) = &self.agent_engine {
            agent_engine.mark_session_closed();
        }
        // TS `closeSession` aborts the side questions per attached client
        // before `closeSessionOnce`'s arms run.
        self.side_questions
            .abort_all_and_settle(SIDE_QUESTION_SETTLE_TIMEOUT)
            .await;
        // `killed` cancels the session's scheduled jobs; `replaced` keeps the plain
        // cron jobs but cancels the subagent's RLM heartbeats; `shutdown` keeps them
        // all. The store cancel is durable, so the stopped session's own heartbeats
        // can never revive it.
        let cancel_result = match reason {
            KillCloseReason::Killed => self.cancel_session_scheduled_jobs().await,
            KillCloseReason::Replaced => self.cancel_session_rlm_heartbeats().await,
            KillCloseReason::Shutdown => Ok(()),
        };
        if let Err(error) = cancel_result {
            return response_failure(None, "kill", &error.to_string(), None);
        }
        // The close cascades to the resident children with the SAME reason before
        // the session's own archive and dispose; a close failure is swallowed.
        let child_reason = match reason {
            KillCloseReason::Killed => crate::rlm_children::ChildCloseReason::Killed,
            KillCloseReason::Shutdown => crate::rlm_children::ChildCloseReason::Shutdown,
            KillCloseReason::Replaced => crate::rlm_children::ChildCloseReason::Replaced,
        };
        if let Err(error) = self.close_rlm_children(child_reason).await {
            eprintln!("pa-daemon: RLM child close at kill failed: {error:#}");
        }
        // The `archived` entry lands before the turn's aborted row (the abort flag
        // below gates the aborted row), so the file order stays the TS one.
        // The guard rides a block, not an explicit drop: a `drop(core)` does not
        // end the guard's slot in an async generator (the awaits need Send).
        {
            let mut core = self.core.lock().unwrap();
            // `shutdown` keeps the resume entry, so its file stays
            // live on disk; the killed and replaced closes archive.
            if reason != KillCloseReason::Shutdown {
                if let Some(store) = core.store.as_mut() {
                    if let Err(error) = store.persist_entry(
                        "session_state",
                        json!({ "state": { "status": "archived" } }),
                    ) {
                        return response_failure(None, "kill", &error.to_string(), None);
                    }
                }
            }
            core.created = false;
        }
        // The abort funnel fires BEFORE every close step that can wait on the
        // session mutex the running turn holds across its provider wait: the later
        // awaits settle on an already-cancelled turn. The cancel sweep must land
        // before the close clears the lanes.
        {
            let mut core = self.core.lock().unwrap();
            core.abort_requested = true;
            core.turn_cancelled = true;
            core.steering.clear();
            core.follow_up.clear();
        }
        self.work_notify.notify_one();
        self.compaction.abort();
        self.tree_navigation.abort();
        self.engine.abort_in_flight_turn();
        // The awaited settle lets the aborted turn's row broadcast and persist (the
        // #247 gate's aborted-row exception); only then does the runtime dispose run
        // (the kernel teardown must not race a live run).
        self.await_session_work_settled().await;
        // The session-ended finalization runs AFTER the awaited settle: the
        // ended-run accounting includes the aborted turn, never a pending response.
        self.engine.archive_session_telemetry().await;
        // The session's kernel dies with the session: the worker keeps the engine
        // object, so the #235 engine-drop teardown cannot run yet — dispose explicitly.
        if let Some(agent_engine) = &self.agent_engine {
            agent_engine.dispose_kernel().await;
        }
        let active_session_id = self.core.lock().unwrap().active_session_id.clone();
        let _ = self.emit_session_closed(&active_session_id, reason.session_closed_reason());
        let _ = self.record_recovery(false, reason.recovery_operation());
        self.release_session_lease();
        // The session runtime ended (TS `prime-agent stop <agent>`): the
        // pane reporter releases its pane as the last write on the wire —
        // no report may reclaim it afterwards. The slot is taken out
        // first (swapped to the disabled no-op) so a later attach can
        // adopt the pane again; the taken handle's release is the
        // release of the session this kill stopped.
        let reporter = std::mem::take(&mut *self.herdr.lock().unwrap());
        reporter.release().await;
        response_success(None, "kill", None)
    }

    /// `mark_anthropic_warning_shown` (Rust-native, operator directive
    /// 2026-09-29): the interactive client reports that it just drew the
    /// Anthropic subscription ban-risk warning; the worker persists the
    /// once-per-session-lifecycle marker row (the gate a reattach, a resume,
    /// or a worker replacement reads). Idempotent — a session already marked
    /// (or an in-memory session with no file) answers success without a
    /// second row.
    fn handle_mark_anthropic_warning_shown(&self) -> DaemonResponse {
        const NAME: &str = "mark_anthropic_warning_shown";
        if let Err(response) = self.require_created(NAME) {
            return response;
        }
        let mut core = self.core.lock().unwrap();
        let Some(store) = core.store.as_mut() else {
            return response_failure(None, NAME, "Session is still initializing", None);
        };
        if store.anthropic_warning_shown() {
            return response_success(None, NAME, None);
        }
        match store.mark_anthropic_warning_shown() {
            Ok(()) => response_success(None, NAME, None),
            Err(error) => response_failure(None, NAME, &error.to_string(), None),
        }
    }

    pub(crate) fn handle_rename(&self, command: &str, payload: &Value) -> DaemonResponse {
        if let Err(response) = self.require_created(command) {
            return response;
        }
        let name = payload.get("name").and_then(Value::as_str).unwrap_or("");
        if name.trim().is_empty() {
            return response_failure(None, command, "Session name cannot be empty", None);
        }
        let (mut core, inputs) = self.summary_inputs();
        let previous = core
            .store
            .as_ref()
            .and_then(|store| store.session_name().map(str::to_string));
        if let Some(store) = core.store.as_mut() {
            if let Err(error) = store.persist_entry("session_info", json!({ "name": name })) {
                return response_failure(None, command, &error.to_string(), None);
            }
        }
        let summary = self.summary_locked(&core, inputs);
        // TS #2529 `applyStateSessionName`: a rename that changed an
        // existing name leaves the renamed session a displayed transcript
        // notice (" by parent" when the rename arrived from the parent
        // session); a first name leaves none. The notice's durable row
        // lands under the SAME core lock as the name write — worker
        // commands run concurrently, so a second lock scope here could
        // interleave another command's writes between the name and its
        // notice, ordering the notice against a contradicted name history.
        // The frames share that lock so a compaction cannot include the
        // notice before its broadcast; `session_info_changed` still comes first.
        let renamed_notice = previous
            .as_deref()
            .filter(|previous| *previous != name)
            .map(|previous| {
                let content = if payload.get("renamedBy").and_then(Value::as_str)
                    == Some(
                        pa_core::session_engine::agent_messaging::AgentFamilyRelationship::Parent
                            .as_str(),
                    ) {
                    format!("Session renamed `{previous}` -> `{name}` by parent")
                } else {
                    format!("Session renamed `{previous}` -> `{name}`")
                };
                json!({
                    "customType": pa_core::session_engine::messages::SESSION_RENAMED_CUSTOM_TYPE,
                    "content": content,
                    "display": true,
                })
            });
        if let Some(notice) = renamed_notice.as_ref() {
            persist_custom_row(&mut core, notice);
        }
        // `session_info_changed` makes every attached client re-read the name.
        super::emit_worker_event_locked(
            &mut core,
            &self.events,
            serde_json::json!({ "type": "session_info_changed", "name": name }),
        );
        if let Some(notice) = renamed_notice.as_ref() {
            super::broadcast_custom_row_locked(&mut core, &self.events, notice);
        }
        drop(core);
        // The sender identity follows the live name.
        if let Ok(summary_value) = serde_json::to_value(&summary) {
            self.engine.set_session_summary(summary_value);
        }
        response_success(
            None,
            command,
            Some(serde_json::to_value(&summary).unwrap_or(Value::Null)),
        )
    }
}

/// The bounce body: the receiver label, the fact, and the dropped body —
/// truncated so the whole notice stays inside the delivery size limit the
/// receiving `worker_deliver_message` enforces.
fn cleared_agent_message_bounce_text(receiver: &str, message: &str) -> String {
    let limit = pa_core::session_engine::agent_messaging::DEFAULT_AGENT_MESSAGE_MAX_CHARS;
    let opening = "Your agent message was cleared from ";
    let ending = "'s queue before delivery: ";
    // Session names are unbounded. Reserve the notice itself before fitting
    // the receiver label, then spend the remaining characters on the body.
    let receiver_budget = limit.saturating_sub(opening.chars().count() + ending.chars().count());
    let receiver: String = receiver.chars().take(receiver_budget).collect();
    let prefix = format!("{opening}{receiver}{ending}");
    let budget = limit.saturating_sub(prefix.chars().count());
    let body: String = message.chars().take(budget).collect();
    format!("{prefix}{body}")
}

/// The display-transcript page a `get_messages` payload asks for, if any.
fn transcript_page(payload: &Value) -> Option<pa_types::daemon::TranscriptPage> {
    serde_json::from_value(payload.get("page")?.clone()).ok()
}

#[cfg(test)]
mod bounce_text_tests {
    use super::cleared_agent_message_bounce_text;
    use pa_core::session_engine::agent_messaging::DEFAULT_AGENT_MESSAGE_MAX_CHARS;

    #[test]
    fn bounce_text_limits_receiver_and_body_without_losing_the_notice() {
        for (receiver, message) in [
            (
                "target".to_string(),
                "界".repeat(DEFAULT_AGENT_MESSAGE_MAX_CHARS),
            ),
            (
                "界".repeat(DEFAULT_AGENT_MESSAGE_MAX_CHARS + 1),
                "body".to_string(),
            ),
            (
                "界".repeat(DEFAULT_AGENT_MESSAGE_MAX_CHARS + 1),
                String::new(),
            ),
        ] {
            let text = cleared_agent_message_bounce_text(&receiver, &message);
            assert_eq!(text.chars().count(), DEFAULT_AGENT_MESSAGE_MAX_CHARS);
            assert!(text.starts_with("Your agent message was cleared from "));
            assert!(text.contains("'s queue before delivery: "));
        }
    }
}

#[cfg(test)]
mod owner_cancel_wire_tests {
    use super::*;
    #[tokio::test]
    async fn session_quarantine_denies_actual_worker_admission_routes_preserving_audit() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = crate::worker::WorkerConfig {
            socket_path: dir.path().join("w.sock"),
            supervisor_socket_path: std::path::PathBuf::new(),
            token: "fixture".into(),
            worker_instance_id: String::new(),
            active_session_id: "child".into(),
            agent_dir: dir.path().join("agent"),
            recovery_journal_path: dir.path().join("r.jsonl"),
            telemetry_disabled: Some(true),
            script: None,
        };
        let worker = Worker::new(config, None);
        assert!(
            worker
                .dispatch("create", &json!({"noSession":true,"cwd":dir.path()}))
                .await
                .success
        );
        let path = dir.path().join("child.jsonl");
        std::fs::write(&path, "preserved").unwrap();
        worker.core.lock().unwrap().store.as_mut().unwrap().path = path.clone();
        let marker = crate::lease::SessionQuarantine {
            version: 1,
            parent_session_file: dir.path().join("parent.jsonl").to_string_lossy().into(),
            child_id: "child".into(),
            child_session_file: path.to_string_lossy().into(),
            task: json!({}),
            assignment: json!({}),
            current_admission_generation: None,
            current_expected_message_id: None,
        };
        crate::lease::persist_session_quarantine(&worker.config.agent_dir, &marker).unwrap();
        for command in [
            "prompt",
            "prompt_and_wait",
            "steer",
            "follow_up",
            "start_side_question",
            "attach",
            "reattach",
            "resume_queue",
            "execute_bash",
            "worker_deliver_message",
            "compact",
            "restore_next_turn",
        ] {
            let response = worker
                .dispatch(command, &json!({"message":"must-not-admit"}))
                .await;
            assert!(!response.success, "quarantine route {command} admitted");
            assert!(
                response.error.as_deref().unwrap().contains("quarantined"),
                "{command}: {response:?}"
            );
        }
        assert!(worker.dispatch("get_state", &json!({})).await.success);
        assert!(worker.core.lock().unwrap().steering.is_empty());
        assert!(worker.core.lock().unwrap().follow_up.is_empty());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "preserved");
    }

    #[tokio::test]
    async fn historical_owner_cancellation_and_owner_cancel_wire_normalize_only_transport_fields() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = crate::worker::WorkerConfig {
            socket_path: dir.path().join("w.sock"),
            supervisor_socket_path: std::path::PathBuf::new(),
            token: "fixture".into(),
            worker_instance_id: String::new(),
            active_session_id: "parent".into(),
            agent_dir: dir.path().join("agent"),
            recovery_journal_path: dir.path().join("r.jsonl"),
            telemetry_disabled: Some(true),
            script: None,
        };
        let worker = Worker::new(config, None);
        assert!(
            worker
                .dispatch("create", &json!({"noSession":true,"cwd":dir.path()}))
                .await
                .success
        );
        let readonly = json!({"type":"get_orchestration_gate","id":"request","activeSessionId":"parent","clientId":"owner-client","historicalOwnerCancellation":{}});
        let mut injected = readonly.clone();
        injected["fact"] = json!({"forged":"evidence"});
        let rejected = worker.dispatch("get_orchestration_gate", &injected).await;
        assert_eq!(
            rejected.error.as_deref(),
            Some("Unexpected historical lookup fields")
        );
        let rejected = worker.dispatch("get_orchestration_gate", &readonly).await;
        assert_eq!(
            rejected.error.as_deref(),
            Some("Parent recovery journal unavailable")
        );
        *worker.recovery.lock().unwrap() = Some(
            crate::journal::WorkerRecoveryJournal::open(&dir.path().join("fixture-recovery.jsonl"))
                .unwrap(),
        );
        let rejected = worker.dispatch("get_orchestration_gate", &readonly).await;
        assert!(rejected.error.as_deref().unwrap().contains("missing field"));
        let wire = json!({"type":"configure_orchestration_gate","id":"request","activeSessionId":"parent","clientId":"owner-client","ownerCancelChild":{"childId":"absent"}});
        let response = worker.dispatch("configure_orchestration_gate", &wire).await;
        assert!(!response.success);
        assert_eq!(response.error.as_deref(), Some("Direct child unavailable"));
        let mut unexpected = wire.clone();
        unexpected["unexpected"] = json!(true);
        let response = worker
            .dispatch("configure_orchestration_gate", &unexpected)
            .await;
        assert_eq!(
            response.error.as_deref(),
            Some("Unexpected owner cancellation fields")
        );
        let mut unexpected = wire;
        unexpected["ownerCancelChild"]["unexpected"] = json!(true);
        let response = worker
            .dispatch("configure_orchestration_gate", &unexpected)
            .await;
        assert_eq!(
            response.error.as_deref(),
            Some("Invalid owner cancellation request")
        );
        let request = json!({"expectedRevision":0,"task":{"projectId":"p","planId":"plan","stepId":"step","taskGeneration":1,"policyRevision":1},
            "assignment":{"childId":"absent","admissionGeneration":"g","incarnation":"old","assignmentId":"a","taskGeneration":1,"ownership":"lane","acceptanceRef":"accept","artifactRef":"artifact","dependenciesSatisfied":true}});
        let wire = json!({"type":"configure_orchestration_gate","id":"request","activeSessionId":"parent","clientId":"owner-client","reconcileOwnerCancellation":request});
        let response = worker.dispatch("configure_orchestration_gate", &wire).await;
        assert_eq!(response.error.as_deref(), Some("Direct child unavailable"));
        let mut extra = wire.clone();
        extra["unexpected"] = json!(true);
        let response = worker
            .dispatch("configure_orchestration_gate", &extra)
            .await;
        assert_eq!(
            response.error.as_deref(),
            Some("Unexpected owner reconciliation fields")
        );
        let mut forged = wire;
        forged["reconcileOwnerCancellation"]["fact"] = json!({});
        let response = worker
            .dispatch("configure_orchestration_gate", &forged)
            .await;
        assert!(response.error.as_deref().unwrap().contains("unknown field"));
    }
}

/// Keep the installed response shape unless the versioned client requested its envelope.
fn goal_control_response(payload: &Value, goal: Value) -> Value {
    if payload.get("extensionVersion").and_then(Value::as_u64) == Some(1)
        || payload.get("expectedGoalId").is_some()
    {
        json!({"extensionVersion":1,"goal":goal})
    } else {
        goal
    }
}
