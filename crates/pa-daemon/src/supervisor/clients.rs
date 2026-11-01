//! Client connections: the per-connection task - read loop, dispatch,
//! and the parsed-command execution surface.
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::{
    app_version, broadcast, command_type_name, current_protocol_info,
    daemon_closing_shutdown_event, input_admission_id, json, parse_supervisor_command_line,
    response_failure, response_line, response_success, salvage_command_type, salvage_id,
    subscribers, update_gate_refuses, util, Arc, AsyncBufReadExt, AsyncWriteExt, BufReader,
    ClientRouting, DaemonCommand, DaemonOutbound, DaemonRuntimeIdentity, Duration,
    EnvelopeParseError, Map, Ordering, Outbound, ResidentWorker, Result, RouteAdmission,
    Supervisor, TransportStream, TypedCreateRejection, Value, DAEMON_SCHEMA_ID,
    DAEMON_SCHEMA_REVISION, ROUTE_TIMEOUT_MS, UPDATE_PREPARING_MESSAGE,
};

/// TS `OWNED_WORKER_DISCONNECT_GRACE_MS`: how long a client-owned worker
/// keeps running after its owner's last connection closes.
pub(super) const OWNED_WORKER_DISCONNECT_GRACE: Duration = Duration::from_secs(30);

struct OwnedCleanupRunning {
    supervisor: Arc<Supervisor>,
    resident: Arc<ResidentWorker>,
    identity: Option<crate::ownership::OwnedWorkerCleanupIdentity>,
}

impl Drop for OwnedCleanupRunning {
    fn drop(&mut self) {
        self.resident
            .owner_cleanup_running
            .store(false, Ordering::SeqCst);
        if let Some(identity) = &self.identity {
            self.supervisor
                .owner_cleanup_receipts
                .lock()
                .unwrap()
                .retain(|(existing, verified)| existing != identity || *verified);
        }
        self.supervisor.owner_cleanup_notify.notify_waiters();
    }
}

async fn write_line<W: AsyncWriteExt + Unpin>(writer: &mut W, value: &Value) -> Result<usize> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    let bytes = line.len();
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await?;
    Ok(bytes)
}

/// Write one pre-serialized client line (the byte relay's raw form
/// already carries its trailing newline).
async fn write_raw_line<W: AsyncWriteExt + Unpin>(writer: &mut W, line: &[u8]) -> Result<usize> {
    let bytes = line.len();
    writer.write_all(line).await?;
    writer.flush().await?;
    Ok(bytes)
}

pub(crate) fn client_command_payload(
    command: &DaemonCommand,
    client_id: &str,
) -> Result<(&'static str, Value)> {
    let type_name = command_type_name(command);
    let mut payload = serde_json::to_value(command)?;
    if let Some(object) = payload.as_object_mut() {
        object.insert("clientId".to_string(), json!(client_id));
        // The supervisor always attaches slim; the client's OWN capability set rides
        // alongside as `clientCapabilities`, which the worker echoes into the attach result.
        if let DaemonCommand::Attach { capabilities, .. }
        | DaemonCommand::Reattach { capabilities, .. } = command
        {
            object.insert(
                "capabilities".to_string(),
                json!(["attach_snapshot", "event_sequence", "slim_attach"]),
            );
            object.insert(
                "clientCapabilities".to_string(),
                json!(crate::snapshot_stream::attach_client_capabilities(
                    capabilities.as_deref()
                )),
            );
        }
        // Create carries its fields under `config`; the worker reads them flat.
        if let Some(config) = object.remove("config") {
            if let Some(config) = config.as_object() {
                for (key, value) in config {
                    object.insert(key.clone(), value.clone());
                }
            }
        }
    }
    Ok((type_name, payload))
}

#[cfg(test)]
mod private_owner_envelope_tests {
    use super::*;
    #[test]
    fn validates_public_private_owner_envelope() {
        let valid = json!({"type":"command","id":"outer","protocol":{"name":"prime-agent.daemon","version":7},"command":{"type":"private_owner_release","id":"inner"}});
        let extracted = private_owner_request(&valid.to_string()).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&extracted).unwrap()["id"],
            "outer"
        );
        for bad in [
            "{\"type\":\"command\",\"type\":\"command\"}".to_string(),
            "{not json}".to_string(),
            "x".repeat(65537),
        ] {
            assert!(private_owner_request(&bad).is_none());
        }
        for version in [0, 6] {
            let mut value = valid.clone();
            value["protocol"]["version"] = json!(version);
            assert!(private_owner_request(&value.to_string()).is_none());
        }
        for (field, value) in [("id", json!(1)), ("clientId", json!({}))] {
            let mut bad = valid.clone();
            bad[field] = value;
            assert!(private_owner_request(&bad.to_string()).is_none());
        }
        let mut wrong = valid;
        wrong["protocol"]["name"] = json!("other");
        assert!(private_owner_request(&wrong.to_string()).is_none());
    }
}

// Validate the ordinary public envelope before entering the separately attested lane.
fn private_owner_request(raw: &str) -> Option<String> {
    if raw.len() > 65536 {
        return None;
    }
    let mut value = super::private_launch::strict(raw.as_bytes()).ok()?;
    if value["type"].as_str()?.starts_with("private_owner_") {
        return Some(raw.to_string());
    }
    if value["type"] != "command"
        || !value["command"]["type"]
            .as_str()?
            .starts_with("private_owner_")
    {
        return None;
    }
    let mut command = value.get_mut("command")?.take();
    value["command"] = json!({"type":"list"});
    let envelope = parse_supervisor_command_line(&serde_json::to_string(&value).ok()?).ok()?;
    command
        .as_object_mut()?
        .insert("id".into(), json!(envelope.id));
    serde_json::to_string(&command).ok()
}

impl Supervisor {
    async fn dispatch_private_owner(
        self: &Arc<Self>,
        raw: &str,
        connection: &str,
        peer: Option<(u32, u32)>,
    ) -> Value {
        let parsed = super::private_launch::strict(raw.as_bytes()).unwrap_or(Value::Null);
        let command = parsed["type"]
            .as_str()
            .unwrap_or("private_owner_invalid_request");
        let _operation = self.private_bootstrap_operation.lock().await;
        let mutating = !matches!(
            command,
            "private_owner_inspect" | "private_owner_acquire_status" | "private_owner_call_status"
        );
        if self.shutting_down.load(Ordering::SeqCst)
            || (mutating && self.update_prepare.active_state().is_some())
        {
            return json!({"type":"response","id":parsed["id"],"command":command,"success":false,"code":"private_owner_unknown","error":"Supervisor is not accepting owner mutations"});
        }
        if mutating {
            self.mutation_drain.begin();
        }
        let action = self
            .private_launch_owner
            .lock()
            .expect("launch lock")
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("private_owner_unsupported"))
            .and_then(|owner| {
                owner.managed_request(
                    raw.as_bytes(),
                    connection,
                    peer,
                    &self.owner_cleanup_generation,
                )
            });
        let result = match action {
            Ok(super::private_launch::ManagedAction::Reply(value)) => Ok(value),
            Ok(super::private_launch::ManagedAction::Bootstrap {
                payload,
                operation,
                issuer,
                sequence,
                digest,
            }) => match if matches!(
                payload["operation"].as_str(),
                Some("resident_drone_bind" | "resident_drone_status" | "resident_drone_control")
            ) {
                let command = payload["operation"]
                    .as_str()
                    .unwrap_or("resident_drone_status")
                    .to_string();
                Ok(self
                    .resident_drone_request(payload)
                    .await
                    .unwrap_or_else(|error| {
                        response_failure(None, &command, &error.to_string(), None)
                    }))
            } else {
                self.forward_private_bootstrap(payload).await
            } {
                Ok(response) => {
                    let value = response_line(&response);
                    Ok(self
                        .private_launch_owner
                        .lock()
                        .expect("launch lock")
                        .as_mut()
                        .expect("owner exists")
                        .finish_managed_call(&operation, &issuer, sequence, &digest, value))
                }
                Err(_) => Err(anyhow::anyhow!("private_owner_unknown")),
            },
            Err(error) => Err(error),
        };
        if mutating {
            self.mutation_drain.end();
        }
        match result {
            Ok(value) => {
                json!({"type":"response","id":parsed["id"],"command":command,"success":true,"data":value})
            }
            Err(error) => {
                let text = error.to_string();
                let code = if text.starts_with("private_owner_") {
                    text.as_str()
                } else {
                    "private_owner_invalid_request"
                };
                json!({"type":"response","id":parsed["id"],"command":command,"success":false,"code":code,"error":"Private owner operation unavailable"})
            }
        }
    }

    async fn forward_private_bootstrap(
        self: &Arc<Self>,
        payload: Value,
    ) -> Result<super::DaemonResponse> {
        let selector = payload["activeSessionId"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Owner session missing"))?;
        let resident = self.registry.resolve(selector).await?;
        let key = resident
            .owner_bootstrap_key
            .lock()
            .expect("owner key lock")
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Private worker owner unavailable"))?;
        let descriptor = resident.descriptor.lock().await;
        let instance = descriptor
            .worker_instance_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Worker instance missing"))?;
        anyhow::ensure!(
            descriptor.session_file.as_deref() == payload["sessionFile"].as_str()
                && descriptor.root_session_id.as_deref() == payload["rootSessionId"].as_str(),
            "Owner target mismatch"
        );
        drop(descriptor);
        let sequence = resident
            .owner_bootstrap_sequence
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        let raw = serde_json::to_string(&payload)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)
            .map_err(|_| anyhow::anyhow!("Owner key unavailable"))?;
        mac.update(&crate::worker::owner_bootstrap::bytes(
            &self.owner_cleanup_generation,
            &instance,
            &resident.worker_id,
            sequence,
            &raw,
        ));
        let signature = format!("{:x}", mac.finalize().into_bytes());
        self.route_command_typed(&resident,"private_owner_bootstrap",json!({"version":1,"generation":self.owner_cleanup_generation,"instance":instance,"worker":resident.worker_id,"sequence":sequence,"payload":raw,"macHex":signature}),super::routing::ROUTE_TIMEOUT_MS,RouteAdmission::SupervisorInternal).await
    }

    /// Register the connection, serve it, then deregister and arm the
    /// owner-disconnect cleanup (TS socket `cleanup`) on every exit path.
    pub(super) async fn handle_client(
        self: Arc<Self>,
        stream: Box<dyn TransportStream>,
    ) -> Result<()> {
        let connection_id = util::new_display_id();
        let effective_client_id = Arc::new(std::sync::Mutex::new(connection_id.clone()));
        self.client_connections
            .lock()
            .unwrap()
            .insert(connection_id.clone(), Arc::clone(&effective_client_id));
        let served = Arc::clone(&self)
            .serve_client(
                stream,
                connection_id.clone(),
                Arc::clone(&effective_client_id),
            )
            .await;
        self.client_connections
            .lock()
            .unwrap()
            .remove(&connection_id);
        let owner = effective_client_id.lock().unwrap().clone();
        self.schedule_owned_worker_cleanup_for_client(&owner).await;
        served
    }

    async fn serve_client(
        self: Arc<Self>,
        stream: Box<dyn TransportStream>,
        connection_id: String,
        effective_client_id: Arc<std::sync::Mutex<String>>,
    ) -> Result<()> {
        let private_peer = stream.peer_credentials();
        let (reader, mut writer) = stream.split();
        // The factory lane's advertisement gate reads the settings file
        // (metadata plus a locked read on a cache miss) — off the
        // executor thread, the same spawn_blocking posture as the daemon's
        // other settings reads. The read stays fresh per connection, so a
        // `/factory on` toggle surfaces on the next client start.
        let agent_dir = self.options.agent_dir.clone();
        let mut factory_capabilities = tokio::task::spawn_blocking(move || {
            crate::protocol::studio_server_capabilities(
                crate::factory_activity::advertised_server_capabilities(&agent_dir),
                crate::protocol::studio_session_preferences_enabled(),
            )
        })
        .await
        .map_err(|error| anyhow::anyhow!("the factory settings read failed: {error:#}"))?;
        if self
            .private_launch_owner
            .lock()
            .expect("launch lock")
            .as_ref()
            .is_some_and(super::private_launch::LaunchOwner::managed_available)
        {
            factory_capabilities.push("private_owner_reacquire_v1".to_string());
        }
        factory_capabilities.push("drone_control_v1".to_string());
        let hello = DaemonOutbound::DaemonHello {
            socket_path: self.options.socket_path.to_string_lossy().to_string(),
            protocol: current_protocol_info(),
            schema_id: Some(DAEMON_SCHEMA_ID.to_string()),
            schema_revision: Some(DAEMON_SCHEMA_REVISION),
            app_version: Some(app_version().to_string()),
            runtime: Some(DaemonRuntimeIdentity {
                build_id: concat!("pa-daemon-rs-", env!("CARGO_PKG_VERSION")).to_string(),
                executable_path: std::env::current_exe()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default(),
                entrypoint_path: None,
                launcher_path: None,
            }),
            supervisor_generation: Some(self.owner_cleanup_generation.clone()),
            supervisor_pid: Some(u64::from(std::process::id())),
            supervisor_owner_token: Some(uuid::Uuid::new_v4().to_string()),
            supervisor_process_start_id: crate::protocol::process_start_id(std::process::id()),
            supervisor_socket_path: Some(self.options.socket_path.to_string_lossy().to_string()),
            update_resume: Some(self.restore.hello_resume()),
            client_id: connection_id.clone(),
            server_capabilities: factory_capabilities,
            rest: Map::default(),
        };
        write_line(&mut writer, &serde_json::to_value(&hello)?).await?;

        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let mut events = self.events.subscribe();
        // Session events ride this per-connection queue (the subscriber
        // registry resolves delivery at publish time, TS `handleWorkerFrame`
        // parity); broadcast-class events keep the ring above.
        let (targeted_tx, mut targeted_rx) = tokio::sync::mpsc::channel::<Arc<Value>>(
            crate::backpressure::TARGETED_EVENT_QUEUE_CAPACITY,
        );
        // Connection state shared with the per-command dispatch tasks (the registry
        // insertion is the delivery boundary).
        let attached = subscribers::ClientSubscriptions::new(connection_id.clone(), targeted_tx);
        // Roster subscription flag shared with the per-command dispatch
        // tasks (`roster_subscribe` flips it; the event arm filters pushes).
        let roster_subscribed: Arc<std::sync::atomic::AtomicBool> =
            Arc::new(std::sync::atomic::AtomicBool::new(false));
        let connection = Arc::new(crate::input_pause_lease::ClientConnectionState::new());
        // Completed dispatches flow back through this channel so the loop keeps
        // writing: a long command must not block this client's events or its other
        // commands. Bounded so a slow client stalls only its own dispatch tasks.
        let (dispatch_tx, mut dispatch_rx) = tokio::sync::mpsc::channel::<(Vec<Outbound>, bool)>(
            crate::backpressure::CLIENT_OUTBOUND_CAPACITY,
        );
        // One dispatch slot per concurrent command. The read arm is armed only while a
        // slot is free — at the bound the loop stops reading the client's socket
        // (transport-level flow control, not unbounded task spawn).
        let dispatch_slots = Arc::new(tokio::sync::Semaphore::new(
            crate::backpressure::CLIENT_DISPATCH_CONCURRENCY,
        ));
        // The loop yields the connection's result; a write failure parks the
        // error and falls through the shared exit tail below.
        let served: Result<()> = 'serve: loop {
            line.clear();
            tokio::select! {
                biased;
                read = reader.read_line(&mut line), if dispatch_slots.available_permits() > 0 => {
                    let Ok(read) = read else { break 'serve Ok(()) };
                    if read == 0 {
                        break 'serve Ok(());
                    }
                    let trimmed = line.trim().to_string();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let private_request = private_owner_request(&trimmed);
                    if let Some(raw) = private_request {
                        let response = self.dispatch_private_owner(&raw, &connection_id, private_peer).await;
                        if let Err(error) = write_line(&mut writer, &response).await { break 'serve Err(error); }
                        continue;
                    }
                    // The arm's guard proved a slot free (this loop is the only slot
                    // acquirer), so the non-blocking take always succeeds.
                    let dispatch_slot = Arc::clone(&dispatch_slots)
                        .try_acquire_owned()
                        .expect("the read arm's guard held a dispatch slot");
                    let supervisor = Arc::clone(&self);
                    let effective_client_id = Arc::clone(&effective_client_id);
                    let attached = Arc::clone(&attached);
                    let roster_subscribed = Arc::clone(&roster_subscribed);
                    let connection = Arc::clone(&connection);
                    let dispatch_tx = dispatch_tx.clone();
                    // A streaming command (list_saved_sessions) writes its progress frames
                    // through the SAME channel the response later takes, so they stay ordered.
                    let stream_tx = dispatch_tx.clone();
                    let connection_id = connection_id.clone();
                    tokio::spawn(async move {
                        let (lines, stop) = supervisor
                            .dispatch_client(
                                &trimmed,
                                &effective_client_id,
                                &attached,
                                &roster_subscribed,
                                &connection,
                                &connection_id,
                                &stream_tx,
                            )
                            .await;
                        if dispatch_tx.send((lines, stop)).await.is_err() && stop {
                            // Only a terminal shutdown owns the descriptor-deleting stop pass; an
                            // update restart must leave its descriptors for the successor.
                            supervisor
                                .start_shutdown_if_owner(&connection_id)
                                .await;
                        }
                        // The slot frees only once the bundle is in the queue.
                        drop(dispatch_slot);
                    });
                }
                targeted = targeted_rx.recv() => {
                    // A session event routed by the subscriber registry at
                    // publish time: the delivery decision already ran, the
                    // frame only writes (the queue preserves per-session
                    // publish order). The arm polls ahead of the response
                    // arm, so an event published before a response bundle
                    // is queued is written first - the worker's own
                    // event-before-response socket order survives the hop.
                    if let Some(payload) = targeted {
                        if let Err(error) = write_line(&mut writer, &payload).await {
                            // An event-write failure exits through the shared cleanup below.
                            break 'serve Err(error);
                        }
                    } else {
                        break 'serve Ok(());
                    }
                }
                dispatched = dispatch_rx.recv() => {
                    let Some((lines, stop)) = dispatched else { break 'serve Ok(()) };
                    for outbound in lines {
                        let written = match &outbound {
                            Outbound::Line(value) => write_line(&mut writer, value).await,
                            Outbound::Raw(line) => write_raw_line(&mut writer, line).await,
                        };
                        let bytes = match written {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                // A failed response write must not strand the
                                // shutdown: the stop pass still has to run.
                                if stop {
                                    self.ensure_shutdown_started().await;
                                }
                                break 'serve Err(error);
                            }
                        };
                        // A large outbound response freed big transients: return the heap to
                        // the OS instead of letting the arenas hold the phase's peak.
                        drop(outbound);
                        pa_types::memory_release::trim_freed_heap_if_large(bytes);
                    }
                    if stop {
                        // The initiating client's response and daemon_closing lines are flushed
                        // above; only now may the stop pass end the runtime (the accept loop
                        // stays up until begin_shutdown sets accept_exit).
                        self.ensure_shutdown_started().await;
                        break 'serve Ok(());
                    }
                }
                event = events.recv() => {
                    match event {
                        Ok((routing, payload)) => {
                            let deliver = match &routing {
                                ClientRouting::Broadcast => true,
                                ClientRouting::BroadcastExcept {
                                    connection_id: excluded,
                                } => excluded.as_str() != connection_id.as_str(),
                                ClientRouting::RosterSubscribers => {
                                    roster_subscribed.load(std::sync::atomic::Ordering::SeqCst)
                                }
                            };
                            if deliver {
                                if let Err(error) = write_line(&mut writer, &payload).await {
                                    // An event-write failure exits through the shared cleanup below.
                                    break 'serve Err(error);
                                }
                            }
                        }
                        // A lagged receiver means the ring dropped this many events for THIS
                        // connection: the loss is the defined backpressure, but never invisible —
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            self.log_line(&format!(
                                "client {connection_id} lagged on the event ring: {skipped} events dropped"
                            ));
                        }
                        Err(broadcast::error::RecvError::Closed) => break 'serve Ok(()),
                    }
                }
            }
        };
        if let Some(owner) = self
            .private_launch_owner
            .lock()
            .expect("launch lock")
            .as_mut()
        {
            owner.disconnect(&connection_id);
        }
        // Only the connection that accepted the shutdown may run the stop pass from this
        // fallback: another client disconnecting in the response window must not preempt the
        // acknowledgement. Every exit path lands here, including the write failures.
        self.retire_client_dispatches(&mut dispatch_rx, &connection_id)
            .await;
        // Detach from every attached session on disconnect (a TUI exit does not
        // stop the session). The registry entries go first — no session event may
        // be enqueued for a connection whose loop has exited.
        attached.detach_all(&self.session_subscribers);
        let attached_sessions = attached.session_ids();
        for active_session_id in &attached_sessions {
            if let Ok(resident) = self.registry.resolve(active_session_id).await {
                let payload = json!({ "type": "detach", "clientId": effective_client_id.lock().unwrap().clone() });
                let _ = self
                    .route_command_typed(
                        &resident,
                        "detach",
                        payload,
                        ROUTE_TIMEOUT_MS,
                        RouteAdmission::SupervisorInternal,
                    )
                    .await;
            }
        }
        // The disconnect's pause-lease cleanup: in-flight acquisitions invalidate, every held
        // lease releases, and waiting prompt admissions cancel with the TS cancellation error.
        self.release_all_client_pauses(&connection).await;
        connection.prompt_admissions.cancel_all_waiting();
        served
    }

    /// Retire a connection's response path before its asynchronous cleanup.
    async fn retire_client_dispatches(
        self: &Arc<Self>,
        dispatch_rx: &mut tokio::sync::mpsc::Receiver<(Vec<Outbound>, bool)>,
        connection_id: &str,
    ) {
        // Cleanup below awaits worker replies. A shutdown that claims this
        // connection later must fail its send and run the owner fallback,
        // rather than successfully enqueueing into a loop that has exited.
        dispatch_rx.close();
        self.start_shutdown_if_owner(connection_id).await;
    }

    /// The accepted-shutdown fallback on a connection's exit: only the
    /// connection that accepted the shutdown may start the stop pass, so
    /// another client disconnecting in the response window cannot preempt
    /// the acknowledgement.
    async fn start_shutdown_if_owner(self: &Arc<Self>, connection_id: &str) {
        let is_shutdown_owner =
            self.shutdown_owner.lock().unwrap().as_deref() == Some(connection_id);
        if is_shutdown_owner
            && self.shutting_down.load(Ordering::SeqCst)
            && !self.accept_exit.load(Ordering::SeqCst)
        {
            self.ensure_shutdown_started().await;
        }
    }

    /// Whether any live connection still speaks for `client_id` (one
    /// process may hold several connections).
    fn client_connected(&self, client_id: &str) -> bool {
        self.client_connections
            .lock()
            .unwrap()
            .values()
            .any(|effective| *effective.lock().unwrap() == client_id)
    }

    /// TS `scheduleOwnedWorkerCleanupForClient`.
    async fn schedule_owned_worker_cleanup_for_client(self: &Arc<Self>, client_id: &str) {
        for resident in self.registry.list().await {
            let owner = resident.descriptor.lock().await.owner_client_id.clone();
            if owner.as_deref() == Some(client_id) {
                self.schedule_owned_worker_cleanup(&resident).await;
            }
        }
    }

    /// Stop a client-owned worker [`OWNED_WORKER_DISCONNECT_GRACE`] after
    /// its owner's last connection closed (TS `scheduleOwnedWorkerCleanup`).
    /// A later arm replaces a pending timer; an owner connected at expiry
    /// keeps the worker.
    pub(super) async fn schedule_owned_worker_cleanup(
        self: &Arc<Self>,
        resident: &Arc<ResidentWorker>,
    ) {
        let owner = resident.descriptor.lock().await.owner_client_id.clone();
        let Some(owner) = owner else { return };
        if self.client_connected(&owner) {
            return;
        }
        let cleanup_identity = self.owned_worker_cleanup_identity(resident).await;
        let supervisor = Arc::clone(self);
        let timer_resident = Arc::clone(resident);
        let deadline = tokio::time::Instant::now() + OWNED_WORKER_DISCONNECT_GRACE;
        let task = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            // Clear this timer's own handle first, so a later arm can only
            // abort a sleeping timer, never a stop in progress. A newer arm's
            // handle in the slot means this timer was replaced (and aborted).
            {
                let mut slot = timer_resident.owner_cleanup.lock().unwrap();
                if slot.as_ref().map(tokio::task::AbortHandle::id) != Some(tokio::task::id()) {
                    return;
                }
                timer_resident
                    .owner_cleanup_running
                    .store(true, Ordering::SeqCst);
                if let Some(identity) = &cleanup_identity {
                    let mut receipts = supervisor.owner_cleanup_receipts.lock().unwrap();
                    if receipts.len() == 1024 {
                        receipts.pop_front();
                    }
                    receipts.push_back((identity.clone(), false));
                }
                slot.take();
            }
            let _running = OwnedCleanupRunning {
                supervisor: Arc::clone(&supervisor),
                resident: Arc::clone(&timer_resident),
                identity: cleanup_identity.clone(),
            };
            if supervisor.shutting_down.load(Ordering::SeqCst) {
                return;
            }
            if supervisor.client_connected(&owner) {
                return;
            }
            if timer_resident
                .descriptor
                .lock()
                .await
                .owner_client_id
                .as_deref()
                != Some(owner.as_str())
            {
                return;
            }
            // Skip if the worker was stopped or replaced since the arm.
            let Some(current) = supervisor.registry.get(&timer_resident.worker_id).await else {
                return;
            };
            if !Arc::ptr_eq(&current, &timer_resident) {
                return;
            }
            // Descriptor and registry reads can yield while the owner
            // reconnects. Recheck immediately before claiming the stop.
            if supervisor.client_connected(&owner) {
                return;
            }
            // A relaunch can reuse the resident Arc: the captured incarnation
            // must still match before an old disconnect may claim its stop.
            if supervisor
                .owned_worker_cleanup_identity(&timer_resident)
                .await
                != cleanup_identity
            {
                return;
            }
            if supervisor.client_connected(&owner) {
                return;
            }
            match supervisor.stop_worker(&timer_resident).await {
                Ok(()) => {
                    // Only stop_worker success proves the durable tombstone and
                    // PID/start-identity exit checks completed. Never infer from absence.
                    if let Some(identity) = cleanup_identity {
                        supervisor.record_owned_worker_cleanup(identity);
                    }
                    supervisor.log_line(&format!(
                        "stopped client-owned worker {} after its owner {owner} disconnected",
                        timer_resident.worker_id
                    ));
                }
                Err(error) => supervisor.log_line(&format!(
                    "could not clean up client-owned worker {}: {error:#}",
                    timer_resident.worker_id
                )),
            }
        });
        let previous = resident
            .owner_cleanup
            .lock()
            .unwrap()
            .replace(task.abort_handle());
        if let Some(previous) = previous {
            previous.abort();
        }
    }

    /// Handle one client command line: returns outbound lines in order and
    /// whether this client connection should stop.
    // The per-connection stream sender rides the same context bundle (lint budget +1).
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_client(
        self: &Arc<Self>,
        line: &str,
        effective_client_id: &Arc<std::sync::Mutex<String>>,
        attached: &Arc<crate::supervisor::subscribers::ClientSubscriptions>,
        roster_subscribed: &Arc<std::sync::atomic::AtomicBool>,
        connection: &Arc<crate::input_pause_lease::ClientConnectionState>,
        connection_id: &str,
        stream: &tokio::sync::mpsc::Sender<(Vec<Outbound>, bool)>,
    ) -> (Vec<Outbound>, bool) {
        let envelope = match parse_supervisor_command_line(line) {
            Ok(envelope) => envelope,
            Err(error) => {
                let id = salvage_id(line);
                // TS has two failure spellings: envelope/protocol failures answer
                // `command: "parse"`, while a known envelope holding an unknown or
                // malformed command type echoes that type.
                let salvaged_type = salvage_command_type(line);
                let type_name = if matches!(
                    error,
                    EnvelopeParseError::UnknownCommand(_) | EnvelopeParseError::Invalid(_)
                ) {
                    salvaged_type.as_deref().unwrap_or("parse")
                } else {
                    "parse"
                };
                return (
                    vec![Outbound::Line(response_line(&response_failure(
                        id.as_deref(),
                        type_name,
                        &error.to_string(),
                        None,
                    )))],
                    false,
                );
            }
        };
        let command_id = envelope.id.clone();
        // THE REQUEST'S OWN CLIENT ID, captured at parse time: the shutdown
        // attribution must name the client that SENT the shutdown, not whoever
        // spoke next (an envelope without a clientId rides the sticky id).
        let request_client_id = envelope
            .client_id
            .clone()
            .unwrap_or_else(|| effective_client_id.lock().unwrap().clone());
        if let Some(client_id) = envelope.client_id.clone() {
            *effective_client_id.lock().unwrap() = client_id;
        }
        // A prompt carrying an admissionId reserves it before dispatch (TS parse-time);
        // duplicates and empty ids answer the TS parse errors with `command: "parse"`.
        if let Some(admission_id) = crate::prompt_admission::input_admission_id(&envelope.command) {
            let active_session_id = crate::protocol::command_active_session_id(&envelope.command)
                .unwrap_or_default()
                .to_string();
            if let Err(error) = connection
                .prompt_admissions
                .register(&active_session_id, admission_id)
            {
                return (
                    vec![Outbound::Line(response_line(&response_failure(
                        Some(&command_id),
                        "parse",
                        &error,
                        None,
                    )))],
                    false,
                );
            }
        }
        let type_name = command_type_name(&envelope.command).to_string();
        if matches!(
            type_name.as_str(),
            "drone_snapshot" | "drone_recover" | "drone_status"
        ) {
            return (
                vec![Outbound::Line(response_line(&response_failure(
                    Some(&command_id),
                    &type_name,
                    "supervisor_internal_only",
                    None,
                )))],
                false,
            );
        }
        // Terminal shutdown admission gate: once `shutting_down` has flipped, no later
        // client command may reach a worker (the stop pass may already be retiring it).
        if self.shutting_down.load(Ordering::SeqCst) {
            return (
                vec![Outbound::Line(response_line(&response_failure(
                    Some(&command_id),
                    &type_name,
                    "Supervisor is shutting down",
                    None,
                )))],
                false,
            );
        }
        // Update-prepare watchdog on any later command (spec §5): an expired marker
        // returns the supervisor to Serving before the command is served.
        if let Some(abort) = self.update_prepare.abort_if_expired(util::now_ms()) {
            self.finish_update_abort(&abort);
        }
        // Admission gate: mutating commands are refused while a prepare transaction is
        // active, except the drain commands during `Draining`.
        let is_update_driver = matches!(
            &envelope.command,
            DaemonCommand::PrepareUpdateRestart { .. } | DaemonCommand::CommitUpdateRestart { .. }
        );
        if !is_update_driver {
            if let Some(state) = self.update_prepare.active_state() {
                if update_gate_refuses(state, &type_name) {
                    return (
                        vec![Outbound::Line(response_line(&response_failure(
                            Some(&command_id),
                            &type_name,
                            UPDATE_PREPARING_MESSAGE,
                            // The typed `update_restarting` info rides beside the plain message,
                            // so clients can recognize the transient state and wait through it.
                            Some(pa_types::daemon::DaemonErrorInfo::UpdateRestarting),
                        )))],
                        false,
                    );
                }
            }
        }
        // Mutating commands count against the prepare transaction's drain.
        let mutating =
            !is_update_driver && pa_types::daemon::is_daemon_mutating_command(&type_name);
        if mutating {
            self.mutation_drain.begin();
        }
        let outcome = self
            .execute_parsed_command(
                &envelope.command,
                effective_client_id,
                &request_client_id,
                attached,
                roster_subscribed,
                connection,
                connection_id,
                command_id,
                type_name,
                stream,
            )
            .await;
        if mutating {
            self.mutation_drain.end();
        }
        (
            outcome
                .0
                .into_iter()
                .map(Outbound::Line)
                .collect::<Vec<_>>(),
            outcome.1,
        )
    }
    /// The parsed-command match of [`Self::dispatch_client`], executed under
    /// the mutation-drain latch by that wrapper.
    #[allow(clippy::too_many_arguments)]
    async fn execute_parsed_command(
        self: &Arc<Self>,
        command: &DaemonCommand,
        effective_client_id: &Arc<std::sync::Mutex<String>>,
        request_client_id: &str,
        attached: &Arc<crate::supervisor::subscribers::ClientSubscriptions>,
        roster_subscribed: &Arc<std::sync::atomic::AtomicBool>,
        connection: &Arc<crate::input_pause_lease::ClientConnectionState>,
        connection_id: &str,
        command_id: String,
        type_name: String,
        stream: &tokio::sync::mpsc::Sender<(Vec<Outbound>, bool)>,
    ) -> (Vec<Value>, bool) {
        if let DaemonCommand::ControlDrone {
            active_session_id,
            root_session_id,
            action,
            expected_revision,
            rest,
            ..
        } = command
        {
            let result = async {
                anyhow::ensure!(rest.is_empty(), "drone_control_invalid_request");
                let identity = self.drone_control_identity(active_session_id).await?;
                anyhow::ensure!(
                    identity.root_session_id == *root_session_id && attached.controls(&identity),
                    "drone_control_attached_root_required"
                );
                self.control_drone(&identity, action, *expected_revision)
                    .await
            }
            .await;
            let response = match result {
                Ok(data) => response_success(Some(&command_id), &type_name, Some(data)),
                Err(error) => {
                    response_failure(Some(&command_id), &type_name, &error.to_string(), None)
                }
            };
            return (vec![response_line(&response)], false);
        }
        if let DaemonCommand::ConfigureOrchestrationGate { rest, .. } = command {
            if let Some(proof) = rest.get("privateOwnerBootstrap") {
                let _operation = self.private_bootstrap_operation.lock().await;
                let result = async {
                    anyhow::ensure!(rest.len() == 1, "Unexpected owner bootstrap wrapper");
                    let payload = self
                        .private_launch_owner
                        .lock()
                        .expect("launch lock")
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("Private owner unavailable"))?
                        .authenticate_bootstrap(proof)?;
                    self.forward_private_bootstrap(payload).await
                }
                .await;
                let mut response = match result {
                    Ok(response) => response,
                    Err(_) => response_failure(
                        Some(&command_id),
                        &type_name,
                        "Private owner bootstrap unavailable",
                        None,
                    ),
                };
                response.id = Some(command_id);
                response.command.clone_from(&type_name);
                return (vec![response_line(&response)], false);
            }
        }
        if let DaemonCommand::GetOrchestrationGate { rest, .. } = command {
            if let Some(probe) = rest.get("privateOwnerProbe") {
                let result = (|| -> Result<Value> {
                    anyhow::ensure!(rest.len() == 1, "Unexpected private probe fields");
                    self.private_launch_owner
                        .lock()
                        .expect("launch lock")
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("Private owner unavailable"))?
                        .probe(probe)
                })();
                let response = match result {
                    Ok(value) => response_success(Some(&command_id), &type_name, Some(value)),
                    Err(_) => response_failure(
                        Some(&command_id),
                        &type_name,
                        "Private owner authentication unavailable",
                        None,
                    ),
                };
                return (vec![response_line(&response)], false);
            }
        }
        match command {
            DaemonCommand::AckResult { .. } => (Vec::new(), false),
            DaemonCommand::Restart { .. } | DaemonCommand::Shutdown { .. } => {
                // WHO asked: the request's client id and command id land in the daemon log
                // the moment the drain commits, so the client that stopped the daemon is
                // nameable from the log alone. Both values are newline-stripped: a
                // client-chosen id carrying \n must not forge attribution lines.
                let logged_client = request_client_id.replace(['\n', '\r'], " ");
                let logged_command = command_id.replace(['\n', '\r'], " ");
                self.log_line(&format!(
                    "{type_name} requested by client {logged_client} (command {logged_command})"
                ));
                let response = response_success(Some(&command_id), &type_name, None);
                let mut lines = vec![response_line(&response)];
                // daemon_closing goes to every client before the exit.
                let closing = daemon_closing_shutdown_event();
                let _ = self.events.send((
                    ClientRouting::BroadcastExcept {
                        connection_id: connection_id.to_string(),
                    },
                    std::sync::Arc::new(closing.clone()),
                ));
                lines.push(closing);
                // Answer first, then shut down: the client receives the response and
                // daemon_closing before the stop pass can end the process. The gate flips
                // synchronously here, so no create dispatched after the shutdown can slip past it.
                *self.shutdown_owner.lock().unwrap() = Some(connection_id.to_string());
                self.shutting_down.store(true, Ordering::SeqCst);
                (lines, true)
            }
            DaemonCommand::List {
                all,
                cwd,
                session_dir,
                ..
            } => {
                let response = self
                    .handle_list(
                        command_id,
                        type_name,
                        *all,
                        cwd.clone(),
                        session_dir.clone(),
                    )
                    .await;
                (vec![response_line(&response)], false)
            }
            DaemonCommand::ListSavedSessions { .. } => {
                let lines = self
                    .handle_saved_session_list(command, &command_id, stream)
                    .await;
                (lines, false)
            }
            DaemonCommand::RosterSubscribe { .. } => {
                roster_subscribed.store(true, std::sync::atomic::Ordering::SeqCst);
                let response = self.handle_roster_subscribe(&command_id, &type_name).await;
                (vec![response_line(&response)], false)
            }
            DaemonCommand::RosterUnsubscribe { .. } => {
                roster_subscribed.store(false, std::sync::atomic::Ordering::SeqCst);
                let response = Self::handle_roster_unsubscribe(&command_id, &type_name);
                (vec![response_line(&response)], false)
            }
            DaemonCommand::WorkerIdlePassivation {
                worker_token,
                idle_minutes,
                ..
            } => {
                let response = self
                    .handle_worker_idle_passivation(
                        &command_id,
                        &type_name,
                        worker_token,
                        *idle_minutes,
                    )
                    .await;
                (vec![response_line(&response)], false)
            }
            DaemonCommand::WorkerRosterDelta {
                worker_token,
                summary,
                removed,
                sequence,
                worker_instance_id,
                ..
            } => {
                let response = self
                    .handle_worker_roster_delta(
                        &command_id,
                        &type_name,
                        crate::supervisor_roster::WorkerRosterDelta {
                            worker_token: worker_token.clone(),
                            summary: summary.clone(),
                            removed: removed.clone().unwrap_or_default(),
                            sequence: *sequence,
                            worker_instance_id: worker_instance_id.clone(),
                        },
                    )
                    .await;
                (vec![response_line(&response)], false)
            }
            DaemonCommand::Create { .. } => {
                let client_id = effective_client_id.lock().unwrap().clone();
                match self.handle_create(command, client_id).await {
                    Ok(summary) => (
                        vec![response_line(&response_success(
                            Some(&command_id),
                            &type_name,
                            Some(summary),
                        ))],
                        false,
                    ),
                    Err(error) => {
                        // A typed worker rejection carries its wire info to the client.
                        let (message, error_info) =
                            match error.downcast_ref::<TypedCreateRejection>() {
                                Some(rejection) => (
                                    rejection.message.clone(),
                                    Some(rejection.error_info.clone()),
                                ),
                                None => (error.to_string(), None),
                            };
                        (
                            vec![response_line(&response_failure(
                                Some(&command_id),
                                &type_name,
                                &message,
                                error_info,
                            ))],
                            false,
                        )
                    }
                }
            }
            DaemonCommand::GetDirectWorkerTransport {
                active_session_id,
                rest,
                ..
            } => {
                // Direct-attach ticket: a single-use grant for a registered session, handed to
                // the client with the worker's own socket; it stays out of the streaming path.
                let Ok(scope) = rest
                    .get("controlScope")
                    .cloned()
                    .map(serde_json::from_value::<pa_types::daemon::DaemonSessionControlScope>)
                    .transpose()
                else {
                    return (
                        vec![response_line(&response_failure(
                            Some(&command_id),
                            &type_name,
                            "invalid_control_scope",
                            None,
                        ))],
                        false,
                    );
                };
                let response = self
                    .handle_get_direct_worker_transport(
                        &command_id,
                        &type_name,
                        active_session_id,
                        scope.as_ref(),
                    )
                    .await;
                if response.success && scope.is_none() {
                    if let Some(data) = response.data.as_ref() {
                        if data["purpose"] == "session_client" {
                            if let (Some(active), Some(instance)) = (
                                data["activeSessionId"].as_str(),
                                data["workerInstanceId"].as_str(),
                            ) {
                                if let Ok(identity) = self.drone_control_identity(active).await {
                                    if identity.worker_instance_id == instance {
                                        attached.register_control_attachment(identity);
                                    }
                                }
                            }
                        }
                    }
                }
                (vec![response_line(&response)], false)
            }
            DaemonCommand::SendMessage { .. } => {
                let client_id = effective_client_id.lock().unwrap().clone();
                let response = self
                    .handle_send_message(&command_id, &client_id, command)
                    .await;
                (vec![response_line(&response)], false)
            }
            DaemonCommand::GetWorkerPeerTransport {
                worker_token,
                target_active_session_id,
                ..
            } => {
                // Worker-to-worker peer ticket: a single-use `worker` grant pushed into
                // the target worker's memory, so the delivery bypasses this route plane.
                let response = self
                    .handle_get_worker_peer_transport(
                        &command_id,
                        &type_name,
                        worker_token,
                        target_active_session_id,
                    )
                    .await;
                (vec![response_line(&response)], false)
            }
            DaemonCommand::WorkerRegister { .. } => {
                // Worker self-registration: rebuilds the roster entry from the worker's
                // own identity instead of routing to a session.
                let response = self
                    .handle_worker_register(&command_id, &type_name, command)
                    .await;
                (vec![response_line(&response)], false)
            }
            DaemonCommand::CommitUpdateRestart { .. } => {
                // The coordinator's commit (spec §5 `Prepared -> Stopping`): consume the
                // prepared transaction, stop the workers in budget, and exit or abandon it.
                self.handle_commit_update_restart(&command_id, &type_name, command)
                    .await
            }
            DaemonCommand::PrepareUpdateRestart { .. } => {
                // The update-flow coordinator's prepare RPC: accepts (or idempotently
                // polls) the supervisor-side prepare transaction (spec §5).
                let response = self
                    .handle_prepare_update_restart(&command_id, &type_name, command)
                    .await;
                (vec![response_line(&response)], false)
            }
            DaemonCommand::UpdateRestoreStatus { .. } => {
                // The boot restore pass's live snapshot (spec §6/§9): the successor
                // coordinator's `Restoring` report polls this for real counts and failures.
                let data = self.restore_status_body();
                (
                    vec![response_line(&response_success(
                        Some(&command_id),
                        &type_name,
                        Some(data),
                    ))],
                    false,
                )
            }
            DaemonCommand::ListPersistentShells { .. } => {
                // The machine-wide persistent shells, each with its assigned session id.
                let agent_dir = self.options.agent_dir.clone();
                let line = crate::persistent_shells::respond(&command_id, &type_name, move || {
                    Ok(Some(
                        json!({ "activities": crate::persistent_shells::shell_rows(&agent_dir) }),
                    ))
                })
                .await;
                (vec![line], false)
            }
            DaemonCommand::TailPersistentShell {
                shell_id, lines, ..
            } => {
                let agent_dir = self.options.agent_dir.clone();
                let (shell_id, lines) = (shell_id.clone(), *lines as usize);
                let line = crate::persistent_shells::respond(&command_id, &type_name, move || {
                    crate::persistent_shells::output_tail(&agent_dir, &shell_id, lines)
                        .map(|tail| tail.map(|tail| json!({ "shellId": shell_id, "tail": tail })))
                })
                .await;
                (vec![line], false)
            }
            DaemonCommand::KillPersistentShell { shell_id, .. } => {
                let agent_dir = self.options.agent_dir.clone();
                let shell_id = shell_id.clone();
                let line = crate::persistent_shells::respond(&command_id, &type_name, move || {
                    crate::persistent_shells::kill_shell(&agent_dir, &shell_id)
                        .map(|known| known.then(|| json!({ "shellId": shell_id })))
                })
                .await;
                (vec![line], false)
            }
            DaemonCommand::Prompt {
                active_session_id, ..
            }
            | DaemonCommand::PromptAndWait {
                active_session_id, ..
            } if input_admission_id(command).is_some_and(|id| !id.is_empty()) => {
                // An admitted prompt: the cancellation checks, the admission-id rewrite,
                // and the owned commit around the routed prompt.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.route_prompt_with_admission(
                    connection,
                    command,
                    &client_id,
                    attached,
                    command_id,
                    type_name,
                    active_session_id,
                )
                .await
            }
            DaemonCommand::CancelPromptAdmission { .. } => {
                // The supervisor's status ladder over the admission registry.
                self.handle_cancel_prompt_admission(connection, command, &command_id, &type_name)
                    .await
            }
            DaemonCommand::GetOwnedWorkerCleanup { .. } => {
                self.handle_get_owned_worker_cleanup(command, &command_id, &type_name)
                    .await
            }
            DaemonCommand::CompleteOwnedSession { .. } => {
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_complete_owned_session(command, &client_id, &command_id, &type_name)
                    .await
            }
            DaemonCommand::PromoteOwnedSession { .. } => {
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_promote_owned_session(command, &client_id, &command_id, &type_name)
                    .await
            }
            DaemonCommand::RetryWorker { .. } => {
                // The recovery is a supervisor arm — the worker never sees the command.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_retry_worker(command, &client_id, &command_id, &type_name)
                    .await
            }
            DaemonCommand::AbortCompaction { .. } => {
                // The supervisor answers the abort itself: a wedged worker must not turn
                // the abort into its own 30s route timeout and a loader that never clears.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_abort_compaction(command, &client_id, attached, &command_id, &type_name)
                    .await
            }
            DaemonCommand::AcquireSessionInputPause { .. } => {
                // The supervisor-owned lease path: resolve, rewrite the lease key, forward, record.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_acquire_session_input_pause(
                    connection,
                    command,
                    &client_id,
                    &command_id,
                    &type_name,
                )
                .await
            }
            DaemonCommand::ReleaseSessionInputPause { .. } => {
                self.handle_release_session_input_pause(
                    connection,
                    command,
                    &command_id,
                    &type_name,
                )
                .await
            }
            DaemonCommand::Detach {
                active_session_id, ..
            } => {
                // Detach carries the pause-lease bookkeeping: mark the detaching sessions and
                // bump the epoch BEFORE the routed detach, then release the client's leases.
                let client_id = effective_client_id.lock().unwrap().clone();
                let attached_ids = attached.session_ids();
                let marked = Self::begin_detach_pause_bookkeeping(
                    connection,
                    active_session_id.as_deref(),
                    &attached_ids,
                );
                let outcome = self
                    .route_client_command(
                        command,
                        &client_id,
                        attached,
                        command_id.clone(),
                        type_name.clone(),
                        Some(stream),
                    )
                    .await;
                // A selector that resolves to nothing detaches nothing and still answers success.
                if outcome.0.first().is_some_and(|line| {
                    line.get("success").and_then(Value::as_bool) == Some(false)
                        && line
                            .get("error")
                            .and_then(Value::as_str)
                            .is_some_and(|error| error.starts_with("Unknown active session:"))
                }) {
                    return (
                        vec![response_line(&response_success(
                            Some(&command_id),
                            &type_name,
                            None,
                        ))],
                        false,
                    );
                }
                let succeeded = outcome
                    .0
                    .first()
                    .is_some_and(|line| line.get("success").and_then(Value::as_bool) == Some(true));
                if succeeded {
                    self.release_client_pauses_for_sessions(connection, &marked)
                        .await;
                }
                outcome
            }
            DaemonCommand::Reattach {
                active_session_id,
                target_active_session_id,
                ..
            } => {
                // Reattach clears the detach marks for the reattached sessions (TS
                // reattach arm): a reattached session may acquire pauses again.
                let client_id = effective_client_id.lock().unwrap().clone();
                let outcome = self
                    .route_client_command(
                        command,
                        &client_id,
                        attached,
                        command_id,
                        type_name,
                        Some(stream),
                    )
                    .await;
                let mut cleared = vec![active_session_id.clone(), target_active_session_id.clone()];
                if let Ok(resident) = self.registry.resolve(target_active_session_id).await {
                    cleared.push(resident.worker_id.clone());
                }
                Self::clear_detaching_after_reattach(connection, &cleared);
                outcome
            }
            DaemonCommand::AgentMessagesStatus {
                active_session_id, ..
            } if active_session_id.is_none() => {
                // Selector-less `agent_messages_status`: the first live worker answers,
                // else the TS empty-status object.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_agent_messages_status_broadcast(
                    command,
                    &client_id,
                    &command_id,
                    &type_name,
                )
                .await
            }
            DaemonCommand::ListAgentPeers { .. } => {
                // `list_agent_peers`: the worker-token-authenticated peer roster.
                self.handle_list_agent_peers(command, &command_id, &type_name)
                    .await
            }
            DaemonCommand::RenameSavedSession { .. } => {
                // `rename_saved_session`: reservation ladder, then catalog rename or worker route.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_rename_saved_session(
                    command,
                    &client_id,
                    attached,
                    &command_id,
                    &type_name,
                )
                .await
            }
            DaemonCommand::DeleteSavedSession {
                active_session_id, ..
            } if active_session_id.is_none() => {
                // Selector-less `delete_saved_session`: the supervisor's catalog delete.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_delete_saved_session(command, &client_id, &command_id, &type_name)
                    .await
            }
            DaemonCommand::CronList {
                active_session_id, ..
            } if active_session_id.is_none() => {
                // Selector-less `cron_list`: merge the live workers' jobs with the passive ones.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_cron_list_catalog(command, &client_id, &command_id, &type_name)
                    .await
            }
            DaemonCommand::HeartbeatsList {
                active_session_id, ..
            } if active_session_id.is_none() => {
                // Selector-less `heartbeats_list`: the merged heartbeat catalog.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_heartbeats_list_catalog(command, &client_id, &command_id, &type_name)
                    .await
            }
            DaemonCommand::CronCancel {
                active_session_id, ..
            } if active_session_id.is_none() => {
                // Selector-less `cron_cancel`: the owner-worker search, then the passive
                // store, then the TS error.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_cron_cancel_catalog(command, &client_id, &command_id, &type_name)
                    .await
            }
            DaemonCommand::HeartbeatManage { .. } => {
                // `heartbeat_manage`: passive jobs are managed against their durable store,
                // live ones route to their worker.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_heartbeat_manage_catalog(
                    command,
                    &client_id,
                    attached,
                    &command_id,
                    &type_name,
                )
                .await
            }
            DaemonCommand::CronAdd { .. } => {
                // `cron_add`: the routed add plus the ownership promotion the command may
                // ask for.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_cron_add_catalog(command, &client_id, attached, &command_id, &type_name)
                    .await
            }
            DaemonCommand::HeartbeatSet { .. } => {
                // `heartbeat_set`: the same forward-and-promote path as `cron_add`.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_heartbeat_set_catalog(
                    command,
                    &client_id,
                    attached,
                    &command_id,
                    &type_name,
                )
                .await
            }
            DaemonCommand::AgentMessagesPause {
                active_session_id, ..
            }
            | DaemonCommand::AgentMessagesResume {
                active_session_id, ..
            } if active_session_id.is_none() => {
                // Selector-less pause/resume: the broadcast to every live worker.
                let client_id = effective_client_id.lock().unwrap().clone();
                self.handle_agent_messages_pause_resume_broadcast(
                    command,
                    &client_id,
                    &command_id,
                    &type_name,
                )
                .await
            }
            command => {
                let client_id = effective_client_id.lock().unwrap().clone();
                self.route_client_command(
                    command,
                    &client_id,
                    attached,
                    command_id,
                    type_name,
                    Some(stream),
                )
                .await
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    #[cfg(unix)]
    use super::*;
    #[cfg(unix)]
    use crate::supervisor::SupervisorOptions;
    #[cfg(unix)]
    use pa_types::daemon::DaemonWorkerDescriptor;
    #[cfg(unix)]
    use pa_types::platform::transport::TransportStream;
    #[cfg(unix)]
    use serde_json::json;
    #[cfg(unix)]
    use std::sync::Arc;
    use std::time::Duration;

    /// A response-write failure can retire the serve loop while an accepted
    /// shutdown task has not claimed its owner yet. Keep the receiver alive
    /// through that cleanup window, then release the real shutdown dispatch.
    #[tokio::test]
    async fn a_late_shutdown_cannot_send_into_a_retired_client_queue() {
        let dir = tempfile::TempDir::new().unwrap();
        let supervisor = Arc::new(
            Supervisor::new(SupervisorOptions {
                socket_path: dir.path().join("daemon.sock"),
                agent_dir: dir.path().join("agent"),
            })
            .expect("supervisor"),
        );
        let (dispatch_tx, mut dispatch_rx) = tokio::sync::mpsc::channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let late_dispatch = {
            let supervisor = Arc::clone(&supervisor);
            tokio::spawn(async move {
                release_rx.await.expect("release the in-flight dispatch");
                let effective = Arc::new(std::sync::Mutex::new("shutdown-client".to_string()));
                let (targeted_tx, _targeted_rx) = tokio::sync::mpsc::channel(1);
                let attached = subscribers::ClientSubscriptions::new(
                    "shutdown-client".to_string(),
                    targeted_tx,
                );
                let roster = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let connection = Arc::new(crate::input_pause_lease::ClientConnectionState::new());
                let command = json!({
                    "type": "command", "id": "late-stop",
                    "protocol": { "name": "prime-agent.daemon", "version": 7 },
                    "command": { "type": "shutdown", "force": true }
                })
                .to_string();
                let (lines, stop) = supervisor
                    .dispatch_client(
                        &command,
                        &effective,
                        &attached,
                        &roster,
                        &connection,
                        "shutdown-client",
                        &dispatch_tx,
                    )
                    .await;
                assert!(stop, "the real shutdown dispatch accepted the stop");
                let rejected = dispatch_tx.send((lines, stop)).await.is_err();
                if rejected {
                    supervisor.start_shutdown_if_owner("shutdown-client").await;
                }
                rejected
            })
        };
        assert!(supervisor.shutdown_owner.lock().unwrap().is_none());
        supervisor
            .retire_client_dispatches(&mut dispatch_rx, "shutdown-client")
            .await;
        assert!(!supervisor.accept_exit.load(Ordering::SeqCst));
        release_tx.send(()).expect("resume the in-flight shutdown");
        let rejected = tokio::time::timeout(Duration::from_secs(5), late_dispatch)
            .await
            .expect("late dispatch settles")
            .expect("late dispatch task");
        assert!(
            rejected,
            "a retired response queue accepted the shutdown bundle"
        );
        assert!(supervisor.shutting_down.load(Ordering::SeqCst));
        assert!(supervisor.shutdown_started.load(Ordering::SeqCst));
        assert!(
            supervisor.accept_exit.load(Ordering::SeqCst),
            "the failed-send fallback completed the stop pass"
        );
        // The caller still owns the receiver during this cleanup window.
        assert!(dispatch_rx.recv().await.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_shutdown_request_logs_its_client() {
        let dir = tempfile::TempDir::new().unwrap();
        let options = SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        };
        let log_path = crate::paths::daemon_log_path(&options.socket_path, &options.agent_dir);
        let supervisor = Arc::new(Supervisor::new(options).expect("supervisor"));
        let (server_side, client_side) = tokio::net::UnixStream::pair().expect("socket pair");
        let connection = {
            let supervisor = Arc::clone(&supervisor);
            let stream: Box<dyn TransportStream> = Box::new(server_side);
            tokio::spawn(async move { supervisor.handle_client(stream).await })
        };
        // The greeting arrives before the loop reads: consume it first.
        let (client_read, mut client_write) = client_side.into_split();
        let mut client = BufReader::new(client_read);
        let mut hello = String::new();
        client.read_line(&mut hello).await.expect("hello line");
        assert!(
            hello.contains("\"type\":\"daemon_hello\""),
            "the greeting: {hello}"
        );
        let envelope = json!({
            "type": "command",
            "id": "installer-stop",
            "protocol": {"name": "prime-agent.daemon", "version": 7},
            "clientId": "install-rust-sh",
            "command": {"type": "shutdown", "force": true, "id": "installer-stop"},
        });
        client_write
            .write_all((serde_json::to_string(&envelope).unwrap() + "\n").as_bytes())
            .await
            .expect("send the shutdown envelope");
        // The drain commits synchronously with the log line, so the log is the wait
        // point; the response and daemon_closing follow on their own schedule.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let log = loop {
            let log = std::fs::read_to_string(&log_path).unwrap_or_default();
            if log.contains("shutdown requested by client") {
                break log;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the shutdown request was never logged; log: {log}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(
            log.contains("shutdown requested by client install-rust-sh (command installer-stop)"),
            "the log names the requesting client and its command: {log}"
        );
        connection.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_lagged_client_event_stream_is_logged() {
        use tokio::io::AsyncReadExt as _;
        let dir = tempfile::TempDir::new().unwrap();
        let options = SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        };
        let log_path = crate::paths::daemon_log_path(&options.socket_path, &options.agent_dir);
        let supervisor = Arc::new(Supervisor::new(options).expect("supervisor"));
        let (server_side, client_side) = tokio::net::UnixStream::pair().expect("socket pair");
        // The write half stays held so the connection's writes fail only when the test ends.
        let (client_read, _client_write) = client_side.into_split();
        let connection = {
            let supervisor = Arc::clone(&supervisor);
            let stream: Box<dyn TransportStream> = Box::new(server_side);
            tokio::spawn(async move { supervisor.handle_client(stream).await })
        };
        // The handshake greeting arrives before the loop's first poll.
        let mut client = BufReader::new(client_read);
        let mut hello = String::new();
        client.read_line(&mut hello).await.expect("hello line");
        assert!(
            hello.contains("\"type\":\"daemon_hello\""),
            "the greeting: {hello}"
        );
        // The greeting is written BEFORE the loop subscribes to the event ring, so the
        // flood waits for the subscription — sends into a receiver-less ring are dropped.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while supervisor.events.receiver_count() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the connection loop never subscribed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Flood the ring past its capacity with frames too big for the client's socket
        // buffer: the loop parks in its event write and its receiver falls out of the
        let capacity = crate::backpressure::EVENT_RING_CAPACITY;
        let padding = "x".repeat(2048);
        let flood = capacity + 2048;
        for index in 0..flood {
            let _ = supervisor.events.send((
                ClientRouting::Broadcast,
                std::sync::Arc::new(json!({
                    "type": "session_event", "index": index, "padding": padding
                })),
            ));
        }
        // Drain the parked connection while watching for the log line: the loop unparks
        // as the reader frees the buffer, and its next event read reports the dropped span.
        let mut buffer = vec![0u8; 64 * 1024];
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let log = loop {
            let log = std::fs::read_to_string(&log_path).unwrap_or_default();
            if log.contains("lagged on the event ring") {
                break log;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the lagged drain was never logged; log: {log}"
            );
            match tokio::time::timeout(Duration::from_millis(150), client.read(&mut buffer)).await {
                Ok(Ok(_) | Err(_)) => {}
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        };
        let line = log
            .lines()
            .rev()
            .find(|line| line.contains("lagged on the event ring"))
            .expect("the lag line");
        assert!(
            line.contains("events dropped"),
            "the log names the dropped count: {line}"
        );
        connection.abort();
    }

    // One real connection speaking for `client_id`, driven through its
    // first response so the envelope id is the connection's effective
    // id before it closes. "Closed" = drop the write half (EOF) and
    // await the connection task, which runs the disconnect cleanup.
    async fn connect_as(
        supervisor: &Arc<Supervisor>,
        client_id: &str,
    ) -> (
        tokio::task::JoinHandle<Result<()>>,
        tokio::net::unix::OwnedWriteHalf,
    ) {
        let (server_side, client_side) = tokio::net::UnixStream::pair().expect("socket pair");
        let connection = {
            let supervisor = Arc::clone(supervisor);
            let stream: Box<dyn TransportStream> = Box::new(server_side);
            tokio::spawn(async move { supervisor.handle_client(stream).await })
        };
        let (client_read, mut client_write) = client_side.into_split();
        let mut client = BufReader::new(client_read);
        let mut hello = String::new();
        client.read_line(&mut hello).await.expect("hello line");
        let envelope = json!({
            "type": "command",
            "id": format!("{client_id}-command"),
            "protocol": {"name": "prime-agent.daemon", "version": 7},
            "clientId": client_id,
            "command": {"type": "roster_unsubscribe", "id": format!("{client_id}-command")},
        });
        client_write
            .write_all((serde_json::to_string(&envelope).unwrap() + "\n").as_bytes())
            .await
            .expect("send the id envelope");
        let mut response = String::new();
        client.read_line(&mut response).await.expect("the response");
        assert!(
            response.contains("\"success\":true"),
            "the roster_unsubscribe response: {response}"
        );
        (connection, client_write)
    }

    /// Reconnecting while an expired timer waits on the descriptor must
    /// prevent a stop. The first connectivity check already happened when
    /// the slot clears, but the descriptor read can yield to a new client.
    #[cfg(unix)]
    #[tokio::test]
    async fn reconnect_during_expired_cleanup_keeps_owned_worker() {
        let dir = tempfile::TempDir::new().unwrap();
        let supervisor = Arc::new(
            Supervisor::new(SupervisorOptions {
                socket_path: dir.path().join("daemon.sock"),
                agent_dir: dir.path().join("agent"),
            })
            .expect("supervisor"),
        );
        let descriptor: DaemonWorkerDescriptor = serde_json::from_value(json!({
            "version": 2,
            "workerId": "w-reconnect",
            "pid": 0,
            "socketPath": "/tmp/none.sock",
            "recoveryJournalPath": "/tmp/none.jsonl",
            "supervisorSocketPath": "/tmp/none.sock",
            "authenticationToken": "token",
            "rootActiveSessionId": "w-reconnect",
            "ownerClientId": "acp:reconnect",
            "createdAt": "t",
            "updatedAt": "t",
            "lifecycle": "ready",
            "createCommand": {},
            "consecutiveFailures": 0,
        }))
        .expect("descriptor");
        let resident = ResidentWorker::new(
            "w-reconnect".to_string(),
            descriptor,
            dir.path().join("w-reconnect.json"),
        );
        supervisor.registry.insert(Arc::clone(&resident)).await;
        let (first, first_write) = connect_as(&supervisor, "acp:reconnect").await;
        drop(first_write);
        first.await.expect("first connection").unwrap();
        assert!(resident.owner_cleanup.lock().unwrap().is_some());

        // Hold the descriptor AFTER arming, while the expired timer passes
        // its first client_connected check and waits to read ownership.
        let guard = resident.descriptor.lock().await;
        tokio::time::pause();
        tokio::time::advance(OWNED_WORKER_DISCONNECT_GRACE + Duration::from_secs(1)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            resident.owner_cleanup.lock().unwrap().is_none(),
            "timer expired"
        );
        let (reconnected, reconnected_write) = connect_as(&supervisor, "acp:reconnect").await;
        drop(guard);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            supervisor.registry.get("w-reconnect").await.is_some(),
            "reconnected owner keeps worker even when old timer expired"
        );
        assert!(
            resident.descriptor.lock().await.stop_requested_at.is_none(),
            "reconnect must veto the stop tombstone"
        );
        drop(reconnected_write);
        reconnected.await.expect("reconnected connection").unwrap();
    }

    /// A client-owned worker stops 30 seconds after its owner's LAST
    /// connection closes (the TS `scheduleOwnedWorkerCleanup` port): a
    /// second connection of the same client id (one process, several
    /// connections) keeps the worker, an owner that reconnects inside
    /// the grace keeps it, and the stop waits out the full grace instead
    /// of firing at the disconnect.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_client_owned_worker_stops_after_its_owner_s_last_connection_closes() {
        let dir = tempfile::TempDir::new().unwrap();
        let supervisor = Arc::new(
            Supervisor::new(SupervisorOptions {
                socket_path: dir.path().join("daemon.sock"),
                agent_dir: dir.path().join("agent"),
            })
            .expect("supervisor"),
        );
        let descriptor: DaemonWorkerDescriptor = serde_json::from_value(json!({
            "version": 2,
            "workerId": "w-owned",
            "pid": 0,
            "socketPath": "/tmp/none.sock",
            "recoveryJournalPath": "/tmp/none.jsonl",
            "supervisorSocketPath": "/tmp/none.sock",
            "authenticationToken": "token",
            "rootActiveSessionId": "w-owned",
            "ownerClientId": "acp:1",
            "createdAt": "t",
            "updatedAt": "t",
            "lifecycle": "ready",
            "createCommand": {},
            "consecutiveFailures": 0,
        }))
        .expect("descriptor");
        let resident = ResidentWorker::new(
            "w-owned".to_string(),
            descriptor,
            dir.path().join("w-owned.json"),
        );
        supervisor.registry.insert(Arc::clone(&resident)).await;

        // Phase 1: two connections speak for acp:1; closing one must leave
        // the worker alone while the other still holds the id.
        let (a, a_write) = connect_as(&supervisor, "acp:1").await;
        let (b, b_write) = connect_as(&supervisor, "acp:1").await;
        drop(a_write);
        a.await.expect("connection a's task").unwrap();
        // Phase 2: the last connection closes -> the grace timer is armed.
        drop(b_write);
        b.await.expect("connection b's task").unwrap();
        // Phase 3: the owner reconnects well inside the grace.
        let (c, c_write) = connect_as(&supervisor, "acp:1").await;
        // Phase 4: the timer expires with the reconnected owner live, so
        // it must leave the worker alone.
        tokio::time::pause();
        tokio::time::advance(OWNED_WORKER_DISCONNECT_GRACE + Duration::from_secs(1)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            resident.owner_cleanup.lock().unwrap().is_none(),
            "the expiry ran"
        );
        assert!(
            supervisor.registry.get("w-owned").await.is_some(),
            "the reconnecting owner keeps its worker"
        );
        // Phase 5: the last connection closes with no timer pending, so a
        // fresh grace runs: the worker keeps running a grace-minus-a-
        // second past the disconnect, then stops.
        drop(c_write);
        c.await.expect("connection c's task").unwrap();
        tokio::time::advance(OWNED_WORKER_DISCONNECT_GRACE.saturating_sub(Duration::from_secs(1)))
            .await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            supervisor.registry.get("w-owned").await.is_some(),
            "the worker keeps running inside the grace"
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        let stopped = tokio::time::timeout(Duration::from_secs(5), async {
            while supervisor.registry.get("w-owned").await.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(stopped.is_ok(), "the owned worker was never stopped");
    }
}

#[cfg(test)]
mod owned_cleanup_tests {
    use super::*;

    async fn fixture() -> (tempfile::TempDir, Arc<Supervisor>, Arc<ResidentWorker>) {
        let dir = tempfile::tempdir().unwrap();
        let supervisor = Arc::new(
            Supervisor::new(super::super::SupervisorOptions {
                socket_path: dir.path().join("daemon.sock"),
                agent_dir: dir.path().join("agent"),
            })
            .unwrap(),
        );
        let descriptor = serde_json::from_value(json!({
            "version":2,"workerId":"w-a","workerInstanceId":"i-a","pid":0,
            "socketPath":dir.path().join("missing.sock"), "recoveryJournalPath":dir.path().join("recovery"),
            "supervisorSocketPath":dir.path().join("daemon.sock"),"authenticationToken":"test",
            "rootActiveSessionId":"w-a","ownerClientId":"old-owner",
            "createdAt":"2026-10-17T00:00:00Z","updatedAt":"2026-10-17T00:00:00Z",
            "lifecycle":"ready","createCommand":{},"consecutiveFailures":0
        })).unwrap();
        let resident = ResidentWorker::new("w-a".into(), descriptor, dir.path().join("w-a.json"));
        supervisor.registry.insert(Arc::clone(&resident)).await;
        (dir, supervisor, resident)
    }

    #[tokio::test(start_paused = true)]
    async fn late_disconnect_cannot_stop_reowned_or_relaunched_resident() {
        for replace_instance in [false, true] {
            let (_dir, supervisor, resident) = fixture().await;
            supervisor.schedule_owned_worker_cleanup(&resident).await;
            {
                let mut descriptor = resident.descriptor.lock().await;
                if replace_instance {
                    descriptor.worker_instance_id = Some("i-new".into());
                } else {
                    descriptor.owner_client_id = Some("new-owner".into());
                }
            }
            tokio::task::yield_now().await;
            tokio::time::advance(OWNED_WORKER_DISCONNECT_GRACE).await;
            tokio::task::yield_now().await;
            assert!(resident.descriptor.lock().await.stop_requested_at.is_none());
            assert!(supervisor.owner_cleanup_receipts.lock().unwrap().is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn owner_reconnect_cancels_cleanup_without_a_receipt() {
        let (_dir, supervisor, resident) = fixture().await;
        supervisor.schedule_owned_worker_cleanup(&resident).await;
        supervisor.client_connections.lock().unwrap().insert(
            "reconnected".into(),
            Arc::new(std::sync::Mutex::new("old-owner".into())),
        );
        tokio::task::yield_now().await;
        tokio::time::advance(OWNED_WORKER_DISCONNECT_GRACE).await;
        tokio::task::yield_now().await;
        assert!(resident.descriptor.lock().await.stop_requested_at.is_none());
        assert!(supervisor.owner_cleanup_receipts.lock().unwrap().is_empty());
        assert!(!resident.owner_cleanup_running.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn disconnect_stop_publishes_only_after_tombstone_and_verified_exit() {
        let (_dir, supervisor, resident) = fixture().await;
        supervisor.schedule_owned_worker_cleanup(&resident).await;
        assert!(supervisor.owner_cleanup_receipts.lock().unwrap().is_empty());
        tokio::task::yield_now().await;
        tokio::time::advance(OWNED_WORKER_DISCONNECT_GRACE).await;
        let notified = supervisor.owner_cleanup_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if supervisor.owner_cleanup_receipts.lock().unwrap().is_empty() {
            tokio::time::timeout(Duration::from_secs(60), notified)
                .await
                .unwrap();
        }
        assert!(resident.descriptor.lock().await.stop_requested_at.is_some());
        assert!(supervisor.registry.get("w-a").await.is_none());
        assert_eq!(supervisor.owner_cleanup_receipts.lock().unwrap().len(), 1);
    }
}
