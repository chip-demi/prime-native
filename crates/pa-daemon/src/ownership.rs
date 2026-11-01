//! The owned-session and worker-recovery surface: `complete_owned_session`
//! stops the owned worker, `promote_owned_session` clears the owner so the
//! session outlives its creating client, and `retry_worker` is the
//! supervisor's manual recovery trigger (the worker never sees it).

use std::sync::Arc;

use serde_json::{json, Value};

use pa_types::daemon::{DaemonCommand, DaemonWorkerLifecycle};

use crate::backpressure::RouteAdmission;
use crate::descriptor::persist_worker;
use crate::protocol::{response_failure, response_line, response_success};
use crate::registry::ResidentWorker;
use crate::supervisor::{Supervisor, ROUTE_TIMEOUT_MS};

/// Exact create-time identity. Not a stop permit; readers never mutate a worker.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OwnedWorkerCleanupIdentity {
    pub active_session_id: String,
    pub worker_id: String,
    pub worker_instance_id: String,
    pub owner_client_id: String,
    pub supervisor_generation: String,
}

/// Lifecycle promotion retains the original trusted supervisor client as controller.
/// This is not a user identity or a public peer grant.
pub(crate) fn session_control_authorized(
    descriptor: &pa_types::daemon::DaemonWorkerDescriptor,
    client_id: &str,
) -> bool {
    if client_id.is_empty() {
        return false;
    }
    match descriptor.owner_client_id.as_deref() {
        Some(owner) => owner == client_id,
        None => {
            descriptor
                .rest
                .get("promotedOwnerClientId")
                .and_then(Value::as_str)
                == Some(client_id)
        }
    }
}

impl Supervisor {
    pub(crate) async fn owned_worker_cleanup_identity(
        &self,
        resident: &Arc<ResidentWorker>,
    ) -> Option<OwnedWorkerCleanupIdentity> {
        let descriptor = resident.descriptor.lock().await;
        Some(OwnedWorkerCleanupIdentity {
            active_session_id: descriptor.root_active_session_id.clone(),
            worker_id: resident.worker_id.clone(),
            worker_instance_id: descriptor.worker_instance_id.clone()?,
            owner_client_id: descriptor.owner_client_id.clone()?,
            supervisor_generation: self.owner_cleanup_generation.clone(),
        })
    }

    pub(crate) fn record_owned_worker_cleanup(&self, identity: OwnedWorkerCleanupIdentity) {
        let mut receipts = self.owner_cleanup_receipts.lock().unwrap();
        // Bounded process-local acknowledgements, never a durable lifecycle database.
        if receipts.len() == 1024 {
            receipts.pop_front();
        }
        receipts.retain(|(existing, _)| existing != &identity);
        receipts.push_back((identity, true));
        drop(receipts);
        self.owner_cleanup_notify.notify_waiters();
    }

    async fn owned_worker_cleanup_status(
        &self,
        identity: &OwnedWorkerCleanupIdentity,
    ) -> &'static str {
        if identity.supervisor_generation != self.owner_cleanup_generation {
            return "unverified";
        }
        if let Some((_, verified)) = self
            .owner_cleanup_receipts
            .lock()
            .unwrap()
            .iter()
            .find(|(existing, _)| existing == identity)
        {
            return if *verified {
                "verified_stopped"
            } else {
                "pending"
            };
        }
        let Some(resident) = self.registry.get(&identity.worker_id).await else {
            return "unverified";
        };
        if self.owned_worker_cleanup_identity(&resident).await.as_ref() != Some(identity) {
            return "unverified";
        }
        if resident.owner_cleanup.lock().unwrap().is_some()
            || resident
                .owner_cleanup_running
                .load(std::sync::atomic::Ordering::SeqCst)
            || resident
                .intentional_stop
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            "pending"
        } else {
            "unverified"
        }
    }

    /// Read-only observation of the existing disconnect cleanup. In particular,
    /// absence is not proof and selectors never follow durable-session rebindings.
    pub(crate) async fn handle_get_owned_worker_cleanup(
        &self,
        command: &DaemonCommand,
        command_id: &str,
        type_name: &str,
    ) -> (Vec<Value>, bool) {
        let DaemonCommand::GetOwnedWorkerCleanup {
            active_session_id,
            worker_id,
            worker_instance_id,
            owner_client_id,
            supervisor_generation,
            wait_timeout_ms,
            ..
        } = command
        else {
            return Self::owned_failure(command_id, type_name, "invalid command");
        };
        let max_wait_ms = crate::supervisor::owned_worker_cleanup_wait_timeout_ms();
        if *wait_timeout_ms > max_wait_ms {
            return Self::owned_failure(
                command_id,
                type_name,
                &format!("waitTimeoutMs must be at most {max_wait_ms}"),
            );
        }
        let identity = OwnedWorkerCleanupIdentity {
            active_session_id: active_session_id.clone(),
            worker_id: worker_id.clone(),
            worker_instance_id: worker_instance_id.clone(),
            owner_client_id: owner_client_id.clone(),
            supervisor_generation: supervisor_generation.clone(),
        };
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(*wait_timeout_ms);
        let status = loop {
            let notified = self.owner_cleanup_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let status = self.owned_worker_cleanup_status(&identity).await;
            if status != "pending" || *wait_timeout_ms == 0 {
                break status;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break self.owned_worker_cleanup_status(&identity).await;
            }
        };
        (
            vec![response_line(&response_success(
                Some(command_id),
                type_name,
                Some(json!({ "status": status })),
            ))],
            false,
        )
    }

    /// Resolve the resident a `retry_worker` targets: the direct match on
    /// the root active session id or the root persisted session id, then
    /// the generic selector resolution. This is the ONE descriptor-scan
    /// primitive behind the owned-session arms, so the boot-reconciliation
    /// quarantine fences HERE: a quarantined resident never matches, and
    /// the refusal reads as the unknown session.
    async fn resolve_retry_target(&self, selector: &str) -> Result<Arc<ResidentWorker>, String> {
        for resident in self.registry.list().await {
            if resident.identity_quarantined() {
                continue;
            }
            let matches = {
                let descriptor = resident.descriptor.lock().await;
                descriptor.root_active_session_id == selector
                    || descriptor.root_session_id.as_deref() == Some(selector)
            };
            if matches {
                return Ok(resident);
            }
        }
        match self.registry.resolve(selector).await {
            Ok(resident) => Ok(resident),
            Err(_) => Err(format!("Unknown active session: {selector}")),
        }
    }

    /// `complete_owned_session`: the owner stops its session worker (the
    /// TS ownership check, then `stopWorker`).
    pub(crate) async fn handle_complete_owned_session(
        self: &Arc<Self>,
        command: &DaemonCommand,
        effective_client_id: &str,
        command_id: &str,
        type_name: &str,
    ) -> (Vec<Value>, bool) {
        let DaemonCommand::CompleteOwnedSession {
            active_session_id, ..
        } = command
        else {
            return Self::owned_failure(command_id, type_name, "invalid command");
        };
        let resident = match self.resolve_retry_target(active_session_id).await {
            Ok(resident) => resident,
            Err(error) => return Self::owned_failure(command_id, type_name, &error),
        };
        let authorized = {
            let descriptor = resident.descriptor.lock().await;
            descriptor.owner_client_id.as_deref() == Some(effective_client_id)
                || (descriptor
                    .rest
                    .get("threadHeartbeatAdmission")
                    .and_then(Value::as_bool)
                    == Some(true)
                    && session_control_authorized(&descriptor, effective_client_id))
        };
        if !authorized {
            return Self::owned_failure(
                command_id,
                type_name,
                "Session is not owned by this client",
            );
        }
        // TS `stopWorker`: the owned stop tears the worker down; the
        // ephemeral schedule cancel rides `stop_worker` itself. The stop's
        // durable intent persists before the worker is told, and a persist
        // failure fails the owned stop.
        if let Err(error) = self.stop_worker(&resident).await {
            return Self::owned_failure(
                command_id,
                type_name,
                &format!("Failed to stop the session worker: {error:#}"),
            );
        }
        (
            vec![response_line(&response_success(
                Some(command_id),
                type_name,
                None,
            ))],
            false,
        )
    }

    /// `promote_owned_session`: the owner clears the ownership so the
    /// session outlives its creating client (TS `promoteOwnedWorker`).
    pub(crate) async fn handle_promote_owned_session(
        self: &Arc<Self>,
        command: &DaemonCommand,
        effective_client_id: &str,
        command_id: &str,
        type_name: &str,
    ) -> (Vec<Value>, bool) {
        let DaemonCommand::PromoteOwnedSession {
            active_session_id, ..
        } = command
        else {
            return Self::owned_failure(command_id, type_name, "invalid command");
        };
        let resident = match self.resolve_retry_target(active_session_id).await {
            Ok(resident) => resident,
            Err(error) => return Self::owned_failure(command_id, type_name, &error),
        };
        let previous_owner = {
            let mut descriptor = resident.descriptor.lock().await;
            match descriptor.owner_client_id.clone() {
                Some(owner) if owner == effective_client_id => {
                    descriptor.owner_client_id = None;
                    // The promotion marker (TS `promotedOwnerClientId`): a
                    // repeat promote by the same client stays a no-op.
                    descriptor
                        .rest
                        .insert("promotedOwnerClientId".to_string(), json!(owner));
                    if let Err(error) = persist_worker(&resident.descriptor_path, &descriptor) {
                        return Self::owned_failure(command_id, type_name, &error.to_string());
                    }
                    // The promotion clear (TS `promoteOwnedWorker`).
                    resident.clear_launch_env();
                    true
                }
                None => {
                    // Already promoted: a repeat by the promoting client is
                    // a no-op, anyone else never owned it.
                    descriptor
                        .rest
                        .get("promotedOwnerClientId")
                        .and_then(Value::as_str)
                        == Some(effective_client_id)
                }
                Some(_) => false,
            }
        };
        if !previous_owner {
            return Self::owned_failure(
                command_id,
                type_name,
                "Session is not owned by this client",
            );
        }
        // The fresh public summary (TS `publicSummary`).
        let summary = self.retry_summary(&resident).await;
        (
            vec![response_line(&response_success(
                Some(command_id),
                type_name,
                summary,
            ))],
            false,
        )
    }

    /// `retry_worker`: the supervisor's manual recovery trigger (TS
    /// supervisor arm - the worker never sees it): clear the stop
    /// markers, reconnect or relaunch the worker, and answer the
    /// session's summary (`data: null` when none can be read).
    pub(crate) async fn handle_retry_worker(
        self: &Arc<Self>,
        command: &DaemonCommand,
        effective_client_id: &str,
        command_id: &str,
        type_name: &str,
    ) -> (Vec<Value>, bool) {
        let DaemonCommand::RetryWorker {
            active_session_id, ..
        } = command
        else {
            return Self::owned_failure(command_id, type_name, "invalid command");
        };
        let resident = match self.resolve_retry_target(active_session_id).await {
            Ok(resident) => resident,
            Err(error) => return Self::owned_failure(command_id, type_name, &error),
        };
        // The client access gate (TS `assertWorkerAccessibleToClient`): a
        // worker another client owns is invisible through this selector.
        {
            let descriptor = resident.descriptor.lock().await;
            if let Some(owner) = descriptor.owner_client_id.as_deref() {
                if owner != effective_client_id {
                    return Self::owned_failure(
                        command_id,
                        type_name,
                        &format!("Unknown active session: {active_session_id}"),
                    );
                }
            }
        }
        if self.is_stopping(&resident) {
            return Self::owned_failure(
                command_id,
                type_name,
                "Session worker is stopping; retry after it finishes",
            );
        }
        // The recovery reset (TS `retryWorkerRecovery`): the intentional-stop
        // marker and the persisted stop flags go, the lifecycle reports
        // recovering until the relaunch lands.
        resident
            .intentional_stop
            .store(false, std::sync::atomic::Ordering::SeqCst);
        resident
            .consecutive_failures
            .store(0, std::sync::atomic::Ordering::SeqCst);
        let last_error = {
            let mut descriptor = resident.descriptor.lock().await;
            descriptor.stop_requested_at = None;
            descriptor.archive_on_stop = None;
            descriptor.lifecycle = DaemonWorkerLifecycle::Recovering;
            let last_error = descriptor.last_error.clone();
            let _ = persist_worker(&resident.descriptor_path, &descriptor);
            last_error
        };
        let connected = resident.cmd_tx.lock().await.is_some();
        if connected {
            // A live worker's recovery is the marker reset itself: the
            // connection answers the lifecycle (TS `effectiveWorkerState`
            // reports a ready, connected worker as ready).
            let mut descriptor = resident.descriptor.lock().await;
            descriptor.lifecycle = DaemonWorkerLifecycle::Ready;
            let _ = persist_worker(&resident.descriptor_path, &descriptor);
        } else if let Err(error) = self.relaunch_worker(&resident).await {
            return Self::owned_failure(
                command_id,
                type_name,
                &last_error.unwrap_or_else(|| error.to_string()),
            );
        }
        let lifecycle = resident.descriptor.lock().await.lifecycle;
        if lifecycle != DaemonWorkerLifecycle::Ready {
            return Self::owned_failure(
                command_id,
                type_name,
                &last_error.unwrap_or_else(|| "Session worker recovery failed".to_string()),
            );
        }
        let summary = self.retry_summary(&resident).await;
        (
            vec![response_line(&response_success(
                Some(command_id),
                type_name,
                summary,
            ))],
            false,
        )
    }

    /// The worker's current summary via `get_state` (`None` when the
    /// worker cannot answer - the TS arm answers `data: null` then).
    async fn retry_summary(&self, resident: &Arc<ResidentWorker>) -> Option<Value> {
        match self
            .route_command_typed(
                resident,
                "get_state",
                json!({}),
                ROUTE_TIMEOUT_MS,
                RouteAdmission::SupervisorInternal,
            )
            .await
        {
            Ok(response) if response.success => response.data,
            _ => None,
        }
    }

    fn owned_failure(command_id: &str, type_name: &str, error: &str) -> (Vec<Value>, bool) {
        (
            vec![response_line(&response_failure(
                Some(command_id),
                type_name,
                error,
                None,
            ))],
            false,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pa_types::daemon::{DaemonCommand, DaemonWorkerDescriptor};
    use serde_json::json;

    use crate::descriptor::{descriptor_dir, persist_worker};
    use crate::registry::ResidentWorker;
    use crate::supervisor::Supervisor;
    use crate::supervisor_roster::WorkerRosterDelta;

    #[tokio::test]
    async fn cleanup_receipts_require_exact_identity_and_generation() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor = supervisor_with_stale_identity(dir.path()).await;
        let resident = supervisor.registry.get("w-a").await.unwrap();
        {
            let mut descriptor = resident.descriptor.lock().await;
            descriptor.owner_client_id = Some("old-owner".into());
            descriptor.worker_instance_id = Some("instance-a".into());
        }
        let identity = supervisor
            .owned_worker_cleanup_identity(&resident)
            .await
            .unwrap();
        assert_eq!(
            supervisor.owned_worker_cleanup_status(&identity).await,
            "unverified"
        );
        // Timer already claimed (slot empty), before any awaited owner checks or tombstone.
        resident
            .owner_cleanup_running
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            supervisor.owned_worker_cleanup_status(&identity).await,
            "pending"
        );
        resident
            .owner_cleanup_running
            .store(false, std::sync::atomic::Ordering::SeqCst);
        resident
            .intentional_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            supervisor.owned_worker_cleanup_status(&identity).await,
            "pending"
        );
        let mut other = identity.clone();
        other.owner_client_id = "new-owner".into();
        assert_eq!(
            supervisor.owned_worker_cleanup_status(&other).await,
            "unverified"
        );
        other = identity.clone();
        other.worker_instance_id = "replacement-instance".into();
        assert_eq!(
            supervisor.owned_worker_cleanup_status(&other).await,
            "unverified"
        );
        supervisor
            .owner_cleanup_receipts
            .lock()
            .unwrap()
            .push_back((identity.clone(), false));
        supervisor.registry.remove("w-a").await;
        assert_eq!(
            supervisor.owned_worker_cleanup_status(&identity).await,
            "pending",
            "registry retirement before stop returns is still pending, never absence proof"
        );
        supervisor.owner_cleanup_receipts.lock().unwrap().clear();
        assert_eq!(
            supervisor.owned_worker_cleanup_status(&identity).await,
            "unverified"
        );
        // Only the existing stop-success caller may publish its proof.
        supervisor.record_owned_worker_cleanup(identity.clone());
        assert_eq!(
            supervisor.owned_worker_cleanup_status(&identity).await,
            "verified_stopped"
        );
        let restarted = supervisor_with_stale_identity(dir.path()).await;
        assert_eq!(
            restarted.owned_worker_cleanup_status(&identity).await,
            "unverified"
        );
    }

    #[tokio::test]
    async fn cleanup_wait_is_read_only_bounded_and_cancellation_safe() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor = supervisor_with_stale_identity(dir.path()).await;
        let resident = supervisor.registry.get("w-a").await.unwrap();
        {
            let mut descriptor = resident.descriptor.lock().await;
            descriptor.owner_client_id = Some("old-owner".into());
            descriptor.worker_instance_id = Some("instance-a".into());
        }
        let identity = supervisor
            .owned_worker_cleanup_identity(&resident)
            .await
            .unwrap();
        resident
            .intentional_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut value = serde_json::to_value(&identity).unwrap();
        value["type"] = json!("get_owned_worker_cleanup");
        value["waitTimeoutMs"] = json!(30_000);
        let command: DaemonCommand = serde_json::from_value(value.clone()).unwrap();
        let waiting_supervisor = Arc::clone(&supervisor);
        let waiting_command = command.clone();
        let wait = tokio::spawn(async move {
            waiting_supervisor
                .handle_get_owned_worker_cleanup(
                    &waiting_command,
                    "read",
                    "get_owned_worker_cleanup",
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!wait.is_finished());
        wait.abort();
        let _ = wait.await;
        assert!(resident.descriptor.lock().await.stop_requested_at.is_none());
        let waiting_supervisor = Arc::clone(&supervisor);
        let wait = tokio::spawn(async move {
            waiting_supervisor
                .handle_get_owned_worker_cleanup(&command, "read", "get_owned_worker_cleanup")
                .await
        });
        tokio::task::yield_now().await;
        supervisor.record_owned_worker_cleanup(identity);
        let (reply, _) = wait.await.unwrap();
        assert_eq!(reply[0]["data"]["status"], "verified_stopped");
        value["waitTimeoutMs"] =
            json!(crate::supervisor::owned_worker_cleanup_wait_timeout_ms() + 1);
        let command: DaemonCommand = serde_json::from_value(value).unwrap();
        let (reply, _) = supervisor
            .handle_get_owned_worker_cleanup(&command, "read", "get_owned_worker_cleanup")
            .await;
        assert_eq!(reply[0]["success"], false);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_total_observation_covers_grace_and_delayed_stop_without_retry() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor = supervisor_with_stale_identity(dir.path()).await;
        let resident = supervisor.registry.get("w-a").await.unwrap();
        {
            let mut descriptor = resident.descriptor.lock().await;
            descriptor.owner_client_id = Some("owner".into());
            descriptor.worker_instance_id = Some("instance".into());
        }
        let identity = supervisor
            .owned_worker_cleanup_identity(&resident)
            .await
            .unwrap();
        supervisor
            .owner_cleanup_receipts
            .lock()
            .unwrap()
            .push_back((identity.clone(), false));
        let make_command = |wait| {
            let mut value = serde_json::to_value(&identity).unwrap();
            value["type"] = json!("get_owned_worker_cleanup");
            value["waitTimeoutMs"] = json!(wait);
            serde_json::from_value::<DaemonCommand>(value).unwrap()
        };
        let budget = crate::supervisor::owned_worker_cleanup_wait_timeout_ms();
        let command = make_command(budget);
        let reader = Arc::clone(&supervisor);
        let wait = tokio::spawn(async move {
            reader
                .handle_get_owned_worker_cleanup(&command, "proof", "get_owned_worker_cleanup")
                .await
        });
        tokio::task::yield_now().await;
        // The existing disconnect grace consumed the old entire observation bound.
        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert!(!wait.is_finished());
        // A real stop may finish after grace, without a second proof request.
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        supervisor.record_owned_worker_cleanup(identity.clone());
        let (reply, _) = wait.await.unwrap();
        assert_eq!(reply[0]["data"]["status"], "verified_stopped");
        supervisor.owner_cleanup_receipts.lock().unwrap().clear();
        supervisor
            .owner_cleanup_receipts
            .lock()
            .unwrap()
            .push_back((identity.clone(), false));
        let command = make_command(30_000);
        let reader = Arc::clone(&supervisor);
        let started = tokio::time::Instant::now();
        let (reply, _) = reader
            .handle_get_owned_worker_cleanup(&command, "proof", "get_owned_worker_cleanup")
            .await;
        assert_eq!(reply[0]["data"]["status"], "pending");
        assert_eq!(
            started.elapsed(),
            std::time::Duration::from_secs(30),
            "waitTimeoutMs remains a total deadline, never implicit grace plus budget"
        );
    }

    /// One supervisor with one registered resident whose persisted identity
    /// (`s0` / `s0.jsonl`) the quarantine fence must never serve.
    async fn supervisor_with_stale_identity(dir: &std::path::Path) -> Arc<Supervisor> {
        let supervisor = Arc::new(
            Supervisor::new(crate::supervisor::SupervisorOptions {
                socket_path: dir.join("daemon.sock"),
                agent_dir: dir.join("agent"),
            })
            .expect("supervisor"),
        );
        let descriptor: DaemonWorkerDescriptor = serde_json::from_value(json!({
            "version": 2,
            "workerId": "w-a",
            "pid": 0,
            "socketPath": "/tmp/none.sock",
            "recoveryJournalPath": "/tmp/none.jsonl",
            "supervisorSocketPath": "/tmp/none.sock",
            "authenticationToken": "token-a",
            "rootActiveSessionId": "w-a",
            "rootSessionId": "s0",
            "sessionFile": dir.join("s0.jsonl").to_string_lossy(),
            "createdAt": "2026-09-30T00:00:00Z",
            "updatedAt": "2026-09-30T00:00:00Z",
            "lifecycle": "ready",
            "createCommand": { "sessionPath": dir.join("s0.jsonl").to_string_lossy() },
            "consecutiveFailures": 0,
        }))
        .expect("descriptor");
        supervisor
            .registry
            .insert(ResidentWorker::new(
                "w-a".to_string(),
                descriptor,
                dir.join("w-a.json"),
            ))
            .await;
        supervisor
    }

    /// The boot-reconciliation quarantine fences the owned-session scan:
    /// the address OR the OLD durable id never reaches the unreconciled
    /// worker; the live word (the worker's own roster push) reconciles
    /// the identity and the scan serves again — on the reconciled
    /// identity only.
    #[tokio::test]
    async fn the_owned_session_scan_refuses_a_quarantined_identity() {
        let dir = std::env::temp_dir().join(format!("pa-own-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let supervisor = supervisor_with_stale_identity(&dir).await;
        let resident = supervisor.registry.get("w-a").await.expect("resident");

        resident.mark_identity_quarantined();
        assert!(
            supervisor.resolve_retry_target("w-a").await.is_err(),
            "the quarantined address never serves the owned-session arms"
        );
        assert!(
            supervisor.resolve_retry_target("s0").await.is_err(),
            "the stale persisted durable id never reaches the worker"
        );

        // The live word: the worker's own roster push (no command
        // channel involved) reconciles the identity onto sA.
        let summary = json!({
            "id": "w-a",
            "lifecycle": "live",
            "activity": "idle",
            "isSessionActive": false,
            "activeSessionId": "w-a",
            "sessionId": "sA",
            "sessionFile": dir.join("a.jsonl").to_string_lossy(),
            "sessionName": "owned",
            "cwd": dir.to_string_lossy(),
            "rlmDepth": 0,
            "runtimeKind": "top-level",
            "messageCount": 1,
            "attachedClients": 0,
            "thinkingLevel": "default",
            "workerState": "ready",
        });
        let response = supervisor
            .handle_worker_roster_delta(
                "d",
                "worker_roster_delta",
                WorkerRosterDelta {
                    worker_token: "token-a".to_string(),
                    summary,
                    removed: Vec::new(),
                    sequence: Some(1),
                    worker_instance_id: Some("i1".to_string()),
                },
            )
            .await;
        assert!(response.success, "the delta applied: {response:?}");

        // The scan serves again: the address resolves, the OLD durable id
        // never does.
        let resolved = supervisor
            .resolve_retry_target("w-a")
            .await
            .expect("the address serves once reconciled");
        assert_eq!(
            resolved.descriptor.lock().await.root_session_id.as_deref(),
            Some("sA"),
            "the served identity is the reconciled one"
        );
        assert!(
            supervisor.resolve_retry_target("s0").await.is_err(),
            "the superseded durable id never resolves again"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn complete_owned_session_refuses_success_while_its_worker_is_unverified_and_live() {
        let dir = tempfile::tempdir().expect("temp dir");
        let supervisor = Arc::new(
            Supervisor::new(crate::supervisor::SupervisorOptions {
                socket_path: dir.path().join("daemon.sock"),
                agent_dir: dir.path().join("agent"),
            })
            .expect("supervisor"),
        );
        let descriptor_path = descriptor_dir(
            &supervisor.options.agent_dir,
            &supervisor.options.socket_path,
        )
        .join("w-live.json");
        let descriptor: DaemonWorkerDescriptor = serde_json::from_value(json!({
            "version": 2,
            "workerId": "w-live",
            "pid": std::process::id(),
            "socketPath": dir.path().join("missing.sock").to_string_lossy(),
            "recoveryJournalPath": dir.path().join("recovery.jsonl").to_string_lossy(),
            "supervisorSocketPath": supervisor.options.socket_path.to_string_lossy(),
            "authenticationToken": "token",
            "rootActiveSessionId": "w-live",
            "ownerClientId": "owner-1",
            "createdAt": "2026-10-07T00:00:00Z",
            "updatedAt": "2026-10-07T00:00:00Z",
            "lifecycle": "ready",
            "createCommand": {},
            "consecutiveFailures": 0
        }))
        .expect("descriptor");
        persist_worker(&descriptor_path, &descriptor).expect("persist descriptor");
        let resident =
            ResidentWorker::new("w-live".to_string(), descriptor, descriptor_path.clone());
        supervisor.registry.insert(Arc::clone(&resident)).await;

        let command = DaemonCommand::CompleteOwnedSession {
            id: None,
            active_session_id: "w-live".to_string(),
            rest: serde_json::Map::default(),
        };
        let (reply, _) = supervisor
            .handle_complete_owned_session(
                &command,
                "owner-1",
                "complete",
                "complete_owned_session",
            )
            .await;
        assert_eq!(reply[0]["success"], false);
        assert!(reply[0]["error"]
            .as_str()
            .unwrap()
            .contains("has not exited"));
        assert!(descriptor_path.exists(), "durable tombstone remains");
        assert!(resident.descriptor.lock().await.stop_requested_at.is_some());
        assert!(supervisor.registry.get("w-live").await.is_some());
        assert!(
            supervisor.owner_cleanup_receipts.lock().unwrap().is_empty(),
            "failed exit verification must never publish a cleanup receipt"
        );
    }
}
