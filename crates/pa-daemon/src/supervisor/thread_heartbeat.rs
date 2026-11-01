//! Saved thread controls on the existing local supervisor control plane.
use super::{json, Arc, DaemonCommand, ResidentWorker, Supervisor, Value};
use pa_core::cron::store::{AgentCronJobStore, CreateAgentCronJobInput};

fn saved_identity(
    session_root: &std::path::Path,
    payload: &Value,
) -> anyhow::Result<(
    crate::session_store::SessionInfo,
    CreateAgentCronJobInput,
    AgentCronJobStore,
)> {
    let value = |key: &str| {
        payload
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("identity_conflict"))
    };
    anyhow::ensure!(
        payload.get("extensionVersion").and_then(Value::as_u64) == Some(1),
        "invalid_input"
    );
    let file = std::fs::canonicalize(value("sessionFile")?)
        .map_err(|_| anyhow::anyhow!("identity_conflict"))?;
    let root =
        std::fs::canonicalize(session_root).map_err(|_| anyhow::anyhow!("identity_conflict"))?;
    anyhow::ensure!(
        file.parent() == Some(root.as_path()) && file.to_str() == Some(value("sessionFile")?),
        "identity_conflict"
    );
    let info = crate::session_store::read_session_info(&file)
        .ok_or_else(|| anyhow::anyhow!("identity_conflict"))?;
    anyhow::ensure!(
        info.id == value("sessionId")?
            && info.cwd == value("cwd")?
            && info.state.as_deref() == Some("active")
            && info.rlm_depth == 0
            && info.parent_session_path.is_none(),
        "identity_conflict"
    );
    anyhow::ensure!(value("activeSessionId")? == info.id, "identity_conflict");
    let dir = crate::scheduled_jobs::session_artifact_dir(&file, &info.id)
        .ok_or_else(|| anyhow::anyhow!("identity_conflict"))?;
    // Canonical nearest existing parent prevents symlink escape even before first enable.
    let artifact_parent = root
        .parent()
        .ok_or_else(|| anyhow::anyhow!("identity_conflict"))?;
    let artifact_root = artifact_parent.join("session-artifacts");
    let canonical_agent = std::fs::canonicalize(artifact_parent)?;
    if artifact_root.exists() {
        anyhow::ensure!(
            std::fs::canonicalize(&artifact_root)?.parent() == Some(canonical_agent.as_path()),
            "identity_conflict"
        );
    }
    if dir.exists() {
        anyhow::ensure!(
            std::fs::canonicalize(&dir)?.parent()
                == Some(std::fs::canonicalize(&artifact_root)?.as_path()),
            "identity_conflict"
        );
    }
    anyhow::ensure!(
        dir.parent() == Some(artifact_root.as_path()),
        "identity_conflict"
    );
    let store = AgentCronJobStore::for_session_artifacts();
    store.register_session_artifact(&info.id, &dir);
    let input = CreateAgentCronJobInput {
        active_session_id: info.id.clone(),
        session_id: info.id.clone(),
        session_file: file.to_string_lossy().into(),
        cwd: info.cwd.clone(),
        runtime_kind: Some("top-level".into()),
        ..Default::default()
    };
    Ok((info, input, store))
}

impl Supervisor {
    pub(crate) fn thread_heartbeat_session_root(
        &self,
        payload: &Value,
    ) -> anyhow::Result<std::path::PathBuf> {
        let configured = crate::paths::sessions_dir(&self.options.agent_dir)?;
        let file = payload
            .get("sessionFile")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("identity_conflict"))?;
        let canonical =
            std::fs::canonicalize(file).map_err(|_| anyhow::anyhow!("identity_conflict"))?;
        if std::fs::canonicalize(&configured).ok().as_deref() == canonical.parent() {
            return Ok(configured);
        }
        // Registered history outside the configured root is authoritative only
        // through a validated existing Native descriptor, never caller sessionDir.
        for entry in std::fs::read_dir(&self.descriptor_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let descriptor: pa_types::daemon::DaemonWorkerDescriptor =
                serde_json::from_slice(&std::fs::read(path)?)?;
            crate::descriptor::validate_descriptor(&descriptor, &self.options.socket_path)?;
            if descriptor.root_session_id.as_deref()
                != payload.get("sessionId").and_then(Value::as_str)
            {
                continue;
            }
            if descriptor
                .session_file
                .as_deref()
                .and_then(|path| std::fs::canonicalize(path).ok())
                .as_ref()
                != Some(&canonical)
            {
                continue;
            }
            return canonical
                .parent()
                .map(std::path::Path::to_path_buf)
                .ok_or_else(|| anyhow::anyhow!("identity_conflict"));
        }
        anyhow::bail!("identity_conflict")
    }

    pub(crate) async fn handle_saved_thread_heartbeat(
        self: &Arc<Self>,
        payload: &Value,
        mutate: bool,
        client_id: &str,
    ) -> anyhow::Result<Value> {
        let root = self.thread_heartbeat_session_root(payload)?;
        let (_info, input, store) = saved_identity(&root, payload)?;
        if mutate {
            crate::lease::assert_session_not_quarantined(
                &self.options.agent_dir,
                std::path::Path::new(&input.session_file),
            )?;
        }
        let control_open: DaemonCommand = serde_json::from_value(
            json!({"type":"create","sessionPath":input.session_file,"lifecycle":"resident"}),
        )?;
        // Match normal Create lock ordering, then re-read the holder and revision.
        let _control_guard = if mutate {
            self.opening_guard(&control_open).await?
        } else {
            None
        };
        let mut data = store.thread_heartbeat_snapshot(&input)?;
        let residents = self
            .registry
            .admission_holders_by_session_file(&input.session_file)
            .await;
        if !mutate {
            data["runtime"] = json!({"health":if residents.is_empty(){"dormant"}else{"unconfirmed"},"waitingState":if residents.is_empty(){"idle"}else{"unknown"}});
            let mutable = if residents.is_empty() {
                true
            } else if residents.len() == 1
                && !residents[0].identity_quarantined()
                && !self.is_stopping(&residents[0])
            {
                let descriptor = residents[0].descriptor.lock().await;
                crate::ownership::session_control_authorized(&descriptor, client_id)
                    || (descriptor.owner_client_id.is_none()
                        && descriptor
                            .rest
                            .get("threadHeartbeatAdmission")
                            .and_then(Value::as_bool)
                            == Some(true)
                        && !descriptor
                            .rest
                            .get("promotedOwnerClientId")
                            .and_then(Value::as_str)
                            .is_some_and(|owner| {
                                self.client_connections
                                    .lock()
                                    .unwrap()
                                    .values()
                                    .any(|id| id.lock().unwrap().as_str() == owner)
                            }))
            } else {
                false
            };
            data["capabilities"] = json!({"savedRead":true,"savedMutation":mutable});
            return Ok(data);
        }
        anyhow::ensure!(
            payload.get("expectedRevision") == Some(&data["revision"]),
            "revision_conflict"
        );
        if residents.is_empty() {
            let mut authorized = payload.clone();
            authorized["supervisorSocket"] = json!(self.options.socket_path.to_string_lossy());
            data = store.mutate_thread_heartbeat(&input, &authorized)?;
            self.broadcast_heartbeats_changed();
            if !matches!(
                payload.get("action").and_then(Value::as_str),
                Some("create" | "resume")
            ) {
                data["runtime"] = json!({"health":"dormant","waitingState":"idle"});
                return Ok(data);
            }
        }
        // Admission uses the existing per-file singleflight/lease/adoption guards.
        // No public payload can set descriptor control ownership.
        let resident = if residents.is_empty() {
            let create: DaemonCommand = serde_json::from_value(
                json!({"type":"create","sessionPath":input.session_file,"lifecycle":"resident"}),
            )?;
            let summary = self
                .handle_create_under_admission_guards(&create, client_id.to_string(), true)
                .await?;
            let active = summary
                .get("activeSessionId")
                .or_else(|| summary.get("id"))
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("owner_unavailable"))?;
            self.registry
                .resolve(active)
                .await
                .map_err(|_| anyhow::anyhow!("owner_unavailable"))?
        } else {
            anyhow::ensure!(residents.len() == 1, "owner_unavailable");
            Arc::clone(&residents[0])
        };
        {
            let descriptor = resident.descriptor.lock().await;
            anyhow::ensure!(
                descriptor.root_session_id.as_deref() == Some(input.session_id.as_str())
                    && descriptor.session_file.as_deref() == Some(input.session_file.as_str()),
                "identity_conflict"
            );
        }
        self.claim_thread_heartbeat_control(&resident, client_id)
            .await?;
        if residents.is_empty() {
            let mut readback = store.thread_heartbeat_snapshot(&input)?;
            readback["runtime"] = json!({"health":"unconfirmed","waitingState":"unknown"});
            return Ok(readback);
        }
        let active = resident
            .descriptor
            .lock()
            .await
            .root_active_session_id
            .clone();
        let mut worker_payload = payload.clone();
        worker_payload["activeSessionId"] = json!(active);
        worker_payload
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("invalid_input"))?
            .remove("scope");
        let response = self
            .route_command_ready_typed(
                &resident,
                "heartbeat_update",
                worker_payload,
                super::ROUTE_TIMEOUT_MS,
                crate::backpressure::RouteAdmission::SupervisorInternal,
            )
            .await?;
        anyhow::ensure!(
            response.success,
            "{}",
            response.error.unwrap_or_else(|| "store_error".into())
        );
        // Durability/promotion before success; never complete an owning scheduler.
        let mut descriptor = resident.descriptor.lock().await;
        anyhow::ensure!(
            crate::ownership::session_control_authorized(&descriptor, client_id),
            "owner_unavailable"
        );
        if descriptor.owner_client_id.is_some() {
            let previous = descriptor.clone();
            descriptor.owner_client_id = None;
            descriptor
                .rest
                .insert("promotedOwnerClientId".into(), json!(client_id));
            descriptor
                .rest
                .insert("threadHeartbeatAdmission".into(), json!(true));
            if let Err(error) =
                crate::descriptor::persist_worker(&resident.descriptor_path, &descriptor)
            {
                *descriptor = previous;
                return Err(error);
            }
            resident.clear_launch_env();
        }
        self.broadcast_heartbeats_changed();
        response.data.ok_or_else(|| anyhow::anyhow!("store_error"))
    }

    /// Called by normal Create while its existing per-file opening guard is held.
    pub(crate) async fn handoff_thread_heartbeat_create(
        &self,
        resident: &Arc<ResidentWorker>,
        client_id: &str,
        heartbeat_admission: bool,
    ) -> anyhow::Result<()> {
        let mut descriptor = resident.descriptor.lock().await;
        if descriptor
            .rest
            .get("threadHeartbeatAdmission")
            .and_then(Value::as_bool)
            != Some(true)
        {
            return Ok(());
        }
        if heartbeat_admission {
            anyhow::ensure!(
                descriptor.owner_client_id.is_none()
                    && (descriptor.rest.get("promotedOwnerClientId").is_none()
                        || crate::ownership::session_control_authorized(&descriptor, client_id)),
                "owner_unavailable"
            );
            return Ok(());
        }
        anyhow::ensure!(descriptor.owner_client_id.is_none(), "owner_unavailable");
        let previous = descriptor.clone();
        // An ordinary local control-plane Create hands its own identity to this
        // already resident scheduler; it cannot take a foreign client-owned worker.
        descriptor
            .rest
            .insert("promotedOwnerClientId".into(), json!(client_id));
        if let Err(error) =
            crate::descriptor::persist_worker(&resident.descriptor_path, &descriptor)
        {
            *descriptor = previous;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn claim_thread_heartbeat_control(
        &self,
        resident: &Arc<ResidentWorker>,
        client_id: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !resident.identity_quarantined() && !self.is_stopping(resident),
            "owner_unavailable"
        );
        let mut descriptor = resident.descriptor.lock().await;
        if crate::ownership::session_control_authorized(&descriptor, client_id) {
            return Ok(());
        }
        let previous_connected = descriptor
            .rest
            .get("promotedOwnerClientId")
            .and_then(Value::as_str)
            .is_some_and(|owner| {
                self.client_connections
                    .lock()
                    .unwrap()
                    .values()
                    .any(|id| id.lock().unwrap().as_str() == owner)
            });
        anyhow::ensure!(
            descriptor.owner_client_id.is_none()
                && !previous_connected
                && descriptor
                    .rest
                    .get("threadHeartbeatAdmission")
                    .and_then(Value::as_bool)
                    == Some(true),
            "owner_unavailable"
        );
        // A controller-vacant resident is adoptable only through exact durable
        // local control-plane identity, never via a session-plane ticket.
        let previous = descriptor.clone();
        descriptor
            .rest
            .insert("promotedOwnerClientId".into(), json!(client_id));
        if let Err(error) =
            crate::descriptor::persist_worker(&resident.descriptor_path, &descriptor)
        {
            *descriptor = previous;
            return Err(error);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;
    #[tokio::test]
    async fn saved_get_is_passive_real_revision_and_exact_identity() {
        let temp = tempfile::tempdir().unwrap();
        let agent = temp.path().join("agent");
        let sessions = agent.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let file = sessions.join("s.jsonl");
        let rows = [
            json!({"type":"session","version":3,"id":"s","timestamp":"2026-10-19T00:00:00Z","cwd":"/w"}),
            json!({"type":"session_state","id":"state","timestamp":"2026-10-19T00:00:01Z","state":{"status":"active"}}),
        ];
        std::fs::write(
            &file,
            rows.iter().fold(String::new(), |mut output, row| {
                let _ = writeln!(output, "{row}");
                output
            }),
        )
        .unwrap();
        let supervisor = Arc::new(
            Supervisor::new(crate::supervisor::SupervisorOptions {
                socket_path: temp.path().join("daemon.sock"),
                agent_dir: agent.clone(),
            })
            .unwrap(),
        );
        let payload = json!({"extensionVersion":1,"scope":"saved","activeSessionId":"s","sessionId":"s","sessionFile":file,"cwd":"/w"});
        let data = supervisor
            .handle_saved_thread_heartbeat(&payload, false, "owner")
            .await
            .unwrap();
        assert_eq!(data["revision"], 0);
        assert_eq!(data["runtime"]["health"], "dormant");
        assert!(supervisor.registry.list().await.is_empty());
        assert!(!agent
            .join("session-artifacts/s/scheduled-jobs.json")
            .exists());
        let (_, input, store) = saved_identity(&sessions, &payload).unwrap();
        let created = store
            .mutate_thread_heartbeat(
                &input,
                &json!({"action":"create","expectedRevision":0,"instruction":"inspect"}),
            )
            .unwrap();
        let data = supervisor
            .handle_saved_thread_heartbeat(&payload, false, "owner")
            .await
            .unwrap();
        assert_eq!(data["revision"], created["revision"]);
        let mut foreign = payload.clone();
        foreign["sessionId"] = json!("foreign");
        assert!(supervisor
            .handle_saved_thread_heartbeat(&foreign, false, "owner")
            .await
            .is_err());
        foreign = payload.clone();
        foreign["activeSessionId"] = json!("stale-active");
        assert!(supervisor
            .handle_saved_thread_heartbeat(&foreign, false, "owner")
            .await
            .is_err());
        assert!(supervisor.registry.list().await.is_empty());
    }

    #[test]
    fn saved_identity_rejects_foreign_root_and_selector_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let foreign = temp.path().join("foreign.jsonl");
        std::fs::write(&foreign, "{}").unwrap();
        let payload = json!({"extensionVersion":1,"scope":"saved","activeSessionId":"s","sessionId":"s","sessionFile":foreign,"cwd":"/w"});
        assert!(saved_identity(&sessions, &payload).is_err());
        assert!(!temp.path().join("session-artifacts").exists());
    }

    #[tokio::test]
    async fn ordinary_create_handoff_persists_its_identity_and_refuses_foreign_owned() {
        let temp = tempfile::tempdir().unwrap();
        let supervisor = Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: temp.path().join("daemon.sock"),
            agent_dir: temp.path().join("agent"),
        })
        .unwrap();
        let descriptor:pa_types::daemon::DaemonWorkerDescriptor=serde_json::from_value(json!({"version":2,"workerId":"w","pid":0,"socketPath":"/tmp/none.sock","recoveryJournalPath":"/tmp/none.jsonl","supervisorSocketPath":"/tmp/none.sock","authenticationToken":"fixture","rootActiveSessionId":"w","createdAt":"t","updatedAt":"t","lifecycle":"ready","createCommand":{},"consecutiveFailures":0,"threadHeartbeatAdmission":true,"promotedOwnerClientId":"prior"})).unwrap();
        let resident = ResidentWorker::new("w".into(), descriptor, temp.path().join("worker.json"));
        supervisor
            .handoff_thread_heartbeat_create(&resident, "new", false)
            .await
            .unwrap();
        let persisted: pa_types::daemon::DaemonWorkerDescriptor =
            serde_json::from_slice(&std::fs::read(&resident.descriptor_path).unwrap()).unwrap();
        assert!(crate::ownership::session_control_authorized(
            &persisted, "new"
        ));
        assert!(!crate::ownership::session_control_authorized(
            &persisted, "prior"
        ));
        resident.descriptor.lock().await.owner_client_id = Some("foreign".into());
        assert!(supervisor
            .handoff_thread_heartbeat_create(&resident, "new", false)
            .await
            .is_err());
        assert_eq!(
            resident.descriptor.lock().await.owner_client_id.as_deref(),
            Some("foreign")
        );
    }

    #[tokio::test]
    async fn saved_mutation_rechecks_holder_after_normal_create_guard() {
        let temp = tempfile::tempdir().unwrap();
        let agent = temp.path().join("agent");
        let sessions = agent.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let file = sessions.join("s.jsonl");
        std::fs::write(&file,format!("{}\n{}\n",json!({"type":"session","version":3,"id":"s","timestamp":"2026-10-19T00:00:00Z","cwd":"/w"}),json!({"type":"session_state","id":"state","timestamp":"2026-10-19T00:00:01Z","state":{"status":"active"}}))).unwrap();
        let supervisor = Arc::new(
            Supervisor::new(crate::supervisor::SupervisorOptions {
                socket_path: temp.path().join("daemon.sock"),
                agent_dir: agent.clone(),
            })
            .unwrap(),
        );
        let payload = json!({"extensionVersion":1,"scope":"saved","activeSessionId":"s","sessionId":"s","sessionFile":file,"cwd":"/w","expectedRevision":0,"action":"create","instruction":"inspect"});
        let create: DaemonCommand = serde_json::from_value(
            json!({"type":"create","sessionPath":file,"lifecycle":"client_owned"}),
        )
        .unwrap();
        let guard = supervisor.opening_guard(&create).await.unwrap();
        let mutation = supervisor.handle_saved_thread_heartbeat(&payload, true, "loser");
        tokio::pin!(mutation);
        assert!(futures::poll!(mutation.as_mut()).is_pending());
        let descriptor:pa_types::daemon::DaemonWorkerDescriptor=serde_json::from_value(json!({"version":2,"workerId":"w","pid":0,"socketPath":"/tmp/none.sock","recoveryJournalPath":"/tmp/none.jsonl","supervisorSocketPath":"/tmp/none.sock","authenticationToken":"fixture","rootActiveSessionId":"w","rootSessionId":"s","sessionFile":file,"ownerClientId":"winner","createdAt":"t","updatedAt":"t","lifecycle":"ready","createCommand":{},"consecutiveFailures":0})).unwrap();
        supervisor
            .registry
            .insert(ResidentWorker::new(
                "w".into(),
                descriptor,
                temp.path().join("worker.json"),
            ))
            .await;
        drop(guard);
        assert_eq!(mutation.await.unwrap_err().to_string(), "owner_unavailable");
        assert!(!agent
            .join("session-artifacts/s/scheduled-jobs.json")
            .exists());
        let data = supervisor
            .handle_saved_thread_heartbeat(&payload, false, "loser")
            .await
            .unwrap();
        assert_eq!(data["revision"], 0);
        assert_eq!(data["capabilities"]["savedMutation"], false);
    }
    #[tokio::test]
    async fn saved_heartbeat_mutation_preserves_session_quarantine() {
        let temp = tempfile::tempdir().unwrap();
        let agent = temp.path().join("agent");
        let sessions = agent.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let file = sessions.join("s.jsonl");
        std::fs::write(&file, format!("{}\n{}\n",
            json!({"type":"session","version":3,"id":"s","timestamp":"2026-10-19T00:00:00Z","cwd":"/w"}),
            json!({"type":"session_state","id":"state","timestamp":"2026-10-19T00:00:01Z","state":{"status":"active"}}))).unwrap();
        let supervisor = Arc::new(
            Supervisor::new(crate::supervisor::SupervisorOptions {
                socket_path: temp.path().join("daemon.sock"),
                agent_dir: agent.clone(),
            })
            .unwrap(),
        );
        crate::lease::persist_session_quarantine(
            &agent,
            &crate::lease::SessionQuarantine {
                version: 1,
                parent_session_file: sessions.join("parent.jsonl").to_string_lossy().into(),
                child_id: "s".into(),
                child_session_file: file.to_string_lossy().into(),
                task: json!({}),
                assignment: json!({}),
                current_admission_generation: None,
                current_expected_message_id: None,
            },
        )
        .unwrap();
        let payload = json!({"extensionVersion":1,"scope":"saved","activeSessionId":"s","sessionId":"s",
            "sessionFile":file,"cwd":"/w","expectedRevision":0,"action":"create","instruction":"inspect"});
        assert!(supervisor
            .handle_saved_thread_heartbeat(&payload, true, "owner")
            .await
            .unwrap_err()
            .to_string()
            .contains("quarantined"));
        assert!(!agent
            .join("session-artifacts/s/scheduled-jobs.json")
            .exists());
        assert!(supervisor.registry.list().await.is_empty());
    }
}
