//! Resident-only operations reached exclusively through the attested owner lane.
use super::{roadmap, Supervisor};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    operation: String,
    root_session_id: String,
    active_session_id: String,
    worker_instance_id: String,
    supervisor_generation: String,
    expected_binding_revision: u64,
    drone_roadmap_path: Option<String>,
    expected_sha256: Option<String>,
    expected_roadmap_revision: Option<String>,
    action: Option<String>,
    expected_revision: Option<u64>,
}
pub(crate) fn validate_resident_request(value: &Value) -> anyhow::Result<()> {
    let request: Request = serde_json::from_value(value.clone())?;
    anyhow::ensure!(
        matches!(
            request.operation.as_str(),
            "resident_drone_bind" | "resident_drone_status" | "resident_drone_control"
        ),
        "private_owner_invalid_request"
    );
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("private_owner_invalid_request"))?;
    if request.operation == "resident_drone_control" {
        anyhow::ensure!(
            [
                "droneRoadmapPath",
                "expectedSha256",
                "expectedRoadmapRevision"
            ]
            .iter()
            .all(|field| !object.contains_key(*field))
                && matches!(request.action.as_deref(), Some("stop" | "resume"))
                && request.expected_revision.is_some(),
            "private_owner_invalid_request"
        );
    } else {
        anyhow::ensure!(
            !object.contains_key("action") && !object.contains_key("expectedRevision"),
            "private_owner_invalid_request"
        );
    }
    if request.operation == "resident_drone_status" {
        anyhow::ensure!(
            request.drone_roadmap_path.is_none()
                && request.expected_sha256.is_none()
                && request.expected_roadmap_revision.is_none(),
            "private_owner_invalid_request"
        );
    }
    Ok(())
}
impl Supervisor {
    pub(crate) async fn resident_drone_request(
        self: &Arc<Self>,
        value: Value,
    ) -> anyhow::Result<crate::protocol::DaemonResponse> {
        validate_resident_request(&value)?;
        let request: Request = serde_json::from_value(value)?;
        anyhow::ensure!(
            request.supervisor_generation == self.owner_cleanup_generation,
            "private_owner_generation_changed"
        );
        let resident = self.registry.resolve(&request.active_session_id).await?;
        let _binding_admission = if request.operation == "resident_drone_bind" {
            Some(resident.drone_binding_admission.lock().await)
        } else {
            None
        };
        let mut descriptor = resident.descriptor.lock().await;
        anyhow::ensure!(
            descriptor.root_session_id.as_deref() == Some(request.root_session_id.as_str())
                && descriptor.root_active_session_id == request.active_session_id
                && descriptor.worker_instance_id.as_deref()
                    == Some(request.worker_instance_id.as_str()),
            "private_owner_target_mismatch"
        );
        let revision = descriptor
            .create_command
            .rest
            .get("droneBindingRevision")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        anyhow::ensure!(
            revision == request.expected_binding_revision,
            "private_owner_conflict"
        );
        if request.operation == "resident_drone_control" {
            let identity = super::controls::ControlIdentity {
                root_session_id: request.root_session_id,
                active_session_id: request.active_session_id,
                worker_instance_id: request.worker_instance_id,
                supervisor_generation: request.supervisor_generation,
            };
            // The shared control takes its own action fence and revalidates the resident.
            // Never hold a descriptor guard across that lock or provider settlement.
            drop(descriptor);
            let data = self
                .control_drone(
                    &identity,
                    request.action.as_deref().unwrap(),
                    request.expected_revision,
                )
                .await?;
            return Ok(crate::protocol::response_success(
                None,
                &request.operation,
                Some(data),
            ));
        }
        let mut next = descriptor.clone();
        if request.operation == "resident_drone_bind" {
            anyhow::ensure!(
                !resident
                    .intentional_stop
                    .load(std::sync::atomic::Ordering::SeqCst)
                    && descriptor.stop_requested_at.is_none(),
                "private_owner_authority_denied"
            );
            let path = request
                .drone_roadmap_path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("private_owner_invalid_request"))?;
            let cwd = descriptor
                .create_command
                .rest
                .get("cwd")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("private_owner_scope_missing"))?;
            let path_buf = std::path::Path::new(path);
            anyhow::ensure!(
                path_buf.is_absolute()
                    && path_buf.canonicalize()? == path_buf
                    && path_buf.starts_with(std::path::Path::new(cwd).canonicalize()?),
                "private_owner_path_scope"
            );
            let evidence = roadmap::read_bound(Some(path), std::path::Path::new(cwd))?;
            anyhow::ensure!(
                request.expected_sha256.as_deref() == evidence["sha256"].as_str()
                    && request.expected_roadmap_revision.as_deref()
                        == evidence["revision"].as_str(),
                "private_owner_stale_roadmap"
            );
            next.create_command
                .rest
                .insert("droneRoadmapPath".into(), json!(path));
            next.create_command.rest.insert(
                "droneBindingRevision".into(),
                json!(revision
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("private_owner_conflict"))?),
            );
            crate::descriptor::persist_worker(&resident.descriptor_path, &next)?;
            *descriptor = next;
        }
        let path = descriptor
            .create_command
            .rest
            .get("droneRoadmapPath")
            .and_then(Value::as_str);
        let cwd = descriptor
            .create_command
            .rest
            .get("cwd")
            .and_then(Value::as_str)
            .unwrap_or("");
        let binding = match roadmap::read_bound(path, std::path::Path::new(cwd)) {
            Ok(evidence) => {
                json!({"state":"bound","path":evidence["path"],"sha256":evidence["sha256"],"roadmapRevision":evidence["revision"]})
            }
            Err(_) => {
                json!({"state":if path.is_none() {"missing_binding"} else {"unreadable_binding"}})
            }
        };
        let status = self.drone.lock().unwrap().status(&request.root_session_id);
        let mut data = json!({"rootSessionId":request.root_session_id,"activeSessionId":request.active_session_id,"workerInstanceId":request.worker_instance_id,"supervisorGeneration":self.owner_cleanup_generation,"bindingRevision":descriptor.create_command.rest.get("droneBindingRevision").and_then(Value::as_u64).unwrap_or(0),"binding":binding,"slot":status});
        drop(descriptor);
        if request.operation == "resident_drone_status" {
            let identity = super::controls::ControlIdentity {
                root_session_id: request.root_session_id,
                active_session_id: request.active_session_id,
                worker_instance_id: request.worker_instance_id,
                supervisor_generation: request.supervisor_generation,
            };
            // Preserve the legacy status envelope even when root controls are unavailable.
            data["controls"] = self
                .control_drone(&identity, "status", None)
                .await
                .unwrap_or(Value::Null);
        }
        Ok(crate::protocol::response_success(
            None,
            &request.operation,
            Some(data),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn drone_action_admission_fences_rebind_without_blocking_event_reader() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let supervisor = Arc::new(
            Supervisor::new(crate::supervisor::SupervisorOptions {
                socket_path: dir.path().join("daemon.sock"),
                agent_dir: dir.path().join("agent"),
            })
            .unwrap(),
        );
        supervisor.drone_controls.resume_for_test("durable");
        let path = dir.path().join("ROADMAP.md");
        std::fs::write(&path, "owned plan").unwrap();
        let roadmap = roadmap::read_bound(path.to_str(), dir.path()).unwrap();
        let descriptor:crate::protocol::DaemonWorkerDescriptor=serde_json::from_value(json!({"version":2,"workerId":"worker","pid":0,"socketPath":"/unused","recoveryJournalPath":"/unused","supervisorSocketPath":"/unused","authenticationToken":"fixture","rootActiveSessionId":"active","rootSessionId":"durable","workerInstanceId":"incarnation","createdAt":"t","updatedAt":"t","lifecycle":"ready","createCommand":{"cwd":dir.path(),"droneRoadmapPath":path,"droneBindingRevision":0},"consecutiveFailures":0})).unwrap();
        let resident = crate::registry::ResidentWorker::new(
            "worker".into(),
            descriptor.clone(),
            dir.path().join("descriptor.json"),
        );
        supervisor.registry.insert(resident.clone()).await;
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        *resident.cmd_tx.lock().await = Some(tx);
        let assessment = supervisor
            .drone
            .lock()
            .unwrap()
            .push("durable", Value::Null)
            .unwrap();
        let action = super::super::handlers::Action {
            handler: "notify_root".into(),
            args: json!({"message":"owned diagnostic"}),
        };
        let snapshot = json!({"activeSessionId":"active"});
        let decision = supervisor.apply_drone_decision(
            &resident,
            &assessment,
            &descriptor,
            path.to_str(),
            dir.path(),
            &roadmap,
            &snapshot,
            None,
            None,
            action.clone(),
        );
        tokio::pin!(decision);
        let snapshot_request = tokio::select! { result=&mut decision=>panic!("premature {result:?}"), request=rx.recv()=>request.unwrap() };
        assert_eq!(snapshot_request.command_type, "drone_snapshot");
        // A worker emits session_event frames before its response. The reader
        // must acquire the descriptor to consume that event and then the reply.
        let reader = Arc::clone(&resident);
        let event_reader = tokio::spawn(async move {
            let _descriptor = reader.descriptor.lock().await;
        });
        tokio::time::timeout(std::time::Duration::from_millis(500), event_reader)
            .await
            .expect("event reader blocked by Drone snapshot route")
            .unwrap();
        resident
            .pending
            .lock()
            .await
            .remove(&snapshot_request.request_id)
            .unwrap()
            .send(crate::registry::WorkerReply::Typed(
                crate::protocol::response_success(
                    None,
                    "drone_snapshot",
                    Some(json!({
                        "activeSessionId":"active", "sessionId":"durable", "paused":false,
                        "stopped":false, "goal":{"status":"active"}
                    })),
                ),
            ))
            .unwrap_or_else(|_| panic!("snapshot reply receiver missing"));
        let sent = tokio::select! { result=&mut decision=>panic!("premature {result:?}"), request=rx.recv()=>request.unwrap() };
        assert_eq!(sent.command_type, "follow_up");
        let reader = Arc::clone(&resident);
        let event_reader = tokio::spawn(async move {
            let _descriptor = reader.descriptor.lock().await;
        });
        tokio::time::timeout(std::time::Duration::from_millis(500), event_reader)
            .await
            .expect("event reader blocked by Drone action route")
            .unwrap();
        let bind = json!({"operation":"resident_drone_bind","rootSessionId":"durable","activeSessionId":"active","workerInstanceId":"incarnation","supervisorGeneration":supervisor.owner_cleanup_generation,"expectedBindingRevision":0,"droneRoadmapPath":path,"expectedSha256":roadmap["sha256"],"expectedRoadmapRevision":roadmap["revision"]});
        let rebind = supervisor.resident_drone_request(bind);
        tokio::pin!(rebind);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut rebind)
                .await
                .is_err()
        );
        let reply = resident
            .pending
            .lock()
            .await
            .remove(&sent.request_id)
            .unwrap();
        reply
            .send(crate::registry::WorkerReply::Typed(
                crate::protocol::response_success(None, "follow_up", None),
            ))
            .unwrap_or_else(|_| panic!("reply receiver missing"));
        decision.await.unwrap();
        assert!(rebind.await.unwrap().success);
        // Bind first: the exact same prior decision must not reach the channel.
        assert!(supervisor
            .apply_drone_decision(
                &resident,
                &assessment,
                &descriptor,
                path.to_str(),
                dir.path(),
                &roadmap,
                &snapshot,
                None,
                None,
                action
            )
            .await
            .is_err());
        assert!(rx.try_recv().is_err());
        resident.descriptor.lock().await.stop_requested_at = Some("stopped".into());
        assert!(resident.descriptor.lock().await.stop_requested_at.is_some());
    }
    #[tokio::test]
    async fn resident_drone_bind_preserves_identity_and_rejects_stale_foreign_scope() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor = Arc::new(
            Supervisor::new(crate::supervisor::SupervisorOptions {
                socket_path: dir.path().join("daemon.sock"),
                agent_dir: dir.path().join("agent"),
            })
            .unwrap(),
        );
        let roadmap_path = dir.path().join("ROADMAP.md");
        std::fs::write(&roadmap_path, "current owner plan").unwrap();
        let evidence = roadmap::read(roadmap_path.to_str()).unwrap();
        let descriptor: crate::protocol::DaemonWorkerDescriptor = serde_json::from_value(json!({
            "version":2,"workerId":"worker","pid":0,"socketPath":"/unused","recoveryJournalPath":"/unused",
            "supervisorSocketPath":"/unused","authenticationToken":"fixture","rootActiveSessionId":"active",
            "rootSessionId":"durable","workerInstanceId":"incarnation","sessionFile":"/sessions/history.jsonl",
            "createdAt":"t","updatedAt":"t","lifecycle":"ready","createCommand":{"cwd":dir.path()},"consecutiveFailures":0
        })).unwrap();
        let resident = crate::registry::ResidentWorker::new(
            "worker".into(),
            descriptor.clone(),
            dir.path().join("descriptor.json"),
        );
        supervisor.registry.insert(resident.clone()).await;
        let payload = json!({"operation":"resident_drone_bind","rootSessionId":"durable","activeSessionId":"active",
            "workerInstanceId":"incarnation","supervisorGeneration":supervisor.owner_cleanup_generation,
            "expectedBindingRevision":0,"droneRoadmapPath":roadmap_path,"expectedSha256":evidence["sha256"],"expectedRoadmapRevision":evidence["revision"]});
        let mut wrong = payload.clone();
        wrong["rootSessionId"] = json!("foreign");
        assert!(supervisor.resident_drone_request(wrong).await.is_err());
        wrong = payload.clone();
        wrong["workerInstanceId"] = json!("stale");
        assert!(supervisor.resident_drone_request(wrong).await.is_err());
        wrong = payload.clone();
        wrong["supervisorGeneration"] = json!("stale");
        assert!(supervisor.resident_drone_request(wrong).await.is_err());
        wrong = payload.clone();
        wrong["expectedSha256"] = json!("stale");
        assert!(supervisor.resident_drone_request(wrong).await.is_err());
        wrong = payload.clone();
        wrong["droneRoadmapPath"] = json!("relative.md");
        assert!(supervisor.resident_drone_request(wrong).await.is_err());
        wrong = payload.clone();
        wrong["droneRoadmapPath"] = json!("/etc/hosts");
        assert!(supervisor.resident_drone_request(wrong).await.is_err());
        resident
            .intentional_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(supervisor
            .resident_drone_request(payload.clone())
            .await
            .is_err());
        resident
            .intentional_stop
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let response = supervisor
            .resident_drone_request(payload.clone())
            .await
            .unwrap();
        assert_eq!(response.data.unwrap()["bindingRevision"], 1);
        assert!(supervisor.resident_drone_request(payload).await.is_err());
        let mut expected = descriptor.clone();
        expected
            .create_command
            .rest
            .insert("droneRoadmapPath".into(), json!(roadmap_path));
        expected
            .create_command
            .rest
            .insert("droneBindingRevision".into(), json!(1));
        assert_eq!(*resident.descriptor.lock().await, expected);
        let persisted: crate::protocol::DaemonWorkerDescriptor =
            serde_json::from_slice(&std::fs::read(&resident.descriptor_path).unwrap()).unwrap();
        // The established descriptor writer stamps updatedAt on disk.
        let mut expected_persisted = expected.clone();
        expected_persisted.updated_at = persisted.updated_at.clone();
        assert_eq!(persisted, expected_persisted);
        let status = json!({"operation":"resident_drone_status","rootSessionId":"durable","activeSessionId":"active",
            "workerInstanceId":"incarnation","supervisorGeneration":supervisor.owner_cleanup_generation,"expectedBindingRevision":1});
        assert_eq!(
            supervisor
                .resident_drone_request(status)
                .await
                .unwrap()
                .data
                .unwrap()["binding"]["state"],
            "bound"
        );
        assert_eq!(*resident.descriptor.lock().await, expected);
        // A bind waits behind the same descriptor fence used by admitted actions.
        let guard = resident.descriptor.lock().await;
        let mut rebind = json!({"operation":"resident_drone_bind","rootSessionId":"durable","activeSessionId":"active","workerInstanceId":"incarnation","supervisorGeneration":supervisor.owner_cleanup_generation,"expectedBindingRevision":1,"droneRoadmapPath":roadmap_path,"expectedSha256":evidence["sha256"],"expectedRoadmapRevision":evidence["revision"]});
        let future = supervisor.resident_drone_request(rebind.clone());
        tokio::pin!(future);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut future)
                .await
                .is_err()
        );
        drop(guard);
        assert!(future.await.unwrap().success);
        assert_eq!(
            resident.descriptor.lock().await.create_command.rest["droneBindingRevision"],
            2
        );
        rebind["expectedBindingRevision"] = json!(1);
        assert!(supervisor.resident_drone_request(rebind).await.is_err());
        #[cfg(unix)]
        {
            let outside = tempfile::tempdir().unwrap();
            let sentinel = outside.path().join("sentinel");
            std::fs::write(&sentinel, "foreign").unwrap();
            std::fs::remove_file(&roadmap_path).unwrap();
            std::os::unix::fs::symlink(&sentinel, &roadmap_path).unwrap();
            let status = json!({"operation":"resident_drone_status","rootSessionId":"durable","activeSessionId":"active","workerInstanceId":"incarnation","supervisorGeneration":supervisor.owner_cleanup_generation,"expectedBindingRevision":2});
            let response = supervisor.resident_drone_request(status).await.unwrap();
            assert_eq!(
                response.data.unwrap()["binding"]["state"],
                "unreadable_binding"
            );
        }
    }
}

#[cfg(test)]
mod control_tests;
