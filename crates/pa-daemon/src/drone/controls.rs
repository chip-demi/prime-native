//! Explicit user controls for one durable root. No goal, heartbeat or worker is stopped.
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{ensure, Result};
use pa_core::session_engine::drone::AssessmentControl;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use super::{registry::Assessment, Supervisor};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DurableControl {
    version: u32,
    root_session_id: String,
    revision: u64,
    user_stopped: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_cancellation: Option<PendingCancellation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PendingCancellation {
    run_id: String,
    assessment_id: String,
    unconfirmed: bool,
}

struct ActiveAssessment {
    id: String,
    assessment_id: String,
    control: AssessmentControl,
    settled: watch::Sender<bool>,
}

struct State {
    durable: DurableControl,
    active: Option<ActiveAssessment>,
    cancellation_unconfirmed: bool,
}

impl State {
    fn enabled(&self) -> bool {
        !self.durable.user_stopped
            && self.durable.pending_cancellation.is_none()
            && !self.cancellation_unconfirmed
    }
}

impl ActiveAssessment {
    fn pending_cancellation(&self, unconfirmed: bool) -> PendingCancellation {
        PendingCancellation {
            run_id: self.id.clone(),
            assessment_id: self.assessment_id.clone(),
            unconfirmed,
        }
    }
}

pub(crate) struct DroneControls {
    agent_dir: PathBuf,
    roots: Mutex<HashMap<String, Arc<RootControl>>>,
}

/// Control-plane authority on one connection, pinned to a current resident incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControlIdentity {
    pub(crate) active_session_id: String,
    pub(crate) root_session_id: String,
    pub(crate) worker_instance_id: String,
    pub(crate) supervisor_generation: String,
}

struct RootControl {
    path: PathBuf,
    state: Mutex<State>,
}

/// One tracked assessment. Completion is explicit; dropping a task is not settlement proof.
pub(super) struct AssessmentRun {
    root: Arc<RootControl>,
    id: String,
    pub(super) control: AssessmentControl,
}

fn control_path(agent_dir: &Path, root: &str) -> Result<PathBuf> {
    ensure!(
        !root.is_empty() && root.len() <= 512,
        "drone_control_identity"
    );
    let directory = agent_dir.join("drone-controls");
    if let Ok(meta) = std::fs::symlink_metadata(&directory) {
        ensure!(
            meta.is_dir() && !meta.file_type().is_symlink(),
            "drone_control_scope"
        );
        ensure!(
            directory.canonicalize()?.parent() == Some(agent_dir.canonicalize()?.as_path()),
            "drone_control_scope"
        );
    }
    // Imported legacy conversations may have non-UUID IDs. Never put caller text in a path.
    Ok(directory.join(format!("{:x}.json", Sha256::digest(root.as_bytes()))))
}

fn read_control(path: &Path, root: &str) -> Result<DurableControl> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DurableControl {
                version: 1,
                root_session_id: root.into(),
                revision: 0,
                // A roadmap binding is evidence, not a user's opt-in. Only an explicit
                // resume writes an enabled choice; existing durable choices stay intact.
                user_stopped: true,
                pending_cancellation: None,
            });
        }
        Err(error) => return Err(error.into()),
    };
    ensure!(
        meta.is_file() && !meta.file_type().is_symlink() && meta.len() <= 4096,
        "drone_control_invalid"
    );
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(4097)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 4096, "drone_control_invalid");
    let record: DurableControl = serde_json::from_slice(&bytes)?;
    ensure!(
        record.version == 1 && record.root_session_id == root,
        "drone_control_identity"
    );
    Ok(record)
}

impl DroneControls {
    pub(crate) fn new(agent_dir: PathBuf) -> Self {
        Self {
            agent_dir,
            roots: Mutex::new(HashMap::new()),
        }
    }

    fn root(&self, root: &str) -> Result<Arc<RootControl>> {
        let mut roots = self.roots.lock().unwrap();
        if let Some(control) = roots.get(root) {
            return Ok(Arc::clone(control));
        }
        let path = control_path(&self.agent_dir, root)?;
        let durable = read_control(&path, root)?;
        let control = Arc::new(RootControl {
            path,
            state: Mutex::new(State {
                durable,
                active: None,
                cancellation_unconfirmed: false,
            }),
        });
        roots.insert(root.into(), Arc::clone(&control));
        Ok(control)
    }

    pub(crate) fn enabled(&self, root: &str) -> Result<bool> {
        let control = self.root(root)?;
        control.refresh()?;
        let state = control.state.lock().unwrap();
        Ok(state.enabled())
    }

    pub(super) fn begin(&self, assessment: &Assessment) -> Result<Option<AssessmentRun>> {
        let root = self.root(&assessment.root)?;
        root.refresh()?;
        let mut state = root.state.lock().unwrap();
        if !state.enabled() || state.active.is_some() {
            return Ok(None);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let assessment_id = format!(
            "{}:{}:{}",
            assessment.epoch, assessment.sequence, assessment.page_cursor
        );
        let control = AssessmentControl::default();
        let (settled, _) = watch::channel(false);
        state.active = Some(ActiveAssessment {
            id: id.clone(),
            assessment_id,
            control: control.clone(),
            settled,
        });
        drop(state);
        Ok(Some(AssessmentRun { root, id, control }))
    }

    #[cfg(test)]
    pub(crate) fn status(&self, root: &str) -> Result<Value> {
        let control = self.root(root)?;
        control.status()
    }

    #[cfg(test)]
    pub(crate) fn resume_for_test(&self, root: &str) {
        // Unit fixtures exercise a specific assessment/action themselves. Make
        // the real durable opt-in without scheduling a second assessment.
        let control = self.root(root).unwrap();
        assert_eq!(control.status().unwrap()["revision"], 0);
        control.mutate("resume", 0).unwrap();
    }
}

impl RootControl {
    // Refresh only on an existing call, never with a new watcher. A confirmed owner may have
    // durably settled a pending cancellation after another reader opened this control record.
    fn refresh(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let disk = read_control(&self.path, &state.durable.root_session_id)?;
        ensure!(
            disk.revision >= state.durable.revision,
            "drone_control_conflict"
        );
        ensure!(
            disk.revision != state.durable.revision || disk == state.durable,
            "drone_control_conflict"
        );
        state.durable = disk;
        Ok(())
    }

    fn persist(&self, state: &mut State, mut next: DurableControl) -> Result<()> {
        ensure!(
            read_control(&self.path, &state.durable.root_session_id)? == state.durable,
            "drone_control_conflict"
        );
        next.revision = state
            .durable
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("drone_control_conflict"))?;
        crate::descriptor::write_file_atomic(&self.path, &serde_json::to_string(&next)?)?;
        state.durable = next;
        Ok(())
    }

    fn status(&self) -> Result<Value> {
        self.refresh()?;
        let state = self.state.lock().unwrap();
        let pending = state.durable.pending_cancellation.as_ref();
        let unconfirmed = state.cancellation_unconfirmed
            || pending.is_some_and(|pending| {
                pending.unconfirmed
                    || state
                        .active
                        .as_ref()
                        .is_none_or(|active| active.id != pending.run_id)
            });
        // inFlight is an outstanding assessment obligation, not proof that an UNKNOWN producer
        // is currently running. The unconfirmed flag preserves that distinction after restart.
        let in_flight = state.active.is_some() || pending.is_some();
        let pending_cancel = pending.is_some()
            || state
                .active
                .as_ref()
                .is_some_and(|active| active.control.is_cancelled());
        let status = if unconfirmed {
            "cancellation_unconfirmed"
        } else if state.durable.user_stopped && in_flight {
            "stopping"
        } else if state.durable.user_stopped {
            "paused"
        } else {
            "enabled"
        };
        Ok(
            json!({"rootSessionId":state.durable.root_session_id,"revision":state.durable.revision,
            "userStopped":state.durable.user_stopped,"enabled":state.enabled(),
            "paused":state.durable.user_stopped && !in_flight && !unconfirmed,
            "state":status,"inFlight":in_flight,"pendingCancel":pending_cancel,
            "cancellationUnconfirmed":unconfirmed}),
        )
    }

    fn mutate(&self, action: &str, revision: u64) -> Result<Option<watch::Receiver<bool>>> {
        self.refresh()?;
        let mut state = self.state.lock().unwrap();
        ensure!(revision == state.durable.revision, "drone_control_conflict");
        let mut next = state.durable.clone();
        match action {
            "stop" => {
                next.user_stopped = true;
                if let Some(active) = &state.active {
                    if let Some(pending) = &next.pending_cancellation {
                        ensure!(pending.run_id == active.id, "drone_control_cancel_pending");
                    } else {
                        // Stop and its pending obligation are one atomic durable write before
                        // cancellation. A crash between this write and settle cannot report paused.
                        next.pending_cancellation = Some(active.pending_cancellation(false));
                    }
                }
            }
            "resume" => {
                ensure!(
                    state.active.is_none()
                        && next.pending_cancellation.is_none()
                        && !state.cancellation_unconfirmed,
                    "drone_control_cancel_pending"
                );
                next.user_stopped = false;
            }
            _ => anyhow::bail!("drone_control_invalid_action"),
        }
        // Persist an explicit stop even when the absent-record default is already paused.
        if next != state.durable || state.durable.revision == 0 {
            self.persist(&mut state, next)?;
        }
        if action == "stop" {
            if let Some(active) = &state.active {
                let receiver = active.settled.subscribe();
                active.control.cancel();
                return Ok(Some(receiver));
            }
        }
        Ok(None)
    }

    fn mark_cancel_unconfirmed(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(active) = &state.active {
            if active.control.is_cancelled() {
                let pending = active.pending_cancellation(true);
                state.cancellation_unconfirmed = true;
                let mut next = state.durable.clone();
                next.pending_cancellation = Some(pending);
                if next != state.durable {
                    self.persist(&mut state, next)?;
                }
            }
        }
        Ok(())
    }
}

impl AssessmentRun {
    pub(super) fn finish(self, confirmed: bool) -> Result<()> {
        let mut state = self.root.state.lock().unwrap();
        let Some(active) = state.active.as_ref().filter(|active| active.id == self.id) else {
            anyhow::bail!("drone_control_assessment_changed");
        };
        if confirmed {
            if let Some(pending) = &state.durable.pending_cancellation {
                // Only this assessment's verified settlement may clear its durable obligation.
                ensure!(
                    pending.run_id == self.id,
                    "drone_control_assessment_changed"
                );
                let mut next = state.durable.clone();
                next.pending_cancellation = None;
                if let Err(error) = self.root.persist(&mut state, next) {
                    state.cancellation_unconfirmed = true;
                    return Err(error);
                }
            }
            if let Some(active) = state.active.take() {
                state.cancellation_unconfirmed = false;
                let _ = active.settled.send(true);
            }
        } else {
            let pending = active.pending_cancellation(true);
            state.cancellation_unconfirmed = true;
            let mut next = state.durable.clone();
            if let Some(prior) = &next.pending_cancellation {
                ensure!(prior.run_id == self.id, "drone_control_assessment_changed");
            }
            next.pending_cancellation = Some(pending);
            if next != state.durable {
                self.root.persist(&mut state, next)?;
            }
        }
        Ok(())
    }
}

fn descriptor_is_root(descriptor: &crate::protocol::DaemonWorkerDescriptor, active: &str) -> bool {
    let rest = &descriptor.create_command.rest;
    let depth_is_root = match rest.get("rlmDepth") {
        None | Some(Value::Null) => true,
        Some(depth) => depth.as_u64() == Some(0),
    };
    let no_child_id = rest.get("rlmChildId").is_none_or(Value::is_null);
    let root_metadata = match rest.get("runtimeMetadata") {
        None | Some(Value::Null) => true,
        Some(Value::Object(metadata)) => {
            metadata.get("kind").is_none_or(|kind| kind == "top-level")
                && ["rlmChildId", "parentActiveSessionId", "parentSessionId"]
                    .iter()
                    .all(|key| metadata.get(*key).is_none_or(Value::is_null))
        }
        _ => false,
    };
    // At depth zero parentSessionPath records fork/create provenance, not a family edge.
    descriptor.root_active_session_id == active && depth_is_root && no_child_id && root_metadata
}

impl Supervisor {
    /// Local attached-client controls; session-plane grants never reach this handler.
    pub(crate) async fn drone_control_identity(&self, active: &str) -> Result<ControlIdentity> {
        let resident = self.registry.resolve(active).await?;
        ensure!(
            !resident.identity_quarantined() && !self.is_stopping(&resident),
            "drone_control_unavailable"
        );
        let descriptor = resident.descriptor.lock().await;
        ensure!(
            descriptor_is_root(&descriptor, active),
            "drone_control_identity"
        );
        Ok(ControlIdentity {
            active_session_id: active.into(),
            root_session_id: descriptor
                .root_session_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("drone_control_identity"))?,
            worker_instance_id: descriptor
                .worker_instance_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("drone_control_identity"))?,
            supervisor_generation: self.owner_cleanup_generation.clone(),
        })
    }

    pub(crate) async fn control_drone(
        self: &Arc<Self>,
        identity: &ControlIdentity,
        action: &str,
        revision: Option<u64>,
    ) -> Result<Value> {
        let active = identity.active_session_id.as_str();
        let root = identity.root_session_id.as_str();
        ensure!(
            matches!(action, "status" | "stop" | "resume"),
            "drone_control_invalid_action"
        );
        ensure!(
            (action == "status") == revision.is_none(),
            "drone_control_revision_required"
        );
        let resident = self.registry.resolve(active).await?;
        ensure!(
            !resident.identity_quarantined() && !self.is_stopping(&resident),
            "drone_control_unavailable"
        );
        let admission = resident.drone_binding_admission.lock().await;
        {
            let descriptor = resident.descriptor.lock().await;
            ensure!(
                identity.supervisor_generation == self.owner_cleanup_generation
                    && descriptor.worker_instance_id.as_deref()
                        == Some(identity.worker_instance_id.as_str())
                    && descriptor.root_session_id.as_deref() == Some(root)
                    && descriptor_is_root(&descriptor, active),
                "drone_control_identity"
            );
        }
        let control = self.drone_controls.root(root)?;
        let was_stopped = control.status()?["userStopped"] == true;
        let wait = if let Some(revision) = revision {
            let wait = control.mutate(action, revision)?;
            if action == "stop" {
                self.drone.lock().unwrap().stop(root);
            } else if was_stopped {
                self.drone.lock().unwrap().resume_explicit(root);
            }
            wait
        } else {
            None
        };
        drop(admission);
        if let Some(mut settled) = wait {
            let confirmed = tokio::time::timeout(std::time::Duration::from_secs(6), async {
                while !*settled.borrow_and_update() {
                    if settled.changed().await.is_err() {
                        return false;
                    }
                }
                true
            })
            .await
                == Ok(true);
            if !confirmed {
                control.mark_cancel_unconfirmed()?;
            }
        }
        if action == "resume" && was_stopped {
            let assessment = self
                .drone
                .lock()
                .unwrap()
                .push(root, json!({"type":"drone_control_resume"}));
            if let Some(assessment) = assessment {
                self.start_drone_chain(Arc::clone(&resident), assessment);
            } else {
                self.wake_drone_chain(Arc::clone(&resident), root.into());
            }
        }
        let mut status = control.status()?;
        status["activeSessionId"] = json!(active);
        status["workerInstanceId"] = json!(identity.worker_instance_id);
        status["supervisorGeneration"] = json!(identity.supervisor_generation);
        status["observer"] = self.drone.lock().unwrap().status(root);
        Ok(status)
    }
}

#[cfg(test)]
mod tests;
