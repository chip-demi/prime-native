//! Supervisor-owned Drone lifecycle. No session, heartbeat, kernel or child worker is created.
mod controls;
pub(crate) use controls::{ControlIdentity, DroneControls};
mod evidence;
mod handlers;
mod parallel;
mod registry;
mod resident;
pub(crate) use resident::validate_resident_request;
mod roadmap;
mod scripts;
use crate::{backpressure::RouteAdmission, registry::ResidentWorker, supervisor::Supervisor};
pub(crate) use registry::DroneRegistry;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};

fn registered_check_manifest(name: &str, script: &scripts::RegisteredScript) -> Value {
    // The model chooses the outer Action's args, not the script's stdin map.
    // branch_facts receives its snapshot from Native after action selection.
    let args = if name == "branch_facts" {
        json!({"name":name})
    } else {
        let arguments = script
            .argument_names
            .iter()
            .map(|key| (key.clone(), json!("string")))
            .collect::<serde_json::Map<String, Value>>();
        json!({"name":name,"arguments":arguments})
    };
    let mut entry = json!({
        "name":"run_registered_check", "version":1, "registeredName":name,
        "effect":"read_only", "runner":"fixed /usr/bin/python3 -I -; pinned Native bytes stdin",
        "args":args, "timeoutMs":script.timeout.as_millis(), "outputCap":script.output_cap
    });
    if name == "branch_facts" {
        entry["argumentSource"] = json!("Native injects the trusted snapshot; send only args.name, never snapshot or arguments.");
    } else {
        entry["argumentNames"] = json!(script.argument_names);
    }
    entry
}

// Observer-only resolution. All supplied parent edges must name one row at
// exactly the preceding RLM depth; missing or conflicting edges fail closed.
fn drone_family_root(
    roster: &crate::agent_roster::AgentRoster,
    active: &str,
) -> Option<(String, String)> {
    use crate::agent_messaging::{same_session_file, FamilyIdentity};
    let mut row = roster.by_active_session_id(active)?.summary.clone();
    let mut visited = std::collections::HashSet::new();
    loop {
        let id = row["activeSessionId"].as_str()?;
        if !visited.insert(id.to_owned()) {
            return None;
        }
        let identity = FamilyIdentity::from_summary(Some(&row), id);
        if identity.rlm_depth == 0 {
            if row["runtimeKind"] == "subagent"
                || identity.parent_active_session_id.is_some()
                || identity.parent_session_id.is_some()
            {
                return None;
            }
            return Some((row["sessionId"].as_str()?.to_owned(), id.to_owned()));
        }
        let mut parents = Vec::new();
        if let Some(parent) = identity.parent_active_session_id.as_deref() {
            parents.push(roster.by_active_session_id(parent)?.summary.clone());
        }
        if let Some(path) = identity.parent_session_path.as_deref() {
            let canonical = crate::lease::canonical_session_path(std::path::Path::new(path));
            let exact = roster.by_session_file(&canonical.to_string_lossy());
            let matches = roster
                .entries()
                .into_iter()
                .filter(|entry| {
                    entry.summary["sessionFile"]
                        .as_str()
                        .is_some_and(|file| same_session_file(file, path))
                })
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return None;
            }
            let parent = exact.unwrap_or(&matches[0]);
            parents.push(parent.summary.clone());
        }
        if let Some(parent) = identity.parent_session_id.as_deref() {
            let matches = roster
                .entries()
                .into_iter()
                .filter(|entry| entry.summary["sessionId"] == parent)
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return None;
            }
            parents.push(matches[0].summary.clone());
        }
        let parent = parents.first()?;
        let parent_id = parent["activeSessionId"].as_str()?;
        if parents
            .iter()
            .any(|candidate| candidate["activeSessionId"] != parent_id)
        {
            return None;
        }
        let parent_identity = FamilyIdentity::from_summary(Some(parent), parent_id);
        if parent_identity.rlm_depth.checked_add(1) != Some(identity.rlm_depth) {
            return None;
        }
        row = parent.clone();
    }
}

impl Supervisor {
    pub(crate) async fn observe_drone_event(
        self: &Arc<Self>,
        resident: &Arc<ResidentWorker>,
        connection_epoch: u64,
        active: &str,
        event: Value,
    ) {
        if !resident.connection_is_current(connection_epoch) {
            return;
        }
        let (root, root_active) = {
            let roster = self.roster.lock().unwrap();
            let Some(root) = drone_family_root(&roster, active) else {
                return;
            };
            root
        };
        let Ok(target) = self.registry.resolve(&root_active).await else {
            return;
        };
        if !self.drone_controls.enabled(&root).unwrap_or(false) {
            return;
        }
        if target.intentional_stop.load(Ordering::SeqCst) {
            self.drone.lock().unwrap().stop(&root);
            return;
        }
        let descriptor = target.descriptor.lock().await.clone();
        if descriptor.stop_requested_at.is_some() {
            self.drone.lock().unwrap().stop(&root);
            return;
        }
        if !has_roadmap_binding(&descriptor) {
            self.drone.lock().unwrap().suspend_ineligible(&root);
            return;
        }
        if !self
            .drone
            .lock()
            .unwrap()
            .scripts
            .contains_key("branch_facts")
        {
            let directory = self.options.agent_dir.join("drone-handlers");
            if tokio::fs::create_dir_all(&directory).await.is_ok() {
                const CHECK:&[u8]=b"#!/usr/bin/python3\nimport json,sys\ns=json.loads(sys.argv[1])\nprint(json.dumps({'queuedInputs':s.get('queuedInputs'),'ownedBackgroundProcess':s.get('hasBackgroundBash'),'activeChildren':s.get('hasRunningChildren'),'request':s.get('request'),'evidenceOrigin':'native_snapshot','physicalQA':'capability_missing'}))\n";
                let executable = directory.join("branch-facts-v1.py");
                let ready = if executable.exists() {
                    tokio::fs::read(&executable).await.ok().as_deref() == Some(CHECK)
                } else {
                    tokio::fs::write(&executable, CHECK).await.is_ok()
                };
                if ready {
                    self.drone.lock().unwrap().scripts.insert(
                        "branch_facts".into(),
                        scripts::RegisteredScript {
                            executable,
                            expected_bytes: CHECK.to_vec(),
                            execution_root: directory,
                            argument_names: vec!["snapshot".into()],
                            timeout: std::time::Duration::from_secs(2),
                            output_cap: 4096,
                        },
                    );
                }
            }
        }
        let event_kind = event["type"].as_str().unwrap_or("unknown");
        let selected = json!({"message":event["message"],"result":event["result"],"partialResult":event["partialResult"],"error":event["error"],"request":event["request"]});
        let selected = serde_json::to_string(&selected).unwrap_or_default();
        let outcome = if event_kind == "message_end" && event["message"]["role"] == "assistant" {
            let content = &event["message"]["content"];
            let text = content.as_str().map_or_else(
                || {
                    content
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|part| part["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                },
                str::to_owned,
            );
            json!({"text":text,"stopReason":event["message"]["stopReason"]})
        } else {
            json!({"result":event["result"],"error":event["error"],"exitCode":event["exitCode"]})
        };
        let evidence_sha256 = {
            use sha2::{Digest, Sha256};
            format!("{:x}", Sha256::digest(outcome.to_string().as_bytes()))
        };
        let evidence = selected.chars().take(2048).collect::<String>();
        let delta = json!({"activeSessionId":active,"type":event_kind,"untrustedEvidence":evidence,
    "evidenceSha256":evidence_sha256,"messageRole":event["message"]["role"],
    "toolCallId":event["toolCallId"],"isError":event["isError"],
    "messageId":event["messageId"],"exitCode":event["exitCode"]});
        let assessment = self.drone.lock().unwrap().push(&root, delta);
        if let Some(assessment) = assessment {
            self.start_drone_chain(target, assessment);
        } else {
            self.wake_drone_chain(target, root);
        }
    }
    fn wake_drone_chain(self: &Arc<Self>, target: Arc<ResidentWorker>, root: String) {
        if !self.drone_controls.enabled(&root).unwrap_or(false) {
            return;
        }
        let (delay, epoch) = {
            let mut drone = self.drone.lock().unwrap();
            (drone.wake_delay(&root), drone.epoch(&root))
        };
        if let (Some(delay), Some(epoch)) = (delay, epoch) {
            let supervisor = Arc::clone(self);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let next = supervisor
                    .drone
                    .lock()
                    .unwrap()
                    .wake_for_epoch(&root, epoch);
                if let Some(next) = next {
                    supervisor.start_drone_chain(target, next);
                }
            });
        }
    }
    fn start_drone_chain(
        self: &Arc<Self>,
        target: Arc<ResidentWorker>,
        assessment: registry::Assessment,
    ) {
        let Ok(Some(run)) = self.drone_controls.begin(&assessment) else {
            self.drone.lock().unwrap().cancel_ineligible(&assessment);
            return;
        };
        let supervisor = Arc::clone(self);
        tokio::spawn(async move {
            let control = run.control.clone();
            let assessed_binding = target.descriptor.lock().await.clone();
            let result = supervisor
                .run_drone_assessment(&target, &assessment, &control)
                .await;
            let unconfirmed = result.as_ref().is_err_and(|error| {
                error
                    .downcast_ref::<pa_core::session_engine::drone::DroneProviderOutcomeUnknown>()
                    .is_some()
                    || error.to_string().contains("unconfirmed")
            });
            if run.finish(!unconfirmed).is_err() {
                let mut registry = supervisor.drone.lock().unwrap();
                registry.progress(&assessment, "control_persistence_unconfirmed");
                registry.stop(&assessment.root);
                return;
            }
            if control.is_cancelled()
                || !supervisor
                    .drone_controls
                    .enabled(&assessment.root)
                    .unwrap_or(false)
            {
                return;
            }
            let ineligible = result
                .as_ref()
                .is_err_and(|error| error.to_string().contains("drone_preflight_ineligible"));
            let withdrawn = result
                .as_ref()
                .is_err_and(|error| error.to_string().contains("drone_authority_withdrawn"));
            let unconfirmed = result.as_ref().is_err_and(|error| {
                error
                    .downcast_ref::<pa_core::session_engine::drone::DroneProviderOutcomeUnknown>()
                    .is_some()
                    || error.to_string().contains("unconfirmed")
            });
            {
                let mut registry = supervisor.drone.lock().unwrap();
                if ineligible {
                    registry.cancel_ineligible(&assessment);
                } else if withdrawn {
                    registry.withdraw(&assessment);
                } else {
                    if unconfirmed {
                        registry.stop(&assessment.root);
                    }
                    if result.is_err() && !unconfirmed {
                        registry.result(
                            &assessment,
                            json!({"type":"assessment_failure","retry":"bounded"}),
                        );
                    }
                    if let Err(error) = &result {
                        registry.failure(&assessment, failure_code(&error.to_string()));
                    }
                    registry.finish(&assessment, result.is_ok());
                }
            }
            if ineligible || withdrawn {
                return;
            }
            let escalation_due = supervisor.drone.lock().unwrap().escalation_due(&assessment);
            if escalation_due
                && supervisor
                    .drone_authority_current(&target, &assessment.root, &assessed_binding)
                    .await
            {
                // Serialize a replacement bind with final admission without
                // blocking the event reader that delivers the worker reply.
                let _binding_admission = target.drone_binding_admission.lock().await;
                if !supervisor
                    .drone_controls
                    .enabled(&assessment.root)
                    .unwrap_or(false)
                {
                    return;
                }
                let binding_guard = target.descriptor.lock().await.clone();
                if binding_guard.stop_requested_at.is_some()
                    || !has_roadmap_binding(&binding_guard)
                    || binding_guard.root_active_session_id
                        != assessed_binding.root_active_session_id
                    || binding_guard.create_command.rest.get("droneRoadmapPath")
                        != assessed_binding.create_command.rest.get("droneRoadmapPath")
                    || binding_guard
                        .create_command
                        .rest
                        .get("droneBindingRevision")
                        != assessed_binding
                            .create_command
                            .rest
                            .get("droneBindingRevision")
                {
                    return;
                }
                let binding = binding_guard
                    .create_command
                    .rest
                    .get("droneBindingRevision")
                    .cloned()
                    .unwrap_or(Value::Null)
                    .to_string();
                let notify = supervisor
                    .drone
                    .lock()
                    .unwrap()
                    .escalation_is_new(&assessment, &binding);
                if notify {
                    let response=supervisor.route_command_typed(&target,"follow_up",json!({"activeSessionId":binding_guard.root_active_session_id.clone(),"message":"[Drone] Repeated assessment failures: the observer could not complete its checks. Inspect current Drone status and the recorded failure; do not treat this as the user token budget or project completion. Continue safe independent roadmap work. No operation was replayed.","source":"drone"}),5000,RouteAdmission::SupervisorInternal).await;
                    if response.as_ref().is_ok_and(|reply| !reply.success) {
                        supervisor
                            .drone
                            .lock()
                            .unwrap()
                            .escalation_refused(&assessment, &binding);
                    }
                }
            }
            supervisor.wake_drone_chain(target, assessment.root);
        });
    }
    async fn drone_authority_current(
        &self,
        target: &Arc<ResidentWorker>,
        root: &str,
        assessed_binding: &crate::protocol::DaemonWorkerDescriptor,
    ) -> bool {
        if !self.drone_controls.enabled(root).unwrap_or(false) {
            return false;
        }
        if target.intentional_stop.load(Ordering::SeqCst) {
            return false;
        }
        let descriptor = target.descriptor.lock().await.clone();
        if descriptor.stop_requested_at.is_some()
            || !has_roadmap_binding(&descriptor)
            || descriptor.root_active_session_id != assessed_binding.root_active_session_id
            || descriptor.create_command.rest.get("droneRoadmapPath")
                != assessed_binding.create_command.rest.get("droneRoadmapPath")
            || descriptor.create_command.rest.get("droneBindingRevision")
                != assessed_binding
                    .create_command
                    .rest
                    .get("droneBindingRevision")
        {
            return false;
        }
        if !self
            .registry
            .resolve(&descriptor.root_active_session_id)
            .await
            .is_ok_and(|current| Arc::ptr_eq(&current, target))
        {
            return false;
        }
        let Ok(response) = self
            .route_command_typed(
                target,
                "drone_snapshot",
                json!({}),
                5000,
                RouteAdmission::SupervisorInternal,
            )
            .await
        else {
            return false;
        };
        response.success
            && response.data.as_ref().is_some_and(|snapshot| {
                snapshot_allows_drone(snapshot, &descriptor.root_active_session_id, root)
            })
    }
    async fn run_drone_assessment(
        &self,
        target: &Arc<ResidentWorker>,
        assessment: &registry::Assessment,
        control: &pa_core::session_engine::drone::AssessmentControl,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !control.is_cancelled()
                && self
                    .drone_controls
                    .enabled(&assessment.root)
                    .unwrap_or(false),
            "drone_control_stopped"
        );
        let descriptor = target.descriptor.lock().await.clone();
        anyhow::ensure!(
            !target.intentional_stop.load(Ordering::SeqCst)
                && descriptor.stop_requested_at.is_none()
                && has_roadmap_binding(&descriptor)
                && self
                    .registry
                    .resolve(&descriptor.root_active_session_id)
                    .await
                    .is_ok_and(|current| Arc::ptr_eq(&current, target)),
            "drone_preflight_ineligible"
        );
        let snapshot = self
            .route_command_typed(
                target,
                "drone_snapshot",
                json!({}),
                5000,
                RouteAdmission::SupervisorInternal,
            )
            .await?;
        anyhow::ensure!(snapshot.success, "drone_preflight_ineligible");
        let mut snapshot = snapshot.data.unwrap_or(Value::Null);
        if snapshot["stopped"] == true {
            self.drone.lock().unwrap().stop(&assessment.root);
            anyhow::bail!("drone_preflight_ineligible");
        }
        anyhow::ensure!(
            snapshot_allows_drone(
                &snapshot,
                &descriptor.root_active_session_id,
                &assessment.root,
            ),
            "drone_preflight_ineligible"
        );
        if let Some(delay) = request_deadline_delay(&snapshot) {
            tokio::select! {
                () = control.cancelled() => anyhow::bail!("drone_control_stopped"),
                () = tokio::time::sleep(delay) => {},
            }
            let fresh = self
                .route_command_typed(
                    target,
                    "drone_snapshot",
                    json!({}),
                    5000,
                    RouteAdmission::SupervisorInternal,
                )
                .await?;
            anyhow::ensure!(fresh.success, "drone_preflight_ineligible");
            snapshot = fresh.data.unwrap_or(Value::Null);
        }
        if snapshot["stopped"] == true {
            self.drone.lock().unwrap().stop(&assessment.root);
            anyhow::bail!("drone_preflight_ineligible");
        }
        anyhow::ensure!(
            !target.intentional_stop.load(Ordering::SeqCst)
                && snapshot_allows_drone(
                    &snapshot,
                    &descriptor.root_active_session_id,
                    &assessment.root,
                ),
            "drone_preflight_ineligible"
        );
        let current_descriptor = target.descriptor.lock().await.clone();
        anyhow::ensure!(
            current_descriptor.stop_requested_at.is_none()
                && current_descriptor
                    .create_command
                    .rest
                    .get("droneRoadmapPath")
                    == descriptor.create_command.rest.get("droneRoadmapPath")
                && current_descriptor
                    .create_command
                    .rest
                    .get("droneBindingRevision")
                    == descriptor.create_command.rest.get("droneBindingRevision"),
            "drone_preflight_ineligible"
        );
        let cwd = std::path::PathBuf::from(
            snapshot["cwd"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("drone_preflight_ineligible"))?,
        );
        let roadmap_path = descriptor
            .create_command
            .rest
            .get("droneRoadmapPath")
            .and_then(Value::as_str);
        // Missing or invalid authoritative evidence is a preflight failure, not a paid model request.
        let roadmap = roadmap::read_bound(roadmap_path, &cwd)
            .map_err(|_| anyhow::anyhow!("drone_preflight_ineligible"))?;
        self.drone.lock().unwrap().evidence(
            assessment,
            roadmap["sha256"].as_str().unwrap_or_default().into(),
        );
        let gate = self
            .route_command_typed(
                target,
                "get_orchestration_gate",
                json!({}),
                5000,
                RouteAdmission::SupervisorInternal,
            )
            .await
            .ok()
            .filter(|response| response.success)
            .and_then(|response| response.data);
        let children = self
            .route_command_typed(
                target,
                "get_rlm_children",
                json!({}),
                5000,
                RouteAdmission::SupervisorInternal,
            )
            .await
            .ok()
            .filter(|response| response.success)
            .and_then(|response| response.data);
        snapshot["advisoryPolicy"] = parallel::advisory_policy(&snapshot, gate.as_ref());
        let state = evidence::model_state(
            &snapshot,
            &roadmap,
            gate.as_ref(),
            children.as_ref(),
            assessment.owned_outcome.as_deref(),
        );
        if !self
            .drone
            .lock()
            .unwrap()
            .admit_model_state(assessment, state)
        {
            return Ok(());
        }
        let model_roadmap = roadmap::model_evidence(&roadmap, assessment.page_cursor)
            .map_err(|_| anyhow::anyhow!("drone_preflight_ineligible"))?;
        let parallel_runtime = parallel::runtime_evidence(gate.as_ref(), children.as_ref());
        let parallel_candidates = if snapshot["advisoryPolicy"]["parallelAllowed"] == true {
            serde_json::from_value::<roadmap::Plan>(roadmap["structured"].clone())
                .ok()
                .map(|plan| {
                    gate.as_ref()
                        .filter(|_| children.as_ref().is_some_and(parallel::roster_available))
                        .map_or_else(Vec::new, |gate| {
                            parallel::candidates(
                                &plan,
                                gate,
                                snapshot["activeSessionId"].as_str().unwrap_or_default(),
                            )
                            .iter()
                            .map(|step| step.id.clone())
                            .collect::<Vec<_>>()
                        })
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let selected_ready = model_roadmap["readyWindow"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row["id"].as_str())
                    .filter(|id| parallel_candidates.iter().any(|candidate| candidate == id))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut manifest = handlers::manifest();
        {
            let registry = self.drone.lock().unwrap();
            for (name, script) in &registry.scripts {
                manifest
                    .as_array_mut()
                    .unwrap()
                    .push(registered_check_manifest(name, script));
            }
        }
        let manifest = evidence::model_manifest(&snapshot, manifest);
        let input = json!({"root":assessment.root,"epoch":assessment.epoch,"cursor":assessment.sequence,
    "pageCursor":assessment.page_cursor,
    "resnapshot":assessment.gap,"snapshot":snapshot,"roadmap":model_roadmap,
    "parallelRuntime":parallel_runtime,"readyIndependentSteps":selected_ready,
    "readyIndependentCount":parallel_candidates.len(),"events":assessment.events,"manifest":manifest});
        anyhow::ensure!(
            self.drone_controls
                .enabled(&assessment.root)
                .unwrap_or(false),
            "drone_control_stopped"
        );
        let decision = pa_core::session_engine::drone::assess_with_control(
            &self.options.agent_dir,
            &cwd,
            input,
            |stage| {
                self.drone.lock().unwrap().progress(assessment, stage);
            },
            control,
        )
        .await?;
        if decision.is_null() {
            return Ok(());
        }
        let action = parse_decision_action(decision)?;
        self.apply_drone_decision(
            target,
            assessment,
            &descriptor,
            roadmap_path,
            &cwd,
            &roadmap,
            &snapshot,
            gate.as_ref(),
            children.as_ref(),
            action,
        )
        .await
    }
    // Keep all immutable assessment fences explicit at the single admission boundary.
    #[allow(clippy::too_many_arguments)]
    async fn apply_drone_decision(
        &self,
        target: &Arc<ResidentWorker>,
        assessment: &registry::Assessment,
        descriptor: &crate::protocol::DaemonWorkerDescriptor,
        roadmap_path: Option<&str>,
        cwd: &std::path::Path,
        roadmap: &Value,
        snapshot: &Value,
        gate_at_assessment: Option<&Value>,
        children_at_assessment: Option<&Value>,
        mut action: handlers::Action,
    ) -> anyhow::Result<()> {
        // Serialize binding replacement with action admission and completion.
        // The descriptor itself cannot be held across a worker route: the
        // event reader needs it before it can consume the command response.
        let _binding_admission = target.drone_binding_admission.lock().await;
        anyhow::ensure!(
            self.drone_controls
                .enabled(&assessment.root)
                .unwrap_or(false),
            "drone_control_stopped"
        );
        let binding_guard = target.descriptor.lock().await.clone();
        anyhow::ensure!(
            binding_guard
                .create_command
                .rest
                .get("droneBindingRevision")
                == descriptor.create_command.rest.get("droneBindingRevision")
                && binding_guard.create_command.rest.get("droneRoadmapPath")
                    == descriptor.create_command.rest.get("droneRoadmapPath"),
            "stale_roadmap_recommendation"
        );
        let fresh = self
            .route_command_typed(
                target,
                "drone_snapshot",
                json!({}),
                5000,
                RouteAdmission::SupervisorInternal,
            )
            .await?;
        anyhow::ensure!(fresh.success, "drone_snapshot_failed");
        anyhow::ensure!(
            !target.intentional_stop.load(Ordering::SeqCst)
                && binding_guard.stop_requested_at.is_none()
                && fresh.data.as_ref().is_some_and(|current| {
                    snapshot_allows_drone(
                        current,
                        &binding_guard.root_active_session_id,
                        &assessment.root,
                    ) && current["activeSessionId"] == snapshot["activeSessionId"]
                        && current["humanInputFingerprint"] == snapshot["humanInputFingerprint"]
                        && current["pendingHumanInputs"].as_u64().unwrap_or(0) == 0
                }),
            "drone_authority_withdrawn"
        );
        if let Some(expected) = roadmap.get("sha256") {
            let current = roadmap::read_bound(roadmap_path, cwd)?;
            anyhow::ensure!(
                &current["sha256"] == expected,
                "stale_roadmap_recommendation"
            );
        }

        if action.handler == "recommend_parallel_work"
            || (action.handler == "notify_root"
                && action.args["message"]
                    .as_str()
                    .is_some_and(parallel::mentions_parallel_work))
        {
            let current = fresh.data.as_ref().unwrap();
            anyhow::ensure!(
                parallel::advisory_policy(current, gate_at_assessment)["parallelAllowed"] == true,
                "parallel_advice_forbidden"
            );
        }
        if action.handler == "recommend_parallel_work" {
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct Proposal {
                step_ids: Vec<String>,
                roadmap_hash: String,
            }
            let proposal: Proposal = serde_json::from_value(action.args.clone())
                .map_err(|_| anyhow::anyhow!("invalid_arguments"))?;
            anyhow::ensure!(
                roadmap["sha256"] == proposal.roadmap_hash,
                "stale_roadmap_recommendation"
            );
            let previous_gate =
                gate_at_assessment.ok_or_else(|| anyhow::anyhow!("parallel_runtime_unknown"))?;
            anyhow::ensure!(
                children_at_assessment.is_some_and(parallel::roster_available),
                "parallel_child_roster_unknown"
            );
            let current_children = self
                .route_command_typed(
                    target,
                    "get_rlm_children",
                    json!({}),
                    5000,
                    RouteAdmission::SupervisorInternal,
                )
                .await?;
            anyhow::ensure!(
                current_children.success
                    && current_children
                        .data
                        .as_ref()
                        .is_some_and(parallel::roster_available),
                "parallel_child_roster_unknown"
            );
            let current_gate = self
                .route_command_typed(
                    target,
                    "get_orchestration_gate",
                    json!({}),
                    5000,
                    RouteAdmission::SupervisorInternal,
                )
                .await?;
            anyhow::ensure!(current_gate.success, "parallel_runtime_unknown");
            let current_gate = current_gate
                .data
                .ok_or_else(|| anyhow::anyhow!("parallel_runtime_unknown"))?;
            anyhow::ensure!(
                parallel::advisory_policy(fresh.data.as_ref().unwrap(), Some(&current_gate))
                    ["parallelAllowed"]
                    == true,
                "parallel_advice_forbidden"
            );
            anyhow::ensure!(
                parallel::same_revision(previous_gate, &current_gate),
                "stale_parallel_runtime"
            );
            let plan: roadmap::Plan = serde_json::from_value(roadmap["structured"].clone())?;
            let assignments = parallel::validate(
                &plan,
                &current_gate,
                snapshot["activeSessionId"].as_str().unwrap_or_default(),
                &proposal.step_ids,
            )?;
            let references = assignments
                .iter()
                .map(|step| {
                    json!({
                        "id":step.id.as_str(),"planId":step.plan_id.as_deref(),"stepId":step.step_id.as_deref(),
                        "scopeEvidence":"declared_ownership_labels_only",
                        "requiresRootScopeReview":true
                    })
                })
                .collect::<Vec<_>>();
            let notice = format!(
                "Roadmap revision {}, SHA256 {}: ready parallel-work candidates with non-overlapping declared ownership labels {}. ROOT must check actual file scopes and dependencies before dispatch, then recheck the owner-scoped gate. ROOT alone dispatches via native RLM admission and verifies terminal delivery and acceptance; this advice proves neither dispatch nor integration.",
                plan.revision,
                proposal.roadmap_hash,
                serde_json::to_string(&references)?
            );
            anyhow::ensure!(notice.len() <= 2048, "parallel_proposal_too_large");
            action = handlers::Action {
                handler: "notify_root".into(),
                args: json!({"message":notice}),
            };
        }
        if action.handler == "run_registered_check" {
            anyhow::ensure!(
                self.drone.lock().unwrap().current(assessment)
                    && !target.intentional_stop.load(Ordering::SeqCst),
                "stale_assessment"
            );
            let name = action.args["name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("invalid_arguments"))?;
            let script = self
                .drone
                .lock()
                .unwrap()
                .scripts
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("capability_missing"))?;
            let arguments = if name == "branch_facts" {
                json!({"snapshot":serde_json::to_string(&snapshot)?})
            } else {
                action.args["arguments"].clone()
            };
            let result = script.execute(&arguments).await?;
            self.drone.lock().unwrap().observation_result(
                assessment,
                &format!("run_registered_check:{name}"),
                json!({"handler":"run_registered_check","result":result}),
            );
            return Ok(());
        }
        let (command, mut payload) = handlers::command(&action).map_err(|error| {
            if error.downcast_ref::<serde_json::Error>().is_some() {
                anyhow::anyhow!("invalid_arguments")
            } else {
                error
            }
        })?;
        if matches!(command, "follow_up" | "drone_recover") {
            anyhow::ensure!(
                roadmap["structured"]["userStopped"] != true
                    && roadmap["structured"]["budgetAvailable"] != false,
                "authority_denied"
            );
        }
        anyhow::ensure!(
            self.drone.lock().unwrap().current(assessment)
                && !target.intentional_stop.load(Ordering::SeqCst),
            "stale_assessment"
        );
        anyhow::ensure!(
            binding_guard.stop_requested_at.is_none(),
            "authority_denied"
        );
        // One advisory per material state, including free-form notify_root.
        // Its own notice, root reply and request counters cannot re-arm it.
        let advice_key = (command == "follow_up").then(|| {
            evidence::notice_fingerprint(
                snapshot,
                roadmap,
                gate_at_assessment,
                children_at_assessment,
                assessment.owned_job_outcome.as_deref(),
            )
        });
        if let Some(key) = &advice_key {
            if !self
                .drone
                .lock()
                .unwrap()
                .advice_is_new(&assessment.root, key.clone())
            {
                return Ok(());
            }
        }
        payload["activeSessionId"] = snapshot["activeSessionId"].clone();
        let result = self
            .route_command_typed(
                target,
                command,
                payload,
                5000,
                RouteAdmission::SupervisorInternal,
            )
            .await?;
        if !result.success {
            if let Some(key) = advice_key {
                self.drone
                    .lock()
                    .unwrap()
                    .advice_refused(&assessment.root, &key);
            }
        }
        let evidence = if serde_json::to_vec(&result)?.len() <= 16 * 1024 {
            json!({"handler":action.handler,"response":result})
        } else {
            json!({"handler":action.handler,"success":result.success,"evidence":"output_cap","unknown":true})
        };
        let mut drone = self.drone.lock().unwrap();
        if result.success
            && matches!(
                action.handler.as_str(),
                "snapshot_branch"
                    | "collect_child_results"
                    | "inspect_owned_process"
                    | "recovery_status"
            )
        {
            drone.observation_result(assessment, &action.handler, evidence);
        } else {
            drone.result(assessment, evidence);
        }
        drop(drone);
        if !result.success
            && result.error.as_deref().is_some_and(|error| {
                error.contains("drone_authority_withdrawn") || error.contains("authority_denied")
            })
        {
            anyhow::bail!("drone_authority_withdrawn");
        }
        anyhow::ensure!(result.success, "drone_handler_failed");
        Ok(())
    }
}

// The existing assessment owns this wait; a quiet transport needs no new event,
// observer or scheduler to reach its Native deadline and obtain a fresh snapshot.
fn request_deadline_delay(snapshot: &Value) -> Option<std::time::Duration> {
    let deadline = snapshot["request"]["deadlineMs"].as_u64()?;
    let elapsed = snapshot["request"]["elapsedMs"].as_u64().unwrap_or(0);
    deadline
        .checked_sub(elapsed)
        .filter(|remaining| *remaining > 0)
        .map(std::time::Duration::from_millis)
}

fn has_roadmap_binding(descriptor: &crate::protocol::DaemonWorkerDescriptor) -> bool {
    descriptor
        .create_command
        .rest
        .get("droneRoadmapPath")
        .and_then(Value::as_str)
        .is_some_and(|path| std::path::Path::new(path).is_absolute())
}

fn snapshot_allows_drone(snapshot: &Value, active: &str, root: &str) -> bool {
    snapshot["activeSessionId"] == active
        && snapshot["sessionId"] == root
        && snapshot["paused"] != true
        && !matches!(
            snapshot["goal"]["status"].as_str(),
            Some("paused" | "budget_limited" | "complete")
        )
        && snapshot["stopped"] != true
        && (snapshot["goal"]["tokenBudget"].is_null()
            || snapshot["goal"]["tokenBudget"]
                .as_u64()
                .is_some_and(|budget| {
                    budget > 0
                        && snapshot["goal"]["tokensUsed"]
                            .as_u64()
                            .is_some_and(|used| used < budget)
                }))
}

fn parse_decision_action(decision: Value) -> anyhow::Result<handlers::Action> {
    serde_json::from_value(decision).map_err(|_| anyhow::anyhow!("invalid_decision_schema"))
}

fn failure_code(error: &str) -> &'static str {
    for (needle, code) in [
        ("roadmap_binding", "missing_binding"),
        ("roadmap_input_cap", "roadmap_input_cap"),
        ("capability_missing: h20-keys/h20-mmfast", "model_missing"),
        ("invalid_decision_schema", "invalid_decision_schema"),
        ("invalid_arguments", "invalid_arguments"),
        ("stale_roadmap_recommendation", "stale_roadmap"),
        ("stale_parallel_runtime", "stale_parallel_runtime"),
        ("parallel_runtime_unknown", "parallel_runtime_unknown"),
        (
            "parallel_child_roster_unknown",
            "parallel_child_roster_unknown",
        ),
        (
            "parallel_capacity_unavailable",
            "parallel_capacity_unavailable",
        ),
        ("parallel_ownership_conflict", "parallel_ownership_conflict"),
        ("parallel_duplicate_step", "parallel_duplicate_step"),
        ("parallel_proposal_too_large", "parallel_proposal_too_large"),
        ("parallel_advice_forbidden", "parallel_advice_forbidden"),
        ("stale_assessment", "stale_assessment"),
        ("parallel_proposal_not_ready", "parallel_not_ready"),
        ("capability_missing", "capability_missing"),
        ("drone_auth_missing", "authentication_missing"),
        ("authority_denied", "authority_denied"),
        ("drone_input_cap", "input_cap"),
        ("drone_output_cap", "output_cap"),
        ("unconfirmed", "request_unconfirmed"),
        ("Session worker timed out", "worker_route_timeout"),
        ("timeout", "request_timeout"),
        ("drone_assessment_failed", "model_failed"),
        ("drone_snapshot_failed", "snapshot_failed"),
        ("drone_handler_failed", "handler_failed"),
    ] {
        if error.contains(needle) {
            return code;
        }
    }
    "assessment_failed"
}

#[cfg(test)]
mod failure_tests {
    #[test]
    fn restored_drone_children_resolve_only_exact_durable_family_edges() {
        let mut roster = crate::agent_roster::AgentRoster::new();
        let root = serde_json::json!({"activeSessionId":"r","sessionId":"root","sessionFile":"/tmp/root.jsonl","rlmDepth":0});
        let child = serde_json::json!({"activeSessionId":"c","sessionId":"child","sessionFile":"/tmp/child.jsonl","runtimeKind":"subagent","parentSessionPath":"/tmp/root.jsonl","rlmDepth":1});
        let grandchild = serde_json::json!({"activeSessionId":"g","sessionId":"grandchild","runtimeKind":"subagent","parentSessionPath":"/tmp/child.jsonl","rlmDepth":2});
        for row in [&root, &child, &grandchild] {
            roster.write_summary(row.clone(), None, None);
        }
        assert_eq!(
            super::drone_family_root(&roster, "c"),
            Some(("root".into(), "r".into()))
        );
        assert_eq!(
            super::drone_family_root(&roster, "g"),
            Some(("root".into(), "r".into()))
        );
        let shared = "01a15340-ce46-73f5-9d29-bf6d4408a1a0";
        let mut ambiguous = crate::agent_roster::AgentRoster::new();
        for (id, prefix) in [("r1", "/a"), ("r2", "/b")] {
            ambiguous.write_summary(serde_json::json!({"activeSessionId":id,"sessionId":id,"sessionFile":format!("{prefix}/{shared}.jsonl"),"rlmDepth":0}), None, None);
        }
        ambiguous.write_summary(serde_json::json!({"activeSessionId":"c","sessionId":"child","runtimeKind":"subagent","parentSessionPath":format!("/a/{shared}.jsonl"),"rlmDepth":1}), None, None);
        assert!(super::drone_family_root(&ambiguous, "c").is_none());
        for patch in [
            serde_json::json!({"parentSessionPath":"/tmp/missing.jsonl"}),
            serde_json::json!({"parentSessionPath":"/tmp/child.jsonl"}),
            serde_json::json!({"rlmDepth":2}),
            serde_json::json!({"parentSessionId":"unrelated"}),
            serde_json::json!({"parentSessionPath":null}),
        ] {
            let mut invalid = child.clone();
            for (key, value) in patch.as_object().unwrap() {
                invalid[key] = value.clone();
            }
            roster.write_summary(invalid, None, None);
            assert!(super::drone_family_root(&roster, "c").is_none());
        }
    }

    #[test]
    fn registered_check_manifest_advertises_dispatch_compatible_args() {
        let script = |argument_names: Vec<String>| super::scripts::RegisteredScript {
            executable: "/usr/bin/python3".into(),
            expected_bytes: Vec::new(),
            execution_root: "/tmp".into(),
            argument_names,
            timeout: std::time::Duration::from_secs(2),
            output_cap: 4096,
        };
        let branch =
            super::registered_check_manifest("branch_facts", &script(vec!["snapshot".into()]));
        let branch_action = super::parse_decision_action(serde_json::json!({
            "handler":"run_registered_check", "args":branch["args"]
        }))
        .unwrap();
        assert_eq!(
            branch_action.args,
            serde_json::json!({"name":"branch_facts"})
        );
        assert!(branch.get("argumentNames").is_none());
        assert!(branch["argumentSource"]
            .as_str()
            .unwrap()
            .contains("Native injects"));

        let general = super::registered_check_manifest(
            "fixed_probe",
            &script(vec!["target".into(), "mode".into()]),
        );
        let general_action = super::parse_decision_action(serde_json::json!({
            "handler":"run_registered_check", "args":general["args"]
        }))
        .unwrap();
        assert_eq!(general_action.args["name"], "fixed_probe");
        let accepted: std::collections::BTreeMap<String, String> =
            serde_json::from_value(general_action.args["arguments"].clone()).unwrap();
        assert_eq!(
            accepted.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["mode", "target"]
        );
        assert!(accepted.values().all(|value| value == "string"));
    }

    #[test]
    fn malformed_drone_decision_reports_only_a_safe_schema_code() {
        let decision = serde_json::json!({
            "handler":"snapshot_branch", "arguments":{}, "privateValue":"must-not-leak"
        });
        let error = super::parse_decision_action(decision)
            .unwrap_err()
            .to_string();
        assert_eq!(error, "invalid_decision_schema");
        assert_eq!(super::failure_code(&error), "invalid_decision_schema");
    }

    #[test]
    fn drone_failure_readback_has_no_raw_error_or_secret() {
        for (error, expected) in [
            ("capability_missing:roadmap_binding", "missing_binding"),
            ("drone_auth_missing", "authentication_missing"),
            ("capability_missing: h20-keys/h20-mmfast", "model_missing"),
            ("drone_input_cap", "input_cap"),
            ("Session worker timed out", "worker_route_timeout"),
            ("invalid_arguments", "invalid_arguments"),
            ("stale_roadmap_recommendation", "stale_roadmap"),
            ("stale_assessment", "stale_assessment"),
            ("parallel_proposal_not_ready", "parallel_not_ready"),
            ("capability_missing: unknown-handler", "capability_missing"),
            ("secret-provider-body", "assessment_failed"),
        ] {
            assert_eq!(super::failure_code(error), expected);
        }
    }
    #[test]
    fn paused_or_stopped_root_is_not_authorized_for_drone() {
        let root = "sid";
        let active = "root";
        let mut snapshot = serde_json::json!({
            "sessionId":root,"activeSessionId":active,
            "paused":false,"stopped":false,"goal":{"status":"active"}
        });
        assert!(super::snapshot_allows_drone(&snapshot, active, root));
        snapshot["paused"] = serde_json::json!(true);
        assert!(!super::snapshot_allows_drone(&snapshot, active, root));
        snapshot["paused"] = serde_json::json!(false);
        snapshot["goal"]["status"] = serde_json::json!("paused");
        assert!(!super::snapshot_allows_drone(&snapshot, active, root));
        for status in ["budget_limited", "complete", "paused"] {
            snapshot["goal"]["status"] = serde_json::json!(status);
            assert!(!super::snapshot_allows_drone(&snapshot, active, root));
        }
        for goal in [
            serde_json::Value::Null,
            serde_json::json!({"status":"error"}),
            serde_json::json!({"status":"active"}),
        ] {
            snapshot["goal"] = goal;
            assert!(super::snapshot_allows_drone(&snapshot, active, root));
        }
        for goal in [
            serde_json::json!({"status":"error","tokenBudget":10,"tokensUsed":10}),
            serde_json::json!({"status":"error","tokenBudget":10}),
            serde_json::json!({"status":"error","tokenBudget":"10","tokensUsed":0}),
        ] {
            snapshot["goal"] = goal;
            assert!(!super::snapshot_allows_drone(&snapshot, active, root));
        }
        snapshot["goal"] = serde_json::json!({"status":"error","tokenBudget":10,"tokensUsed":9});
        assert!(super::snapshot_allows_drone(&snapshot, active, root));
        snapshot["stopped"] = serde_json::json!(true);
        assert!(!super::snapshot_allows_drone(&snapshot, active, root));
        snapshot["stopped"] = serde_json::json!(false);
        assert!(!super::snapshot_allows_drone(
            &snapshot,
            "another-worker",
            root
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn drone_quiet_transport_reaches_the_owned_deadline_without_stream_events() {
        let mut snapshot = serde_json::json!({"request":{"requestId":7,"revision":1,"phase":"transport",
            "elapsedMs":10,"deadlineMs":100},"tools":[],"hasBackgroundBash":false,"compacting":false});
        let before =
            super::evidence::model_state(&snapshot, &serde_json::Value::Null, None, None, None);
        let wait = tokio::time::sleep(super::request_deadline_delay(&snapshot).unwrap());
        tokio::pin!(wait);
        tokio::time::advance(std::time::Duration::from_millis(89)).await;
        assert!(!wait.is_elapsed());
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        wait.await;
        // The production path rereads this snapshot; no streaming delta wakes it.
        snapshot["request"]["elapsedMs"] = serde_json::json!(100);
        assert!(super::request_deadline_delay(&snapshot).is_none());
        let after =
            super::evidence::model_state(&snapshot, &serde_json::Value::Null, None, None, None);
        assert_ne!(before.key, after.key);
        assert!(after.unresolved);
    }
}
