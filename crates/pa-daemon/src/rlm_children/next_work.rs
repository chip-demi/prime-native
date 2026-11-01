//! Owner-authenticated Roadmap projection at existing continuation/admission boundaries.
use super::{
    anyhow, json, DaemonCommand, Duration, Gate, Map, PathBuf, Result, SupervisorChildSessions,
    Value,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct CreateNotSent(pub(super) String);

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Source {
    socket_path: PathBuf,
    token: String,
    pub(super) instance_id: String,
}
impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NextWorkSource([redacted])")
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Binding {
    pub(super) child_id: String,
    plan_id: String,
    step_id: String,
    snapshot_revision: u64,
    ownership: Vec<String>,
    #[serde(default)]
    pub(super) start_permitted: bool,
    #[serde(default)]
    task: Option<super::orchestration::Task>,
    #[serde(default)]
    policy: Option<super::orchestration::Policy>,
    #[serde(default)]
    pub(super) started_result: Option<Value>,
    #[serde(default)]
    pub(super) terminal_delivery: Option<String>,
    #[serde(default)]
    pub(super) review_consumed: bool,
    #[serde(skip)]
    pub(super) start_token: Option<String>,
}
pub(super) fn accepted_proof<'a>(
    snapshot: Option<&'a Value>,
    scope: Option<&Value>,
    binding: &Binding,
) -> Option<&'a Value> {
    let snapshot = snapshot.filter(|snapshot| snapshot["complete"] == true)?;
    let scope = scope?;
    if !binding.start_permitted || binding.terminal_delivery.is_none() {
        return None;
    }
    let item = snapshot["items"].as_array()?.iter().find(|item| {
        item["planId"] == binding.plan_id
            && item["stepId"] == binding.step_id
            && item["status"] == "accepted"
    })?;
    let proof = &item["completion"]["managedAcceptance"];
    (binding.review_consumed
        && binding.started_result.as_ref().is_some_and(|result| {
            proof["result"]["childId"] == result["childId"]
                && proof["result"]["admissionGeneration"] == result["admissionGeneration"]
                && proof["result"]["incarnation"] == result["incarnation"]
                && proof["result"]["expectedMessageId"] == result["expectedMessageId"]
        })
        && proof["result"]["terminalDeliveryId"].as_str() == binding.terminal_delivery.as_deref()
        && item["completion"]["sessionId"] == scope["rootSessionId"]
        && proof["schemaVersion"] == 1
        && proof["policy"] == "causal_results_v1"
        && [
            "rootSessionId",
            "goalId",
            "taskGeneration",
            "policyRevision",
            "ownerBindingId",
        ]
        .iter()
        .all(|key| proof[*key] == scope[*key])
        && proof["planId"] == binding.plan_id
        && proof["stepId"] == binding.step_id
        && proof["committedRevision"].as_u64().is_some_and(|revision| {
            snapshot["revision"]
                .as_u64()
                .is_some_and(|current| revision <= current)
        })
        && !proof["artifactRef"].as_str().unwrap_or_default().is_empty()
        && !proof["acceptanceRef"]
            .as_str()
            .unwrap_or_default()
            .is_empty()
        && proof["artifactRef"] == item["artifactRef"]
        && proof["acceptanceRef"] == item["acceptanceRef"])
        .then_some(proof)
}

impl Source {
    pub(crate) async fn owner_callback(&self, identity: Value, params: Value) -> Result<Value> {
        let body = serde_json::to_vec(
            &json!({"action":"owner_bootstrap","identity":identity,"params":params}),
        )?;
        let stream = pa_types::platform::transport::connect_transport(&self.socket_path).await?;
        let (reader, mut writer) = stream.split();
        let header=format!("POST / HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",self.token,body.len());
        writer.write_all(header.as_bytes()).await?;
        writer.write_all(&body).await?;
        let mut bytes = Vec::new();
        reader.take(131_073).read_to_end(&mut bytes).await?;
        anyhow::ensure!(
            bytes.len() <= 131_072
                && (bytes.starts_with(b"HTTP/1.1 200 ") || bytes.starts_with(b"HTTP/1.0 200 ")),
            "Owner source unavailable"
        );
        let end = bytes
            .windows(4)
            .position(|p| p == b"\r\n\r\n")
            .ok_or_else(|| anyhow!("Invalid owner response"))?;
        Ok(serde_json::from_slice(&bytes[end + 4..])?)
    }
    pub(crate) fn parse(value: &Value) -> Result<Self> {
        let source: Self = serde_json::from_value(value.clone())?;
        anyhow::ensure!(
            !source.token.is_empty()
                && !source.instance_id.is_empty()
                && source.socket_path.is_absolute(),
            "Invalid next_work source"
        );
        Ok(source)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("gate_changed: Work start revision changed; expected={expected} current={current}; start_permitted=false")]
struct StartRevisionChanged {
    expected: u64,
    current: u64,
}

impl SupervisorChildSessions {
    pub(super) fn release_unsent_work(&self, child_id: &str) -> Result<()> {
        let mut gate = self.inner.gate.lock().expect("gate lock");
        gate.work_bindings
            .retain(|binding| binding.child_id != child_id);
        gate.revision += 1;
        self.persist_gate(&gate)
    }
    pub(crate) async fn managed_review_candidate(&self, request: &Value) -> Result<Value> {
        let status = self.gate_status().await;
        let gate = self.inner.gate.lock().expect("gate lock");
        anyhow::ensure!(
            gate.policy.as_ref().is_some_and(
                |policy| policy.managed_pass_policy.as_deref() == Some("causal_results_v1")
            ),
            "Not a managed result turn"
        );
        anyhow::ensure!(
            !gate.suspended
                && status["complete"] == true
                && status["unknown"] == 0
                && status["revision"].as_u64() == Some(gate.revision),
            "Managed review accounting blocked"
        );
        let binding = gate
            .work_bindings
            .iter()
            .find(|binding| {
                request["childId"] == binding.child_id
                    && request["terminalDeliveryId"].as_str()
                        == binding.terminal_delivery.as_deref()
            })
            .ok_or_else(|| anyhow!("Managed result identity mismatch"))?;
        anyhow::ensure!(
            binding.start_permitted && binding.started_result.is_some() && !binding.review_consumed,
            "Managed review already consumed or actual start missing"
        );
        let scope = gate
            .management_scope
            .as_ref()
            .ok_or_else(|| anyhow!("Management scope absent"))?;
        anyhow::ensure!(
            scope["goalId"].as_str().is_some(),
            "Management goal pending"
        );
        Ok(
            json!({"result":binding.started_result,"goalId":scope["goalId"],"revision":gate.revision,
            "terminalDeliveryId":binding.terminal_delivery,"childId":binding.child_id}),
        )
    }

    pub(crate) fn consume_managed_review(&self, candidate: &Value) -> Result<()> {
        let mut gate = self.inner.gate.lock().expect("gate lock");
        anyhow::ensure!(
            candidate["revision"].as_u64() == Some(gate.revision) && !gate.suspended,
            "Managed review changed before consume"
        );
        let mut next = gate.clone();
        let binding = next
            .work_bindings
            .iter_mut()
            .find(|binding| {
                candidate["childId"] == binding.child_id
                    && candidate["terminalDeliveryId"].as_str()
                        == binding.terminal_delivery.as_deref()
            })
            .ok_or_else(|| anyhow!("Managed result disappeared"))?;
        anyhow::ensure!(
            !binding.review_consumed && binding.started_result.as_ref() == candidate.get("result"),
            "Managed review changed or consumed"
        );
        binding.review_consumed = true;
        next.consumed_managed_results.push(candidate.clone());
        self.persist_gate(&next)?;
        *gate = next;
        Ok(())
    }

    pub(crate) async fn begin_work_start(&self, permit: &Value) -> Result<Value> {
        match self.begin_work_start_snapshot(permit).await {
            Err(error) if error.downcast_ref::<StartRevisionChanged>().is_some() => {
                self.begin_work_start_snapshot(permit).await
            }
            result => result,
        }
    }

    async fn begin_work_start_snapshot(&self, permit: &Value) -> Result<Value> {
        self.refresh_next_work().await;
        let revision = self.inner.gate.lock().expect("gate lock").revision;
        let epoch = self
            .inner
            .accounting_epoch
            .load(std::sync::atomic::Ordering::SeqCst);
        let child_id = permit["childId"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing start child identity"))?;
        let record = self
            .inner
            .find_record(child_id)
            .await
            .ok_or_else(|| anyhow!("Current start child missing"))?;
        let state = self
            .inner
            .command(
                &DaemonCommand::GetState {
                    id: None,
                    active_session_id: permit["childActiveSessionId"]
                        .as_str()
                        .ok_or_else(|| anyhow!("Missing start session identity"))?
                        .into(),
                    rest: Map::default(),
                },
                2_000,
            )
            .await?;
        let accounting = self.accounting_rows().await;
        let _admissions = self.inner.spawn_gate.lock().await;
        let row = record.lock().await;
        anyhow::ensure!(
            row.rlm_child_id == child_id
                && !row.closed_by_parent
                && !row.settled
                && !row.admission_blocked
                && row.prompt_admitted
                && row
                    .admission_generation
                    .as_deref()
                    .is_some_and(|generation| permit["admissionGeneration"] == generation)
                && row
                    .expected_message_id
                    .as_deref()
                    .is_some_and(|message| permit["expectedMessageId"] == message)
                && permit["childActiveSessionId"] == row.active_session_id
                && permit["childSessionId"].as_str() == row.session_id.as_deref()
                && permit["incarnation"].as_str().is_some_and(
                    |instance| !instance.is_empty() && state["workerInstanceId"] == instance
                ),
            "identity_invalid: Work start admission identity changed; start_permitted=false"
        );
        let mut gate = self.inner.gate.lock().expect("gate lock");
        anyhow::ensure!(
            self.inner.accounting_epoch.load(std::sync::atomic::Ordering::SeqCst) == epoch,
            "gate_changed: Work start accounting epoch changed; expected={} current={}; start_permitted=false",
            epoch,self.inner.accounting_epoch.load(std::sync::atomic::Ordering::SeqCst)
        );
        anyhow::ensure!(
            gate.start_accounting_complete(&accounting, permit),
            "accounting_unknown: Work start global UNKNOWN accounting blocked; start_permitted=false"
        );
        anyhow::ensure!(
            gate.enabled() && !gate.suspended,
            "Work start gate suspended"
        );
        anyhow::ensure!(
            gate.next_work_source
                .as_ref()
                .is_some_and(|source| permit["sourceInstanceId"] == source.instance_id),
            "Stale work start owner source"
        );
        let snapshot = gate
            .next_work
            .as_ref()
            .ok_or_else(|| anyhow!("Work start source unknown"))?;
        anyhow::ensure!(
            snapshot["complete"] == true
                && snapshot["items"]
                    .as_array()
                    .is_some_and(|items| items
                        .iter()
                        .any(|item| item["planId"] == permit["planId"]
                            && item["stepId"] == permit["stepId"]
                            && item["status"] == "ready")),
            "Work no longer ready before provider start"
        );
        let current_revision = gate.revision;
        let task = gate.task.clone();
        let policy = gate.policy.clone();
        let binding = gate
            .work_bindings
            .iter_mut()
            .find(|binding| {
                permit["childId"] == binding.child_id
                    && permit["planId"] == binding.plan_id
                    && permit["stepId"] == binding.step_id
            })
            .ok_or_else(|| anyhow!("Work start binding missing"))?;
        anyhow::ensure!(
            binding.task.is_some() && binding.task == task && binding.policy == policy,
            "Work start task or policy changed"
        );
        anyhow::ensure!(
            binding
                .start_token
                .as_deref()
                .is_some_and(|token| permit["startToken"] == token),
            "Work start private identity changed"
        );
        anyhow::ensure!(
            !binding.start_permitted,
            "Work start already admitted; reconcile without replay"
        );
        if current_revision != revision {
            return Err(StartRevisionChanged {
                expected: revision,
                current: current_revision,
            }
            .into());
        }
        binding.start_permitted = true;
        gate.revision += 1;
        self.persist_gate(&gate)?;
        Ok(
            json!({"workStartPermitted":true,"revision":gate.revision,"managedResultTracking":gate.policy.as_ref().is_some_and(|policy| policy.managed_pass_policy.as_deref() == Some("causal_results_v1"))}),
        )
    }
    pub(crate) async fn invalidate_next_work(&self) -> Result<Value> {
        let _admissions = self.inner.spawn_gate.lock().await;
        let mut gate = self.inner.gate.lock().expect("gate lock");
        gate.next_work = None;
        gate.correction_pending = false;
        gate.revision += 1;
        self.persist_gate(&gate)?;
        Ok(json!({"revision":gate.revision,"nextWorkInvalidated":true}))
    }
    pub(crate) async fn rebind_next_work_source(
        &self,
        value: &Value,
        goal_id: Option<&str>,
    ) -> Result<Value> {
        let source: Source = serde_json::from_value(value["nextWorkSource"].clone())?;
        anyhow::ensure!(
            !source.token.is_empty()
                && !source.instance_id.is_empty()
                && source.socket_path.is_absolute(),
            "Invalid next_work source"
        );
        let _admissions = self.inner.spawn_gate.lock().await;
        let identity = self.inner.identity.lock().expect("identity lock").clone();
        let mut gate = self.inner.gate.lock().expect("gate lock");
        anyhow::ensure!(
            identity.rlm_depth == 0
                && value["version"] == 1
                && value["expectedSessionId"].as_str() == identity.session_id.as_deref()
                && value["expectedGoalId"].as_str() == goal_id
                && goal_id.is_some()
                && value["expectedGateRevision"].as_u64() == Some(gate.revision)
                && value["expectedTask"] == serde_json::to_value(&gate.task)?
                && value["expectedPolicy"] == serde_json::to_value(&gate.policy)?
                && gate.management_scope.as_ref().is_some_and(|scope| {
                    scope["ownerBindingId"] == value["expectedManagementOwner"]
                        && scope["goalId"] == value["expectedGoalId"]
                        && scope["rootSessionId"] == value["expectedSessionId"]
                        && scope["envelope"] == value["envelope"]
                }),
            "managed_rebind_mismatch"
        );
        let mut next = gate.clone();
        next.next_work_source = Some(source);
        next.next_work = None;
        next.revision += 1;
        self.persist_gate(&next)?;
        *gate = next;
        Ok(
            json!({"sourceRebound":true,"sessionId":identity.session_id,"revision":gate.revision,
            "task":gate.task,"policy":gate.policy,"correctiveAttempts":gate.attempts,
            "suspended":gate.suspended,"managementScope":gate.management_scope,
            "managedContract":{"version":1,"sourceOnlyRebind":true,"causalResults":true}}),
        )
    }

    #[cfg(test)]
    pub(crate) fn install_next_work_source(&self, value: &Value) -> Result<()> {
        let source: Source = serde_json::from_value(value.clone())?;
        anyhow::ensure!(
            !source.token.is_empty()
                && !source.instance_id.is_empty()
                && source.socket_path.is_absolute(),
            "Invalid next_work source"
        );
        let mut gate = self.inner.gate.lock().expect("gate lock");
        gate.next_work_source = Some(source);
        gate.next_work_enabled = true;
        gate.next_work = None;
        self.persist_gate(&gate)
    }
    pub(super) async fn refresh_next_work(&self) {
        let (source, revision) = {
            let gate = self.inner.gate.lock().expect("gate lock");
            if !gate.next_work_enabled {
                return;
            }
            (gate.next_work_source.clone(), gate.revision)
        };
        let identity = self.inner.identity.lock().expect("identity lock").clone();
        let result = async {
            let source = source
                .as_ref()
                .ok_or_else(|| anyhow!("next_work owner must rebind after recovery"))?;
            let mut offset = 0;
            let mut combined: Option<Value> = None;
            loop {
                let body = serde_json::to_vec(&json!({"action":"next_work", "identity":{
                "sessionId":identity.session_id,"sessionFile":identity.session_file,"cwd":identity.cwd},
                "params":{"instanceId":source.instance_id,"offset":offset,"limit":50,"expectedRevision":combined.as_ref().and_then(|value| value["revision"].as_u64())}}))?;
                let stream =
                    pa_types::platform::transport::connect_transport(&source.socket_path).await?;
                let (reader, mut writer) = stream.split();
                let header = format!("POST / HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", source.token, body.len());
                writer.write_all(header.as_bytes()).await?;
                writer.write_all(&body).await?;
                let mut bytes = Vec::new();
                reader.take(131_073).read_to_end(&mut bytes).await?;
                anyhow::ensure!(bytes.len() <= 131_072, "next_work response exceeds bound");
                let boundary = bytes
                    .windows(4)
                    .position(|part| part == b"\r\n\r\n")
                    .ok_or_else(|| anyhow!("Invalid next_work HTTP response"))?;
                anyhow::ensure!(
                    bytes.starts_with(b"HTTP/1.1 200 ") || bytes.starts_with(b"HTTP/1.0 200 "),
                    "next_work owner unavailable"
                );
                let value: Value = serde_json::from_slice(&bytes[boundary + 4..])?;
                anyhow::ensure!(
                    value["schemaVersion"] == 1
                        && value["instanceId"].as_str() == Some(source.instance_id.as_str())
                        && value["revision"].as_u64().is_some()
                        && value["items"].is_array(),
                    "Unsupported next_work projection"
                );
                if let Some(first) = &mut combined {
                    anyhow::ensure!(
                        value["revision"] == first["revision"],
                        "next_work changed during pagination"
                    );
                    first["items"]
                        .as_array_mut()
                        .unwrap()
                        .extend(value["items"].as_array().unwrap().iter().cloned());
                } else {
                    combined = Some(value.clone());
                }
                match value["nextOffset"].as_u64() {
                    Some(next) => {
                        anyhow::ensure!(next > offset, "Invalid next_work page");
                        offset = next;
                    }
                    None => break,
                }
            }
            let mut value = combined.unwrap();
            anyhow::ensure!(
                value["total"].as_u64()
                    == value["items"].as_array().map(|items| items.len() as u64),
                "Incomplete next_work pages"
            );
            value["complete"] = json!(true);
            Ok::<_, anyhow::Error>(value)
        };
        let snapshot = match tokio::time::timeout(Duration::from_millis(2_000), result).await {
            Ok(Ok(value)) => Some(value),
            Ok(Err(_)) | Err(_) => None,
        };
        let deleted = self
            .inner
            .deleted_children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut gate = self.inner.gate.lock().expect("gate lock");
        if gate.revision != revision {
            return;
        }
        if let Some(snapshot) = &snapshot {
            let accepted = snapshot["items"].as_array().cloned().unwrap_or_default();
            let managed = gate
                .policy
                .as_ref()
                .is_some_and(|policy| policy.managed_pass_policy.is_some());
            let scope = gate.management_scope.clone();
            gate.work_bindings.retain(|binding| {
                !(deleted.contains(&binding.child_id)
                    && (!managed
                        || accepted_proof(Some(snapshot), scope.as_ref(), binding).is_some())
                    && accepted.iter().any(|item| {
                        item["planId"] == binding.plan_id
                            && item["stepId"] == binding.step_id
                            && item["status"] == "accepted"
                    }))
            });
        }
        if gate.next_work != snapshot {
            let accepted_progress = snapshot
                .as_ref()
                .and_then(|value| value["items"].as_array())
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        item["status"] == "accepted"
                            && gate
                                .next_work
                                .as_ref()
                                .and_then(|old| old["items"].as_array())
                                .is_some_and(|old_items| {
                                    old_items.iter().any(|old| {
                                        old["planId"] == item["planId"]
                                            && old["stepId"] == item["stepId"]
                                            && old["status"] != "accepted"
                                    })
                                })
                    })
                });
            if accepted_progress
                && gate
                    .policy
                    .as_ref()
                    .is_none_or(|policy| policy.managed_pass_policy.is_none())
            {
                gate.attempts = 0;
            }
            if gate.next_work.is_some() {
                gate.revision += 1;
            }
            gate.next_work = snapshot;
            gate.correction_pending = false;
        }
        if gate
            .policy
            .as_ref()
            .is_some_and(|policy| policy.managed_pass_policy.is_some())
        {
            if let Err(error) = self.persist_gate(&gate) {
                gate.suspended = true;
                eprintln!("pa-daemon: managed source reconciliation persistence failed: {error:#}");
            }
        }
    }
    pub(super) fn next_work_view(gate: &Gate, rows: &[Value]) -> Value {
        if !gate.next_work_enabled {
            return Value::Null;
        }
        let reserved = rows
            .iter()
            .filter(|row| row["obligation"] == true || row["executionKind"] == "unknown")
            .count()
            + gate
                .work_bindings
                .iter()
                .filter(|binding| !rows.iter().any(|row| row["childId"] == binding.child_id))
                .count();
        let free = gate
            .policy
            .as_ref()
            .map_or(0, |p| p.max_active.saturating_sub(reserved));
        let mut snapshot = gate
            .next_work
            .clone()
            .unwrap_or_else(|| json!({"complete":false,"items":[]}));
        let items = snapshot["items"].as_array().cloned().unwrap_or_default();
        let ready: Vec<Value> = items
            .iter()
            .filter(|item| {
                item["status"] == "ready"
                    && !gate.work_bindings.iter().any(|binding| {
                        item["planId"] == binding.plan_id && item["stepId"] == binding.step_id
                            || item["ownership"].as_array().is_some_and(|owners| {
                                binding
                                    .ownership
                                    .iter()
                                    .any(|owner| owners.iter().any(|v| v == owner))
                            })
                    })
            })
            .cloned()
            .collect();
        let acceptance: Vec<Value> = gate.work_bindings.iter().filter(|binding| rows.iter().any(|row| row["childId"] == binding.child_id && row["obligation"] == false) && (if gate.policy.as_ref().is_some_and(|policy| policy.managed_pass_policy.is_some()) { accepted_proof(gate.next_work.as_ref(), gate.management_scope.as_ref(), binding).is_none() } else { !items.iter().any(|item| item["planId"] == binding.plan_id && item["stepId"] == binding.step_id && item["status"] == "accepted") })).map(|binding| json!({"childId":binding.child_id,"planId":binding.plan_id,"stepId":binding.step_id,"result":binding.started_result,"terminalDeliveryId":binding.terminal_delivery,"reviewConsumed":binding.review_consumed,"state":if binding.terminal_delivery.is_none(){"terminal_delivery_unproven"}else{"awaiting_acceptance"}})).collect();
        snapshot["actionRequired"] = json!(!acceptance.is_empty() || free > 0 && !ready.is_empty());
        snapshot["runtime"] = json!({"complete":rows.iter().all(|row| row["executionKind"] != "unknown"),"freeSlots":free,"ownership":gate.work_bindings,"ready":ready,"awaitingAcceptance":acceptance});
        snapshot
    }
    pub(crate) fn reserve_work(&self, child_id: &str, work: Option<&Value>) -> Result<()> {
        let mut gate = self.inner.gate.lock().expect("gate lock");
        if !gate.next_work_enabled {
            anyhow::ensure!(work.is_none(), "next_work is not activated by Studio");
            return Ok(());
        }
        anyhow::ensure!(
            gate.enabled() && !gate.suspended,
            "next_work admissions suspended"
        );
        let work =
            work.ok_or_else(|| anyhow!("Owner-scoped work ref is required before admission"))?;
        let snapshot = gate
            .next_work
            .as_ref()
            .ok_or_else(|| anyhow!("next_work source unknown; reconcile without resending"))?;
        anyhow::ensure!(
            snapshot["complete"] == true
                && work["snapshotRevision"].as_u64() == snapshot["revision"].as_u64(),
            "Incomplete or stale next_work admission"
        );
        let item = snapshot["items"]
            .as_array()
            .and_then(|items| {
                items.iter().find(|item| {
                    item["planId"] == work["planId"]
                        && item["stepId"] == work["stepId"]
                        && item["status"] == "ready"
                })
            })
            .ok_or_else(|| anyhow!("Work is not ready in owner-scoped Roadmap"))?;
        let ownership: Vec<String> = serde_json::from_value(item["ownership"].clone())?;
        anyhow::ensure!(
            !gate
                .work_bindings
                .iter()
                .any(|binding| binding.child_id == child_id
                    || item["planId"] == binding.plan_id && item["stepId"] == binding.step_id
                    || ownership
                        .iter()
                        .any(|owner| binding.ownership.contains(owner))),
            "Work or ownership remains reserved until confirmed STOP and acceptance/handoff"
        );
        let binding = Binding {
            child_id: child_id.into(),
            plan_id: item["planId"]
                .as_str()
                .ok_or_else(|| anyhow!("Invalid work plan"))?
                .into(),
            step_id: item["stepId"]
                .as_str()
                .ok_or_else(|| anyhow!("Invalid work step"))?
                .into(),
            snapshot_revision: snapshot["revision"].as_u64().unwrap(),
            ownership,
            start_permitted: false,
            started_result: None,
            terminal_delivery: None,
            review_consumed: false,
            task: gate.task.clone(),
            policy: gate.policy.clone(),
            start_token: Some(uuid::Uuid::new_v4().to_string()),
        };
        gate.work_bindings.push(binding);
        if gate
            .policy
            .as_ref()
            .is_none_or(|policy| policy.managed_pass_policy.is_none())
        {
            gate.attempts = 0;
        }
        gate.revision += 1;
        self.persist_gate(&gate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rlm_children::{ParentIdentity, RlmChildIdentity};
    use std::sync::Arc;
    fn sessions() -> SupervisorChildSessions {
        let sessions = SupervisorChildSessions::new(
            Arc::new(crate::supervisor_link::SupervisorLink::new(PathBuf::new())),
            std::env::temp_dir(),
            "parent".into(),
            Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                std::env::temp_dir(),
                true,
            )),
            String::new(),
        );
        sessions.set_identity(ParentIdentity {
            session_id: Some("root".into()),
            session_file: None,
            cwd: Some("/tmp".into()),
            ..ParentIdentity::with_default_depth()
        });
        sessions
    }
    #[test]
    fn managed_notice_consult_uses_private_goal_pending_and_once_result_guards() {
        use crate::engine::SessionEngine;
        let _faux = crate::agent_engine::tests::FAUX_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut engine, _dir) = crate::agent_engine::tests::faux_engine_with_settings(
            &json!({"responses":[{"text":"warm"},{"text":"goal"},{"text":"accepted A"}]}),
            u64::MAX,
        );
        engine.children = Some(Arc::new(sessions()));
        let engine = Arc::new(engine);
        engine.register_arc();
        let _work = crate::agent_engine::tests::goal_admission_collector(&engine);
        crate::agent_engine::tests::admit(&engine, "warm".into(), &mut Vec::new());
        crate::agent_engine::tests::admit(&engine, "/goal review results".into(), &mut Vec::new());
        let goal_id = engine.goal_state_value()["goalId"]
            .as_str()
            .unwrap()
            .to_string();
        let children = engine.children.as_ref().unwrap();
        let scope = json!({"rootSessionId":"root","goalId":goal_id,"taskGeneration":1,"policyRevision":1,"envelope":{"planIds":["p"],"requiredLeaves":[]}});
        let config = json!({"expectedRevision":0,"policy":{"version":1,"enabled":true,"minActive":1,"maxActive":2,"rootWritesCode":false,"correctiveBudget":2,"managedPassPolicy":"causal_results_v1"},
            "task":{"projectId":"project","planId":"p","stepId":"root","taskGeneration":1,"policyRevision":1},"assignments":[]});
        engine
            .runtime
            .block_on(children.configure_gate_scoped(&config, Some(scope), None))
            .unwrap();
        {
            let mut gate = children.inner.gate.lock().unwrap();
            gate.attempts = 2;
            let task = gate.task.clone();
            let policy = gate.policy.clone();
            gate.work_bindings.push(Binding {child_id:"A".into(),plan_id:"p".into(),step_id:"leaf".into(),snapshot_revision:1,ownership:vec!["A".into()],start_permitted:true,task,policy,start_token:None,
                started_result:Some(json!({"childId":"A","admissionGeneration":"g","expectedMessageId":"m","incarnation":"i"})),terminal_delivery:Some("t".into()),review_consumed:false});
        }
        let request = || json!({"beginManagedReview":{"childId":"A","terminalDeliveryId":"t","nativeNonce":crate::child_status_notices::mint()}});
        let handles = engine.goal_runtime.lock().unwrap().clone().unwrap();
        engine.runtime.block_on(async {
            handles
                .driver
                .lock()
                .await
                .pending_continuation_handle()
                .store(true, std::sync::atomic::Ordering::SeqCst);
        });
        // The outstanding ordinary goal mint keeps the new paid consult out.
        assert!(engine
            .runtime
            .block_on(engine.orchestration_gate(Some(request())))
            .is_err());
        engine
            .goal_budget_crossed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(engine
            .runtime
            .block_on(engine.orchestration_gate(Some(request())))
            .is_err());
        engine
            .goal_budget_crossed
            .store(false, std::sync::atomic::Ordering::SeqCst);
        *engine.goal_input_probe.lock().unwrap() = Some(Arc::new(|| true));
        assert!(engine
            .runtime
            .block_on(engine.orchestration_gate(Some(request())))
            .is_err());
        *engine.goal_input_probe.lock().unwrap() = Some(Arc::new(|| false));
        assert!(engine.runtime.block_on(engine.orchestration_gate(Some(json!({"beginManagedReview":{"childId":"A","terminalDeliveryId":"t","nativeNonce":"forged"}})))).is_err());
        assert!(!children.inner.gate.lock().unwrap().work_bindings[0].review_consumed);
        let handles = engine.goal_runtime.lock().unwrap().clone().unwrap();
        engine.runtime.block_on(async {
            handles.driver.lock().await.continuation_consumed();
        });
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probe_count = probes;
        *engine.goal_input_probe.lock().unwrap() = Some(Arc::new(move || {
            probe_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0
        }));
        assert!(engine
            .runtime
            .block_on(engine.orchestration_gate(Some(request())))
            .is_err());
        assert!(!children.inner.gate.lock().unwrap().work_bindings[0].review_consumed);
        *engine.goal_input_probe.lock().unwrap() = Some(Arc::new(|| false));
        assert!(engine
            .runtime
            .block_on(engine.orchestration_gate(Some(request())))
            .is_ok());
        assert!(engine
            .runtime
            .block_on(engine.orchestration_gate(Some(request())))
            .is_err());
        {
            let mut gate = children.inner.gate.lock().unwrap();
            let mut late = gate.work_bindings[0].clone();
            late.child_id = "late".into();
            late.terminal_delivery = Some("late-terminal".into());
            late.review_consumed = false;
            gate.work_bindings.push(late);
        }
        let late_probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let late_count = late_probes;
        *engine.goal_input_probe.lock().unwrap() = Some(Arc::new(move || {
            late_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 2
        }));
        assert!(engine.runtime.block_on(engine.orchestration_gate(Some(json!({"beginManagedReview":{"childId":"late","terminalDeliveryId":"late-terminal","nativeNonce":crate::child_status_notices::mint()}})))).is_err());
        assert!(children.inner.gate.lock().unwrap().work_bindings[1].review_consumed);
        *engine.goal_input_probe.lock().unwrap() = Some(Arc::new(|| false));
        assert!(engine.runtime.block_on(engine.orchestration_gate(Some(json!({"beginManagedReview":{"childId":"late","terminalDeliveryId":"late-terminal","nativeNonce":crate::child_status_notices::mint()}})))).is_err());
        let before_tokens = engine.goal_state_value()["tokensUsed"].as_u64().unwrap();
        let mut events = Vec::new();
        crate::agent_engine::tests::admit(&engine, "review actual A result".into(), &mut events);
        assert!(events.iter().any(|event| matches!(event,crate::engine::EngineEvent::AssistantMessage(message) if message.to_string().contains("accepted A"))));
        assert!(engine.goal_state_value()["tokensUsed"].as_u64().unwrap() >= before_tokens);
        assert_eq!(
            engine.runtime.block_on(children.gate_status())["correctiveAttempts"],
            2
        );
    }

    #[tokio::test]
    async fn typed_closure_joins_flat_refs_and_exact_native_result_without_new_credit() {
        let sessions = sessions();
        let result = json!({"childId":"A","expectedMessageId":"m","admissionGeneration":"g","incarnation":"i","terminalDeliveryId":"t"});
        let proof = json!({"schemaVersion":1,"policy":"causal_results_v1","rootSessionId":"root","goalId":"goal",
            "taskGeneration":1,"policyRevision":1,"ownerBindingId":"owner","planId":"p","stepId":"leaf", "result":result,
            "artifactRef":"artifact","acceptanceRef":"review","committedRevision":2});
        let _source = source(&sessions,json!({"schemaVersion":1,"instanceId":"instance","revision":2,"total":1,"nextOffset":null,
            "items":[{"planId":"p","stepId":"leaf","status":"accepted","artifactRef":"artifact","acceptanceRef":"review","completion":{"sessionId":"root","managedAcceptance":proof}}]})).await;
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.policy.as_mut().unwrap().managed_pass_policy = Some("causal_results_v1".into());
            gate.management_scope = Some(
                json!({"rootSessionId":"root","goalId":"goal","taskGeneration":1,"policyRevision":1,"ownerBindingId":"owner"}),
            );
            gate.attempts = 2;
            let task = gate.task.clone();
            let policy = gate.policy.clone();
            gate.work_bindings.push(Binding {child_id:"A".into(),plan_id:"p".into(),step_id:"leaf".into(),snapshot_revision:1,
                ownership:vec!["a".into()],start_permitted:true,task,policy,start_token:None,started_result:Some(json!({"childId":"A","expectedMessageId":"m","admissionGeneration":"g","incarnation":"i"})),
                terminal_delivery:Some("t".into()),review_consumed:true});
        }
        sessions.refresh_next_work().await;
        let mut gate = sessions.inner.gate.lock().unwrap();
        assert_eq!(
            accepted_proof(
                gate.next_work.as_ref(),
                gate.management_scope.as_ref(),
                &gate.work_bindings[0]
            ),
            Some(&proof)
        );
        assert_eq!(gate.attempts, 2);
        assert!(gate.work_bindings[0].review_consumed);
        let snapshot = gate.next_work.clone().unwrap();
        let scope = gate.management_scope.clone().unwrap();
        let binding = gate.work_bindings[0].clone();
        let rows = vec![json!({"childId":"A","obligation":false,"executionKind":"settled"})];
        assert!(
            SupervisorChildSessions::next_work_view(&gate, &rows)["runtime"]["awaitingAcceptance"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        for mutation in ["proofless", "forged_owner", "reopened", "incomplete"] {
            let mut changed = snapshot.clone();
            match mutation {
                "proofless" => {
                    changed["items"][0]["completion"] = Value::Null;
                }
                "forged_owner" => {
                    changed["items"][0]["completion"]["managedAcceptance"]["ownerBindingId"] =
                        json!("other");
                }
                "reopened" => {
                    changed["items"][0]["status"] = json!("ready");
                }
                "incomplete" => {
                    changed["complete"] = json!(false);
                }
                _ => unreachable!(),
            }
            gate.next_work = Some(changed);
            assert!(
                accepted_proof(gate.next_work.as_ref(), Some(&scope), &binding).is_none(),
                "{mutation}"
            );
            assert_eq!(
                SupervisorChildSessions::next_work_view(&gate, &rows)["runtime"]
                    ["awaitingAcceptance"]
                    .as_array()
                    .unwrap()
                    .len(),
                1,
                "{mutation}"
            );
        }
        for mutation in ["delivery", "start", "review"] {
            let mut changed = binding.clone();
            match mutation {
                "delivery" => changed.terminal_delivery = None,
                "start" => changed.started_result = None,
                "review" => changed.review_consumed = false,
                _ => unreachable!(),
            }
            assert!(accepted_proof(Some(&snapshot), Some(&scope), &changed).is_none());
        }
        gate.next_work = None;
        assert!(accepted_proof(None, Some(&scope), &binding).is_none());
        assert_eq!(
            SupervisorChildSessions::next_work_view(&gate, &rows)["runtime"]["awaitingAcceptance"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        gate.next_work = Some(snapshot);
        assert!(accepted_proof(gate.next_work.as_ref(), Some(&scope), &binding).is_some());
        gate.next_work.as_mut().unwrap()["items"][0]["completion"] = Value::Null;
        assert_eq!(
            SupervisorChildSessions::next_work_view(&gate, &rows)["runtime"]["awaitingAcceptance"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        gate.policy.as_mut().unwrap().managed_pass_policy = None;
        assert!(
            SupervisorChildSessions::next_work_view(&gate, &rows)["runtime"]["awaitingAcceptance"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn live_d_projection_does_not_hold_a_result_review_but_unknown_d_does() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("supervisor.sock");
        let listener = pa_types::platform::transport::bind_transport(&socket)
            .await
            .unwrap();
        let known = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let source_known = known.clone();
        tokio::spawn(async move {
            loop {
                let stream = listener.accept().await.unwrap();
                let known = source_known.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.split();
                    writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n").await.unwrap();
                    let mut reader = BufReader::new(reader);
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).await.unwrap() == 0 {
                            return;
                        }
                        let input: Value = serde_json::from_str(&line).unwrap();
                        let state = if known.load(std::sync::atomic::Ordering::SeqCst) {
                            json!({"workerInstanceId":"D-worker","isStreaming":true,"ownExecutionStarted":true,"runningMessageIds":["D-message"]})
                        } else {
                            json!({})
                        };
                        let response = crate::protocol::response_success(
                            input["id"].as_str(),
                            "get_state",
                            Some(state),
                        );
                        writer
                            .write_all(
                                format!("{}\n", serde_json::to_string(&response).unwrap())
                                    .as_bytes(),
                            )
                            .await
                            .unwrap();
                    }
                });
            }
        });
        let sessions = SupervisorChildSessions::new(
            Arc::new(crate::supervisor_link::SupervisorLink::new(socket)),
            dir.path().into(),
            "root".into(),
            Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                dir.path().into(),
                true,
            )),
            String::new(),
        );
        sessions.set_identity(ParentIdentity {
            session_id: Some("root".into()),
            ..ParentIdentity::with_default_depth()
        });
        let config = json!({"expectedRevision":0,"policy":{"version":1,"enabled":true,"minActive":1,"maxActive":2,"rootWritesCode":false,"correctiveBudget":2,"managedPassPolicy":"causal_results_v1"},"task":{"projectId":"project","planId":"p","stepId":"root","taskGeneration":1,"policyRevision":1},"assignments":[]});
        sessions.configure_gate_scoped(&config,Some(json!({"rootSessionId":"root","goalId":"goal","taskGeneration":1,"policyRevision":1,"envelope":{"planIds":["p"],"requiredLeaves":[]}})),None).await.unwrap();
        sessions
            .push_test_child(RlmChildIdentity {
                rlm_child_id: "D".into(),
                active_session_id: "D-active".into(),
                session_id: None,
                session_name: "D".into(),
            })
            .await;
        let record = sessions.inner.find_record("D").await.unwrap();
        {
            let mut row = record.lock().await;
            row.admission_generation = Some("D-generation".into());
            row.expected_message_id = Some("D-message".into());
        }
        let mut bind = config.clone();
        bind["expectedRevision"] = json!(1);
        bind["policy"]
            .as_object_mut()
            .unwrap()
            .remove("managedPassPolicy");
        bind["assignments"] = json!([{"childId":"D","admissionGeneration":"D-generation","incarnation":"D-worker","assignmentId":"D-assignment","taskGeneration":1,"ownership":"D","acceptanceRef":"D-review","artifactRef":"D-artifact","dependenciesSatisfied":true}]);
        sessions.configure_gate(&bind).await.unwrap();
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            let task = gate.task.clone();
            let policy = gate.policy.clone();
            gate.attempts = 2;
            gate.work_bindings.push(Binding {child_id:"A".into(),plan_id:"p".into(),step_id:"leaf".into(),snapshot_revision:1,ownership:vec!["A".into()],start_permitted:true,task,policy,start_token:None,started_result:Some(json!({"childId":"A","admissionGeneration":"g","expectedMessageId":"m","incarnation":"i"})),terminal_delivery:Some("t".into()),review_consumed:false});
        }
        assert!(sessions.any_running().await);
        let accounting = sessions.gate_status().await;
        assert_eq!(accounting["knownActive"], 1, "{accounting}");
        assert_eq!(accounting["unknown"], 0, "{accounting}");
        assert_eq!(accounting["complete"], true, "{accounting}");
        let request = json!({"childId":"A","terminalDeliveryId":"t"});
        let candidate = sessions.managed_review_candidate(&request).await;
        assert!(candidate.is_ok(), "{candidate:?}");
        known.store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(sessions.managed_review_candidate(&request).await.is_err());
    }

    #[tokio::test]
    async fn causal_review_requires_actual_start_and_consumes_once_without_reset() {
        let sessions = sessions();
        let scope = json!({"rootSessionId":"root","goalId":"goal","taskGeneration":1,"policyRevision":1,
            "envelope":{"planIds":["p"],"requiredLeaves":[{"planId":"p","stepId":"leaf"}]}});
        let config = json!({"expectedRevision":0,"policy":{"version":1,"enabled":true,"minActive":1,"maxActive":2,
            "rootWritesCode":false,"correctiveBudget":2,"managedPassPolicy":"causal_results_v1"},
            "task":{"projectId":"project","planId":"p","stepId":"root","taskGeneration":1,"policyRevision":1},"assignments":[]});
        sessions
            .configure_gate_scoped(&config, Some(scope), None)
            .await
            .unwrap();
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.attempts = 2;
            let task = gate.task.clone();
            let policy = gate.policy.clone();
            gate.work_bindings.push(Binding {
                child_id: "A".into(),
                plan_id: "p".into(),
                step_id: "leaf".into(),
                snapshot_revision: 1,
                ownership: vec!["a".into()],
                start_permitted: true,
                task,
                policy,
                start_token: None,
                started_result: None,
                terminal_delivery: Some("terminal".into()),
                review_consumed: false,
            });
        }
        let request = json!({"childId":"A","terminalDeliveryId":"terminal"});
        assert!(sessions.managed_review_candidate(&request).await.is_err());
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.work_bindings[0].started_result = Some(
                json!({"childId":"A","expectedMessageId":"m","admissionGeneration":"g","incarnation":"i"}),
            );
        }
        assert!(sessions.gate_correction().await.is_none());
        let candidate = sessions.managed_review_candidate(&request).await.unwrap();
        sessions.consume_managed_review(&candidate).unwrap();
        assert!(sessions.gate_correction().await.is_none());
        assert!(sessions.consume_managed_review(&candidate).is_err());
        assert!(sessions.managed_review_candidate(&request).await.is_err());
        let restored = {
            let gate = sessions.inner.gate.lock().unwrap();
            assert_eq!(gate.attempts, 2);
            assert_eq!(gate.consumed_managed_results.len(), 1);
            let consumed = gate.consumed_managed_results.clone();
            let mut persisted = serde_json::to_value(&*gate).unwrap();
            persisted["workBindings"][0]["acceptedClosure"] = json!({"stale":true});
            let restored: super::super::orchestration::Gate =
                serde_json::from_value(persisted).unwrap();
            assert!(restored.work_bindings[0].review_consumed);
            assert_eq!(restored.consumed_managed_results, consumed);
            assert!(restored.next_work.is_none());
            assert!(accepted_proof(
                restored.next_work.as_ref(),
                restored.management_scope.as_ref(),
                &restored.work_bindings[0]
            )
            .is_none());
            restored
        };
        *sessions.inner.gate.lock().unwrap() = restored;
        assert!(sessions.consume_managed_review(&candidate).is_err());
        assert!(sessions.managed_review_candidate(&request).await.is_err());
    }

    #[tokio::test]
    async fn causal_result_reserves_gate_boundary_against_settle_correction() {
        let sessions = sessions();
        let scope = json!({"rootSessionId":"root","goalId":"goal","taskGeneration":1,"policyRevision":1,
            "envelope":{"planIds":["p"],"requiredLeaves":[{"planId":"p","stepId":"leaf"}]}});
        let config = json!({"expectedRevision":0,"policy":{"version":1,"enabled":true,"minActive":1,"maxActive":2,
            "rootWritesCode":false,"correctiveBudget":2,"managedPassPolicy":"causal_results_v1"},
            "task":{"projectId":"project","planId":"p","stepId":"root","taskGeneration":1,"policyRevision":1},"assignments":[]});
        sessions
            .configure_gate_scoped(&config, Some(scope), None)
            .await
            .unwrap();
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.attempts = 0;
            let task = gate.task.clone();
            let policy = gate.policy.clone();
            gate.work_bindings.push(Binding {
                child_id: "A".into(),
                plan_id: "p".into(),
                step_id: "leaf".into(),
                snapshot_revision: 1,
                ownership: vec!["a".into()],
                start_permitted: true,
                task,
                policy,
                start_token: None,
                started_result: None,
                terminal_delivery: Some("terminal".into()),
                review_consumed: false,
            });
        }
        let request = json!({"childId":"A","terminalDeliveryId":"terminal"});
        assert!(sessions.managed_review_candidate(&request).await.is_err());
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.work_bindings[0].started_result = Some(
                json!({"childId":"A","expectedMessageId":"m","admissionGeneration":"g","incarnation":"i"}),
            );
        }
        assert!(sessions.gate_correction().await.is_none());
        let candidate = sessions.managed_review_candidate(&request).await.unwrap();
        sessions.consume_managed_review(&candidate).unwrap();
        assert!(sessions.gate_correction().await.is_none());
        assert!(sessions.consume_managed_review(&candidate).is_err());
        assert!(sessions.managed_review_candidate(&request).await.is_err());
        let gate = sessions.inner.gate.lock().unwrap();
        assert_eq!(gate.attempts, 0);
        assert_eq!(gate.consumed_managed_results.len(), 1);
    }

    #[tokio::test]
    async fn source_rebind_preserves_fences_and_rejects_scope_mismatch() {
        let sessions = sessions();
        let scope = json!({"rootSessionId":"root","goalId":"goal","taskGeneration":1,"policyRevision":1,
            "envelope":{"planIds":["p"],"requiredLeaves":[{"planId":"p","stepId":"leaf"}]}});
        let config = json!({"expectedRevision":0,"policy":{"version":1,"enabled":true,"minActive":1,"maxActive":2,
            "rootWritesCode":false,"correctiveBudget":2,"managedPassPolicy":"causal_results_v1"},
            "task":{"projectId":"project","planId":"p","stepId":"root","taskGeneration":1,"policyRevision":1},"assignments":[]});
        assert!(sessions.configure_gate(&config).await.is_err());
        sessions
            .configure_gate_scoped(&config, Some(scope), None)
            .await
            .unwrap();
        let mut model = config.clone();
        model["expectedRevision"] = json!(1);
        model["policy"]
            .as_object_mut()
            .unwrap()
            .remove("managedPassPolicy");
        let kept = sessions.configure_gate(&model).await.unwrap();
        assert_eq!(kept["policy"]["managedPassPolicy"], "causal_results_v1");
        model["expectedRevision"] = json!(2);
        model["policy"]["enabled"] = json!(false);
        assert!(sessions.configure_gate(&model).await.is_err());
        model["policy"]["enabled"] = json!(true);
        model["task"]["taskGeneration"] = json!(2);
        assert!(sessions.configure_gate(&model).await.is_err());
        model["task"]["taskGeneration"] = json!(1);
        model["task"]["policyRevision"] = json!(2);
        assert!(sessions.configure_gate(&model).await.is_err());
        for _ in 0..2 {
            sessions.gate_correction().await.unwrap();
            sessions.consume_gate_correction();
        }
        let (original_scope, mut request) = {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.suspended = true;
            let original_scope = gate.management_scope.clone().unwrap();
            let request = json!({"version":1,"expectedSessionId":"root","expectedGoalId":"goal",
            "expectedManagementOwner":original_scope["ownerBindingId"],"expectedGateRevision":gate.revision,
            "expectedTask":gate.task,"expectedPolicy":gate.policy,"envelope":original_scope["envelope"],
            "nextWorkSource":{"socketPath":"/tmp/source.sock","token":"fixture","instanceId":"new"}});
            (original_scope, request)
        };
        request["expectedManagementOwner"] = json!("wrong");
        assert!(sessions
            .rebind_next_work_source(&request, Some("goal"))
            .await
            .is_err());
        request["expectedManagementOwner"] = original_scope["ownerBindingId"].clone();
        let result = sessions
            .rebind_next_work_source(&request, Some("goal"))
            .await
            .unwrap();
        assert_eq!(result["correctiveAttempts"], 2);
        assert_eq!(result["suspended"], true);
        assert_eq!(result["managementScope"], original_scope);
        assert!(sessions.gate_correction().await.is_none());
        assert!(sessions
            .rebind_next_work_source(&request, Some("goal"))
            .await
            .is_err());
    }

    async fn source(sessions: &SupervisorChildSessions, value: Value) -> tempfile::TempDir {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("roadmap.sock");
        sessions.inner.identity.lock().unwrap().session_file =
            Some(dir.path().join("root.jsonl").display().to_string());
        let listener = pa_types::platform::transport::bind_transport(&socket)
            .await
            .unwrap();
        tokio::spawn(async move {
            loop {
                let stream = listener.accept().await.unwrap();
                let value = value.clone();
                tokio::spawn(async move {
                    let (mut reader, mut writer) = stream.split();
                    let mut request = Vec::new();
                    loop {
                        let mut byte = [0u8; 1];
                        reader.read_exact(&mut byte).await.unwrap();
                        request.push(byte[0]);
                        if request.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    let header = String::from_utf8(request).unwrap();
                    assert!(header.contains("Authorization: Bearer fixture"));
                    let length: usize = header
                        .lines()
                        .find_map(|line| line.strip_prefix("Content-Length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    let mut body = vec![0u8; length];
                    reader.read_exact(&mut body).await.unwrap();
                    let input: Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(input["action"], "next_work");
                    assert_eq!(input["identity"]["sessionId"], "root");
                    assert_eq!(input["params"]["instanceId"], "instance");
                    assert!(input["params"].get("planIds").is_none());
                    let body = serde_json::to_vec(&value).unwrap();
                    writer.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
                    writer.write_all(&body).await.unwrap();
                });
            }
        });
        sessions
            .install_next_work_source(
                &json!({"socketPath":socket,"token":"fixture","instanceId":"instance"}),
            )
            .unwrap();
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.task = Some(serde_json::from_value(json!({"projectId":"project","planId":"p","stepId":"root","taskGeneration":1,"policyRevision":1})).unwrap());
            gate.policy = Some(serde_json::from_value(json!({"version":1,"enabled":true,"minActive":1,"maxActive":2,"rootWritesCode":false,"correctiveBudget":2})).unwrap());
        }
        sessions.refresh_next_work().await;
        dir
    }
    fn projection() -> Value {
        json!({"schemaVersion":1,"instanceId":"instance","revision":7,"complete":true,"total":3,"nextOffset":null,
            "items":[{"planId":"p","stepId":"A","status":"blocked","ownership":["a"]},
            {"planId":"p","stepId":"B","status":"ready","ownership":["b"]},
            {"planId":"p","stepId":"D","status":"ready","ownership":["d"]}]})
    }
    #[tokio::test]
    async fn trusted_source_binds_before_admission_and_rejects_stale_blocked_duplicate_ownership() {
        let sessions = sessions();
        let _source = source(&sessions, projection()).await;
        for work in [
            json!({"planId":"p","stepId":"A","snapshotRevision":7}),
            json!({"planId":"p","stepId":"B","snapshotRevision":6}),
        ] {
            assert!(sessions.reserve_work("kid", Some(&work)).is_err());
        }
        let work = json!({"planId":"p","stepId":"B","snapshotRevision":7});
        sessions.reserve_work("B", Some(&work)).unwrap();
        assert!(sessions.reserve_work("duplicate", Some(&work)).is_err());
        let gate = sessions.inner.gate.lock().unwrap().clone();
        assert_eq!(gate.work_bindings.len(), 1);
        let view = SupervisorChildSessions::next_work_view(
            &gate,
            &[
                json!({"childId":"B","obligation":false,"executionKind":"idle"}),
                json!({"childId":"C","obligation":true,"executionKind":"executing"}),
            ],
        );
        assert_eq!(view["runtime"]["freeSlots"], 1);
        assert_eq!(view["runtime"]["ready"][0]["stepId"], "D");
        assert_eq!(view["actionRequired"], true);
        assert_eq!(view["runtime"]["awaitingAcceptance"][0]["stepId"], "B");
        sessions
            .reserve_work(
                "D",
                Some(&json!({"planId":"p","stepId":"D","snapshotRevision":7})),
            )
            .unwrap();
        assert_eq!(sessions.inner.gate.lock().unwrap().work_bindings.len(), 2);
    }
    #[tokio::test]
    async fn missing_or_wrong_instance_source_fails_closed_and_credentials_are_not_serialized() {
        let sessions = sessions();
        let mut value = projection();
        value["instanceId"] = json!("stale");
        let _source = source(&sessions, value).await;
        assert!(sessions
            .reserve_work(
                "B",
                Some(&json!({"planId":"p","stepId":"B","snapshotRevision":7}))
            )
            .is_err());
        let gate = sessions.inner.gate.lock().unwrap();
        let stored = serde_json::to_string(&*gate).unwrap();
        assert!(!stored.contains("fixture"));
        assert!(!stored.contains("token"));
        assert!(gate.next_work.is_none());
    }
    #[tokio::test]
    async fn invalidation_waits_for_admission_lock_and_revokes_snapshot_and_control() {
        let sessions = sessions();
        let _source = source(&sessions, projection()).await;
        sessions.refresh_next_work().await;
        let prior = sessions.inner.gate.lock().unwrap().revision;
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.correction_pending = true;
        }
        let admission = sessions.inner.spawn_gate.lock().await;
        let invalidating = sessions.clone();
        let mut invalidation = Box::pin(invalidating.invalidate_next_work());
        assert!(futures::poll!(invalidation.as_mut()).is_pending());
        assert!(sessions.inner.gate.lock().unwrap().next_work.is_some());
        drop(admission);
        let result = invalidation.await.unwrap();
        assert_eq!(
            result,
            json!({"revision":prior+1,"nextWorkInvalidated":true})
        );
        let gate = sessions.inner.gate.lock().unwrap();
        assert!(gate.next_work.is_none());
        assert!(!gate.correction_pending);
        assert!(gate.next_work_source.is_some());
        drop(gate);
        assert!(sessions
            .reserve_work(
                "stale",
                Some(&json!({"planId":"p","stepId":"B","snapshotRevision":7}))
            )
            .is_err());
    }
    #[tokio::test]
    async fn model_cannot_install_or_invalidate_owner_source() {
        let sessions = sessions();
        for field in [
            "nextWorkSource",
            "nextWork",
            "invalidateNextWork",
            "beginWorkStart",
        ] {
            let mut payload = json!({"expectedRevision":0,"policy":{},"task":{},"assignments":[]});
            payload[field] = json!(true);
            let error = sessions.configure_gate(&payload).await.unwrap_err();
            assert!(error.to_string().contains("Studio owner"));
        }
    }

    async fn start_fixture() -> (
        SupervisorChildSessions,
        tempfile::TempDir,
        Value,
        Arc<tokio::sync::Notify>,
    ) {
        start_fixture_with_state(json!({"workerInstanceId":"worker","isStreaming":true,"ownExecutionStarted":false,"runningMessageIds":[],"sessionActions":{"queuedCount":1}})).await
    }
    async fn start_fixture_with_state(
        state: Value,
    ) -> (
        SupervisorChildSessions,
        tempfile::TempDir,
        Value,
        Arc<tokio::sync::Notify>,
    ) {
        start_fixture_with_revision_changes(state, false).await
    }
    async fn start_fixture_with_revision_changes(
        state: Value,
        change_revision: bool,
    ) -> (
        SupervisorChildSessions,
        tempfile::TempDir,
        Value,
        Arc<tokio::sync::Notify>,
    ) {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut sessions = sessions();
        let source = source(&sessions, projection()).await;
        let socket = source.path().join("supervisor.sock");
        let listener = pa_types::platform::transport::bind_transport(&socket)
            .await
            .unwrap();
        Arc::get_mut(&mut sessions.inner).unwrap().link =
            Arc::new(crate::supervisor_link::SupervisorLink::new(socket));
        let state_read = Arc::new(tokio::sync::Notify::new());
        let server_read = state_read.clone();
        let server_gate = sessions.inner.clone();
        tokio::spawn(async move {
            loop {
                let stream = listener.accept().await.unwrap();
                let state_read = server_read.clone();
                let state = state.clone();
                let gate = server_gate.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.split();
                    writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n").await.unwrap();
                    let mut reader = BufReader::new(reader);
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    let request: Value = serde_json::from_str(&line).unwrap();
                    if change_revision {
                        gate.gate.lock().unwrap().revision += 1;
                    }
                    let response = crate::protocol::response_success(
                        request["id"].as_str(),
                        "get_state",
                        Some(state),
                    );
                    writer
                        .write_all(
                            format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes(),
                        )
                        .await
                        .unwrap();
                    state_read.notify_one();
                });
            }
        });
        sessions
            .reserve_work(
                "B",
                Some(&json!({"planId":"p","stepId":"B","snapshotRevision":7})),
            )
            .unwrap();
        sessions
            .push_test_child(RlmChildIdentity {
                rlm_child_id: "B".into(),
                active_session_id: "active".into(),
                session_id: Some("durable".into()),
                session_name: "B".into(),
            })
            .await;
        let record = sessions.inner.find_record("B").await.unwrap();
        {
            let mut row = record.lock().await;
            row.admission_generation = Some("generation".into());
            row.expected_message_id = Some("message".into());
        }
        let token = sessions.inner.gate.lock().unwrap().work_bindings[0]
            .start_token
            .clone();
        let permit = json!({"childId":"B","childActiveSessionId":"active","childSessionId":"durable","planId":"p","stepId":"B","sourceInstanceId":"instance","admissionGeneration":"generation","expectedMessageId":"message","incarnation":"worker","startToken":token});
        (sessions, source, permit, state_read)
    }

    #[tokio::test]
    async fn uncertain_current_start_does_not_consume_private_token() {
        let (sessions, _source, permit, _) =
            start_fixture_with_state(json!({"workerInstanceId":"worker"})).await;
        assert!(sessions
            .begin_work_start(&permit)
            .await
            .unwrap_err()
            .to_string()
            .contains("global UNKNOWN"));
        assert!(!sessions.inner.gate.lock().unwrap().work_bindings[0].start_permitted);
    }

    #[tokio::test]
    async fn stale_other_assignment_blocks_known_current_start_until_supported_reconciliation() {
        let (sessions, _source, permit, _) = start_fixture().await;
        let status = sessions.gate_status().await;
        let mut config = json!({"expectedRevision":status["revision"],"policy":status["policy"],"task":status["task"],
            "assignments":[{"childId":"stale-A","admissionGeneration":"old","incarnation":"old-worker","assignmentId":"A","taskGeneration":1,"ownership":"a","acceptanceRef":"review","artifactRef":"artifact","dependenciesSatisfied":true}]});
        // An already configured, now retired child is represented by the prior contract.
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            let mut persisted = serde_json::to_value(&*gate).unwrap();
            persisted["assignments"] = config["assignments"].clone();
            let mut stale: Gate = serde_json::from_value(persisted).unwrap();
            stale.next_work_source = gate.next_work_source.clone();
            stale.next_work = gate.next_work.clone();
            stale.work_bindings[0].start_token = gate.work_bindings[0].start_token.clone();
            *gate = stale;
        }
        assert!(sessions
            .begin_work_start(&permit)
            .await
            .unwrap_err()
            .to_string()
            .contains("global UNKNOWN"));
        assert!(!sessions.inner.gate.lock().unwrap().work_bindings[0].start_permitted);
        config["expectedRevision"] = sessions.gate_status().await["revision"].clone();
        config["assignments"] = json!([]);
        sessions.configure_gate(&config).await.unwrap();
        assert_eq!(
            sessions.begin_work_start(&permit).await.unwrap()["workStartPermitted"],
            true
        );
        assert!(sessions.begin_work_start(&permit).await.is_err());
    }

    #[tokio::test]
    async fn start_permit_linearizes_before_invalidation_and_never_replays() {
        let (sessions, _source, permit, _state_read) = start_fixture().await;
        assert_eq!(
            sessions.begin_work_start(&permit).await.unwrap()["workStartPermitted"],
            true
        );
        assert!(sessions.begin_work_start(&permit).await.is_err());
        sessions.invalidate_next_work().await.unwrap();
        assert!(sessions.inner.gate.lock().unwrap().work_bindings[0].start_permitted);
    }

    #[tokio::test]
    async fn start_permit_rejects_stale_or_forged_identity_without_consuming_current() {
        let (sessions, _source, permit, _state_read) = start_fixture().await;
        for field in [
            "admissionGeneration",
            "expectedMessageId",
            "childActiveSessionId",
            "childSessionId",
            "incarnation",
            "startToken",
        ] {
            let mut stale = permit.clone();
            stale[field] = json!("stale");
            assert!(sessions.begin_work_start(&stale).await.is_err(), "{field}");
            assert!(!sessions.inner.gate.lock().unwrap().work_bindings[0].start_permitted);
        }
        let mut missing = permit.clone();
        missing["childId"] = json!("missing");
        assert!(sessions.begin_work_start(&missing).await.is_err());
        let serialized = serde_json::to_value(&*sessions.inner.gate.lock().unwrap()).unwrap();
        assert!(serialized["workBindings"][0].get("startToken").is_none());
        assert_eq!(
            sessions.begin_work_start(&permit).await.unwrap()["workStartPermitted"],
            true
        );
    }

    #[tokio::test]
    async fn repeated_revision_changes_deny_after_bounded_resample() {
        let (sessions,_source,permit,_)=start_fixture_with_revision_changes(json!({"workerInstanceId":"worker","isStreaming":true,"ownExecutionStarted":false,"runningMessageIds":[],"sessionActions":{"queuedCount":1}}),true).await;
        assert!(sessions
            .begin_work_start(&permit)
            .await
            .unwrap_err()
            .downcast_ref::<StartRevisionChanged>()
            .is_some());
        assert!(!sessions.inner.gate.lock().unwrap().work_bindings[0].start_permitted);
    }

    #[tokio::test]
    async fn supported_assignment_cas_race_resamples_once_before_same_start() {
        let (sessions, _source, permit, state_read) = start_fixture().await;
        let gate = sessions.inner.gate.lock().unwrap().clone();
        let config = json!({"expectedRevision":gate.revision,"policy":gate.policy,"task":gate.task,"assignments":[]});
        let admission = sessions.inner.spawn_gate.lock().await;
        let starting_sessions = sessions.clone();
        let request = permit.clone();
        let starting =
            tokio::spawn(async move { starting_sessions.begin_work_start(&request).await });
        state_read.notified().await;
        sessions.configure_gate(&config).await.unwrap();
        drop(admission);
        assert_eq!(starting.await.unwrap().unwrap()["workStartPermitted"], true);
        assert!(sessions.begin_work_start(&permit).await.is_err());
        assert!(sessions.inner.gate.lock().unwrap().work_bindings[0].start_permitted);
    }

    #[tokio::test]
    async fn start_permit_rechecks_record_and_task_after_waiting_for_admission_lock() {
        for change in [
            "generation",
            "closed",
            "settled",
            "blocked",
            "task",
            "policy",
            "epoch",
        ] {
            let (sessions, _source, permit, state_read) = start_fixture().await;
            let admission = sessions.inner.spawn_gate.lock().await;
            let starting_sessions = sessions.clone();
            let starting =
                tokio::spawn(async move { starting_sessions.begin_work_start(&permit).await });
            state_read.notified().await;
            assert!(!starting.is_finished());
            let record = sessions.inner.find_record("B").await.unwrap();
            {
                let mut row = record.lock().await;
                match change {
                    "generation" => row.admission_generation = Some("replacement".into()),
                    "closed" => row.closed_by_parent = true,
                    "settled" => row.settled = true,
                    "blocked" => row.admission_blocked = true,
                    _ => {}
                }
            }
            if change == "epoch" {
                sessions
                    .inner
                    .accounting_epoch
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            if change == "task" || change == "policy" {
                let mut gate = sessions.inner.gate.lock().unwrap();
                let mut task = serde_json::to_value(gate.task.as_ref().unwrap()).unwrap();
                task[if change == "task" {
                    "taskGeneration"
                } else {
                    "policyRevision"
                }] = json!(2);
                gate.task = Some(serde_json::from_value(task).unwrap());
            }
            drop(admission);
            assert!(starting.await.unwrap().is_err(), "{change}");
            assert!(!sessions.inner.gate.lock().unwrap().work_bindings[0].start_permitted);
        }
    }

    #[tokio::test]
    async fn unsent_create_release_reopens_work_but_unknown_reservation_keeps_capacity() {
        let sessions = sessions();
        let _source = source(&sessions, projection()).await;
        let work = json!({"planId":"p","stepId":"B","snapshotRevision":7});
        sessions.reserve_work("pending", Some(&work)).unwrap();
        assert!(sessions.reserve_work("duplicate", Some(&work)).is_err());
        let gate = sessions.inner.gate.lock().unwrap().clone();
        assert_eq!(
            SupervisorChildSessions::next_work_view(&gate, &[])["runtime"]["freeSlots"],
            1
        );
        sessions.release_unsent_work("pending").unwrap();
        sessions.reserve_work("replacement", Some(&work)).unwrap();
    }
}
