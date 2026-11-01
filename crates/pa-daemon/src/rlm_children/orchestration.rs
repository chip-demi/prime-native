//! Task-scoped orchestration accounting. No task text or automatic worker admission.
use super::{
    anyhow, durable_child_selector, json, now_ms, Arc, ChildRecord, DaemonCommand, Duration, Map,
    Path, PathBuf, Result, SupervisorChildSessions, SupervisorChildSessionsInner, Value,
    STATE_TIMEOUT_MS,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::io::Write;
type BootstrapFinalize<'a> = dyn Fn(&mut dyn FnMut() -> Result<Value>) -> Result<Value> + Sync + 'a;
mod legacy_closure;
use legacy_closure::LegacyClosureReceipt;

pub(crate) const DEFAULT_CORRECTIVE_BUDGET: u32 = 2;
pub(crate) const MAX_CORRECTIVE_BUDGET: u32 = 5;
pub(crate) const DEFAULT_MIN_ACTIVE: usize = 5;
pub(crate) const MAX_ACTIVE: usize = 20;
const SNAPSHOT_TIMEOUT_MS: u64 = 2_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Policy {
    version: u32,
    enabled: bool,
    min_active: usize,
    pub(super) max_active: usize,
    root_writes_code: bool,
    #[serde(default = "default_budget")]
    corrective_budget: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) managed_pass_policy: Option<String>,
}
fn default_budget() -> u32 {
    DEFAULT_CORRECTIVE_BUDGET
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Task {
    project_id: String,
    plan_id: String,
    step_id: String,
    pub(super) task_generation: u64,
    pub(super) policy_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Assignment {
    child_id: String,
    admission_generation: String,
    incarnation: String,
    assignment_id: String,
    task_generation: u64,
    ownership: String,
    acceptance_ref: String,
    artifact_ref: String,
    dependencies_satisfied: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Exception {
    reason: String,
    evidence: String,
    dependency: String,
    clear_condition: String,
    task_generation: u64,
    policy_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OwnerCancellationReceipt {
    version: u32,
    task: Task,
    assignment: Assignment,
    fact: crate::journal::OwnerCancelFact,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReconcileOwnerCancellation {
    expected_revision: u64,
    task: Task,
    assignment: Assignment,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QuarantineRequest {
    parent_session_file: String,
    expected_revision: u64,
    task: Task,
    targets: Vec<QuarantineTarget>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QuarantineTarget {
    assignment: Assignment,
    session_file: String,
    current_admission_generation: Option<String>,
    current_expected_message_id: Option<String>,
}

/// A retained management boundary, not permission to admit a model or worker turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManagementObligation {
    session_id: Option<String>,
    goal_id: String,
    task_generation: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Gate {
    pub(super) revision: u64,
    pub(super) policy: Option<Policy>,
    pub(super) task: Option<Task>,
    assignments: Vec<Assignment>,
    #[serde(default)]
    owner_cancellation_receipts: Vec<OwnerCancellationReceipt>,
    #[serde(default)]
    legacy_closure_receipts: Vec<LegacyClosureReceipt>,
    #[serde(skip)]
    pub(super) authenticated_owner_cancellations: Vec<crate::journal::OwnerCancelFact>,
    exception: Option<Exception>,
    pub(super) attempts: u32,
    #[serde(default)]
    pub(super) suspended: bool,
    #[serde(default)]
    pub(super) correction_pending: bool,
    #[serde(default)]
    pending_revision: u64,
    #[serde(default)]
    pub(super) next_work_enabled: bool,
    #[serde(skip)]
    pub(super) next_work_source: Option<super::next_work::Source>,
    #[serde(skip)]
    pub(super) next_work: Option<Value>,
    #[serde(default)]
    pub(super) work_bindings: Vec<super::next_work::Binding>,
    #[serde(default)]
    management_obligation: Option<ManagementObligation>,
    #[serde(default)]
    pub(super) management_scope: Option<Value>,
    #[serde(default)]
    pub(super) consumed_managed_results: Vec<Value>,
    #[serde(default)]
    pub(super) owner_bootstrap_receipt: Option<Value>,
}
impl Gate {
    pub(super) fn enabled(&self) -> bool {
        self.policy.as_ref().is_some_and(|p| p.enabled)
    }
    pub(super) fn start_accounting_complete(&self, rows: &[Value], permit: &Value) -> bool {
        if !rows.iter().any(|row| {
            row["childId"] == permit["childId"]
                && row["admissionGeneration"] == permit["admissionGeneration"]
                && row["incarnation"] == permit["incarnation"]
                && row["expectedMessageId"] == permit["expectedMessageId"]
                && matches!(row["executionKind"].as_str(), Some("queued" | "executing"))
        }) {
            return false;
        }
        if self.assignments.iter().any(|assignment| {
            !self.cancellation_settles(assignment, rows)
                && !self.legacy_closure_applies(assignment, rows)
                && !rows.iter().any(|row| {
                    assignment_matches(assignment, row) && row["executionKind"] != "unknown"
                })
        }) {
            return false;
        }
        rows.iter()
            .filter(|row| row["obligation"] == true)
            .all(|row| {
                let current = row["childId"] == permit["childId"]
                    && row["admissionGeneration"] == permit["admissionGeneration"]
                    && row["incarnation"] == permit["incarnation"]
                    && row["expectedMessageId"] == permit["expectedMessageId"]
                    && matches!(row["executionKind"].as_str(), Some("queued" | "executing"));
                current
                    || self.assignments.iter().any(|assignment| {
                        assignment_matches(assignment, row) && row["executionKind"] != "unknown"
                    })
            })
    }

    fn cancellation_settles(&self, assignment: &Assignment, rows: &[Value]) -> bool {
        self.owner_cancellation_receipts.iter().any(|receipt| {
            receipt.version == 1
                && self.task.as_ref() == Some(&receipt.task)
                && &receipt.assignment == assignment
                && rows.iter().any(|row| {
                    row["childId"] == assignment.child_id
                        && row["admissionGeneration"] == assignment.admission_generation
                        && row["obligation"] == false
                        && row["executionKind"] == "idle"
                        && serde_json::from_value::<crate::journal::OwnerCancelFact>(
                            row["ownerCancellation"].clone(),
                        )
                        .ok()
                        .as_ref()
                            == Some(&receipt.fact)
                })
        })
    }

    fn historical_owner_cancellation(
        &self,
        request: &ReconcileOwnerCancellation,
        parent: &str,
        child: &str,
        facts: &[crate::journal::OwnerCancelFact],
    ) -> Result<Value> {
        anyhow::ensure!(
            self.revision == request.expected_revision,
            "Orchestration revision conflict"
        );
        anyhow::ensure!(
            self.task.as_ref() == Some(&request.task)
                && self
                    .assignments
                    .iter()
                    .filter(|a| *a == &request.assignment)
                    .count()
                    == 1,
            "Exact persisted task and assignment required"
        );
        let mut matching = Vec::new();
        for fact in facts.iter().filter(|fact| {
            fact.child_id == request.assignment.child_id
                && fact.admission_generation.as_deref()
                    == Some(&request.assignment.admission_generation)
        }) {
            anyhow::ensure!(
                fact.parent_session_file == parent && fact.child_session_file == child,
                "Retained cancellation session identity mismatch"
            );
            if !matching.contains(fact) {
                matching.push(fact.clone());
            }
        }
        anyhow::ensure!(matching.len() <= 1, "Ambiguous retained cancellation facts");
        let fact = matching.pop();
        let status = if fact.is_some() {
            "found"
        } else {
            "not_found_in_parent_fact_set"
        };
        let message_identity_recorded = fact
            .as_ref()
            .and_then(|fact| fact.expected_message_id.as_deref())
            .is_some_and(|id| !id.is_empty());
        Ok(
            json!({"version":1,"revision":self.revision,"task":request.task,"assignment":request.assignment,
            "status":status,"source":"authenticated_parent_worker_retained_owner_cancel_facts",
            "coverage":"parent worker held fact set only; not a global history search",
            "ownerCancellation":fact,"messageIdentityRecorded":message_identity_recorded,
            "incarnationAuthenticated":false,"lifecycleFactsAvailable":false,
            "historicalSettlementProven":false,"exclusionApplicabilityProven":false,
            "processExitObserved":false,"sufficientForSettlement":false}),
        )
    }

    fn same_task(&self, task: &Task) -> bool {
        self.task.as_ref().is_some_and(|t| {
            t.project_id == task.project_id
                && t.plan_id == task.plan_id
                && t.step_id == task.step_id
                && t.task_generation == task.task_generation
        })
    }
}

impl SupervisorChildSessions {
    pub(crate) async fn owner_bootstrap(
        &self,
        payload: &Value,
        finalize: &BootstrapFinalize<'_>,
    ) -> Result<Value> {
        let identity = self.inner.identity.lock().expect("identity lock").clone();
        anyhow::ensure!(
            identity.rlm_depth == 0
                && payload["rootSessionId"].as_str() == identity.session_id.as_deref()
                && payload["sessionFile"].as_str() == identity.session_file.as_deref()
                && payload["cwd"].as_str() == identity.cwd.as_deref(),
            "Owner identity mismatch"
        );
        let rows = self.accounting_rows().await;
        anyhow::ensure!(
            rows.iter()
                .all(|row| row["obligation"] == false && row["executionKind"] != "unknown"),
            "Unresolved runtime accounting blocks owner bootstrap"
        );
        let epoch = self
            .inner
            .accounting_epoch
            .load(std::sync::atomic::Ordering::SeqCst);
        let snapshot = self.inner.gate.lock().expect("gate lock").clone();
        anyhow::ensure!(
            snapshot.enabled()
                && snapshot.suspended
                && snapshot
                    .assignments
                    .iter()
                    .all(|a| snapshot.cancellation_settles(a, &rows)
                        || snapshot.legacy_closure_applies(a, &rows)),
            "Configured owner reconciliation incomplete"
        );
        anyhow::ensure!(
            snapshot.management_scope.is_none() || snapshot.owner_bootstrap_receipt.is_some(),
            "Existing management owner cannot be replaced"
        );
        let view = json!({"version":1,"gateRevision":snapshot.revision,"accountingEpoch":epoch,"task":snapshot.task,"policy":snapshot.policy,
            "configuredAssignments":snapshot.assignments,"legacyClosureReceipts":snapshot.legacy_closure_receipts,
            "correctiveAttempts":snapshot.attempts,"suspended":snapshot.suspended,"admissionPermission":false,
            "rootSessionId":identity.session_id,"sessionFile":identity.session_file,"cwd":identity.cwd});
        if payload["operation"] == "owner_bootstrap_preflight" {
            let preflight_id = uuid::Uuid::new_v4().to_string();
            *self
                .inner
                .owner_preflight
                .lock()
                .expect("owner preflight lock") = Some((preflight_id.clone(), view.clone()));
            let mut response = view;
            response["preflightId"] = json!(preflight_id);
            return Ok(response);
        }
        anyhow::ensure!(
            payload["operation"] == "owner_bootstrap_commit"
                && payload["mode"] == "source_bootstrap_only",
            "Owner operation unavailable"
        );
        let mut replay_request = payload.clone();
        if let Some(object) = replay_request.as_object_mut() {
            object.remove("pinnedSource");
        }
        let digest = payload["operationId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("Owner operation identity missing"))?;
        if let Some(receipt) = &snapshot.owner_bootstrap_receipt {
            anyhow::ensure!(
                receipt["operationId"] == digest && receipt["request"] == replay_request,
                "Changed owner replay"
            );
            return Ok(receipt.clone());
        }
        anyhow::ensure!(
            self.inner
                .owner_preflight
                .lock()
                .expect("owner preflight lock")
                .as_ref()
                .is_some_and(
                    |(id, before)| payload["preflightId"].as_str() == Some(id.as_str())
                        && *before == view
                ),
            "Owner preflight changed"
        );
        anyhow::ensure!(
            payload["expectedGateRevision"].as_u64() == Some(snapshot.revision)
                && payload["expectedAccountingEpoch"].as_u64() == Some(epoch)
                && payload["expectedTask"] == serde_json::to_value(&snapshot.task)?
                && payload["expectedPolicy"] == serde_json::to_value(&snapshot.policy)?
                && payload["expectedAssignments"] == serde_json::to_value(&snapshot.assignments)?,
            "Owner snapshot changed"
        );
        anyhow::ensure!(
            payload["callback"]
                .as_object()
                .is_some_and(|o| o.keys().all(|k| [
                    "instanceId",
                    "operationId",
                    "envelope",
                    "intendedWork",
                    "expectedSourceSnapshotRevision",
                    "expectedSourceEpoch"
                ]
                .contains(&k.as_str())))
                && payload["callback"]["operationId"] == digest
                && payload["callback"]["envelope"] == payload["envelope"]
                && payload["callback"]["intendedWork"] == payload["intendedWork"],
            "Invalid owner callback"
        );
        let source = super::next_work::Source::parse(&payload["pinnedSource"])?;
        let confirmation=tokio::time::timeout(Duration::from_millis(2000),source.owner_callback(json!({"sessionId":identity.session_id,"sessionFile":identity.session_file,"cwd":identity.cwd}),payload["callback"].clone())).await??;
        anyhow::ensure!(
            confirmation["operationId"] == digest
                && confirmation["envelope"] == payload["envelope"]
                && confirmation["complete"] == true
                && confirmation["instanceId"].as_str() == Some(source.instance_id.as_str())
                && confirmation["sourceRevision"]
                    == payload["callback"]["expectedSourceSnapshotRevision"]
                && confirmation["sourceEpoch"] == payload["callback"]["expectedSourceEpoch"],
            "Owner source confirmation mismatch"
        );
        let source_snapshot = &confirmation["snapshot"];
        anyhow::ensure!(
            source_snapshot["complete"] == true
                && source_snapshot["revision"] == confirmation["sourceRevision"]
                && source_snapshot["items"]
                    .as_array()
                    .is_some_and(|items| items.len() <= 2000
                        && items
                            .iter()
                            .any(|item| item["planId"] == payload["intendedWork"]["planId"]
                                && item["stepId"] == payload["intendedWork"]["stepId"])),
            "Owner source snapshot mismatch"
        );
        // Never hold spawn admission across a callback: invalidation needs it to ACK.
        let _admission = self.inner.spawn_gate.lock().await;
        let fresh_rows = self.accounting_rows().await;
        anyhow::ensure!(
            fresh_rows
                .iter()
                .all(|r| r["obligation"] == false && r["executionKind"] != "unknown")
                && snapshot
                    .assignments
                    .iter()
                    .all(|a| snapshot.cancellation_settles(a, &fresh_rows)
                        || snapshot.legacy_closure_applies(a, &fresh_rows)),
            "Owner accounting changed"
        );
        finalize(&mut || {
            let mut gate = self.inner.gate.lock().expect("gate lock");
            let owner = self
                .inner
                .identity
                .try_lock()
                .map_err(|_| anyhow!("Owner identity busy"))?;
            anyhow::ensure!(
                owner.session_file == identity.session_file
                    && owner.session_id == identity.session_id
                    && gate.revision == snapshot.revision
                    && self
                        .inner
                        .accounting_epoch
                        .load(std::sync::atomic::Ordering::SeqCst)
                        == epoch,
                "Owner final CAS changed"
            );
            let mut next = gate.clone();
            let scope = json!({"rootSessionId":identity.session_id,"goalId":payload["expectedGoalId"],"taskGeneration":snapshot.task.as_ref().map(|t|t.task_generation),"policyRevision":snapshot.task.as_ref().map(|t|t.policy_revision),"ownerBindingId":uuid::Uuid::new_v4().to_string(),"envelope":payload["envelope"],"intendedWork":payload["intendedWork"]});
            let receipt = json!({"version":1,"operationId":digest,"request":replay_request,"revision":next.revision+1,"managementScope":scope,"envelope":payload["envelope"],"sourceInstanceId":confirmation["instanceId"],"reconciliationResolved":true,"admissionPermission":false,"pilotAdmitted":false,"automaticCorrectionAvailable":false});
            next.management_scope = Some(scope);
            next.next_work_source = Some(source.clone());
            next.next_work_enabled = true;
            next.next_work = Some(confirmation["snapshot"].clone());
            next.owner_bootstrap_receipt = Some(receipt.clone());
            next.revision += 1;
            drop(owner);
            self.persist_gate(&next)?;
            *gate = next;
            Ok(receipt)
        })
    }
    #[cfg(test)]
    pub(crate) fn inner_disable_gate_for_test(&self) {
        let mut gate = self.inner.gate.lock().expect("gate lock");
        if let Some(policy) = &mut gate.policy {
            policy.enabled = false;
        }
        gate.revision += 1;
    }
    pub(crate) fn bind_management_goal(&self, goal_id: &str) -> Result<()> {
        let mut gate = self.inner.gate.lock().expect("gate lock");
        let mut next = gate.clone();
        let Some(scope) = next.management_scope.as_mut() else {
            return Ok(());
        };
        anyhow::ensure!(
            scope["goalId"].is_null() || scope["goalId"] == goal_id,
            "Management goal replacement requires trusted renewal"
        );
        if scope["goalId"] == goal_id {
            return Ok(());
        }
        scope["goalId"] = json!(goal_id);
        self.persist_gate(&next)?;
        *gate = next;
        Ok(())
    }

    pub(crate) fn retain_management_obligation(&self, goal_id: &str) -> Result<()> {
        let session_id = self
            .inner
            .identity
            .lock()
            .expect("identity lock")
            .session_id
            .clone();
        let mut gate = self.inner.gate.lock().expect("gate lock");
        let Some(task) = gate.task.as_ref().filter(|_| gate.enabled()) else {
            return Ok(());
        };
        let obligation = ManagementObligation {
            session_id,
            goal_id: goal_id.to_string(),
            task_generation: task.task_generation,
        };
        if gate.management_obligation.as_ref() == Some(&obligation) {
            return Ok(());
        }
        let mut next = gate.clone();
        if let Some(scope) = next.management_scope.as_mut() {
            if scope["goalId"].is_null() {
                scope["goalId"] = json!(goal_id);
            }
            anyhow::ensure!(scope["goalId"] == goal_id, "Management goal changed");
        }
        next.management_obligation = Some(obligation);
        self.persist_gate(&next)?;
        *gate = next;
        Ok(())
    }

    pub(crate) fn has_management_obligation(&self) -> bool {
        self.inner
            .gate
            .lock()
            .expect("gate lock")
            .management_obligation
            .is_some()
    }

    pub(crate) fn gate_correction_permit(&self) -> Value {
        let gate = self.inner.gate.lock().expect("gate lock");
        json!({"revision":gate.pending_revision,"attempt":gate.attempts,"taskGeneration":gate.task.as_ref().map(|task|task.task_generation)})
    }
    pub(crate) fn gate_permit_is_current(&self, permit: &Value) -> bool {
        let gate = self.inner.gate.lock().expect("gate lock");
        gate.enabled()
            && !gate.suspended
            && gate.exception.is_none()
            && gate.correction_pending
            && permit["revision"].as_u64() == Some(gate.revision)
            && permit["attempt"].as_u64() == Some(u64::from(gate.attempts))
            && permit["taskGeneration"].as_u64()
                == gate.task.as_ref().map(|task| task.task_generation)
    }
    pub(crate) fn gate_correction_is_current(&self) -> bool {
        let gate = self.inner.gate.lock().expect("gate lock");
        gate.enabled()
            && !gate.suspended
            && gate.exception.is_none()
            && gate.correction_pending
            && gate.pending_revision == gate.revision
    }
    pub(crate) fn consume_gate_correction(&self) {
        self.inner
            .gate
            .lock()
            .expect("gate lock")
            .correction_pending = false;
    }
    pub(crate) fn gate_stops_continuation(&self) -> bool {
        let gate = self.inner.gate.lock().expect("gate lock");
        gate.suspended || gate.enabled()
    }
    /// Gate control owns the goal boundary only while reconciliation is
    /// needed. A passing gate or a scoped exception is not goal completion:
    /// it hands continuation back to the existing goal driver.
    pub(crate) async fn gate_owns_goal_boundary(&self) -> bool {
        let status = self.gate_status().await;
        let gate = self.inner.gate.lock().expect("gate lock");
        gate.suspended
            || (gate.enabled()
                && (status["revision"].as_u64() != Some(gate.revision)
                    || status["accountingEpoch"].as_u64()
                        != Some(
                            self.inner
                                .accounting_epoch
                                .load(std::sync::atomic::Ordering::SeqCst),
                        )
                    || status["unknown"].as_u64() != Some(0)
                    || (gate.correction_pending && gate.pending_revision == gate.revision)
                    || !matches!(status["decision"].as_str(), Some("pass" | "exception"))))
    }
    fn gate_path(&self) -> Option<PathBuf> {
        self.inner.gate_path()
    }
    pub(super) fn persist_gate(&self, gate: &Gate) -> Result<()> {
        self.inner.persist_gate(gate)
    }
    pub(crate) fn restore_gate(&self) {
        let Some(path) = self.gate_path() else {
            return;
        };
        if !path.exists() {
            return;
        }
        let restored = std::fs::read(&path)
            .map_err(anyhow::Error::from)
            .and_then(|data| serde_json::from_slice::<Gate>(&data).map_err(Into::into));
        let mut gate = self.inner.gate.lock().expect("gate lock");
        match restored {
            Ok(mut state) => {
                state.suspended = state.enabled();
                state.correction_pending = false;
                *gate = state;
            }
            Err(error) => {
                // A corrupt enabled policy must never degrade to disabled.
                gate.suspended = true;
                eprintln!("pa-daemon: orchestration policy recovery failed: {error:#}");
            }
        }
    }
    pub(crate) async fn request_accept_legacy_closure(&self, request: Value) -> Result<Value> {
        let token = std::env::var("PRIME_AGENT_INTERNAL_DAEMON_WORKER_TOKEN")
            .map_err(|_| anyhow!("Native owner authentication unavailable"))?;
        self.inner
            .command(
                &DaemonCommand::ConfigureOrchestrationGate {
                    id: None,
                    active_session_id: self.inner.parent_active_session_id.clone(),
                    rest: Map::from_iter([
                        ("workerToken".into(), json!(token)),
                        ("acceptLegacyClosure".into(), request),
                    ]),
                },
                STATE_TIMEOUT_MS,
            )
            .await
    }

    pub(crate) async fn request_quarantine_sessions(&self, request: Value) -> Result<Value> {
        let token = std::env::var("PRIME_AGENT_INTERNAL_DAEMON_WORKER_TOKEN")
            .map_err(|_| anyhow!("Native owner authentication unavailable"))?;
        self.inner
            .command(
                &DaemonCommand::ConfigureOrchestrationGate {
                    id: None,
                    active_session_id: self.inner.parent_active_session_id.clone(),
                    rest: Map::from_iter([
                        ("workerToken".into(), json!(token)),
                        ("quarantineSessions".into(), request),
                    ]),
                },
                STATE_TIMEOUT_MS,
            )
            .await
    }

    /// Future admission fence only. Existing work and old execution obligations remain unknown.
    pub(crate) async fn quarantine_sessions(&self, payload: &Value) -> Result<Value> {
        let mut request: QuarantineRequest = serde_json::from_value(payload.clone())?;
        request
            .targets
            .sort_by(|a, b| a.assignment.child_id.cmp(&b.assignment.child_id));
        anyhow::ensure!(
            !request.targets.is_empty() && request.targets.len() <= MAX_ACTIVE,
            "Bounded session target list required"
        );
        let snapshot = self.inner.children.lock().await.clone();
        let mut selected = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for target in &request.targets {
            anyhow::ensure!(
                seen.insert(target.assignment.child_id.clone()),
                "Duplicate quarantine child"
            );
            let mut found = Vec::new();
            for record in &snapshot {
                if record.lock().await.rlm_child_id == target.assignment.child_id {
                    found.push(record.clone());
                }
            }
            anyhow::ensure!(found.len() == 1, "Unique direct child identity required");
            selected.push(found.pop().expect("one child"));
        }
        let mut rows = Vec::new();
        for record in selected {
            rows.push(record.lock_owned().await);
        }
        let children = self
            .inner
            .children
            .try_lock()
            .map_err(|_| anyhow!("Child registry busy; retry exact quarantine request"))?;
        anyhow::ensure!(
            children.len() == snapshot.len()
                && children
                    .iter()
                    .zip(&snapshot)
                    .all(|(a, b)| Arc::ptr_eq(a, b)),
            "Child registry changed"
        );
        let identity = self.inner.identity.lock().expect("identity lock").clone();
        anyhow::ensure!(
            identity.rlm_depth == 0,
            "Only root owner may quarantine sessions"
        );
        let parent = identity
            .session_file
            .as_deref()
            .ok_or_else(|| anyhow!("Parent identity unavailable"))?;
        anyhow::ensure!(
            parent == request.parent_session_file
                && Path::new(parent).is_absolute()
                && crate::lease::canonical_session_path(Path::new(parent)) == Path::new(parent),
            "Canonical parent required"
        );
        let gate = self.inner.gate.lock().expect("gate lock");
        anyhow::ensure!(
            gate.revision == request.expected_revision && gate.task.as_ref() == Some(&request.task),
            "Exact quarantine revision/task required"
        );
        let current_identity = self
            .inner
            .identity
            .try_lock()
            .map_err(|_| anyhow!("Parent identity busy; retry exact quarantine request"))?;
        anyhow::ensure!(
            current_identity.rlm_depth == 0
                && current_identity.session_file.as_deref() == Some(parent),
            "Parent identity changed"
        );
        let mut markers = Vec::new();
        for (target, row) in request.targets.iter().zip(&rows) {
            anyhow::ensure!(
                gate.assignments
                    .iter()
                    .filter(|a| *a == &target.assignment)
                    .count()
                    == 1,
                "Exact persisted assignment required"
            );
            anyhow::ensure!(
                row.session_file.as_deref() == Some(&target.session_file)
                    && row.admission_generation == target.current_admission_generation
                    && row.expected_message_id == target.current_expected_message_id
                    && Path::new(&target.session_file).is_absolute()
                    && crate::lease::canonical_session_path(Path::new(&target.session_file))
                        == Path::new(&target.session_file),
                "Exact current child session/admission required"
            );
            markers.push(crate::lease::SessionQuarantine {
                version: 1,
                parent_session_file: parent.into(),
                child_id: row.rlm_child_id.clone(),
                child_session_file: target.session_file.clone(),
                task: serde_json::to_value(&request.task)?,
                assignment: serde_json::to_value(&target.assignment)?,
                current_admission_generation: target.current_admission_generation.clone(),
                current_expected_message_id: target.current_expected_message_id.clone(),
            });
        }
        let mut effects = Vec::new();
        for marker in markers {
            match crate::lease::persist_session_quarantine(&self.inner.agent_dir, &marker) {
                Ok(()) => effects.push(json!({"childId":marker.child_id,"sessionFile":marker.child_session_file,"status":"future_fenced"})),
                Err(error) => return Ok(json!({"status":"partial_or_pending","revision":gate.revision,"quarantines":effects,
                    "failedChildId":marker.child_id,"failedTargetMayBeFenced":true,"error":format!("{error:#}"),"historicalSettlementProven":false,
                    "processExitObserved":false,"sufficientForSettlement":false})),
            }
        }
        Ok(
            json!({"status":"future_fenced_historical_coverage_pending","revision":gate.revision,
            "quarantines":effects,"historicalSettlementProven":false,"processExitObserved":false,"sufficientForSettlement":false}),
        )
    }

    pub(crate) async fn request_historical_owner_cancellation(
        &self,
        request: Value,
    ) -> Result<Value> {
        let token = std::env::var("PRIME_AGENT_INTERNAL_DAEMON_WORKER_TOKEN")
            .map_err(|_| anyhow!("Native owner authentication unavailable"))?;
        self.inner
            .command(
                &DaemonCommand::GetOrchestrationGate {
                    id: None,
                    active_session_id: self.inner.parent_active_session_id.clone(),
                    rest: Map::from_iter([
                        ("workerToken".into(), json!(token)),
                        ("historicalOwnerCancellation".into(), request),
                    ]),
                },
                STATE_TIMEOUT_MS,
            )
            .await
    }

    /// Inspect only the authenticated parent worker's retained cancellation facts.
    /// This does not restore/apply cancellations or establish historic incarnation settlement.
    pub(crate) async fn historical_owner_cancellation(
        &self,
        request: &Value,
        facts: &[crate::journal::OwnerCancelFact],
    ) -> Result<Value> {
        let request: ReconcileOwnerCancellation = serde_json::from_value(request.clone())?;
        let identity = self.inner.identity.lock().expect("identity lock").clone();
        anyhow::ensure!(
            identity.rlm_depth == 0,
            "Only a root owner may inspect retained cancellation facts"
        );
        let parent = identity
            .session_file
            .ok_or_else(|| anyhow!("Parent identity unavailable"))?;
        let records = self.inner.children.lock().await.clone();
        let registry_snapshot = records.clone();
        let mut matches = Vec::new();
        for record in records {
            if record.lock().await.rlm_child_id == request.assignment.child_id {
                matches.push(record);
            }
        }
        anyhow::ensure!(matches.len() == 1, "Unique direct child identity required");
        let record = matches.pop().expect("one direct child");
        let row = record.lock().await;
        let child = row
            .session_file
            .clone()
            .ok_or_else(|| anyhow!("Child session identity unavailable"))?;
        // A substituted symlink must not rebind an authenticated fact to another session.
        for file in [&parent, &child] {
            let path = Path::new(file);
            anyhow::ensure!(path.is_absolute(), "Canonical session identity required");
            let mut current = PathBuf::new();
            for component in path.components() {
                anyhow::ensure!(
                    !matches!(
                        component,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    ),
                    "Canonical session identity required"
                );
                current.push(component);
                let metadata = std::fs::symlink_metadata(&current)?;
                anyhow::ensure!(
                    !metadata.file_type().is_symlink(),
                    "Symlink session identity refused"
                );
            }
            anyhow::ensure!(
                std::fs::metadata(path)?.is_file(),
                "Session identity is not a file"
            );
        }
        // Linearize direct-child ownership after the awaited row lock, not its earlier clone.
        let current_records = self
            .inner
            .children
            .try_lock()
            .map_err(|_| anyhow!("Child registry busy during lookup"))?;
        anyhow::ensure!(
            current_records
                .iter()
                .filter(|candidate| Arc::ptr_eq(candidate, &record))
                .count()
                == 1,
            "Direct child ownership changed during lookup"
        );
        anyhow::ensure!(
            current_records.len() == registry_snapshot.len()
                && current_records
                    .iter()
                    .zip(&registry_snapshot)
                    .all(|(current, old)| Arc::ptr_eq(current, old)),
            "Child registry changed during lookup"
        );
        let current = self.inner.identity.lock().expect("identity lock");
        anyhow::ensure!(
            current.rlm_depth == 0 && current.session_file.as_deref() == Some(&parent),
            "Parent owner changed during lookup"
        );
        let gate = self.inner.gate.lock().expect("gate lock");
        gate.historical_owner_cancellation(&request, &parent, &child, facts)
    }

    /// Bind already authenticated owner exclusion to one retained obligation. No kill or unsuspend.
    pub(crate) async fn reconcile_owner_cancellation(&self, request: &Value) -> Result<Value> {
        let request: ReconcileOwnerCancellation = serde_json::from_value(request.clone())?;
        anyhow::ensure!(
            self.inner.identity.lock().expect("identity lock").rlm_depth == 0,
            "Only a root session may reconcile its orchestration gate"
        );
        let record = self
            .inner
            .find_record(&request.assignment.child_id)
            .await
            .ok_or_else(|| anyhow!("Direct child unavailable"))?;
        let lock = record.lock().await.admission_lock.clone();
        let _admission = lock.lock().await;
        let row = record.lock().await;
        let parent = self
            .inner
            .identity
            .lock()
            .expect("identity lock")
            .session_file
            .clone();
        let facts = self
            .inner
            .gate
            .lock()
            .expect("gate lock")
            .authenticated_owner_cancellations
            .clone();
        let fact = authenticated_cancellation(&row, parent.as_deref(), &facts)
            .ok_or_else(|| anyhow!("Authenticated restored owner cancellation required"))?;
        anyhow::ensure!(
            fact.admission_generation.as_deref() == Some(&request.assignment.admission_generation),
            "Cancelled admission mismatch"
        );
        let mut gate = self.inner.gate.lock().expect("gate lock");
        anyhow::ensure!(
            gate.revision == request.expected_revision,
            "Orchestration revision conflict"
        );
        anyhow::ensure!(
            gate.task.as_ref() == Some(&request.task)
                && gate.assignments.iter().any(|a| a == &request.assignment),
            "Exact persisted task and assignment required"
        );
        let receipt = OwnerCancellationReceipt {
            version: 1,
            task: request.task,
            assignment: request.assignment,
            fact,
        };
        if !gate.owner_cancellation_receipts.contains(&receipt) {
            let mut next = gate.clone();
            next.owner_cancellation_receipts.push(receipt.clone());
            next.revision += 1;
            anyhow::ensure!(self.gate_path().is_some(), "Durable gate path required");
            self.persist_gate(&next)?;
            *gate = next;
        }
        Ok(
            json!({"revision":gate.revision,"ownerCancellationReceipt":receipt,
            "kind":"owner_cancel","processExitObserved":false}),
        )
    }

    pub(crate) async fn configure_gate(&self, payload: &Value) -> Result<Value> {
        self.configure_gate_scoped(payload, None, None).await
    }

    pub(crate) async fn configure_gate_scoped(
        &self,
        payload: &Value,
        scope: Option<Value>,
        source: Option<super::next_work::Source>,
    ) -> Result<Value> {
        anyhow::ensure!(
            self.inner.identity.lock().expect("identity lock").rlm_depth == 0,
            "Only a root session may configure its orchestration gate"
        );
        anyhow::ensure!(
            payload.get("beginManagedReview").is_none()
                && payload.get("rebindNextWorkSource").is_none()
                && payload.get("managementScope").is_none()
                && payload.get("nextWorkSource").is_none()
                && payload.get("nextWork").is_none()
                && payload.get("invalidateNextWork").is_none()
                && payload.get("beginWorkStart").is_none(),
            "next_work source is installed only by the Studio owner"
        );
        let mut policy: Policy = serde_json::from_value(payload["policy"].clone())?;
        anyhow::ensure!(
            scope.is_some() || policy.managed_pass_policy.is_none(),
            "Managed policy is installed only by the Studio owner"
        );
        let task: Task = serde_json::from_value(payload["task"].clone())?;
        anyhow::ensure!(
            policy.version == 1
                && (1..=MAX_ACTIVE).contains(&policy.min_active)
                && (policy.min_active..=MAX_ACTIVE).contains(&policy.max_active)
                && policy.corrective_budget <= MAX_CORRECTIVE_BUDGET
                && policy
                    .managed_pass_policy
                    .as_deref()
                    .is_none_or(|mode| mode == "causal_results_v1"),
            "Invalid orchestration v1 policy"
        );
        anyhow::ensure!(
            [&task.project_id, &task.plan_id, &task.step_id]
                .iter()
                .all(|s| !s.trim().is_empty())
                && (1..=9_007_199_254_740_991).contains(&task.task_generation),
            "Task refs are required"
        );
        let assignments: Vec<Assignment> = serde_json::from_value(payload["assignments"].clone())?;
        let exception: Option<Exception> = payload
            .get("exception")
            .filter(|v| !v.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?;
        let binding_rows = if assignments.is_empty() {
            Vec::new()
        } else {
            self.accounting_rows().await
        };
        let mut ids = std::collections::HashSet::new();
        let mut owners = std::collections::HashSet::new();
        let mut children = std::collections::HashSet::new();
        for a in &assignments {
            anyhow::ensure!(
                [
                    &a.child_id,
                    &a.admission_generation,
                    &a.incarnation,
                    &a.assignment_id,
                    &a.ownership,
                    &a.acceptance_ref,
                    &a.artifact_ref
                ]
                .iter()
                .all(|s| !s.trim().is_empty())
                    && a.task_generation == task.task_generation
                    && ids.insert(&a.assignment_id)
                    && owners.insert(&a.ownership)
                    && children.insert(&a.child_id),
                "Invalid or duplicate assignment refs"
            );
            anyhow::ensure!(
                binding_rows.iter().any(|row| row["childId"] == a.child_id
                    && row["admissionGeneration"] == a.admission_generation
                    && row["incarnation"] == a.incarnation),
                "Current worker incarnation required; reconcile replacements before rebinding"
            );
            let record = self
                .inner
                .find_record(&a.child_id)
                .await
                .ok_or_else(|| anyhow!("Unknown child binding"))?;
            anyhow::ensure!(
                record.lock().await.admission_generation.as_deref()
                    == Some(&a.admission_generation),
                "Stale child admission generation"
            );
        }
        if let Some(e) = &exception {
            anyhow::ensure!(
                [&e.reason, &e.evidence, &e.dependency, &e.clear_condition]
                    .iter()
                    .all(|s| !s.trim().is_empty())
                    && e.task_generation == task.task_generation
                    && e.policy_revision == task.policy_revision,
                "Exception needs current task/revision and evidence"
            );
        }
        {
            let mut guard = self.inner.gate.lock().expect("gate lock");
            anyhow::ensure!(
                payload["expectedRevision"].as_u64() == Some(guard.revision),
                "Orchestration revision conflict"
            );
            let mut next = guard.clone();
            if scope.is_none()
                && next
                    .policy
                    .as_ref()
                    .is_some_and(|policy| policy.managed_pass_policy.is_some())
            {
                anyhow::ensure!(
                    next.task.as_ref() == Some(&task)
                        && policy.enabled
                        && next
                            .policy
                            .as_ref()
                            .is_some_and(|old| old.version == policy.version
                                && old.min_active == policy.min_active
                                && old.max_active == policy.max_active
                                && old.root_writes_code == policy.root_writes_code
                                && old.corrective_budget == policy.corrective_budget),
                    "Managed policy alteration is owner-only"
                );
                policy.managed_pass_policy = next
                    .policy
                    .as_ref()
                    .and_then(|policy| policy.managed_pass_policy.clone());
            }
            if !next.same_task(&task) {
                next.attempts = 0;
                next.management_obligation = None;
            }
            if let Some(mut scope) = scope {
                let identity = self.inner.identity.lock().expect("identity lock");
                anyhow::ensure!(
                    scope["rootSessionId"].as_str() == identity.session_id.as_deref()
                        && scope["taskGeneration"] == task.task_generation
                        && scope["policyRevision"] == task.policy_revision
                        && scope["envelope"]["planIds"]
                            .as_array()
                            .is_some_and(|ids| !ids.is_empty())
                        && scope["envelope"]["requiredLeaves"].is_array(),
                    "Invalid management scope"
                );
                if let Some(old) = next.management_scope.as_ref() {
                    if scope.get("ownerBindingId").is_none() {
                        scope["ownerBindingId"] = old["ownerBindingId"].clone();
                    }
                    anyhow::ensure!(old == &scope, "Management scope replacement denied");
                } else {
                    anyhow::ensure!(
                        scope.get("ownerBindingId").is_none(),
                        "Management owner identity is Native-issued"
                    );
                    scope["ownerBindingId"] = json!(uuid::Uuid::new_v4().to_string());
                }
                next.management_scope = Some(scope);
            }
            if let Some(source) = source {
                next.next_work_source = Some(source);
                next.next_work_enabled = true;
                next.next_work = None;
            }
            next.revision += 1;
            next.policy = Some(policy);
            if next.task.as_ref() != Some(&task) || next.assignments != assignments {
                next.owner_cancellation_receipts.clear();
            }
            next.task = Some(task);
            next.assignments = assignments;
            next.exception = exception;
            next.suspended = false;
            self.persist_gate(&next)?;
            *guard = next;
        }
        Ok(self.gate_status().await)
    }
    pub(crate) async fn gate_status(&self) -> Value {
        self.refresh_next_work().await;
        let gate = self.inner.gate.lock().expect("gate lock").clone();
        let epoch = self
            .inner
            .accounting_epoch
            .load(std::sync::atomic::Ordering::SeqCst);
        let rows = self.accounting_rows().await;
        let current_revision = self.inner.gate.lock().expect("gate lock").revision;
        let revision_changed = current_revision != gate.revision
            || epoch
                != self
                    .inner
                    .accounting_epoch
                    .load(std::sync::atomic::Ordering::SeqCst);
        let mut active = 0;
        let mut unknown = 0;
        for assignment in &gate.assignments {
            if gate.cancellation_settles(assignment, &rows)
                || gate.legacy_closure_applies(assignment, &rows)
            {
                continue;
            }
            match rows.iter().find(|row| assignment_matches(assignment, row)) {
                Some(row)
                    if row["executionKind"] == "executing" && assignment.dependencies_satisfied =>
                {
                    active += 1;
                }
                Some(row) if row["executionKind"] != "unknown" => {}
                _ => unknown += 1,
            }
        }
        // Unbound obligations are visible unknown, never counted as active.
        unknown += rows
            .iter()
            .filter(|row| {
                row["obligation"] == true
                    && !gate.assignments.iter().any(|a| {
                        row["childId"] == a.child_id
                            && row["admissionGeneration"] == a.admission_generation
                            && row["incarnation"] == a.incarnation
                    })
            })
            .count();
        let next_work = SupervisorChildSessions::next_work_view(&gate, &rows);
        let decision = if revision_changed {
            "unknown"
        } else if gate.suspended {
            "suspended"
        } else if !gate.enabled() {
            "disabled"
        } else if gate.next_work_enabled && next_work["complete"] != true {
            "unknown"
        } else if gate.next_work_enabled && next_work["actionRequired"] == true {
            if gate.attempts >= gate.policy.as_ref().map_or(0, |p| p.corrective_budget) {
                "suspended"
            } else {
                "corrective"
            }
        } else if gate.next_work_enabled {
            "pass"
        } else if gate.exception.is_some() {
            "exception"
        } else if unknown == 0
            && active
                >= gate
                    .policy
                    .as_ref()
                    .map_or(DEFAULT_MIN_ACTIVE, |p| p.min_active)
        {
            "pass"
        } else if gate.attempts >= gate.policy.as_ref().map_or(0, |p| p.corrective_budget) {
            "suspended"
        } else if unknown > 0 {
            "unknown"
        } else {
            "corrective"
        };
        json!({"version":1,"activeSessionId":self.inner.parent_active_session_id,
            "sessionId":self.inner.identity.lock().expect("identity lock").session_id,
            "revision":gate.revision,"accountingEpoch":epoch,"task":gate.task,"policy":gate.policy,
            "decision":decision,"knownActive":active,"unknown":unknown,"observedAt":now_ms(),
            "complete":unknown==0 && !gate.suspended && !revision_changed,"exception":gate.exception,
            "correctiveAttempts":gate.attempts,"assignments":rows,
            "ownerCancellationReceipts":gate.owner_cancellation_receipts,
            "ownerBootstrapReceipt":gate.owner_bootstrap_receipt,
            "ownerAcceptedLegacyClosureReceipts":gate.legacy_closure_receipts,
            "managedContract":{"version":1,"sourceOnlyRebind":true,"causalResults":true},
            "managementScope":gate.management_scope,
            "managementObligation":gate.management_obligation.as_ref().map(|obligation| json!({
                "sessionId":obligation.session_id,"goalId":obligation.goal_id,
                "taskGeneration":obligation.task_generation,"state":if gate.correction_pending {"pending"} else {"owed"},
                "blockReason":if gate.suspended && gate.owner_bootstrap_receipt.is_some() {Some("corrective_budget_exhausted")} else if gate.suspended {Some("reconciliation_required")} else if unknown > 0 {Some("unknown_obligation")} else if decision == "suspended" {Some("corrective_budget_exhausted")} else {None},
                "admissionPermission":false})),
            "nextWorkSourceInstanceId":gate.next_work_source.as_ref().map(|source| source.instance_id.as_str()),"nextWork":next_work,"rootWritesCodeEnforcement":"policy-audit-only"})
    }
    pub(crate) async fn gate_correction(&self) -> Option<String> {
        if !self.gate_stops_continuation() {
            return None;
        }
        let status = self.gate_status().await;
        if !matches!(status["decision"].as_str(), Some("corrective" | "unknown")) {
            return None;
        }
        let mut gate = self.inner.gate.lock().expect("gate lock");
        if status["accountingEpoch"].as_u64()
            != Some(
                self.inner
                    .accounting_epoch
                    .load(std::sync::atomic::Ordering::SeqCst),
            )
            || status["revision"].as_u64() != Some(gate.revision)
            || !gate.enabled()
            || gate.suspended
            || gate.correction_pending
            || (status["unknown"] == 0
                && status["complete"] == true
                && gate.policy.as_ref().is_some_and(|policy| {
                    policy.managed_pass_policy.as_deref() == Some("causal_results_v1")
                })
                && gate.work_bindings.iter().any(|binding| {
                    binding.started_result.is_some()
                        && binding.terminal_delivery.is_some()
                        && super::next_work::accepted_proof(
                            gate.next_work.as_ref(),
                            gate.management_scope.as_ref(),
                            binding,
                        )
                        .is_none()
                }))
            || gate.exception.is_some()
            || gate.attempts >= gate.policy.as_ref()?.corrective_budget
        {
            return None;
        }
        gate.attempts += 1;
        gate.correction_pending = true;
        gate.pending_revision = gate.revision;
        if let Err(error) = self.persist_gate(&gate) {
            gate.suspended = true;
            eprintln!("pa-daemon: orchestration attempt persistence failed: {error:#}");
            return None;
        }
        if gate.next_work_enabled {
            return Some(format!("[next-work]\nCurrent owner-scoped Roadmap and runtime projection: {}. Accept completed artifacts first, then start an uncovered ready branch with rlm.spawn(..., work={{planId,stepId,snapshotRevision}}). UNKNOWN reserves capacity; never resend it. Files remain owned until confirmed STOP and acceptance/handoff. If no safe action exists, report the exact dependency and suspend. No fake work or silent scope reduction. Correction {} of {}.", status["nextWork"], gate.attempts, gate.policy.as_ref()?.corrective_budget));
        }
        Some(format!("[orchestration-gate]\nTask-linked executing workers: {}; unknown: {}. Reconcile current assignments with rlm.orchestration_status(), then bind real work with rlm.configure_orchestration_gate(payload), or record an evidence-backed exception. UNKNOWN delivery must not be resent. No fake work. This does not complete a goal. Corrective attempt {} of {}.",
            status["knownActive"],status["unknown"],gate.attempts,gate.policy.as_ref()?.corrective_budget))
    }
    pub(crate) async fn assert_gate_capacity(&self) -> Result<()> {
        let limit = {
            let gate = self.inner.gate.lock().expect("gate lock");
            if !gate.enabled() {
                return Ok(());
            }
            gate.policy.as_ref().map_or(MAX_ACTIVE, |p| p.max_active)
        };
        let records = self.inner.children.lock().await.clone();
        let mut reserved = 0;
        let mut child_ids = Vec::new();
        for record in records {
            let row = record.lock().await;
            child_ids.push(row.rlm_child_id.clone());
            if !row.settled || row.admission_blocked {
                reserved += 1;
            }
        }
        reserved += self
            .inner
            .gate
            .lock()
            .expect("gate lock")
            .work_bindings
            .iter()
            .filter(|binding| !child_ids.contains(&binding.child_id))
            .count();
        anyhow::ensure!(
            reserved < limit,
            "Orchestration max admission capacity reached (UNKNOWN reserves capacity)"
        );
        Ok(())
    }
    pub(crate) async fn accounting_rows(&self) -> Vec<Value> {
        let records = self.inner.children.lock().await.clone();
        let parent = self
            .inner
            .identity
            .lock()
            .expect("identity lock")
            .session_file
            .clone();
        let (facts, closures) = {
            let gate = self.inner.gate.lock().expect("gate lock");
            (
                gate.authenticated_owner_cancellations.clone(),
                gate.legacy_closure_receipts.clone(),
            )
        };
        let agent_dir = self.inner.agent_dir.clone();
        futures::stream::iter(records).map(|record| { let parent = parent.clone(); let facts = facts.clone(); let closures = closures.clone(); let agent_dir = agent_dir.clone(); async move {
            let (id, selector, generation, expected, blocked, obligation)={let row=record.lock().await;
                (row.rlm_child_id.clone(),durable_child_selector(row.session_id.as_deref(),&row.rlm_child_id),
                 row.admission_generation.clone(),row.expected_message_id.clone(),row.admission_blocked,!row.settled)};
            let command=DaemonCommand::GetState{id:None,active_session_id:selector,rest:Map::default()};
            let state=self.inner.command(&command,SNAPSHOT_TIMEOUT_MS).await.ok();
            let current=record.lock().await;
            let kind=if current.owner_cancelled && current.settled && !current.admission_blocked && current.admission_generation==generation { "idle" }
                else if blocked || current.admission_blocked || current.admission_generation!=generation || generation.is_none() { "unknown" }
                else if current.settled { "idle" }
                else { execution_kind(state.as_ref(),expected.as_deref()) };
            json!({"childId":id,"admissionGeneration":generation,"expectedMessageId":expected,
                "executionKind":kind,"obligation":obligation,"ownerCancellation":authenticated_cancellation(&current,parent.as_deref(),&facts),
                "ownerAcceptedLegacyClosures":closures.iter().filter(|receipt| receipt.applies_to_record(&agent_dir,parent.as_deref(),&current)).map(|receipt|receipt.authorization_digest.clone()).collect::<Vec<_>>(),
                "incarnation":state.as_ref().and_then(|s|s.get("workerInstanceId")),
                "observedAt":now_ms()})
        }}).buffer_unordered(4).collect().await
    }
}
// owner_cancelled is set only by authenticated journal restoration / owner cancellation.
fn authenticated_cancellation(
    row: &ChildRecord,
    parent: Option<&str>,
    facts: &[crate::journal::OwnerCancelFact],
) -> Option<crate::journal::OwnerCancelFact> {
    if !row.owner_cancelled || !row.settled || row.admission_blocked {
        return None;
    }
    let fact = crate::journal::OwnerCancelFact {
        parent_session_file: parent?.to_owned(),
        child_id: row.rlm_child_id.clone(),
        child_session_file: row.session_file.clone()?,
        admission_generation: Some(row.admission_generation.clone().filter(|s| !s.is_empty())?),
        expected_message_id: Some(row.expected_message_id.clone().filter(|s| !s.is_empty())?),
    };
    facts.contains(&fact).then_some(fact)
}
fn assignment_matches(a: &Assignment, row: &Value) -> bool {
    row["childId"] == a.child_id
        && row["admissionGeneration"] == a.admission_generation
        && row["incarnation"] == a.incarnation
}
fn execution_kind(state: Option<&Value>, expected: Option<&str>) -> &'static str {
    let Some(s) = state else {
        return "unknown";
    };
    if s["workerInstanceId"].as_str().is_none_or(str::is_empty) {
        return "unknown";
    }
    if s["isCompacting"] == true || s["isQuotaParked"] == true {
        return "maintenance";
    }
    let started = s["ownExecutionStarted"].as_bool();
    let Some(ids) = s["runningMessageIds"].as_array() else {
        return "unknown";
    };
    if started == Some(true) && expected.is_some_and(|id| ids.iter().any(|v| v == id)) {
        return "executing";
    }
    if s["isStreaming"] == true || s["sessionActions"]["queuedCount"].as_u64().unwrap_or(0) > 0 {
        return "queued";
    }
    if s["hasBackgroundBash"] == true {
        return "unknown";
    } // no job-generation ack yet
    "idle"
}

impl SupervisorChildSessionsInner {
    pub(super) fn gate_path(&self) -> Option<PathBuf> {
        self.identity
            .lock()
            .expect("identity lock")
            .session_file
            .as_ref()
            .map(|file| PathBuf::from(format!("{file}.orchestration-gate.json")))
    }
    pub(super) fn persist_gate(&self, gate: &Gate) -> Result<()> {
        if let Some(path) = self.gate_path() {
            let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            let mut options = std::fs::OpenOptions::new();
            options.create_new(true).write(true);
            pa_core::platform::perms::set_private_mode(&mut options);
            let mut file = options.open(&temp)?;
            file.write_all(&serde_json::to_vec(gate)?)?;
            file.sync_all()?;
            pa_core::platform::rename_onto(&temp, &path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rlm_children::{ParentIdentity, RlmChildIdentity};
    #[test]
    fn owner_cancel_receipt_requires_exact_restored_fact_and_cannot_prove_execution() {
        let task: Task = serde_json::from_value(configure(2)["task"].clone()).unwrap();
        let assignment: Assignment = serde_json::from_value(json!({"childId":"old","admissionGeneration":"g","incarnation":"historic",
            "assignmentId":"a","taskGeneration":1,"ownership":"lane","acceptanceRef":"accept","artifactRef":"artifact","dependenciesSatisfied":true})).unwrap();
        let fact = crate::journal::OwnerCancelFact {
            parent_session_file: "parent".into(),
            child_id: "old".into(),
            child_session_file: "child".into(),
            admission_generation: Some("g".into()),
            expected_message_id: Some("m".into()),
        };
        let mut gate = Gate {
            task: Some(task.clone()),
            assignments: vec![assignment.clone()],
            owner_cancellation_receipts: vec![OwnerCancellationReceipt {
                version: 1,
                task,
                assignment: assignment.clone(),
                fact: fact.clone(),
            }],
            ..Gate::default()
        };
        let old = json!({"childId":"old","admissionGeneration":"g","incarnation":null,"executionKind":"idle","obligation":false,"ownerCancellation":fact});
        assert!(!assignment_matches(&assignment, &old));
        assert!(gate.cancellation_settles(&assignment, std::slice::from_ref(&old)));
        let permit = json!({"childId":"new","admissionGeneration":"new-g","incarnation":"new-worker","expectedMessageId":"new-m"});
        let new = json!({"childId":"new","admissionGeneration":"new-g","incarnation":"new-worker","expectedMessageId":"new-m","executionKind":"executing","obligation":true});
        assert!(gate.start_accounting_complete(&[old.clone(), new.clone()], &permit));
        let mut missing = old.clone();
        missing["ownerCancellation"] = Value::Null;
        assert!(!gate.start_accounting_complete(&[missing, new.clone()], &permit));
        let mut obligated = old.clone();
        obligated["obligation"] = json!(true);
        assert!(!gate.start_accounting_complete(&[obligated, new], &permit));
        gate.task.as_mut().unwrap().task_generation += 1;
        assert!(!gate.cancellation_settles(&assignment, &[old]));
    }
    #[tokio::test]
    async fn historical_owner_cancellation_keeps_newer_child_unchanged_and_rejects_links() {
        let dir = tempfile::TempDir::new().unwrap();
        let parent = dir.path().join("parent.jsonl");
        let child = dir.path().join("child.jsonl");
        std::fs::write(&parent, "fixture parent").unwrap();
        std::fs::write(&child, "fixture child").unwrap();
        let sessions = sessions();
        sessions.set_identity(ParentIdentity {
            session_file: Some(parent.to_string_lossy().into()),
            ..ParentIdentity::with_default_depth()
        });
        sessions
            .push_test_child(super::super::RlmChildIdentity {
                rlm_child_id: "old".into(),
                active_session_id: "new-worker".into(),
                session_id: Some("child".into()),
                session_name: "child".into(),
            })
            .await;
        let record = sessions.inner.children.lock().await[0].clone();
        {
            let mut row = record.lock().await;
            row.session_file = Some(child.to_string_lossy().into());
            row.admission_generation = Some("newer-generation".into());
            row.expected_message_id = Some("newer-message".into());
            row.admission_blocked = true;
            row.settled = false;
        }
        let request = json!({"expectedRevision":0,"task":configure(2)["task"],"assignment":{"childId":"old","admissionGeneration":"old-g","incarnation":"old-worker","assignmentId":"a","taskGeneration":1,"ownership":"lane","acceptanceRef":"accept","artifactRef":"artifact","dependenciesSatisfied":true}});
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.task = Some(serde_json::from_value(request["task"].clone()).unwrap());
            gate.assignments = vec![serde_json::from_value(request["assignment"].clone()).unwrap()];
        }
        let fact = crate::journal::OwnerCancelFact {
            parent_session_file: parent.to_string_lossy().into(),
            child_session_file: child.to_string_lossy().into(),
            child_id: "old".into(),
            admission_generation: Some("old-g".into()),
            expected_message_id: Some("old-message".into()),
        };
        let before = serde_json::to_value(&*sessions.inner.gate.lock().unwrap()).unwrap();
        assert_eq!(
            sessions
                .historical_owner_cancellation(&request, std::slice::from_ref(&fact))
                .await
                .unwrap()["status"],
            "found"
        );
        assert_eq!(
            serde_json::to_value(&*sessions.inner.gate.lock().unwrap()).unwrap(),
            before
        );
        {
            let row = record.lock().await;
            assert_eq!(
                row.admission_generation.as_deref(),
                Some("newer-generation")
            );
            assert_eq!(row.expected_message_id.as_deref(), Some("newer-message"));
            assert!(row.admission_blocked && !row.settled && !row.owner_cancelled);
        }
        let mut forged = request.clone();
        forged["fact"] = json!(fact);
        assert!(sessions
            .historical_owner_cancellation(&forged, &[])
            .await
            .is_err());
        sessions.set_identity(ParentIdentity {
            rlm_depth: 1,
            session_file: Some(parent.to_string_lossy().into()),
            ..ParentIdentity::with_default_depth()
        });
        assert!(sessions
            .historical_owner_cancellation(&request, &[])
            .await
            .is_err());
        sessions.set_identity(ParentIdentity {
            session_file: Some(parent.to_string_lossy().into()),
            ..ParentIdentity::with_default_depth()
        });
        {
            let held_row = record.lock().await;
            let lookup =
                sessions.historical_owner_cancellation(&request, std::slice::from_ref(&fact));
            tokio::pin!(lookup);
            assert!(futures::poll!(&mut lookup).is_pending());
            sessions.inner.children.lock().await.clear();
            drop(held_row);
            assert!(lookup.await.is_err());
            sessions.inner.children.lock().await.push(record.clone());
        }
        {
            let held_row = record.lock().await;
            let lookup =
                sessions.historical_owner_cancellation(&request, std::slice::from_ref(&fact));
            tokio::pin!(lookup);
            assert!(futures::poll!(&mut lookup).is_pending());
            let held_registry = sessions.inner.children.lock().await;
            drop(held_row);
            assert!(lookup
                .await
                .unwrap_err()
                .to_string()
                .contains("Child registry busy"));
            drop(held_registry);
        }
        #[cfg(unix)]
        {
            let original = dir.path().join("original.jsonl");
            std::fs::rename(&child, &original).unwrap();
            std::os::unix::fs::symlink(&original, &child).unwrap();
            assert!(sessions
                .historical_owner_cancellation(&request, &[fact])
                .await
                .is_err());
        }
        sessions.inner.children.lock().await.push(record.clone());
        assert!(sessions
            .historical_owner_cancellation(&request, &[])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn session_quarantine_exact_owner_refs_leave_gate_and_old_obligation_unchanged() {
        let dir = tempfile::TempDir::new().unwrap();
        let parent = dir.path().join("parent.jsonl");
        let child = dir.path().join("child.jsonl");
        std::fs::write(&parent, "parent").unwrap();
        std::fs::write(&child, "child").unwrap();
        let sessions = SupervisorChildSessions::new(
            Arc::new(crate::supervisor_link::SupervisorLink::new(PathBuf::new())),
            dir.path().into(),
            "parent".into(),
            Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                dir.path().into(),
                true,
            )),
            String::new(),
        );
        sessions.set_identity(ParentIdentity {
            session_file: Some(parent.to_string_lossy().into()),
            ..ParentIdentity::with_default_depth()
        });
        sessions
            .push_test_child(super::super::RlmChildIdentity {
                rlm_child_id: "child".into(),
                active_session_id: "current".into(),
                session_id: Some("child".into()),
                session_name: "child".into(),
            })
            .await;
        let record = sessions.inner.children.lock().await[0].clone();
        {
            let mut row = record.lock().await;
            row.session_file = Some(child.to_string_lossy().into());
            row.admission_generation = Some("current-g".into());
            row.expected_message_id = Some("current-m".into());
            row.settled = false;
            row.admission_blocked = true;
        }
        let assignment = json!({"childId":"child","admissionGeneration":"old-g","incarnation":"old-worker","assignmentId":"a","taskGeneration":1,"ownership":"lane","acceptanceRef":"accept","artifactRef":"artifact","dependenciesSatisfied":true});
        let request = json!({"parentSessionFile":parent,"expectedRevision":7,"task":configure(2)["task"],"targets":[{"assignment":assignment,"sessionFile":child,"currentAdmissionGeneration":"current-g","currentExpectedMessageId":"current-m"}]});
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.revision = 7;
            gate.task = Some(serde_json::from_value(request["task"].clone()).unwrap());
            gate.assignments = vec![serde_json::from_value(assignment).unwrap()];
            gate.suspended = true;
            gate.attempts = 2;
        }
        let before = serde_json::to_value(&*sessions.inner.gate.lock().unwrap()).unwrap();
        let mut wrong = request.clone();
        wrong["expectedRevision"] = json!(6);
        assert!(sessions.quarantine_sessions(&wrong).await.is_err());
        wrong = request.clone();
        wrong["targets"][0]["sessionFile"] = json!(parent);
        assert!(sessions.quarantine_sessions(&wrong).await.is_err());
        wrong = request.clone();
        wrong["targets"][0]["assignment"]["ownership"] = json!("foreign");
        assert!(sessions.quarantine_sessions(&wrong).await.is_err());
        assert!(crate::lease::assert_session_not_quarantined(dir.path(), &child).is_ok());
        let result = sessions.quarantine_sessions(&request).await.unwrap();
        assert_eq!(
            result["status"],
            "future_fenced_historical_coverage_pending"
        );
        assert_eq!(result["sufficientForSettlement"], false);
        assert_eq!(
            sessions.quarantine_sessions(&request).await.unwrap(),
            result
        );
        assert_eq!(
            serde_json::to_value(&*sessions.inner.gate.lock().unwrap()).unwrap(),
            before
        );
        assert!(!record.lock().await.owner_cancelled);
        assert!(record.lock().await.admission_blocked);
        assert!(crate::lease::assert_session_not_quarantined(dir.path(), &child).is_err());
        assert!(crate::lease::assert_session_not_quarantined(dir.path(), &parent).is_ok());
        let second = dir.path().join("second.jsonl");
        std::fs::write(&second, "second").unwrap();
        sessions
            .push_test_child(super::super::RlmChildIdentity {
                rlm_child_id: "z-second".into(),
                active_session_id: "second".into(),
                session_id: Some("second".into()),
                session_name: "second".into(),
            })
            .await;
        let second_record = sessions.inner.children.lock().await[1].clone();
        {
            let mut row = second_record.lock().await;
            row.session_file = Some(second.to_string_lossy().into());
            row.admission_generation = Some("second-g".into());
            row.expected_message_id = Some("second-m".into());
        }
        let mut second_assignment = request["targets"][0]["assignment"].clone();
        second_assignment["childId"] = json!("z-second");
        second_assignment["assignmentId"] = json!("second");
        sessions
            .inner
            .gate
            .lock()
            .unwrap()
            .assignments
            .push(serde_json::from_value(second_assignment.clone()).unwrap());
        let conflict = crate::lease::SessionQuarantine {
            version: 1,
            parent_session_file: parent.to_string_lossy().into(),
            child_id: "z-second".into(),
            child_session_file: second.to_string_lossy().into(),
            task: json!({"different":"receipt"}),
            assignment: second_assignment.clone(),
            current_admission_generation: Some("second-g".into()),
            current_expected_message_id: Some("second-m".into()),
        };
        crate::lease::persist_session_quarantine(dir.path(), &conflict).unwrap();
        let mut batch = request.clone();
        batch["targets"].as_array_mut().unwrap().push(json!({"assignment":second_assignment,"sessionFile":second,"currentAdmissionGeneration":"second-g","currentExpectedMessageId":"second-m"}));
        let partial = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            sessions.quarantine_sessions(&batch),
        )
        .await
        .expect("two-target batch must not self-deadlock")
        .unwrap();
        assert_eq!(partial["status"], "partial_or_pending");
        assert_eq!(partial["quarantines"].as_array().unwrap().len(), 1);
        assert_eq!(partial["failedChildId"], "z-second");
        assert!(crate::lease::assert_session_not_quarantined(dir.path(), &child).is_err());
        sessions.set_identity(ParentIdentity {
            rlm_depth: 1,
            session_file: Some(parent.to_string_lossy().into()),
            ..ParentIdentity::with_default_depth()
        });
        assert!(sessions.quarantine_sessions(&request).await.is_err());
    }

    #[test]
    fn historical_owner_cancellation_is_exact_readonly_and_coverage_limited() {
        let task: Task = serde_json::from_value(configure(2)["task"].clone()).unwrap();
        let assignment: Assignment = serde_json::from_value(json!({"childId":"old","admissionGeneration":"old-g","incarnation":"old-worker",
            "assignmentId":"a","taskGeneration":1,"ownership":"lane","acceptanceRef":"accept","artifactRef":"artifact","dependenciesSatisfied":true})).unwrap();
        let gate = Gate {
            revision: 4,
            task: Some(task.clone()),
            assignments: vec![assignment.clone()],
            ..Gate::default()
        };
        let before = serde_json::to_value(&gate).unwrap();
        let request = ReconcileOwnerCancellation {
            expected_revision: 4,
            task,
            assignment,
        };
        let fact = crate::journal::OwnerCancelFact {
            parent_session_file: "parent".into(),
            child_session_file: "child".into(),
            child_id: "old".into(),
            admission_generation: Some("old-g".into()),
            expected_message_id: Some("old-message".into()),
        };
        let found = gate
            .historical_owner_cancellation(&request, "parent", "child", std::slice::from_ref(&fact))
            .unwrap();
        assert_eq!(found["ownerCancellation"], json!(fact));
        assert_eq!(found["status"], "found");
        assert_eq!(found["historicalSettlementProven"], false);
        assert_eq!(found["incarnationAuthenticated"], false);
        assert_eq!(found["exclusionApplicabilityProven"], false);
        assert_eq!(found["sufficientForSettlement"], false);
        assert_eq!(
            gate.historical_owner_cancellation(&request, "parent", "child", &[])
                .unwrap()["status"],
            "not_found_in_parent_fact_set"
        );
        let mut newer = fact.clone();
        newer.admission_generation = Some("newer-g".into());
        assert_eq!(
            gate.historical_owner_cancellation(&request, "parent", "child", &[newer])
                .unwrap()["ownerCancellation"],
            Value::Null
        );
        let mut missing_message = fact.clone();
        missing_message.expected_message_id = None;
        assert_eq!(
            gate.historical_owner_cancellation(
                &request,
                "parent",
                "child",
                std::slice::from_ref(&missing_message)
            )
            .unwrap()["messageIdentityRecorded"],
            false
        );
        assert!(gate
            .historical_owner_cancellation(
                &request,
                "parent",
                "child",
                &[fact.clone(), missing_message]
            )
            .is_err());
        for (parent, child) in [("foreign", "child"), ("parent", "foreign")] {
            assert!(gate
                .historical_owner_cancellation(&request, parent, child, std::slice::from_ref(&fact))
                .is_err());
        }
        let mut bad = ReconcileOwnerCancellation {
            expected_revision: 3,
            task: request.task.clone(),
            assignment: request.assignment.clone(),
        };
        assert!(gate
            .historical_owner_cancellation(&bad, "parent", "child", std::slice::from_ref(&fact))
            .is_err());
        bad.expected_revision = 4;
        bad.task.policy_revision += 1;
        assert!(gate
            .historical_owner_cancellation(&bad, "parent", "child", std::slice::from_ref(&fact))
            .is_err());
        bad.task = request.task;
        bad.assignment.artifact_ref = "different".into();
        assert!(gate
            .historical_owner_cancellation(&bad, "parent", "child", &[fact])
            .is_err());
        assert_eq!(serde_json::to_value(&gate).unwrap(), before);
    }

    #[test]
    fn retask_requires_current_message_start_not_old_busy_or_queue() {
        let mut state = json!({"workerInstanceId":"new-worker","isStreaming":true,
            "ownExecutionStarted":true,"runningMessageIds":["old"],"sessionActions":{"queuedCount":1}});
        assert_eq!(execution_kind(Some(&state), Some("new")), "queued");
        state["runningMessageIds"] = json!(["new"]);
        assert_eq!(execution_kind(Some(&state), Some("new")), "executing");
        state["ownExecutionStarted"] = json!(false);
        assert_eq!(execution_kind(Some(&state), Some("new")), "queued");
        state["workerInstanceId"] = Value::Null;
        assert_eq!(execution_kind(Some(&state), Some("new")), "unknown");
    }

    #[tokio::test]
    async fn owner_bootstrap_preflight_retains_server_snapshot_and_blocks_modified_view() {
        let dir = tempfile::TempDir::new().unwrap();
        let parent = dir.path().join("root.jsonl");
        let sessions = sessions();
        sessions.set_identity(ParentIdentity {
            session_id: Some("root".into()),
            session_file: Some(parent.to_string_lossy().into()),
            cwd: Some("/fixture".into()),
            ..ParentIdentity::with_default_depth()
        });
        sessions.configure_gate(&configure(2)).await.unwrap();
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.suspended = true;
            gate.attempts = 2;
        }
        let payload = json!({"version":1,"operation":"owner_bootstrap_preflight","rootSessionId":"root","sessionFile":parent,"cwd":"/fixture","expectedGoalId":"goal","intendedWork":{"planId":"p","stepId":"s"}});
        let finalize = |commit: &mut dyn FnMut() -> Result<Value>| commit();
        let view = sessions.owner_bootstrap(&payload, &finalize).await.unwrap();
        let preflight = sessions
            .inner
            .owner_preflight
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(preflight.0, view["preflightId"].as_str().unwrap());
        assert_eq!(preflight.1["configuredAssignments"], json!([]));
        let before = sessions.inner.gate.lock().unwrap().clone();
        let mut commit = payload.clone();
        commit["operation"] = json!("owner_bootstrap_commit");
        commit["operationId"] = json!("op");
        commit["mode"] = json!("source_bootstrap_only");
        commit["preflightId"] = json!("caller-recomputed");
        assert!(sessions.owner_bootstrap(&commit, &finalize).await.is_err());
        assert_eq!(
            serde_json::to_value(sessions.inner.gate.lock().unwrap().clone()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
    }

    #[tokio::test]
    async fn owner_bootstrap_source_cas_both_invalidation_orders_and_denied_final_goal() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for mode in ["commit_first", "invalidate_first", "goal_changed"] {
            let dir = tempfile::TempDir::new().unwrap();
            let socket = dir.path().join("source.sock");
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let sessions = std::sync::Arc::new(sessions());
            let file = dir.path().join("root.jsonl");
            std::fs::write(&file, "fixture").unwrap();
            sessions.set_identity(ParentIdentity {
                session_id: Some("root".into()),
                session_file: Some(file.to_string_lossy().into()),
                cwd: Some(dir.path().to_string_lossy().into()),
                ..ParentIdentity::with_default_depth()
            });
            sessions.configure_gate(&configure(2)).await.unwrap();
            {
                let mut gate = sessions.inner.gate.lock().unwrap();
                gate.suspended = true;
                gate.attempts = 2;
            }
            let identity = json!({"version":1,"operation":"owner_bootstrap_preflight","rootSessionId":"root","sessionFile":file,"cwd":dir.path(),"expectedGoalId":"goal","intendedWork":{"planId":"p","stepId":"s"}});
            let before = sessions.owner_bootstrap(&identity, &|f| f()).await.unwrap();
            let envelope = json!({"planIds":["p"],"requiredLeaves":[{"planId":"p","stepId":"s"}],"launchRevision":4});
            let mut request = identity.clone();
            request["operation"] = json!("owner_bootstrap_commit");
            request["preflightId"] = before["preflightId"].clone();
            request["mode"] = json!("source_bootstrap_only");
            request["operationId"] = json!("op");
            request["expectedGateRevision"] = before["gateRevision"].clone();
            request["expectedAccountingEpoch"] = before["accountingEpoch"].clone();
            request["expectedTask"] = before["task"].clone();
            request["expectedPolicy"] = before["policy"].clone();
            request["expectedAssignments"] = before["configuredAssignments"].clone();
            request["envelope"] = envelope.clone();
            request["pinnedSource"] =
                json!({"socketPath":socket,"token":"fixture-secret","instanceId":"source"});
            request["callback"] = json!({"instanceId":"source","operationId":"op","envelope":envelope,"intendedWork":{"planId":"p","stepId":"s"},"expectedSourceSnapshotRevision":4,"expectedSourceEpoch":0});
            let server_sessions = sessions.clone();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = vec![0u8; 8192];
                let _ = stream.read(&mut bytes).await.unwrap();
                if mode == "invalidate_first" {
                    server_sessions.invalidate_next_work().await.unwrap();
                }
                let body=json!({"operationId":"op","envelope":envelope,"complete":true,"instanceId":"source","sourceRevision":4,"sourceEpoch":0,
           "snapshot":{"complete":true,"revision":4,"planIds":["p"],"items":[{"planId":"p","stepId":"s","status":"ready"}]}}).to_string();
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                stream.shutdown().await.unwrap();
            });
            let result = sessions
                .owner_bootstrap(&request, &|f| {
                    anyhow::ensure!(mode != "goal_changed", "Owner goal changed");
                    f()
                })
                .await;
            server.await.unwrap();
            let gate = sessions.inner.gate.lock().unwrap().clone();
            assert!(gate.suspended);
            assert_eq!(gate.attempts, 2);
            assert_eq!(gate.assignments.len(), 0);
            assert!(!sessions.gate_permit_is_current(&json!({})));
            if mode == "commit_first" {
                assert_eq!(result.as_ref().unwrap()["admissionPermission"], false);
                assert!(gate.next_work.is_some());
                assert!(gate.management_scope.is_some());
                let replay = sessions.owner_bootstrap(&request, &|f| f()).await.unwrap();
                assert_eq!(result.unwrap(), replay);
                let mut changed = request.clone();
                changed["intendedWork"]["stepId"] = json!("foreign");
                assert!(sessions.owner_bootstrap(&changed, &|f| f()).await.is_err());
                sessions.invalidate_next_work().await.unwrap();
                assert!(sessions.inner.gate.lock().unwrap().next_work.is_none());
            } else {
                assert!(result.is_err());
                assert!(gate.management_scope.is_none());
                assert!(gate.next_work_source.is_none());
            }
        }
    }

    fn sessions() -> SupervisorChildSessions {
        SupervisorChildSessions::new(
            Arc::new(crate::supervisor_link::SupervisorLink::new(PathBuf::new())),
            std::env::temp_dir(),
            "parent".into(),
            Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                std::env::temp_dir(),
                true,
            )),
            String::new(),
        )
    }
    fn configure(budget: u32) -> Value {
        json!({"expectedRevision":0,"policy":{"version":1,"enabled":true,"minActive":5,
            "maxActive":20,"rootWritesCode":false,"correctiveBudget":budget},
            "task":{"projectId":"p","planId":"plan","stepId":"step","taskGeneration":1,"policyRevision":1},
            "assignments":[]})
    }
    #[tokio::test]
    async fn retained_debt_never_duplicates_pending_correction() {
        let sessions = sessions();
        sessions.configure_gate(&configure(2)).await.unwrap();
        sessions.retain_management_obligation("goal").unwrap();
        assert!(sessions.gate_correction().await.is_some());
        let permit = sessions.gate_correction_permit();
        for _ in 0..3 {
            sessions.retain_management_obligation("goal").unwrap();
            assert!(sessions.gate_correction().await.is_none());
            assert_eq!(sessions.gate_correction_permit(), permit);
        }
        assert_eq!(sessions.gate_status().await["correctiveAttempts"], 1);
    }

    #[tokio::test]
    async fn management_debt_coalesces_persists_and_never_resets_exhausted_budget() {
        let sessions = sessions();
        let temp = tempfile::TempDir::new().unwrap();
        sessions.set_identity(ParentIdentity {
            session_file: Some(temp.path().join("session.jsonl").display().to_string()),
            ..ParentIdentity::with_default_depth()
        });
        let mut config = configure(2);
        sessions.configure_gate(&config).await.unwrap();
        for _ in 0..2 {
            sessions.gate_correction().await.unwrap();
            sessions.consume_gate_correction();
        }
        sessions.retain_management_obligation("goal").unwrap();
        let before = std::fs::read(sessions.gate_path().unwrap()).unwrap();
        sessions.retain_management_obligation("goal").unwrap();
        assert_eq!(
            std::fs::read(sessions.gate_path().unwrap()).unwrap(),
            before
        );
        let debt = sessions.gate_status().await["managementObligation"].clone();
        assert_eq!(debt["blockReason"], "corrective_budget_exhausted");
        assert_eq!(debt["admissionPermission"], false);
        assert!(sessions.gate_correction().await.is_none());
        sessions.restore_gate();
        let restored = sessions.gate_status().await;
        assert_eq!(restored["correctiveAttempts"], 2);
        assert_eq!(restored["managementObligation"]["goalId"], "goal");
        assert_eq!(
            restored["managementObligation"]["blockReason"],
            "reconciliation_required"
        );
        assert!(sessions.gate_correction().await.is_none());
        config["expectedRevision"] = json!(1);
        let reconciled = sessions.configure_gate(&config).await.unwrap();
        assert_eq!(reconciled["correctiveAttempts"], 2);
        assert_eq!(reconciled["managementObligation"]["goalId"], "goal");
        assert!(sessions.gate_correction().await.is_none());
    }

    #[tokio::test]
    async fn budget_is_bounded_and_reads_or_policy_revision_do_not_reset_it() {
        let sessions = sessions();
        let mut config = configure(2);
        assert_eq!(
            sessions.configure_gate(&config).await.unwrap()["decision"],
            "corrective"
        );
        assert!(sessions.gate_correction().await.is_some());
        for _ in 0..4 {
            assert_eq!(sessions.gate_status().await["correctiveAttempts"], 1);
        }
        config["expectedRevision"] = json!(1);
        config["task"]["policyRevision"] = json!(2);
        sessions.configure_gate(&config).await.unwrap();
        assert!(sessions.gate_correction().await.is_none());
        sessions.consume_gate_correction();
        assert!(sessions.gate_correction().await.is_some());
        assert!(sessions.gate_correction().await.is_none());
        assert_eq!(sessions.gate_status().await["decision"], "suspended");
        config["expectedRevision"] = json!(2);
        config["task"]["taskGeneration"] = json!(2);
        assert_eq!(
            sessions.configure_gate(&config).await.unwrap()["correctiveAttempts"],
            0
        );
    }
    #[tokio::test]
    async fn zero_budget_safe_exit_and_exception_requires_current_evidence() {
        let sessions = sessions();
        let mut config = configure(0);
        assert_eq!(
            sessions.configure_gate(&config).await.unwrap()["decision"],
            "suspended"
        );
        assert!(sessions.gate_correction().await.is_none());
        config["expectedRevision"] = json!(1);
        config["exception"] = json!({"reason":"input","evidence":"roadmap step needs approval",
            "dependency":"user authority","clearCondition":"approval received","taskGeneration":1,"policyRevision":0});
        assert!(sessions.configure_gate(&config).await.is_err());
        config["exception"]["policyRevision"] = json!(1);
        assert_eq!(
            sessions.configure_gate(&config).await.unwrap()["decision"],
            "exception"
        );
        assert!(sessions.gate_correction().await.is_none());
    }
    #[tokio::test]
    async fn recovery_keeps_budget_and_requires_explicit_reconciliation() {
        let sessions = sessions();
        let temp = tempfile::TempDir::new().unwrap();
        sessions.set_identity(ParentIdentity {
            session_file: Some(temp.path().join("session.jsonl").display().to_string()),
            ..ParentIdentity::with_default_depth()
        });
        sessions.configure_gate(&configure(2)).await.unwrap();
        sessions.gate_correction().await.unwrap();
        sessions.restore_gate();
        let status = sessions.gate_status().await;
        assert_eq!(status["decision"], "suspended");
        assert_eq!(status["correctiveAttempts"], 1);
        assert!(sessions.gate_owns_goal_boundary().await);
        assert!(sessions.gate_correction().await.is_none());
    }
    #[tokio::test]
    async fn historical_unbound_unknown_without_obligation_does_not_hold_goal() {
        let sessions = sessions();
        let mut config = configure(2);
        config["exception"] = json!({"reason":"scoped dependency", "evidence":"known input",
            "dependency":"integration", "clearCondition":"input arrives",
            "taskGeneration":1,"policyRevision":1});
        sessions.configure_gate(&config).await.unwrap();
        sessions
            .push_test_child(RlmChildIdentity {
                rlm_child_id: "historical".into(),
                active_session_id: "gone".into(),
                session_id: None,
                session_name: "historical".into(),
            })
            .await;
        let record = sessions.inner.find_record("historical").await.unwrap();
        {
            let mut record = record.lock().await;
            record.settled_status = Some("completed");
            record.settled = true;
        }
        let status = sessions.gate_status().await;
        assert_eq!(status["assignments"][0]["executionKind"], "unknown");
        assert_eq!(status["assignments"][0]["obligation"], false);
        assert_eq!(status["unknown"], 0);
        assert!(!sessions.gate_owns_goal_boundary().await);
    }
    #[test]
    fn five_queued_or_maintenance_rows_are_never_execution() {
        let states = [
            json!({"workerInstanceId":"w","isStreaming":false,"ownExecutionStarted":false,
            "runningMessageIds":[],"sessionActions":{"queuedCount":1}}),
            json!({"workerInstanceId":"w","isStreaming":true,"ownExecutionStarted":true,
                "runningMessageIds":["new"],"isCompacting":true}),
        ];
        for s in states {
            assert_ne!(execution_kind(Some(&s), Some("new")), "executing");
        }
    }

    #[tokio::test]
    async fn studio_numeric_generation_roundtrip_rejects_strings_zero_and_unsafe_integer() {
        let sessions = sessions();
        let valid = configure(2);
        let status = sessions.configure_gate(&valid).await.unwrap();
        assert_eq!(status["task"]["taskGeneration"], 1);
        for generation in [json!("1"), json!(0), json!(9_007_199_254_740_992u64)] {
            let mut invalid = valid.clone();
            invalid["expectedRevision"] = json!(1);
            invalid["task"]["taskGeneration"] = generation;
            assert!(sessions.configure_gate(&invalid).await.is_err());
        }
    }

    #[tokio::test]
    async fn settle_or_config_mutation_during_snapshot_never_returns_complete_pass() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::sync::oneshot;
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("fake.sock");
        let listener = pa_types::platform::transport::bind_transport(&socket)
            .await
            .unwrap();
        let sessions = SupervisorChildSessions::new(
            Arc::new(crate::supervisor_link::SupervisorLink::new(socket)),
            dir.path().into(),
            "parent".into(),
            Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                dir.path().into(),
                true,
            )),
            String::new(),
        );
        sessions.configure_gate(&configure(2)).await.unwrap();
        sessions
            .push_test_child(RlmChildIdentity {
                rlm_child_id: "child".into(),
                active_session_id: "child".into(),
                session_id: None,
                session_name: "child".into(),
            })
            .await;
        let record = sessions.inner.children.lock().await[0].clone();
        {
            let mut row = record.lock().await;
            row.admission_generation = Some("generation".into());
            row.expected_message_id = Some("message".into());
        }
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (stream, ()) = {
                let stream = listener.accept().await.unwrap();
                (stream, ())
            };
            let (reader, mut writer) = stream.split();
            writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n").await.unwrap();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            started_tx.send(()).unwrap();
            release_rx.await.unwrap();
            let response = crate::protocol::response_success(
                request["id"].as_str(),
                "get_state",
                Some(json!({
                "workerInstanceId":"worker","ownExecutionStarted":true,"isStreaming":true,"runningMessageIds":["message"]})),
            );
            writer
                .write_all(format!("{}\n", serde_json::to_string(&response).unwrap()).as_bytes())
                .await
                .unwrap();
        });
        let read_sessions = sessions.clone();
        let reading = tokio::spawn(async move { read_sessions.gate_status().await });
        started_rx.await.unwrap();
        sessions
            .inner
            .accounting_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        record.lock().await.settled = true;
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.revision += 1;
        }
        release_tx.send(()).unwrap();
        let status = reading.await.unwrap();
        assert_eq!(status["decision"], "unknown");
        assert_eq!(status["complete"], false);
        assert_eq!(status["knownActive"], 0);
    }

    #[test]
    fn four_started_and_one_admitted_queue_do_not_meet_minimum() {
        let active = json!({"workerInstanceId":"worker","ownExecutionStarted":true,
            "runningMessageIds":["new"],"isStreaming":true,"sessionActions":{"queuedCount":0}});
        let mut queued = active.clone();
        queued["ownExecutionStarted"] = json!(false);
        queued["sessionActions"]["queuedCount"] = json!(1);
        let mut observations = vec![active.clone(); 4];
        observations.push(queued);
        let count = observations
            .iter()
            .filter(|row| execution_kind(Some(row), Some("new")) == "executing")
            .count();
        assert_eq!(count, 4);
        assert!(count < DEFAULT_MIN_ACTIVE);
        observations[4] = active;
        assert_eq!(
            observations
                .iter()
                .filter(|row| execution_kind(Some(row), Some("new")) == "executing")
                .count(),
            5
        );
    }
    #[tokio::test]
    async fn child_cannot_configure_gate_or_impersonate_root_task() {
        let sessions = sessions();
        sessions.set_identity(ParentIdentity {
            rlm_depth: 1,
            ..ParentIdentity::with_default_depth()
        });
        let result = sessions.configure_gate(&configure(2)).await;
        assert!(format!("{:#}", result.unwrap_err()).contains("Only a root"));
        assert_eq!(sessions.inner.gate.lock().unwrap().revision, 0);
    }
    #[test]
    fn replacement_incarnation_does_not_match_existing_assignment_binding() {
        let assignment = Assignment {
            child_id: "kid".into(),
            admission_generation: "g".into(),
            incarnation: "worker-old".into(),
            assignment_id: "a".into(),
            task_generation: 1,
            ownership: "o".into(),
            acceptance_ref: "accept".into(),
            artifact_ref: "artifact".into(),
            dependencies_satisfied: true,
        };
        let row = json!({"childId":"kid","admissionGeneration":"g","incarnation":"worker-new","executionKind":"executing"});
        assert!(!assignment_matches(&assignment, &row));
        let reconciled = Assignment {
            incarnation: "worker-new".into(),
            ..assignment
        };
        assert!(assignment_matches(&reconciled, &row));
    }

    #[tokio::test]
    async fn disabled_after_mint_revokes_current_correction_permit() {
        let sessions = sessions();
        sessions.configure_gate(&configure(2)).await.unwrap();
        assert!(sessions.gate_correction().await.is_some());
        assert!(sessions.gate_correction_is_current());
        sessions.inner_disable_gate_for_test();
        assert!(!sessions.gate_correction_is_current());
    }
    #[tokio::test]
    async fn configurable_min_max_accept_valid_ranges_and_preserve_budget_bounds() {
        for (min, max) in [(1, 1), (3, 3), (3, 8), (5, 20)] {
            let sessions = sessions();
            let mut config = configure(2);
            config["policy"]["minActive"] = json!(min);
            config["policy"]["maxActive"] = json!(max);
            let status = sessions.configure_gate(&config).await.unwrap();
            assert_eq!(status["policy"]["minActive"], min);
            assert_eq!(status["policy"]["maxActive"], max);
            assert_eq!(status["decision"], "corrective");
            assert!(sessions.gate_correction().await.is_some());
            sessions.consume_gate_correction();
            assert!(sessions.gate_correction().await.is_some());
            sessions.consume_gate_correction();
            assert!(sessions.gate_correction().await.is_none());
            assert_eq!(sessions.gate_status().await["decision"], "suspended");
        }
        for (min, max) in [(0, 20), (3, 2), (1, 21), (21, 21), (1, 0)] {
            let sessions = sessions();
            let mut config = configure(2);
            config["policy"]["minActive"] = json!(min);
            config["policy"]["maxActive"] = json!(max);
            assert!(sessions.configure_gate(&config).await.is_err());
        }
    }
    #[tokio::test]
    async fn three_live_executions_pass_min_three_but_two_do_not() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("range.sock");
        let listener = pa_types::platform::transport::bind_transport(&socket)
            .await
            .unwrap();
        let sessions = SupervisorChildSessions::new(
            Arc::new(crate::supervisor_link::SupervisorLink::new(socket)),
            dir.path().into(),
            "parent".into(),
            Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                dir.path().into(),
                true,
            )),
            String::new(),
        );
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(3));
        let active_server = active.clone();
        tokio::spawn(async move {
            loop {
                let stream = listener.accept().await.unwrap();
                let active = active_server.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.split();
                    writer.write_all(b"{\"type\":\"daemon_hello\",\"protocol\":{\"name\":\"prime-agent.daemon\",\"version\":7}}\n").await.unwrap();
                    let mut reader = BufReader::new(reader);
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).await.unwrap() == 0 {
                            return;
                        }
                        let request: Value = serde_json::from_str(&line).unwrap();
                        let index = request["command"]["activeSessionId"]
                            .as_str()
                            .unwrap()
                            .strip_prefix("kid-")
                            .unwrap()
                            .parse::<usize>()
                            .unwrap();
                        let executing = index < active.load(std::sync::atomic::Ordering::SeqCst);
                        let state = json!({"workerInstanceId":"worker","ownExecutionStarted":executing,"isStreaming":executing,"runningMessageIds":["message"],"sessionActions":{"queuedCount":0}});
                        let response = crate::protocol::response_success(
                            request["id"].as_str(),
                            "get_state",
                            Some(state),
                        );
                        if writer
                            .write_all(
                                format!("{}\n", serde_json::to_string(&response).unwrap())
                                    .as_bytes(),
                            )
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });
        let mut config = configure(2);
        config["policy"]["minActive"] = json!(3);
        config["policy"]["maxActive"] = json!(8);
        sessions.configure_gate(&config).await.unwrap();
        let mut assignments = Vec::new();
        for i in 0..3 {
            let id = format!("kid-{i}");
            sessions
                .push_test_child(RlmChildIdentity {
                    rlm_child_id: id.clone(),
                    active_session_id: id.clone(),
                    session_id: None,
                    session_name: id.clone(),
                })
                .await;
            let record = sessions.inner.find_record(&id).await.unwrap();
            {
                let mut row = record.lock().await;
                row.admission_generation = Some("generation".into());
                row.expected_message_id = Some("message".into());
            }
            assignments.push(json!({"childId":id,"admissionGeneration":"generation","incarnation":"worker","assignmentId":format!("assignment-{i}"),"taskGeneration":1,"ownership":format!("lane-{i}"),"acceptanceRef":"accept","artifactRef":"artifact","dependenciesSatisfied":true}));
        }
        config["expectedRevision"] = json!(1);
        config["assignments"] = json!(assignments);
        let status = sessions.configure_gate(&config).await.unwrap();
        assert_eq!(status["knownActive"], 3);
        assert_eq!(status["decision"], "pass");
        active.store(2, std::sync::atomic::Ordering::SeqCst);
        let status = sessions.gate_status().await;
        assert_eq!(status["knownActive"], 2);
        assert_eq!(status["decision"], "corrective");
    }
}
