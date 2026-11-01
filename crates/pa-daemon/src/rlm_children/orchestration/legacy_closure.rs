//! Explicit operator evidence policy for retained legacy bindings, not terminal proof.
use super::{
    anyhow, json, Arc, Assignment, ChildRecord, Deserialize, Gate, Path, PathBuf, Result,
    Serialize, SupervisorChildSessions, Task, Value, MAX_ACTIVE,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
const APPROVAL_ROOT: &str = "/etc/prime-agent/operator-approvals";
const POLICY: &str = "owner_accepted_bounded_exit_cleanup_with_sustained_quarantine_v1";
const MAX_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Evidence {
    path: String,
    sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Binding {
    assignment: Assignment,
    quarantine: crate::lease::SessionQuarantine,
    quarantine_sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Approval {
    version: u32,
    grant_id: String,
    parent_session_file: String,
    task: Task,
    evidence_policy: String,
    reason: String,
    acknowledged_residual_risk: String,
    evidence_root: String,
    evidence: Vec<Evidence>,
    bindings: Vec<Binding>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct LegacyClosureReceipt {
    version: u32,
    kind: String,
    approval: Approval,
    pub(super) authorization_digest: String,
    process_exit_observed: bool,
    historical_settlement_proven: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    grant_id: String,
    expected_revision: u64,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn bounded_read(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= MAX_BYTES,
        "Invalid bounded evidence file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        anyhow::ensure!(metadata.nlink() == 1, "Hardlinked evidence refused");
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    }
    let mut file = options.open(path)?;
    let opened = file.metadata()?;
    anyhow::ensure!(
        opened.is_file() && opened.len() <= MAX_BYTES,
        "Invalid opened evidence file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        anyhow::ensure!(
            opened.nlink() == 1 && opened.dev() == metadata.dev() && opened.ino() == metadata.ino(),
            "Evidence identity changed"
        );
    }
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() as u64 <= MAX_BYTES, "Evidence too large");
    Ok(bytes)
}
fn trusted_approval(path: &Path) -> Result<Vec<u8>> {
    #[cfg(not(unix))]
    anyhow::bail!("Operator approval ownership unsupported on this platform");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        for ancestor in path.ancestors() {
            let metadata = fs::symlink_metadata(ancestor)?;
            anyhow::ensure!(
                !metadata.file_type().is_symlink()
                    && metadata.uid() == 0
                    && metadata.mode() & 0o022 == 0,
                "Untrusted operator approval ownership"
            );
        }
        let metadata = fs::symlink_metadata(path)?;
        anyhow::ensure!(
            matches!(metadata.mode() & 0o777, 0o600 | 0o444),
            "Invalid operator approval mode"
        );
        let mut options = fs::OpenOptions::new();
        options.read(true);
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
        let mut file = options.open(path)?;
        let opened = file.metadata()?;
        anyhow::ensure!(
            opened.is_file()
                && opened.uid() == 0
                && opened.nlink() == 1
                && matches!(opened.mode() & 0o777, 0o600 | 0o444)
                && opened.ino() == metadata.ino()
                && opened.dev() == metadata.dev()
                && opened.len() <= MAX_BYTES,
            "Operator approval changed during open"
        );
        let mut bytes = Vec::new();
        std::io::Read::by_ref(&mut file)
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)?;
        anyhow::ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "Operator approval too large"
        );
        Ok(bytes)
    }
}
fn validate(approval: &Approval, agent_dir: &Path) -> Result<()> {
    anyhow::ensure!(
        approval.version == 1
            && approval.evidence_policy == POLICY
            && safe_id(&approval.grant_id)
            && !approval.reason.trim().is_empty()
            && !approval.acknowledged_residual_risk.trim().is_empty()
            && !approval.bindings.is_empty()
            && approval.bindings.len() <= MAX_ACTIVE
            && !approval.evidence.is_empty(),
        "Invalid operator legacy closure approval"
    );
    let root = Path::new(&approval.evidence_root);
    anyhow::ensure!(
        root.is_absolute() && root.canonicalize()? == root,
        "Canonical evidence allowlist root required"
    );
    let mut seen = std::collections::HashSet::new();
    for evidence in &approval.evidence {
        let path = Path::new(&evidence.path);
        anyhow::ensure!(
            path.starts_with(root) && path.canonicalize()? == path,
            "Evidence outside approved canonical root"
        );
        anyhow::ensure!(seen.insert(path), "Duplicate evidence reference");
        anyhow::ensure!(
            hash(&bounded_read(path)?) == evidence.sha256,
            "Evidence hash mismatch"
        );
    }
    let mut children = std::collections::HashSet::new();
    for binding in &approval.bindings {
        anyhow::ensure!(
            children.insert(&binding.assignment.child_id),
            "Duplicate approved child"
        );
        let marker = &binding.quarantine;
        anyhow::ensure!(
            marker.parent_session_file == approval.parent_session_file
                && marker.child_id == binding.assignment.child_id
                && marker.task == serde_json::to_value(&approval.task)?
                && marker.assignment == serde_json::to_value(&binding.assignment)?,
            "Approval quarantine binding mismatch"
        );
        crate::lease::verified_session_quarantine(
            agent_dir,
            Path::new(&marker.child_session_file),
            marker,
            &binding.quarantine_sha256,
        )?;
    }
    Ok(())
}
impl LegacyClosureReceipt {
    fn matches_current_approval(&self, bytes: &[u8], agent_dir: &Path) -> bool {
        hash(bytes) == self.authorization_digest
            && serde_json::from_slice::<Approval>(bytes).is_ok_and(|approval| {
                approval == self.approval && validate(&approval, agent_dir).is_ok()
            })
    }
    pub(super) fn applies_to_record(
        &self,
        agent_dir: &Path,
        parent: Option<&str>,
        row: &ChildRecord,
    ) -> bool {
        self.version == 1
            && self.kind == "owner_accepted_legacy_closure"
            && !self.process_exit_observed
            && !self.historical_settlement_proven
            && self.approval.evidence_policy == POLICY
            && parent == Some(self.approval.parent_session_file.as_str())
            && safe_id(&self.approval.grant_id)
            && trusted_approval(
                &Path::new(APPROVAL_ROOT).join(format!("{}.json", self.approval.grant_id)),
            )
            .is_ok_and(|bytes| self.matches_current_approval(&bytes, agent_dir))
            && self.approval.bindings.iter().any(|binding| {
                let marker = &binding.quarantine;
                binding.assignment.child_id == row.rlm_child_id
                    && row.session_file.as_deref() == Some(&marker.child_session_file)
                    && row.admission_generation == marker.current_admission_generation
                    && row.expected_message_id == marker.current_expected_message_id
                    && crate::lease::verified_session_quarantine(
                        agent_dir,
                        Path::new(&marker.child_session_file),
                        marker,
                        &binding.quarantine_sha256,
                    )
                    .is_ok()
            })
    }
}
impl Gate {
    pub(super) fn legacy_closure_applies(&self, assignment: &Assignment, rows: &[Value]) -> bool {
        self.legacy_closure_receipts.iter().any(|receipt| {
            self.task.as_ref() == Some(&receipt.approval.task)
                && receipt
                    .approval
                    .bindings
                    .iter()
                    .any(|binding| &binding.assignment == assignment)
                && rows.iter().any(|row| {
                    row["childId"] == assignment.child_id
                        && row["obligation"] == false
                        && row["executionKind"] == "idle"
                        && row["ownerAcceptedLegacyClosures"]
                            .as_array()
                            .is_some_and(|digests| {
                                digests
                                    .iter()
                                    .any(|digest| digest == &receipt.authorization_digest)
                            })
                })
        })
    }
}
impl SupervisorChildSessions {
    pub(crate) async fn accept_legacy_closure(&self, payload: &Value) -> Result<Value> {
        let request: Request = serde_json::from_value(payload.clone())?;
        anyhow::ensure!(safe_id(&request.grant_id), "Invalid operator grant id");
        let path = Path::new(APPROVAL_ROOT).join(format!("{}.json", request.grant_id));
        let bytes = trusted_approval(&path)?;
        let approval: Approval = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            approval.grant_id == request.grant_id,
            "Operator grant selector mismatch"
        );
        self.apply_legacy_approval(approval, hash(&bytes), request.expected_revision)
            .await
    }
    async fn apply_legacy_approval(
        &self,
        approval: Approval,
        digest: String,
        revision: u64,
    ) -> Result<Value> {
        validate(&approval, &self.inner.agent_dir)?;
        let identity = self.inner.identity.lock().expect("identity lock").clone();
        anyhow::ensure!(
            identity.rlm_depth == 0
                && identity.session_file.as_deref() == Some(&approval.parent_session_file),
            "Exact root approval owner required"
        );
        let snapshot = self.inner.children.lock().await.clone();
        let mut selected = Vec::new();
        let mut bindings = approval.bindings.iter().collect::<Vec<_>>();
        bindings.sort_by_key(|b| &b.assignment.child_id);
        for binding in &bindings {
            let mut found = Vec::new();
            for row in &snapshot {
                if row.lock().await.rlm_child_id == binding.assignment.child_id {
                    found.push(row.clone());
                }
            }
            anyhow::ensure!(found.len() == 1, "Unique approved direct child required");
            selected.push(found.pop().expect("one child"));
        }
        let mut rows = Vec::new();
        for row in selected {
            rows.push(row.lock_owned().await);
        }
        let children = self
            .inner
            .children
            .try_lock()
            .map_err(|_| anyhow!("Child registry busy"))?;
        anyhow::ensure!(
            children.len() == snapshot.len()
                && children
                    .iter()
                    .zip(&snapshot)
                    .all(|(a, b)| Arc::ptr_eq(a, b)),
            "Child registry changed"
        );
        for (row, binding) in rows.iter().zip(bindings) {
            let marker = &binding.quarantine;
            anyhow::ensure!(
                row.session_file.as_deref() == Some(&marker.child_session_file)
                    && row.admission_generation == marker.current_admission_generation
                    && row.expected_message_id == marker.current_expected_message_id
                    && row.settled
                    && !row.admission_blocked,
                "Current child state differs from approved quarantine"
            );
        }
        validate(&approval, &self.inner.agent_dir)?;
        let mut gate = self.inner.gate.lock().expect("gate lock");
        let owner = self
            .inner
            .identity
            .try_lock()
            .map_err(|_| anyhow!("Owner identity busy"))?;
        anyhow::ensure!(
            owner.rlm_depth == 0
                && owner.session_file == identity.session_file
                && gate.task.as_ref() == Some(&approval.task),
            "Owner/task changed"
        );
        anyhow::ensure!(
            approval.bindings.len() == gate.assignments.len()
                && approval.bindings.iter().all(|b| gate
                    .assignments
                    .iter()
                    .filter(|a| *a == &b.assignment)
                    .count()
                    == 1),
            "Exact configured legacy assignments required"
        );
        if let Some(existing) = gate
            .legacy_closure_receipts
            .iter()
            .find(|r| r.approval.grant_id == approval.grant_id)
        {
            anyhow::ensure!(
                existing.authorization_digest == digest && existing.approval == approval,
                "Operator grant changed after acceptance"
            );
            anyhow::ensure!(
                revision == gate.revision || revision + 1 == gate.revision,
                "Orchestration revision conflict"
            );
            return Ok(json!({"revision":gate.revision,"legacyClosureReceipt":existing}));
        }
        anyhow::ensure!(gate.revision == revision, "Orchestration revision conflict");
        let receipt = LegacyClosureReceipt {
            version: 1,
            kind: "owner_accepted_legacy_closure".into(),
            approval,
            authorization_digest: digest,
            process_exit_observed: false,
            historical_settlement_proven: false,
        };
        let mut next = gate.clone();
        next.legacy_closure_receipts.push(receipt.clone());
        next.revision += 1;
        // Already hold owner identity: persist exact owner path, avoiding gate->identity relock.
        let path = PathBuf::from(format!(
            "{}.orchestration-gate.json",
            owner.session_file.as_deref().expect("verified owner")
        ));
        let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        pa_core::platform::perms::set_private_mode(&mut options);
        let mut file = options.open(&temp)?;
        file.write_all(&serde_json::to_vec(&next)?)?;
        file.sync_all()?;
        pa_core::platform::rename_onto(&temp, &path)?;
        *gate = next;
        Ok(json!({"revision":gate.revision,"legacyClosureReceipt":receipt}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rlm_children::ParentIdentity;
    fn fixture() -> (tempfile::TempDir, Approval) {
        let dir = tempfile::TempDir::new().unwrap();
        let parent = dir.path().join("parent.jsonl");
        let child = dir.path().join("child.jsonl");
        fs::write(&parent, "parent").unwrap();
        fs::write(&child, "child").unwrap();
        let evidence = dir.path().join("exit-cleanup.md");
        fs::write(&evidence, "verified finite exit and cleanup").unwrap();
        let task:Task=serde_json::from_value(json!({"projectId":"p","planId":"plan","stepId":"step","taskGeneration":1,"policyRevision":1})).unwrap();
        let assignment:Assignment=serde_json::from_value(json!({"childId":"child","admissionGeneration":"old-g","incarnation":"old-worker","assignmentId":"a","taskGeneration":1,"ownership":"lane","acceptanceRef":"accept","artifactRef":"artifact","dependenciesSatisfied":true})).unwrap();
        let marker = crate::lease::SessionQuarantine {
            version: 1,
            parent_session_file: parent.to_string_lossy().into(),
            child_id: "child".into(),
            child_session_file: child.to_string_lossy().into(),
            task: serde_json::to_value(&task).unwrap(),
            assignment: serde_json::to_value(&assignment).unwrap(),
            current_admission_generation: Some("new-g".into()),
            current_expected_message_id: Some("new-m".into()),
        };
        crate::lease::persist_session_quarantine(dir.path(), &marker).unwrap();
        let key = hash(child.to_string_lossy().as_bytes());
        let marker_bytes = fs::read(
            dir.path()
                .join("session-leases")
                .join(format!("{key}.quarantine.json")),
        )
        .unwrap();
        let approval = Approval {
            version: 1,
            grant_id: "approved".into(),
            parent_session_file: parent.to_string_lossy().into(),
            task,
            evidence_policy: POLICY.into(),
            reason: "accept finite exit/cleanup with sustained fence".into(),
            acknowledged_residual_risk: "membership incomplete; no observed survivor".into(),
            evidence_root: dir.path().to_string_lossy().into(),
            evidence: vec![Evidence {
                path: evidence.to_string_lossy().into(),
                sha256: hash(&fs::read(evidence).unwrap()),
            }],
            bindings: vec![Binding {
                assignment,
                quarantine: marker,
                quarantine_sha256: hash(&marker_bytes),
            }],
        };
        (dir, approval)
    }
    #[test]
    fn legacy_closure_denies_self_issued_grants_and_invalid_evidence_or_marker() {
        let (dir, approval) = fixture();
        validate(&approval, dir.path()).unwrap();
        let forged = dir.path().join("approved.json");
        fs::write(&forged, serde_json::to_vec(&approval).unwrap()).unwrap();
        assert!(
            trusted_approval(&forged).is_err(),
            "writable tmp ancestor cannot issue approval"
        );
        let mut changed = approval.clone();
        changed.evidence[0].sha256 = "wrong".into();
        assert!(validate(&changed, dir.path()).is_err());
        changed = approval.clone();
        changed.bindings[0].quarantine_sha256 = "wrong".into();
        assert!(validate(&changed, dir.path()).is_err());
        changed = approval;
        changed.bindings[0].assignment.ownership = "foreign".into();
        assert!(validate(&changed, dir.path()).is_err());
        assert!(!safe_id("../forged"));
        assert!(!safe_id("/etc/passwd"));
    }
    #[test]
    fn legacy_closure_projection_revalidates_actual_approval_and_evidence() {
        let (dir, approval) = fixture();
        let bytes = serde_json::to_vec(&approval).unwrap();
        let receipt = LegacyClosureReceipt {
            version: 1,
            kind: "owner_accepted_legacy_closure".into(),
            approval: approval.clone(),
            authorization_digest: hash(&bytes),
            process_exit_observed: false,
            historical_settlement_proven: false,
        };
        assert!(receipt.matches_current_approval(&bytes, dir.path()));
        let mut forged = receipt.clone();
        forged.approval.task.task_generation += 1;
        assert!(!forged.matches_current_approval(&bytes, dir.path()));
        forged = receipt.clone();
        forged.approval.bindings[0].assignment.ownership = "foreign".into();
        assert!(!forged.matches_current_approval(&bytes, dir.path()));
        assert!(!receipt.matches_current_approval(b"corrupt", dir.path()));
        fs::write(&approval.evidence[0].path, "changed evidence").unwrap();
        assert!(!receipt.matches_current_approval(&bytes, dir.path()));
        fs::remove_file(&approval.evidence[0].path).unwrap();
        assert!(!receipt.matches_current_approval(&bytes, dir.path()));
    }

    #[tokio::test]
    async fn legacy_closure_exact_binding_policy_preserves_new_obligations_and_state() {
        let (dir, approval) = fixture();
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
            session_file: Some(approval.parent_session_file.clone()),
            ..ParentIdentity::with_default_depth()
        });
        sessions
            .push_test_child(super::super::super::RlmChildIdentity {
                rlm_child_id: "child".into(),
                active_session_id: "new".into(),
                session_id: Some("child".into()),
                session_name: "child".into(),
            })
            .await;
        let record = sessions.inner.children.lock().await[0].clone();
        {
            let mut row = record.lock().await;
            row.session_file = Some(approval.bindings[0].quarantine.child_session_file.clone());
            row.admission_generation = Some("new-g".into());
            row.expected_message_id = Some("new-m".into());
            row.settled = true;
            row.admission_blocked = false;
        }
        {
            let mut gate = sessions.inner.gate.lock().unwrap();
            gate.task = Some(approval.task.clone());
            gate.assignments = vec![approval.bindings[0].assignment.clone()];
            gate.attempts = 2;
            gate.suspended = true;
        }
        let before = serde_json::to_value(&*sessions.inner.gate.lock().unwrap()).unwrap();
        assert!(sessions
            .apply_legacy_approval(approval.clone(), "digest".into(), 9)
            .await
            .is_err());
        let receipt = sessions
            .apply_legacy_approval(approval.clone(), "digest".into(), 0)
            .await
            .unwrap();
        assert_eq!(
            receipt["legacyClosureReceipt"]["processExitObserved"],
            false
        );
        assert_eq!(
            receipt["legacyClosureReceipt"]["historicalSettlementProven"],
            false
        );
        assert_eq!(
            sessions
                .apply_legacy_approval(approval.clone(), "digest".into(), 0)
                .await
                .unwrap(),
            receipt
        );
        assert!(sessions
            .apply_legacy_approval(approval.clone(), "changed".into(), 1)
            .await
            .is_err());
        let mut gate = sessions.inner.gate.lock().unwrap().clone();
        let assignment = approval.bindings[0].assignment.clone();
        let idle = json!({"childId":"child","admissionGeneration":"new-g","incarnation":null,"executionKind":"idle","obligation":false,"ownerAcceptedLegacyClosures":["digest"]});
        assert!(gate.legacy_closure_applies(&assignment, std::slice::from_ref(&idle)));
        let mut active = idle.clone();
        active["obligation"] = json!(true);
        assert!(!gate.legacy_closure_applies(&assignment, &[active]));
        gate.task.as_mut().unwrap().task_generation += 1;
        assert!(!gate.legacy_closure_applies(&assignment, &[idle]));
        let after = serde_json::to_value(&*sessions.inner.gate.lock().unwrap()).unwrap();
        for key in [
            "task",
            "policy",
            "assignments",
            "correctiveAttempts",
            "attempts",
            "suspended",
            "consumedManagedResults",
        ] {
            assert_eq!(before[key], after[key], "{key}");
        }
        let restored: Gate = serde_json::from_slice(
            &fs::read(format!(
                "{}.orchestration-gate.json",
                approval.parent_session_file
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(restored.legacy_closure_receipts.len(), 1);
    }
}
