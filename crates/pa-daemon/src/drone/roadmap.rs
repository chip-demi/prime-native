//! Explicit owner-bound roadmap evidence, never guessed from cwd.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::Path;
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Plan {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,
    pub revision: String,
    pub goal: String,
    pub acceptance: Vec<String>,
    pub capacity: usize,
    pub user_stopped: bool,
    pub budget_available: bool,
    pub steps: Vec<Step>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Step {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_reasons: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_ref: Option<String>,
    pub status: String,
    pub dependencies: Vec<String>,
    pub owner: Option<String>,
    pub files: Vec<String>,
    pub acceptance: Vec<String>,
}
pub(crate) fn read_bound(path: Option<&str>, cwd: &std::path::Path) -> anyhow::Result<Value> {
    let path = path.ok_or_else(|| anyhow::anyhow!("capability_missing:roadmap_binding"))?;
    let path_buf = Path::new(path);
    anyhow::ensure!(
        path_buf.is_absolute()
            && path_buf.canonicalize()? == path_buf
            && path_buf.starts_with(cwd.canonicalize()?),
        "roadmap_path_scope"
    );
    let result = read(Some(path))?;
    anyhow::ensure!(
        path_buf.canonicalize()? == path_buf && path_buf.starts_with(cwd.canonicalize()?),
        "roadmap_path_scope"
    );
    Ok(result)
}
pub(crate) fn read(path: Option<&str>) -> anyhow::Result<Value> {
    const READ_CAP: usize = 1_048_576;
    const EVIDENCE_CHARS: usize = 6_144;
    use std::io::Read;
    let path = path.ok_or_else(|| anyhow::anyhow!("capability_missing:roadmap_binding"))?;
    let path = Path::new(path);
    anyhow::ensure!(path.is_absolute(), "roadmap_binding_must_be_absolute");
    let metadata = std::fs::metadata(path)?;
    anyhow::ensure!(
        metadata.is_file() && metadata.len() <= READ_CAP as u64,
        "roadmap_input_cap"
    );
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((READ_CAP + 1) as u64)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= READ_CAP, "roadmap_input_cap");
    let full = String::from_utf8(bytes.clone())?;
    let hash = format!("{:x}", Sha256::digest(&bytes));
    let char_count = full.chars().count();
    let json_looking = full.trim_start().starts_with('{');
    let truncated = char_count > EVIDENCE_CHARS;
    let content = if truncated {
        let head = full.chars().take(1_024).collect::<String>();
        let tail = full
            .chars()
            .skip(char_count - (EVIDENCE_CHARS - 1_024))
            .collect::<String>();
        format!("{head}\n[Middle omitted by Native; full-file SHA256 and byte count above. Do not treat omitted steps as absent.]\n{tail}")
    } else {
        full
    };
    // The excerpt is for the model only. Parsing it must never hide authoritative
    // steps near the middle or end of a complete, bounded projection.
    let structured = match serde_json::from_slice::<Plan>(&bytes) {
        Ok(plan) => Some(plan),
        Err(_) if json_looking => anyhow::bail!("roadmap_schema_invalid"),
        Err(_) => None,
    };
    if let Some(plan) = &structured {
        anyhow::ensure!(
            plan.schema_version.is_none_or(|version| version == 2),
            "roadmap_schema_version"
        );
        anyhow::ensure!(plan.steps.len() <= 2_000, "roadmap_step_cap");
        let mut ids = std::collections::HashSet::new();
        let mut work_refs = std::collections::HashSet::new();
        for step in &plan.steps {
            anyhow::ensure!(
                !step.id.is_empty() && ids.insert(&step.id),
                "roadmap_duplicate_step"
            );
            if let (Some(plan_id), Some(step_id)) = (&step.plan_id, &step.step_id) {
                anyhow::ensure!(
                    !plan_id.is_empty()
                        && !step_id.is_empty()
                        && work_refs.insert((plan_id, step_id)),
                    "roadmap_duplicate_work_ref"
                );
            }
        }
    }
    Ok(
        serde_json::json!({"path":path,"sha256":hash,"revision":structured.as_ref().map_or(hash.as_str(), |p|p.revision.as_str()),"byteLength":bytes.len(),"truncated":truncated,"selection":if truncated{"first1024+last5120characters"}else{"complete"},"content":content,"structured":structured}),
    )
}
pub(crate) fn ready_parallel(plan: &Plan) -> Vec<&Step> {
    if plan.user_stopped || !plan.budget_available {
        return vec![];
    }
    let mut ready = Vec::<&Step>::new();
    for step in &plan.steps {
        if step.status != "ready"
            || step.owner.is_some()
            || step.acceptance.is_empty()
            || !step.dependencies.iter().all(|dep| {
                plan.steps
                    .iter()
                    .any(|s| &s.id == dep && s.status == "verified")
            })
        {
            continue;
        }
        if plan
            .steps
            .iter()
            .filter(|s| s.status == "in_progress")
            .any(|other| step.files.iter().any(|f| other.files.contains(f)))
        {
            continue;
        }
        ready.push(step);
    }
    ready
}

fn preview_text(value: Option<&str>, max_chars: usize) -> (Option<String>, usize) {
    let Some(value) = value else { return (None, 0) };
    let full_len = value.chars().count();
    (
        Some(value.chars().take(max_chars).collect()),
        full_len.saturating_sub(max_chars),
    )
}

fn preview_list(
    values: &[String],
    max_items: usize,
    max_chars: usize,
) -> (Vec<String>, usize, usize) {
    let previews = values
        .iter()
        .take(max_items)
        .map(|value| value.chars().take(max_chars).collect())
        .collect::<Vec<_>>();
    let shortened = values
        .iter()
        .take(max_items)
        .filter(|value| value.chars().count() > max_chars)
        .count();
    (previews, values.len().saturating_sub(max_items), shortened)
}

fn model_step(step: &Step) -> anyhow::Result<Value> {
    let (text, text_omitted_chars) = preview_text(step.text.as_deref(), 300);
    let (note, note_omitted_chars) = preview_text(step.note.as_deref(), 80);
    let (phase, phase_omitted_chars) = preview_text(step.phase.as_deref(), 64);
    let (artifact_ref, artifact_ref_omitted_chars) = preview_text(step.artifact_ref.as_deref(), 64);
    let (dependencies, dependencies_omitted, dependencies_shortened) =
        preview_list(&step.dependencies, 3, 32);
    let (ownership, ownership_omitted, ownership_shortened) = preview_list(&step.files, 3, 32);
    let (acceptance, acceptance_omitted, acceptance_shortened) =
        preview_list(&step.acceptance, 1, 60);
    let reasons = step.readiness_reasons.as_deref().unwrap_or(&[]);
    let (readiness_reasons, reasons_omitted, reasons_shortened) = preview_list(reasons, 3, 32);
    let evidence = serde_json::json!({
        "id":step.id,"planId":step.plan_id,"stepId":step.step_id,"status":step.status,
        "text":text,"textOmittedChars":text_omitted_chars,
        "note":note,"noteOmittedChars":note_omitted_chars,
        "phase":phase,"phaseOmittedChars":phase_omitted_chars,
        "artifactRef":artifact_ref,"artifactRefOmittedChars":artifact_ref_omitted_chars,
        "dependencies":dependencies,"dependenciesOmitted":dependencies_omitted,
        "dependenciesShortened":dependencies_shortened,
        "files":ownership,"ownershipOmitted":ownership_omitted,"ownershipShortened":ownership_shortened,
        "acceptance":acceptance,"acceptanceOmitted":acceptance_omitted,"acceptanceShortened":acceptance_shortened,
        "readinessReasons":readiness_reasons,"reasonsOmitted":reasons_omitted,
        "reasonsShortened":reasons_shortened
    });
    anyhow::ensure!(
        serde_json::to_vec(&evidence)?.len() <= 2048,
        "roadmap_step_evidence_cap"
    );
    Ok(evidence)
}

/// Bounded model view; the full parsed plan remains the admission authority.
pub(crate) fn model_evidence(reading: &Value, cursor: u64) -> anyhow::Result<Value> {
    const MODEL_CAP: usize = 14_336;
    let Some(plan) = reading
        .get("structured")
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value::<Plan>(value.clone()))
        .transpose()?
    else {
        // Legacy non-JSON Roadmaps already carry an explicitly marked excerpt.
        return Ok(reading.clone());
    };
    let ready = ready_parallel(&plan);
    let planning = plan
        .steps
        .iter()
        .filter(|step| !matches!(step.status.as_str(), "ready" | "verified" | "in_progress"))
        .collect::<Vec<_>>();
    let mut status_counts = std::collections::BTreeMap::<&str, usize>::new();
    for step in &plan.steps {
        *status_counts.entry(&step.status).or_default() += 1;
    }
    let ready_offset = if ready.is_empty() {
        0
    } else {
        cursor as usize % ready.len()
    };
    let planning_offset = if planning.is_empty() {
        0
    } else {
        cursor as usize % planning.len()
    };
    let mut evidence = serde_json::json!({
        "path":reading["path"],"sha256":reading["sha256"],"revision":plan.revision,
        "byteLength":reading["byteLength"],"schemaVersion":plan.schema_version,
        "goal":plan.goal,"acceptance":plan.acceptance,
        "capacity":if plan.capacity == 0 {Value::Null} else {serde_json::json!(plan.capacity)},
        "capacitySource":if plan.capacity == 0 {"runtime_gate"} else {"roadmap_cap"},
        "userStopped":plan.user_stopped,"budgetAvailable":plan.budget_available,
        "totalSteps":plan.steps.len(),"statusCounts":status_counts,
        "readyCount":ready.len(),"planningCount":planning.len(),
        "readyWindow":[],"planningWindow":[],
        "selection":{"kind":"rotating_ready_first","cursor":cursor,
            "readyOffset":ready_offset,"planningOffset":planning_offset,
            "readyOmitted":ready.len(),"planningOmitted":planning.len(),
            "fullFileRetainedForAdmission":true}
    });
    anyhow::ensure!(
        serde_json::to_vec(&evidence)?.len() <= MODEL_CAP,
        "roadmap_model_evidence_cap"
    );
    let ready_limit = if planning.is_empty() {
        MODEL_CAP
    } else {
        MODEL_CAP / 2
    };
    for step in ready.iter().cycle().skip(ready_offset).take(ready.len()) {
        evidence["readyWindow"]
            .as_array_mut()
            .unwrap()
            .push(model_step(step)?);
        if serde_json::to_vec(&evidence)?.len() > ready_limit {
            evidence["readyWindow"].as_array_mut().unwrap().pop();
        }
    }
    let ready_omitted = ready.len() - evidence["readyWindow"].as_array().unwrap().len();
    evidence["selection"]["readyOmitted"] = serde_json::json!(ready_omitted);
    for step in planning
        .iter()
        .cycle()
        .skip(planning_offset)
        .take(planning.len())
    {
        evidence["planningWindow"]
            .as_array_mut()
            .unwrap()
            .push(model_step(step)?);
        if serde_json::to_vec(&evidence)?.len() > MODEL_CAP {
            evidence["planningWindow"].as_array_mut().unwrap().pop();
        }
    }
    let planning_omitted = planning.len() - evidence["planningWindow"].as_array().unwrap().len();
    evidence["selection"]["planningOmitted"] = serde_json::json!(planning_omitted);
    Ok(evidence)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn plan() -> Plan {
        serde_json::from_value(serde_json::json!({"revision":"r1","goal":"deliver","acceptance":["actualresult"],"capacity":2,"userStopped":false,"budgetAvailable":true,"steps":[{"id":"a","status":"ready","dependencies":[],"owner":null,"files":["a.rs"],"acceptance":["testa"]},{"id":"b","status":"ready","dependencies":[],"owner":null,"files":["b.rs"],"acceptance":["testb"]}]})).unwrap()
    }
    #[test]
    fn roadmap_ready_index_keeps_all_candidates_and_respects_dependencies_stop() {
        let mut p = plan();
        assert_eq!(ready_parallel(&p).len(), 2);
        p.steps[1].dependencies = vec!["a".into()];
        assert_eq!(ready_parallel(&p).len(), 1);
        p.steps[1].dependencies.clear();
        p.steps[1].files = vec!["a.rs".into()];
        // Conflicting ready candidates remain visible; selection checks the
        // overlap against the live owner gate before recommending a set.
        assert_eq!(ready_parallel(&p).len(), 2);
        p.user_stopped = true;
        assert!(ready_parallel(&p).is_empty());
        p.user_stopped = false;
        p.budget_available = false;
        assert!(ready_parallel(&p).is_empty());
    }
    #[test]
    fn large_authoritative_roadmap_hashes_all_bytes_and_bounds_selected_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ROADMAP.md");
        let mut bytes = "CURRENT HEADER\n".to_string();
        bytes.push_str(&"старые шаги ".repeat(20_000));
        bytes.push_str("\nLATEST OWNER CORRECTION");
        std::fs::write(&path, &bytes).unwrap();
        let snapshot = read(path.to_str()).unwrap();
        assert!(snapshot["truncated"].as_bool().unwrap());
        assert_eq!(
            snapshot["sha256"],
            format!("{:x}", Sha256::digest(bytes.as_bytes()))
        );
        assert_eq!(snapshot["byteLength"], bytes.len());
        assert!(snapshot["content"]
            .as_str()
            .unwrap()
            .starts_with("CURRENT HEADER"));
        assert!(snapshot["content"]
            .as_str()
            .unwrap()
            .ends_with("LATEST OWNER CORRECTION"));
        assert!(serde_json::to_vec(&snapshot).unwrap().len() < 32768);
        std::fs::write(&path, vec![b'x'; 1_048_577]).unwrap();
        assert!(read(path.to_str()).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn bound_read_rejects_replaced_file_outside_authorized_cwd() {
        let cwd = tempfile::tempdir().unwrap();
        let foreign = tempfile::tempdir().unwrap();
        let path = cwd.path().join("ROADMAP.md");
        let sentinel = foreign.path().join("sentinel");
        std::fs::write(&path, "approved").unwrap();
        std::fs::write(&sentinel, "foreign secret sentinel").unwrap();
        assert!(read_bound(path.to_str(), cwd.path()).is_ok());
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&sentinel, &path).unwrap();
        assert!(read_bound(path.to_str(), cwd.path()).is_err());
    }
    #[test]
    fn roadmap_reread_changes_hash_and_rejects_missing_binding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("approved-plan.json");
        std::fs::write(&path, serde_json::to_vec(&plan()).unwrap()).unwrap();
        let before = read(path.to_str()).unwrap();
        std::fs::write(&path, b"changed owner roadmap").unwrap();
        let after = read(path.to_str()).unwrap();
        assert_ne!(before["sha256"], after["sha256"]);
        assert!(read(None).is_err());
    }
    #[test]
    fn complete_v2_projection_survives_excerpt_and_pages_ready_and_unassessed_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("projected.json");
        let steps = (0..240).map(|index| serde_json::json!({
            "id":format!("task-{index}"),"planId":"p","stepId":format!("s-{index}"),
            "status":if index % 2 == 0 {"ready"} else {"unassessed"},
            "dependencies":[],"owner":null,"files":[format!("scope-{index}")],
            "acceptance":["verified result"],"text":format!("Задача {index}: {}", "смысл ".repeat(40)),
            "readinessReasons":if index % 2 == 0 {vec![]} else {vec!["not_assessed"]}
        })).collect::<Vec<_>>();
        let projection = serde_json::json!({"schemaVersion":2,"revision":"r2","goal":"Сохранить требования владельца",
            "acceptance":["verified outcome"],"capacity":0,"userStopped":false,"budgetAvailable":true,"steps":steps});
        std::fs::write(&path, serde_json::to_vec(&projection).unwrap()).unwrap();
        let full = read(path.to_str()).unwrap();
        assert_eq!(full["structured"]["steps"].as_array().unwrap().len(), 240);
        assert!(full["truncated"].as_bool().unwrap());
        assert_eq!(
            ready_parallel(&serde_json::from_value::<Plan>(full["structured"].clone()).unwrap())
                .len(),
            120
        );
        let first = model_evidence(&full, 0).unwrap();
        let later = model_evidence(&full, 100).unwrap();
        for page in [&first, &later] {
            assert!(serde_json::to_vec(page).unwrap().len() <= 14_336);
            assert_eq!(page["readyCount"], 120);
            assert_eq!(page["planningCount"], 120);
            assert!(page["selection"]["readyOmitted"].as_u64().unwrap() > 0);
            assert!(page["selection"]["planningOmitted"].as_u64().unwrap() > 0);
            assert!(!page["readyWindow"].as_array().unwrap().is_empty());
            assert!(!page["planningWindow"].as_array().unwrap().is_empty());
            assert!(page.get("structured").is_none());
            assert!(page.get("content").is_none());
        }
        assert_ne!(first["readyWindow"][0]["id"], later["readyWindow"][0]["id"]);
        assert_ne!(
            first["planningWindow"][0]["id"],
            later["planningWindow"][0]["id"]
        );
    }
    #[test]
    fn projection_over_leaf_limit_fails_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("too-many.json");
        let mut plan = plan();
        plan.schema_version = Some(2);
        plan.steps = vec![plan.steps[0].clone(); 2_001];
        std::fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
        assert!(read(path.to_str()).is_err());
    }
    #[test]
    fn malformed_json_projection_does_not_become_unstructured_model_advice() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("malformed.json");
        std::fs::write(
            &path,
            br#"{"schemaVersion":2,"revision":"studio:9","steps":[]} "#,
        )
        .unwrap();
        assert!(read(path.to_str()).is_err());
    }
    #[test]
    fn oversized_cyrillic_note_and_ownership_still_leave_an_identifiable_ready_step() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("long-task.json");
        let projection = serde_json::json!({"schemaVersion":2,"revision":"studio:12","goal":"План",
            "acceptance":["checked"],"capacity":0,"userStopped":false,"budgetAvailable":true,
            "steps":[{"id":"p/target","planId":"p","stepId":"target","status":"ready",
                "text":"Описание ".repeat(1000),"note":"Пояснение ".repeat(2000),
                "readinessReasons":[],"dependencies":[],"owner":null,
                "files":(0..100).map(|_| "область ".repeat(50)).collect::<Vec<_>>(),
                "acceptance":["Проверка ".repeat(200)]}]});
        std::fs::write(&path, serde_json::to_vec(&projection).unwrap()).unwrap();
        let full = read(path.to_str()).unwrap();
        let page = model_evidence(&full, 0).unwrap();
        assert_eq!(page["readyWindow"][0]["id"], "p/target");
        assert!(page["readyWindow"][0]["textOmittedChars"].as_u64().unwrap() > 0);
        assert!(page["readyWindow"][0]["noteOmittedChars"].as_u64().unwrap() > 0);
        assert!(page["readyWindow"][0]["ownershipOmitted"].as_u64().unwrap() > 0);
        assert!(serde_json::to_vec(&page).unwrap().len() <= 14_336);
        assert_eq!(
            full["structured"]["steps"][0]["files"]
                .as_array()
                .unwrap()
                .len(),
            100
        );
    }
}
