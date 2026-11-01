//! Advisory parallel work is admitted only against the owner's live gate.
use super::roadmap::{ready_parallel, Plan, Step};
use anyhow::{anyhow, ensure, Result};
use serde_json::{json, Value};
use std::collections::HashSet;

/// Advice eligibility is narrower than real root authority. Human opt-in here
/// only clears an advisory restriction; a current complete gate is still required.
pub(super) fn advisory_policy(snapshot: &Value, gate: Option<&Value>) -> Value {
    let reason = if snapshot["subagentPolicy"]["taskAllowsSubagents"] == false {
        Some("human_task_forbids_subagents")
    } else if snapshot["subagentPolicy"]["maxDepth"] == 0 {
        Some("subagent_depth_zero")
    } else if let Some(gate) = gate {
        if gate["policy"]["enabled"] != true {
            Some("parallel_policy_disabled")
        } else if gate["policy"]["maxActive"] == 0 {
            Some("parallel_capacity_zero")
        } else if gate["complete"] != true || gate["nextWork"]["runtime"]["complete"] != true {
            Some("parallel_runtime_incomplete")
        } else {
            match gate["nextWork"]["runtime"]["freeSlots"].as_u64() {
                Some(0) => Some("parallel_capacity_zero"),
                Some(_) => None,
                None => Some("parallel_capacity_unknown"),
            }
        }
    } else {
        Some("parallel_runtime_unknown")
    };
    json!({"parallelAllowed":reason.is_none(),"reason":reason})
}

pub(super) fn mentions_parallel_work(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("parallel work")
        || lower.contains("parallelize")
        || lower.contains("параллельн")
        || ([
            "subagent",
            "sub-agent",
            "sub agent",
            "child",
            "worker",
            "субагент",
            "дочерн",
        ]
        .iter()
        .any(|word| lower.contains(word))
            && [
                "spawn ",
                "delegate ",
                "dispatch ",
                "launch ",
                "start ",
                "create ",
                "add ",
                "refill ",
                "recruit ",
                "use ",
                "запуст",
                "запус",
                "созда",
                "добав",
                "делегир",
                "привлеч",
            ]
            .iter()
            .any(|word| lower.contains(word)))
}

pub(crate) fn candidates<'a>(plan: &'a Plan, gate: &Value, active: &str) -> Vec<&'a Step> {
    let studio_revision = plan
        .revision
        .strip_prefix("studio:")
        .and_then(|part| part.parse::<u64>().ok());
    if plan.schema_version != Some(2)
        || studio_revision.is_none()
        || studio_revision != gate["nextWork"]["revision"].as_u64()
        || gate["activeSessionId"] != active
        || gate["complete"] != true
        || gate["unknown"] != 0
        || gate["policy"]["enabled"] != true
        || gate["policy"]["maxActive"] == 0
        || !matches!(gate["decision"].as_str(), Some("pass" | "corrective"))
        || gate["revision"].as_u64().is_none()
        || gate["accountingEpoch"].as_u64().is_none()
        || gate["nextWorkSourceInstanceId"]
            .as_str()
            .is_none_or(str::is_empty)
        || gate["nextWork"]["complete"] != true
        || gate["nextWork"]["revision"].as_u64().is_none()
        || gate["nextWork"]["runtime"]["complete"] != true
        || !gate["nextWork"]["runtime"]["awaitingAcceptance"]
            .as_array()
            .is_some_and(Vec::is_empty)
        || gate["nextWork"]["runtime"]["freeSlots"]
            .as_u64()
            .is_none_or(|slots| slots == 0)
    {
        return Vec::new();
    }
    let Some(ready) = gate["nextWork"]["runtime"]["ready"].as_array() else {
        return Vec::new();
    };
    ready_parallel(plan)
        .into_iter()
        .filter(|step| {
            if step.files.is_empty() {
                return false;
            }
            let (Some(plan_id), Some(step_id)) = (&step.plan_id, &step.step_id) else {
                return false;
            };
            ready.iter().any(|item| {
                item["planId"].as_str() == Some(plan_id.as_str())
                    && item["stepId"].as_str() == Some(step_id.as_str())
                    && item["status"] == "ready"
                    && item["ownership"].as_array().is_some_and(|labels| {
                        let owner_labels = labels
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<HashSet<_>>();
                        let step_labels = step
                            .files
                            .iter()
                            .map(String::as_str)
                            .collect::<HashSet<_>>();
                        labels.len() == owner_labels.len()
                            && step.files.len() == step_labels.len()
                            && owner_labels == step_labels
                    })
            })
        })
        .collect()
}

pub(crate) fn roster_available(roster: &Value) -> bool {
    roster["eventSequence"].as_u64().is_some() && roster["children"].as_array().is_some()
}

pub(crate) fn same_revision(previous: &Value, current: &Value) -> bool {
    current["activeSessionId"] == previous["activeSessionId"]
        && current["revision"] == previous["revision"]
        && current["accountingEpoch"] == previous["accountingEpoch"]
        && current["nextWorkSourceInstanceId"] == previous["nextWorkSourceInstanceId"]
        && current["nextWork"]["revision"] == previous["nextWork"]["revision"]
}

pub(crate) fn validate(
    plan: &Plan,
    gate: &Value,
    active: &str,
    step_ids: &[String],
) -> Result<Vec<Step>> {
    let available = gate["nextWork"]["runtime"]["freeSlots"]
        .as_u64()
        .ok_or_else(|| anyhow!("parallel_runtime_unknown"))? as usize;
    let limit = if plan.capacity == 0 {
        available
    } else {
        available.min(plan.capacity)
    };
    ensure!(
        !step_ids.is_empty() && step_ids.len() <= limit,
        "parallel_capacity_unavailable"
    );
    let unique = step_ids.iter().collect::<HashSet<_>>();
    ensure!(unique.len() == step_ids.len(), "parallel_duplicate_step");
    let candidates = candidates(plan, gate, active);
    let mut selected = Vec::<Step>::new();
    for id in step_ids {
        let step = candidates
            .iter()
            .find(|step| &step.id == id)
            .ok_or_else(|| anyhow!("parallel_proposal_not_ready"))?;
        ensure!(
            !selected
                .iter()
                .any(|prior| step.files.iter().any(|label| prior.files.contains(label))),
            "parallel_ownership_conflict"
        );
        selected.push((*step).clone());
    }
    Ok(selected)
}

pub(crate) fn runtime_evidence(gate: Option<&Value>, children: Option<&Value>) -> Value {
    let mut terminal_results = super::evidence::terminal_children(children);
    while serde_json::to_vec(&terminal_results).is_ok_and(|bytes| bytes.len() > 2048) {
        let remaining = terminal_results["selected"]
            .as_array_mut()
            .and_then(|selected| selected.pop().map(|_| selected.len()));
        let Some(remaining) = remaining else { break };
        terminal_results["omitted"] = json!(terminal_results["count"]
            .as_u64()
            .unwrap_or(0)
            .saturating_sub(remaining as u64));
    }
    let child_rows = children
        .filter(|value| roster_available(value))
        .and_then(|value| value["children"].as_array());
    let mut child_roster = if let Some(rows) = child_rows {
        json!({"state":"available","eventSequence":children.unwrap()["eventSequence"],
            "count":rows.len(),"selected":rows.iter().take(24).map(|row|json!({
                "id":row["id"],"status":row["status"],"executionStatus":row["executionStatus"],"runtimeStatus":if row["observation"]["available"] == true {row["observation"]["status"].clone()} else {json!("unknown")}
            })).collect::<Vec<_>>(),"omitted":rows.len().saturating_sub(24)})
    } else {
        json!({"state":"unavailable"})
    };
    while serde_json::to_vec(&child_roster).is_ok_and(|bytes| bytes.len() > 1024) {
        let remaining = child_roster["selected"]
            .as_array_mut()
            .and_then(|selected| selected.pop().map(|_| selected.len()));
        let Some(remaining) = remaining else { break };
        child_roster["omitted"] = json!(child_rows.map_or(0, Vec::len) - remaining);
    }
    child_roster["runtime"] = if let Some(rows) = child_rows {
        let unknown = rows
            .iter()
            .filter(|row| row["observation"]["available"] != true)
            .count();
        let busy = rows
            .iter()
            .filter(|row| {
                row["observation"]["available"] == true
                    && matches!(
                        row["observation"]["status"].as_str(),
                        Some("running" | "queued")
                    )
            })
            .count();
        json!({"state":if unknown == 0 {"complete"} else {"unknown"},"knownBusy":busy,"unknown":unknown})
    } else {
        json!({"state":"unknown","knownBusy":null,"unknown":null})
    };
    let Some(gate) = gate else {
        return json!({"state":"unavailable","capacity":"unknown","ownership":"unknown",
            "scopeEvidence":"declared_ownership_labels_only","requiresRootScopeReview":true,
            "childRoster":child_roster,"terminalChildResults":terminal_results});
    };
    let next = &gate["nextWork"];
    let runtime = &next["runtime"];
    let ready = runtime["ready"].as_array();
    let owners = runtime["ownership"].as_array();
    let awaiting = runtime["awaitingAcceptance"].as_array();
    let complete = gate["complete"] == true && runtime["complete"] == true;
    let mut evidence = json!({
        "state":if complete {"complete"} else {"incomplete"},
        "activeSessionId":gate["activeSessionId"],"revision":gate["revision"],
        "accountingEpoch":gate["accountingEpoch"],"decision":gate["decision"],
        "knownActive":gate["knownActive"],"unknown":gate["unknown"],
        "scopeEvidence":"declared_ownership_labels_only","requiresRootScopeReview":true,
        "policy":{"enabled":gate["policy"]["enabled"],"minActive":gate["policy"]["minActive"],
            "maxActive":gate["policy"]["maxActive"]},
        "nextWorkSourceInstanceId":gate["nextWorkSourceInstanceId"],
        "nextWorkRevision":next["revision"],"nextWorkComplete":next["complete"],
        "freeSlots":if complete {runtime["freeSlots"].clone()} else {Value::Null},
        "readyCount":ready.map(Vec::len),"reservedCount":owners.map(Vec::len),
        "awaitingAcceptanceCount":awaiting.map(Vec::len),
        "selectedOwnership":owners.map(|rows|rows.iter().take(24).map(|row|json!({
            "childId":row["childId"],"planId":row["planId"],"stepId":row["stepId"],
            "ownership":row["ownership"]
        })).collect::<Vec<_>>()),
        "ownershipOmitted":owners.map(|rows|rows.len().saturating_sub(24)),
        "childRoster":child_roster,"terminalChildResults":terminal_results
    });
    while serde_json::to_vec(&evidence).is_ok_and(|bytes| bytes.len() > 4096) {
        let remaining = evidence["selectedOwnership"]
            .as_array_mut()
            .and_then(|selected| selected.pop().map(|_| selected.len()));
        if let Some(remaining) = remaining {
            evidence["ownershipOmitted"] = json!(owners.map_or(0, Vec::len) - remaining);
            continue;
        }
        let remaining = evidence["childRoster"]["selected"]
            .as_array_mut()
            .and_then(|selected| selected.pop().map(|_| selected.len()));
        if let Some(remaining) = remaining {
            evidence["childRoster"]["omitted"] = json!(child_rows.map_or(0, Vec::len) - remaining);
            continue;
        }
        let remaining = evidence["terminalChildResults"]["selected"]
            .as_array_mut()
            .and_then(|selected| selected.pop().map(|_| selected.len()));
        if let Some(remaining) = remaining {
            evidence["terminalChildResults"]["omitted"] = json!(evidence["terminalChildResults"]
                ["count"]
                .as_u64()
                .unwrap_or(0)
                .saturating_sub(remaining as u64));
            continue;
        }
        break;
    }
    evidence
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plan() -> Plan {
        serde_json::from_value(json!({"schemaVersion":2,"revision":"studio:7","goal":"g","acceptance":["proof"],"capacity":0,"userStopped":false,"budgetAvailable":true,"steps":[
            {"id":"a","planId":"p","stepId":"a","status":"ready","dependencies":[],"owner":null,"files":["a"],"acceptance":["test a"]},
            {"id":"b","planId":"p","stepId":"b","status":"ready","dependencies":[],"owner":null,"files":["b"],"acceptance":["test b"]}
        ]})).unwrap()
    }
    fn gate(slots: u64) -> Value {
        json!({"activeSessionId":"root","complete":true,"decision":"corrective","revision":3,"accountingEpoch":4,
            "unknown":0,"policy":{"enabled":true,"maxActive":2,"minActive":1},
            "nextWorkSourceInstanceId":"source",
            "nextWork":{"complete":true,"revision":7,"runtime":{"complete":true,"freeSlots":slots,
                "ready":[{"planId":"p","stepId":"a","status":"ready","ownership":["a"]},
                         {"planId":"p","stepId":"b","status":"ready","ownership":["b"]}],
                "ownership":[],"awaitingAcceptance":[]}}})
    }
    #[test]
    fn human_no_subagents_overrides_ready_capacity_and_hides_parallel_choices() {
        let gate = gate(2);
        let mut snapshot = json!({"subagentPolicy":{"taskAllowsSubagents":false,
            "instruction":"Работай без субагентов.","source":"genuine_user","maxDepth":5}});
        assert_eq!(candidates(&plan(), &gate, "root").len(), 2);
        snapshot["advisoryPolicy"] = advisory_policy(&snapshot, Some(&gate));
        assert_eq!(snapshot["advisoryPolicy"]["parallelAllowed"], false);
        assert_eq!(
            snapshot["advisoryPolicy"]["reason"],
            "human_task_forbids_subagents"
        );
        let choices =
            super::super::evidence::model_manifest(&snapshot, super::super::handlers::manifest());
        assert!(!choices
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["name"] == "recommend_parallel_work"));
        assert!(choices
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["name"] == "notify_root"));
        snapshot["subagentPolicy"]["taskAllowsSubagents"] = json!(true);
        assert_eq!(
            advisory_policy(&snapshot, Some(&gate))["parallelAllowed"],
            true
        );
        for message in [
            "Spawn a subagent for ready step a",
            "Delegate to a child worker",
            "Запусти субагента",
            "Нужен параллельный refill",
        ] {
            assert!(mentions_parallel_work(message));
        }
        assert!(!mentions_parallel_work(
            "The owned request is overdue; inspect its confirmed status."
        ));
        for (key, value) in [("maxActive", json!(0)), ("enabled", json!(false))] {
            let mut forbidden = gate.clone();
            forbidden["policy"][key] = value;
            assert!(candidates(&plan(), &forbidden, "root").is_empty());
            assert_eq!(
                advisory_policy(&snapshot, Some(&forbidden))["parallelAllowed"],
                false
            );
        }
        assert_eq!(
            advisory_policy(&snapshot, Some(&self::gate(0)))["parallelAllowed"],
            false
        );
        snapshot["subagentPolicy"]["maxDepth"] = json!(0);
        assert_eq!(
            advisory_policy(&snapshot, Some(&gate))["reason"],
            "subagent_depth_zero"
        );
    }
    #[test]
    fn drone_active_child_does_not_hide_finished_result_idle_assignment_or_ready_work() {
        let plan = plan();
        let gate = gate(1);
        let children = json!({"eventSequence":9,"children":[
            {"id":"active","status":"running","executionStatus":"running"},
            {"id":"finished","status":"done","executionStatus":"completed","answerPreview":"actual source-stop result; root acceptance required"},
            {"id":"idle-owner","status":"running","executionStatus":"idle"}
        ]});
        let evidence = runtime_evidence(Some(&gate), Some(&children));
        assert_eq!(
            evidence["terminalChildResults"]["selected"][0]["answerPreview"],
            "actual source-stop result; root acceptance required"
        );
        assert_eq!(
            evidence["terminalChildResults"]["idleAssignments"][0],
            json!({"id":"idle-owner","assignmentStatus":"running","executionStatus":"idle","result":"unconfirmed"})
        );
        assert_eq!(
            validate(&plan, &gate, "root", &["a".into()]).unwrap()[0].id,
            "a"
        );
        assert_eq!(evidence["freeSlots"], 1);
        assert_eq!(evidence["childRoster"]["count"], 3);
        assert!(serde_json::to_vec(&evidence).unwrap().len() <= 4096);
    }
    #[test]
    fn two_ready_steps_require_real_free_slots_and_exact_ownership() {
        let plan = plan();
        let ids = vec!["a".into(), "b".into()];
        assert_eq!(validate(&plan, &gate(2), "root", &ids).unwrap().len(), 2);
        assert!(validate(&plan, &gate(1), "root", &ids).is_err());
        let mut reserved = gate(2);
        reserved["unknown"] = json!(1);
        reserved["complete"] = json!(false);
        assert!(validate(&plan, &reserved, "root", &ids).is_err());
        let mut conflict = gate(2);
        conflict["nextWork"]["runtime"]["ready"][0]["ownership"] = json!(["foreign"]);
        assert!(validate(&plan, &conflict, "root", &["a".into()]).is_err());
        let mut waiting = gate(2);
        waiting["nextWork"]["runtime"]["awaitingAcceptance"] = json!([{"childId":"finished"}]);
        assert!(validate(&plan, &waiting, "root", &["a".into()]).is_err());
        assert!(validate(&plan, &gate(2), "foreign", &["a".into()]).is_err());
    }
    #[test]
    fn duplicate_or_shared_ownership_is_not_parallel() {
        let mut plan = plan();
        assert!(validate(&plan, &gate(2), "root", &["a".into(), "a".into()]).is_err());
        plan.steps[1].files = vec!["a".into()];
        let mut same_owner = gate(2);
        same_owner["nextWork"]["runtime"]["ready"][1]["ownership"] = json!(["a"]);
        assert!(validate(&plan, &same_owner, "root", &["a".into(), "b".into()]).is_err());
    }
    #[test]
    fn source_revision_or_accounting_epoch_change_invalidates_advice() {
        let prior = gate(2);
        let mut changed = prior.clone();
        changed["accountingEpoch"] = json!(5);
        assert!(!same_revision(&prior, &changed));
        changed = prior.clone();
        changed["nextWork"]["revision"] = json!(8);
        assert!(!same_revision(&prior, &changed));
        changed = prior.clone();
        changed["nextWorkSourceInstanceId"] = json!("replacement");
        assert!(!same_revision(&prior, &changed));
        assert!(same_revision(&prior, &prior));
    }
    #[test]
    fn projection_revision_and_nonempty_ownership_are_required() {
        let mut plan = plan();
        plan.revision = "studio:8".into();
        assert!(candidates(&plan, &gate(2), "root").is_empty());
        plan.revision = "studio:7".into();
        plan.steps[0].files.clear();
        let mut runtime = gate(2);
        runtime["nextWork"]["runtime"]["ready"][0]["ownership"] = json!([]);
        assert!(validate(&plan, &runtime, "root", &["a".into()]).is_err());
        assert!(validate(&plan, &runtime, "root", &["b".into()]).is_ok());
    }
    #[test]
    fn missing_gate_keeps_child_roster_visible_but_capacity_unknown() {
        let children = json!({"eventSequence":9,"children":[{"id":"child","status":"running",
            "executionStatus":"executing"}]});
        let evidence = runtime_evidence(None, Some(&children));
        assert_eq!(evidence["state"], "unavailable");
        assert_eq!(evidence["capacity"], "unknown");
        assert_eq!(evidence["childRoster"]["count"], 1);
        assert_eq!(evidence["scopeEvidence"], "declared_ownership_labels_only");
        assert_eq!(evidence["requiresRootScopeReview"], true);
    }
}

#[cfg(test)]
mod runtime_observation_regression {
    use super::*;
    #[test]
    fn terminal_assignment_busy_runtime_is_not_known_zero_and_unavailable_is_unknown() {
        let children = json!({"eventSequence":1,"children":[{"id":"child","status":"done","executionStatus":"completed","observation":{"available":true,"status":"running"}}]});
        let evidence = runtime_evidence(None, Some(&children));
        assert_eq!(evidence["childRoster"]["runtime"]["knownBusy"], 1);
        assert_eq!(evidence["childRoster"]["selected"][0]["status"], "done");
        assert_eq!(
            evidence["childRoster"]["selected"][0]["runtimeStatus"],
            "running"
        );
        let absent = runtime_evidence(None, None);
        assert_eq!(absent["childRoster"]["runtime"]["state"], "unknown");
        assert!(absent["childRoster"]["runtime"]["knownBusy"].is_null());
    }
}
