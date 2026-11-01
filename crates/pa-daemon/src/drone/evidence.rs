//! Bounded outcome facts and material state for advisory admission.
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(super) fn terminal_children(children: Option<&Value>) -> Value {
    let Some(rows) = children
        .filter(|value| super::parallel::roster_available(value))
        .and_then(|value| value["children"].as_array())
    else {
        return json!({"state":"unavailable"});
    };
    let mut outcomes = rows
        .iter()
        .filter(|row| matches!(row["status"].as_str(), Some("done" | "error" | "cancelled")))
        .map(|row| {
            json!({
                "id":row["id"],"status":row["status"],
                "answerPreview":row["answerPreview"],"error":row["error"]
            })
        })
        .collect::<Vec<_>>();
    let selected = outcomes.iter().rev().take(4).map(|row|json!({
        "id":row["id"],"status":row["status"],
        "answerPreview":row["answerPreview"].as_str().map(|text|text.chars().take(384).collect::<String>()),
        "error":row["error"].as_str().map(|text|text.chars().take(384).collect::<String>())
    })).collect::<Vec<_>>();
    outcomes.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    let idle = rows
        .iter()
        .filter(|row| row["status"] == "running" && row["executionStatus"] == "idle")
        .collect::<Vec<_>>();
    json!({"state":"available","count":outcomes.len(),"selected":selected,
        "omitted":outcomes.len().saturating_sub(4),
        "idleAssignments":idle.iter().take(4).map(|row|json!({"id":row["id"],"assignmentStatus":row["status"],"executionStatus":row["executionStatus"],"result":"unconfirmed"})).collect::<Vec<_>>(),
        "idleAssignmentsOmitted":idle.len().saturating_sub(4),
        "fingerprint":format!("{:x}",Sha256::digest(json!(outcomes).to_string().as_bytes()))})
}

fn child_assignments(children: Option<&Value>) -> Value {
    let rows = children
        .filter(|value| super::parallel::roster_available(value))
        .and_then(|value| value["children"].as_array());
    let mut assignments = rows
        .into_iter()
        .flatten()
        .map(|row| {
            json!({
                "id":row["id"],"status":row["status"],"executionStatus":row["executionStatus"],"runtimeStatus":if row["observation"]["available"] == true {row["observation"]["status"].clone()} else {json!("unknown")}
            })
        })
        .collect::<Vec<_>>();
    assignments.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    json!({"available":rows.is_some(),"assignments":assignments})
}

fn goal_state(snapshot: &Value) -> Value {
    let goal = &snapshot["goal"];
    json!({"id":goal["id"],"goalId":goal["goalId"],"status":goal["status"],
        "active":goal["active"],"objective":goal["objective"],"tokenBudget":goal["tokenBudget"]})
}

fn capacity_state(gate: Option<&Value>) -> Value {
    let gate = gate.unwrap_or(&Value::Null);
    let runtime = &gate["nextWork"]["runtime"];
    json!({"complete":gate["complete"],"decision":gate["decision"],"unknown":gate["unknown"],
        "knownActive":gate["knownActive"],"policy":{"enabled":gate["policy"]["enabled"],
            "minActive":gate["policy"]["minActive"],"maxActive":gate["policy"]["maxActive"]},
        "source":gate["nextWorkSourceInstanceId"],"completeNext":gate["nextWork"]["complete"],
        "runtimeComplete":runtime["complete"],"freeSlots":runtime["freeSlots"],
        "ready":runtime["ready"],"ownership":runtime["ownership"],"awaitingAcceptance":runtime["awaitingAcceptance"]})
}

pub(super) fn notice_fingerprint(
    snapshot: &Value,
    roadmap: &Value,
    gate: Option<&Value>,
    children: Option<&Value>,
    owned_job_outcome: Option<&str>,
) -> String {
    let mut plan = roadmap["structured"].clone();
    if let Some(object) = plan.as_object_mut() {
        // A Studio revision bump without a plan change is not new evidence.
        object.remove("revision");
    } else {
        plan = roadmap["sha256"].clone();
    }
    let outcomes = terminal_children(children);
    // Root replies and transport/queue counters are intentionally absent: a
    // delivered advisory must not authorize its own feedback notice.
    let material = json!({"human":snapshot["humanInputFingerprint"],"roadmap":plan,
        "terminalChildren":{"state":outcomes["state"],"fingerprint":outcomes["fingerprint"]},
        "childAssignments":child_assignments(children),"ownedJobOutcome":owned_job_outcome,
        "goal":goal_state(snapshot),"capacity":capacity_state(gate),
        "subagentPolicy":snapshot["subagentPolicy"]});
    format!("{:x}", Sha256::digest(material.to_string().as_bytes()))
}

pub(super) struct ModelState {
    pub key: String,
    pub unresolved: bool,
}

/// Time/counter noise is excluded, but real work, results and deadline eligibility
/// remain material. Native still validates the complete fresh state at admission.
pub(super) fn model_state(
    snapshot: &Value,
    roadmap: &Value,
    gate: Option<&Value>,
    children: Option<&Value>,
    owned_outcome: Option<&str>,
) -> ModelState {
    let request = &snapshot["request"];
    let overdue = request["phase"] == "transport"
        && request["deadlineMs"]
            .as_u64()
            .zip(request["elapsedMs"].as_u64())
            .is_some_and(|(deadline, elapsed)| elapsed >= deadline);
    let tools = snapshot["tools"].as_array();
    let idle = request.is_null()
        && tools.is_some_and(Vec::is_empty)
        && snapshot["hasBackgroundBash"] == false
        && snapshot["compacting"] == false;
    let gate = gate.unwrap_or(&Value::Null);
    let runtime = &gate["nextWork"]["runtime"];
    let child_rows = children.and_then(|value| value["children"].as_array());
    let mut assignments = child_rows
        .into_iter()
        .flatten()
        .map(|row| {
            json!({
                "id":row["id"],"status":row["status"],"executionStatus":row["executionStatus"],"runtimeStatus":if row["observation"]["available"] == true {row["observation"]["status"].clone()} else {json!("unknown")}
            })
        })
        .collect::<Vec<_>>();
    assignments.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let idle_child = assignments
        .iter()
        .any(|row| row["status"] == "running" && row["executionStatus"] == "idle");
    let open_plan = roadmap["structured"]["steps"]
        .as_array()
        .is_some_and(|steps| {
            steps.iter().any(|step| {
                !matches!(
                    step["status"].as_str(),
                    Some("done" | "complete" | "completed" | "verified" | "cancelled" | "skipped")
                )
            })
        });
    let unknown = gate["unknown"].as_u64().is_none_or(|count| count > 0);
    let unaccepted = runtime["awaitingAcceptance"]
        .as_array()
        .is_some_and(|rows| !rows.is_empty());
    let unresolved = overdue || idle_child || unknown || unaccepted || (idle && open_plan);
    let assistant = snapshot["recentRootAssistantEvidence"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|row| row["text"].clone())
                .collect::<Vec<_>>()
        });
    let material = json!({
        "noticeState":notice_fingerprint(snapshot, roadmap, Some(gate), children, None),
        "root":{"idle":idle,"phase":request["phase"],"overdue":overdue,
            "overdueIdentity":overdue.then(||json!({"requestId":request["requestId"],"revision":request["revision"]})),
            "tools":snapshot["tools"],"compacting":snapshot["compacting"],
            "background":snapshot["hasBackgroundBash"],"pendingHumanInputs":snapshot["pendingHumanInputs"],
            "paused":snapshot["paused"],"stopped":snapshot["stopped"],
            "goal":goal_state(snapshot)},
        "assistantResults":assistant,"ownedOutcome":owned_outcome,
        "children":{"available":child_rows.is_some(),"assignments":assignments},
        "capacity":capacity_state(Some(gate))
    });
    ModelState {
        key: format!("{:x}", Sha256::digest(material.to_string().as_bytes())),
        unresolved,
    }
}

/// Remove only checks which return the exact supplied complete snapshot. An
/// incomplete/capability-missing snapshot retains every existing handler choice.
pub(super) fn model_manifest(snapshot: &Value, mut manifest: Value) -> Value {
    if snapshot["advisoryPolicy"]["parallelAllowed"] == false {
        if let Some(rows) = manifest.as_array_mut() {
            rows.retain(|row| row["name"] != "recommend_parallel_work");
        }
    }
    let identity_present = [
        "activeSessionId",
        "sessionId",
        "sessionFile",
        "cwd",
        "workerIncarnation",
    ]
    .iter()
    .all(|key| {
        snapshot[key]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    });
    let flags_present = [
        "hasBackgroundBash",
        "hasRunningChildren",
        "compacting",
        "paused",
        "stopped",
    ]
    .iter()
    .all(|key| snapshot[key].is_boolean());
    let request = &snapshot["request"];
    let request_present = snapshot.get("request").is_some()
        && (request.is_null()
            || (request["requestId"].is_u64()
                && request["revision"].is_u64()
                && request["phase"].is_string()));
    let complete = identity_present
        && flags_present
        && request_present
        && snapshot["tools"].is_array()
        && snapshot["generation"].is_u64()
        && snapshot["queuedInputs"].is_u64()
        && snapshot["pendingHumanInputs"].is_u64()
        && snapshot["latestUserCorrections"].is_array()
        && snapshot["humanInputFingerprint"].is_string()
        && snapshot.get("goal").is_some();
    if complete {
        if let Some(rows) = manifest.as_array_mut() {
            rows.retain(|row| {
                row["name"] != "snapshot_branch"
                    && !(row["name"] == "run_registered_check"
                        && row["registeredName"] == "branch_facts")
            });
        }
    }
    manifest
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drone::registry::DroneRegistry;

    #[test]
    fn drone_notice_rearms_for_material_work_without_human_plan_or_reply_counter_noise() {
        let mut registry = DroneRegistry::default();
        registry.cooldown = std::time::Duration::ZERO;
        let mut snapshot = json!({"humanInputFingerprint":"actual-human", "turnSerial":1,
            "request":{"revision":1},"recentRootAssistantEvidence":["blocked"]});
        let mut roadmap = json!({"sha256":"bytes-1","structured":{"revision":"studio:1",
            "goal":"authorized independent work","steps":[{"id":"a","status":"running"}]}});
        let mut children = json!({"eventSequence":1,"children":[
            {"id":"finished","status":"done","answerPreview":"actual acceptance evidence","durationMs":1},
            {"id":"running","status":"running","progressNote":"working"}]});
        let first = registry.push("root", Value::Null).unwrap();
        let key = notice_fingerprint(&snapshot, &roadmap, None, Some(&children), None);
        assert!(registry.advice_is_new("root", key.clone()));
        registry.finish(&first, true);
        for turn in 2..8 {
            snapshot["turnSerial"] = json!(turn);
            snapshot["request"]["revision"] = json!(turn);
            snapshot["recentRootAssistantEvidence"] =
                json!([format!("already answered blocker {turn}")]);
            children["eventSequence"] = json!(turn);
            children["children"][0]["durationMs"] = json!(turn);
            children["children"][1]["progressNote"] = json!(format!("progress {turn}"));
            roadmap["sha256"] = json!(format!("revision-bytes-{turn}"));
            roadmap["structured"]["revision"] = json!(format!("studio:{turn}"));
            let assessment = registry
                .push(
                    "root",
                    json!({"type":"message_end","source":"drone","sequence":turn}),
                )
                .unwrap();
            assert_eq!(
                notice_fingerprint(&snapshot, &roadmap, None, Some(&children), None),
                key
            );
            assert!(!registry.advice_is_new("root", key.clone()));
            registry.finish(&assessment, true);
        }
        snapshot["humanInputFingerprint"] = json!("fresh genuine human correction");
        let human = notice_fingerprint(&snapshot, &roadmap, None, Some(&children), None);
        assert_ne!(human, key);
        assert!(registry.advice_is_new("root", human.clone()));
        roadmap["structured"]["steps"][0]["status"] = json!("verified");
        let plan = notice_fingerprint(&snapshot, &roadmap, None, Some(&children), None);
        assert_ne!(plan, human);
        assert!(registry.advice_is_new("root", plan.clone()));
        children["children"][1]["status"] = json!("done");
        children["children"][1]["answerPreview"] = json!("next actual result");
        let outcome = notice_fingerprint(&snapshot, &roadmap, None, Some(&children), None);
        assert_ne!(outcome, plan);
        assert!(registry.advice_is_new("root", outcome));
        let before = terminal_children(Some(&children));
        children["children"].as_array_mut().unwrap().reverse();
        assert_eq!(
            terminal_children(Some(&children))["fingerprint"],
            before["fingerprint"]
        );
        assert_eq!(before["selected"][0]["answerPreview"], "next actual result");
    }

    #[test]
    fn drone_model_state_tracks_work_results_capacity_and_deadline_without_counter_noise() {
        let mut snapshot = json!({"humanInputFingerprint":"human","request":{"requestId":7,"revision":2,
            "phase":"transport","deadlineMs":100,"elapsedMs":1},"tools":[],"compacting":false,
            "hasBackgroundBash":false,"pendingHumanInputs":0,"queuedInputs":0,"paused":false,"stopped":false,
            "recentRootAssistantEvidence":[{"entryId":"one","text":"actual result"}]});
        let roadmap = json!({"structured":{"revision":"studio:1","steps":[{"id":"plan","status":"unassessed"}]}});
        let mut gate = json!({"complete":true,"unknown":0,"nextWork":{"complete":true,"runtime":{
            "complete":true,"freeSlots":1,"ready":[],"ownership":[],"awaitingAcceptance":[]}}});
        let mut children = json!({"eventSequence":1,"children":[{"id":"child","status":"running","executionStatus":"active"}]});
        let state = |snapshot: &Value, gate: &Value, children: &Value| {
            model_state(snapshot, &roadmap, Some(gate), Some(children), None)
        };
        let initial = state(&snapshot, &gate, &children);
        snapshot["request"]["elapsedMs"] = json!(99);
        snapshot["request"]["lastProgressMs"] = json!(90);
        snapshot["turnSerial"] = json!(999);
        snapshot["queuedInputs"] = json!(99); // background Drone advice is not a human change
        snapshot["recentRootAssistantEvidence"][0]["entryId"] = json!("new-id-same-content");
        gate["accountingEpoch"] = json!(999);
        children["eventSequence"] = json!(999);
        assert_eq!(state(&snapshot, &gate, &children).key, initial.key);
        snapshot["request"]["elapsedMs"] = json!(100);
        let overdue = state(&snapshot, &gate, &children);
        assert_ne!(overdue.key, initial.key);
        assert!(overdue.unresolved);
        snapshot["request"] = Value::Null;
        let idle = state(&snapshot, &gate, &children);
        assert_ne!(idle.key, overdue.key);
        assert!(idle.unresolved); // unassessed, READY0
        for change in [
            json!({"tools":["new-owned-job"]}),
            json!({"hasBackgroundBash":true}),
            json!({"humanInputFingerprint":"new-human"}),
            json!({"stopped":true}),
            json!({"recentRootAssistantEvidence":[{"text":"new actual blocker/result"}]}),
        ] {
            let mut next = snapshot.clone();
            for (key, value) in change.as_object().unwrap() {
                next[key] = value.clone();
            }
            assert_ne!(state(&next, &gate, &children).key, idle.key);
        }
        let before = state(&snapshot, &gate, &children).key;
        gate["unknown"] = json!(1);
        assert_ne!(state(&snapshot, &gate, &children).key, before);
        gate["nextWork"]["runtime"]["ready"] = json!([{"stepId":"new-ready"}]);
        let ready = state(&snapshot, &gate, &children).key;
        gate["nextWork"]["runtime"]["freeSlots"] = json!(0);
        assert_ne!(state(&snapshot, &gate, &children).key, ready);
        children["children"][0]["executionStatus"] = json!("idle");
        let child_idle = state(&snapshot, &gate, &children).key;
        children["children"][0]["status"] = json!("done");
        children["children"][0]["answerPreview"] = json!("verified result");
        assert_ne!(state(&snapshot, &gate, &children).key, child_idle);
        assert_ne!(
            model_state(
                &snapshot,
                &roadmap,
                Some(&gate),
                Some(&children),
                Some("new-job-failure")
            )
            .key,
            state(&snapshot, &gate, &children).key
        );
    }

    #[test]
    fn drone_model_choices_skip_only_complete_duplicate_facts_and_keep_real_actions() {
        let complete = json!({"activeSessionId":"active","sessionId":"root","sessionFile":"/owned/root.jsonl",
            "cwd":"/owned","workerIncarnation":"worker","generation":1,"tools":[],"request":null,
            "hasBackgroundBash":false,"hasRunningChildren":false,"compacting":false,"paused":false,"stopped":false,
            "queuedInputs":0,"pendingHumanInputs":0,"latestUserCorrections":[],"humanInputFingerprint":"human","goal":null});
        let mut all = crate::drone::handlers::manifest();
        all.as_array_mut().unwrap().extend([
            json!({"name":"run_registered_check","registeredName":"branch_facts","args":{"name":"branch_facts"}}),
            json!({"name":"run_registered_check","registeredName":"missing-result-check","args":{"name":"missing-result-check"}})]);
        let choices = model_manifest(&complete, all.clone());
        assert!(
            !choices
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["name"] == "snapshot_branch"
                    || row["registeredName"] == "branch_facts")
        );
        assert!(choices
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["registeredName"] == "missing-result-check"));
        let action = crate::drone::handlers::Action {
            handler: "notify_root".into(),
            args: json!({"message":"fresh proved blocker; root verifies the affected path"}),
        };
        assert!(choices
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["name"] == action.handler));
        let (command, payload) = crate::drone::handlers::command(&action).unwrap();
        assert_eq!(command, "follow_up");
        assert_eq!(payload["source"], "drone");
        let mut incomplete = complete;
        incomplete["tools"] = json!("capability_missing");
        assert_eq!(model_manifest(&incomplete, all.clone()), all);
    }

    #[test]
    fn drone_advice_rearms_for_owned_failure_idle_or_passivation_without_reply_feedback() {
        let mut registry = DroneRegistry::default();
        registry.push("root", Value::Null).unwrap();
        let mut snapshot = json!({"humanInputFingerprint":"same-human","request":{"revision":1},
            "recentRootAssistantEvidence":[{"text":"before"}],"queuedInputs":0,
            "goal":{"id":"same-goal","status":"active","active":true,"tokenBudget":1000,"tokensUsed":1}});
        let roadmap = json!({"structured":{"revision":"studio:1","steps":[{"id":"F4","status":"unassessed"}]}});
        let mut gate = json!({"complete":true,"unknown":0,"accountingEpoch":1,
            "nextWork":{"complete":true,"revision":1,"runtime":{"complete":true,"freeSlots":1,
                "ready":[],"ownership":[],"awaitingAcceptance":[]}}});
        let mut children = json!({"eventSequence":1,"children":[{"id":"owner","status":"running","executionStatus":"active"}]});
        let key = |snapshot: &Value, gate: &Value, children: &Value, job: Option<&str>| {
            notice_fingerprint(snapshot, &roadmap, Some(gate), Some(children), job)
        };
        let first = key(&snapshot, &gate, &children, None);
        assert!(registry.advice_is_new("root", first.clone()));
        snapshot["request"] = Value::Null; // reply finished: not a new advisory reason
        snapshot["turnSerial"] = json!(999);
        snapshot["recentRootAssistantEvidence"] = json!([{"text":"reply to Drone"}]);
        snapshot["queuedInputs"] = json!(1);
        snapshot["goal"]["tokensUsed"] = json!(999);
        gate["accountingEpoch"] = json!(999);
        gate["nextWork"]["revision"] = json!(999);
        children["eventSequence"] = json!(999);
        assert_eq!(key(&snapshot, &gate, &children, None), first);
        assert!(!registry.advice_is_new("root", first));

        let job = Some("actual-owned-tool-failure");
        let failure = key(&snapshot, &gate, &children, job);
        assert!(registry.advice_is_new("root", failure.clone()));
        snapshot["recentRootAssistantEvidence"] = json!([{"text":"another reply/ACK"}]);
        assert_eq!(key(&snapshot, &gate, &children, job), failure);
        assert!(!registry.advice_is_new("root", failure));
        children["children"][0]["executionStatus"] = json!("idle");
        let idle = key(&snapshot, &gate, &children, job);
        assert!(registry.advice_is_new("root", idle.clone()));
        assert!(!registry.advice_is_new("root", idle));
        children["children"] = json!([]); // same unfinished plan, READY0; owner passivated
        assert!(registry.advice_is_new("root", key(&snapshot, &gate, &children, job)));
        gate["unknown"] = json!(1);
        assert!(registry.advice_is_new("root", key(&snapshot, &gate, &children, job)));
        gate["nextWork"]["runtime"]["ready"] = json!([{"stepId":"F4-successor","status":"ready"}]);
        assert!(registry.advice_is_new("root", key(&snapshot, &gate, &children, job)));
        gate["nextWork"]["runtime"]["freeSlots"] = json!(0);
        assert!(registry.advice_is_new("root", key(&snapshot, &gate, &children, job)));
        snapshot["goal"]["status"] = json!("error");
        snapshot["goal"]["active"] = json!(false);
        let goal_held = key(&snapshot, &gate, &children, job);
        assert!(registry.advice_is_new("root", goal_held));
        snapshot["recentRootAssistantEvidence"] = json!([{"text":"held response"}]);
        assert!(!registry.advice_is_new("root", key(&snapshot, &gate, &children, job)));
        snapshot["goal"]["status"] = json!("active");
        snapshot["goal"]["active"] = json!(true);
        assert!(registry.advice_is_new("root", key(&snapshot, &gate, &children, job)));
    }
}

#[cfg(test)]
mod retained_runtime_material_regression {
    use super::*;
    #[test]
    fn settled_busy_runtime_is_material_input_without_changing_assignment() {
        let snapshot = json!({});
        let roadmap = json!({"structured":{}});
        let mut children = json!({"eventSequence":1,"children":[{"id":"retained","status":"done","executionStatus":"completed","observation":{"available":false,"status":"unknown"}}]});
        let unknown = model_state(&snapshot, &roadmap, None, Some(&children), None);
        children["children"][0]["observation"] = json!({"available":true,"status":"running"});
        let busy = model_state(&snapshot, &roadmap, None, Some(&children), None);
        assert_ne!(unknown.key, busy.key);
        let compact = super::super::parallel::runtime_evidence(None, Some(&children));
        assert_eq!(compact["childRoster"]["runtime"]["knownBusy"], 1);
        assert_eq!(compact["childRoster"]["selected"][0]["status"], "done");
        assert_eq!(
            compact["childRoster"]["selected"][0]["runtimeStatus"],
            "running"
        );
    }
}
