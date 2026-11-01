//! CAS projection over the existing per-thread scheduled-jobs artifact.
use super::{iso_from_millis, AgentCronJobStore, CreateAgentCronJobInput};
use crate::cron::{
    next_run_at_for_schedule, parse_agent_cron_schedule, parse_iso_millis, AgentCronJob,
    DeliveryMode, JobStatus,
};
use serde_json::{json, Value};

const BINDING: &str = "studio-roadmap-progress";
fn scoped(job: &AgentCronJob, input: &CreateAgentCronJobInput) -> bool {
    job.session_id == input.session_id
        && job.session_file == input.session_file
        && job.cwd == input.cwd
}
fn bound(job: &AgentCronJob) -> bool {
    job.source.as_deref() == Some("rlm_heartbeat") && job.label.as_deref() == Some(BINDING)
}
fn revision(jobs: &[AgentCronJob], input: &CreateAgentCronJobInput) -> u64 {
    jobs.iter()
        .filter(|j| scoped(j, input))
        .filter_map(|j| parse_iso_millis(&j.updated_at))
        .max()
        .unwrap_or(0)
}
fn state_revision(state: &super::CronJobsState, input: &CreateAgentCronJobInput) -> u64 {
    state
        .jobs
        .iter()
        .filter(|job| scoped(job, input))
        .filter_map(|job| {
            state
                .thread_heartbeat_results
                .get(&job.id)
                .and_then(|result| result.get("timestamp"))
                .and_then(Value::as_str)
                .and_then(parse_iso_millis)
        })
        .max()
        .unwrap_or(0)
        .max(revision(&state.jobs, input))
}
fn projection(job: &AgentCronJob, results: &std::collections::HashMap<String, Value>) -> Value {
    json!({"ownership":if job.source.as_deref()==Some("heartbeat") {"user"} else {"agent"},
        "binding":job.label,"instruction":job.prompt,"intervalMinutes":job.schedule.interval_ms.map(|ms|ms/60_000),
        "deliveryMode":job.delivery_mode.unwrap_or(DeliveryMode::Steer),"status":job.status,
        "lastActualRun":job.last_run_at,"nextRun":job.next_run_at,"timezone":"UTC",
        "lastError":job.last_error,"runCount":job.run_count,
        "lastActualResult":results.get(&job.id).cloned().unwrap_or_else(||json!({"supported":true,"outcome":"not_recorded","modelCompletionConfirmed":false}))})
}
fn snapshot(
    state: &super::CronJobsState,
    input: &CreateAgentCronJobInput,
) -> anyhow::Result<Value> {
    let jobs = &state.jobs;
    let matching: Vec<_> = jobs
        .iter()
        .filter(|j| {
            scoped(j, input)
                && bound(j)
                && matches!(j.status, JobStatus::Active | JobStatus::Paused)
        })
        .collect();
    anyhow::ensure!(matching.len() <= 1, "binding_conflict");
    Ok(
        json!({"extensionVersion":1,"identity":{"sessionId":input.session_id,"sessionFile":input.session_file,"cwd":input.cwd},
        "revision":state_revision(state,input),"heartbeat":matching.first().map(|j|projection(j,&state.thread_heartbeat_results)),
        "userHeartbeats":jobs.iter().filter(|j|scoped(j,input)&&j.source.as_deref()==Some("heartbeat")&&matches!(j.status,JobStatus::Active|JobStatus::Paused)).map(|j|projection(j,&state.thread_heartbeat_results)).collect::<Vec<_>>(),"durable":true}),
    )
}
fn read_strict(path: &std::path::Path) -> anyhow::Result<super::CronJobsState> {
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(super::CronJobsState::default())
        }
        Err(error) => Err(error.into()),
    }
}
impl AgentCronJobStore {
    fn thread_heartbeat_path(
        &self,
        input: &CreateAgentCronJobInput,
    ) -> anyhow::Result<std::path::PathBuf> {
        if self.session_artifact_mode {
            self.session_artifact_files
                .lock()
                .unwrap()
                .get(&input.session_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("identity_conflict"))
        } else {
            Ok(self.require_file_path())
        }
    }
    /// # Errors
    ///
    /// Fails on artifact identity, persistence, or heartbeat binding conflicts.
    pub fn thread_heartbeat_snapshot(
        &self,
        input: &CreateAgentCronJobInput,
    ) -> anyhow::Result<Value> {
        let path = self.thread_heartbeat_path(input)?;
        let state = read_strict(&path)?;
        snapshot(&state, input)
    }
    /// Restored queue items have lost their settle waiter. The scheduler's held
    /// dispatch, not an old journal prompt, owns reconciliation of these fires.
    /// # Errors
    ///
    /// Fails if the scoped artifact cannot be resolved or read strictly.
    pub fn thread_heartbeat_recovery_queue_keys(
        &self,
        input: &CreateAgentCronJobInput,
    ) -> anyhow::Result<Vec<String>> {
        let state = read_strict(&self.thread_heartbeat_path(input)?)?;
        Ok(state
            .jobs
            .iter()
            .filter(|job| scoped(job, input) && bound(job))
            .map(|job| format!("heartbeat:{}", job.id))
            .collect())
    }
    /// Validates and commits under the SAME existing artifact lock as due-claim.
    /// Does not rewrite the history, counters or other jobs, and never starts a turn.
    /// # Errors
    ///
    /// Fails on invalid input, stale revision, authority/binding conflicts, or persistence failure.
    pub fn mutate_thread_heartbeat(
        &self,
        input: &CreateAgentCronJobInput,
        payload: &Value,
    ) -> anyhow::Result<Value> {
        let expected = payload
            .get("expectedRevision")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("invalid_input"))?;
        let mut result = Err(anyhow::anyhow!("identity_conflict"));
        let path = self.thread_heartbeat_path(input)?;
        super::state::with_state_locks(std::slice::from_ref(&path), || -> anyhow::Result<()> {
            let mut state = read_strict(&path)?;
            let apply = (|| -> anyhow::Result<Value> {
                let current = state_revision(&state, input);
                anyhow::ensure!(current == expected, "revision_conflict");
                let before = snapshot(&state, input)?;
                let action = payload.get("action").and_then(Value::as_str).unwrap_or("");
                if matches!(action, "create" | "resume") {
                    anyhow::ensure!(
                        !state.dispatches.iter().any(|dispatch| state
                            .thread_heartbeat_results
                            .get(&dispatch.job_id)
                            .is_some_and(|result| result["outcome"] == "unknown")),
                        "dispatch_unconfirmed"
                    );
                }
                let found = state.jobs.iter().position(|j| {
                    scoped(j, input)
                        && bound(j)
                        && matches!(j.status, JobStatus::Active | JobStatus::Paused)
                });
                let now = input
                    .now
                    .unwrap_or_else(super::now_millis)
                    .max(current.saturating_add(1));
                let mut next = match (action, found) {
                    ("create", None) => {
                        let (schedule, next) = parse_agent_cron_schedule("every 10m", now)?;
                        AgentCronJob {
                            id: uuid::Uuid::new_v4().to_string(),
                            status: JobStatus::Active,
                            source: Some("rlm_heartbeat".into()),
                            runtime_kind: input.runtime_kind.clone(),
                            delivery_mode: Some(DeliveryMode::FollowUp),
                            active_session_id: input.active_session_id.clone(),
                            session_id: input.session_id.clone(),
                            session_file: input.session_file.clone(),
                            cwd: input.cwd.clone(),
                            label: Some(BINDING.into()),
                            prompt: String::new(),
                            schedule,
                            created_at: iso_from_millis(now),
                            updated_at: iso_from_millis(now),
                            next_run_at: Some(iso_from_millis(next)),
                            last_run_at: None,
                            last_skipped_at: None,
                            last_error: None,
                            run_count: 0,
                        }
                    }
                    ("create", Some(_)) => anyhow::bail!("binding_conflict"),
                    ("update" | "pause" | "resume" | "delete", Some(index)) => {
                        state.jobs[index].clone()
                    }
                    _ => anyhow::bail!("invalid_input"),
                };
                if let Some(instruction) = payload.get("instruction") {
                    next.prompt = instruction
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("invalid_input"))?
                        .trim()
                        .to_string();
                }
                anyhow::ensure!(!next.prompt.is_empty(), "invalid_input");
                if let Some(minutes) = payload.get("intervalMinutes") {
                    let minutes = minutes
                        .as_u64()
                        .filter(|m| *m > 0 && *m <= 10080)
                        .ok_or_else(|| anyhow::anyhow!("invalid_input"))?;
                    next.schedule = parse_agent_cron_schedule(&format!("every {minutes}m"), now)?.0;
                }
                if let Some(mode) = payload.get("deliveryMode") {
                    next.delivery_mode = Some(match mode.as_str() {
                        Some("follow_up") => DeliveryMode::FollowUp,
                        Some("steer") => DeliveryMode::Steer,
                        _ => anyhow::bail!("invalid_input"),
                    });
                }
                match action {
                    "pause" => next.status = JobStatus::Paused,
                    "resume" => next.status = JobStatus::Active,
                    "delete" => next.status = JobStatus::Cancelled,
                    _ => {}
                }
                next.updated_at = iso_from_millis(now);
                next.next_run_at = if next.status == JobStatus::Active {
                    next_run_at_for_schedule(&next.schedule, now)?.map(iso_from_millis)
                } else {
                    None
                };
                let next_id = next.id.clone();
                let mut jobs = state.jobs.clone();
                if let Some(index) = found {
                    jobs[index] = next;
                } else {
                    jobs.push(next);
                }
                let mut next_state = state.clone();
                next_state.jobs.clone_from(&jobs);
                let after = snapshot(&next_state, input)?;
                anyhow::ensure!(after["revision"] != before["revision"], "revision_conflict");
                if action == "create" || action == "resume" {
                    state.thread_heartbeat_authorizations.insert(
                        next_id.clone(),
                        json!({"revision":now,"supervisorSocket":payload.get("supervisorSocket")}),
                    );
                }
                if matches!(action, "pause" | "delete")
                    && state
                        .thread_heartbeat_results
                        .get(&next_id)
                        .is_some_and(|result| result["outcome"] == "unknown")
                {
                    state
                        .dispatches
                        .retain(|dispatch| dispatch.job_id != next_id);
                }
                state.jobs = jobs;
                Ok(after)
            })();
            result = apply;
            if result.is_ok() {
                super::state::write_jobs_state(&path, &state)?;
            }
            Ok(())
        })
        .ok_or_else(|| anyhow::anyhow!("store_error"))??;
        if result.is_ok() {
            self.notify_heartbeat_change();
        }
        result
    }
}

impl AgentCronJobStore {
    /// # Errors
    ///
    /// Fails if a heartbeat artifact cannot be read, locked, or committed.
    pub fn revoke_thread_heartbeat_restart(&self) -> anyhow::Result<()> {
        self.mutate_states(|state| {
            state.thread_heartbeat_authorizations.clear();
            Vec::new()
        })?;
        Ok(())
    }
}

/// Boot reads only explicit API authorization in the existing artifact; labels
/// created through the agent skill are not permission to auto-start a worker.
///
/// # Errors
///
/// Fails on artifact read errors, unconfirmed dispatches, or conflicting authorized bindings.
pub fn authorized_thread_heartbeats(
    path: &std::path::Path,
    socket: &str,
) -> anyhow::Result<Vec<AgentCronJob>> {
    let state = read_strict(path)?;
    let mut jobs = Vec::new();
    for job in state
        .jobs
        .iter()
        .filter(|job| bound(job) && job.status == JobStatus::Active)
    {
        if state
            .thread_heartbeat_authorizations
            .get(&job.id)
            .and_then(|a| a.get("supervisorSocket"))
            .and_then(Value::as_str)
            != Some(socket)
        {
            continue;
        }
        anyhow::ensure!(
            !state.dispatches.iter().any(|d| d.job_id == job.id),
            "unconfirmed_dispatch"
        );
        jobs.push(job.clone());
    }
    anyhow::ensure!(jobs.len() <= 1, "binding_conflict");
    Ok(jobs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_permission_is_explicit_socket_scoped_and_revocable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.json");
        let store = AgentCronJobStore::new(path.clone());
        let mut input = super::super::input("legacy", "every 10m", 1_700_000_000_000);
        input.label = Some(BINDING.into());
        store.create_rlm_heartbeat(&input).unwrap();
        assert!(authorized_thread_heartbeats(&path, "/daemon.sock")
            .unwrap()
            .is_empty());
        let rev = store.thread_heartbeat_snapshot(&input).unwrap()["revision"].clone();
        store.mutate_thread_heartbeat(&input,&json!({"action":"resume","expectedRevision":rev,"supervisorSocket":"/daemon.sock"})).unwrap();
        assert_eq!(
            authorized_thread_heartbeats(&path, "/daemon.sock")
                .unwrap()
                .len(),
            1
        );
        assert!(authorized_thread_heartbeats(&path, "/other.sock")
            .unwrap()
            .is_empty());
        store.revoke_thread_heartbeat_restart().unwrap();
        assert!(authorized_thread_heartbeats(&path, "/daemon.sock")
            .unwrap()
            .is_empty());
    }
    #[test]
    fn corrupt_store_fails_closed_without_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.json");
        std::fs::write(&path, "invalid json").unwrap();
        let store = AgentCronJobStore::new(path.clone());
        let input = super::super::input("x", "every 10m", 1_700_000_000_000);
        assert!(store.thread_heartbeat_snapshot(&input).is_err());
        assert!(store
            .mutate_thread_heartbeat(
                &input,
                &json!({"action":"create","expectedRevision":0,"instruction":"x"})
            )
            .is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "invalid json");
    }
    #[test]
    fn duplicate_binding_and_unknown_dispatch_block_restore() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.json");
        let store = AgentCronJobStore::new(path.clone());
        let input = super::super::input("x", "every 10m", 1_700_000_000_000);
        store.mutate_thread_heartbeat(&input,&json!({"action":"create","expectedRevision":0,"instruction":"x","supervisorSocket":"/daemon.sock"})).unwrap();
        let dispatch = store
            .claim_due(1_700_001_000_000, 1_700_001_000_000)
            .unwrap();
        assert_eq!(dispatch.len(), 1);
        assert!(authorized_thread_heartbeats(&path, "/daemon.sock")
            .unwrap_err()
            .to_string()
            .contains("unconfirmed_dispatch"));
        let mut other = input.clone();
        other.label = Some(BINDING.into());
        store.create_rlm_heartbeat(&other).unwrap();
        assert!(store
            .thread_heartbeat_snapshot(&input)
            .unwrap_err()
            .to_string()
            .contains("binding_conflict"));
    }

    #[test]
    fn cas_binding_and_history_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentCronJobStore::new(dir.path().join("jobs.json"));
        let input = super::super::input("unused", "every 10m", 1_700_000_000_000);
        let first = store
            .mutate_thread_heartbeat(
                &input,
                &json!({"expectedRevision":0,"action":"create","instruction":"inspect roadmap"}),
            )
            .unwrap();
        assert_eq!(first["heartbeat"]["deliveryMode"], "follow_up");
        assert_eq!(first["heartbeat"]["intervalMinutes"], 10);
        assert!(store
            .mutate_thread_heartbeat(&input, &json!({"expectedRevision":0,"action":"pause"}))
            .unwrap_err()
            .to_string()
            .contains("revision_conflict"));
        let paused = store
            .mutate_thread_heartbeat(
                &input,
                &json!({"expectedRevision":first["revision"],"action":"pause"}),
            )
            .unwrap();
        assert_eq!(paused["heartbeat"]["status"], "paused");
        let cleared = store
            .mutate_thread_heartbeat(
                &input,
                &json!({"expectedRevision":paused["revision"],"action":"delete"}),
            )
            .unwrap();
        assert!(cleared["heartbeat"].is_null());
        assert_eq!(store.list().len(), 1);
        assert_eq!(store.list()[0].status, JobStatus::Cancelled);
        assert!(cleared["revision"].as_u64().unwrap() > paused["revision"].as_u64().unwrap());
    }
    #[test]
    fn user_and_other_bindings_not_adopted() {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentCronJobStore::new(dir.path().join("jobs.json"));
        let input = super::super::input("user instruction", "every 10m", 1_700_000_000_000);
        let user = store.create_heartbeat(&input).unwrap();
        let snapshot = store.thread_heartbeat_snapshot(&input).unwrap();
        assert!(snapshot["heartbeat"].is_null());
        assert_eq!(snapshot["userHeartbeats"].as_array().unwrap().len(), 1);
        let created=store.mutate_thread_heartbeat(&input,&json!({"expectedRevision":snapshot["revision"],"action":"create","instruction":"agent instruction"})).unwrap();
        assert!(created["heartbeat"].is_object());
        assert!(store.list().contains(&user));
        assert!(store.mutate_thread_heartbeat(&input,&json!({"expectedRevision":created["revision"],"action":"create","instruction":"duplicate"})).unwrap_err().to_string().contains("binding_conflict"));
    }
    #[test]
    fn typed_settled_result_survives_reopen_and_unknown_requires_explicit_pause() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.json");
        let store = AgentCronJobStore::new(path.clone());
        let input = super::super::input("x", "every 10m", 1_700_000_000_000);
        store.mutate_thread_heartbeat(&input,&json!({"action":"create","expectedRevision":0,"instruction":"x","supervisorSocket":"/daemon.sock"})).unwrap();
        let dispatch = store
            .claim_due(1_700_001_000_000, 1_700_001_000_000)
            .unwrap()
            .remove(0);
        store
            .record_dispatch_result(
                &dispatch.id,
                &super::super::DispatchResultOptions {
                    now: Some(1_700_001_000_001),
                    outcome: "completed",
                    error: None,
                },
            )
            .unwrap();
        let durable: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(durable["threadHeartbeatResults"].is_object());
        assert!(durable["threadHeartbeatAuthorizations"].is_object());
        let reopened = AgentCronJobStore::new(path.clone());
        let completed = reopened.thread_heartbeat_snapshot(&input).unwrap();
        assert_eq!(
            completed["heartbeat"]["lastActualResult"]["outcome"],
            "completed"
        );
        assert_eq!(
            completed["heartbeat"]["lastActualResult"]["modelCompletionConfirmed"],
            true
        );
        let dispatch = store
            .claim_due(1_700_002_000_000, 1_700_002_000_000)
            .unwrap()
            .remove(0);
        store
            .record_dispatch_result(
                &dispatch.id,
                &super::super::DispatchResultOptions {
                    now: Some(1_700_002_000_001),
                    outcome: "unknown",
                    error: None,
                },
            )
            .unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        store
            .recover_interrupted_dispatches(1_700_002_000_002)
            .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
        assert!(store
            .claim_due(1_700_009_000_000, 1_700_009_000_000)
            .unwrap()
            .is_empty());
        let unknown = store.thread_heartbeat_snapshot(&input).unwrap();
        assert_eq!(
            unknown["heartbeat"]["lastActualResult"]["outcome"],
            "unknown"
        );
        assert!(store
            .mutate_thread_heartbeat(
                &input,
                &json!({"action":"resume","expectedRevision":unknown["revision"]})
            )
            .unwrap_err()
            .to_string()
            .contains("dispatch_unconfirmed"));
        let paused = store
            .mutate_thread_heartbeat(
                &input,
                &json!({"action":"pause","expectedRevision":unknown["revision"]}),
            )
            .unwrap();
        store.mutate_thread_heartbeat(&input,&json!({"action":"resume","expectedRevision":paused["revision"],"supervisorSocket":"/daemon.sock"})).unwrap();
        assert_eq!(
            authorized_thread_heartbeats(&path, "/daemon.sock")
                .unwrap()
                .len(),
            1
        );
    }
}
