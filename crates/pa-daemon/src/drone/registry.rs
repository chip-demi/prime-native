//! One daemon-owned assessment slot per canonical user root, bounded event deltas.
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::time::Instant;
const UNRESOLVED_RECHECK: std::time::Duration = std::time::Duration::from_secs(300);
const EVENT_CAP: usize = 64;
const MAX_INTERNAL_CONTINUATIONS: usize = 4;
const COOLDOWN: std::time::Duration = std::time::Duration::from_secs(30);
pub(crate) struct DroneRegistry {
    pub cooldown: std::time::Duration,
    slots: HashMap<String, Slot>,
    pub scripts: HashMap<String, super::scripts::RegisteredScript>,
}
impl Default for DroneRegistry {
    fn default() -> Self {
        Self {
            slots: HashMap::new(),
            scripts: HashMap::new(),
            cooldown: COOLDOWN,
        }
    }
}
struct Slot {
    epoch: u64,
    active: bool,
    in_flight: bool,
    sequence: u64,
    assessment_index: u64,
    /// Last external-event sequence admitted to an assessment. Internal
    /// handler results do not advance it or schedule another model call.
    last_started_sequence: u64,
    continuation_pending: bool,
    continued_observations: HashSet<String>,
    events: VecDeque<Value>,
    gap: bool,
    last_assessment: Option<Instant>,
    failures: u32,
    wake_owned: bool,
    budget: u32,
    last_advice: Option<String>,
    last_failure: Option<String>,
    progress: String,
    last_escalation: Option<String>,
    evidence_key: String,
    model_state: Option<String>,
    pending_model_state: Option<String>,
    last_model_assessment: Option<Instant>,
    unresolved: bool,
    owned_outcome: Option<String>,
    owned_job_outcome: Option<String>,
}
#[derive(Clone)]
pub(crate) struct Assessment {
    pub root: String,
    pub epoch: u64,
    pub sequence: u64,
    pub page_cursor: u64,
    pub events: Vec<Value>,
    pub gap: bool,
    pub continuation: bool,
    pub owned_outcome: Option<String>,
    pub owned_job_outcome: Option<String>,
}
impl DroneRegistry {
    /// Only an explicit user resume may reopen a stopped observer slot.
    pub(crate) fn resume_explicit(&mut self, root: &str) {
        self.suspend_ineligible(root);
        if let Some(slot) = self.slots.get_mut(root) {
            slot.active = true;
            slot.failures = 0;
            slot.budget = 0;
            slot.last_failure = None;
            slot.last_escalation = None;
        }
    }
    pub(crate) fn push(&mut self, root: &str, event: Value) -> Option<Assessment> {
        // Stream deltas are progress, not new evidence or a new observation episode.
        if matches!(
            event["type"].as_str(),
            Some("message_update" | "tool_execution_update")
        ) {
            return None;
        }
        let slot = self.slots.entry(root.into()).or_insert_with(|| Slot {
            epoch: 1,
            active: true,
            in_flight: false,
            sequence: 0,
            assessment_index: 0,
            last_started_sequence: 0,
            continuation_pending: false,
            continued_observations: HashSet::new(),
            events: VecDeque::new(),
            gap: true,
            last_assessment: None,
            failures: 0,
            wake_owned: false,
            budget: 0,
            last_advice: None,
            last_failure: None,
            progress: "snapshot".into(),
            last_escalation: None,
            evidence_key: String::new(),
            model_state: None,
            pending_model_state: None,
            last_model_assessment: None,
            unresolved: false,
            owned_outcome: None,
            owned_job_outcome: None,
        });
        if !slot.active {
            return None;
        }
        slot.sequence += 1;
        if matches!(
            event["type"].as_str(),
            Some("tool_execution_end" | "bash_execution_end")
        ) || (event["type"] == "message_end" && event["messageRole"] == "assistant")
        {
            let result = serde_json::json!({"type":event["type"],"toolCallId":event["toolCallId"],
                "isError":event["isError"],"exitCode":event["exitCode"],"evidenceSha256":event["evidenceSha256"]});
            slot.owned_outcome = Some(format!(
                "{:x}",
                Sha256::digest(result.to_string().as_bytes())
            ));
            if matches!(
                event["type"].as_str(),
                Some("tool_execution_end" | "bash_execution_end")
            ) {
                // A reply to the notice can update model evidence, but cannot
                // erase the last owned job result used for notice admission.
                slot.owned_job_outcome = slot.owned_outcome.clone();
            }
        }
        if slot.events.len() == EVENT_CAP {
            slot.events.pop_front();
            slot.gap = true;
        }
        slot.events.push_back(event);
        if slot.in_flight
            || slot.failures >= 3
            || slot
                .last_assessment
                .is_some_and(|at| at.elapsed() < self.cooldown)
        {
            return None;
        }
        if slot.budget >= 8 {
            return None;
        }
        slot.budget += 1;
        let page_cursor = slot.assessment_index;
        slot.assessment_index = slot.assessment_index.wrapping_add(1);
        slot.in_flight = true;
        slot.last_started_sequence = slot.sequence;
        slot.last_assessment = Some(Instant::now());
        Some(Assessment {
            root: root.into(),
            epoch: slot.epoch,
            sequence: slot.sequence,
            page_cursor,
            events: slot.events.drain(..).collect(),
            gap: std::mem::replace(&mut slot.gap, false),
            continuation: std::mem::replace(&mut slot.continuation_pending, false),
            owned_outcome: slot.owned_outcome.clone(),
            owned_job_outcome: slot.owned_job_outcome.clone(),
        })
    }
    pub(crate) fn wake_delay(&mut self, root: &str) -> Option<std::time::Duration> {
        let slot = self.slots.get_mut(root)?;
        if !slot.active
            || slot.in_flight
            || slot.wake_owned
            || (slot.sequence == slot.last_started_sequence
                && slot.failures == 0
                && (!slot.continuation_pending || slot.budget >= 8)
                && !slot.unresolved)
        {
            return None;
        }
        slot.wake_owned = true;
        if slot.failures >= 3 || slot.budget >= 8 {
            return Some(std::time::Duration::from_secs(120));
        }
        if slot.failures == 0
            && slot.sequence == slot.last_started_sequence
            && !slot.continuation_pending
            && slot.unresolved
        {
            return Some(
                slot.last_model_assessment
                    .map_or(std::time::Duration::ZERO, |at| {
                        UNRESOLVED_RECHECK.saturating_sub(at.elapsed())
                    }),
            );
        }
        Some(
            slot.last_assessment
                .map_or(std::time::Duration::ZERO, |at| {
                    self.cooldown.saturating_sub(at.elapsed())
                }),
        )
    }
    pub(crate) fn wake(&mut self, root: &str) -> Option<Assessment> {
        let slot = self.slots.get_mut(root)?;
        slot.wake_owned = false;
        if !slot.active
            || slot.in_flight
            || (slot.sequence == slot.last_started_sequence
                && slot.failures == 0
                && (!slot.continuation_pending || slot.budget >= 8)
                && !slot.unresolved)
        {
            return None;
        }
        if slot.failures >= 3 || slot.budget >= 8 {
            slot.failures = 0;
            slot.budget = 0;
            slot.gap = true;
        }
        slot.budget += 1;
        let page_cursor = slot.assessment_index;
        slot.assessment_index = slot.assessment_index.wrapping_add(1);
        slot.in_flight = true;
        slot.last_started_sequence = slot.sequence;
        slot.last_assessment = Some(Instant::now());
        Some(Assessment {
            root: root.into(),
            epoch: slot.epoch,
            sequence: slot.sequence,
            page_cursor,
            events: slot.events.drain(..).collect(),
            gap: std::mem::replace(&mut slot.gap, false),
            continuation: std::mem::replace(&mut slot.continuation_pending, false),
            owned_outcome: slot.owned_outcome.clone(),
            owned_job_outcome: slot.owned_job_outcome.clone(),
        })
    }
    pub(super) fn admit_model_state(
        &mut self,
        a: &Assessment,
        state: super::evidence::ModelState,
    ) -> bool {
        let Some(slot) = self
            .slots
            .get_mut(&a.root)
            .filter(|slot| slot.active && slot.in_flight && slot.epoch == a.epoch)
        else {
            return false;
        };
        slot.unresolved = state.unresolved;
        let unchanged = slot.model_state.as_ref() == Some(&state.key);
        let due = state.unresolved
            && slot
                .last_model_assessment
                .is_none_or(|at| at.elapsed() >= UNRESOLVED_RECHECK);
        if unchanged && !a.continuation && slot.failures == 0 && !due {
            slot.progress = "state_unchanged".into();
            return false;
        }
        if !unchanged {
            slot.continued_observations.clear();
        }
        slot.pending_model_state = Some(state.key);
        slot.last_model_assessment = Some(Instant::now());
        true
    }
    pub(crate) fn epoch(&self, root: &str) -> Option<u64> {
        self.slots.get(root).map(|s| s.epoch)
    }
    pub(crate) fn wake_for_epoch(&mut self, root: &str, epoch: u64) -> Option<Assessment> {
        if self.epoch(root) != Some(epoch) {
            return None;
        }
        self.wake(root)
    }
    pub(crate) fn evidence(&mut self, a: &Assessment, key: String) {
        if let Some(s) = self
            .slots
            .get_mut(&a.root)
            .filter(|s| s.epoch == a.epoch && s.active)
        {
            s.evidence_key = key;
        }
    }
    pub(crate) fn progress(&mut self, a: &Assessment, stage: &str) {
        if let Some(s) = self
            .slots
            .get_mut(&a.root)
            .filter(|s| s.epoch == a.epoch && s.active)
        {
            s.progress = stage.into();
        }
    }
    pub(crate) fn failure(&mut self, a: &Assessment, code: &str) {
        if let Some(s) = self.slots.get_mut(&a.root).filter(|s| s.epoch == a.epoch) {
            s.last_failure = Some(if code == "assessment_failed" {
                format!("{code}:{}", s.progress)
            } else {
                code.into()
            });
        }
    }
    pub(crate) fn status(&self, root: &str) -> Value {
        self.slots.get(root).map_or_else(|| serde_json::json!({"state":"not_observed"}), |s| serde_json::json!({
            "state":if s.active {"active"} else {"stopped"}, "epoch":s.epoch,"inFlight":s.in_flight,
            "assessmentCountInBurst":s.budget,"consecutiveFailures":s.failures,
            // A successful handler's own result remains buffered as evidence,
            // but is not pending work until an external event arrives.
            "pendingEvents":if s.sequence > s.last_started_sequence || s.failures > 0 || (s.continuation_pending && s.budget < 8) {s.events.len()} else {0},
            "wakeOwned":s.wake_owned,"lastFailure":s.last_failure,"modelRequestProgress":s.progress,
            "renewalDelaySeconds":120,"failureLimit":3,"assessmentLimit":8
        }))
    }
    pub(crate) fn escalation_due(&self, a: &Assessment) -> bool {
        self.slots.get(&a.root).is_some_and(|s| {
            s.active
                && !s.in_flight
                && s.epoch == a.epoch
                && s.sequence == a.sequence
                && s.failures >= 3
        })
    }
    pub(crate) fn escalation_is_new(&mut self, a: &Assessment, binding: &str) -> bool {
        let Some(s) = self.slots.get_mut(&a.root) else {
            return false;
        };
        if !s.active
            || s.in_flight
            || s.epoch != a.epoch
            || s.sequence != a.sequence
            || s.failures < 3
        {
            return false;
        }
        let key = format!(
            "{}:{binding}:{}",
            s.last_failure.as_deref().unwrap_or("assessment_limit"),
            s.evidence_key
        );
        if s.last_escalation.as_ref() == Some(&key) {
            return false;
        }
        s.last_escalation = Some(key);
        true
    }
    /// A typed worker refusal proves no notice was queued. Keep ambiguous
    /// transport outcomes deduplicated, because they may have been admitted.
    pub(crate) fn escalation_refused(&mut self, a: &Assessment, binding: &str) {
        let Some(s) = self.slots.get_mut(&a.root) else {
            return;
        };
        if s.epoch != a.epoch || s.sequence != a.sequence {
            return;
        }
        let key = format!(
            "{}:{binding}:{}",
            s.last_failure.as_deref().unwrap_or("assessment_limit"),
            s.evidence_key
        );
        if s.last_escalation.as_ref() == Some(&key) {
            s.last_escalation = None;
        }
    }
    pub(crate) fn advice_is_new(&mut self, root: &str, key: String) -> bool {
        let Some(slot) = self.slots.get_mut(root) else {
            return false;
        };
        if slot.last_advice.as_ref() == Some(&key) {
            return false;
        }
        slot.last_advice = Some(key);
        true
    }
    /// A typed worker refusal proves the advisory was not admitted.
    pub(crate) fn advice_refused(&mut self, root: &str, key: &str) {
        if let Some(slot) = self.slots.get_mut(root) {
            if slot.last_advice.as_deref() == Some(key) {
                slot.last_advice = None;
            }
        }
    }
    pub(crate) fn current(&self, a: &Assessment) -> bool {
        self.slots
            .get(&a.root)
            .is_some_and(|s| s.active && s.in_flight && s.epoch == a.epoch)
    }
    pub(crate) fn finish(&mut self, a: &Assessment, success: bool) {
        if let Some(s) = self.slots.get_mut(&a.root).filter(|s| s.epoch == a.epoch) {
            s.in_flight = false;
            if success {
                if let Some(state) = s.pending_model_state.take() {
                    s.model_state = Some(state);
                }
                s.failures = 0;
                s.last_failure = None;
                s.last_escalation = None;
                // The burst budget bounds one observation/repair episode,
                // not the lifetime of a healthy event-driven observer.
                // Keep it across internal observations; renew only when the
                // successful episode has no pending internal continuation.
                if !s.continuation_pending {
                    s.budget = 0;
                }
            } else {
                s.pending_model_state = None;
                s.failures += 1;
                s.gap = true;
            }
        }
    }
    /// Authority went away after assessment began. Preserve genuine provider
    /// failures and the spent attempt, but discard stale pending work.
    pub(crate) fn withdraw(&mut self, a: &Assessment) {
        if self.slots.get(&a.root).is_some_and(|s| s.epoch == a.epoch) {
            self.suspend_ineligible(&a.root);
        }
    }
    /// An authority preflight refused the assessment before any provider work.
    /// Do not spend a retry, schedule a wake, or keep events from that state.
    pub(crate) fn cancel_ineligible(&mut self, a: &Assessment) {
        if self.slots.get(&a.root).is_some_and(|s| s.epoch == a.epoch) {
            if let Some(s) = self.slots.get_mut(&a.root) {
                s.budget = s.budget.saturating_sub(1);
            }
            self.suspend_ineligible(&a.root);
        }
    }
    /// Withdraw a root's queued Drone work without permanently stopping a later bind.
    pub(crate) fn suspend_ineligible(&mut self, root: &str) {
        if let Some(s) = self.slots.get_mut(root) {
            s.epoch += 1;
            s.in_flight = false;
            s.sequence = 0;
            s.last_started_sequence = 0;
            s.continuation_pending = false;
            s.continued_observations.clear();
            s.events.clear();
            s.gap = true;
            s.last_assessment = None;
            s.wake_owned = false;
            s.last_advice = None;
            s.evidence_key.clear();
            s.model_state = None;
            s.pending_model_state = None;
            s.last_model_assessment = None;
            s.unresolved = false;
            s.owned_outcome = None;
            s.owned_job_outcome = None;
            s.progress = "snapshot".into();
        }
    }
    /// Keep handler evidence without scheduling a new assessment on its own.
    pub(crate) fn result(&mut self, a: &Assessment, value: Value) {
        if let Some(s) = self
            .slots
            .get_mut(&a.root)
            .filter(|s| s.epoch == a.epoch && s.active)
        {
            if s.events.len() == EVENT_CAP {
                s.events.pop_front();
                s.gap = true;
            }
            s.events.push_back(value);
        }
    }
    /// A read-only observation may inform one next decision per handler key
    /// during this external-event episode. Repeating the same observation
    /// without a new external event cannot trigger an endless model loop.
    pub(crate) fn observation_result(&mut self, a: &Assessment, key: &str, value: Value) {
        self.result(a, value);
        if let Some(s) = self.slots.get_mut(&a.root).filter(|s| {
            s.epoch == a.epoch
                && s.active
                && s.budget < 8
                && s.continued_observations.len() < MAX_INTERNAL_CONTINUATIONS
        }) {
            s.continuation_pending |= s.continued_observations.insert(key.to_owned());
        }
    }
    pub(crate) fn stop(&mut self, root: &str) {
        if let Some(s) = self.slots.get_mut(root) {
            s.active = false;
            s.epoch += 1;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn healthy_external_episodes_do_not_exhaust_observer_or_alert_root() {
        let mut registry = DroneRegistry {
            cooldown: std::time::Duration::ZERO,
            ..DroneRegistry::default()
        };
        for index in 0..32 {
            let assessment = registry
                .push(
                    "root",
                    serde_json::json!({"type":"tool_result","index":index}),
                )
                .unwrap();
            registry.finish(&assessment, true);
            assert!(!registry.escalation_due(&assessment));
            assert!(!registry.escalation_is_new(&assessment, "bound"));
            assert_eq!(registry.status("root")["assessmentCountInBurst"], 0);
            assert!(registry.wake_delay("root").is_none());
        }
    }
    #[test]
    fn successful_observations_keep_the_same_finite_episode_budget() {
        let mut registry = DroneRegistry::default();
        let first = registry.push("root", Value::Null).unwrap();
        registry.observation_result(&first, "snapshot_branch", Value::Null);
        registry.finish(&first, true);
        assert_eq!(registry.status("root")["assessmentCountInBurst"], 1);
        let next = registry.wake("root").unwrap();
        registry.finish(&next, true);
        assert_eq!(registry.status("root")["assessmentCountInBurst"], 0);
        assert!(registry.wake_delay("root").is_none());
    }
    #[test]
    fn typed_advice_refusal_reopens_only_its_exact_fingerprint() {
        let mut registry = DroneRegistry::default();
        registry.push("root", Value::Null).unwrap();
        assert!(registry.advice_is_new("root", "hash:revision:step".into()));
        assert!(!registry.advice_is_new("root", "hash:revision:step".into()));
        registry.advice_refused("root", "different");
        assert!(!registry.advice_is_new("root", "hash:revision:step".into()));
        registry.advice_refused("root", "hash:revision:step");
        assert!(registry.advice_is_new("root", "hash:revision:step".into()));
    }
    #[test]
    fn model_page_cursor_advances_per_assessment_independently_of_event_sequence() {
        let mut registry = DroneRegistry {
            cooldown: std::time::Duration::ZERO,
            ..DroneRegistry::default()
        };
        let first = registry.push("root", Value::Null).unwrap();
        assert_eq!(first.page_cursor, 0);
        registry.finish(&first, true);
        let second = registry.push("root", Value::Null).unwrap();
        assert_eq!(second.page_cursor, 1);
        registry.finish(&second, true);
        let third = registry.push("root", Value::Null).unwrap();
        assert_eq!(third.page_cursor, 2);
    }
    #[test]
    fn drone_observation_continues_once_then_rearms_for_a_material_external_event() {
        let mut registry = DroneRegistry::default();
        let first = registry
            .push("root", serde_json::json!({"type":"message_end"}))
            .unwrap();
        assert!(registry.admit_model_state(
            &first,
            super::super::evidence::ModelState {
                key: "before-result".into(),
                unresolved: false
            }
        ));
        let result = serde_json::json!({"handler":"snapshot_branch","success":true});
        registry.observation_result(&first, "snapshot_branch", result.clone());
        registry.finish(&first, true);
        assert!(registry.wake_delay("root").is_some());
        let second = registry.wake("root").unwrap();
        assert!(registry.admit_model_state(
            &second,
            super::super::evidence::ModelState {
                key: "before-result".into(),
                unresolved: false
            }
        ));
        assert_eq!(second.events, vec![result.clone()]);
        registry.observation_result(&second, "snapshot_branch", result.clone());
        registry.finish(&second, true);
        assert_eq!(registry.status("root")["pendingEvents"], 0);
        assert!(registry.wake_delay("root").is_none());
        assert!(registry.wake("root").is_none());

        let external = serde_json::json!({"type":"tool_result"});
        assert!(registry.push("root", external.clone()).is_none());
        assert!(registry.wake_delay("root").is_some());
        let third = registry.wake("root").unwrap();
        assert!(registry.admit_model_state(
            &third,
            super::super::evidence::ModelState {
                key: "fresh-owned-result".into(),
                unresolved: false
            }
        ));
        assert_eq!(third.events, vec![result.clone(), external]);
        registry.observation_result(&third, "snapshot_branch", result.clone());
        registry.finish(&third, true);
        assert!(registry.wake_delay("root").is_some());
        let fourth = registry.wake("root").unwrap();
        assert!(registry.admit_model_state(
            &fourth,
            super::super::evidence::ModelState {
                key: "fresh-owned-result".into(),
                unresolved: false
            }
        ));
        assert_eq!(fourth.events, vec![result.clone()]);
        registry.observation_result(&fourth, "snapshot_branch", result);
        registry.finish(&fourth, true);
        assert_eq!(registry.status("root")["pendingEvents"], 0);
        assert!(registry.wake_delay("root").is_none());
    }

    #[test]
    fn drone_registered_check_continues_to_action_and_failure_still_retries() {
        let mut checking = DroneRegistry::default();
        let check = checking.push("check", Value::Null).unwrap();
        let evidence =
            serde_json::json!({"handler":"run_registered_check","result":{"ready":true}});
        checking.observation_result(
            &check,
            "run_registered_check:branch_facts",
            evidence.clone(),
        );
        checking.finish(&check, true);
        assert!(checking.wake_delay("check").is_some());
        let action = checking.wake("check").unwrap();
        assert_eq!(action.events, vec![evidence]);
        checking.result(
            &action,
            serde_json::json!({"handler":"recover_root_request","success":true}),
        );
        checking.finish(&action, true);
        assert!(checking.wake_delay("check").is_none());

        let mut failing = DroneRegistry::default();
        let failed = failing.push("other", Value::Null).unwrap();
        failing.result(&failed, serde_json::json!({"type":"assessment_failure"}));
        failing.finish(&failed, false);
        assert!(failing.wake_delay("other").is_some());
        let retry = failing.wake("other").unwrap();
        assert_eq!(retry.sequence, failed.sequence);
        assert_eq!(
            retry.events,
            vec![serde_json::json!({"type":"assessment_failure"})]
        );
    }
    #[test]
    fn drone_distinct_observations_have_one_finite_continuation_budget() {
        let mut registry = DroneRegistry::default();
        let mut assessment = registry.push("root", Value::Null).unwrap();
        for index in 0..MAX_INTERNAL_CONTINUATIONS {
            registry.observation_result(&assessment, &format!("observation-{index}"), Value::Null);
            registry.finish(&assessment, true);
            assert!(registry.wake_delay("root").is_some());
            assessment = registry.wake("root").unwrap();
        }
        registry.observation_result(&assessment, "one-more", Value::Null);
        registry.finish(&assessment, true);
        assert_eq!(registry.status("root")["pendingEvents"], 0);
        assert!(registry.wake_delay("root").is_none());

        let mut exhausted = DroneRegistry::default();
        let final_assessment = exhausted.push("exhausted", Value::Null).unwrap();
        exhausted.slots.get_mut("exhausted").unwrap().budget = 8;
        exhausted.observation_result(&final_assessment, "snapshot_branch", Value::Null);
        exhausted.finish(&final_assessment, true);
        assert!(exhausted.wake_delay("exhausted").is_none());
    }
    #[test]
    fn drone_external_event_during_assessment_is_not_lost() {
        let mut registry = DroneRegistry::default();
        let first = registry.push("root", Value::Null).unwrap();
        let external = serde_json::json!({"type":"tool_execution_end"});
        assert!(registry.push("root", external.clone()).is_none());
        let evidence = serde_json::json!({"handler":"snapshot_branch","success":true});
        registry.observation_result(&first, "snapshot_branch", evidence.clone());
        registry.finish(&first, true);
        assert!(registry.wake_delay("root").is_some());
        let next = registry.wake("root").unwrap();
        assert_eq!(next.events, vec![external, evidence]);
    }
    #[test]
    fn authority_preflight_does_not_spend_assessment_or_failure_budget() {
        let mut registry = DroneRegistry::default();
        let assessment = registry.push("root", Value::Null).unwrap();
        registry.cancel_ineligible(&assessment);
        let status = registry.status("root");
        assert_eq!(status["assessmentCountInBurst"], 0);
        assert_eq!(status["consecutiveFailures"], 0);
        assert_eq!(status["pendingEvents"], 0);
        assert_eq!(status["inFlight"], false);
        assert_eq!(status["wakeOwned"], false);
        assert!(!registry.escalation_is_new(&assessment, "bound"));
        assert!(registry.push("root", Value::Null).is_some());
    }
    #[test]
    fn canceled_authority_invalidates_an_old_scheduled_wake() {
        let mut registry = DroneRegistry::default();
        let assessment = registry.push("root", Value::Null).unwrap();
        registry.result(&assessment, Value::Null);
        registry.finish(&assessment, false);
        assert!(registry.wake_delay("root").is_some());
        let epoch = registry.epoch("root").unwrap();
        registry.suspend_ineligible("root");
        assert!(registry.wake_for_epoch("root", epoch).is_none());
        assert_eq!(registry.status("root")["wakeOwned"], false);
    }
    #[test]
    fn pause_preserves_provider_failures_without_escalating_stale_assessment() {
        let mut registry = DroneRegistry {
            cooldown: std::time::Duration::ZERO,
            ..DroneRegistry::default()
        };
        for _ in 0..2 {
            let assessment = registry.push("root", Value::Null).unwrap();
            registry.failure(&assessment, "model_failed");
            registry.finish(&assessment, false);
        }
        let pending = registry.push("root", Value::Null).unwrap();
        registry.cancel_ineligible(&pending);
        let status = registry.status("root");
        assert_eq!(status["consecutiveFailures"], 2);
        assert_eq!(status["assessmentCountInBurst"], 2);
        assert_eq!(status["pendingEvents"], 0);
        assert!(!registry.escalation_due(&pending));
        assert!(!registry.escalation_is_new(&pending, "binding"));
        let next = registry.push("root", Value::Null).unwrap();
        registry.failure(&next, "model_failed");
        registry.finish(&next, false);
        assert!(registry.escalation_due(&next));
        assert!(registry.escalation_is_new(&next, "binding"));
        registry.escalation_refused(&next, "binding");
        assert!(registry.escalation_is_new(&next, "binding"));
        assert!(!registry.escalation_is_new(&pending, "binding"));
    }
    #[test]
    fn withdrawn_action_keeps_spent_provider_budget_but_does_not_escalate() {
        let mut registry = DroneRegistry::default();
        let assessment = registry.push("root", Value::Null).unwrap();
        registry.withdraw(&assessment);
        let status = registry.status("root");
        assert_eq!(status["assessmentCountInBurst"], 1);
        assert_eq!(status["consecutiveFailures"], 0);
        assert_eq!(status["pendingEvents"], 0);
        assert!(!registry.escalation_due(&assessment));
        assert!(!registry.escalation_is_new(&assessment, "binding"));
    }
    #[test]
    fn assessor_failure_reuses_one_slot_and_bounds_restart_budget() {
        let mut registry = DroneRegistry::default();
        let first = registry.push("root", Value::Null).unwrap();
        registry.finish(&first, false);
        assert!(registry.push("root", Value::Null).is_none());
        for expected in [2, 3] {
            registry.slots.get_mut("root").unwrap().last_assessment = None;
            let next = registry.push("root", Value::Null).unwrap();
            assert_eq!(next.epoch, first.epoch);
            registry.finish(&next, false);
            assert_eq!(registry.slots["root"].failures, expected);
        }
        registry.slots.get_mut("root").unwrap().last_assessment = None;
        assert!(registry.push("root", Value::Null).is_none());
        assert_eq!(registry.slots.len(), 1);
    }
    #[test]
    fn renewal_keeps_failure_notice_fingerprint_and_stop_limits() {
        let mut r = DroneRegistry {
            cooldown: std::time::Duration::ZERO,
            ..DroneRegistry::default()
        };
        let mut latest = None;
        for _ in 0..3 {
            let a = r.push("root", Value::Null).unwrap();
            r.failure(&a, "missing_binding");
            r.finish(&a, false);
            latest = Some(a);
        }
        let latest = latest.unwrap();
        assert!(r.escalation_is_new(&latest, "0"));
        assert!(!r.escalation_is_new(&latest, "0"));
        assert!(r.push("root", Value::Null).is_none());
        assert_eq!(
            r.wake_delay("root"),
            Some(std::time::Duration::from_secs(120))
        );
        assert!(r.wake_delay("root").is_none());
        let a = r.wake("root").unwrap();
        assert_eq!(r.status("root")["assessmentCountInBurst"], 1);
        r.failure(&a, "missing_binding");
        r.finish(&a, false);
        let mut latest = None;
        for _ in 0..2 {
            let a = r.push("root", Value::Null).unwrap();
            r.finish(&a, false);
            latest = Some(a);
        }
        let latest = latest.unwrap();
        assert!(!r.escalation_is_new(&latest, "0"));
        assert!(r.escalation_is_new(&latest, "1"));
        let current = Assessment {
            root: "root".into(),
            epoch: 1,
            sequence: 0,
            page_cursor: 0,
            events: vec![],
            gap: false,
            continuation: false,
            owned_outcome: None,
            owned_job_outcome: None,
        };
        r.evidence(&current, "changed-file-hash".into());
        assert!(r.escalation_is_new(&latest, "1"));
        assert!(!r.escalation_is_new(&latest, "1"));
        r.stop("root");
        assert!(r.wake("root").is_none());
        assert!(!r.escalation_is_new(&latest, "2"));
    }
    #[test]
    fn successful_assessment_allows_a_new_failure_episode() {
        let mut r = DroneRegistry {
            cooldown: std::time::Duration::ZERO,
            ..DroneRegistry::default()
        };
        let mut latest = None;
        for _ in 0..3 {
            let a = r.push("root", Value::Null).unwrap();
            r.failure(&a, "model_missing");
            r.finish(&a, false);
            latest = Some(a);
        }
        let latest = latest.unwrap();
        assert!(r.escalation_is_new(&latest, "1"));
        assert!(!r.escalation_is_new(&latest, "1"));
        r.slots
            .get_mut("root")
            .unwrap()
            .events
            .push_back(Value::Null);
        let healthy = r.wake("root").unwrap();
        r.finish(&healthy, true);
        let mut latest = None;
        for _ in 0..3 {
            let a = r.push("root", Value::Null).unwrap();
            r.failure(&a, "model_missing");
            r.finish(&a, false);
            latest = Some(a);
        }
        assert!(r.escalation_is_new(&latest.unwrap(), "1"));
    }
    #[test]
    fn one_slot_overflow_and_stale_stop() {
        let mut r = DroneRegistry::default();
        let a = r.push("old", Value::Null).unwrap();
        for _ in 0..100 {
            assert!(r.push("old", Value::Null).is_none());
        }
        assert_eq!(r.slots["old"].events.len(), EVENT_CAP);
        assert!(r.slots["old"].gap);
        r.stop("old");
        assert!(!r.current(&a));
        r.finish(&a, true);
        assert!(r.push("old", Value::Null).is_none());
        assert!(r.push("new", Value::Null).is_some());
        assert_eq!(r.slots.len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn drone_material_gate_suppresses_noise_but_rechecks_unresolved_with_the_owned_wake() {
        let mut r = DroneRegistry {
            cooldown: std::time::Duration::ZERO,
            ..DroneRegistry::default()
        };
        let state = |key: &str, unresolved| super::super::evidence::ModelState {
            key: key.into(),
            unresolved,
        };
        let first = r
            .push("root", serde_json::json!({"type":"message_start"}))
            .unwrap();
        assert!(r.admit_model_state(&first, state("same", false)));
        r.finish(&first, true);
        for n in 0..500 {
            assert!(r
                .push(
                    "root",
                    serde_json::json!({"type":"message_update","delta":n})
                )
                .is_none());
        }
        assert!(r.wake_delay("root").is_none());
        let notice = r
            .push(
                "root",
                serde_json::json!({"type":"message_end","messageRole":"user","source":"drone"}),
            )
            .unwrap();
        assert!(!r.admit_model_state(&notice, state("same", false)));
        r.finish(&notice, true);
        assert!(r.wake_delay("root").is_none());
        let unknown = r
            .push(
                "root",
                serde_json::json!({"type":"tool_execution_end","isError":true}),
            )
            .unwrap();
        assert!(r.admit_model_state(&unknown, state("new-unresolved", true)));
        r.finish(&unknown, true);
        let delay = r.wake_delay("root").unwrap();
        assert_eq!(delay, UNRESOLVED_RECHECK);
        // No new event arrives. The one existing wake remains sufficient.
        tokio::time::advance(delay).await;
        let retry = r.wake("root").unwrap();
        assert!(r.admit_model_state(&retry, state("new-unresolved", true)));
        r.finish(&retry, false);
        assert_eq!(r.wake_delay("root").unwrap(), std::time::Duration::ZERO); // original provider retry
        let recovered = r.wake("root").unwrap();
        assert!(r.admit_model_state(&recovered, state("new-unresolved", true)));
        r.finish(&recovered, true);
        assert_eq!(r.wake_delay("root").unwrap(), UNRESOLVED_RECHECK);
        let healthy = r
            .push("root", serde_json::json!({"type":"message_end"}))
            .unwrap();
        assert!(r.admit_model_state(&healthy, state("healthy", false)));
        r.finish(&healthy, true);
        // Retire the already-owned old wake; no healthy replacement is scheduled.
        tokio::time::advance(UNRESOLVED_RECHECK).await;
        assert!(r.wake("root").is_none());
        assert!(r.wake_delay("root").is_none());
    }

    #[test]
    fn drone_material_gate_keeps_bounded_observation_and_inflight_new_results() {
        let mut r = DroneRegistry {
            cooldown: std::time::Duration::ZERO,
            ..DroneRegistry::default()
        };
        let state = |key: &str| super::super::evidence::ModelState {
            key: key.into(),
            unresolved: false,
        };
        let first = r.push("root", Value::Null).unwrap();
        assert!(r.admit_model_state(&first, state("a")));
        r.observation_result(
            &first,
            "inspect_owned_process",
            serde_json::json!({"handler":"inspect_owned_process","result":"new fact"}),
        );
        // An external event must not erase the owed observation or its bounded key.
        assert!(r
            .push(
                "root",
                serde_json::json!({"type":"message_end","messageRole":"user"})
            )
            .is_none());
        r.finish(&first, true);
        let continued = r.wake("root").unwrap();
        assert!(continued.continuation);
        assert!(r.admit_model_state(&continued, state("a")));
        r.observation_result(&continued, "inspect_owned_process", Value::Null);
        r.finish(&continued, true);
        assert!(r.wake_delay("root").is_none());
        let busy = r.push("root", Value::Null).unwrap();
        assert!(!r.admit_model_state(&busy, state("a")));
        assert!(r.push("root",serde_json::json!({"type":"tool_execution_end","isError":true,"evidenceSha256":"fresh-result"})).is_none());
        r.finish(&busy, true);
        let result = r.wake("root").unwrap();
        assert!(result.owned_outcome.is_some());
        assert!(r.admit_model_state(&result, state("b")));
        r.finish(&result, false);
        let retry = r.wake("root").unwrap();
        assert!(r.admit_model_state(&retry, state("b")));
        r.finish(&retry, true);
        let result=r.push("root",serde_json::json!({"type":"message_end","messageRole":"assistant",
            "messageId":"one","untrustedEvidence":"timestamp1","evidenceSha256":"same-visible-result"})).unwrap();
        let job = result.owned_job_outcome.clone();
        assert!(job.is_some());
        let key = result.owned_outcome.clone();
        r.finish(&result, true);
        let duplicate=r.push("root",serde_json::json!({"type":"message_end","messageRole":"assistant",
            "messageId":"two","untrustedEvidence":"timestamp2","evidenceSha256":"same-visible-result"})).unwrap();
        assert_eq!(duplicate.owned_job_outcome, job);
        assert_eq!(duplicate.owned_outcome, key);
        r.finish(&duplicate, true);
        let changed = r
            .push(
                "root",
                serde_json::json!({"type":"message_end","messageRole":"assistant",
            "evidenceSha256":"new-visible-failure"}),
            )
            .unwrap();
        assert_ne!(changed.owned_outcome, key);
    }

    #[tokio::test(start_paused = true)]
    async fn drone_idle_open_plan_with_passivated_owners_and_ready_zero_is_not_blinded_by_null() {
        let snapshot = serde_json::json!({"humanInputFingerprint":"scope","request":null,"tools":[],
            "hasBackgroundBash":false,"hasRunningChildren":false,"compacting":false,
            "goal":{"active":true,"status":"active"},"queuedInputs":0,"pendingHumanInputs":0});
        let roadmap = serde_json::json!({"structured":{"revision":"studio:4943","userStopped":false,
            "steps":[{"id":"accepted-result","status":"done"},{"id":"F4-successor","status":"unassessed"}]}});
        let gate = serde_json::json!({"complete":true,"unknown":0,"nextWork":{"complete":true,"runtime":{
            "complete":true,"freeSlots":2,"ready":[],"ownership":[],"awaitingAcceptance":[]}}});
        let children = serde_json::json!({"eventSequence":80,"children":[]});
        let state = || {
            super::super::evidence::model_state(
                &snapshot,
                &roadmap,
                Some(&gate),
                Some(&children),
                None,
            )
        };
        assert!(state().unresolved);
        let mut r = DroneRegistry::default();
        let first = r
            .push("root", serde_json::json!({"type":"agent_end"}))
            .unwrap();
        assert!(r.admit_model_state(&first, state()));
        r.finish(&first, true); // model returned null
        let delay = r.wake_delay("root").unwrap();
        let wake = tokio::time::sleep(delay);
        tokio::pin!(wake);
        tokio::time::advance(std::time::Duration::from_secs(299)).await;
        assert!(!wake.is_elapsed());
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        wake.await;
        let next = r.wake("root").unwrap(); // no event, no child, no READY item
        assert!(r.admit_model_state(&next, state()));
        r.finish(&next, true);
        assert_eq!(r.wake_delay("root").unwrap(), UNRESOLVED_RECHECK);
        r.stop("root");
        tokio::time::advance(UNRESOLVED_RECHECK).await;
        assert!(r.wake("root").is_none());
    }
}
