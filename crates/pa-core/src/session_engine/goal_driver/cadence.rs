//! A bounded cadence for automatic turns that admitted no new input or action.
//! This deliberately slows even novel text-only work; it never judges its meaning,
//! pauses a goal, changes accounting, or spends the empty-output error budget.

use super::{GoalContextKind, GoalState, GOAL_STATE_CUSTOM_TYPE};
use crate::goals::GoalContextDetails;
use crate::session::manager::SessionManager;
use pa_types::session::{AgentMessage, FileEntry};

#[derive(Default)]
pub(super) struct TextCadence {
    scope: Option<(String, String)>,
    last_turn: Option<String>,
    streak: u32,
    pub(super) until_ms: u64,
    pub(super) claimed_until_ms: u64,
}

pub(super) enum Evidence {
    GoalTurn(String),
    MaterialInput,
    ObserveOnly,
}

impl TextCadence {
    pub(super) fn clear(&mut self) {
        *self = Self::default();
    }

    pub(super) fn deadline(&self, goal: &GoalState) -> u64 {
        self.scope
            .as_ref()
            .filter(|(id, objective)| {
                goal.goal_id.as_ref() == Some(id) && goal.objective.as_ref() == Some(objective)
            })
            .map_or(0, |_| self.until_ms)
    }

    pub(super) fn observe(&mut self, goal: &GoalState, evidence: Evidence, now: u64) {
        let (Some(id), Some(objective)) = (&goal.goal_id, &goal.objective) else {
            self.clear();
            return;
        };
        if !self
            .scope
            .as_ref()
            .is_some_and(|(current_id, current_objective)| {
                current_id == id && current_objective == objective
            })
        {
            self.clear();
            self.scope = Some((id.clone(), objective.clone()));
        }
        match evidence {
            Evidence::MaterialInput => self.clear(),
            Evidence::GoalTurn(turn) if self.last_turn.as_ref() != Some(&turn) => {
                self.last_turn = Some(turn);
                self.streak = self.streak.saturating_add(1);
                if self.streak >= 3 {
                    let delay =
                        (10_000u64 * 2u64.pow(self.streak.saturating_sub(3).min(3))).min(60_000);
                    self.until_ms = now.saturating_add(delay);
                    self.claimed_until_ms = 0;
                }
            }
            Evidence::ObserveOnly | Evidence::GoalTurn(_) => {}
        }
    }
}

enum Input {
    Automatic,
    Noise,
    Material,
}

fn custom_input(kind: &str, details: Option<&serde_json::Value>, goal: &GoalState) -> Input {
    match kind {
        "goal_context" => {
            match details
                .cloned()
                .and_then(|value| serde_json::from_value::<GoalContextDetails>(value).ok())
            {
                Some(context)
                    if context.kind == GoalContextKind::Continuation
                        && context.goal_id == goal.goal_id
                        && Some(&context.objective) == goal.objective.as_ref() =>
                {
                    Input::Automatic
                }
                _ => Input::Material,
            }
        }
        super::GOAL_BACKOFF_WAKE_CUSTOM_TYPE
            if details.is_some_and(|details| {
                details["source"] == super::GOAL_BACKOFF_WAKE_CUSTOM_TYPE
                    && details["label"] == super::GOAL_BACKOFF_WAKE_CRON_LABEL
            }) =>
        {
            Input::Automatic
        }
        "drone_diagnostic" | "heartbeat_prompt" | "harness_digest" => Input::Noise,
        _ => Input::Material,
    }
}

/// Inspect at most one retained turn/batch. Unknown or truncated provenance fails
/// open for cadence; no whole-history hydration, prose classification or polling.
pub(super) fn turn_evidence(
    session: &SessionManager,
    goal: &GoalState,
    turn: &pa_agent::types::AssistantMessage,
) -> Evidence {
    let Ok(timestamp) = u64::try_from(turn.timestamp) else {
        return Evidence::MaterialInput;
    };
    let mut next = session.get_leaf_id();
    let retained = session.retained_entries();
    let recent = &retained[retained.len().saturating_sub(64)..];
    let mut seen = std::collections::HashSet::new();
    let mut current = None;
    let mut automatic = false;
    for _ in 0..64 {
        let Some(id) = next else {
            return match current {
                Some(id) if automatic => Evidence::GoalTurn(id),
                Some(_) => Evidence::ObserveOnly,
                None => Evidence::MaterialInput,
            };
        };
        if !seen.insert(id) {
            return Evidence::MaterialInput;
        }
        let Some(entry) = recent.iter().rev().find(|entry| entry.id() == Some(id)) else {
            return Evidence::MaterialInput;
        };
        next = entry.parent_id();
        let input = match entry {
            FileEntry::Message {
                message: AgentMessage::Assistant(message),
                ..
            } => {
                if message
                    .content
                    .iter()
                    .any(|part| matches!(part, pa_types::ai::AssistantContentBlock::ToolCall(_)))
                {
                    return Evidence::MaterialInput;
                }
                if current.is_some() {
                    return current
                        .filter(|_| automatic)
                        .map_or(Evidence::ObserveOnly, Evidence::GoalTurn);
                }
                if message.timestamp != timestamp {
                    return Evidence::MaterialInput;
                }
                current = Some(id.to_string());
                continue;
            }
            FileEntry::CustomMessage { payload, .. } => {
                custom_input(&payload.custom_type, payload.details.as_ref(), goal)
            }
            FileEntry::Message {
                message: AgentMessage::Custom(message),
                ..
            } => custom_input(&message.custom_type, message.details.as_ref(), goal),
            FileEntry::Custom { payload, .. } if payload.custom_type == GOAL_STATE_CUSTOM_TYPE => {
                Input::Noise
            }
            FileEntry::ToolProgress { .. }
            | FileEntry::ChildUsageAttributed { .. }
            | FileEntry::Label { .. }
            | FileEntry::SessionInfo { .. }
            | FileEntry::SessionState { .. }
            | FileEntry::GitState { .. }
            | FileEntry::Header { .. } => Input::Noise,
            FileEntry::Message { .. }
            | FileEntry::ToolStarted { .. }
            | FileEntry::ModelChange { .. }
            | FileEntry::ThinkingLevelChange { .. }
            | FileEntry::ServiceTierChange { .. }
            | FileEntry::Custom { .. }
            | FileEntry::Compaction { .. }
            | FileEntry::BranchSummary { .. }
            | FileEntry::Unknown { .. } => Input::Material,
        };
        match input {
            Input::Material => return Evidence::MaterialInput,
            Input::Automatic if current.is_some() => automatic = true,
            Input::Automatic | Input::Noise => {}
        }
    }
    Evidence::MaterialInput
}

#[cfg(test)]
#[path = "cadence_tests.rs"]
mod tests;
