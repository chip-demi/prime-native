use super::super::{create_goal_context_message, now_millis, GoalBranchReload, GoalDriver};
use super::*;
use pa_types::ai::UserContent;
use serde_json::json;
use std::fmt::Write as _;

struct Fixture {
    dir: tempfile::TempDir,
    session: SessionManager,
    driver: GoalDriver,
    timestamp: i64,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut session = SessionManager::in_memory(dir.path());
        let mut driver = GoalDriver::new();
        driver
            .start(&mut session, "Write a complete explanation", Some(100_000))
            .unwrap();
        let timestamp = i64::try_from(driver.state().created_at.unwrap()).unwrap();
        Self {
            dir,
            session,
            driver,
            timestamp,
        }
    }

    fn input(&mut self, kind: &str) {
        match kind {
            "goal" => {
                let row = create_goal_context_message(
                    self.driver.state(),
                    GoalContextKind::Continuation,
                    &[],
                )
                .unwrap();
                self.session
                    .append_custom_message(&row.custom_type, row.content, row.display, row.details)
                    .unwrap();
            }
            "wake" => {
                self.session.append_custom_message(super::super::GOAL_BACKOFF_WAKE_CUSTOM_TYPE,
                    UserContent::Text("unchanged wake content".into()), true,
                    Some(json!({"source":super::super::GOAL_BACKOFF_WAKE_CUSTOM_TYPE,
                        "label":super::super::GOAL_BACKOFF_WAKE_CRON_LABEL,"jobId":"existing-job"}))).unwrap();
            }
            "user" | "cron" => {
                self.session.append_message(serde_json::from_value(json!({
                    "role":"user","content":format!("fresh {kind} input"),"timestamp":self.timestamp
                })).unwrap()).unwrap();
            }
            "child" | "job" | "advice" => {
                let custom_type = match kind {
                    "child" => "rlm_child_terminal_notice",
                    "job" => crate::session_engine::messages::ASYNC_BASH_COMPLETION_CUSTOM_TYPE,
                    "advice" => "drone_diagnostic",
                    _ => unreachable!(),
                };
                self.session
                    .append_custom_message(
                        custom_type,
                        UserContent::Text("fresh fact".into()),
                        true,
                        Some(json!({"operationId":"owned"})),
                    )
                    .unwrap();
            }
            "tool" => {
                self.session
                    .append_message(
                        serde_json::from_value(json!({
                            "role":"toolResult","toolCallId":"owned-call","toolName":"python_repl",
                            "content":[{"type":"text","text":"actual result"}],"isError":false,
                            "timestamp":self.timestamp
                        }))
                        .unwrap(),
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
    }

    fn reply(&mut self, text: &str, thinking: &str) -> pa_agent::types::AssistantMessage {
        self.timestamp += 1;
        let mut content = Vec::new();
        if !thinking.is_empty() {
            content.push(json!({"type":"thinking","thinking":thinking}));
        }
        content.push(json!({"type":"text","text":text}));
        let value = json!({"role":"assistant","content":content,"api":"openai-completions",
            "provider":"faux","model":"fixture","stopReason":"stop","timestamp":self.timestamp,
            "usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,
                "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}}});
        let turn = serde_json::from_value(value.clone()).unwrap();
        self.session
            .append_message(serde_json::from_value(value).unwrap())
            .unwrap();
        turn
    }

    fn consult(&mut self, turn: &pa_agent::types::AssistantMessage) -> bool {
        let minted = self
            .driver
            .next_continuation_message(&mut self.session, Some(turn))
            .unwrap()
            .is_some();
        if minted {
            self.driver.continuation_consumed();
        }
        minted
    }

    fn arm(&mut self) -> pa_agent::types::AssistantMessage {
        let mut last = None;
        for index in 0..3 {
            self.input("goal");
            let turn = self.reply(&format!("new text {index}"), &format!("thinking {index}"));
            assert_eq!(self.consult(&turn), index < 2);
            last = Some(turn);
        }
        last.unwrap()
    }
}

#[test]
fn differing_goal_only_replies_back_off_without_failing_or_spending_a_mint() {
    let mut f = Fixture::new();
    for (index, delay) in [0, 0, 10_000, 20_000, 40_000, 60_000, 60_000]
        .into_iter()
        .enumerate()
    {
        f.input(if index == 3 { "wake" } else { "goal" });
        let turn = f.reply(
            &format!("Novel output number {index}"),
            &format!("different reasoning {index}"),
        );
        let before = f.driver.state().clone();
        let start = now_millis();
        assert_eq!(f.consult(&turn), delay == 0);
        if delay != 0 {
            let until = f.driver.text_cadence.until_ms;
            assert!((start + delay..=now_millis() + delay).contains(&until));
            assert_eq!(
                f.driver.state(),
                &before,
                "cadence is ephemeral, not goal failure or usage"
            );
            assert!(!f.consult(&turn));
            assert_eq!(
                f.driver.text_cadence.until_ms, until,
                "a consult never extends the window"
            );
            f.driver.text_cadence.until_ms = 0;
            assert!(
                f.consult(&turn),
                "novel text-only work keeps continuing when due"
            );
        }
        assert_eq!(f.driver.state().status, crate::goals::GoalStatus::Active);
        assert_eq!(f.driver.no_progress_streak, 0);
        assert_eq!(f.driver.state().token_budget, Some(100_000));
        assert_eq!(f.driver.state().tokens_used, 0);
        assert_eq!(
            f.driver.state().continuations_used,
            u64::try_from(index + 1).unwrap()
        );
    }
}

#[test]
fn claiming_a_cadence_wake_keeps_the_guard_and_due_mints_only_once() {
    let mut f = Fixture::new();
    let turn = f.arm();
    let until = f.driver.text_cadence.until_ms;
    let before = f.driver.state().clone();
    assert_eq!(f.driver.take_backoff_wake_at(), Some(until));
    assert_eq!(f.driver.take_backoff_wake_at(), None);
    assert_eq!(f.driver.text_cadence.until_ms, until);
    assert!(!f.consult(&turn));
    assert_eq!(f.driver.state(), &before);
    assert_eq!(f.driver.take_backoff_wake_at(), None);
    // Advance the owned deadline rather than sleeping or relying on scheduling.
    let due = now_millis().saturating_sub(1);
    f.driver.text_cadence.until_ms = due;
    f.driver.text_cadence.claimed_until_ms = due;
    assert_eq!(f.driver.take_backoff_wake_at(), None);
    assert!(f
        .driver
        .next_continuation_message(&mut f.session, Some(&turn))
        .unwrap()
        .is_some());
    assert!(f
        .driver
        .next_continuation_message(&mut f.session, Some(&turn))
        .unwrap()
        .is_none());
    assert_eq!(
        f.driver.state().continuations_used,
        before.continuations_used + 1
    );
}

#[test]
fn fresh_human_child_tool_and_job_inputs_reset_an_armed_cadence() {
    for kind in ["user", "cron", "child", "tool", "job"] {
        let mut f = Fixture::new();
        let turn = f.arm();
        let before = f.driver.state().clone();
        assert!(f.driver.take_backoff_wake_at().is_some());
        f.input(kind);
        assert!(f.consult(&turn), "fresh {kind} is immediately admissible");
        assert_eq!(f.driver.text_cadence.until_ms, 0);
        assert_eq!(f.driver.text_cadence.streak, 0);
        assert_eq!(f.driver.state().goal_id, before.goal_id);
        assert_eq!(f.driver.state().tokens_used, before.tokens_used);
        assert_eq!(f.driver.state().token_budget, before.token_budget);
        assert_eq!(
            f.driver.state().continuations_used,
            before.continuations_used + 1
        );
    }
}

#[test]
fn fresh_input_in_the_same_batch_and_a_tool_in_the_turn_reset_cadence() {
    for kind in ["user", "child", "tool"] {
        let mut f = Fixture::new();
        f.arm();
        f.input("goal");
        f.input(kind);
        f.input("wake");
        let turn = f.reply("new final text after real input", "new thinking");
        assert!(
            f.consult(&turn),
            "a later synthetic row must not hide {kind}"
        );
        assert_eq!(f.driver.text_cadence.streak, 0);
    }
}

#[test]
fn a_persisted_assistant_tool_call_resets_text_cadence_without_a_result_row() {
    let mut f = Fixture::new();
    f.arm();
    f.input("goal");
    let before = f.driver.state().clone();
    let mut turn = f.reply("taking an owned action", "");
    turn.content = vec![pa_agent::types::AssistantContent::ToolCall(
        pa_agent::types::ToolCall {
            id: "owned-action".into(),
            name: "python_repl".into(),
            arguments: json!({"code":"owned_action()"}),
            thought_signature: None,
        },
    )];
    turn.stop_reason = pa_agent::types::StopReason::ToolUse;
    let mut row = serde_json::to_value(&turn).unwrap();
    row["role"] = json!("assistant");
    f.session
        .append_message(serde_json::from_value(row).unwrap())
        .unwrap();
    assert!(
        f.consult(&turn),
        "an actual persisted action clears text-only cadence"
    );
    assert_eq!(f.driver.text_cadence.until_ms, 0);
    assert_eq!(f.driver.text_cadence.streak, 0);
    assert_eq!(
        f.driver.state().no_progress_streak,
        before.no_progress_streak
    );
    assert_eq!(f.driver.state().goal_id, before.goal_id);
    assert_eq!(f.driver.state().token_budget, before.token_budget);
    assert_eq!(f.driver.state().tokens_used, before.tokens_used);
    assert_eq!(
        f.driver.state().continuations_used,
        before.continuations_used + 1
    );
}

#[tokio::test]
async fn a_lazy_recovered_goal_uses_fresh_retained_turns_without_hydrating_history() {
    let mut f = Fixture::new();
    let dir = f.dir.path().to_owned();
    let path = dir.join("lazy.jsonl");
    let rows = [
        json!({"type":"session","version":3,"id":"lazy-cadence","cwd":dir,"timestamp":"2026-01-01T00:00:00Z"}),
        json!({"type":"custom","id":"goal","parentId":null,"customType":GOAL_STATE_CUSTOM_TYPE,"data":f.driver.state()}),
        json!({"type":"message","id":"old","parentId":"goal","message":{"role":"user","content":"old history","timestamp":0}}),
        json!({"type":"message","id":"kept","parentId":"old","message":{"role":"user","content":"retained","timestamp":0}}),
        json!({"type":"compaction","id":"compact","parentId":"kept","summary":"summary","firstKeptEntryId":"kept","tokensBefore":1000}),
    ];
    let original = rows.iter().fold(String::new(), |mut output, row| {
        writeln!(output, "{row}").unwrap();
        output
    });
    std::fs::write(&path, &original).unwrap();
    f.session = SessionManager::open_windowed(&dir, &dir, &path)
        .await
        .unwrap();
    f.driver = GoalDriver::load_persisted(&f.session);
    assert!(!f.session.is_full_history());
    f.input("user");
    let turn = f.reply("fresh accepted user work", "");
    assert!(f.consult(&turn));
    let turn = f.arm();
    assert!(!f.consult(&turn));
    assert!(f.driver.backoff_wake_at().is_some());
    assert!(
        !f.session.is_full_history(),
        "no cadence consult hydrates old history"
    );
    f.input("child");
    assert!(f.consult(&turn));
    assert_eq!(f.driver.text_cadence.until_ms, 0);
    assert!(!f.session.is_full_history());
    assert!(std::fs::read_to_string(path)
        .unwrap()
        .starts_with(&original));
}

#[test]
fn a_turn_older_than_the_retained_scan_bound_clears_cadence_conservatively() {
    let mut f = Fixture::new();
    let turn = f.arm();
    for _ in 0..64 {
        f.session
            .append_custom_entry(GOAL_STATE_CUSTOM_TYPE, None)
            .unwrap();
    }
    assert!(f.consult(&turn));
    assert_eq!(f.driver.text_cadence.until_ms, 0);
    assert_eq!(f.driver.text_cadence.streak, 0);
    assert_eq!(f.driver.state().status, crate::goals::GoalStatus::Active);
}

#[test]
fn state_context_and_own_advice_do_not_reset_the_window() {
    let mut f = Fixture::new();
    let turn = f.arm();
    let until = f.driver.text_cadence.until_ms;
    let streak = f.driver.text_cadence.streak;
    f.input("advice");
    f.input("goal");
    f.session
        .append_custom_entry(
            GOAL_STATE_CUSTOM_TYPE,
            Some(serde_json::to_value(f.driver.state()).unwrap()),
        )
        .unwrap();
    assert!(!f.consult(&turn));
    assert_eq!(
        (f.driver.text_cadence.streak, f.driver.text_cadence.until_ms),
        (streak, until)
    );
    // The actual typed scan, not a prose/hash classifier, sees the same turn.
    assert!(matches!(
        turn_evidence(&f.session, f.driver.state(), &turn),
        Evidence::GoalTurn(_)
    ));
}

#[test]
fn same_timestamp_rows_and_thinking_only_turns_are_distinct_automatic_turns() {
    let mut f = Fixture::new();
    let timestamp = f.timestamp;
    for index in 0..3 {
        f.timestamp = timestamp;
        f.input("goal");
        let turn = f.reply("", &format!("different hidden thought {index}"));
        assert_eq!(f.consult(&turn), index < 2);
    }
    assert_eq!(f.driver.text_cadence.streak, 3);
    assert_eq!(f.driver.no_progress_streak, 0);
}

#[test]
fn reload_preserves_same_timeline_but_owner_and_objective_changes_reset() {
    let mut f = Fixture::new();
    let turn = f.arm();
    let until = f.driver.text_cadence.until_ms;
    f.driver
        .reload_from_branch(&f.session, GoalBranchReload::SameTimeline);
    assert_eq!(f.driver.text_cadence.until_ms, until);
    assert!(!f.consult(&turn));
    let goal = f.driver.state().clone();
    f.driver
        .edit_objective(
            &mut f.session,
            goal.goal_id.as_deref().unwrap(),
            goal.objective.as_deref().unwrap(),
            "Fresh objective",
        )
        .unwrap();
    assert_eq!(f.driver.text_cadence.until_ms, 0);
    assert_eq!(f.driver.state().goal_id, goal.goal_id);
    assert_eq!(f.driver.state().token_budget, goal.token_budget);
    assert_eq!(f.driver.state().tokens_used, goal.tokens_used);
    f.driver
        .pause(&mut f.session, "Actual owner pause")
        .unwrap();
    assert!(f
        .driver
        .next_continuation_message(&mut f.session, Some(&turn))
        .unwrap()
        .is_none());
    assert_eq!(f.driver.backoff_wake_at(), None);
    f.driver.resume(&mut f.session).unwrap();
    assert_eq!(f.driver.text_cadence.until_ms, 0);
    assert_eq!(f.driver.state().goal_id, goal.goal_id);
    f.driver
        .reload_from_branch(&f.session, GoalBranchReload::FaithfulBranch);
    assert_eq!(f.driver.text_cadence.until_ms, 0);
}

#[test]
fn empty_output_policy_keeps_its_original_cap_after_text_cadence() {
    let mut f = Fixture::new();
    f.arm();
    for strike in 1..=3 {
        f.input("goal");
        let turn = f.reply("", "");
        assert!(!f.consult(&turn));
        assert_eq!(f.driver.no_progress_streak, strike);
        assert_eq!(f.driver.text_cadence.until_ms, 0);
    }
    assert_eq!(f.driver.state().status, crate::goals::GoalStatus::Error);
    assert_eq!(
        f.driver.state().last_reason.as_deref(),
        Some(super::super::CONTINUATION_NO_PROGRESS_CAP_REASON)
    );
}

#[test]
fn quota_park_clears_cadence_without_scheduling_a_probe_or_spending_empty_strikes() {
    let mut f = Fixture::new();
    f.arm();
    let mut turn = f.reply("", "");
    turn.stop_reason = pa_agent::types::StopReason::Error;
    turn.error_message = Some("429 quota parked".into());
    turn.diagnostics = Some(vec![pa_agent::types::AssistantMessageDiagnostic {
        kind: "provider_stream_failure".into(),
        timestamp: 0,
        error: None,
        details: Some(json!({"kind":"rate_limit","status":429})),
    }]);
    let mut persisted = serde_json::to_value(&turn).unwrap();
    persisted["role"] = json!("assistant");
    f.session
        .append_message(serde_json::from_value(persisted).unwrap())
        .unwrap();
    assert!(!f.consult(&turn));
    assert_eq!(f.driver.state().status, crate::goals::GoalStatus::Active);
    assert_eq!(f.driver.no_progress_streak, 0);
    assert_eq!(f.driver.backoff_wake_at(), None);
    assert_eq!(f.driver.take_backoff_wake_at(), None);
    assert_eq!(f.driver.text_cadence.until_ms, 0);
}
