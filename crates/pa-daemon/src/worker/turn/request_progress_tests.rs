//! Request telemetry shares a finite queue with terminal turn events.
use super::*;
use pa_agent::agent_loop::request::RequestSnapshot;

fn snapshot() -> RequestSnapshot {
    RequestSnapshot {
        request_id: 1,
        revision: 1,
        phase: "transport".into(),
        elapsed_ms: 0,
        last_progress_ms: 0,
        deadline_ms: Some(120_000),
        operation_id: None,
    }
}

#[test]
fn request_progress_burst_preserves_child_notice_and_turn_boundaries_once() {
    let mut cadence = RequestProgressCadence::default();
    let pump = EventPump::new();
    let mut subscription = pump.subscribe();
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Arc<Value>>(
        crate::backpressure::TARGETED_EVENT_QUEUE_CAPACITY,
    );
    let emit = |event: Value| {
        pump.send(OutboundFrame::session_event(
            serde_json::to_vec(&json!({
                "type": "session_event",
                "activeSessionId": "root",
                "event": event,
            }))
            .unwrap(),
        ));
    };
    let now = Instant::now();
    let mut request = snapshot();
    // Leave the actual EventPump and targeted queue undrained while the
    // provider floods progress. Without coalescing this loses frames.
    let mut published = 0;
    for revision in 1..=16_384 {
        request.revision = revision;
        request.elapsed_ms = revision;
        if cadence.should_publish(&request, now) {
            emit(json!({"type": "drone_request_progress", "request": request}));
            published += 1;
        }
    }
    let terminal = [
        json!({
            "type": "message_end",
            "message": {
                "role": "custom",
                "customType": "rlm_child_terminal_notice",
                "content": "[child-exited: no-reply child:lane]",
                "display": true,
                "details": {
                    "kind": "completed_without_reply",
                    "childId": "sub-1",
                    "sessionName": "lane",
                },
            },
        }),
        json!({"type": "turn_end"}),
        json!({"type": "agent_end"}),
    ];
    for event in &terminal {
        emit(event.clone());
    }
    // Same finite worker pump and Full-drop targeted queue used by routing.
    // A slow reader consumes neither until every turn frame is published.
    while !subscription.is_empty() {
        let frame = subscription
            .try_recv()
            .expect("request progress must not lag the pump");
        let payload: Value = serde_json::from_slice(&frame.payload).unwrap();
        let _ = sender.try_send(Arc::new(payload));
    }
    let mut semantic = Vec::new();
    while let Ok(payload) = receiver.try_recv() {
        if payload["event"]["type"] != "drone_request_progress" {
            semantic.push(payload["event"].clone());
        }
    }
    assert_eq!(published, 1);
    assert_eq!(
        semantic, terminal,
        "one child completion notice and both turn boundaries must survive once, in order"
    );
    // Sampling does not leave the next public snapshot stuck on an old revision.
    assert!(cadence.should_publish(&request, now + crate::streaming::UPDATE_FLUSH_INTERVAL));
    assert_eq!(cadence.last.as_ref().unwrap().0, request);
}

#[test]
fn request_progress_phase_operation_deadline_and_new_request_bypass_coalescing() {
    let mut cadence = RequestProgressCadence::default();
    let now = Instant::now();
    let mut request = snapshot();
    assert!(cadence.should_publish(&request, now));
    request.revision += 1;
    request.elapsed_ms += 1;
    assert!(!cadence.should_publish(&request, now));
    // A policy edit can be the last callback before a stalled provider. It
    // must publish immediately, without waiting for another callback/timer.
    request.deadline_ms = Some(90_000);
    assert!(cadence.should_publish(&request, now));
    request.deadline_ms = None;
    assert!(cadence.should_publish(&request, now));
    request.phase = "emitter".into();
    assert!(cadence.should_publish(&request, now));
    request.operation_id = Some("recovery-1".into());
    assert!(cadence.should_publish(&request, now));
    request.request_id += 1;
    request.phase = "transport".into();
    request.operation_id = None;
    assert!(cadence.should_publish(&request, now));
    assert_eq!(cadence.last.as_ref().unwrap().0, request);
}
