use super::*;
#[tokio::test]
async fn live_user_provenance_is_atomic_with_consumed_rows_and_restores_by_id() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("session.jsonl");
    let engine: Arc<dyn SessionEngine> =
        Arc::new(ScriptedEngine::from_value(&json!({"responses":["reply"]})).unwrap());
    let runner = burst_runner(engine.clone());
    let mut store = SessionFile::create("/fixture", None, 0);
    store.path = path.clone();
    runner.core.lock().unwrap().store = Some(store);
    let items = [
        "studio-live:run-1:request-abcdefghijkl1",
        "studio-live:run-1:request-abcdefghijkl2",
    ]
    .into_iter()
    .map(|key| QueuedItem {
        priority: QueuePriority::Human,
        preview: None,
        message: "same text".into(),
        custom_message: None,
        agent_message: None,
        input_identity: None,
        queue_key: Some(key.into()),
        admission_id: None,
        images: vec![],
        done: None,
        queue_visible: true,
        policy: TurnPolicy::Queued,
        forced_batch: false,
    })
    .collect();
    runner
        .run_turn(engine, crate::worker::queue::Lane::FollowUp, items)
        .await;
    let reopened = SessionFile::open(&path).unwrap();
    let rows: Vec<_> = reopened
        .entries
        .iter()
        .filter(|row| row.type_ == "message" && row.fields["message"]["role"] == "user")
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].fields["studioLiveInputKey"],
        "studio-live:run-1:request-abcdefghijkl1"
    );
    assert_eq!(
        rows[1].fields["studioLiveInputKey"],
        "studio-live:run-1:request-abcdefghijkl2"
    );
    assert_ne!(rows[0].id, rows[1].id);
    assert_eq!(rows[1].parent_id.as_deref(), Some(rows[0].id.as_str()));
}
