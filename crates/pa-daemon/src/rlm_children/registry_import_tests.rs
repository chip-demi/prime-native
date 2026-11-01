use super::*;
use crate::rlm_children::{ParentIdentity, SupervisorChildSessions};

struct Fixture {
    _dir: tempfile::TempDir,
    sessions: SupervisorChildSessions,
    parent: PathBuf,
    child: PathBuf,
    ledger: PathBuf,
}

impl Fixture {
    fn new(status: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let agent = dir.path().join("agent");
        let session_dir = agent.join("sessions");
        std::fs::create_dir_all(&session_dir).unwrap();
        let parent = session_dir.join("parent.jsonl");
        let child = agent.join("session-artifacts/parent/sub-kid/kid.jsonl");
        std::fs::create_dir_all(child.parent().unwrap()).unwrap();
        std::fs::write(&parent, json!({"type":"session","id":"parent","timestamp":"2026-10-22T00:00:00Z","cwd":dir.path(),"rlmDepth":0}).to_string()+"\n").unwrap();
        std::fs::write(&child, json!({"type":"session","id":"kid","timestamp":"2026-10-22T00:00:01Z","cwd":dir.path(),"parentSession":parent,"rlmDepth":1}).to_string()+"\n").unwrap();
        let socket = dir.path().join("supervisor.sock");
        crate::descriptor::persist_supervisor_config(
            &crate::descriptor::descriptor_dir(&agent, &socket)
                .join(crate::descriptor::SUPERVISOR_CONFIG_FILE_NAME),
            &crate::descriptor::PersistedSupervisorConfig {
                version: 1,
                socket_path: socket.display().to_string(),
                default_session_dir: Some(session_dir.display().to_string()),
            },
        )
        .unwrap();
        let ledger = crate::rlm_ledger::rlm_ledger_path(&agent, &session_dir);
        std::fs::create_dir_all(ledger.parent().unwrap()).unwrap();
        std::fs::write(&ledger, Self::edge(&parent, &child) + "\n").unwrap();
        crate::rlm_ledger::write_rlm_subagent_display(
            &crate::rlm_ledger::RlmSubagentDisplayEntry {
                type_tag: "rlm_subagent".into(),
                child_id: "sub-kid".into(),
                session_name: "kid".into(),
                session_dir: child.parent().unwrap().display().to_string(),
                session_file: child.display().to_string(),
                rlm_parent_node_id: None,
                prompt: Some("owned task".into()),
                spawn_code: None,
                model: None,
                status: status.into(),
                admission_unresolved: None,
                admission_generation: Some("generation-1".into()),
                expected_message_id: Some("spawn:sub-kid".into()),
                created_at: 1,
            },
        )
        .unwrap();
        let sessions = SupervisorChildSessions::new(
            Arc::new(crate::supervisor_link::SupervisorLink::new(socket)),
            agent.clone(),
            "parent-live".into(),
            Arc::new(crate::model_allowlist::ModelRefusalTelemetry::new(
                agent, true,
            )),
            String::new(),
        );
        sessions.set_identity(ParentIdentity {
            session_id: Some("parent".into()),
            session_file: Some(parent.display().to_string()),
            ..ParentIdentity::with_default_depth()
        });
        Self {
            _dir: dir,
            sessions,
            parent,
            child,
            ledger,
        }
    }

    fn edge(parent: &Path, child: &Path) -> String {
        json!({"v":1,"op":"spawn","at":"2026-10-22T00:00:01Z","childId":"sub-kid","parent":parent,"child":child,"depth":1,"name":"kid"}).to_string()
    }

    async fn import(
        &self,
        facts: &[crate::journal::OwnerCancelFact],
    ) -> Result<crate::rlm_children::ImportedChildAssignment> {
        self.sessions
            .import_missing_family_child(
                "sub-kid",
                "kid",
                &self.child.display().to_string(),
                1,
                facts,
            )
            .await
    }

    fn display(&self) -> crate::rlm_ledger::RlmSubagentDisplayEntry {
        crate::rlm_ledger::read_rlm_subagent_display(self.child.parent().unwrap()).unwrap()
    }

    fn cancel(&self) -> crate::journal::OwnerCancelFact {
        crate::journal::OwnerCancelFact {
            parent_session_file: self.parent.display().to_string(),
            child_id: "sub-kid".into(),
            child_session_file: self.child.display().to_string(),
            admission_generation: Some("generation-1".into()),
            expected_message_id: Some("spawn:sub-kid".into()),
        }
    }
}

#[tokio::test]
async fn missing_family_child_import_preserves_live_assignment_and_normal_admission() {
    let f = Fixture::new("running");
    let original = std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap();
    let assignment = f.import(&[]).await.unwrap();
    let record = f.sessions.inner.find_record("sub-kid").await.unwrap();
    {
        let row = record.lock().await;
        assert!(!row.settled);
        assert_eq!(
            row.settled_status, None,
            "a live import never invents the restoration error verdict"
        );
        assert_eq!(
            row.active_session_id, "kid",
            "transport stays unbound until the receipt"
        );
        assert_eq!(row.expected_message_id.as_deref(), Some("spawn:sub-kid"));
        assert_eq!(row.admission_generation.as_deref(), Some("generation-1"));
    }
    assert_eq!(
        std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap(),
        original
    );
    let admission = f
        .sessions
        .begin_imported_child_admission("sub-kid", &assignment)
        .await
        .unwrap()
        .unwrap();
    {
        let row = record.lock().await;
        assert!(row.admission_blocked);
        assert_ne!(row.admission_generation.as_deref(), Some("generation-1"));
        assert_eq!(row.expected_message_id, None);
    }
    assert_eq!(f.display().admission_unresolved, Some(true));
    admission.rejected().await.unwrap();
    assert_eq!(
        record.lock().await.admission_generation.as_deref(),
        Some("generation-1")
    );
    assert_eq!(
        std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap(),
        original
    );
}

#[tokio::test]
async fn missing_family_child_import_is_single_and_current_memory_wins() {
    let f = Fixture::new("completed");
    let (first, second) = tokio::join!(f.import(&[]), f.import(&[]));
    first.unwrap();
    second.unwrap();
    assert_eq!(f.sessions.inner.children.lock().await.len(), 1);
    let record = f.sessions.inner.find_record("sub-kid").await.unwrap();
    record.lock().await.admission_generation = Some("memory-generation".into());
    f.import(&[]).await.unwrap();
    assert!(Arc::ptr_eq(
        &record,
        &f.sessions.inner.find_record("sub-kid").await.unwrap()
    ));
    assert_eq!(
        record.lock().await.admission_generation.as_deref(),
        Some("memory-generation")
    );
    for flag in ["cancel", "unknown", "closed"] {
        {
            let mut row = record.lock().await;
            row.owner_cancelled = flag == "cancel";
            row.admission_blocked = flag == "unknown";
            row.closed_by_parent = flag == "closed";
        }
        assert!(f.import(&[]).await.is_err(), "{flag}");
        assert_eq!(f.sessions.inner.children.lock().await.len(), 1);
        assert_eq!(
            record.lock().await.admission_generation.as_deref(),
            Some("memory-generation")
        );
    }
}

#[tokio::test]
async fn missing_family_child_import_rejects_foreign_missing_ambiguous_and_depth() {
    for case in [
        "missing",
        "foreign-ledger",
        "foreign-header",
        "wrong-id",
        "wrong-depth",
        "duplicate",
        "wrong-parent-id",
    ] {
        let f = Fixture::new("completed");
        let foreign = f.parent.with_file_name("foreign.jsonl");
        std::fs::write(&foreign, json!({"type":"session","id":"foreign","timestamp":"2026-10-22T00:00:00Z","cwd":"/","rlmDepth":0}).to_string()+"\n").unwrap();
        match case {
            "missing" => std::fs::remove_file(&f.ledger).unwrap(),
            "foreign-ledger" => std::fs::write(&f.ledger, Fixture::edge(&foreign, &f.child)+"\n").unwrap(),
            "foreign-header" => std::fs::write(&f.child, json!({"type":"session","id":"kid","timestamp":"2026-10-22T00:00:01Z","cwd":"/","parentSession":foreign,"rlmDepth":1}).to_string()+"\n").unwrap(),
            "wrong-id" => std::fs::write(&f.child, json!({"type":"session","id":"other","timestamp":"2026-10-22T00:00:01Z","cwd":"/","parentSession":f.parent,"rlmDepth":1}).to_string()+"\n").unwrap(),
            "wrong-depth" => std::fs::write(&f.child, json!({"type":"session","id":"kid","timestamp":"2026-10-22T00:00:01Z","cwd":"/","parentSession":f.parent,"rlmDepth":2}).to_string()+"\n").unwrap(),
            "duplicate" => std::fs::write(&f.ledger, Fixture::edge(&f.parent, &f.child)+"\n"+&Fixture::edge(&f.parent, &foreign)+"\n").unwrap(),
            "wrong-parent-id" => {
                let mut identity = f.sessions.inner.identity.lock().unwrap();
                identity.session_id = Some("foreign".into());
            },
            _ => unreachable!(),
        }
        assert!(f.import(&[]).await.is_err(), "{case}");
        assert!(f.sessions.inner.children.lock().await.is_empty(), "{case}");
    }
}

#[tokio::test]
async fn missing_family_child_import_keeps_deleted_unknown_and_generation_gaps_closed() {
    for case in [
        "deleted",
        "unknown",
        "generation",
        "expected",
        "wrong-display-file",
        "ledger-depth",
    ] {
        let f = Fixture::new("completed");
        let mut display = f.display();
        match case {
            "deleted" => display.status = "deleted".into(),
            "unknown" => display.admission_unresolved = Some(true),
            "generation" => display.admission_generation = None,
            "expected" => display.expected_message_id = None,
            "wrong-display-file" => display.session_file = f.parent.display().to_string(),
            "ledger-depth" => {
                let mut edge: serde_json::Value =
                    serde_json::from_str(&Fixture::edge(&f.parent, &f.child)).unwrap();
                edge["depth"] = json!(2);
                std::fs::write(&f.ledger, edge.to_string() + "\n").unwrap();
            }
            _ => unreachable!(),
        }
        crate::rlm_ledger::write_rlm_subagent_display(&display).unwrap();
        let bytes = std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap();
        assert!(f.import(&[]).await.is_err(), "{case}");
        assert!(f.sessions.inner.children.lock().await.is_empty(), "{case}");
        assert_eq!(
            std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap(),
            bytes,
            "{case}"
        );
    }
}

#[tokio::test]
async fn missing_family_child_import_respects_persisted_cancel_and_tombstones() {
    let f = Fixture::new("completed");
    assert!(f.import(&[f.cancel()]).await.is_err());
    assert!(f.sessions.inner.children.lock().await.is_empty());
    f.sessions
        .inner
        .gate
        .lock()
        .unwrap()
        .authenticated_owner_cancellations
        .push(f.cancel());
    assert!(f.import(&[]).await.is_err());
    assert!(f.sessions.inner.children.lock().await.is_empty());

    let f = Fixture::new("completed");
    f.import(&[]).await.unwrap();
    let record = f.sessions.inner.find_record("sub-kid").await.unwrap();
    f.sessions
        .inner
        .remember_deleted_child(&*record.lock().await);
    f.sessions.inner.children.lock().await.clear();
    assert!(f.import(&[]).await.is_err());
    assert!(f.sessions.inner.children.lock().await.is_empty());
}

#[tokio::test]
async fn missing_family_child_import_allows_only_stale_assignment_cancellations() {
    for field in ["generation", "expected"] {
        let f = Fixture::new("completed");
        let stale = f.cancel();
        let mut display = f.display();
        if field == "generation" {
            display.admission_generation = Some("new-generation".into());
        } else {
            display.expected_message_id = Some("new-message".into());
        }
        crate::rlm_ledger::write_rlm_subagent_display(&display).unwrap();
        f.sessions
            .inner
            .gate
            .lock()
            .unwrap()
            .authenticated_owner_cancellations
            .push(stale.clone());
        f.import(std::slice::from_ref(&stale)).await.unwrap();
        let record = f.sessions.inner.find_record("sub-kid").await.unwrap();
        let expected = (
            display.admission_generation.clone(),
            display.expected_message_id.clone(),
        );
        {
            let row = record.lock().await;
            assert_eq!(
                (
                    row.admission_generation.clone(),
                    row.expected_message_id.clone()
                ),
                expected
            );
        }
        f.import(std::slice::from_ref(&stale)).await.unwrap();
        let mut current = stale;
        current.admission_generation = display.admission_generation;
        current.expected_message_id = display.expected_message_id;
        assert!(
            f.import(&[current]).await.is_err(),
            "current {field} cancellation"
        );
        assert!(Arc::ptr_eq(
            &record,
            &f.sessions.inner.find_record("sub-kid").await.unwrap()
        ));
    }
}

#[tokio::test]
async fn missing_family_child_import_rechecks_parent_after_async_current_record_lookup() {
    use std::future::Future;
    use std::task::Poll;

    let f = Fixture::new("completed");
    f.import(&[]).await.unwrap();
    let record = f.sessions.inner.find_record("sub-kid").await.unwrap();
    let held = record.lock().await;
    let mut pending = Box::pin(f.import(&[]));
    // The exact row is locked: polling reaches that observable await after
    // capturing the old parent, without sleeps or an assumed I/O duration.
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    f.sessions.set_identity(ParentIdentity {
        session_id: Some("replacement-parent".into()),
        session_file: Some(f.parent.display().to_string()),
        ..ParentIdentity::with_default_depth()
    });
    drop(held);
    assert!(pending.await.is_err());
    assert_eq!(f.sessions.inner.children.lock().await.len(), 1);
    assert!(Arc::ptr_eq(
        &record,
        &f.sessions.inner.find_record("sub-kid").await.unwrap()
    ));
}

#[tokio::test]
async fn imported_family_child_changed_generation_or_unknown_blocks_admission() {
    for case in [
        "generation",
        "expected",
        "unknown",
        "parent",
        "memory-generation",
        "owner-cancel",
    ] {
        let f = Fixture::new("completed");
        let assignment = f.import(&[]).await.unwrap();
        let record = f.sessions.inner.find_record("sub-kid").await.unwrap();
        let mut display = f.display();
        match case {
            "generation" => display.admission_generation = Some("new-external-generation".into()),
            "expected" => display.expected_message_id = Some("new-external-message".into()),
            "unknown" => display.admission_unresolved = Some(true),
            "parent" => f.sessions.inner.identity.lock().unwrap().rlm_depth = 1,
            "memory-generation" => {
                record.lock().await.admission_generation = Some("new-memory-generation".into());
            }
            "owner-cancel" => record.lock().await.owner_cancelled = true,
            _ => unreachable!(),
        }
        crate::rlm_ledger::write_rlm_subagent_display(&display).unwrap();
        let bytes = std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap();
        let before = {
            let row = record.lock().await;
            (
                row.admission_generation.clone(),
                row.expected_message_id.clone(),
                row.admission_blocked,
                row.owner_cancelled,
            )
        };
        assert!(
            f.sessions
                .begin_imported_child_admission("sub-kid", &assignment)
                .await
                .is_err(),
            "{case}"
        );
        let row = record.lock().await;
        assert_eq!(
            (
                row.admission_generation.clone(),
                row.expected_message_id.clone(),
                row.admission_blocked,
                row.owner_cancelled
            ),
            before,
            "{case}"
        );
        assert_eq!(
            std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap(),
            bytes,
            "{case}"
        );
    }
}

#[tokio::test]
async fn imported_family_child_delete_or_close_before_admission_stays_closed() {
    for case in ["deleted", "closed"] {
        let f = Fixture::new("completed");
        f.import(&[]).await.unwrap();
        let record = f.sessions.inner.find_record("sub-kid").await.unwrap();
        {
            let mut row = record.lock().await;
            match case {
                "closed" => row.closed_by_parent = true,
                "deleted" => f.sessions.inner.remember_deleted_child(&row),
                _ => unreachable!(),
            }
        }
        let original = std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap();
        assert!(
            f.sessions.begin_child_admission("sub-kid").await.is_err(),
            "{case}"
        );
        assert_eq!(
            std::fs::read(f.child.parent().unwrap().join("rlm-subagent.json")).unwrap(),
            original
        );
        assert_eq!(
            record.lock().await.admission_generation.as_deref(),
            Some("generation-1")
        );
    }
}

#[tokio::test]
async fn current_cancelled_child_cannot_be_imported_but_explicit_retask_is_preserved() {
    let f = Fixture::new("completed");
    f.import(&[]).await.unwrap();
    let record = f.sessions.inner.find_record("sub-kid").await.unwrap();
    {
        let mut row = record.lock().await;
        row.owner_cancelled = true;
        row.settled_status = Some("cancelled");
        row.admission_blocked = false;
    }
    let mut display = f.display();
    display.admission_unresolved = Some(true);
    crate::rlm_ledger::write_rlm_subagent_display(&display).unwrap();
    assert!(f.import(&[]).await.is_err());
    let admission = f
        .sessions
        .begin_child_admission("sub-kid")
        .await
        .unwrap()
        .unwrap();
    {
        let row = record.lock().await;
        assert!(!row.owner_cancelled);
        assert_ne!(row.admission_generation.as_deref(), Some("generation-1"));
    }
    admission.rejected().await.unwrap();
}
