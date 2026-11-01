use super::*;

#[tokio::test]
async fn create_restores_cross_namespace_cancel_before_kernel_accounting() {
    for case in [
        "matching",
        "foreign-parent",
        "foreign-child",
        "new-generation",
        "unknown",
    ] {
        let dir = tempfile::TempDir::new().unwrap();
        let agent = dir.path().join("agent");
        let sessions = agent.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let parent = sessions.join("parent.jsonl");
        let mut store = SessionFile::create(dir.path().to_str().unwrap(), None, 0);
        store.path = parent.clone();
        store.rewrite().unwrap();
        let child_dir = agent.join("session-artifacts/parent/sub-9719ef01");
        std::fs::create_dir_all(&child_dir).unwrap();
        let child = child_dir.join("child.jsonl");
        std::fs::write(&child, "").unwrap();
        let socket = dir.path().join("supervisor.sock");
        crate::descriptor::persist_supervisor_config(
            &crate::descriptor::descriptor_dir(&agent, &socket)
                .join(crate::descriptor::SUPERVISOR_CONFIG_FILE_NAME),
            &crate::descriptor::PersistedSupervisorConfig {
                version: 1,
                socket_path: socket.to_string_lossy().into(),
                default_session_dir: Some(sessions.to_string_lossy().into()),
            },
        )
        .unwrap();
        let ledger = crate::rlm_ledger::rlm_ledger_path(&agent, &sessions);
        std::fs::create_dir_all(ledger.parent().unwrap()).unwrap();
        std::fs::write(
            &ledger,
            format!(
                "{}\n",
                json!({
                    "v":1,"op":"spawn","at":"2026-10-19T00:00:00Z", "childId":"sub-9719ef01",
                    "parent":parent,"child":child,"depth":1,"name":"old child"
                })
            ),
        )
        .unwrap();
        crate::rlm_ledger::write_rlm_subagent_display(
            &crate::rlm_ledger::RlmSubagentDisplayEntry {
                type_tag: "rlm_subagent".into(),
                child_id: "sub-9719ef01".into(),
                session_name: "old child".into(),
                session_dir: child_dir.to_string_lossy().into(),
                session_file: child.to_string_lossy().into(),
                rlm_parent_node_id: None,
                prompt: None,
                spawn_code: None,
                model: None,
                status: "running".into(),
                admission_unresolved: Some(true),
                admission_generation: (case == "new-generation").then(|| "new".into()),
                expected_message_id: None,
                created_at: 1,
            },
        )
        .unwrap();
        let old_journal = agent.join("daemon-workers/f0084df628f2/old.recovery.jsonl");
        std::fs::create_dir_all(old_journal.parent().unwrap()).unwrap();
        let fact = crate::journal::OwnerCancelFact {
            parent_session_file: if case == "foreign-parent" {
                sessions.join("other.jsonl")
            } else {
                parent.clone()
            }
            .to_string_lossy()
            .into(),
            child_id: "sub-9719ef01".into(),
            child_session_file: if case == "foreign-child" {
                child_dir.join("other.jsonl")
            } else {
                child.clone()
            }
            .to_string_lossy()
            .into(),
            admission_generation: None,
            expected_message_id: None,
        };
        if case != "unknown" {
            crate::journal::WorkerRecoveryJournal::open(&old_journal)
                .unwrap()
                .record_owner_cancel(fact)
                .unwrap();
        }
        let original = std::fs::read(&old_journal).unwrap_or_default();
        let destination = agent.join("daemon-workers/c2f405805d29/new.recovery.jsonl");
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        crate::journal::WorkerRecoveryJournal::restore_parent_owner_cancellations(
            &agent.join("daemon-workers"),
            &destination,
            &parent,
        )
        .unwrap();
        let worker = Worker::new(
            WorkerConfig {
                socket_path: dir.path().join("worker.sock"),
                supervisor_socket_path: socket,
                token: "fixture".into(),
                worker_instance_id: String::new(),
                active_session_id: "parent".into(),
                agent_dir: agent,
                recovery_journal_path: destination.clone(),
                telemetry_disabled: Some(true),
                script: Some(json!({"engine":"faux","responses":[]})),
            },
            None,
        );
        // serve() normally opens this handle before dispatching durable create.
        *worker.recovery.lock().unwrap() =
            Some(crate::journal::WorkerRecoveryJournal::open(&destination).unwrap());
        let response = worker
            .dispatch("create", &json!({"sessionPath":parent,"cwd":dir.path()}))
            .await;
        assert!(response.success, "{case}: {response:?}");
        // No get_orchestration_gate/get_rlm_children RPC: use the same registry
        // queried directly by the kernel handler and goal continuation boundary.
        let children = worker
            .agent_engine
            .as_ref()
            .unwrap()
            .children
            .as_ref()
            .unwrap();
        assert_eq!(children.any_running().await, case != "matching", "{case}");
        let status = children.gate_status().await;
        let row = status["assignments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["childId"] == "sub-9719ef01")
            .unwrap();
        assert_eq!(
            row["obligation"],
            json!(case != "matching"),
            "{case}: {status}"
        );
        let facts = worker
            .recovery
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .owner_cancel_facts()
            .to_vec();
        children.restore_owner_cancellations(&facts).await.unwrap();
        assert_eq!(
            children.any_running().await,
            case != "matching",
            "idempotent {case}"
        );
        assert_eq!(std::fs::read(&old_journal).unwrap_or_default(), original);
    }
}
