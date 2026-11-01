use super::*;
use pa_core::session_engine::goal_driver::{GoalControlAction, GoalDriver};

struct AtomicEngine {
    state: tokio::sync::Mutex<(GoalDriver, pa_core::session::manager::SessionManager)>,
    aborts: std::sync::atomic::AtomicUsize,
    race: Option<Arc<Mutex<SessionCore>>>,
}
impl crate::engine::SessionEngine for AtomicEngine {
    fn run_side_question(
        &self,
        _: crate::engine::SideQuestionRequest,
        _: &pa_agent::abort::AbortSignal,
        _: &pa_core::session_engine::side_question::SideQuestionSink,
    ) -> crate::engine::SideQuestionOutcome {
        unreachable!("not used")
    }
    fn run_compaction(
        &self,
        _: crate::engine::CompactionRequest,
        _: &pa_agent::abort::AbortSignal,
    ) -> crate::engine::CompactionOutcome {
        unreachable!("not used")
    }
    fn run_branch_summary(
        &self,
        _: crate::engine::BranchSummaryRequest,
        _: &pa_agent::abort::AbortSignal,
    ) -> crate::engine::BranchSummaryOutcome {
        unreachable!("not used")
    }
    fn rebuild_session_context(
        &self,
        _: Vec<pa_types::session::FileEntry>,
        _: pa_core::session_engine::goal_driver::GoalBranchReload,
    ) -> Result<()> {
        Ok(())
    }

    fn run_prompt(
        &self,
        _: usize,
        _: crate::engine::PromptRequest,
        _: &dyn Fn() -> bool,
        _: &mut dyn FnMut(crate::engine::EngineEvent) -> bool,
    ) {
    }
    fn atomic_goal_stop<'a>(
        &'a self,
        id: &'a str,
        objective: &'a str,
        action: GoalControlAction,
        commit: &'a mut (dyn FnMut(&pa_core::goals::GoalState) -> Result<()> + Send),
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<pa_core::goals::GoalState>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            let (driver, session) = &mut *state;
            let goal = driver
                .control_goal(session, id, objective, action)?
                .unwrap_or_else(pa_core::goals::empty_goal_state);
            if let Some(core) = &self.race {
                core.lock().unwrap().turn_serial += 1;
            }
            commit(&goal).map_err(|error| anyhow::anyhow!("UNKNOWN: {error:#}"))?;
            Ok(goal)
        })
    }
    fn abort_in_flight_turn(&self) {
        self.aborts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn fixture(
    dir: &std::path::Path,
    race: bool,
) -> (
    Worker,
    Arc<AtomicEngine>,
    pa_types::daemon::DaemonWorkerPeerGrant,
    String,
) {
    let mut worker = Worker::new(
        WorkerConfig {
            socket_path: dir.join("worker.sock"),
            supervisor_socket_path: PathBuf::new(),
            token: "synthetic".into(),
            worker_instance_id: "instance".into(),
            active_session_id: "atomic".into(),
            agent_dir: dir.join("agent"),
            recovery_journal_path: dir.join("recovery.jsonl"),
            telemetry_disabled: Some(true),
            script: Some(json!({"responses":[]})),
        },
        None,
    );
    let mut store = SessionFile::create(dir.to_str().unwrap(), None, 0);
    store.set_path(dir.join("session.jsonl"));
    store.rewrite().unwrap();
    let mut core = SessionCore::test_core(Some(store), dir.to_string_lossy().into());
    core.active_session_id = "atomic".into();
    core.generation = "generation".into();
    *worker.core.lock().unwrap() = core;
    let mut session = pa_core::session::manager::SessionManager::in_memory(dir);
    let mut driver = GoalDriver::new();
    let goal = driver.start(&mut session, "mission", Some(1000)).unwrap();
    let engine = Arc::new(AtomicEngine {
        state: tokio::sync::Mutex::new((driver, session)),
        aborts: std::sync::atomic::AtomicUsize::new(0),
        race: race.then(|| worker.core.clone()),
    });
    worker.engine = engine.clone();
    *worker.recovery.lock().unwrap() =
        Some(WorkerRecoveryJournal::open(&dir.join("recovery.jsonl")).unwrap());
    worker.peer_grants.replace_issuer(Some("issuer".into()));
    let context = worker.grant_context();
    let mut grant = pa_types::daemon::DaemonWorkerPeerGrant {
        grant_id: "grant".into(),
        token: "secret".into(),
        expires_at: crate::util::iso_from_unix_ms(crate::util::now_ms() + 10000),
        purpose: "session_client".into(),
        worker_instance_id: context.worker_instance_id.clone(),
        active_session_id: "atomic".into(),
        issuer_generation: "sup".into(),
        control_scope: Some(pa_types::daemon::DaemonSessionControlScope {
            control_version: 1,
            root_session_id: context.root_session_id.clone(),
            active_session_id: "atomic".into(),
            session_file: context.session_file.clone(),
            cwd: context.cwd.clone(),
            operations: vec!["control_goal".into(), "get_queue".into()],
        }),
        issuer_incarnation: Some("issuer".into()),
        control_generation: None,
    };
    worker
        .peer_grants
        .register(grant.clone(), &context, "sup", crate::util::now_ms())
        .unwrap();
    // Exercise the real single-use auth, not a payload pretending to be a controller.
    let auth:pa_types::daemon::DaemonPeerCommand=serde_json::from_value(json!({"type":"peer_auth","grantId":"grant","token":"secret","workerInstanceId":"instance","purpose":"session_client"})).unwrap();
    grant = worker
        .peer_grants
        .authenticate(&auth, &context, crate::util::now_ms())
        .unwrap();
    (worker, engine, grant, goal.goal_id.unwrap())
}

#[tokio::test]
async fn atomic_stop_cas_append_checkpoint_before_one_cancel_and_no_replay() {
    for action in ["pause_and_stop", "clear_and_stop"] {
        let dir = tempfile::tempdir().unwrap();
        let (worker, engine, grant, id) = fixture(dir.path(), false);
        let request = json!({"activeSessionId":"atomic","expectedGoalId":id,"expectedObjective":"mission","controlVersion":1,"action":action});
        let mut stale = request.clone();
        stale["expectedObjective"] = json!("stale");
        assert!(
            !worker
                .dispatch_scoped("control_goal", &stale, &grant)
                .await
                .success
        );
        assert_eq!(engine.aborts.load(std::sync::atomic::Ordering::SeqCst), 0);
        let result = worker
            .dispatch_scoped("control_goal", &request, &grant)
            .await;
        assert!(result.success, "{result:?}");
        let data = result.data.unwrap();
        assert_eq!(data["controlVersion"], 1);
        assert_eq!(data["cancellationRequested"], true);
        if action == "clear_and_stop" {
            assert!(data["goal"].is_null());
        } else {
            assert_eq!(data["goal"]["status"], "paused");
        }
        assert_eq!(engine.aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
        {
            let core = worker.core.lock().unwrap();
            assert!(core.abort_requested && core.queued_input_suspended);
            assert!(core.steering.is_empty() && core.follow_up.is_empty());
            let durable = SessionFile::open(&core.store.as_ref().unwrap().path).unwrap();
            assert!(durable
                .entries
                .iter()
                .any(|row| row.fields["customType"] == pa_core::goals::GOAL_STATE_CUSTOM_TYPE));
        }
        assert!(
            !worker
                .dispatch_scoped("control_goal", &request, &grant)
                .await
                .success
        );
        assert_eq!(engine.aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn atomic_stop_wrong_scope_revocation_turn_race_and_append_failure_never_cancel() {
    for failure in [
        "issuer",
        "generation",
        "turn",
        "append",
        "journal",
        "missing",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (worker, engine, grant, id) = fixture(dir.path(), failure == "turn");
        let request = json!({"activeSessionId":"atomic","expectedGoalId":id,"expectedObjective":"mission","controlVersion":1,"action":"pause_and_stop"});
        match failure {
            "issuer" => worker.peer_grants.replace_issuer(Some("new-issuer".into())),
            "generation" => worker.core.lock().unwrap().generation = "replaced".into(),
            "append" => {
                let path = worker
                    .core
                    .lock()
                    .unwrap()
                    .store
                    .as_ref()
                    .unwrap()
                    .path
                    .clone();
                std::fs::remove_file(&path).unwrap();
                std::fs::create_dir(&path).unwrap();
            }
            "journal" => {
                *worker.recovery.lock().unwrap() = None;
            }
            "missing" => worker.core.lock().unwrap().store = None,
            "turn" => {}
            _ => unreachable!(),
        }
        let result = worker
            .dispatch_scoped("control_goal", &request, &grant)
            .await;
        assert!(!result.success, "{failure}: {result:?}");
        assert_eq!(engine.aborts.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(!worker.core.lock().unwrap().abort_requested);
        if matches!(failure, "turn" | "journal" | "append") {
            assert!(result.error.unwrap().starts_with("UNKNOWN:"));
            assert!(worker
                .goal_edit_dirty
                .load(std::sync::atomic::Ordering::SeqCst));
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn atomic_stop_actual_peer_frames_lost_ack_never_replays_and_revokes() {
    async fn frame(
        writer: &mut tokio::net::unix::OwnedWriteHalf,
        reader: &mut crate::framing::PrivateFrameReader<tokio::net::unix::OwnedReadHalf>,
        kind: &str,
        payload: Value,
    ) -> Value {
        crate::framing::write_frame(
            writer,
            &json!({"kind":"command","requestId":"test","commandType":kind}),
            &serde_json::to_vec(&payload).unwrap(),
            DEFAULT_PRIVATE_FRAME_LIMITS,
        )
        .await
        .unwrap();
        loop {
            let frame = reader.read_frame().await.unwrap().unwrap();
            let body: Value = serde_json::from_slice(&frame.payload).unwrap();
            if body["type"] == "response" {
                return body;
            }
        }
    }
    use tokio::io::AsyncWriteExt;
    let dir = tempfile::tempdir().unwrap();
    let (worker, engine, _, id) = fixture(dir.path(), false);
    // Fixture burned one grant to exercise admission; mint another for the framed socket.
    let context = worker.grant_context();
    let grant = pa_types::daemon::DaemonWorkerPeerGrant {
        grant_id: "framed".into(),
        token: "framed-secret".into(),
        expires_at: crate::util::iso_from_unix_ms(crate::util::now_ms() + 10000),
        purpose: "session_client".into(),
        worker_instance_id: "instance".into(),
        active_session_id: "atomic".into(),
        issuer_generation: "sup".into(),
        issuer_incarnation: Some("issuer".into()),
        control_generation: None,
        control_scope: Some(pa_types::daemon::DaemonSessionControlScope {
            control_version: 1,
            root_session_id: context.root_session_id.clone(),
            active_session_id: "atomic".into(),
            session_file: context.session_file,
            cwd: context.cwd,
            operations: vec!["control_goal".into(), "get_queue".into()],
        }),
    };
    worker
        .peer_grants
        .register(
            grant.clone(),
            &worker.grant_context(),
            "sup",
            crate::util::now_ms(),
        )
        .unwrap();
    let worker = Arc::new(worker);
    let (server, client) = tokio::net::UnixStream::pair().unwrap();
    let task = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.handle_connection(Box::new(server)).await })
    };
    let (reader, mut writer) = client.into_split();
    let mut reader = crate::framing::PrivateFrameReader::new(reader, DEFAULT_PRIVATE_FRAME_LIMITS);
    reader.read_frame().await.unwrap().unwrap();
    let auth=frame(&mut writer,&mut reader,"peer_auth",json!({"type":"peer_auth","grantId":"framed","token":"framed-secret","workerInstanceId":"instance","purpose":"session_client"})).await;
    assert_eq!(auth["success"], true);
    let wrong=frame(&mut writer,&mut reader,"control_goal",json!({"activeSessionId":"foreign","controlVersion":1,"expectedGoalId":id,"expectedObjective":"mission","action":"pause_and_stop"})).await;
    assert_eq!(wrong["success"], false);
    assert_eq!(engine.aborts.load(std::sync::atomic::Ordering::SeqCst), 0);
    let request = json!({"activeSessionId":"atomic","controlVersion":1,"expectedGoalId":id,"expectedObjective":"mission","action":"pause_and_stop"});
    let response = frame(&mut writer, &mut reader, "control_goal", request.clone()).await;
    assert_eq!(response["success"], true);
    assert_eq!(engine.aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
    // Treat ACK as lost: lookup/observe only. Issuer replacement revokes already-authenticated socket.
    worker
        .peer_grants
        .replace_issuer(Some("replacement".into()));
    let revoked = frame(&mut writer, &mut reader, "control_goal", request).await;
    assert_eq!(revoked["success"], false);
    assert_eq!(engine.aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
    writer.shutdown().await.unwrap();
    drop(writer);
    drop(reader);
    task.await.unwrap().unwrap();
    // No restart state or retry authority is inferred from the discarded response.
}

#[tokio::test]
async fn atomic_stop_real_driver_stale_does_not_commit_and_accounting_is_retained() {
    let dir = tempfile::tempdir().unwrap();
    let agent_dir = dir.path().join("agent");
    let engine =
        crate::agent_engine::AgentSessionEngine::new(crate::agent_engine::AgentEngineConfig {
            cwd: dir.path().into(),
            agent_dir,
            provider: None,
            model: None,
            api_key: None,
            thinking: None,
            session_dir: None,
            session_file: None,
            faux_script: None,
            supervisor_link: None,
            telemetry_disabled: Some(true),
            cron_store: None,
            queued_steering_probe: None,
        })
        .unwrap();
    let mut session = pa_core::session::manager::SessionManager::in_memory(dir.path());
    let mut driver = GoalDriver::new();
    let mut goal = driver.start(&mut session, "mission", Some(1000)).unwrap();
    goal.tokens_used = 123;
    goal.time_used_seconds = 27;
    goal.continuations_used = 4;
    let id = goal.goal_id.clone().unwrap();
    driver.restore_from_persisted(goal);
    *engine.goal_runtime.lock().unwrap() = Some(crate::agent_engine::GoalRuntimeHandles {
        driver: Arc::new(tokio::sync::Mutex::new(driver)),
        session: Arc::new(tokio::sync::Mutex::new(session)),
    });
    let mut commits = 0;
    let mut commit = |goal: &pa_core::goals::GoalState| -> Result<()> {
        commits += 1;
        assert_eq!(goal.status, pa_core::goals::GoalStatus::Paused);
        assert_eq!(goal.tokens_used, 123);
        assert_eq!(goal.time_used_seconds, 27);
        assert_eq!(goal.continuations_used, 4);
        Ok(())
    };
    assert!(engine
        .atomic_goal_stop(&id, "stale", GoalControlAction::Pause, &mut commit)
        .await
        .is_err());
    let result = engine
        .atomic_goal_stop(&id, "mission", GoalControlAction::Pause, &mut commit)
        .await
        .unwrap();
    assert_eq!(result.status, pa_core::goals::GoalStatus::Paused);
    assert_eq!(commits, 1);
}

#[tokio::test]
async fn scoped_input_revoked_or_replaced_never_admits() {
    for reason in ["issuer", "generation", "cwd", "file"] {
        let dir = tempfile::tempdir().unwrap();
        let (worker, _, mut grant, _) = fixture(dir.path(), false);
        grant.control_scope.as_mut().unwrap().operations = vec![
            "steer".into(),
            "follow_up".into(),
            "mutate_queued_message".into(),
        ];
        match reason {
            "issuer" => worker
                .peer_grants
                .replace_issuer(Some("replacement".into())),
            "generation" => worker.core.lock().unwrap().generation = "replacement".into(),
            "cwd" => worker.core.lock().unwrap().cwd = "/foreign".into(),
            "file" => worker
                .core
                .lock()
                .unwrap()
                .store
                .as_mut()
                .unwrap()
                .set_path(dir.path().join("foreign.jsonl")),
            _ => unreachable!(),
        }
        for command in ["steer", "follow_up", "mutate_queued_message"] {
            let result = worker.dispatch_scoped(command, &json!({"activeSessionId":"atomic","extensionVersion":2,"queueKey":"denied","message":"never"}), &grant).await;
            assert!(!result.success, "{reason}/{command}: {result:?}");
        }
        let core = worker.core.lock().unwrap();
        assert!(core.steering.is_empty() && core.follow_up.is_empty());
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_core_rejects_grant_without_blocking_reader_or_replay() {
    async fn send(w: &mut tokio::net::unix::OwnedWriteHalf, kind: &str, id: &str, payload: Value) {
        crate::framing::write_frame(
            w,
            &json!({"kind":"command","requestId":id,"commandType":kind}),
            &serde_json::to_vec(&payload).unwrap(),
            DEFAULT_PRIVATE_FRAME_LIMITS,
        )
        .await
        .unwrap();
    }
    use tokio::io::AsyncWriteExt;
    let dir = tempfile::tempdir().unwrap();
    let (worker, _, _, _) = fixture(dir.path(), false);
    let context = worker.grant_context();
    let grant = pa_types::daemon::DaemonWorkerPeerGrant {
        grant_id: "repro-grant".into(),
        token: "repro-secret".into(),
        expires_at: crate::util::iso_from_unix_ms(crate::util::now_ms() + 15000),
        purpose: "session_client".into(),
        worker_instance_id: context.worker_instance_id.clone(),
        active_session_id: context.active_session_id.clone(),
        issuer_generation: "sup".into(),
        control_scope: None,
        issuer_incarnation: None,
        control_generation: None,
    };
    let worker = Arc::new(worker);
    let (server, client) = tokio::net::UnixStream::pair().unwrap();
    let task = {
        let w = worker.clone();
        tokio::spawn(async move { w.handle_connection(Box::new(server)).await })
    };
    let (reader, mut writer) = client.into_split();
    let mut reader = crate::framing::PrivateFrameReader::new(reader, DEFAULT_PRIVATE_FRAME_LIMITS);
    reader.read_frame().await.unwrap().unwrap();

    send(&mut writer,"worker_auth","auth",json!({"token":"synthetic","supervisorGeneration":"sup","supervisorPid":1,"supervisorSocketPath":"/fake","controlIssuerIncarnation":"issuer"})).await;
    let auth = tokio::time::timeout(std::time::Duration::from_secs(2), reader.read_frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&auth.payload).unwrap()["success"],
        true
    );
    let core = worker.core.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let lock_thread = std::thread::spawn(move || {
        let _held = core.lock().unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));
    });
    locked_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    send(
        &mut writer,
        "worker_register_peer_transport",
        "reg",
        json!({"type":"worker_register_peer_transport","grant":grant.clone()}),
    )
    .await;
    send(
        &mut writer,
        "worker_register_peer_transport",
        "probe",
        json!({"type":"worker_register_peer_transport","grant":{}}),
    )
    .await;
    let mut responses = std::collections::HashMap::new();
    let mut order = Vec::new();
    while responses.len() < 2 {
        let f = tokio::time::timeout(std::time::Duration::from_secs(2), reader.read_frame())
            .await
            .expect("reader stalled behind busy core")
            .unwrap()
            .unwrap();
        if let Some(id) = f.header.get("requestId").and_then(Value::as_str) {
            order.push(id.to_string());
            responses.insert(
                id.to_string(),
                serde_json::from_slice::<Value>(&f.payload).unwrap(),
            );
        }
    }
    assert_eq!(
        order,
        vec!["reg", "probe"],
        "registration frames must be answered in order"
    );
    assert_eq!(responses["probe"]["success"], false);
    assert_eq!(responses["reg"]["success"], false);
    assert!(responses["reg"]["error"]
        .as_str()
        .unwrap_or_default()
        .contains("grant not registered"));
    assert!(
        worker.core.try_lock().is_err(),
        "responses must arrive while core remains held"
    );
    lock_thread.join().unwrap();
    let peer_auth:pa_types::daemon::DaemonPeerCommand=serde_json::from_value(json!({"type":"peer_auth","grantId":"repro-grant","token":"repro-secret","workerInstanceId":"instance","purpose":"session_client"})).unwrap();
    assert!(
        worker
            .peer_grants
            .authenticate(&peer_auth, &worker.grant_context(), crate::util::now_ms())
            .is_err(),
        "busy rejection must not leave a grant"
    );
    send(
        &mut writer,
        "worker_register_peer_transport",
        "after",
        json!({"type":"worker_register_peer_transport","grant":grant}),
    )
    .await;
    loop {
        let f = tokio::time::timeout(std::time::Duration::from_secs(2), reader.read_frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if f.header.get("requestId").and_then(Value::as_str) == Some("after") {
            assert_eq!(
                serde_json::from_slice::<Value>(&f.payload).unwrap()["success"],
                true
            );
            break;
        }
    }
    let ctx = worker.grant_context();
    assert!(worker
        .peer_grants
        .authenticate(&peer_auth, &ctx, crate::util::now_ms())
        .is_ok());
    assert!(
        worker
            .peer_grants
            .authenticate(&peer_auth, &ctx, crate::util::now_ms())
            .is_err(),
        "one-shot grant replay must fail"
    );
    writer.shutdown().await.unwrap();
    drop(writer);
    drop(reader);
    task.await.unwrap().unwrap();
}
