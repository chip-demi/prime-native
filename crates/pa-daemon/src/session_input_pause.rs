//! The session input-pause surface: the worker arms plus the input-admission
//! gate the turn runner consults before it admits queued work. While any pause
//! is held, queued input stays queued; a pause is leased to one owner identity:
//! reacquiring with the same lease key answers the existing id, and a foreign
//! release answers the TS ownership error.

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::{json, Value};

use crate::protocol::{response_failure, response_success, DaemonResponse};
use crate::worker::Worker;

/// One held pause: the session it gates, the owning client identity, and
/// the lease key (a reacquire from the same connection deduplicates).
#[derive(Debug, Clone, PartialEq, Eq)]
enum PauseOwner {
    Client(String),
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InputPauseEntry {
    active_session_id: String,
    owner: PauseOwner,
    lease_key: String,
}

/// The worker's pause table: pause id -> entry. Cheap to clone: the
/// table is one allocation shared by the worker and its turn runner.
#[derive(Default, Clone)]
pub(crate) struct InputPauseTable {
    pauses: std::sync::Arc<Mutex<HashMap<String, InputPauseEntry>>>,
}

/// The release outcome (TS error strings surface through the arm).
enum ReleaseOutcome {
    Released,
    Unknown,
    /// A foreign owner or session (the TS arm answers the same ownership
    /// error for both).
    OwnedByAnotherClient,
}

impl InputPauseTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Whether any pause is held (the runner's admission gate).
    pub(crate) fn paused(&self) -> bool {
        !self
            .pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }

    pub(crate) fn with_unpaused<T>(&self, admit: impl FnOnce() -> T) -> Option<T> {
        let pauses = self.pauses.lock().unwrap();
        if !pauses.is_empty() {
            return None;
        }
        Some(admit())
    }

    /// `acquire_session_input_pause`: dedupe on (owner, session, lease key) —
    /// an identical lease answers its existing pause id, else a fresh id.
    fn acquire(&self, active_session_id: &str, owner_client_id: &str, lease_key: &str) -> String {
        self.acquire_owned(
            active_session_id,
            PauseOwner::Client(owner_client_id.to_string()),
            lease_key,
        )
    }

    fn acquire_owned(&self, active_session_id: &str, owner: PauseOwner, lease_key: &str) -> String {
        let mut pauses = self
            .pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (pause_id, entry) in pauses.iter() {
            if entry.active_session_id == active_session_id
                && entry.owner == owner
                && entry.lease_key == lease_key
            {
                return pause_id.clone();
            }
        }
        let pause_id = uuid::Uuid::new_v4().to_string();
        pauses.insert(
            pause_id.clone(),
            InputPauseEntry {
                active_session_id: active_session_id.to_string(),
                owner,
                lease_key: lease_key.to_string(),
            },
        );
        pause_id
    }

    /// `release_session_input_pause`: `Unknown` answers the plain TS success
    /// (idempotent), a foreign owner the TS ownership error, `Released` lifts the gate.
    fn release(
        &self,
        pause_id: &str,
        owner_client_id: &str,
        active_session_id: &str,
    ) -> ReleaseOutcome {
        self.release_owned(
            pause_id,
            &PauseOwner::Client(owner_client_id.to_string()),
            active_session_id,
        )
    }

    fn release_owned(
        &self,
        pause_id: &str,
        owner: &PauseOwner,
        active_session_id: &str,
    ) -> ReleaseOutcome {
        let mut pauses = self
            .pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = pauses.get(pause_id) else {
            return ReleaseOutcome::Unknown;
        };
        if &entry.owner != owner || entry.active_session_id != active_session_id {
            return ReleaseOutcome::OwnedByAnotherClient;
        }
        pauses.remove(pause_id);
        ReleaseOutcome::Released
    }

    /// `detach` (and worker shutdown) drops every pause this owner holds
    /// (TS `detach` releases the client's pauses before detaching).
    fn release_all_for_owner(&self, owner_client_id: &str) -> bool {
        let mut pauses = self
            .pauses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = pauses.len();
        pauses.retain(
            |_, entry| !matches!(&entry.owner, PauseOwner::Client(id) if id == owner_client_id),
        );
        before != pauses.len()
    }
}

/// The owner identity of a daemon-internal queued-work pause.
const INTERNAL_PAUSE_OWNER: &str = "worker-internal";

/// A held queued-work pause for one daemon-internal flow (TS
/// `_navigateTree`'s `acquireQueuedWorkPause`): the runner parks queued input
/// until the guard drops, which releases through the wire handler's lift.
pub(crate) struct QueuedWorkPause<'a> {
    worker: &'a Worker,
    pause_id: String,
}

impl Worker {
    /// Hold the queued-work pause for one daemon-internal flow. The lease
    /// key carries a fresh uuid so concurrent flows hold distinct pauses.
    pub(crate) fn hold_queued_work_pause(&self, purpose: &str) -> QueuedWorkPause<'_> {
        let lease_key = format!("internal-{purpose}-{}", uuid::Uuid::new_v4());
        let pause_id = self.input_pauses.acquire_owned(
            &self.config.active_session_id,
            PauseOwner::Internal,
            &lease_key,
        );
        // The runner re-evaluates its admission gate and parks.
        self.idle_notify.notify_waiters();
        QueuedWorkPause {
            worker: self,
            pause_id,
        }
    }

    /// One release lift shared by the wire handler and the internal guard:
    /// every `Released` outcome wakes the runner and re-evaluates the owed
    /// goal continuation.
    fn release_session_pause(
        &self,
        pause_id: &str,
        owner_client_id: &str,
        active_session_id: &str,
        internal: bool,
    ) -> ReleaseOutcome {
        let result = if internal {
            self.input_pauses
                .release_owned(pause_id, &PauseOwner::Internal, active_session_id)
        } else {
            self.input_pauses
                .release(pause_id, owner_client_id, active_session_id)
        };
        match result {
            ReleaseOutcome::Released => {
                self.work_notify.notify_one();
                if let Some(engine) = self.agent_engine.as_ref() {
                    engine.retry_owed_goal_continuation();
                }
                ReleaseOutcome::Released
            }
            other => other,
        }
    }

    /// `acquire_session_input_pause`: the pause id wire object `{ pauseId
    /// }`, with the dedupe on the identical lease.
    pub(crate) fn handle_acquire_session_input_pause(&self, payload: &Value) -> DaemonResponse {
        if let Err(response) = self.require_created("acquire_session_input_pause") {
            return response;
        }
        let active_session_id = payload
            .get("activeSessionId")
            .and_then(Value::as_str)
            .unwrap_or(&self.config.active_session_id)
            .to_string();
        let Some(lease_key) = payload.get("leaseKey").and_then(Value::as_str) else {
            return response_failure(
                None,
                "acquire_session_input_pause",
                "acquire_session_input_pause requires a leaseKey",
                None,
            );
        };
        let owner_client_id = payload
            .get("clientId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let pause_id = self
            .input_pauses
            .acquire(&active_session_id, &owner_client_id, lease_key);
        self.idle_notify.notify_waiters();
        response_success(
            None,
            "acquire_session_input_pause",
            Some(json!({ "pauseId": pause_id })),
        )
    }

    /// `release_session_input_pause`: the TS outcome ladder — an unknown id
    /// answers the plain success, a foreign owner the ownership error.
    pub(crate) fn handle_release_session_input_pause(&self, payload: &Value) -> DaemonResponse {
        if let Err(response) = self.require_created("release_session_input_pause") {
            return response;
        }
        let pause_id = payload
            .get("pauseId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let active_session_id = payload
            .get("activeSessionId")
            .and_then(Value::as_str)
            .unwrap_or(&self.config.active_session_id)
            .to_string();
        let owner_client_id = payload
            .get("clientId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        match self.release_session_pause(pause_id, &owner_client_id, &active_session_id, false) {
            ReleaseOutcome::Released | ReleaseOutcome::Unknown => {
                response_success(None, "release_session_input_pause", None)
            }
            ReleaseOutcome::OwnedByAnotherClient => response_failure(
                None,
                "release_session_input_pause",
                &format!("Session input pause is owned by another client: {pause_id}"),
                None,
            ),
        }
    }

    /// `detach` (TS worker arm): every pause this client holds on the
    /// detaching session goes with the detach.
    pub(crate) fn release_input_pauses_for_detach(&self, owner_client_id: &str) {
        if self.input_pauses.release_all_for_owner(owner_client_id) {
            self.work_notify.notify_one();
        }
    }
}

impl Drop for QueuedWorkPause<'_> {
    fn drop(&mut self) {
        let _ = self.worker.release_session_pause(
            &self.pause_id,
            INTERNAL_PAUSE_OWNER,
            &self.worker.config.active_session_id,
            true,
        );
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn drone_pause_acquire_cannot_overtake_locked_registration() {
        let table = super::InputPauseTable::new();
        let (inside_tx, inside_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let gate = table.clone();
        let register = std::thread::spawn(move || {
            gate.with_unpaused(|| {
                inside_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
        });
        inside_rx.recv().unwrap();
        let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let pause = table.clone();
        let acquire = std::thread::spawn(move || {
            attempt_tx.send(()).unwrap();
            let id = pause.acquire("root", "owner", "p");
            acquired_tx.send(id).unwrap();
        });
        attempt_rx.recv().unwrap();
        assert!(acquired_rx.try_recv().is_err());
        release_tx.send(()).unwrap();
        assert!(register.join().unwrap().is_some());
        acquired_rx.recv().unwrap();
        acquire.join().unwrap();
        assert!(table.with_unpaused(|| ()).is_none());
    }
    #[test]
    fn drone_pause_and_request_registration_have_one_linearization_point() {
        let table = super::InputPauseTable::new();
        let token = table.acquire("root", "owner", "stop");
        let calls = std::sync::atomic::AtomicUsize::new(0);
        assert!(table
            .with_unpaused(|| calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
            .is_none());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let _ = table.release(&token, "owner", "root");
        assert!(table
            .with_unpaused(|| calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
            .is_some());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    async fn created_worker() -> Arc<Worker> {
        let dir = std::env::temp_dir().join(format!("pa-worker-pause-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = crate::worker::WorkerConfig {
            socket_path: dir.join("worker.sock"),
            supervisor_socket_path: std::path::PathBuf::new(),
            token: "token".to_string(),
            worker_instance_id: String::new(),
            active_session_id: "pause-session".to_string(),
            agent_dir: dir.join("agent"),
            recovery_journal_path: dir.join("recovery.jsonl"),
            telemetry_disabled: None,
            script: Some(json!({ "responses": ["ack"] })),
        };
        let worker = Arc::new(Worker::new(config, None));
        let created = worker
            .dispatch(
                "create",
                &json!({ "noSession": true, "cwd": "/tmp", "name": "pauses" }),
            )
            .await;
        assert!(created.success, "create failed: {created:?}");
        worker
    }

    #[tokio::test]
    async fn detach_with_internal_name_cannot_release_internal_pause() {
        let worker = created_worker().await;
        let pause = worker.hold_queued_work_pause("test");
        assert!(worker.input_pauses.paused());
        let forged_release = worker.handle_release_session_input_pause(&json!({
            "clientId": "worker-internal", "pauseId": pause.pause_id,
        }));
        assert!(
            !forged_release.success,
            "client cannot release internal pause by ID"
        );
        let external = worker.handle_acquire_session_input_pause(&json!({
            "clientId": "worker-internal", "leaseKey": "external",
        }));
        assert!(external.success);
        let result = worker.handle_detach(&json!({"clientId": "worker-internal"}));
        assert!(result.success);
        assert!(
            worker.input_pauses.paused(),
            "client detach must not release internal pause"
        );
        drop(pause);
        assert!(!worker.input_pauses.paused());
    }

    #[tokio::test]
    async fn a_pause_wakes_idle_wait_holding_queued_input() {
        let worker = created_worker().await;
        {
            let mut core = worker.core.lock().unwrap();
            core.queued_input_suspended = true;
            core.follow_up.push_back(crate::worker::QueuedItem {
                priority: crate::worker::QueuePriority::Human,
                preview: None,
                message: "held follow-up".to_string(),
                custom_message: None,
                agent_message: None,
                input_identity: None,
                queue_key: None,
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: crate::worker::TurnPolicy::Queued,
                forced_batch: false,
            });
        }
        // Clear a create-time permit so acquisition must wake this waiter.
        worker.idle_notify.notify_one();
        worker.idle_notify.notified().await;
        let waiting = tokio::spawn({
            let worker = Arc::clone(&worker);
            async move { worker.wait_until_idle().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished(), "queued input holds the idle wait");
        let pause = worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId": "pause-session", "leaseKey": "idle-wake", "clientId": "test"
                }),
            )
            .await;
        assert!(pause.success, "pause failed: {pause:?}");
        tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
            .await
            .expect("pause acquisition did not wake the idle waiter")
            .expect("idle waiter panicked");
    }

    /// Wire shape: acquire answers `{ pauseId }`, the same lease
    /// deduplicates to the same id, a different lease mints a new one.
    #[tokio::test]
    async fn acquire_dedupes_on_the_lease_key() {
        let worker = created_worker().await;
        let first = worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId": "pause-session",
                    "leaseKey": "[\"conn-1\",\"owner\",\"lease-1\"]",
                    "clientId": "conn-1",
                }),
            )
            .await;
        assert!(first.success, "{first:?}");
        let pause_id = first.data.unwrap()["pauseId"].clone();
        assert!(pause_id.as_str().is_some_and(|id| !id.is_empty()));

        let again = worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId": "pause-session",
                    "leaseKey": "[\"conn-1\",\"owner\",\"lease-1\"]",
                    "clientId": "conn-1",
                }),
            )
            .await;
        assert_eq!(again.data.unwrap()["pauseId"], pause_id);

        let other = worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({
                    "activeSessionId": "pause-session",
                    "leaseKey": "[\"conn-1\",\"owner\",\"lease-2\"]",
                    "clientId": "conn-1",
                }),
            )
            .await;
        assert_ne!(other.data.unwrap()["pauseId"], pause_id);
    }

    #[tokio::test]
    async fn release_answers_the_ts_outcome_ladder() {
        let worker = created_worker().await;
        let acquire = worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({ "activeSessionId": "pause-session", "leaseKey": "k", "clientId": "conn-1" }),
            )
            .await;
        let pause_id = acquire.data.unwrap()["pauseId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(worker.input_pauses.paused());

        // Unknown id: the plain TS success.
        let unknown = worker
            .dispatch(
                "release_session_input_pause",
                &json!({ "activeSessionId": "pause-session", "pauseId": "ghost", "clientId": "conn-1" }),
            )
            .await;
        assert!(unknown.success);
        assert_eq!(unknown.data, None);

        // Foreign owner: the TS ownership error.
        let foreign = worker
            .dispatch(
                "release_session_input_pause",
                &json!({ "activeSessionId": "pause-session", "pauseId": pause_id, "clientId": "conn-2" }),
            )
            .await;
        assert!(!foreign.success);
        assert_eq!(
            foreign.error.as_deref(),
            Some(format!("Session input pause is owned by another client: {pause_id}").as_str())
        );

        // Owner release lifts the gate.
        let released = worker
            .dispatch(
                "release_session_input_pause",
                &json!({ "activeSessionId": "pause-session", "pauseId": pause_id, "clientId": "conn-1" }),
            )
            .await;
        assert!(released.success);
        assert!(!worker.input_pauses.paused());
    }

    #[tokio::test]
    async fn the_gate_holds_queued_input_until_released() {
        let worker = created_worker().await;
        worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({ "activeSessionId": "pause-session", "leaseKey": "k", "clientId": "conn-1" }),
            )
            .await;
        assert!(worker.input_pauses.paused());
        worker.release_input_pauses_for_detach("conn-1");
        assert!(!worker.input_pauses.paused());
        // A foreign client's detach release does not lift the gate.
        worker
            .dispatch(
                "acquire_session_input_pause",
                &json!({ "activeSessionId": "pause-session", "leaseKey": "k", "clientId": "conn-1" }),
            )
            .await;
        worker.release_input_pauses_for_detach("conn-2");
        assert!(worker.input_pauses.paused());
    }
}
