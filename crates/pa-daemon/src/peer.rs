//! Direct peer transport on the worker's own socket: a session client
//! never learns the worker's bootstrap token — the supervisor issues a
//! single-use grant (TTL 10s), the client presents it via `peer_auth`, and
//! it burns on first use (before the token is even checked), so a leaked
//! or replayed ticket is worthless.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pa_types::daemon::{
    is_session_plane_daemon_command, DaemonPeerCommand, DaemonWorkerCommand, DaemonWorkerPeerGrant,
};
use serde_json::Value;

use crate::protocol::{response_failure, response_success, DaemonResponse};
use crate::util::iso_to_unix_ms;
use crate::worker::AuthOutcome;
use crate::worker::Worker;

/// TS `PEER_GRANT_TTL_LIMIT_MS`: a grant whose expiry is further out than
/// this is invalid at registration, so a supervisor cannot mint long-lived
/// credentials even if compromised.
pub(crate) const PEER_GRANT_TTL_LIMIT_MS: i64 = 30_000;
/// TS `PEER_GRANT_LIMIT`: maximum concurrently live grants (expired grants
/// are swept first).
pub(crate) const PEER_GRANT_LIMIT: usize = 1024;

/// TS failure strings (wire parity).
pub(crate) const PEER_AUTH_FAILED: &str = "Peer authentication failed";
pub(crate) const PEER_GRANT_INVALID: &str = "Peer transport grant is invalid";
pub(crate) const PEER_COMMAND_NOT_ALLOWED: &str =
    "Command is not allowed on this direct peer transport";

/// TS purposes plus the Rust `worker` grant: a `worker` grant admits a
/// peer worker (agent-message delivery), not a session client.
pub(crate) const PEER_PURPOSE_SESSION_CLIENT: &str = "session_client";
pub(crate) const PEER_PURPOSE_WORKER: &str = "worker";

/// Whether a grant purpose is one this worker accepts.
pub(crate) fn peer_purpose_valid(purpose: &str) -> bool {
    purpose == PEER_PURPOSE_SESSION_CLIENT || purpose == PEER_PURPOSE_WORKER
}

/// The authenticated role one worker-socket connection holds.
#[derive(Debug, Clone)]
pub(crate) enum ConnectionRole {
    Unauthenticated,
    /// The supervisor: owns the worker, may send every worker command.
    Supervisor {
        generation: String,
    },
    /// A session client admitted through `peer_auth` with a single-use
    /// grant bound to one session.
    SessionClient {
        session: Arc<PeerSession>,
    },
    /// A peer worker admitted through `peer_auth` with a single-use
    /// `worker`-purpose grant: delivery only, no session events.
    PeerWorker {
        session: Arc<PeerSession>,
    },
}

impl ConnectionRole {
    /// Whether event fan-out may stream to this connection.
    pub(crate) fn streams_events(&self) -> bool {
        match self {
            ConnectionRole::Supervisor { .. } => true,
            ConnectionRole::SessionClient { session } => session.is_attached(),
            ConnectionRole::Unauthenticated | ConnectionRole::PeerWorker { .. } => false,
        }
    }
}

/// One admitted session client: the burned grant plus its attach state.
#[derive(Debug)]
pub(crate) struct PeerSession {
    pub(crate) grant: DaemonWorkerPeerGrant,
    attached: AtomicBool,
}

impl PeerSession {
    fn new(grant: DaemonWorkerPeerGrant) -> Self {
        PeerSession {
            grant,
            attached: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_attached(&self) -> bool {
        self.attached.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_attached(&self) {
        self.attached.store(true, Ordering::SeqCst);
    }

    pub(crate) fn mark_detached(&self) {
        self.attached.store(false, Ordering::SeqCst);
    }
}

/// The worker identity a grant must match (TS compares against the live
/// worker's own instance id and its known sessions).
pub(crate) struct GrantContext {
    pub(crate) worker_instance_id: String,
    pub(crate) active_session_id: String,
    /// The grant's session exists (the worker's create has completed).
    pub(crate) session_created: bool,
    pub(crate) root_session_id: String,
    pub(crate) session_file: String,
    pub(crate) cwd: String,
    pub(crate) generation: String,
}

/// In-memory grant store: worker-memory only, never persisted, swept on
/// every registration.
#[derive(Default)]
pub(crate) struct PeerGrantStore {
    grants: Mutex<HashMap<String, DaemonWorkerPeerGrant>>,
    issuer_incarnation: Mutex<Option<String>>,
}

impl PeerGrantStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Authenticated supervisor replacement revokes scoped grants, including burned ones.
    pub(crate) fn replace_issuer(&self, incarnation: Option<String>) {
        let mut current = self.issuer_incarnation.lock().unwrap();
        if *current != incarnation {
            self.grants
                .lock()
                .unwrap()
                .retain(|_, grant| grant.control_scope.is_none());
            *current = incarnation;
        }
    }

    pub(crate) fn scope_current(
        &self,
        grant: &DaemonWorkerPeerGrant,
        context: &GrantContext,
    ) -> bool {
        let Some(scope) = &grant.control_scope else {
            return true;
        };
        scope.validate().is_ok()
            && grant.purpose == PEER_PURPOSE_SESSION_CLIENT
            && grant.issuer_incarnation.is_some()
            && grant.issuer_incarnation == *self.issuer_incarnation.lock().unwrap()
            && grant.worker_instance_id == context.worker_instance_id
            && grant.control_generation.as_deref() == Some(context.generation.as_str())
            && context.session_created
            && scope.active_session_id == context.active_session_id
            && grant.active_session_id == context.active_session_id
            && scope.root_session_id == context.root_session_id
            && scope.session_file == context.session_file
            && scope.cwd == context.cwd
    }

    /// TS `worker_register_peer_transport` validation: the grant must name
    /// this exact worker instance and one of its live sessions, expire in
    /// the future but no further out than the TTL limit, and fit under the
    /// grant cap after the expired sweep.
    pub(crate) fn register(
        &self,
        mut grant: DaemonWorkerPeerGrant,
        context: &GrantContext,
        issuer_generation: &str,
        now_ms: u64,
    ) -> Result<(), &'static str> {
        // Serialize issuer replacement with registration; never downgrade malformed scopes.
        let issuer = self.issuer_incarnation.lock().unwrap();
        if let Some(scope) = &grant.control_scope {
            scope.validate()?;
            if grant.issuer_incarnation.is_none()
                || grant.issuer_incarnation != *issuer
                || grant.purpose != PEER_PURPOSE_SESSION_CLIENT
                || scope.active_session_id != context.active_session_id
                || scope.root_session_id != context.root_session_id
                || scope.session_file != context.session_file
                || scope.cwd != context.cwd
            {
                return Err("execution_authority_unavailable");
            }
            grant.control_generation = Some(context.generation.clone());
        } else if grant.issuer_incarnation.is_some() || grant.control_generation.is_some() {
            return Err(PEER_GRANT_INVALID);
        }
        let mut grants = self.grants.lock().unwrap();
        let now = now_ms as i64;
        grants.retain(|_, pending| match iso_to_unix_ms(&pending.expires_at) {
            Some(expires) => expires as i64 > now,
            None => false,
        });
        let expires = iso_to_unix_ms(&grant.expires_at).map(|ms| ms as i64);
        let valid_expiry = matches!(expires, Some(expires) if expires > now && expires - now <= PEER_GRANT_TTL_LIMIT_MS);
        let known_session =
            context.session_created && grant.active_session_id == context.active_session_id;
        if !peer_purpose_valid(&grant.purpose)
            || grant.grant_id.is_empty()
            || grant.token.is_empty()
            || grant.worker_instance_id != context.worker_instance_id
            || !known_session
            || grants.len() >= PEER_GRANT_LIMIT
            || !valid_expiry
            || grant.issuer_generation != issuer_generation
        {
            return Err(PEER_GRANT_INVALID);
        }
        grants.insert(grant.grant_id.clone(), grant);
        Ok(())
    }

    /// TS `peer_auth`: the grant burns first (single use, even on a failed
    /// presentation), then the token, instance, purpose, and expiry are
    /// checked against the burned grant. Success returns the grant so the
    /// connection can be bound to its session.
    pub(crate) fn authenticate(
        &self,
        presented: &DaemonPeerCommand,
        context: &GrantContext,
        now_ms: u64,
    ) -> Result<DaemonWorkerPeerGrant, &'static str> {
        let DaemonPeerCommand::PeerAuth {
            grant_id,
            token,
            worker_instance_id,
            purpose,
            ..
        } = presented;
        let burned = self.grants.lock().unwrap().remove(grant_id);
        let Some(grant) = burned else {
            return Err(PEER_AUTH_FAILED);
        };
        let expires_ok =
            matches!(iso_to_unix_ms(&grant.expires_at), Some(expires) if expires > now_ms);
        // The local instance id must match too: a grant minted for a dead
        // worker incarnation is useless here.
        let token_ok = grant_token_matches(token, &grant.token);
        let instance_ok = worker_instance_id.as_str() == grant.worker_instance_id.as_str()
            && grant.worker_instance_id == context.worker_instance_id;
        if token_ok
            && instance_ok
            && purpose.as_str() == grant.purpose.as_str()
            && peer_purpose_valid(&grant.purpose)
            && expires_ok
            && self.scope_current(&grant, context)
        {
            return Ok(grant);
        }
        Err(PEER_AUTH_FAILED)
    }
}

/// Timing-safe grant-token comparison: both sides are hashed (sha256, the
/// same digest TS compares) so the comparison never short-circuits on a
/// matching prefix.
fn grant_token_matches(presented: &str, expected: &str) -> bool {
    use sha2::{Digest, Sha256};
    let presented = Sha256::digest(presented.as_bytes());
    let expected = Sha256::digest(expected.as_bytes());
    constant_time_eq(&presented, &expected)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut difference = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}

/// Parse one `peer_auth` presentation from the wire.
pub(crate) fn parse_peer_auth(payload: &Value) -> Result<DaemonPeerCommand, &'static str> {
    serde_json::from_value::<DaemonPeerCommand>(payload.clone()).map_err(|_| PEER_AUTH_FAILED)
}

/// Parse one `worker_register_peer_transport` command from the wire.
pub(crate) fn parse_grant_registration(
    payload: &Value,
) -> Result<DaemonWorkerCommand, &'static str> {
    serde_json::from_value::<DaemonWorkerCommand>(payload.clone()).map_err(|_| PEER_GRANT_INVALID)
}

/// Command gate for a peer-worker connection: agent-message delivery
/// only, addressed to the grant's session.
pub(crate) fn worker_peer_command_allowed(
    command_type: &str,
    payload: &Value,
    grant: &DaemonWorkerPeerGrant,
) -> bool {
    command_type == "worker_deliver_message"
        && payload.get("targetActiveSessionId").and_then(Value::as_str)
            == Some(grant.active_session_id.as_str())
}

/// Session-plane gate for a direct peer command (TS `peerClaims` branch):
/// the command must be session-plane and address the grant's session.
pub(crate) fn peer_command_allowed(
    command_type: &str,
    payload: &Value,
    grant: &DaemonWorkerPeerGrant,
) -> bool {
    if matches!(
        command_type,
        "drone_snapshot" | "drone_recover" | "drone_status" | "drone_control"
    ) {
        return false;
    }
    let operation_allowed = match &grant.control_scope {
        Some(scope) => {
            scope.validate().is_ok()
                && scope
                    .operations
                    .iter()
                    .any(|operation| operation == command_type)
        }
        None => is_session_plane_daemon_command(command_type),
    };
    !(command_type == "configure_orchestration_gate"
        && (payload.get("quarantineSessions").is_some()
            || payload.get("acceptLegacyClosure").is_some()))
        && operation_allowed
        && !(command_type == "get_orchestration_gate"
            && (payload.get("historicalOwnerCancellation").is_some()
                || payload.get("privateOwnerProbe").is_some()))
        && payload.get("activeSessionId").and_then(Value::as_str)
            == Some(grant.active_session_id.as_str())
}

#[cfg(test)]
mod historical_lookup_tests {
    use super::*;
    #[test]
    fn drone_user_control_is_never_a_session_peer_capability() {
        let mut grant = DaemonWorkerPeerGrant {
            grant_id: "g".into(),
            token: "fixture".into(),
            expires_at: "future".into(),
            purpose: "session_client".into(),
            worker_instance_id: "worker".into(),
            active_session_id: "active".into(),
            issuer_generation: "sup".into(),
            control_scope: None,
            issuer_incarnation: None,
            control_generation: None,
        };
        let payload = serde_json::json!({"activeSessionId":"active","rootSessionId":"root","action":"stop","expectedRevision":0});
        assert!(!is_session_plane_daemon_command("drone_control"));
        assert!(!peer_command_allowed("drone_control", &payload, &grant));
        grant.control_scope = Some(pa_types::daemon::DaemonSessionControlScope {
            control_version: 1,
            root_session_id: "root".into(),
            active_session_id: "active".into(),
            session_file: "/sessions/root.jsonl".into(),
            cwd: "/owned".into(),
            operations: vec!["drone_control".into()],
        });
        assert!(!peer_command_allowed("drone_control", &payload, &grant));
    }

    #[test]
    fn historical_owner_cancellation_is_not_a_peer_client_or_worker_read() {
        let grant = DaemonWorkerPeerGrant {
            grant_id: "g".into(),
            token: "t".into(),
            expires_at: "future".into(),
            purpose: "session_client".into(),
            worker_instance_id: "i".into(),
            active_session_id: "parent".into(),
            issuer_generation: "epoch".into(),
            control_scope: None,
            issuer_incarnation: None,
            control_generation: None,
        };
        let plain = serde_json::json!({"activeSessionId":"parent"});
        let historical =
            serde_json::json!({"activeSessionId":"parent","historicalOwnerCancellation":{}});
        assert!(peer_command_allowed(
            "get_orchestration_gate",
            &plain,
            &grant
        ));
        assert!(!peer_command_allowed(
            "get_orchestration_gate",
            &historical,
            &grant
        ));
        assert!(!worker_peer_command_allowed(
            "get_orchestration_gate",
            &historical,
            &grant
        ));
    }
}

/// The grant fields a peer-auth success response echoes (TS `peer_auth`
/// response data).
pub(crate) fn peer_auth_success_data(grant: &DaemonWorkerPeerGrant) -> Value {
    let mut data = serde_json::json!({
        "workerInstanceId": grant.worker_instance_id,
        "activeSessionId": grant.active_session_id,
        "purpose": grant.purpose,
    });
    if let Some(scope) = &grant.control_scope {
        data["controlScope"] = serde_json::json!(scope);
        data["issuerIncarnation"] = serde_json::json!(grant.issuer_incarnation);
    }
    data
}

impl Worker {
    /// One `peer_auth` presentation on an unauthenticated connection: burn
    /// the grant, validate it, promote the connection, write the response.
    pub(crate) async fn handle_peer_auth(
        self: &Arc<Self>,
        payload: &Value,
        request_id: &str,
        role: &Arc<Mutex<ConnectionRole>>,
        sink: &crate::worker::ConnectionSink,
    ) -> AuthOutcome {
        let presentation = match parse_peer_auth(payload) {
            Ok(presentation) => presentation,
            Err(reason) => {
                let failure = response_failure(Some(request_id), "peer_auth", reason, None);
                self.write_response_frame(sink, request_id, failure).await;
                return AuthOutcome::Failed;
            }
        };
        let context = self.grant_context();
        match self
            .peer_grants
            .authenticate(&presentation, &context, crate::util::now_ms())
        {
            Ok(grant) => {
                let session = Arc::new(PeerSession::new(grant.clone()));
                *role.lock().unwrap() = if grant.purpose == PEER_PURPOSE_WORKER {
                    ConnectionRole::PeerWorker {
                        session: Arc::clone(&session),
                    }
                } else {
                    ConnectionRole::SessionClient {
                        session: Arc::clone(&session),
                    }
                };
                let success = response_success(
                    Some(request_id),
                    "peer_auth",
                    Some(peer_auth_success_data(&grant)),
                );
                self.write_response_frame(sink, request_id, success).await;
                AuthOutcome::Authenticated
            }
            Err(reason) => {
                let failure = response_failure(Some(request_id), "peer_auth", reason, None);
                self.write_response_frame(sink, request_id, failure).await;
                AuthOutcome::Failed
            }
        }
    }

    /// `worker_register_peer_transport` (supervisor role only): accept one
    /// single-use grant; the issuer generation must match the supervisor's.
    pub(crate) fn handle_worker_register_peer_transport(
        &self,
        payload: &Value,
        issuer_generation: &str,
    ) -> DaemonResponse {
        const COMMAND: &str = "worker_register_peer_transport";
        let grant = match parse_grant_registration(payload) {
            Ok(pa_types::daemon::DaemonWorkerCommand::WorkerRegisterPeerTransport {
                grant,
                ..
            }) => grant,
            Ok(_) => {
                return response_failure(None, COMMAND, PEER_GRANT_INVALID, None);
            }
            Err(reason) => {
                return response_failure(None, COMMAND, reason, None);
            }
        };
        // This handler runs inline on the supervisor IPC reader. Never block
        // the whole connection on a contended session lock: no grant has been
        // registered yet, so fail closed and let the supervisor reconcile.
        let Ok(core) = self.core.try_lock() else {
            return response_failure(
                None,
                COMMAND,
                "worker_state_unavailable: grant not registered",
                None,
            );
        };
        let context = self.grant_context_locked(&core);
        drop(core);
        match self
            .peer_grants
            .register(grant, &context, issuer_generation, crate::util::now_ms())
        {
            Ok(()) => response_success(None, COMMAND, None),
            Err(reason) => response_failure(None, COMMAND, reason, None),
        }
    }

    /// The worker identity grants are validated against, read under the
    /// core lock (the session-known check is the live `created` state).
    pub(crate) fn grant_context(&self) -> GrantContext {
        let core = self.core.lock().unwrap();
        self.grant_context_locked(&core)
    }

    fn grant_context_locked(&self, core: &crate::worker::SessionCore) -> GrantContext {
        GrantContext {
            worker_instance_id: self.config.worker_instance_id.clone(),
            active_session_id: self.config.active_session_id.clone(),
            session_created: core.created && !core.shutdown_requested,
            root_session_id: core
                .store
                .as_ref()
                .map(|store| store.session_id().to_string())
                .unwrap_or_default(),
            session_file: core
                .store
                .as_ref()
                .map(|store| store.path.to_string_lossy().to_string())
                .unwrap_or_default(),
            cwd: core.cwd.clone(),
            generation: core.generation.clone(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;

    fn grant_context() -> GrantContext {
        GrantContext {
            worker_instance_id: "inst-1".to_string(),
            active_session_id: "abc123".to_string(),
            session_created: true,
            root_session_id: "persistent-1".to_string(),
            session_file: "/sessions/1.jsonl".to_string(),
            cwd: "/project".to_string(),
            generation: "session-generation-1".to_string(),
        }
    }

    fn grant(expires_at: &str) -> DaemonWorkerPeerGrant {
        DaemonWorkerPeerGrant {
            grant_id: "g1".to_string(),
            token: "secret".to_string(),
            expires_at: expires_at.to_string(),
            purpose: "session_client".to_string(),
            worker_instance_id: "inst-1".to_string(),
            active_session_id: "abc123".to_string(),
            issuer_generation: "sup:1".to_string(),
            control_scope: None,
            issuer_incarnation: None,
            control_generation: None,
        }
    }

    fn peer_auth(token: &str) -> DaemonPeerCommand {
        DaemonPeerCommand::PeerAuth {
            id: None,
            grant_id: "g1".to_string(),
            token: token.to_string(),
            worker_instance_id: "inst-1".to_string(),
            purpose: "session_client".to_string(),
            rest: Map::default(),
        }
    }

    fn soon(now_ms: u64, delta_ms: i64) -> String {
        crate::util::iso_from_unix_ms(((now_ms as i64) + delta_ms) as u64)
    }

    const NOW: u64 = 1_000_000;

    fn scoped_grant() -> DaemonWorkerPeerGrant {
        let mut grant = grant(&soon(NOW, 10_000));
        grant.control_scope = Some(pa_types::daemon::DaemonSessionControlScope {
            control_version: 1,
            root_session_id: "persistent-1".into(),
            active_session_id: "abc123".into(),
            session_file: "/sessions/1.jsonl".into(),
            cwd: "/project".into(),
            operations: vec![
                "attach".into(),
                "get_queue".into(),
                "control_goal".into(),
                "steer".into(),
                "follow_up".into(),
                "mutate_queued_message".into(),
            ],
        });
        grant.issuer_incarnation = Some("boot-1".into());
        grant
    }

    #[test]
    fn scoped_connection_is_revoked_on_issuer_and_live_identity_replacement() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        store.replace_issuer(Some("boot-1".into()));
        store
            .register(scoped_grant(), &context, "sup:1", NOW)
            .unwrap();
        let admitted = store
            .authenticate(&peer_auth("secret"), &context, NOW)
            .unwrap();
        assert!(store.scope_current(&admitted, &context));
        let mut moved = grant_context();
        moved.cwd = "/other-project".into();
        assert!(!store.scope_current(&admitted, &moved));
        moved = grant_context();
        moved.session_file = "/sessions/other.jsonl".into();
        assert!(!store.scope_current(&admitted, &moved));
        moved = grant_context();
        moved.generation = "replacement".into();
        assert!(!store.scope_current(&admitted, &moved));
        moved = grant_context();
        moved.worker_instance_id = "other-instance".into();
        assert!(!store.scope_current(&admitted, &moved));
        moved = grant_context();
        moved.session_created = false;
        assert!(!store.scope_current(&admitted, &moved));
        store.replace_issuer(Some("boot-2".into()));
        assert!(!store.scope_current(&admitted, &context));
        assert_eq!(
            store.register(scoped_grant(), &context, "sup:1", NOW),
            Err("execution_authority_unavailable")
        );
    }

    #[test]
    fn scoped_auth_rejects_expiry_forgery_wrong_worker_and_invalid_scope() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        store.replace_issuer(Some("boot-1".into()));
        store
            .register(scoped_grant(), &context, "sup:1", NOW)
            .unwrap();
        assert_eq!(
            store
                .authenticate(&peer_auth("secret"), &context, NOW + 10_000)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
        let mut invalid = scoped_grant();
        invalid
            .control_scope
            .as_mut()
            .unwrap()
            .operations
            .push("shutdown".into());
        assert_eq!(
            store.register(invalid, &context, "sup:1", NOW),
            Err("invalid_control_scope")
        );
        store
            .register(scoped_grant(), &context, "sup:1", NOW)
            .unwrap();
        let mut wrong = grant_context();
        wrong.worker_instance_id = "wrong".into();
        assert_eq!(
            store
                .authenticate(&peer_auth("secret"), &wrong, NOW)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
        store
            .register(scoped_grant(), &context, "sup:1", NOW)
            .unwrap();
        let mut forged = peer_auth("secret");
        let DaemonPeerCommand::PeerAuth { grant_id, .. } = &mut forged;
        *grant_id = "forged".into();
        assert_eq!(
            store.authenticate(&forged, &context, NOW).unwrap_err(),
            PEER_AUTH_FAILED
        );
        assert!(store
            .authenticate(&peer_auth("secret"), &context, NOW)
            .is_ok());
    }

    #[test]
    fn scoped_grant_cannot_retarget_or_gain_legacy_session_plane_authority() {
        let scoped = scoped_grant();
        let target = serde_json::json!({"activeSessionId":"abc123"});
        assert!(peer_command_allowed("control_goal", &target, &scoped));
        for operation in [
            "new_session",
            "switch_session",
            "fork",
            "import_jsonl",
            "navigate_tree",
            "shutdown",
            "complete_owned_session",
            "configure_native_task_policy",
            "prompt",
        ] {
            assert!(
                !peer_command_allowed(operation, &target, &scoped),
                "{operation}"
            );
        }
        assert!(!peer_command_allowed(
            "get_queue",
            &serde_json::json!({"activeSessionId":"other"}),
            &scoped
        ));
        let legacy = grant(&soon(NOW, 10_000));
        assert!(peer_command_allowed("control_goal", &target, &legacy));
    }

    #[test]
    fn scoped_pending_grant_clears_on_boot_replacement_and_wrong_token_burns() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        store.replace_issuer(Some("boot-1".into()));
        store
            .register(scoped_grant(), &context, "sup:1", NOW)
            .unwrap();
        assert_eq!(
            store
                .authenticate(&peer_auth("wrong"), &context, NOW)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
        assert_eq!(
            store
                .authenticate(&peer_auth("secret"), &context, NOW)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
        store
            .register(scoped_grant(), &context, "sup:1", NOW)
            .unwrap();
        store.replace_issuer(Some("boot-2".into()));
        assert_eq!(
            store
                .authenticate(&peer_auth("secret"), &context, NOW)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
        // Unscoped grants retain compatibility across authenticated boot replacement.
        store
            .register(grant(&soon(NOW, 10_000)), &context, "sup:1", NOW)
            .unwrap();
        store.replace_issuer(Some("boot-3".into()));
        assert!(store
            .authenticate(&peer_auth("secret"), &context, NOW)
            .is_ok());
    }

    #[test]
    fn grant_issue_burn_single_use() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        store
            .register(grant(&soon(NOW, 10_000)), &context, "sup:1", NOW)
            .expect("register");
        let admitted = store
            .authenticate(&peer_auth("secret"), &context, NOW)
            .expect("auth");
        assert_eq!(admitted.active_session_id, "abc123");
        let replay = store.authenticate(&peer_auth("secret"), &context, NOW);
        assert_eq!(replay.unwrap_err(), PEER_AUTH_FAILED);
    }

    #[test]
    fn grant_burns_even_on_wrong_token() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        store
            .register(grant(&soon(NOW, 10_000)), &context, "sup:1", NOW)
            .expect("register");
        assert_eq!(
            store
                .authenticate(&peer_auth("wrong"), &context, NOW)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
        // Burned by the failed attempt: the correct token no longer works.
        assert_eq!(
            store
                .authenticate(&peer_auth("secret"), &context, NOW)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
    }

    #[test]
    fn grant_expires() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        store
            .register(grant(&soon(NOW, 10_000)), &context, "sup:1", NOW)
            .expect("register");
        let later = NOW + 10_001;
        assert_eq!(
            store
                .authenticate(&peer_auth("secret"), &context, later)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
    }

    #[test]
    fn grant_registration_rejects_bad_shape() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        let mut expired = grant(&soon(NOW, 10_000));
        expired.expires_at = soon(NOW, -1);
        assert_eq!(
            store.register(expired, &context, "sup:1", NOW).unwrap_err(),
            PEER_GRANT_INVALID
        );
        // TTL over the limit (31s out).
        assert_eq!(
            store
                .register(grant(&soon(NOW, 31_000)), &context, "sup:1", NOW)
                .unwrap_err(),
            PEER_GRANT_INVALID
        );
        let mut purpose = grant(&soon(NOW, 10_000));
        purpose.purpose = "supervisor".to_string();
        assert_eq!(
            store.register(purpose, &context, "sup:1", NOW).unwrap_err(),
            PEER_GRANT_INVALID
        );
        let mut instance = grant(&soon(NOW, 10_000));
        instance.worker_instance_id = "inst-2".to_string();
        assert_eq!(
            store
                .register(instance, &context, "sup:1", NOW)
                .unwrap_err(),
            PEER_GRANT_INVALID
        );
        let mut session = grant(&soon(NOW, 10_000));
        session.active_session_id = "other".to_string();
        assert_eq!(
            store.register(session, &context, "sup:1", NOW).unwrap_err(),
            PEER_GRANT_INVALID
        );
        let mut uncreated = grant_context();
        uncreated.session_created = false;
        assert_eq!(
            store
                .register(grant(&soon(NOW, 10_000)), &uncreated, "sup:1", NOW)
                .unwrap_err(),
            PEER_GRANT_INVALID
        );
        assert_eq!(
            store
                .register(grant(&soon(NOW, 10_000)), &context, "sup:2", NOW)
                .unwrap_err(),
            PEER_GRANT_INVALID
        );
        store
            .register(grant(&soon(NOW, 10_000)), &context, "sup:1", NOW)
            .expect("register");
    }

    #[test]
    fn grant_cap_sweeps_expired() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        for index in 0..PEER_GRANT_LIMIT {
            let mut one = grant(&soon(NOW, 10_000));
            one.grant_id = format!("live-{index}");
            store
                .register(one, &context, "sup:1", NOW)
                .expect("register within cap");
        }
        let mut over = grant(&soon(NOW, 10_000));
        over.grant_id = "over-cap".to_string();
        assert_eq!(
            store.register(over, &context, "sup:1", NOW).unwrap_err(),
            PEER_GRANT_INVALID
        );
        // Advance past the live grants' expiry: the registration sweep
        // frees the cap for fresh grants.
        let later = NOW + 10_001;
        let mut fresh = grant(&soon(later, 10_000));
        fresh.grant_id = "fresh".to_string();
        store
            .register(fresh, &context, "sup:1", later)
            .expect("swept the expired grants");
        let mut stale_auth = peer_auth("secret");
        let DaemonPeerCommand::PeerAuth {
            grant_id: stale_id, ..
        } = &mut stale_auth;
        *stale_id = "live-0".to_string();
        assert_eq!(
            store
                .authenticate(&stale_auth, &context, later)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
    }

    #[test]
    fn peer_gate_requires_session_plane_and_matching_session() {
        let grant = grant(&soon(NOW, 10_000));
        let attach = serde_json::json!({ "activeSessionId": "abc123" });
        let wrong = serde_json::json!({ "activeSessionId": "other" });
        assert!(peer_command_allowed("attach", &attach, &grant));
        assert!(peer_command_allowed("prompt", &attach, &grant));
        // The abort-and-send interrupt rides the session plane too (TS
        // `abort_and_send_queued: "session"`).
        assert!(peer_command_allowed(
            "abort_and_send_queued",
            &attach,
            &grant
        ));
        assert!(!peer_command_allowed("prompt", &wrong, &grant));
        assert!(!peer_command_allowed(
            "attach",
            &serde_json::json!({}),
            &grant
        ));
        assert!(!peer_command_allowed("shutdown", &attach, &grant));
        assert!(!peer_command_allowed("create", &attach, &grant));
        assert!(!peer_command_allowed("rename", &attach, &grant));
        assert!(!peer_command_allowed(
            "worker_register_peer_transport",
            &attach,
            &grant
        ));
    }

    #[test]
    fn worker_purpose_grants_burn_and_gate_to_delivery() {
        let store = PeerGrantStore::new();
        let context = grant_context();
        let mut grant = grant(&soon(NOW, 10_000));
        grant.purpose = "worker".to_string();
        store
            .register(grant.clone(), &context, "sup:1", NOW)
            .expect("worker grant registers");
        let mut presentation = peer_auth("secret");
        let DaemonPeerCommand::PeerAuth {
            purpose: auth_purpose,
            ..
        } = &mut presentation;
        *auth_purpose = "worker".to_string();
        let admitted = store
            .authenticate(&presentation, &context, NOW)
            .expect("worker grant authenticates");
        assert_eq!(admitted.purpose, "worker");
        assert_eq!(
            store
                .authenticate(&presentation, &context, NOW)
                .unwrap_err(),
            PEER_AUTH_FAILED
        );
        let delivery = serde_json::json!({ "targetActiveSessionId": "abc123" });
        let wrong = serde_json::json!({ "targetActiveSessionId": "other" });
        assert!(worker_peer_command_allowed(
            "worker_deliver_message",
            &delivery,
            &grant
        ));
        assert!(!worker_peer_command_allowed(
            "worker_deliver_message",
            &wrong,
            &grant
        ));
        assert!(!worker_peer_command_allowed("get_state", &delivery, &grant));
        assert!(!worker_peer_command_allowed("list", &delivery, &grant));
        assert!(!worker_peer_command_allowed("shutdown", &delivery, &grant));
        let role = ConnectionRole::PeerWorker {
            session: Arc::new(PeerSession::new(grant)),
        };
        assert!(!role.streams_events());
    }

    #[test]
    fn peer_session_attach_gates_event_stream() {
        let role = ConnectionRole::SessionClient {
            session: Arc::new(PeerSession::new(grant(&soon(NOW, 10_000)))),
        };
        assert!(!role.streams_events(), "not attached yet");
        if let ConnectionRole::SessionClient { session } = &role {
            session.mark_attached();
        }
        assert!(role.streams_events());
        if let ConnectionRole::SessionClient { session } = &role {
            session.mark_detached();
        }
        assert!(!role.streams_events());
        assert!(!ConnectionRole::Unauthenticated.streams_events());
        assert!(ConnectionRole::Supervisor {
            generation: "sup:1".to_string()
        }
        .streams_events());
    }
}
