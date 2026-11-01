//! Protocol-scoped launch owner authentication; not same-UID isolation.
use anyhow::{anyhow, Result};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::Path;
/// Private in-memory generation state, never serialized.
pub struct LaunchOwner {
    launch_id: String,
    nonce: String,
    key: Vec<u8>,
    instance_id: String,
    source: Source,
    sequence: u64,
    last: Option<(String, Value)>,
    bootstrap_last: Option<(String, Value)>,
    managed: ManagedOwner,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Source {
    socket_path: String,
    token: String,
    instance_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Setup {
    version: u32,
    kind: String,
    launch_id: String,
    generation_nonce: String,
    daemon_socket_path: String,
    setup_purpose: String,
    owner_key_hex: String,
    source: Source,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Probe {
    version: u32,
    launch_id: String,
    generation_nonce: String,
    sequence: u64,
    payload: String,
    mac_hex: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    operation: String,
}
// Recursive duplicate rejection; serde_json Value alone silently replaces keys.
struct StrictValue(Value);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StrictValue;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "strict JSON")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut out = serde_json::Map::new();
                while let Some(k) = map.next_key::<String>()? {
                    if out.contains_key(&k) {
                        return Err(serde::de::Error::custom("Duplicate private field"));
                    }
                    let v = map.next_value::<StrictValue>()?;
                    out.insert(k, v.0);
                }
                Ok(StrictValue(Value::Object(out)))
            }
            fn visit_seq<S: serde::de::SeqAccess<'de>>(
                self,
                mut seq: S,
            ) -> std::result::Result<Self::Value, S::Error> {
                let mut out = Vec::new();
                while let Some(v) = seq.next_element::<StrictValue>()? {
                    out.push(v.0);
                }
                Ok(StrictValue(Value::Array(out)))
            }
            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(v.into())))
            }
            fn visit_bool<E: serde::de::Error>(
                self,
                v: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(serde_json::json!(v)))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
        }
        d.deserialize_any(Visitor)
    }
}
pub(crate) fn strict(bytes: &[u8]) -> Result<Value> {
    Ok(serde_json::from_slice::<StrictValue>(bytes)?.0)
}
fn unhex(text: &str) -> Result<Vec<u8>> {
    anyhow::ensure!(
        text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit()),
        "Invalid private encoding"
    );
    (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(Into::into))
        .collect()
}
fn id(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 100
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn message(launch: &str, nonce: &str, sequence: u64, payload: &str) -> Vec<u8> {
    let mut bytes = b"prime-studio-owner-probe-v1\0".to_vec();
    for part in [launch, nonce, &sequence.to_string(), payload] {
        bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    bytes
}
impl LaunchOwner {
    fn validate_bootstrap_payload(&self, payload: &mut Value) -> Result<()> {
        anyhow::ensure!(
            matches!(
                payload["operation"].as_str(),
                Some("owner_bootstrap_preflight" | "owner_bootstrap_commit")
            ) && payload.get("pinnedSource").is_none(),
            "Private bootstrap operation unavailable"
        );
        let operation = payload["operation"].as_str().unwrap_or_default();
        let common = [
            "version",
            "operation",
            "rootSessionId",
            "activeSessionId",
            "sessionFile",
            "cwd",
            "expectedGoalId",
            "intendedWork",
        ];
        let commit = [
            "preflightId",
            "operationId",
            "mode",
            "expectedGateRevision",
            "expectedAccountingEpoch",
            "expectedTask",
            "expectedPolicy",
            "expectedAssignments",
            "envelope",
            "callback",
        ];
        anyhow::ensure!(
            payload["version"] == 1
                && payload
                    .as_object()
                    .is_some_and(|obj| obj.keys().all(|k| common.contains(&k.as_str())
                        || (operation == "owner_bootstrap_commit"
                            && commit.contains(&k.as_str())))),
            "Unexpected owner fields"
        );
        anyhow::ensure!(
            [
                "rootSessionId",
                "activeSessionId",
                "sessionFile",
                "cwd",
                "expectedGoalId"
            ]
            .iter()
            .all(|key| payload[*key]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 4096))
                && payload["intendedWork"]
                    .as_object()
                    .is_some_and(|o| o.len() == 2
                        && ["planId", "stepId"].iter().all(|key| o
                            .get(*key)
                            .and_then(Value::as_str)
                            .is_some_and(|s| !s.is_empty()))),
            "Invalid owner target"
        );
        payload["pinnedSource"] = serde_json::json!({"socketPath":self.source.socket_path,"token":self.source.token,"instanceId":self.source.instance_id});
        Ok(())
    }

    pub(crate) fn authenticate_bootstrap(&mut self, value: &Value) -> Result<Value> {
        anyhow::ensure!(!self.managed.bootstrap_revoked, "Private owner revoked");
        let probe: Probe = serde_json::from_value(value.clone())?;
        anyhow::ensure!(
            probe.version == 1
                && probe.launch_id == self.launch_id
                && probe.generation_nonce == self.nonce
                && (probe.sequence == self.sequence + 1 || probe.sequence == self.sequence)
                && probe.payload.len() <= 32768,
            "Private bootstrap identity mismatch"
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| anyhow!("Invalid private key"))?;
        mac.update(&message(
            &probe.launch_id,
            &probe.generation_nonce,
            probe.sequence,
            &probe.payload,
        ));
        mac.verify_slice(&unhex(&probe.mac_hex)?)
            .map_err(|_| anyhow!("Private bootstrap authentication failed"))?;
        if probe.sequence == self.sequence {
            return self
                .bootstrap_last
                .as_ref()
                .filter(|(raw, _)| raw == &probe.payload)
                .map(|(_, payload)| payload.clone())
                .ok_or_else(|| anyhow!("Changed bootstrap replay"));
        }
        let mut payload = strict(probe.payload.as_bytes())?;
        self.validate_bootstrap_payload(&mut payload)?;
        self.sequence = probe.sequence;
        self.bootstrap_last = Some((probe.payload, payload.clone()));
        Ok(payload)
    }
    pub(crate) fn probe(&mut self, value: &Value) -> Result<Value> {
        anyhow::ensure!(!self.managed.bootstrap_revoked, "Private owner revoked");
        let probe: Probe = serde_json::from_value(value.clone())?;
        anyhow::ensure!(
            probe.version == 1
                && probe.launch_id == self.launch_id
                && probe.generation_nonce == self.nonce
                && probe.sequence > 0
                && probe.sequence <= 9_007_199_254_740_991
                && probe.payload.len() <= 1024,
            "Private owner identity mismatch"
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| anyhow!("Invalid private key"))?;
        mac.update(&message(
            &probe.launch_id,
            &probe.generation_nonce,
            probe.sequence,
            &probe.payload,
        ));
        mac.verify_slice(&unhex(&probe.mac_hex)?)
            .map_err(|_| anyhow!("Private owner authentication failed"))?;
        let payload: Payload = serde_json::from_value(strict(probe.payload.as_bytes())?)?;
        anyhow::ensure!(
            payload.operation == "owner_probe",
            "Private operation unavailable"
        );
        let digest = format!("{:x}", Sha256::digest(probe.payload.as_bytes()));
        if probe.sequence == self.sequence {
            let (old, result) = self
                .last
                .as_ref()
                .ok_or_else(|| anyhow!("Private replay unavailable"))?;
            anyhow::ensure!(old == &digest, "Changed private replay");
            return Ok(result.clone());
        }
        anyhow::ensure!(
            probe.sequence == self.sequence + 1,
            "Stale private sequence"
        );
        self.sequence = probe.sequence;
        let result = serde_json::json!({"version":1,"privateOwnerAuthenticated":true,"launchId":self.launch_id,"generationNonce":self.nonce,"sourceInstanceId":self.instance_id,"operation":"owner_probe","admissionPermission":false,"bootstrapApplied":false});
        self.last = Some((digest, result.clone()));
        Ok(result)
    }
}
/// Consume and close inherited setup before the public supervisor starts.
///
/// # Errors
/// Returns an error for setup I/O failures, deadlines, malformed frames, or
/// mismatched launch/source identities.
#[cfg(target_os = "linux")]
pub fn setup(mut stream: std::os::unix::net::UnixStream, socket: &Path) -> Result<LaunchOwner> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
    let nonce = uuid::Uuid::new_v4().to_string();
    writeln!(
        stream,
        "{}",
        serde_json::json!({"version":1,"kind":"ready","generationNonce":nonce,"supervisorPid":std::process::id()})
    )?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut bytes = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.set_read_timeout(Some(
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .max(std::time::Duration::from_millis(1)),
        ))?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "Private setup deadline"
        );
        anyhow::ensure!(bytes.len() < 16384, "Private setup exceeds bound");
        stream.read_exact(&mut byte)?;
        if byte[0] == b'\n' {
            break;
        }
        bytes.push(byte[0]);
    }
    let setup: Setup = serde_json::from_value(strict(&bytes)?)?;
    anyhow::ensure!(
        setup.version == 1
            && setup.kind == "setup"
            && setup.generation_nonce == nonce
            && id(&setup.launch_id)
            && setup.daemon_socket_path == socket.to_string_lossy()
            && setup.setup_purpose == "source_bootstrap_only"
            && Path::new(&setup.source.socket_path).is_absolute()
            && !setup.source.token.is_empty()
            && id(&setup.source.instance_id),
        "Private setup identity mismatch"
    );
    let key = unhex(&setup.owner_key_hex)?;
    let mut tail = [0u8; 1];
    anyhow::ensure!(
        stream.read(&mut tail)? == 0,
        "Unexpected private setup tail"
    );
    writeln!(
        stream,
        "{}",
        serde_json::json!({"version":1,"kind":"ready_ack","launchId":setup.launch_id,"generationNonce":nonce})
    )?;
    let owner = LaunchOwner {
        launch_id: setup.launch_id,
        nonce,
        key,
        instance_id: setup.source.instance_id.clone(),
        source: setup.source,
        sequence: 0,
        last: None,
        bootstrap_last: None,
        managed: ManagedOwner::default(),
    };
    drop(stream);
    Ok(owner)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn private_launch_rejects_duplicate_json() {
        assert!(strict(br#"{"x":1,"x":2}"#).is_err());
        assert!(strict(br#"{"source":{"x":1,"x":2}}"#).is_err());
    }
    #[test]
    fn private_launch_mac_replay_is_bounded_and_nonmutating() {
        let key = vec![7; 32];
        let mut owner = LaunchOwner {
            launch_id: "launch".into(),
            nonce: "nonce".into(),
            key: key.clone(),
            instance_id: "source".into(),
            source: Source {
                socket_path: "/bridge".into(),
                token: "private".into(),
                instance_id: "source".into(),
            },
            sequence: 0,
            last: None,
            bootstrap_last: None,
            managed: ManagedOwner::default(),
        };
        let payload = r#"{"operation":"owner_probe"}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(&key).unwrap();
        mac.update(&message("launch", "nonce", 1, payload));
        let hex = format!("{:x}", mac.finalize().into_bytes());
        let probe = serde_json::json!({"version":1,"launchId":"launch","generationNonce":"nonce","sequence":1,"payload":payload,"macHex":hex});
        let mut bad = probe.clone();
        bad["macHex"] = Value::String("00".repeat(32));
        assert!(owner.probe(&bad).is_err());
        assert_eq!(owner.sequence, 0);
        let first = owner.probe(&probe).unwrap();
        assert_eq!(first["admissionPermission"], false);
        assert_eq!(owner.probe(&probe).unwrap(), first);
        assert_eq!(owner.sequence, 1);
        bad = probe.clone();
        bad["generationNonce"] = Value::String("other".into());
        assert!(owner.probe(&bad).is_err());
    }
}

#[derive(Default)]
struct ManagedOwner {
    policy: Option<super::private_service::ServicePolicy>,
    issuer: Option<Issuer>,
    acquisitions: std::collections::VecDeque<Acquisition>,
    calls: std::collections::VecDeque<CallReceipt>,
    bootstrap_revoked: bool,
}
struct Issuer {
    connection: String,
    identity: super::private_service::ServiceIdentity,
    generation: String,
    lease: String,
    nonce: String,
    sequence: u64,
    live: bool,
}
struct Acquisition {
    operation: String,
    connection: String,
    identity: super::private_service::ServiceIdentity,
    challenge: String,
    nonce: String,
    expected: Option<String>,
    source: String,
    old_source: String,
    deadline: std::time::Instant,
    result: Option<Value>,
}
struct CallReceipt {
    operation: String,
    issuer: String,
    identity: super::private_service::ServiceIdentity,
    sequence: u64,
    kind: String,
    digest: String,
    result: Option<Value>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExpectedSupervisor {
    pid: u32,
    process_start_id: String,
    generation: String,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ManagedRequest {
    #[serde(rename = "private_owner_inspect", rename_all = "camelCase")]
    Inspect {
        #[serde(rename = "id")]
        _id: Option<String>,
        version: u32,
        expected_supervisor: ExpectedSupervisor,
    },
    #[serde(rename = "private_owner_acquire_begin", rename_all = "camelCase")]
    AcquireBegin {
        #[serde(rename = "id")]
        _id: Option<String>,
        version: u32,
        operation_id: String,
        expected_supervisor: ExpectedSupervisor,
        expected_source_instance_id: String,
        expected_issuer_generation: Option<String>,
    },
    #[serde(rename = "private_owner_acquire_commit", rename_all = "camelCase")]
    AcquireCommit {
        #[serde(rename = "id")]
        _id: Option<String>,
        version: u32,
        operation_id: String,
        challenge_id: String,
        nonce: String,
        expected_issuer_generation: Option<String>,
        expected_source_instance_id: String,
    },
    #[serde(rename = "private_owner_acquire_status", rename_all = "camelCase")]
    AcquireStatus {
        #[serde(rename = "id")]
        _id: Option<String>,
        version: u32,
        operation_id: String,
        challenge_id: String,
    },
    #[serde(rename = "private_owner_call", rename_all = "camelCase")]
    Call {
        #[serde(rename = "id")]
        _id: Option<String>,
        version: u32,
        lease_id: String,
        issuer_generation: String,
        sequence: u64,
        operation_id: String,
        operation: String,
        payload: String,
    },
    #[serde(rename = "private_owner_call_status", rename_all = "camelCase")]
    CallStatus {
        #[serde(rename = "id")]
        _id: Option<String>,
        version: u32,
        lease_id: String,
        issuer_generation: String,
        operation_id: String,
        payload_sha256: String,
    },
    #[serde(rename = "private_owner_release", rename_all = "camelCase")]
    Release {
        #[serde(rename = "id")]
        _id: Option<String>,
        version: u32,
        lease_id: String,
        issuer_generation: String,
        operation_id: String,
    },
}
pub(super) enum ManagedAction {
    Reply(Value),
    Bootstrap {
        payload: Value,
        operation: String,
        issuer: String,
        sequence: u64,
        digest: String,
    },
}
fn fresh() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn operation_id(s: &str) -> Result<()> {
    anyhow::ensure!(
        uuid::Uuid::parse_str(s).is_ok(),
        "private_owner_invalid_request"
    );
    Ok(())
}
impl LaunchOwner {
    pub(super) fn enable_managed(&mut self, agent: &Path, socket: &Path) -> Result<()> {
        self.managed.policy = super::private_service::ServicePolicy::load(agent, socket)?;
        Ok(())
    }
    pub(super) fn managed_available(&self) -> bool {
        self.managed.policy.is_some()
    }
    pub(super) fn disconnect(&mut self, connection: &str) {
        if let Some(issuer) = &mut self.managed.issuer {
            if issuer.connection == connection {
                issuer.live = false;
            }
        }
    }
    fn issuer_generation(&self) -> Option<String> {
        self.managed.issuer.as_ref().map(|i| i.generation.clone())
    }
    fn require_issuer(
        &self,
        connection: &str,
        identity: &super::private_service::ServiceIdentity,
        generation: &str,
        lease: &str,
    ) -> Result<()> {
        anyhow::ensure!(
            self.managed.issuer.as_ref().is_some_and(|i| i.live
                && i.connection == connection
                && i.identity == *identity
                && i.generation == generation
                && i.lease == lease),
            "private_owner_revoked"
        );
        Ok(())
    }
    pub(super) fn managed_request(
        &mut self,
        raw: &[u8],
        connection: &str,
        peer: Option<(u32, u32)>,
        supervisor_generation: &str,
    ) -> Result<ManagedAction> {
        let value = strict(raw)?;
        let request: ManagedRequest = serde_json::from_value(value)?;
        let policy = self
            .managed
            .policy
            .as_ref()
            .ok_or_else(|| anyhow!("private_owner_unsupported"))?;
        let identity = policy.attest(peer)?;
        self.managed_authenticated(request, connection, identity, supervisor_generation)
    }
    fn managed_authenticated(
        &mut self,
        request: ManagedRequest,
        connection: &str,
        identity: super::private_service::ServiceIdentity,
        supervisor_generation: &str,
    ) -> Result<ManagedAction> {
        let policy = self
            .managed
            .policy
            .as_ref()
            .ok_or_else(|| anyhow!("private_owner_unsupported"))?;
        let reply = match request {
            ManagedRequest::Inspect {
                _id: _,
                version,
                expected_supervisor: supervisor,
            } => {
                anyhow::ensure!(
                    version == 1
                        && supervisor.pid == std::process::id()
                        && Some(supervisor.process_start_id)
                            == crate::protocol::process_start_id(std::process::id())
                        && supervisor.generation == supervisor_generation,
                    "private_owner_generation_changed"
                );
                serde_json::json!({"currentIssuerGeneration":self.issuer_generation(),"supervisorGeneration":supervisor_generation,"admissionPermission":false})
            }
            ManagedRequest::AcquireBegin {
                _id: _,
                version,
                operation_id: op,
                expected_supervisor: supervisor,
                expected_source_instance_id: source,
                expected_issuer_generation: expected,
            } => {
                operation_id(&op)?;
                anyhow::ensure!(
                    version == 1
                        && supervisor.pid == std::process::id()
                        && Some(supervisor.process_start_id)
                            == crate::protocol::process_start_id(std::process::id())
                        && supervisor.generation == supervisor_generation,
                    "private_owner_generation_changed"
                );
                anyhow::ensure!(id(&source), "private_owner_source_changed");
                anyhow::ensure!(
                    self.issuer_generation() == expected,
                    "private_owner_conflict"
                );
                anyhow::ensure!(
                    !self.managed.acquisitions.iter().any(|a| a.operation == op),
                    "private_owner_conflict"
                );
                let a = Acquisition {
                    operation: op.clone(),
                    connection: connection.into(),
                    identity: identity.clone(),
                    challenge: fresh(),
                    nonce: fresh(),
                    expected: expected.clone(),
                    source: source.clone(),
                    old_source: self.instance_id.clone(),
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
                    result: None,
                };
                let result = serde_json::json!({"operationId":op,"challengeId":a.challenge,"nonce":a.nonce,"supervisorGeneration":supervisor_generation,"currentIssuerGeneration":expected,"peerIdentityDigest":identity.digest(),"sourceInstanceId":source,"admissionPermission":false});
                if self.managed.acquisitions.len() == 64 {
                    self.managed.acquisitions.pop_front();
                }
                self.managed.acquisitions.push_back(a);
                result
            }
            ManagedRequest::AcquireCommit {
                _id: _,
                version,
                operation_id: op,
                challenge_id: challenge,
                nonce,
                expected_issuer_generation: expected,
                expected_source_instance_id: source,
            } => {
                operation_id(&op)?;
                anyhow::ensure!(version == 1 && id(&source), "private_owner_source_changed");
                anyhow::ensure!(
                    self.issuer_generation() == expected,
                    "private_owner_conflict"
                );
                let a = self
                    .managed
                    .acquisitions
                    .iter_mut()
                    .find(|a| a.operation == op)
                    .ok_or_else(|| anyhow!("private_owner_unknown"))?;
                anyhow::ensure!(
                    a.connection == connection
                        && a.identity == identity
                        && a.challenge == challenge
                        && a.nonce == nonce
                        && a.expected == expected
                        && a.source == source
                        && a.old_source == self.instance_id
                        && a.result.is_none()
                        && std::time::Instant::now() < a.deadline,
                    "private_owner_conflict"
                );
                let rebound = acquire_source(
                    policy,
                    &identity,
                    &op,
                    supervisor_generation,
                    expected.as_deref(),
                    &challenge,
                    &nonce,
                    &source,
                )?;
                self.instance_id = rebound.instance_id.clone();
                self.source = rebound;
                let issuer = Issuer {
                    connection: connection.into(),
                    identity,
                    generation: fresh(),
                    lease: fresh(),
                    nonce: fresh(),
                    sequence: 0,
                    live: true,
                };
                let result = serde_json::json!({"operationId":op,"status":"committed","leaseId":issuer.lease,"issuerGeneration":issuer.generation,"generationNonce":issuer.nonce,"nextSequence":1,"sourceInstanceId":source,"admissionPermission":false});
                a.result = Some(result.clone());
                self.managed.issuer = Some(issuer);
                self.managed.bootstrap_revoked = true;
                self.key.fill(0);
                result
            }
            ManagedRequest::AcquireStatus {
                _id: _,
                version,
                operation_id: op,
                challenge_id: challenge,
            } => {
                operation_id(&op)?;
                anyhow::ensure!(version == 1, "private_owner_invalid_request");
                match self.managed.acquisitions.iter().find(|a| {
                    a.operation == op && a.challenge == challenge && a.identity == identity
                }) {
                    Some(a) => {
                        let current = self.managed.issuer.as_ref();
                        let committed = a.result.as_ref();
                        let live = current.is_some_and(|i| {
                            i.live
                                && committed.is_some_and(|r| r["issuerGeneration"] == i.generation)
                        });
                        serde_json::json!({"operationId":op,"status":if committed.is_none() {"not_committed"} else if live {"committed"} else {"revoked"},"issuerGeneration":committed.map(|r|r["issuerGeneration"].clone()),"connectionBound":live && current.is_some_and(|i| i.connection == connection),"nextSequence":current.filter(|_|live).map(|i|i.sequence+1)})
                    }
                    None => {
                        serde_json::json!({"operationId":op,"status":"unknown","issuerGeneration":null,"connectionBound":false,"nextSequence":null})
                    }
                }
            }
            ManagedRequest::Call {
                _id: _,
                version,
                lease_id: lease,
                issuer_generation: generation,
                sequence,
                operation_id: op,
                operation,
                payload,
            } => {
                operation_id(&op)?;
                anyhow::ensure!(
                    version == 1
                        && payload.len() <= 32768
                        && sequence > 0
                        && sequence <= 9_007_199_254_740_991,
                    "private_owner_invalid_request"
                );
                self.require_issuer(connection, &identity, &generation, &lease)?;
                let digest = format!("{:x}", Sha256::digest(payload.as_bytes()));
                if let Some(receipt) = self.managed.calls.iter().find(|r| {
                    r.issuer == generation && (r.operation == op || r.sequence == sequence)
                }) {
                    anyhow::ensure!(
                        receipt.operation == op
                            && receipt.sequence == sequence
                            && receipt.kind == operation
                            && receipt.digest == digest,
                        "private_owner_sequence_invalid"
                    );
                    return Ok(ManagedAction::Reply(
                        receipt
                            .result
                            .clone()
                            .ok_or_else(|| anyhow!("private_owner_unknown"))?,
                    ));
                }
                anyhow::ensure!(
                    self.managed
                        .issuer
                        .as_ref()
                        .is_some_and(|i| sequence == i.sequence + 1),
                    "private_owner_sequence_invalid"
                );
                let mut target = strict(payload.as_bytes())?;
                anyhow::ensure!(
                    target.is_object() && target.get("pinnedSource").is_none(),
                    "private_owner_invalid_request"
                );
                let result = if operation == "owner_probe" {
                    anyhow::ensure!(
                        target.as_object().is_some_and(serde_json::Map::is_empty),
                        "private_owner_invalid_request"
                    );
                    Some(
                        serde_json::json!({"version":1,"privateOwnerAuthenticated":true,"launchId":generation,"generationNonce":self.managed.issuer.as_ref().unwrap().nonce,"sourceInstanceId":self.instance_id,"operation":"owner_probe","admissionPermission":false,"bootstrapApplied":false}),
                    )
                } else {
                    anyhow::ensure!(
                        matches!(
                            operation.as_str(),
                            "owner_bootstrap_preflight"
                                | "owner_bootstrap_commit"
                                | "resident_drone_bind"
                                | "resident_drone_status"
                                | "resident_drone_control"
                        ) && target.get("operation").is_none(),
                        "private_owner_invalid_request"
                    );
                    target["operation"] = operation.clone().into();
                    if matches!(
                        operation.as_str(),
                        "resident_drone_bind" | "resident_drone_status" | "resident_drone_control"
                    ) {
                        crate::drone::validate_resident_request(&target)?;
                    } else {
                        self.validate_bootstrap_payload(&mut target)?;
                    }
                    None
                };
                self.managed.issuer.as_mut().unwrap().sequence = sequence;
                if self.managed.calls.len() == 128 {
                    self.managed.calls.pop_front();
                }
                let wrapped = result.map(|r|serde_json::json!({"operationId":op,"issuerGeneration":generation,"sequence":sequence,"payloadSha256":digest,"result":r}));
                self.managed.calls.push_back(CallReceipt {
                    operation: op.clone(),
                    issuer: generation.clone(),
                    identity,
                    sequence,
                    kind: operation,
                    digest: digest.clone(),
                    result: wrapped.clone(),
                });
                return Ok(match wrapped {
                    Some(r) => ManagedAction::Reply(r),
                    None => ManagedAction::Bootstrap {
                        payload: target,
                        operation: op,
                        issuer: generation,
                        sequence,
                        digest,
                    },
                });
            }
            ManagedRequest::CallStatus {
                _id: _,
                version,
                lease_id: lease,
                issuer_generation: generation,
                operation_id: op,
                payload_sha256: digest,
            } => {
                operation_id(&op)?;
                anyhow::ensure!(
                    version == 1 && digest.len() == 64,
                    "private_owner_invalid_request"
                );
                let receipt = self.managed.calls.iter().find(|r| {
                    r.operation == op && r.issuer == generation && r.identity == identity
                });
                if let Some(r) = receipt {
                    anyhow::ensure!(r.digest == digest, "private_owner_conflict");
                }
                let current = self.managed.issuer.as_ref().is_some_and(|i| {
                    i.live
                        && i.generation == generation
                        && i.lease == lease
                        && i.identity == identity
                });
                serde_json::json!({"operationId":op,"status":if !current {"revoked"} else if receipt.is_some_and(|r|r.result.is_some()) {"acknowledged"} else {"unknown"},"payloadSha256":digest,"result":receipt.and_then(|r|r.result.clone())})
            }
            ManagedRequest::Release {
                _id: _,
                version,
                lease_id: lease,
                issuer_generation: generation,
                operation_id: op,
            } => {
                operation_id(&op)?;
                anyhow::ensure!(version == 1, "private_owner_invalid_request");
                let i = self
                    .managed
                    .issuer
                    .as_mut()
                    .ok_or_else(|| anyhow!("private_owner_revoked"))?;
                anyhow::ensure!(
                    i.connection == connection
                        && i.identity == identity
                        && i.generation == generation
                        && i.lease == lease,
                    "private_owner_revoked"
                );
                let was_live = i.live;
                i.live = false;
                serde_json::json!({"operationId":op,"status":if was_live {"released"} else {"already_released"},"issuerGeneration":generation,"workersStopped":false})
            }
        };
        Ok(ManagedAction::Reply(reply))
    }
    pub(super) fn finish_managed_call(
        &mut self,
        operation: &str,
        issuer: &str,
        sequence: u64,
        digest: &str,
        result: Value,
    ) -> Value {
        let mut wrapped = serde_json::json!({"operationId":operation,"issuerGeneration":issuer,"sequence":sequence,"payloadSha256":digest});
        wrapped["result"] = result;
        if let Some(r) = self.managed.calls.iter_mut().find(|r| {
            r.operation == operation
                && r.issuer == issuer
                && r.sequence == sequence
                && r.digest == digest
        }) {
            r.result = Some(wrapped.clone());
        }
        wrapped
    }
}

#[cfg(target_os = "linux")]
// Keep each independently validated generation/challenge fence explicit at this boundary.
#[allow(clippy::too_many_arguments)]
fn acquire_source(
    policy: &super::private_service::ServicePolicy,
    identity: &super::private_service::ServiceIdentity,
    operation: &str,
    supervisor_generation: &str,
    issuer_generation: Option<&str>,
    challenge: &str,
    nonce: &str,
    instance: &str,
) -> Result<Source> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Reply {
        version: u32,
        challenge_id: String,
        source: Source,
    }
    use std::os::unix::fs::MetadataExt;
    let path = &policy.source_socket_path;
    anyhow::ensure!(
        std::fs::symlink_metadata(path)?.uid() == identity.uid,
        "private_owner_source_changed"
    );
    let mut stream = std::os::unix::net::UnixStream::connect(path)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(3)))?;
    let credentials =
        nix::sys::socket::getsockopt(&stream, nix::sys::socket::sockopt::PeerCredentials)?;
    anyhow::ensure!(
        u32::try_from(credentials.pid()).ok() == Some(identity.pid)
            && credentials.uid() == identity.uid,
        "private_owner_identity_mismatch"
    );
    let body=serde_json::json!({"version":1,"operation":"private_owner_source","operationId":operation,"challengeId":challenge,"nonce":nonce,"supervisorGeneration":supervisor_generation,"sourceInstanceId":instance,"issuerGeneration":issuer_generation}).to_string();
    write!(
        stream,
        "POST /private-owner-source HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )?;
    let mut bytes = Vec::new();
    stream.take(32769).read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= 32768, "private_owner_source_changed");
    let text = String::from_utf8(bytes)?;
    let (header, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow!("private_owner_source_changed"))?;
    anyhow::ensure!(
        header
            .lines()
            .next()
            .is_some_and(|l| l == "HTTP/1.1 200 OK")
            && !header.to_ascii_lowercase().contains("transfer-encoding"),
        "private_owner_source_changed"
    );
    let reply: Reply = serde_json::from_value(strict(body.as_bytes())?)?;
    anyhow::ensure!(
        reply.version == 1
            && reply.challenge_id == challenge
            && reply.source.instance_id == instance
            && Path::new(&reply.source.socket_path) == path
            && !reply.source.token.is_empty()
            && reply.source.token.len() <= 4096
            && id(instance),
        "private_owner_source_changed"
    );
    anyhow::ensure!(
        policy.attest(Some((identity.pid, identity.uid)))? == *identity,
        "private_owner_generation_changed"
    );
    Ok(reply.source)
}
#[cfg(not(target_os = "linux"))]
// Keep each independently validated generation/challenge fence explicit at this boundary.
#[allow(clippy::too_many_arguments)]
fn acquire_source(
    _: &super::private_service::ServicePolicy,
    _: &super::private_service::ServiceIdentity,
    _: &str,
    _: &str,
    _: Option<&str>,
    _: &str,
    _: &str,
    _: &str,
) -> Result<Source> {
    anyhow::bail!("private_owner_unsupported")
}

#[cfg(test)]
mod managed_tests {
    use super::*;
    fn owner() -> LaunchOwner {
        LaunchOwner {
            launch_id: "initial".into(),
            nonce: "initial".into(),
            key: vec![7; 32],
            instance_id: "source".into(),
            source: Source {
                socket_path: "/pinned".into(),
                token: "fixture".into(),
                instance_id: "source".into(),
            },
            sequence: 0,
            last: None,
            bootstrap_last: None,
            managed: ManagedOwner::default(),
        }
    }
    fn identity() -> super::super::private_service::ServiceIdentity {
        super::super::private_service::ServiceIdentity {
            pid: 42,
            uid: 1020,
            start: "proc:1".into(),
            invocation: "1".repeat(32),
        }
    }
    fn policy() -> super::super::private_service::ServicePolicy {
        super::super::private_service::ServicePolicy {
            unit: "fixture.service".into(),
            unit_path: "/fixture.service".into(),
            unit_sha256: "0".repeat(64),
            uid: 1020,
            executable_path: "/fixture".into(),
            executable_sha256: "0".repeat(64),
            launch_path: "/launch".into(),
            launch_sha256: "0".repeat(64),
            agent_dir: "/state/agent".into(),
            daemon_socket_path: "/daemon".into(),
            source_socket_path: "/pinned".into(),
        }
    }
    fn leased() -> LaunchOwner {
        let mut o = owner();
        o.managed.policy = Some(policy());
        o.managed.bootstrap_revoked = true;
        o.managed.issuer = Some(Issuer {
            connection: "connection".into(),
            identity: identity(),
            generation: "generation".into(),
            lease: "lease".into(),
            nonce: "nonce".into(),
            sequence: 0,
            live: true,
        });
        o
    }
    fn call(op: &str, seq: u64, payload: &str) -> ManagedRequest {
        ManagedRequest::Call {
            _id: None,
            version: 1,
            lease_id: "lease".into(),
            issuer_generation: "generation".into(),
            sequence: seq,
            operation_id: op.into(),
            operation: "owner_probe".into(),
            payload: payload.into(),
        }
    }
    fn reply(a: ManagedAction) -> Value {
        match a {
            ManagedAction::Reply(v) => v,
            ManagedAction::Bootstrap { .. } => panic!("unexpected effect"),
        }
    }
    #[test]
    fn inspect_is_attested_readonly_and_preserves_cas() {
        let mut o = leased();
        let inspect = || ManagedRequest::Inspect {
            _id: None,
            version: 1,
            expected_supervisor: ExpectedSupervisor {
                pid: std::process::id(),
                process_start_id: crate::protocol::process_start_id(std::process::id()).unwrap(),
                generation: "sup".into(),
            },
        };
        let value = reply(
            o.managed_authenticated(inspect(), "new", identity(), "sup")
                .unwrap(),
        );
        assert_eq!(
            value,
            serde_json::json!({"currentIssuerGeneration":"generation","supervisorGeneration":"sup","admissionPermission":false})
        );
        assert!(o
            .require_issuer("connection", &identity(), "generation", "lease")
            .is_ok());
        assert!(o
            .require_issuer("new", &identity(), "generation", "lease")
            .is_err());
        o.disconnect("connection");
        assert!(o
            .require_issuer("connection", &identity(), "generation", "lease")
            .is_err());
        assert_eq!(
            reply(
                o.managed_authenticated(inspect(), "new", identity(), "sup")
                    .unwrap()
            ),
            value
        );
        assert!(o
            .managed_authenticated(inspect(), "new", identity(), "replaced-supervisor")
            .is_err());
        let begin = |expected, op: &str| ManagedRequest::AcquireBegin {
            _id: None,
            version: 1,
            operation_id: op.into(),
            expected_supervisor: ExpectedSupervisor {
                pid: std::process::id(),
                process_start_id: crate::protocol::process_start_id(std::process::id()).unwrap(),
                generation: "sup".into(),
            },
            expected_source_instance_id: "source".into(),
            expected_issuer_generation: expected,
        };
        assert!(o
            .managed_authenticated(
                begin(None, "00000000-0000-4000-8000-000000000001"),
                "new",
                identity(),
                "sup"
            )
            .is_err());
        assert!(o
            .managed_authenticated(
                begin(
                    Some("generation".into()),
                    "00000000-0000-4000-8000-000000000001"
                ),
                "new",
                identity(),
                "sup"
            )
            .is_ok());
        // Another successful acquisition makes this previously inspected generation stale.
        o.managed.issuer.as_mut().unwrap().generation = "replacement".into();
        assert!(o
            .managed_authenticated(
                begin(
                    Some("generation".into()),
                    "00000000-0000-4000-8000-000000000002"
                ),
                "new",
                identity(),
                "sup"
            )
            .is_err());
        let raw=br#"{"type":"private_owner_inspect","version":1,"expectedSupervisor":{"pid":1,"processStartId":"proc:1","generation":"sup"}}"#;
        assert!(o.managed_request(raw, "new", None, "sup").is_err());
        assert!(o
            .managed_request(raw, "new", Some((42, 1001)), "sup")
            .is_err());
        assert!(o
            .managed_request(raw, "new", Some((std::process::id(), 1020)), "sup")
            .is_err());
    }
    #[test]
    fn no_peer_or_unconfigured_service_never_grants() {
        let mut o = owner();
        assert!(!o.managed_available());
        assert!(o.managed_request(br#"{"type":"private_owner_acquire_status","version":1,"operationId":"00000000-0000-4000-8000-000000000001","challengeId":"none"}"#,"c",None,"g").is_err());
        o.managed.policy = Some(policy());
        assert!(o.managed.policy.as_ref().unwrap().attest(None).is_err());
        assert!(o
            .managed
            .policy
            .as_ref()
            .unwrap()
            .attest(Some((42, 1001)))
            .is_err());
        assert!(o
            .managed
            .policy
            .as_ref()
            .unwrap()
            .attest(Some((std::process::id(), 1020)))
            .is_err());
    }
    #[test]
    fn lease_revokes_old_issuer_connection_and_generation() {
        let mut o = leased();
        assert!(o
            .require_issuer("other", &identity(), "generation", "lease")
            .is_err());
        let mut changed = identity();
        changed.start = "proc:2".into();
        assert!(o
            .require_issuer("connection", &changed, "generation", "lease")
            .is_err());
        changed = identity();
        changed.invocation = "2".repeat(32);
        assert!(o
            .require_issuer("connection", &changed, "generation", "lease")
            .is_err());
        assert!(o
            .require_issuer("connection", &identity(), "old", "lease")
            .is_err());
        o.disconnect("connection");
        assert!(o
            .require_issuer("connection", &identity(), "generation", "lease")
            .is_err());
        assert!(o.probe(&serde_json::json!({})).is_err());
    }
    #[test]
    fn resident_drone_operations_require_live_lease_and_reject_authority_fields() {
        let mut o = leased();
        let payload = serde_json::json!({"rootSessionId":"root","activeSessionId":"active","workerInstanceId":"incarnation","supervisorGeneration":"s","expectedBindingRevision":0}).to_string();
        let request = |payload: String| ManagedRequest::Call {
            _id: None,
            version: 1,
            lease_id: "lease".into(),
            issuer_generation: "generation".into(),
            sequence: 1,
            operation_id: "00000000-0000-4000-8000-000000000001".into(),
            operation: "resident_drone_status".into(),
            payload,
        };
        assert!(o
            .managed_authenticated(request(payload.clone()), "foreign", identity(), "s")
            .is_err());
        let mut bad: Value = serde_json::from_str(&payload).unwrap();
        bad["model"] = serde_json::json!("other-provider");
        assert!(o
            .managed_authenticated(request(bad.to_string()), "connection", identity(), "s")
            .is_err());
        assert!(matches!(
            o.managed_authenticated(request(payload), "connection", identity(), "s")
                .unwrap(),
            ManagedAction::Bootstrap { .. }
        ));
    }
    #[test]
    fn exact_ack_replay_and_conflicting_sequence_have_no_second_effect() {
        let mut o = leased();
        let op = "00000000-0000-4000-8000-000000000001";
        let first = reply(
            o.managed_authenticated(call(op, 1, "{}"), "connection", identity(), "s")
                .unwrap(),
        );
        assert_eq!(
            first,
            reply(
                o.managed_authenticated(call(op, 1, "{}"), "connection", identity(), "s")
                    .unwrap()
            )
        );
        assert!(o
            .managed_authenticated(call(op, 1, "{ }"), "connection", identity(), "s")
            .is_err());
        assert!(o
            .managed_authenticated(
                call("00000000-0000-4000-8000-000000000002", 1, "{}"),
                "connection",
                identity(),
                "s"
            )
            .is_err());
        assert_eq!(o.managed.issuer.as_ref().unwrap().sequence, 1);
        let digest = format!("{:x}", Sha256::digest(b"{}"));
        let status = ManagedRequest::CallStatus {
            _id: None,
            version: 1,
            lease_id: "lease".into(),
            issuer_generation: "generation".into(),
            operation_id: op.into(),
            payload_sha256: digest,
        };
        assert_eq!(
            reply(
                o.managed_authenticated(status, "reconnected", identity(), "s")
                    .unwrap()
            )["status"],
            "acknowledged"
        );
    }
    #[test]
    fn unknown_lost_ack_and_source_injection_fail_closed() {
        let mut o = leased();
        let op = "00000000-0000-4000-8000-000000000001";
        assert!(o
            .managed_authenticated(
                call(op, 1, r#"{"pinnedSource":{}}"#),
                "connection",
                identity(),
                "s"
            )
            .is_err());
        assert_eq!(o.managed.issuer.as_ref().unwrap().sequence, 0);
        let status = ManagedRequest::CallStatus {
            _id: None,
            version: 1,
            lease_id: "lease".into(),
            issuer_generation: "generation".into(),
            operation_id: op.into(),
            payload_sha256: "0".repeat(64),
        };
        assert_eq!(
            reply(
                o.managed_authenticated(status, "reconnected", identity(), "s")
                    .unwrap()
            )["status"],
            "unknown"
        );
        let duplicate = br#"{"type":"private_owner_release","version":1,"version":1}"#;
        assert!(strict(duplicate).is_err());
        assert!(serde_json::from_value::<ManagedRequest>(serde_json::json!({"type":"private_owner_acquire_status","version":1,"operationId":op,"challengeId":"a","source":{}})).is_err());
    }
    #[test]
    fn wrong_unit_and_untrusted_policy_are_rejected() {
        let mut p = policy();
        p.unit = "not-approved.service".into();
        assert!(p.attest(Some((std::process::id(), 1020))).is_err());
        let bytes = br#"{"unit":"x.service","uid":1020,"arbitrarySigner":true}"#;
        assert!(
            serde_json::from_value::<super::super::private_service::ServicePolicy>(
                strict(bytes).unwrap()
            )
            .is_err()
        );
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn callback_wrong_actual_peer_and_failed_callback_do_not_rebind() {
        use std::os::unix::fs::MetadataExt;
        use std::os::unix::net::UnixListener;
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("callback.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let mut p = policy();
        p.source_socket_path = socket.clone();
        let mut actual = identity();
        actual.uid = std::fs::metadata(&socket).unwrap().uid();
        actual.pid = std::process::id().saturating_add(1);
        assert!(acquire_source(
            &p,
            &actual,
            "operation",
            "supervisor",
            None,
            "challenge",
            "nonce",
            "new-source"
        )
        .is_err());
        drop(listener);
        assert!(acquire_source(
            &p,
            &actual,
            "operation",
            "supervisor",
            None,
            "challenge",
            "nonce",
            "new-source"
        )
        .is_err());
        assert_eq!(owner().instance_id, "source");
    }
    #[test]
    fn source_target_validator_is_shared_and_never_accepts_caller_source() {
        let o = owner();
        let mut payload = serde_json::json!({"version":1,"operation":"owner_bootstrap_preflight","rootSessionId":"r","activeSessionId":"a","sessionFile":"/s","cwd":"/w","expectedGoalId":"g","intendedWork":{"planId":"p","stepId":"s"}});
        o.validate_bootstrap_payload(&mut payload).unwrap();
        assert_eq!(payload["pinnedSource"]["instanceId"], "source");
        assert!(o.validate_bootstrap_payload(&mut payload).is_err());
    }
    #[test]
    fn resident_drone_control_requires_exact_signed_lease_and_strict_payload() {
        let payload = serde_json::json!({"rootSessionId":"root","activeSessionId":"active","workerInstanceId":"incarnation","supervisorGeneration":"s","expectedBindingRevision":0,"action":"stop","expectedRevision":0});
        let request = |payload: &Value, lease: &str, generation: &str| ManagedRequest::Call {
            _id: None,
            version: 1,
            lease_id: lease.into(),
            issuer_generation: generation.into(),
            sequence: 1,
            operation_id: "00000000-0000-4000-8000-000000000001".into(),
            operation: "resident_drone_control".into(),
            payload: payload.to_string(),
        };
        for (connection, lease, generation) in [
            ("foreign", "lease", "generation"),
            ("connection", "forged", "generation"),
            ("connection", "lease", "old"),
        ] {
            assert!(leased()
                .managed_authenticated(
                    request(&payload, lease, generation),
                    connection,
                    identity(),
                    "s"
                )
                .is_err());
        }
        let mut disconnected = leased();
        disconnected.disconnect("connection");
        assert!(disconnected
            .managed_authenticated(
                request(&payload, "lease", "generation"),
                "connection",
                identity(),
                "s"
            )
            .is_err());
        for field in [
            "operation",
            "droneRoadmapPath",
            "expectedSha256",
            "expectedRoadmapRevision",
            "model",
        ] {
            let mut wrong = payload.clone();
            wrong[field] = Value::Null;
            assert!(
                leased()
                    .managed_authenticated(
                        request(&wrong, "lease", "generation"),
                        "connection",
                        identity(),
                        "s"
                    )
                    .is_err(),
                "{field}"
            );
        }
        let mut owner = leased();
        let action = owner
            .managed_authenticated(
                request(&payload, "lease", "generation"),
                "connection",
                identity(),
                "s",
            )
            .unwrap();
        let ManagedAction::Bootstrap {
            payload: admitted, ..
        } = action
        else {
            panic!("control must reach the resident dispatch")
        };
        let mut expected = payload.clone();
        expected["operation"] = serde_json::json!("resident_drone_control");
        assert_eq!(admitted, expected);
        assert!(
            owner
                .managed_authenticated(
                    request(&payload, "lease", "generation"),
                    "connection",
                    identity(),
                    "s"
                )
                .is_err(),
            "a pending signed call remains UNKNOWN, never a second effect"
        );
        assert_eq!(owner.managed.issuer.as_ref().unwrap().sequence, 1);
    }
}
