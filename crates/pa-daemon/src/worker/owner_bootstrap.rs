//! Independent supervisor-to-worker owner proof. Not the model-known worker token.
use super::{
    anyhow, response_failure, response_success, DaemonResponse, Result, Value, Worker, WorkerConfig,
};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Setup {
    version: u32,
    kind: String,
    supervisor_generation: String,
    worker_instance_id: String,
    worker_id: String,
    key_hex: String,
}
/// Generation-only secret, absent from config, env, descriptors and kernel.
pub(super) struct WorkerOwner {
    key: Vec<u8>,
    generation: String,
    instance: String,
    worker: String,
    sequence: u64,
    last: Option<(String, Value)>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Proof {
    version: u32,
    generation: String,
    instance: String,
    worker: String,
    sequence: u64,
    payload: String,
    mac_hex: String,
}
fn decode(s: &str) -> Result<Vec<u8>> {
    anyhow::ensure!(
        s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()),
        "Invalid owner proof"
    );
    (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(Into::into))
        .collect()
}
pub(crate) fn bytes(
    generation: &str,
    instance: &str,
    worker: &str,
    sequence: u64,
    payload: &str,
) -> Vec<u8> {
    let mut out = b"prime-native-worker-owner-v1\0".to_vec();
    for value in [generation, instance, worker, &sequence.to_string(), payload] {
        out.extend_from_slice(&(value.len() as u64).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out
}
pub(super) fn read_setup(config: &WorkerConfig) -> Result<WorkerOwner> {
    #[cfg(not(target_os = "linux"))]
    anyhow::bail!("Private worker setup unsupported");
    #[cfg(target_os = "linux")]
    {
        let frame = pa_types::platform::process::read_private_worker_setup_frame()?;
        anyhow::ensure!(
            frame.len() <= 4096 && frame.last() == Some(&b'\n'),
            "Invalid private worker frame"
        );
        let setup: Setup =
            serde_json::from_value(crate::supervisor::private_launch::strict(&frame)?)?;
        anyhow::ensure!(
            setup.version == 1
                && setup.kind == "owner_setup"
                && setup.worker_instance_id == config.worker_instance_id
                && setup.worker_id == config.active_session_id
                && !setup.supervisor_generation.is_empty(),
            "Private worker identity mismatch"
        );
        let key = decode(&setup.key_hex)?;
        pa_types::platform::process::close_private_worker_setup_stdin()?;
        Ok(WorkerOwner {
            key,
            generation: setup.supervisor_generation,
            instance: setup.worker_instance_id,
            worker: setup.worker_id,
            sequence: 0,
            last: None,
        })
    }
}
impl WorkerOwner {
    fn verify(&mut self, value: &Value) -> Result<Value> {
        let proof: Proof = serde_json::from_value(value.clone())?;
        anyhow::ensure!(
            proof.version == 1
                && proof.generation == self.generation
                && proof.instance == self.instance
                && proof.worker == self.worker
                && (proof.sequence == self.sequence + 1 || proof.sequence == self.sequence)
                && proof.payload.len() <= 65536,
            "Private worker owner identity mismatch"
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| anyhow!("Private owner key invalid"))?;
        mac.update(&bytes(
            &proof.generation,
            &proof.instance,
            &proof.worker,
            proof.sequence,
            &proof.payload,
        ));
        mac.verify_slice(&decode(&proof.mac_hex)?)
            .map_err(|_| anyhow!("Private worker owner authentication failed"))?;
        if proof.sequence == self.sequence {
            return self
                .last
                .as_ref()
                .filter(|(raw, _)| raw == &proof.payload)
                .map(|(_, payload)| payload.clone())
                .ok_or_else(|| anyhow!("Changed owner replay"));
        }
        let payload: Value = crate::supervisor::private_launch::strict(proof.payload.as_bytes())?;
        anyhow::ensure!(
            matches!(
                payload["operation"].as_str(),
                Some("owner_bootstrap_preflight" | "owner_bootstrap_commit")
            ),
            "Private worker operation unavailable"
        );
        self.sequence = proof.sequence;
        self.last = Some((proof.payload, payload.clone()));
        Ok(payload)
    }
}
impl Worker {
    pub(super) async fn handle_private_owner(&self, payload: &Value) -> DaemonResponse {
        let _operation = self.owner_bootstrap_operation.lock().await;
        let verified = (|| -> Result<Value> {
            self.owner_bootstrap
                .lock()
                .expect("owner lock")
                .as_mut()
                .ok_or_else(|| anyhow!("Private worker owner unavailable"))?
                .verify(payload)
        })();
        if let Ok(value) = &verified {
            if let Some((prior, response)) = self
                .owner_bootstrap_result
                .lock()
                .expect("owner result lock")
                .as_ref()
            {
                if prior == value {
                    return response.clone();
                }
            }
        }
        let cache_value = verified.as_ref().ok().cloned();
        let result = match verified {
            Ok(value) => match &self.agent_engine {
                Some(engine) => {
                    crate::engine::SessionEngine::owner_bootstrap(engine.as_ref(), value).await
                }
                None => Err(anyhow!("Owner engine unavailable")),
            },
            Err(error) => Err(error),
        };
        let response = match result {
            Ok(value) => response_success(None, "private_owner_bootstrap", Some(value)),
            Err(_) => response_failure(
                None,
                "private_owner_bootstrap",
                "Private owner bootstrap denied",
                None,
            ),
        };
        if let Some(value) = cache_value {
            *self
                .owner_bootstrap_result
                .lock()
                .expect("owner result lock") = Some((value, response.clone()));
        }
        response
    }
}

#[cfg(test)]
mod owner_bootstrap_tests {
    use super::*;
    use serde_json::json;
    fn owner() -> WorkerOwner {
        WorkerOwner {
            key: vec![7; 32],
            generation: "g".into(),
            instance: "i".into(),
            worker: "w".into(),
            sequence: 0,
            last: None,
        }
    }
    fn proof(sequence: u64, payload: &str) -> Value {
        let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
        mac.update(&bytes("g", "i", "w", sequence, payload));
        json!({"version":1,"generation":"g","instance":"i","worker":"w","sequence":sequence,"payload":payload,"macHex":format!("{:x}", mac.finalize().into_bytes())})
    }
    #[test]
    fn owner_bootstrap_connection_preserves_strict_signed_proof() {
        let mut valid = proof(1, r#"{"operation":"owner_bootstrap_preflight"}"#);
        let original = valid.clone();
        super::super::connection::stamp_assignment_trust(
            "private_owner_bootstrap",
            &mut valid,
            &super::super::ConnectionRole::Supervisor {
                generation: "g".into(),
            },
        );
        assert_eq!(valid, original);
        assert!(owner().verify(&valid).is_ok());
        valid["nativeAssignmentTrusted"] = json!(true);
        assert!(owner().verify(&valid).is_err());
    }

    #[test]
    fn assignment_trust_is_overwritten_only_for_prompt_commands() {
        use super::super::ConnectionRole;
        for command in ["prompt", "prompt_and_wait"] {
            let mut spoof = json!({"nativeAssignmentTrusted":true});
            super::super::connection::stamp_assignment_trust(
                command,
                &mut spoof,
                &ConnectionRole::Unauthenticated,
            );
            assert_eq!(spoof["nativeAssignmentTrusted"], false);
            super::super::connection::stamp_assignment_trust(
                command,
                &mut spoof,
                &ConnectionRole::Supervisor {
                    generation: "g".into(),
                },
            );
            assert_eq!(spoof["nativeAssignmentTrusted"], true);
        }
    }

    #[test]
    fn owner_bootstrap_mac_targets_generation_instance_sequence_and_exact_payload() {
        let mut verifier = owner();
        let valid = proof(1, r#"{"operation":"owner_bootstrap_preflight"}"#);
        for key in ["generation", "instance", "worker", "macHex"] {
            let mut forged = valid.clone();
            forged[key] = json!("wrong");
            assert!(verifier.verify(&forged).is_err());
            assert_eq!(verifier.sequence, 0);
        }
        let response = verifier.verify(&valid).unwrap();
        assert_eq!(verifier.verify(&valid).unwrap(), response);
        assert!(verifier
            .verify(&proof(1, r#"{"operation":"owner_bootstrap_commit"}"#))
            .is_err());
        assert!(verifier
            .verify(&proof(3, r#"{"operation":"owner_bootstrap_preflight"}"#))
            .is_err());
        assert!(verifier
            .verify(&proof(
                2,
                r#"{"operation":"owner_bootstrap_preflight","operation":"owner_bootstrap_commit"}"#
            ))
            .is_err());
    }
}
