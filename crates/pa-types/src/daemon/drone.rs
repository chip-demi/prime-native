//! Narrow Native Drone request-control contract.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DroneRequestIdentity {
    pub active_session_id: String,
    pub session_id: String,
    pub session_file: String,
    pub cwd: String,
    pub worker_incarnation: String,
    pub generation: String,
    pub turn_serial: u64,
    pub request_id: u64,
    pub revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DroneRecoverRequest {
    pub operation_id: String,
    pub identity: DroneRequestIdentity,
}
