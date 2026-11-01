//! Manifest and Native action dispatch. Model text never becomes executable or shell input.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Action {
    pub handler: String,
    pub args: Value,
}
pub(crate) fn manifest() -> Value {
    json!([
     {"name":"recommend_parallel_work","version":1,"effect":"queued_recommendation","args":{"stepIds":"array<string>","roadmapHash":"string"},"timeoutMs":5000,"outputCap":4096},
     {"name":"notify_root","version":1,"effect":"queued_diagnostic","args":{"message":"string<=2048"},"timeoutMs":5000,"outputCap":4096},
     {"name":"snapshot_branch","version":1,"effect":"read_only","args":{},"timeoutMs":5000,"outputCap":16384},
     {"name":"collect_child_results","version":1,"effect":"read_only","args":{},"timeoutMs":5000,"outputCap":16384},
     {"name":"inspect_owned_process","version":1,"effect":"read_only","args":{},"timeoutMs":5000,"outputCap":16384},
     {"name":"recover_root_request","version":1,"effect":"request_only","args":{"operationId":"string","identity":"native_snapshot_identity"},"timeoutMs":5000,"outputCap":4096},
     {"name":"recovery_status","version":1,"effect":"read_only","args":{},"timeoutMs":5000,"outputCap":4096}
    ])
}
pub(crate) fn command(action: &Action) -> anyhow::Result<(&'static str, Value)> {
    anyhow::ensure!(action.args.is_object(), "invalid_arguments");
    match action.handler.as_str() {
        "snapshot_branch"
        | "collect_child_results"
        | "inspect_owned_process"
        | "recovery_status" => {
            anyhow::ensure!(
                action
                    .args
                    .as_object()
                    .is_some_and(serde_json::Map::is_empty),
                "invalid_arguments"
            );
            let command = match action.handler.as_str() {
                "snapshot_branch" => "drone_snapshot",
                "collect_child_results" => "get_rlm_children",
                "inspect_owned_process" => "list_kernel_bash",
                "recovery_status" => "drone_status",
                _ => unreachable!(),
            };
            Ok((command, json!({})))
        }
        "notify_root" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Notice {
                message: String,
            }
            let notice: Notice = serde_json::from_value(action.args.clone())?;
            anyhow::ensure!(
                !notice.message.is_empty() && notice.message.len() <= 2048,
                "invalid_arguments"
            );
            Ok((
                "follow_up",
                json!({"message":format!("[Drone diagnostic; unresolved] {}",notice.message),"source":"drone"}),
            ))
        }
        "recover_root_request" => {
            let request: pa_types::daemon::drone::DroneRecoverRequest =
                serde_json::from_value(action.args.clone())?;
            Ok(("drone_recover", json!({"request":request})))
        }
        _ => anyhow::bail!("capability_missing"),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn handler_injection_is_data_not_command() {
        assert!(command(&Action {
            handler: "sh -c id".into(),
            args: json!({})
        })
        .is_err());
        assert!(command(&Action {
            handler: "inspect_owned_process".into(),
            args: json!({"shell":"id"})
        })
        .is_err());
        assert_eq!(
            command(&Action {
                handler: "inspect_owned_process".into(),
                args: json!({})
            })
            .unwrap(),
            ("list_kernel_bash", json!({}))
        );
    }
}
