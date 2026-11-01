//! Studio's optional per-project `rlm.spawn` defaults. The daemon reads the
//! Studio-owned v1 file at spawn time; absent configuration preserves native inheritance.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

const POLICY_ENV: &str = "PRIME_STUDIO_SUBAGENT_POLICY";
const MAX_POLICY_BYTES: u64 = 1024 * 1024;
const THINKING_LEVELS: &[&str] = &["off", "minimal", "low", "medium", "high", "xhigh", "max"];

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct SubagentPolicy {
    pub model: String,
    pub thinking: String,
}

impl SubagentPolicy {
    pub(super) fn apply<'a>(
        &'a self,
        model: Option<&'a str>,
        thinking: Option<&'a str>,
    ) -> (Option<&'a str>, Option<&'a str>) {
        (
            model.or_else(|| (!self.model.is_empty()).then_some(self.model.as_str())),
            thinking.or_else(|| (!self.thinking.is_empty()).then_some(self.thinking.as_str())),
        )
    }
}

#[derive(Deserialize)]
struct PolicyFile {
    version: u64,
    #[serde(rename = "revision")]
    _revision: String,
    global: SubagentPolicy,
    projects: HashMap<String, SubagentPolicy>,
}

fn invalid_policy() -> anyhow::Error {
    anyhow::anyhow!(
        "Studio subagent settings are unreadable. Fix the configuration before delegating."
    )
}

fn valid_policy(policy: &SubagentPolicy) -> bool {
    if !policy.thinking.is_empty() && !THINKING_LEVELS.contains(&policy.thinking.as_str()) {
        return false;
    }
    if policy.model.is_empty() {
        return true;
    }
    let Some((provider, model)) = policy.model.split_once('/') else {
        return false;
    };
    let mut provider_chars = provider.chars();
    if provider.len() > 100
        || !provider_chars
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        || !provider_chars
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c))
    {
        return false;
    }
    let model_len = model.chars().count();
    (1..=300).contains(&model_len)
        && !model
            .chars()
            .any(|c| c.is_whitespace() || c.is_ascii_control())
}

fn project_key(cwd: &str) -> Result<String> {
    let absolute = std::path::absolute(cwd)?;
    let key = absolute.to_string_lossy().to_string();
    if cfg!(windows) {
        Ok(key.to_lowercase())
    } else {
        Ok(key)
    }
}

fn policy_for_file(cwd: &str, file: Option<&Path>) -> Result<SubagentPolicy> {
    let Some(file) = file else {
        return Ok(SubagentPolicy::default());
    };
    let result = (|| -> Result<SubagentPolicy> {
        let metadata = match fs::symlink_metadata(file) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(SubagentPolicy::default());
            }
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAX_POLICY_BYTES
        {
            return Err(invalid_policy());
        }
        let mut data = Vec::new();
        File::open(file)?
            .take(MAX_POLICY_BYTES + 1)
            .read_to_end(&mut data)?;
        if data.len() as u64 > MAX_POLICY_BYTES {
            return Err(invalid_policy());
        }
        let parsed: PolicyFile = serde_json::from_slice(&data)?;
        if parsed.version != 1
            || !valid_policy(&parsed.global)
            || !parsed.projects.values().all(valid_policy)
        {
            return Err(invalid_policy());
        }
        Ok(parsed
            .projects
            .get(&project_key(cwd)?)
            .cloned()
            .unwrap_or(parsed.global))
    })();
    result.map_err(|_| invalid_policy())
}

pub(super) async fn policy_for(cwd: &str) -> Result<SubagentPolicy> {
    let Some(file) = std::env::var_os(POLICY_ENV).map(PathBuf::from) else {
        return Ok(SubagentPolicy::default());
    };
    let cwd = cwd.to_string();
    tokio::task::spawn_blocking(move || policy_for_file(&cwd, Some(&file)))
        .await
        .context("Studio subagent policy read task failed")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_override_and_explicit_empty_values() {
        let dir = tempfile::TempDir::new().unwrap();
        let cwd = dir.path().join("project");
        let key = project_key(cwd.to_str().unwrap()).unwrap();
        let file = dir.path().join("policy.json");
        fs::write(
            &file,
            serde_json::json!({
                "version": 1, "revision": "r1",
                "global": { "model": "openai/default", "thinking": "low" },
                "projects": { (key.clone()): { "model": "meta/special", "thinking": "high" } }
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            policy_for_file(cwd.to_str().unwrap(), Some(&file)).unwrap(),
            SubagentPolicy {
                model: "meta/special".to_string(),
                thinking: "high".to_string(),
            }
        );
        let selected = policy_for_file(cwd.to_str().unwrap(), Some(&file)).unwrap();
        assert_eq!(
            selected.apply(None, None),
            (Some("meta/special"), Some("high"))
        );
        assert_eq!(
            selected.apply(Some("openai/explicit"), Some("off")),
            (Some("openai/explicit"), Some("off"))
        );
        assert_eq!(
            policy_for_file(dir.path().to_str().unwrap(), Some(&file)).unwrap(),
            SubagentPolicy {
                model: "openai/default".to_string(),
                thinking: "low".to_string(),
            }
        );
        assert_eq!(
            policy_for_file(cwd.to_str().unwrap(), None).unwrap(),
            SubagentPolicy::default()
        );
        fs::write(
            &file,
            serde_json::json!({
                "version": 1, "revision": "r2",
                "global": { "model": "openai/default", "thinking": "low" },
                "projects": { (key): { "model": "", "thinking": "" } }
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            policy_for_file(cwd.to_str().unwrap(), Some(&file)).unwrap(),
            SubagentPolicy::default()
        );
    }

    #[test]
    fn invalid_file_fails_closed() {
        let dir = tempfile::TempDir::new().unwrap();
        let file = dir.path().join("policy.json");
        assert_eq!(
            policy_for_file("/project", Some(&file)).unwrap(),
            SubagentPolicy::default()
        );
        fs::write(&file, r#"{"version":1,"revision":"r","global":{"model":"bad","thinking":"low"},"projects":{}}"#).unwrap();
        assert!(policy_for_file("/project", Some(&file)).is_err());
        fs::write(&file, "x".repeat(MAX_POLICY_BYTES as usize + 1)).unwrap();
        assert!(policy_for_file("/project", Some(&file)).is_err());
    }
}
