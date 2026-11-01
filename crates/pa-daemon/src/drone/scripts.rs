//! Registered executable dispatch: fixed absolute executable and schema, never shell text.
use anyhow::Context as _;
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf};
#[derive(Clone)]
pub(crate) struct RegisteredScript {
    pub executable: PathBuf,
    pub expected_bytes: Vec<u8>,
    pub execution_root: PathBuf,
    pub argument_names: Vec<String>,
    pub timeout: std::time::Duration,
    pub output_cap: usize,
}
impl RegisteredScript {
    pub(crate) async fn execute(&self, args: &Value) -> anyhow::Result<Value> {
        use tokio::io::AsyncReadExt;
        anyhow::ensure!(self.executable.is_absolute(), "invalid_executable");
        let actual = tokio::fs::read(&self.executable).await?;
        anyhow::ensure!(actual == self.expected_bytes, "stale_executable");
        anyhow::ensure!(
            self.expected_bytes.starts_with(b"#!/usr/bin/python3\n"),
            "capability_missing:python_only_registered_check"
        );
        let args: BTreeMap<String, String> = serde_json::from_value(args.clone())?;
        anyhow::ensure!(
            args.len() == self.argument_names.len()
                && self
                    .argument_names
                    .iter()
                    .all(|name| args.contains_key(name)),
            "invalid_arguments"
        );
        anyhow::ensure!(self.execution_root.is_absolute(), "invalid_execution_root");
        let directory = self
            .execution_root
            .join(format!("drone-script-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir(&directory).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).await?;
        }
        // The registered source pin is data. Never execute a freshly writable image.
        let mut command = tokio::process::Command::new("/usr/bin/python3");
        command
            .arg("-I")
            .arg("-")
            .env_clear()
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        for name in &self.argument_names {
            command.arg(args.get(name).unwrap());
        }
        let mut child = command.spawn().context("script_spawn")?;
        let result = async {
            let read = async {
                use tokio::io::AsyncWriteExt;
                let mut input = child.stdin.take().context("script_stdin")?;
                input
                    .write_all(&self.expected_bytes)
                    .await
                    .context("script_stdin_write")?;
                input.shutdown().await.context("script_stdin_close")?;
                drop(input);
                let stdout = child.stdout.take().context("missing_stdout")?;
                let mut output = Vec::new();
                stdout
                    .take((self.output_cap + 1) as u64)
                    .read_to_end(&mut output)
                    .await?;
                anyhow::ensure!(output.len() <= self.output_cap, "output_cap");
                let status = child.wait().await.context("script_wait")?;
                anyhow::ensure!(status.success(), "handler_failed");
                Ok::<_, anyhow::Error>(serde_json::from_slice::<Value>(&output)?)
            };
            let result = match tokio::time::timeout(self.timeout, read).await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(error)) => {
                    if child.try_wait()?.is_none() {
                        child.kill().await.context("script_kill")?;
                        let _ = child.wait().await.context("script_wait")?;
                    }
                    Err(error)
                }
                Err(_) => {
                    child.kill().await.context("script_kill")?;
                    let _ = child.wait().await.context("script_wait")?;
                    Err(anyhow::anyhow!("handler_timeout_unconfirmed"))
                }
            };
            result
        }
        .await;
        match result {
            Ok(value) => {
                tokio::fs::remove_dir(&directory)
                    .await
                    .context("script_settled_cleanup")?;
                Ok(value)
            }
            Err(error) => Err(recovery_error(&directory, error)),
        }
    }
}

fn recovery_error(directory: &std::path::Path, error: anyhow::Error) -> anyhow::Error {
    error.context(format!(
        "handler_recovery_unconfirmed:{}",
        directory.display()
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn post_spawn_recovery_io_error_keeps_ownership_and_original_chain() {
        let dir = tempfile::tempdir().unwrap();
        let injected = anyhow::Error::new(std::io::Error::other("injected recovery I/O fault"))
            .context("script_kill");
        let classified = recovery_error(dir.path(), injected);
        assert_eq!(
            classified.to_string(),
            format!("handler_recovery_unconfirmed:{}", dir.path().display())
        );
        assert!(format!("{classified:#}").contains("script_kill: injected recovery I/O fault"));
        assert!(dir.path().exists());
    }
    #[tokio::test]
    async fn writable_source_fd_cannot_block_pinned_python_stdin_execution() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pinned-check");
        let bytes =
            b"#!/usr/bin/python3\nimport json,sys\nprint(json.dumps({'value':sys.argv[1]}))\n"
                .to_vec();
        std::fs::write(&path, &bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let writable = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let direct = std::process::Command::new(&path)
            .arg("safe")
            .spawn()
            .unwrap_err();
        assert_eq!(direct.raw_os_error(), Some(26));
        let script = RegisteredScript {
            executable: path,
            expected_bytes: bytes,
            execution_root: dir.path().to_path_buf(),
            argument_names: vec!["value".into()],
            timeout: std::time::Duration::from_secs(2),
            output_cap: 256,
        };
        assert_eq!(
            script
                .execute(&serde_json::json!({"value":"safe"}))
                .await
                .unwrap(),
            serde_json::json!({"value":"safe"})
        );
        drop(writable);
    }
    #[tokio::test]
    async fn unsupported_script_timeout_is_never_reported_settled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held");
        let bytes = b"#!/usr/bin/python3\nimport time\ntime.sleep(60)\n".to_vec();
        std::fs::write(&path, &bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let script = RegisteredScript {
            executable: path,
            expected_bytes: bytes,
            execution_root: dir.path().to_path_buf(),
            argument_names: vec![],
            timeout: std::time::Duration::from_millis(25),
            output_cap: 16,
        };
        let failure = script.execute(&serde_json::json!({})).await.unwrap_err();
        assert!(
            failure
                .to_string()
                .starts_with("handler_recovery_unconfirmed:"),
            "actual failure: {failure:#}"
        );
    }
    #[tokio::test]
    async fn timeout_and_output_cap_preserve_unconfirmed_script_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held");
        let bytes = b"#!/usr/bin/python3\nimport time\ntime.sleep(60)\n".to_vec();
        std::fs::write(&path, &bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let script = RegisteredScript {
            executable: path.clone(),
            expected_bytes: bytes,
            execution_root: dir.path().to_path_buf(),
            argument_names: vec![],
            timeout: std::time::Duration::from_millis(25),
            output_cap: 16,
        };
        let failure = script.execute(&serde_json::json!({})).await.unwrap_err();
        assert!(
            failure
                .to_string()
                .starts_with("handler_recovery_unconfirmed:"),
            "actual failure: {failure:#}"
        );
        let bytes = b"#!/usr/bin/python3\nprint('x'*1000)\n".to_vec();
        std::fs::write(&path, &bytes).unwrap();
        let script = RegisteredScript {
            expected_bytes: bytes,
            timeout: std::time::Duration::from_secs(2),
            ..script
        };
        let failure = script.execute(&serde_json::json!({})).await.unwrap_err();
        assert!(
            failure
                .to_string()
                .starts_with("handler_recovery_unconfirmed:"),
            "actual failure: {failure:#}"
        );
    }
    #[tokio::test]
    async fn fixed_argv_injection_and_executable_pin_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("check");
        let bytes =
            b"#!/usr/bin/python3\nimport json,sys\nprint(json.dumps({'argument':sys.argv[1]}))\n"
                .to_vec();
        std::fs::write(&path, &bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let script = RegisteredScript {
            executable: path.clone(),
            expected_bytes: bytes,
            execution_root: dir.path().to_path_buf(),
            argument_names: vec!["value".into()],
            timeout: std::time::Duration::from_secs(2),
            output_cap: 256,
        };
        let injection = "$(touch /not-a-real-target); arbitrary | shell";
        assert_eq!(
            script
                .execute(&serde_json::json!({"value":injection}))
                .await
                .unwrap(),
            serde_json::json!({"argument":injection})
        );
        assert!(script
            .execute(&serde_json::json!({"shell":"id"}))
            .await
            .is_err());
        std::fs::write(&path, b"changed").unwrap();
        assert!(script
            .execute(&serde_json::json!({"value":"x"}))
            .await
            .is_err());
    }
}
