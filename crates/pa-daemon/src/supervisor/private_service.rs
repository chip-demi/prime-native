//! Root-provisioned service identity, checked against kernel and system manager facts.
use anyhow::{ensure, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ServicePolicy {
    pub unit: String,
    pub unit_path: PathBuf,
    pub unit_sha256: String,
    pub uid: u32,
    pub executable_path: PathBuf,
    pub executable_sha256: String,
    pub launch_path: PathBuf,
    pub launch_sha256: String,
    pub agent_dir: PathBuf,
    pub daemon_socket_path: PathBuf,
    pub source_socket_path: PathBuf,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ServiceIdentity {
    pub pid: u32,
    pub uid: u32,
    pub start: String,
    pub invocation: String,
}
impl ServiceIdentity {
    pub fn digest(&self) -> String {
        format!(
            "{:x}",
            Sha256::digest(format!(
                "{}\0{}\0{}\0{}",
                self.pid, self.uid, self.start, self.invocation
            ))
        )
    }
}
#[cfg(target_os = "linux")]
fn root_metadata(path: &Path, limit: u64) -> Result<std::fs::Metadata> {
    use std::os::unix::fs::MetadataExt;
    ensure!(path.is_absolute(), "private_owner_identity_mismatch");
    let mut current = Some(path);
    while let Some(p) = current {
        let metadata = std::fs::symlink_metadata(p)?;
        ensure!(
            !metadata.file_type().is_symlink()
                && metadata.uid() == 0
                && metadata.mode() & 0o022 == 0,
            "private_owner_identity_mismatch"
        );
        current = p.parent();
    }
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && metadata.len() <= limit,
        "private_owner_identity_mismatch"
    );
    Ok(metadata)
}
#[cfg(target_os = "linux")]
fn same_file(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mode() == after.mode()
        && before.uid() == after.uid()
        && before.gid() == after.gid()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}
#[cfg(target_os = "linux")]
fn root_read(path: &Path, limit: u64, mut consume: impl FnMut(&[u8])) -> Result<()> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    let before = root_metadata(path, limit)?;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)?;
    ensure!(
        same_file(&before, &file.metadata()?),
        "private_owner_identity_mismatch"
    );
    let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
    let mut total = 0u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total += count as u64;
        ensure!(total <= limit, "private_owner_identity_mismatch");
        consume(&buffer[..count]);
    }
    ensure!(
        total == before.len()
            && same_file(&before, &file.metadata()?)
            && same_file(&before, &root_metadata(path, limit)?),
        "private_owner_identity_mismatch"
    );
    Ok(())
}
#[cfg(target_os = "linux")]
fn root_file(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    root_read(path, 16384, |chunk| bytes.extend_from_slice(chunk))?;
    Ok(bytes)
}
#[cfg(target_os = "linux")]
fn root_digest(path: &Path, limit: u64) -> Result<String> {
    let mut digest = Sha256::new();
    root_read(path, limit, |chunk| digest.update(chunk))?;
    Ok(format!("{:x}", digest.finalize()))
}
impl ServicePolicy {
    pub fn load(agent: &Path, socket: &Path) -> Result<Option<Self>> {
        #[cfg(target_os = "linux")]
        {
            let Some(path) = std::env::var_os("PRIME_PRIVATE_OWNER_POLICY") else {
                return Ok(None);
            };
            let bytes = root_file(Path::new(&path))?;
            ensure!(bytes.len() <= 16384, "private_owner_invalid_request");
            let policy: Self = serde_json::from_value(super::private_launch::strict(&bytes)?)?;
            ensure!(
                policy.unit.ends_with(".service")
                    && policy
                        .unit
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.@".contains(&b)),
                "private_owner_identity_mismatch"
            );
            ensure!(
                policy.agent_dir == agent
                    && policy.daemon_socket_path == socket
                    && policy.source_socket_path.is_absolute(),
                "private_owner_identity_mismatch"
            );
            policy.check_files()?;
            Ok(Some(policy))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (agent, socket);
            Ok(None)
        }
    }
    #[cfg(target_os = "linux")]
    fn check_files(&self) -> Result<()> {
        for (path, digest, limit) in [
            // Bound host ELF hashing without allocating the executable in memory.
            (
                &self.executable_path,
                &self.executable_sha256,
                512 * 1024 * 1024,
            ),
            (&self.launch_path, &self.launch_sha256, 64 * 1024 * 1024),
            (&self.unit_path, &self.unit_sha256, 64 * 1024 * 1024),
        ] {
            ensure!(
                digest.len() == 64 && root_digest(path, limit)? == *digest,
                "private_owner_identity_mismatch"
            );
        }
        Ok(())
    }
    #[cfg(target_os = "linux")]
    pub fn attest(&self, peer: Option<(u32, u32)>) -> Result<ServiceIdentity> {
        use std::os::unix::fs::MetadataExt;
        let (pid, uid) = peer.ok_or_else(|| anyhow::anyhow!("private_owner_identity_mismatch"))?;
        ensure!(
            pid > 0 && uid == self.uid,
            "private_owner_identity_mismatch"
        );
        self.check_files()?;
        let read_unit = || -> Result<(u32, String)> {
            let output = std::process::Command::new("/usr/bin/systemctl")
                .args([
                    "show",
                    "--no-pager",
                    "--property=MainPID,InvocationID,ActiveState,FragmentPath,DropInPaths",
                    "--",
                    &self.unit,
                ])
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .output()?;
            ensure!(
                output.status.success() && output.stdout.len() <= 4096,
                "private_owner_identity_mismatch"
            );
            let text = String::from_utf8(output.stdout)?;
            let fields: std::collections::HashMap<_, _> =
                text.lines().filter_map(|s| s.split_once('=')).collect();
            ensure!(
                fields.get("ActiveState") == Some(&"active"),
                "private_owner_identity_mismatch"
            );
            ensure!(
                fields
                    .get("FragmentPath")
                    .is_some_and(|p| Path::new(p) == self.unit_path)
                    && fields.get("DropInPaths").is_some_and(|p| p.is_empty()),
                "private_owner_identity_mismatch"
            );
            let main = fields
                .get("MainPID")
                .ok_or_else(|| anyhow::anyhow!("private_owner_identity_mismatch"))?
                .parse::<u32>()?;
            let invocation = fields
                .get("InvocationID")
                .ok_or_else(|| anyhow::anyhow!("private_owner_identity_mismatch"))?
                .to_string();
            ensure!(
                invocation.len() == 32 && invocation.bytes().all(|b| b.is_ascii_hexdigit()),
                "private_owner_identity_mismatch"
            );
            Ok((main, invocation))
        };
        let first = read_unit()?;
        ensure!(first.0 == pid, "private_owner_identity_mismatch");
        let start = crate::protocol::process_start_id(pid)
            .ok_or_else(|| anyhow::anyhow!("private_owner_identity_mismatch"))?;
        let proc = PathBuf::from(format!("/proc/{pid}"));
        ensure!(
            std::fs::metadata(&proc)?.uid() == uid
                && std::fs::read_link(proc.join("exe"))? == self.executable_path,
            "private_owner_identity_mismatch"
        );
        let command = std::fs::read(proc.join("cmdline"))?;
        ensure!(
            command
                .split(|b| *b == 0)
                .any(|arg| arg == self.launch_path.as_os_str().as_encoded_bytes()),
            "private_owner_identity_mismatch"
        );
        let cgroup = std::fs::read_to_string(proc.join("cgroup"))?;
        ensure!(
            cgroup.lines().any(|line| line
                .split(':')
                .nth(2)
                .is_some_and(|path| path.split('/').any(|unit| unit == self.unit))),
            "private_owner_identity_mismatch"
        );
        // Check actual mount namespace view, not a caller-supplied namespace name.
        for path in [&self.agent_dir, &self.daemon_socket_path] {
            let local = std::fs::metadata(path)?;
            let remote = std::fs::metadata(proc.join("root").join(path.strip_prefix("/")?))?;
            ensure!(
                local.dev() == remote.dev() && local.ino() == remote.ino(),
                "private_owner_identity_mismatch"
            );
        }
        ensure!(
            read_unit()? == first
                && crate::protocol::process_start_id(pid).as_ref() == Some(&start),
            "private_owner_generation_changed"
        );
        Ok(ServiceIdentity {
            pid,
            uid,
            start,
            invocation: first.1,
        })
    }
    #[cfg(not(target_os = "linux"))]
    pub fn attest(&self, _: Option<(u32, u32)>) -> Result<ServiceIdentity> {
        anyhow::bail!("private_owner_unsupported")
    }
}

#[cfg(all(test, target_os = "linux"))]
mod kernel_tests {
    use super::*;
    use pa_types::platform::transport::TransportStream;

    #[test]
    fn root_stream_bounds_and_identity() {
        use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
        let Ok(dir) = std::env::var("PRIME_TEST_ROOT_STREAM_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        assert_eq!(std::fs::metadata(&dir).unwrap().uid(), 0);
        let node = Path::new("/opt/prime-host/releases/host-migration-20261019/node/node");
        assert_eq!(std::fs::metadata(node).unwrap().len(), 123_438_592);
        assert_eq!(
            root_digest(node, 512 * 1024 * 1024).unwrap(),
            "b2959781cc5a74c357ffa02367efa8a0330cbb1c9cb347732fdfaaaca381cbcd"
        );
        assert!(root_digest(node, 64 * 1024 * 1024).is_err());
        let small = dir.join("small.json");
        std::fs::write(&small, b"{}").unwrap();
        assert_eq!(root_file(&small).unwrap(), b"{}");
        let large = dir.join("oversize.json");
        std::fs::write(&large, vec![b' '; 16385]).unwrap();
        assert!(root_file(&large).is_err());
        let oversized_elf = dir.join("oversize.elf");
        std::fs::File::create(&oversized_elf)
            .unwrap()
            .set_len(512 * 1024 * 1024 + 1)
            .unwrap();
        assert!(root_digest(&oversized_elf, 512 * 1024 * 1024).is_err());
        let link = dir.join("link");
        symlink(&small, &link).unwrap();
        assert!(root_file(&link).is_err());
        std::fs::set_permissions(&small, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(root_file(&small).is_err());
        std::fs::set_permissions(&small, std::fs::Permissions::from_mode(0o644)).unwrap();
        let writable = dir.join("writable");
        std::fs::create_dir(&writable).unwrap();
        let child = writable.join("child");
        std::fs::write(&child, b"{}").unwrap();
        std::fs::set_permissions(&writable, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(root_file(&child).is_err());
        std::fs::set_permissions(&writable, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(std::process::Command::new("/usr/bin/chown")
            .args(["1001:1001", child.to_str().unwrap()])
            .status()
            .unwrap()
            .success());
        assert!(root_file(&child).is_err());
        let replacement = dir.join("replacement");
        std::fs::write(&replacement, b"{}").unwrap();
        let mut replaced = false;
        assert!(root_read(&small, 16384, |_| {
            if !replaced {
                std::fs::rename(&replacement, &small).unwrap();
                replaced = true;
            }
        })
        .is_err());
        let mut changed = false;
        assert!(root_read(&small, 16384, |_| {
            if !changed {
                std::fs::write(&small, b"changed").unwrap();
                changed = true;
            }
        })
        .is_err());
    }

    #[tokio::test]
    async fn actual_kernel_peer_cannot_forge_service_process() {
        let (stream, _other) = tokio::net::UnixStream::pair().unwrap();
        let (pid, uid) = stream.peer_credentials().expect("actual SO_PEERCRED");
        assert_eq!(pid, std::process::id());
        let policy = ServicePolicy {
            unit: "not-an-approved-service.service".into(),
            unit_path: "/nonexistent".into(),
            unit_sha256: "0".repeat(64),
            uid,
            executable_path: "/nonexistent".into(),
            executable_sha256: "0".repeat(64),
            launch_path: "/nonexistent".into(),
            launch_sha256: "0".repeat(64),
            agent_dir: "/".into(),
            daemon_socket_path: "/".into(),
            source_socket_path: "/".into(),
        };
        assert!(policy.attest(Some((pid, uid))).is_err());
        assert!(policy.attest(None).is_err());
    }

    // Explicit opt-in permits read-only inspection of a real root-owned service.
    // Never starts, stops, or modifies a service or its files.
    #[test]
    fn actual_systemd_root_policy_rejects_forged_peer_and_hash() {
        let Ok(unit) = std::env::var("PRIME_TEST_ATTEST_UNIT") else {
            return;
        };
        let output = std::process::Command::new("/usr/bin/systemctl")
            .args(["show", &unit, "--property=MainPID,FragmentPath"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        let fields: std::collections::HashMap<_, _> =
            text.lines().filter_map(|s| s.split_once('=')).collect();
        let pid: u32 = fields["MainPID"].parse().unwrap();
        let exe = std::fs::read_link(format!("/proc/{pid}/exe")).unwrap();
        let fragment = PathBuf::from(fields["FragmentPath"]);
        let hash = |p: &Path| format!("{:x}", Sha256::digest(root_file(p).unwrap()));
        let mut policy = ServicePolicy {
            unit,
            unit_path: fragment.clone(),
            unit_sha256: hash(&fragment),
            uid: 0,
            executable_path: exe.clone(),
            executable_sha256: hash(&exe),
            launch_path: exe.clone(),
            launch_sha256: hash(&exe),
            agent_dir: "/".into(),
            daemon_socket_path: "/".into(),
            source_socket_path: "/".into(),
        };
        let identity = policy
            .attest(Some((pid, 0)))
            .expect("actual systemd/proc identity");
        assert_eq!(identity.pid, pid);
        assert!(policy.attest(Some((std::process::id(), 0))).is_err());
        assert!(policy.attest(Some((pid, 1020))).is_err());
        policy.unit_sha256 = "0".repeat(64);
        assert!(policy.attest(Some((pid, 0))).is_err());
        assert!(root_file(Path::new("relative")).is_err());
    }
}
