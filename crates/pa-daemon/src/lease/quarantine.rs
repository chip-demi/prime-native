//! Durable session-level admission fence. Not process termination or historical settlement.
use super::{
    canonical_session_path, fs, lease_directory, with_lease_guard, Deserialize, Digest, GuardWait,
    Path, PathBuf, Result, Serialize, Sha256,
};
use std::io::{Read, Write};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SessionQuarantine {
    pub version: u32,
    pub parent_session_file: String,
    pub child_id: String,
    pub child_session_file: String,
    pub task: serde_json::Value,
    pub assignment: serde_json::Value,
    pub current_admission_generation: Option<String>,
    pub current_expected_message_id: Option<String>,
}

pub(crate) fn quarantine_command_is_control(command: &str) -> bool {
    matches!(
        command,
        "kill"
            | "shutdown"
            | "abort"
            | "abort_compaction"
            | "abort_side_question"
            | "abort_bash"
            | "get_state"
            | "get_orchestration_gate"
            | "get_rlm_children"
            | "get_session_header"
            | "get_session_stats"
            | "get_queue"
            | "list_kernel_bash"
            | "tail_kernel_bash"
            | "get_owned_worker_cleanup"
            | "persist_owner_cancel"
            | "update_snapshot"
    )
}

fn marker_path(agent_dir: &Path, session: &Path) -> PathBuf {
    lease_directory(agent_dir, session).with_extension("quarantine.json")
}

pub(super) fn read_marker(agent_dir: &Path, session: &Path) -> Result<Option<SessionQuarantine>> {
    let root = agent_dir.join("session-leases");
    if let Ok(metadata) = fs::symlink_metadata(&root) {
        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "Symlink lease authority refused"
        );
    }
    let path = marker_path(agent_dir, session);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "Invalid session quarantine file"
    );
    let marker: SessionQuarantine = serde_json::from_slice(&fs::read(&path)?)?;
    anyhow::ensure!(
        marker.version == 1
            && Path::new(&marker.child_session_file) == canonical_session_path(session),
        "Invalid session quarantine identity/version"
    );
    Ok(Some(marker))
}

pub(super) fn assert_not_quarantined_locked(agent_dir: &Path, session: &Path) -> Result<()> {
    anyhow::ensure!(
        read_marker(agent_dir, session)?.is_none(),
        "Session quarantined: future admission denied; historical execution remains unproved"
    );
    Ok(())
}

pub(crate) fn verified_session_quarantine(
    agent_dir: &Path,
    session: &Path,
    expected: &SessionQuarantine,
    digest: &str,
) -> Result<()> {
    #[cfg(not(unix))]
    anyhow::bail!("Secure quarantine ownership validation unsupported");
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let directory = lease_directory(agent_dir, session);
        with_lease_guard(&directory, GuardWait::ThroughStale, || {
            let root = agent_dir.join("session-leases");
            anyhow::ensure!(
                !fs::symlink_metadata(&root)?.file_type().is_symlink(),
                "Symlink quarantine authority refused"
            );
            let mut options = fs::OpenOptions::new();
            options
                .read(true)
                .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC | nix::libc::O_NONBLOCK);
            let mut file = options.open(marker_path(agent_dir, session))?;
            let metadata = file.metadata()?;
            anyhow::ensure!(
                metadata.is_file()
                    && metadata.nlink() == 1
                    && metadata.uid() == pa_core::platform::process::current_user_id()
                    && metadata.mode() & 0o777 == 0o600
                    && metadata.len() <= 1024 * 1024,
                "Invalid quarantine file identity/permissions/size"
            );
            let mut bytes = Vec::new();
            std::io::Read::by_ref(&mut file)
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            anyhow::ensure!(bytes.len() <= 1024 * 1024, "Quarantine file too large");
            anyhow::ensure!(
                format!("{:x}", Sha256::digest(&bytes)) == digest,
                "Quarantine hash mismatch"
            );
            let parsed: SessionQuarantine = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                &parsed == expected
                    && parsed.version == 1
                    && Path::new(&parsed.child_session_file) == canonical_session_path(session),
                "Exact durable quarantine required"
            );
            Ok(())
        })
    }
}

pub(crate) fn assert_session_not_quarantined(agent_dir: &Path, session: &Path) -> Result<()> {
    let root = agent_dir.join("session-leases");
    fs::create_dir_all(&root)?;
    let directory = lease_directory(agent_dir, session);
    with_lease_guard(&directory, GuardWait::ThroughStale, || {
        assert_not_quarantined_locked(agent_dir, session)
    })
}

pub(crate) fn persist_session_quarantine(
    agent_dir: &Path,
    marker: &SessionQuarantine,
) -> Result<()> {
    let canonical = canonical_session_path(Path::new(&marker.child_session_file));
    anyhow::ensure!(
        marker.version == 1
            && canonical == Path::new(&marker.child_session_file)
            && canonical.is_absolute(),
        "Exact canonical session quarantine identity required"
    );
    let root = agent_dir.join("session-leases");
    fs::create_dir_all(&root)?;
    anyhow::ensure!(
        !fs::symlink_metadata(&root)?.file_type().is_symlink(),
        "Symlink lease authority refused"
    );
    let directory = lease_directory(agent_dir, &canonical);
    with_lease_guard(&directory, GuardWait::ThroughStale, || {
        if let Some(existing) = read_marker(agent_dir, &canonical)? {
            anyhow::ensure!(&existing == marker, "Session quarantine identity conflict");
            return Ok(());
        }
        let path = marker_path(agent_dir, &canonical);
        let temp = path.with_extension(format!("quarantine.tmp-{}", uuid::Uuid::new_v4()));
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        pa_core::platform::perms::set_private_mode(&mut options);
        let mut file = options.open(&temp)?;
        file.write_all(&serde_json::to_vec(marker)?)?;
        file.sync_all()?;
        pa_core::platform::rename_onto(&temp, &path)?;
        #[cfg(unix)]
        fs::File::open(&root)?.sync_all()?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::acquire_runtime_session_lease;
    #[cfg(unix)]
    #[test]
    fn legacy_closure_secure_marker_denies_link_mode_size_and_missing() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::TempDir::new().unwrap();
        let session = dir.path().join("s.jsonl");
        fs::write(&session, "s").unwrap();
        let marker = SessionQuarantine {
            version: 1,
            parent_session_file: dir.path().join("p.jsonl").to_string_lossy().into(),
            child_id: "c".into(),
            child_session_file: session.to_string_lossy().into(),
            task: serde_json::json!({}),
            assignment: serde_json::json!({}),
            current_admission_generation: None,
            current_expected_message_id: None,
        };
        persist_session_quarantine(dir.path(), &marker).unwrap();
        let path = marker_path(dir.path(), &session);
        let bytes = fs::read(&path).unwrap();
        let digest = format!("{:x}", Sha256::digest(&bytes));
        verified_session_quarantine(dir.path(), &session, &marker, &digest).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(verified_session_quarantine(dir.path(), &session, &marker, &digest).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("linked");
        fs::hard_link(&path, &link).unwrap();
        assert!(verified_session_quarantine(dir.path(), &session, &marker, &digest).is_err());
        fs::remove_file(link).unwrap();
        let saved = dir.path().join("saved");
        fs::rename(&path, &saved).unwrap();
        std::os::unix::fs::symlink(&saved, &path).unwrap();
        assert!(verified_session_quarantine(dir.path(), &session, &marker, &digest).is_err());
        fs::remove_file(&path).unwrap();
        fs::rename(saved, &path).unwrap();
        fs::write(&path, vec![b'x'; 1024 * 1024 + 1]).unwrap();
        assert!(verified_session_quarantine(dir.path(), &session, &marker, &digest).is_err());
        fs::write(&path, "corrupt").unwrap();
        assert!(verified_session_quarantine(dir.path(), &session, &marker, &digest).is_err());
        fs::remove_file(&path).unwrap();
        assert!(verified_session_quarantine(dir.path(), &session, &marker, &digest).is_err());
    }

    #[test]
    fn session_quarantine_guard_linearizes_held_mutation_before_durable_fence() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("s.jsonl");
        fs::write(&path, "before").unwrap();
        let lease = std::sync::Arc::new(acquire_runtime_session_lease(&path, dir.path()).unwrap());
        let marker = SessionQuarantine {
            version: 1,
            parent_session_file: dir.path().join("p.jsonl").to_string_lossy().into(),
            child_id: "c".into(),
            child_session_file: path.to_string_lossy().into(),
            task: serde_json::json!({}),
            assignment: serde_json::json!({}),
            current_admission_generation: None,
            current_expected_message_id: None,
        };
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held = lease.clone();
        let target = path.clone();
        let mutation = std::thread::spawn(move || {
            held.with_owned_mutation(&target, || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                fs::write(&target, "prior admitted mutation")?;
                Ok(())
            })
        });
        entered_rx.recv().unwrap();
        let root = dir.path().to_path_buf();
        let fence = std::thread::spawn(move || persist_session_quarantine(&root, &marker));
        release_tx.send(()).unwrap();
        mutation.join().unwrap().unwrap();
        fence.join().unwrap().unwrap();
        assert!(lease
            .with_owned_mutation(&path, || -> Result<()> { panic!("new mutation admitted") })
            .is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), "prior admitted mutation");
    }

    #[test]
    fn session_quarantine_fences_acquire_and_held_mutation_without_settlement() {
        let dir = tempfile::TempDir::new().unwrap();
        let session = dir.path().join("session.jsonl");
        fs::write(&session, "preserved").unwrap();
        let lease = acquire_runtime_session_lease(&session, dir.path()).unwrap();
        let marker = SessionQuarantine {
            version: 1,
            parent_session_file: dir.path().join("parent.jsonl").to_string_lossy().into(),
            child_id: "child".into(),
            child_session_file: session.to_string_lossy().into(),
            task: serde_json::json!({"generation":1}),
            assignment: serde_json::json!({"assignment":"old"}),
            current_admission_generation: Some("new".into()),
            current_expected_message_id: Some("m".into()),
        };
        persist_session_quarantine(dir.path(), &marker).unwrap();
        persist_session_quarantine(dir.path(), &marker).unwrap();
        assert!(lease
            .with_owned_mutation(&session, || -> Result<()> { panic!("must not mutate") })
            .is_err());
        drop(lease);
        assert!(acquire_runtime_session_lease(&session, dir.path()).is_err());
        assert!(
            acquire_runtime_session_lease(&dir.path().join("unrelated.jsonl"), dir.path()).is_ok()
        );
        assert_eq!(fs::read_to_string(&session).unwrap(), "preserved");
        fs::write(marker_path(dir.path(), &session), "corrupt").unwrap();
        assert!(assert_session_not_quarantined(dir.path(), &session).is_err());
    }
}
