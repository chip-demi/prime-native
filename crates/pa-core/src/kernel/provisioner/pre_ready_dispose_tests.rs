//! Disposal while the owned process has spawned but has not announced protocol readiness.

use super::{snapshot_path_in, IpythonKernelProvisioner, IpythonKernelProvisionerOptions};
use crate::platform::process;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

const NEVER_READY_PYTHON: &str = r#"#!/usr/bin/env python3
import os
import sys

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "spawned-pids"), "a") as f:
    f.write(str(os.getpid()) + "\n")
for line in sys.stdin:
    pass
"#;

enum PreReadyDisposal {
    Dispose,
    KillThenDispose,
}

#[tokio::test]
async fn dispose_before_protocol_ready_reaps_the_owned_process_within_shutdown_grace() {
    assert_pre_ready_disposal_reaps(PreReadyDisposal::Dispose).await;
}

#[tokio::test]
async fn kill_then_dispose_before_protocol_ready_reaps_the_owned_process_within_shutdown_grace() {
    assert_pre_ready_disposal_reaps(PreReadyDisposal::KillThenDispose).await;
}

async fn assert_pre_ready_disposal_reaps(disposal: PreReadyDisposal) {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let python = dir.path().join("not-ready-python");
    let pid_path = dir.path().join("spawned-pids");
    // The first observable action records the real child identity. No ready
    // frame, bootstrap response or provider request can complete this startup.
    std::fs::write(&python, NEVER_READY_PYTHON).expect("write fake interpreter");
    std::fs::set_permissions(&python, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake interpreter");
    let snapshot_dir = dir.path().join("snapshot");
    std::fs::create_dir(&snapshot_dir).expect("snapshot directory");
    let snapshot_path = snapshot_path_in(&snapshot_dir);
    let saved_snapshot = b"the earlier saved namespace must survive a never-ready boot";
    std::fs::write(&snapshot_path, saved_snapshot).expect("write prior snapshot");
    let provisioner = IpythonKernelProvisioner::new(
        dir.path(),
        IpythonKernelProvisionerOptions {
            python: Some(python),
            snapshot_dir: Some(snapshot_dir),
            ..Default::default()
        },
    );
    let starting = provisioner.clone();
    let mut startup = tokio::spawn(async move { starting.ensure(None, None).await });
    let pid = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(recorded) = std::fs::read_to_string(&pid_path) {
                if let Ok(pid) = recorded.trim().parse::<u32>() {
                    break pid;
                }
            }
            assert!(
                !startup.is_finished(),
                "startup failed before the process identity was recorded"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the owned fake process must announce its identity");
    assert!(
        process::pid_exists(pid),
        "the process must still be withholding ready"
    );
    assert!(
        !startup.is_finished(),
        "the protocol-ready handshake must still be pending"
    );
    // Pin the exact instance for red-path cleanup rather than signal a possibly recycled PID.
    let fd = process::open_pidfd(pid).expect("pin the owned fake process");
    // SAFETY: open_pidfd returns a fresh descriptor owned by this test.
    let held_process = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut startup_result = None;
    // The existing Studio SDK gives shutdown three seconds before SIGKILL.
    // Count disposal, the shared startup result and actual reap against that same grace.
    let settled = tokio::time::timeout(Duration::from_secs(3), async {
        match disposal {
            PreReadyDisposal::Dispose => {}
            PreReadyDisposal::KillThenDispose => provisioner.kill(),
        }
        provisioner.dispose(None).await;
        startup_result = Some((&mut startup).await);
        // pid_exists includes zombies: disappearance proves the manager watcher reaped it.
        while process::pid_exists(pid) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if settled.is_err() {
        // Retain a clean red result without leaking the deliberately wedged owned child.
        let _ = process::pidfd_signal(held_process.as_raw_fd(), process::Signal::Kill);
        let cleanup = tokio::time::timeout(Duration::from_secs(3), async {
            if startup_result.is_none() {
                startup_result = Some((&mut startup).await);
            }
            while process::pid_exists(pid) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            cleanup.is_ok(),
            "failed regression must still reap its exact owned child"
        );
    }
    assert!(
        settled.is_ok(),
        "dispose must cancel the pre-ready startup and reap before the SDK shutdown grace"
    );
    let failure = startup_result
        .expect("startup result was joined")
        .expect("startup task must not panic")
        .expect_err("a disposed never-ready startup cannot publish a kernel");
    assert!(format!("{failure:#}").contains("Kernel startup aborted"));
    assert!(!provisioner.has_running_kernel());
    assert!(!process::pid_exists(pid));
    assert_eq!(
        std::fs::read_to_string(&pid_path).expect("recorded child"),
        format!("{pid}\n"),
        "dispose must not retry the cancelled startup"
    );
    assert_eq!(
        std::fs::read(&snapshot_path).expect("prior snapshot survives"),
        saved_snapshot.to_vec(),
        "a never-ready kernel must not overwrite the existing namespace"
    );
}
