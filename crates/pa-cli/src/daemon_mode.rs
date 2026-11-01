//! `--mode daemon`: the supervisor process. The interactive client spawns this
//! mode (detached) when no daemon is listening.

use anyhow::Result;

use crate::config;

pub fn run_daemon_mode(daemon_socket: Option<&str>) -> Result<i32> {
    let socket_path = config::resolve_daemon_socket_path(daemon_socket);
    let agent_dir = config::get_agent_dir();
    let options = pa_daemon::supervisor::SupervisorOptions {
        socket_path,
        agent_dir,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(pa_daemon::supervisor::run_supervisor(options))?;
    Ok(0)
}

pub fn run_private_daemon_mode(daemon_socket: Option<&str>) -> Result<i32> {
    #[cfg(not(target_os = "linux"))]
    anyhow::bail!("Private Studio launch unsupported");
    #[cfg(target_os = "linux")]
    {
        let options = pa_daemon::supervisor::SupervisorOptions {
            socket_path: config::resolve_daemon_socket_path(daemon_socket),
            agent_dir: config::get_agent_dir(),
        };
        let stream = pa_types::platform::process::take_studio_launch_stream()?;
        let owner = pa_daemon::supervisor::private_launch::setup(stream, &options.socket_path)?;
        // setup consumes/drops fd3 before the runtime can adopt or spawn workers.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(pa_daemon::supervisor::run_private_supervisor(
            options, owner,
        ))?;
        Ok(0)
    }
}
