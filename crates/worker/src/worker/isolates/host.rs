//! Starting one `legion-isolate host` process and the two gRPC channels to
//! it (brief item 2): the worker is the client of `IsolateHost` on the
//! control socket and serves `IsolateWorker` on the worker socket. Both are
//! one end of a `socketpair` the host inherits; the third fd carries its
//! `HostConfig` as JSON.
//!
//! [`HostLauncher`] is the seam tests use: [`ProcessLauncher`] starts the
//! real binary; a test launcher serves a fake `IsolateHost` in-process over
//! the same kind of socket, so everything above it runs unchanged.

use crate::prelude::*;
use crate::structs::isolate_host::isolate_host_client::IsolateHostClient;
use crate::structs::isolate_host::isolate_worker_server::{IsolateWorker, IsolateWorkerServer};
use crate::structs::isolate_host::HostConfig;
use std::path::{Path, PathBuf};
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};

/// The largest message either side sends: a run's staged files and reply
/// are a few MiB; checkpoints are streamed.
pub const MAX_MESSAGE_BYTES: usize = 64 << 20;

/// One host process the launcher started.
pub struct Launched {
    pub client: IsolateHostClient<Channel>,
    pub pid: u32,
    /// Becomes `Some(why)` when the process exits.
    pub exited: watch::Receiver<Option<String>>,
    /// Kills the process (idempotent).
    pub kill: Box<dyn Fn() + Send + Sync>,
    /// The host's own cgroup, when it has one.
    pub cgroup: Option<PathBuf>,
}

impl std::fmt::Debug for Launched {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Launched").field("pid", &self.pid).finish()
    }
}

/// What to start.
#[derive(Debug, Clone)]
pub struct HostSpawn {
    pub cordon: String,
    pub replica: u32,
    pub config: HostConfig,
    /// The host's cgroup `memory.max`, when the node governs it.
    pub memory_max_bytes: Option<u64>,
}

/// Starts host processes.
#[tonic::async_trait]
pub trait HostLauncher: Send + Sync + 'static {
    /// Start one host and serve `worker` on its worker socket.
    async fn launch(&self, spawn: &HostSpawn, worker: super::service::WorkerService) -> R<Launched>;
}

/// A connected `IsolateHost` client over an already-connected stream.
pub async fn client_over(stream: tokio::net::UnixStream) -> R<IsolateHostClient<Channel>> {
    let slot = Arc::new(Mutex::new(Some(stream)));
    let channel = Endpoint::try_from("http://isolate-host.local")?
        .connect_with_connector(tower::service_fn(move |_: http::Uri| {
            let stream = slot.lock().ok().and_then(|mut slot| slot.take());
            async move {
                stream.map(hyper_util::rt::TokioIo::new).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "the isolate host's control socket is closed",
                    )
                })
            }
        }))
        .await
        .context("connect to the isolate host's control socket")?;
    Ok(IsolateHostClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES))
}

/// Serve `service` on one already-connected stream until it closes.
pub fn serve_over<S: IsolateWorker>(stream: tokio::net::UnixStream, service: S) {
    let server = IsolateWorkerServer::new(service)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    tokio::spawn(async move {
        let incoming = tokio_stream::once(Ok::<_, std::io::Error>(stream));
        if let Err(err) = tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(incoming)
            .await
        {
            debug!(error = %err, "Isolate worker socket closed");
        }
    });
}

/// A connected pair of Unix stream sockets, both close-on-exec.
#[cfg(unix)]
pub fn socket_pair() -> R<(std::os::fd::OwnedFd, std::os::fd::OwnedFd)> {
    use std::os::fd::FromRawFd;
    let mut fds = [0i32; 2];
    // SAFETY: socketpair writes two fds into the array on success.
    let rc = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("socketpair");
    }
    // SAFETY: both fds were just created and are owned by nobody else.
    Ok(unsafe {
        (
            std::os::fd::OwnedFd::from_raw_fd(fds[0]),
            std::os::fd::OwnedFd::from_raw_fd(fds[1]),
        )
    })
}

/// A tokio stream over one end of a socket pair.
#[cfg(unix)]
pub fn tokio_stream(fd: std::os::fd::OwnedFd) -> R<tokio::net::UnixStream> {
    let std_stream = std::os::unix::net::UnixStream::from(fd);
    std_stream.set_nonblocking(true)?;
    Ok(tokio::net::UnixStream::from_std(std_stream)?)
}

/// Starts the real `legion-isolate host`.
#[derive(Debug, Clone)]
pub struct ProcessLauncher {
    pub binary: PathBuf,
    /// Where host cgroups go (`<cgroup root>/<parent>/isolate-hosts`), when
    /// the node governs resources with cgroup v2.
    pub cgroup_dir: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
fn config_memfd(config: &HostConfig) -> R<std::os::fd::OwnedFd> {
    use std::io::{Seek, Write};
    use std::os::fd::FromRawFd;
    let json = serde_json::to_vec(config)?;
    let name = std::ffi::CString::new("legion-isolate-config").expect("no NUL");
    // SAFETY: a plain syscall; the fd it returns is owned below.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("memfd_create");
    }
    // SAFETY: fd was just created.
    let owned = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
    let mut file = std::fs::File::from(owned);
    file.write_all(&json)?;
    file.seek(std::io::SeekFrom::Start(0))?;
    Ok(file.into())
}

fn sanitize(cordon: &str) -> String {
    cordon
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' })
        .collect()
}

/// Create the host's cgroup and move `pid` into it. Best effort: a node
/// that cannot (no delegation, cgroup v1) runs the host in the worker's.
fn place_in_cgroup(dir: &Path, spawn: &HostSpawn, pid: u32) -> R<PathBuf> {
    std::fs::create_dir_all(dir)?;
    if let Some(parent) = dir.parent() {
        let _ = std::fs::write(parent.join("cgroup.subtree_control"), "+memory +pids");
    }
    let _ = std::fs::write(dir.join("cgroup.subtree_control"), "+memory +pids");
    let leaf = dir.join(format!("{}-{}", sanitize(&spawn.cordon), spawn.replica));
    std::fs::create_dir_all(&leaf)?;
    if let Some(max) = spawn.memory_max_bytes.filter(|max| *max > 0) {
        std::fs::write(leaf.join("memory.max"), max.to_string())?;
    }
    // Shards plus the epoch ticker and the I/O runtime; generous.
    std::fs::write(leaf.join("pids.max"), "1024")?;
    std::fs::write(leaf.join("cgroup.procs"), pid.to_string())?;
    Ok(leaf)
}

#[cfg(target_os = "linux")]
#[tonic::async_trait]
impl HostLauncher for ProcessLauncher {
    async fn launch(
        &self,
        spawn: &HostSpawn,
        worker: super::service::WorkerService,
    ) -> R<Launched> {
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;
        let (control_ours, control_theirs) = socket_pair()?;
        let (worker_ours, worker_theirs) = socket_pair()?;
        let config = config_memfd(&spawn.config)?;
        let inherited = [
            control_theirs.as_raw_fd(),
            worker_theirs.as_raw_fd(),
            config.as_raw_fd(),
        ];
        let mut command = std::process::Command::new(&self.binary);
        command
            .arg("host")
            .arg("--control-fd")
            .arg(inherited[0].to_string())
            .arg("--worker-fd")
            .arg(inherited[1].to_string())
            .arg("--config-fd")
            .arg(inherited[2].to_string())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .env_clear()
            .env("RUST_BACKTRACE", "1");
        // SAFETY: only async-signal-safe calls (fcntl, prctl) between fork
        // and exec.
        unsafe {
            command.pre_exec(move || {
                for fd in inherited {
                    if libc::fcntl(fd, libc::F_SETFD, 0) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                // The host dies with the worker: a crashed worker never
                // leaves hosts behind (it also exits when its control
                // socket closes).
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        // Spawned from an async (long-lived runtime) thread: the death
        // signal follows the thread that forked, so never a blocking-pool
        // thread that may exit while the host runs.
        let mut child = tokio::process::Command::from(command)
            .kill_on_drop(false)
            .spawn()
            .with_context(|| format!("start {} host", self.binary.display()))?;
        drop((control_theirs, worker_theirs, config));
        let pid = child.id().unwrap_or_default();
        let mut cgroup = None;
        if let Some(dir) = self.cgroup_dir.as_ref() {
            match place_in_cgroup(dir, spawn, pid) {
                Ok(leaf) => {
                    debug!(cordon = %spawn.cordon, cgroup = %leaf.display(), "Isolate host placed in its cgroup");
                    cgroup = Some(leaf);
                }
                Err(err) => warn!(cordon = %spawn.cordon, error = %format!("{err:#}"), "Could not place the isolate host in its own cgroup; it runs in the worker's"),
            }
        }
        if let Some(stderr) = child.stderr.take() {
            let cordon = spawn.cordon.clone();
            let replica = spawn.replica;
            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut lines = tokio::io::BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if line.contains("ERROR") || line.contains("WARN") || line.contains("panicked") {
                        info!(cordon = %cordon, replica, "isolate host: {line}");
                    } else {
                        debug!(cordon = %cordon, replica, "isolate host: {line}");
                    }
                }
            });
        }
        let (exit_tx, exited) = watch::channel(None);
        tokio::spawn(async move {
            let why = match child.wait().await {
                Ok(status) => format!("exited ({status})"),
                Err(err) => format!("could not be waited for: {err}"),
            };
            let _ = exit_tx.send(Some(why));
        });
        serve_over(tokio_stream(worker_ours)?, worker);
        let client = client_over(tokio_stream(control_ours)?).await?;
        let kill = Box::new(move || {
            if pid > 1 {
                // SAFETY: a plain kill(2) of the child we started.
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
        });
        Ok(Launched {
            client,
            pid,
            exited,
            kill,
            cgroup,
        })
    }
}

#[cfg(not(target_os = "linux"))]
#[tonic::async_trait]
impl HostLauncher for ProcessLauncher {
    async fn launch(
        &self,
        _spawn: &HostSpawn,
        _worker: super::service::WorkerService,
    ) -> R<Launched> {
        bail!("isolate hosts run on Linux only")
    }
}
