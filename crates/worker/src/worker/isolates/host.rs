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

/// The most `IsolateWorker` calls one host may have in flight (egress,
/// child VMs, checkpoints, logs). Each egress call may buffer up to
/// `isolate.maxBodyBytes` each way, so this also bounds what one host can
/// make the worker hold.
pub const MAX_HOST_CALLS: u32 = 256;

/// The longest host stderr line the worker keeps; the rest of the line is
/// dropped.
pub const MAX_STDERR_LINE: usize = 4096;

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
    async fn launch(&self, spawn: &HostSpawn, worker: super::service::WorkerService)
        -> R<Launched>;
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
        // A host is not trusted with the worker's memory: bound how many
        // calls it may have open at once on its one connection.
        if let Err(err) = tonic::transport::Server::builder()
            .max_concurrent_streams(Some(MAX_HOST_CALLS))
            .concurrency_limit_per_connection(MAX_HOST_CALLS as usize)
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
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Create the host's cgroup with its limits, before the host starts, and
/// open its `cgroup.procs` for the child to join itself between fork and
/// exec (the host forks again at startup into a PID namespace, so moving
/// the spawned pid afterwards would leave the real host behind). Best
/// effort: a node that cannot (no delegation, cgroup v1) runs the host in
/// the worker's.
fn prepare_cgroup(dir: &Path, spawn: &HostSpawn) -> R<(PathBuf, std::os::fd::OwnedFd)> {
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
    use std::os::unix::fs::OpenOptionsExt;
    let procs = std::fs::OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_CLOEXEC)
        .open(leaf.join("cgroup.procs"))?;
    Ok((leaf, procs.into()))
}

/// Whether `pid` is a member of the cgroup `leaf`.
fn in_cgroup(leaf: &Path, pid: u32) -> bool {
    std::fs::read_to_string(leaf.join("cgroup.procs"))
        .map(|procs| procs.lines().any(|line| line.trim() == pid.to_string()))
        .unwrap_or(false)
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
        // The cgroup exists, with its limits, before the host does.
        let prepared = self.cgroup_dir.as_ref().and_then(|dir| {
            prepare_cgroup(dir, spawn)
                .map_err(|err| {
                    warn!(cordon = %spawn.cordon, error = %format!("{err:#}"), "Could not create the isolate host's cgroup; it runs in the worker's")
                })
                .ok()
        });
        let procs_fd = prepared.as_ref().map(|(_, fd)| fd.as_raw_fd());
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
                // Join the host's cgroup ("0" = the writing process) before
                // exec, so everything the host forks is in it too. A failed
                // write is detected after spawn.
                if let Some(fd) = procs_fd {
                    let _ = libc::write(fd, b"0".as_ptr().cast(), 1);
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
        // A pidfd names this process, not its number: signalling through it
        // can never hit another process that reused the pid after the host
        // exited and was reaped.
        let pidfd = pidfd_open(pid);
        let mut cgroup = None;
        if let Some((leaf, fd)) = prepared {
            drop(fd);
            if in_cgroup(&leaf, pid) {
                debug!(cordon = %spawn.cordon, cgroup = %leaf.display(), "Isolate host started in its cgroup");
                cgroup = Some(leaf);
            } else {
                warn!(cordon = %spawn.cordon, cgroup = %leaf.display(), "The isolate host did not join its own cgroup; it runs in the worker's");
            }
        }
        if let Some(stderr) = child.stderr.take() {
            let cordon = spawn.cordon.clone();
            let replica = spawn.replica;
            tokio::spawn(async move {
                let mut lines = BoundedLines::new(stderr, MAX_STDERR_LINE);
                while let Some(line) = lines.next().await {
                    if line.contains("ERROR") || line.contains("WARN") || line.contains("panicked")
                    {
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
        let alive = exited.clone();
        let kill = Box::new(move || match pidfd.as_ref() {
            Some(fd) => {
                use std::os::fd::AsRawFd;
                // SAFETY: pidfd_send_signal on a pidfd this closure owns.
                unsafe {
                    libc::syscall(
                        libc::SYS_pidfd_send_signal,
                        fd.as_raw_fd(),
                        libc::SIGKILL,
                        std::ptr::null::<libc::siginfo_t>(),
                        0,
                    );
                }
            }
            // A kernel without pidfds (before 5.3): signal by pid only while
            // the host has not been reaped, which narrows the reuse window
            // to the instant between the check and the kill.
            None if pid > 1 && alive.borrow().is_none() => {
                // SAFETY: a plain kill(2) of the child we started.
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }
            None => {}
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

/// `pidfd_open(2)`, or `None` where the kernel has none.
#[cfg(target_os = "linux")]
fn pidfd_open(pid: u32) -> Option<std::os::fd::OwnedFd> {
    use std::os::fd::FromRawFd;
    if pid <= 1 {
        return None;
    }
    // SAFETY: pidfd_open takes a pid and flags and returns a new fd.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
    // SAFETY: a fresh descriptor this function owns.
    (fd >= 0).then(|| unsafe { std::os::fd::OwnedFd::from_raw_fd(fd as i32) })
}

/// Lines from a reader, each cut at `max` bytes (the rest of an over-long
/// line is skipped without being buffered), lossily decoded.
pub struct BoundedLines<R> {
    reader: R,
    max: usize,
    buf: Vec<u8>,
    skipping: bool,
    pending: std::collections::VecDeque<String>,
    done: bool,
}

impl<R: tokio::io::AsyncRead + Unpin> BoundedLines<R> {
    pub fn new(reader: R, max: usize) -> Self {
        Self {
            reader,
            max,
            buf: Vec::new(),
            skipping: false,
            pending: Default::default(),
            done: false,
        }
    }

    pub async fn next(&mut self) -> Option<String> {
        use tokio::io::AsyncReadExt;
        loop {
            if let Some(line) = self.pending.pop_front() {
                return Some(line);
            }
            if self.done {
                return None;
            }
            let mut chunk = [0u8; 4096];
            let n = self.reader.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                self.done = true;
                if !self.buf.is_empty() {
                    let line = String::from_utf8_lossy(&self.buf).into_owned();
                    self.buf.clear();
                    return Some(line);
                }
                continue;
            }
            for &byte in &chunk[..n] {
                if byte == b'\n' {
                    if !self.skipping || !self.buf.is_empty() {
                        self.pending
                            .push_back(String::from_utf8_lossy(&self.buf).into_owned());
                    }
                    self.buf.clear();
                    self.skipping = false;
                } else if self.skipping {
                } else if self.buf.len() >= self.max {
                    let mut line = String::from_utf8_lossy(&self.buf).into_owned();
                    line.push_str(" [cut]");
                    self.pending.push_back(line);
                    self.buf.clear();
                    self.skipping = true;
                } else {
                    self.buf.push(byte);
                }
            }
        }
    }
}

#[cfg(test)]
mod bounded_lines_test {
    use super::BoundedLines;

    #[tokio::test]
    async fn long_lines_are_cut_and_the_rest_skipped_test() {
        let mut input = b"short\n".to_vec();
        input.extend(std::iter::repeat_n(b'x', 100_000));
        input.extend(b"\nnext\ntail");
        let mut lines = BoundedLines::new(&input[..], 16);
        let mut got = Vec::new();
        while let Some(line) = lines.next().await {
            got.push(line);
        }
        assert_eq!(got, vec!["short", "xxxxxxxxxxxxxxxx [cut]", "next", "tail"]);
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
