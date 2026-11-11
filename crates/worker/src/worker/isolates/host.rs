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

/// The largest message the worker sends a host or accepts in a reply from
/// it (`IsolateHost`, where the worker is the client: a run's staged files
/// and its reply are a few MiB; checkpoints are streamed).
pub const MAX_MESSAGE_BYTES: usize = 64 << 20;

/// What a message on the worker socket may carry beyond its body: headers
/// (the host caps a header set at 64 KiB), the URL, the origin, framing.
pub const MESSAGE_OVERHEAD_BYTES: usize = 1 << 20;

/// The largest `LogBatch` a honest host sends: an invocation's stdout and
/// stderr (each at most `limits.output`, at most 1 MiB) split into lines,
/// with protobuf's per-line framing (a run of one-character lines encodes
/// at about six times its text).
pub const MAX_LOG_BATCH_BYTES: usize = 16 << 20;

/// Outbound requests one invocation may have open at once (the host's
/// `MAX_EGRESS_IN_FLIGHT`).
pub const EGRESS_IN_FLIGHT_PER_INVOCATION: u32 = 8;

/// The fewest and the most `IsolateWorker` calls one host may have open
/// at once (egress, child VMs, checkpoints, logs).
pub const MIN_HOST_CALLS: u32 = 16;
pub const MAX_HOST_CALLS: u32 = 64;

/// The least the worker lets one host hold in request bodies at once.
pub const MIN_BODY_BUDGET_BYTES: usize = 64 << 20;

/// What the worker accepts from one host on its worker socket (security
/// review N3). Each limit is derived from the host's own configuration:
///
/// - `max_message_bytes`: the largest legitimate message, an egress body
///   (`isolate.maxBodyBytes`) or a log batch, plus headers and framing:
///   17 MiB with the default 16 MiB `maxBodyBytes`. A child-VM `put` or
///   `exec` stdin larger than this is refused by the transport.
/// - `max_calls`: the host's shards times the per-invocation egress cap
///   (8), within 16..=64.
/// - `body_budget_bytes`: the request bodies (egress bodies, child-VM file
///   content and stdin) the worker holds for the host across calls while
///   they wait on an upstream or a VM, at least 64 MiB or four messages; a
///   call that would pass it is refused at once with RESOURCE_EXHAUSTED.
///
/// Worst case per host replica: `max_calls * max_message_bytes` decoded at
/// the same instant (64 * 17 MiB = 1.06 GiB at the ceiling; 32 * 17 MiB =
/// 544 MiB for a 4-shard host), of which at most `body_budget_bytes` (68
/// MiB by default) is held across an await. It was 256 * 64 MiB = 16 GiB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostCallLimits {
    pub max_message_bytes: usize,
    pub max_calls: u32,
    pub body_budget_bytes: usize,
}

impl HostCallLimits {
    pub fn for_host(config: &HostConfig) -> Self {
        let body = usize::try_from(config.max_body_bytes).unwrap_or(usize::MAX);
        let max_message_bytes = body
            .max(MAX_LOG_BATCH_BYTES)
            .saturating_add(MESSAGE_OVERHEAD_BYTES);
        let shards = match config.shards {
            0 => std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(4),
            n => n,
        };
        Self {
            max_message_bytes,
            max_calls: shards
                .saturating_mul(EGRESS_IN_FLIGHT_PER_INVOCATION)
                .clamp(MIN_HOST_CALLS, MAX_HOST_CALLS),
            body_budget_bytes: MIN_BODY_BUDGET_BYTES.max(max_message_bytes.saturating_mul(4)),
        }
    }
}

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
pub fn serve_over<S: IsolateWorker>(
    stream: tokio::net::UnixStream,
    service: S,
    limits: HostCallLimits,
) {
    // A host is not trusted with the worker's memory: bound the size of
    // what it sends and how many calls it may have open at once on its one
    // connection (see `HostCallLimits`).
    let server = IsolateWorkerServer::new(service)
        .max_decoding_message_size(limits.max_message_bytes)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    tokio::spawn(async move {
        let incoming = tokio_stream::once(Ok::<_, std::io::Error>(stream));
        if let Err(err) = tonic::transport::Server::builder()
            .max_concurrent_streams(Some(limits.max_calls))
            .concurrency_limit_per_connection(limits.max_calls as usize)
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

/// The cgroup directory name of one host: the cordon's [`path_component`]
/// (a readable prefix and a digest of the exact cordon name, so cordons
/// `job:acme/billing` and `job:acme_billing` never share a cgroup and its
/// limits) and the replica.
///
/// [`path_component`]: super::path_component
pub fn cgroup_leaf_name(cordon: &str, replica: u32) -> String {
    format!("{}-r{replica}", super::path_component(cordon))
}

/// Make a cgroup leaf `dir/name` with its limits and open its
/// `cgroup.procs` for a child to join between fork and exec (by writing
/// `0`). Shared by hosts and the compiler.
pub fn prepare_leaf(
    dir: &Path,
    name: &str,
    memory_max_bytes: Option<u64>,
    pids_max: u32,
) -> R<(PathBuf, std::os::fd::OwnedFd)> {
    std::fs::create_dir_all(dir)?;
    if let Some(parent) = dir.parent() {
        let _ = std::fs::write(parent.join("cgroup.subtree_control"), "+memory +pids");
    }
    let _ = std::fs::write(dir.join("cgroup.subtree_control"), "+memory +pids");
    let leaf = dir.join(name);
    std::fs::create_dir_all(&leaf)?;
    if let Some(max) = memory_max_bytes.filter(|max| *max > 0) {
        std::fs::write(leaf.join("memory.max"), max.to_string())?;
    }
    std::fs::write(leaf.join("pids.max"), pids_max.to_string())?;
    use std::os::unix::fs::OpenOptionsExt;
    // `create`: a no-op on cgroupfs, where the file exists (and cannot be
    // created where it does not); lets a plain directory stand in in tests.
    let procs = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_CLOEXEC)
        .open(leaf.join("cgroup.procs"))?;
    Ok((leaf, procs.into()))
}

/// Create the host's cgroup with its limits, before the host starts, and
/// open its `cgroup.procs` for the child to join itself between fork and
/// exec (the host forks again at startup into a PID namespace, so moving
/// the spawned pid afterwards would leave the real host behind). Best
/// effort: a node that cannot (no delegation, cgroup v1) runs the host in
/// the worker's.
fn prepare_cgroup(dir: &Path, spawn: &HostSpawn) -> R<(PathBuf, std::os::fd::OwnedFd)> {
    // pids.max: shards plus the epoch ticker and the I/O runtime; generous.
    prepare_leaf(
        dir,
        &cgroup_leaf_name(&spawn.cordon, spawn.replica),
        spawn.memory_max_bytes,
        1024,
    )
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
        // The death signal follows the THREAD that forked, not the
        // process: fork on the spawner thread, which lives as long as the
        // worker, never on a runtime or blocking-pool thread that may retire
        // while the host runs (security review N8).
        let handle = tokio::runtime::Handle::current();
        let mut child = on_spawner(move || {
            // tokio's child bookkeeping (SIGCHLD, the reaper) belongs to the
            // worker's runtime.
            let _runtime = handle.enter();
            tokio::process::Command::from(command)
                .kill_on_drop(false)
                .spawn()
        })
        .await
        .context("the isolate host spawner thread is gone")?
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
        let limits = worker.limits();
        serve_over(tokio_stream(worker_ours)?, worker, limits);
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

/// Work for the spawner thread.
type SpawnJob = Box<dyn FnOnce() + Send>;

/// Run `job` on the worker's one spawner thread and hand back its result.
///
/// `PR_SET_PDEATHSIG` fires when the thread that forked the child exits,
/// not when the process does. Tokio's threads are not that thread: an idle
/// blocking-pool thread retires after a few seconds, and `block_in_place`
/// hands a runtime worker's core to a new thread and lets the old one
/// retire. A host forked on one of them would be killed while the worker
/// runs. The spawner is a plain OS thread started once and never stopped,
/// so the death signal fires only when the worker process itself is gone.
pub fn on_spawner<T: Send + 'static>(
    job: impl FnOnce() -> T + Send + 'static,
) -> tokio::sync::oneshot::Receiver<T> {
    static SPAWNER: std::sync::OnceLock<Mutex<std::sync::mpsc::Sender<SpawnJob>>> =
        std::sync::OnceLock::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sender = SPAWNER.get_or_init(|| {
        let (send, receive) = std::sync::mpsc::channel::<SpawnJob>();
        std::thread::Builder::new()
            .name("isolate-spawner".into())
            .spawn(move || {
                // The sender is a static: this loop never ends.
                while let Ok(job) = receive.recv() {
                    job();
                }
            })
            .expect("start the isolate spawner thread");
        Mutex::new(send)
    });
    let job: SpawnJob = Box::new(move || {
        let _ = tx.send(job());
    });
    if let Ok(sender) = sender.lock() {
        let _ = sender.send(job);
    }
    rx
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

#[cfg(all(test, target_os = "linux"))]
mod host_process_test {
    use super::*;

    #[test]
    fn colliding_cordons_get_their_own_cgroup_test() {
        // The review's pair: different jobs (and namespaces) whose cordon
        // names sanitize alike.
        let a = cgroup_leaf_name("job:acme/billing", 0);
        let b = cgroup_leaf_name("job:acme_billing", 0);
        assert_ne!(a, b);
        assert_ne!(
            cgroup_leaf_name("job:t/a.b", 0),
            cgroup_leaf_name("job:t/a_b", 0)
        );
        assert_ne!(a, cgroup_leaf_name("job:acme/billing", 1));
        // Each gets its own leaf with its own memory.max.
        let dir = tempfile::tempdir().unwrap();
        let spawn = |cordon: &str, max: u64| HostSpawn {
            cordon: cordon.into(),
            replica: 0,
            config: HostConfig::default(),
            memory_max_bytes: Some(max),
        };
        let (leaf_a, _) = prepare_cgroup(dir.path(), &spawn("job:acme/billing", 1 << 30)).unwrap();
        let (leaf_b, _) = prepare_cgroup(dir.path(), &spawn("job:acme_billing", 1 << 20)).unwrap();
        assert_ne!(leaf_a, leaf_b);
        let max = |leaf: &Path| std::fs::read_to_string(leaf.join("memory.max")).unwrap();
        assert_eq!(max(&leaf_a), (1u64 << 30).to_string());
        assert_eq!(max(&leaf_b), (1u64 << 20).to_string());
        assert!(leaf_a
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("job_acme_billing-"));
    }

    #[test]
    fn worker_socket_limits_follow_the_host_config_test() {
        let limits = HostCallLimits::for_host(&HostConfig {
            shards: 4,
            max_body_bytes: 16 << 20,
            ..Default::default()
        });
        assert_eq!(
            limits.max_message_bytes,
            (16 << 20) + MESSAGE_OVERHEAD_BYTES
        );
        assert_eq!(limits.max_calls, 32);
        assert_eq!(
            limits.body_budget_bytes,
            4 * ((16 << 20) + MESSAGE_OVERHEAD_BYTES)
        );
        // Bounded both ways, and never below what a log batch needs.
        let small = HostCallLimits::for_host(&HostConfig {
            shards: 1,
            max_body_bytes: 1024,
            ..Default::default()
        });
        assert_eq!(small.max_calls, MIN_HOST_CALLS);
        assert_eq!(
            small.max_message_bytes,
            MAX_LOG_BATCH_BYTES + MESSAGE_OVERHEAD_BYTES
        );
        assert_eq!(small.body_budget_bytes, 4 * small.max_message_bytes);
        let big = HostCallLimits::for_host(&HostConfig {
            shards: 1000,
            ..Default::default()
        });
        assert_eq!(big.max_calls, MAX_HOST_CALLS);
    }

    /// `sleep 30` with the death signal the launcher sets, forked by `fork`.
    fn sleeper(
        fork: impl FnOnce(std::process::Command) -> std::process::Child,
    ) -> std::process::Child {
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new("sleep");
        command.arg("30");
        // SAFETY: prctl is async-signal-safe.
        unsafe {
            command.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        fork(command)
    }

    fn alive_after_a_moment(child: &mut std::process::Child) -> bool {
        std::thread::sleep(std::time::Duration::from_millis(300));
        let alive = child.try_wait().unwrap().is_none();
        let _ = child.kill();
        let _ = child.wait();
        alive
    }

    #[test]
    fn a_child_forked_on_the_spawner_outlives_the_thread_that_asked_test() {
        // Without the spawner: forked on a thread that then exits, the
        // child is killed (the hazard is real here).
        let mut direct = std::thread::spawn(|| sleeper(|mut c| c.spawn().unwrap()))
            .join()
            .unwrap();
        assert!(
            !alive_after_a_moment(&mut direct),
            "the death signal follows the thread"
        );
        // With it: the asking thread exits, the child lives on.
        let mut spawned = std::thread::spawn(|| {
            sleeper(|c| {
                let mut c = c;
                on_spawner(move || c.spawn())
                    .blocking_recv()
                    .unwrap()
                    .unwrap()
            })
        })
        .join()
        .unwrap();
        assert!(
            alive_after_a_moment(&mut spawned),
            "forked on the spawner thread"
        );
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
