//! The per-allocation API socket (apps plan §6.4, calls entry 9;
//! docs/workload-identity.md, "The API socket").
//!
//! Every allocation whose task declares the reserved `legion` identity gets
//! a unix socket at `$LEGION_SECRETS_DIR/legion.sock` (the host's
//! `<task dir>/secrets/legion.sock`). The worker listens on it and proxies
//! gRPC to the managers:
//!
//! - It speaks plain HTTP/2 to the task and forwards each request, as is,
//!   over the worker's own manager channel
//!   ([`crate::worker::managers::ManagerEndpoints`]): TLS pinned to the
//!   cluster CA, and failover to the next manager when the active one stops
//!   answering. A transport failure counts toward that failover like a
//!   failed heartbeat does.
//! - A request with no `authorization` header gets the allocation's current
//!   `legion` identity token, which the refresh loop keeps fresh. A request
//!   that carries its own credential (a token from `legion login`) keeps it:
//!   it grants nothing the caller does not already hold.
//! - The task never needs a route to a manager. The host guard drops an
//!   allocation's connections to Legion's own ports on a manager's node, and
//!   a gated allocation has no route out at all; the socket is the
//!   allocation's own resource, so neither applies.
//!
//! **Who may connect.** Tasks can share a uid, so the file's mode is not
//! enough. When the worker's resource governor runs (Linux, cgroup v2) every
//! process of an allocation lives in its leaf, `<cgroupParent>/{share,
//! reserve}/legion-<alloc>`, and the worker reads the connecting process's
//! credentials (`SO_PEERCRED`), then its cgroup (`/proc/<pid>/cgroup`), and
//! refuses a process outside this allocation's leaf. Without cgroups
//! (macOS, enforcement off) only the directory modes guard the socket: the
//! allocation directory is reachable by the allocation's own user alone,
//! the same boundary that protects its token file. That is weaker when
//! allocations share a user, and root reaches everything.

use crate::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::RwLock as StdRwLock;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

/// The socket's file name inside `$LEGION_SECRETS_DIR`.
pub const API_SOCKET_NAME: &str = "legion.sock";

/// Everything every allocation's socket on a worker shares.
#[derive(Clone)]
pub struct ApiSocketContext {
    /// The worker's manager channel; follows failovers.
    pub channel: Channel,
    /// Told about transport failures, so a dead manager is failed over even
    /// when only a task's calls notice it. `None` in tests.
    pub managers: Option<Arc<crate::worker::managers::ManagerEndpoints>>,
    /// How a connecting process is checked.
    pub peer_check: PeerCheck,
}

/// How the socket decides whether a connecting process belongs to its
/// allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerCheck {
    /// The peer's cgroup must be in the allocation's leaf under this parent
    /// (the worker's `resourceEnforcement.cgroupParent`).
    Cgroup { parent: String },
    /// No cgroup attribution on this node: the socket's directory modes are
    /// the only guard.
    FileMode,
}

impl PeerCheck {
    /// The check this node can make: cgroup attribution on a cgroup v2 host
    /// where every task is guaranteed a leaf (the resource governor
    /// enforces limits, or the kernel host guard runs), else file modes.
    pub fn for_node(leaves_guaranteed: bool, cgroup_parent: &str) -> PeerCheck {
        let unified = Path::new(crate::task_plugins::CGROUP_ROOT)
            .join("cgroup.controllers")
            .exists();
        if leaves_guaranteed && unified && cfg!(target_os = "linux") {
            PeerCheck::Cgroup {
                parent: cgroup_parent.to_string(),
            }
        } else {
            PeerCheck::FileMode
        }
    }
}

/// One allocation's socket: the listener task and the token it attaches.
/// Dropping it stops accepting, forgets the token (so a connection still
/// open from the stopped allocation gets nothing more), and removes the
/// file.
pub struct AllocationSocket {
    path: PathBuf,
    token: Arc<StdRwLock<Option<String>>>,
    task: JoinHandle<()>,
}

impl AllocationSocket {
    /// The token attached to calls from now on. Never logged.
    pub fn set_token(&self, token: &str) {
        if let Ok(mut current) = self.token.write() {
            *current = Some(token.to_string());
        }
    }

    /// The shared cell the refresh loop writes.
    pub fn token_cell(&self) -> Arc<StdRwLock<Option<String>>> {
        self.token.clone()
    }

    /// Where the socket is on the host.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for AllocationSocket {
    fn drop(&mut self) {
        self.task.abort();
        if let Ok(mut current) = self.token.write() {
            *current = None;
        }
        if let Err(err) = std::fs::remove_file(&self.path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                debug!(path = %self.path.display(), error = %err, "Failed to remove the API socket");
            }
        }
    }
}

/// Bind `<host_task_dir>/secrets/legion.sock` for `allocation_id` and start
/// serving it. `token` is the allocation's current `legion` token, when the
/// worker already holds one. The directory is opened without following
/// links, since the task can write its own task directory.
pub fn serve(
    context: ApiSocketContext,
    allocation_id: i64,
    host_task_dir: &Path,
    token: Option<String>,
) -> R<AllocationSocket> {
    let secrets = Path::new(crate::structs::workload_identity::SECRETS_SUBDIR);
    let listener = bind_beneath(host_task_dir, secrets, API_SOCKET_NAME)?;
    let path = host_task_dir.join(secrets).join(API_SOCKET_NAME);
    let token = Arc::new(StdRwLock::new(token));
    let proxy = Arc::new(Proxy {
        allocation_id,
        context,
        token: token.clone(),
    });
    let task = tokio::spawn(accept_loop(listener, proxy));
    info!(
        allocation_id,
        path = %path.display(),
        "Serving the allocation's API socket"
    );
    Ok(AllocationSocket { path, token, task })
}

/// Bind a unix socket named `name` in `root/relative`, making the
/// directories when missing. On Linux the directory is opened without
/// following links and the socket bound through `/proc/self/fd`, which also
/// sidesteps the 108-byte `sun_path` limit for deep task directories.
fn bind_beneath(root: &Path, relative: &Path, name: &str) -> R<UnixListener> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let dir = crate::util::open_dir_beneath(root, relative)?;
        let name_c = std::ffi::CString::new(name).context("socket name")?;
        // A stale socket from a previous worker process; never a link
        // followed (unlinkat removes the name itself).
        // SAFETY: valid descriptor and C string.
        unsafe { libc::unlinkat(dir.as_raw_fd(), name_c.as_ptr(), 0) };
        let via = format!("/proc/self/fd/{}/{name}", dir.as_raw_fd());
        let listener = UnixListener::bind(&via)
            .with_context(|| format!("bind {}", root.join(relative).join(name).display()))?;
        // Reachable by whoever can reach the directory: the allocation's own
        // processes (see the module docs for who else is refused).
        set_mode_at(&dir, &name_c, 0o666)?;
        Ok(listener)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let dir = root.join(relative);
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        let listener =
            UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666))?;
        }
        Ok(listener)
    }
}

#[cfg(target_os = "linux")]
fn set_mode_at(dir: &std::os::fd::OwnedFd, name: &std::ffi::CStr, mode: u32) -> R<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: valid descriptor and C string; fchmodat on a socket inode.
    let changed = unsafe { libc::fchmodat(dir.as_raw_fd(), name.as_ptr(), mode, 0) };
    if changed < 0 {
        return Err(std::io::Error::last_os_error()).context("chmod the API socket");
    }
    Ok(())
}

struct Proxy {
    allocation_id: i64,
    context: ApiSocketContext,
    token: Arc<StdRwLock<Option<String>>>,
}

async fn accept_loop(listener: UnixListener, proxy: Arc<Proxy>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(err) => {
                warn!(
                    allocation_id = proxy.allocation_id,
                    error = %err,
                    "API socket accept failed"
                );
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        };
        if let Err(reason) = admit(&stream, proxy.allocation_id, &proxy.context.peer_check) {
            warn!(
                allocation_id = proxy.allocation_id,
                "Refused an API socket connection: {reason}"
            );
            continue;
        }
        let proxy = proxy.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let proxy = proxy.clone();
                async move { Ok::<_, std::convert::Infallible>(proxy.forward(request).await) }
            });
            if let Err(err) =
                hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await
            {
                debug!(error = %err, "API socket connection ended");
            }
        });
    }
}

impl Proxy {
    /// Forward one request to the managers, with the allocation's token
    /// unless the caller sent a credential of its own.
    async fn forward(
        &self,
        request: http::Request<hyper::body::Incoming>,
    ) -> http::Response<tonic::body::BoxBody> {
        use http_body_util::BodyExt;
        use tower::{Service, ServiceExt};

        let (mut parts, body) = request.into_parts();
        if !parts.headers.contains_key(http::header::AUTHORIZATION) {
            let token = self.token.read().ok().and_then(|token| token.clone());
            if let Some(value) =
                token.and_then(|token| http::HeaderValue::from_str(&format!("Bearer {token}")).ok())
            {
                parts.headers.insert(http::header::AUTHORIZATION, value);
            }
        }
        let body = body
            .map_err(|err| tonic::Status::internal(format!("reading the request: {err}")))
            .boxed_unsync();
        let request = http::Request::from_parts(parts, body);
        let mut channel = self.context.channel.clone();
        let result = match channel.ready().await {
            Ok(ready) => ready.call(request).await,
            Err(err) => Err(err),
        };
        match result {
            Ok(response) => {
                if let Some(managers) = &self.context.managers {
                    managers.record_success();
                }
                response
            }
            Err(err) => {
                let status = tonic::Status::unavailable(format!(
                    "the worker could not reach a manager for this allocation's API socket: {err}"
                ));
                if let Some(managers) = &self.context.managers {
                    managers.record_failure(&status);
                }
                status.into_http()
            }
        }
    }
}

/// Whether the process on the other end of `stream` belongs to
/// `allocation_id` (see [`PeerCheck`]).
fn admit(stream: &UnixStream, allocation_id: i64, check: &PeerCheck) -> Result<(), String> {
    let PeerCheck::Cgroup { parent } = check else {
        return Ok(());
    };
    let credentials = stream
        .peer_cred()
        .map_err(|err| format!("could not read the peer's credentials: {err}"))?;
    let pid = credentials
        .pid()
        .ok_or_else(|| "the kernel reported no peer pid".to_string())?;
    let cgroups = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .map_err(|err| format!("could not read the cgroup of pid {pid}: {err}"))?;
    let path = unified_cgroup_path(&cgroups)
        .ok_or_else(|| format!("pid {pid} is in no cgroup v2 hierarchy"))?;
    if in_allocation_leaf(path, parent, allocation_id) {
        Ok(())
    } else {
        Err(format!(
            "pid {pid} (uid {}) runs in cgroup {path}, not in allocation {allocation_id}'s",
            credentials.uid()
        ))
    }
}

/// The cgroup v2 path in a `/proc/<pid>/cgroup` file: its `0::` line.
pub fn unified_cgroup_path(proc_cgroup: &str) -> Option<&str> {
    proc_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(str::trim)
}

/// Whether the cgroup at `path` is an allocation's leaf, or below it:
/// `/<parent>/share/legion-<id>` or `/<parent>/reserve/legion-<id>`.
pub fn in_allocation_leaf(path: &str, parent: &str, allocation_id: i64) -> bool {
    let parent = parent.trim_matches('/');
    ["share", "reserve"].iter().any(|slice| {
        let leaf = if parent.is_empty() {
            format!("/{slice}/legion-{allocation_id}")
        } else {
            format!("/{parent}/{slice}/legion-{allocation_id}")
        };
        path == leaf || path.starts_with(&format!("{leaf}/"))
    })
}

#[cfg(test)]
mod api_socket_test {
    use super::*;

    #[test]
    fn the_unified_line_names_the_cgroup_test() {
        let v1_and_v2 = "12:pids:/user.slice\n1:name=systemd:/x\n0::/legion/share/legion-7\n";
        assert_eq!(
            unified_cgroup_path(v1_and_v2),
            Some("/legion/share/legion-7")
        );
        assert_eq!(unified_cgroup_path("4:memory:/x\n"), None);
    }

    #[test]
    fn only_the_allocations_own_leaf_is_admitted_test() {
        assert!(in_allocation_leaf("/legion/share/legion-7", "legion", 7));
        assert!(in_allocation_leaf(
            "/legion/reserve/legion-7",
            "/legion/",
            7
        ));
        assert!(in_allocation_leaf(
            "/legion/share/legion-7/sub",
            "legion",
            7
        ));
        // Another allocation, a longer id with the same prefix, the parent
        // itself, and a different parent are all refused.
        assert!(!in_allocation_leaf("/legion/share/legion-8", "legion", 7));
        assert!(!in_allocation_leaf("/legion/share/legion-70", "legion", 7));
        assert!(!in_allocation_leaf("/legion/share", "legion", 7));
        assert!(!in_allocation_leaf("/legion", "legion", 7));
        assert!(!in_allocation_leaf(
            "/legion-system/share/legion-7",
            "legion",
            7
        ));
        assert!(in_allocation_leaf(
            "/legion-system/share/legion-7",
            "legion-system",
            7
        ));
    }

    #[test]
    fn file_mode_admits_any_peer_test() -> R<()> {
        let (left, _right) = std::os::unix::net::UnixStream::pair()?;
        left.set_nonblocking(true)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let stream = UnixStream::from_std(left)?;
            assert!(admit(&stream, 7, &PeerCheck::FileMode).is_ok());
            Ok::<_, anyhow::Error>(())
        })
    }

    /// The real check against the test's own process, which is in no
    /// allocation's leaf: refused. Needs a cgroup v2 host.
    #[test]
    #[cfg(target_os = "linux")]
    fn a_process_outside_the_leaf_is_refused_test() -> R<()> {
        let own = std::fs::read_to_string("/proc/self/cgroup")?;
        if unified_cgroup_path(&own).is_none() {
            return Ok(());
        }
        let (left, _right) = std::os::unix::net::UnixStream::pair()?;
        left.set_nonblocking(true)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let stream = UnixStream::from_std(left)?;
            let refused = admit(
                &stream,
                7,
                &PeerCheck::Cgroup {
                    parent: "legion".into(),
                },
            )
            .expect_err("the test process is in no allocation's leaf");
            assert!(refused.contains("not in allocation 7's"), "{refused}");
            Ok::<_, anyhow::Error>(())
        })
    }
}
