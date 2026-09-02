//! Gated network namespaces (Linux).
//!
//! `network.mode = "gated"` means the workload's namespace contains loopback
//! and the datapath, nothing else. This module owns the namespace: it is
//! created empty with `unshare(CLONE_NEWNET)` on a throwaway thread, loopback
//! is brought up inside it, and the namespace is bind-mounted at
//! `/var/run/netns/legion-<allocation>` — the same convention `ip netns` uses,
//! so an operator's `ip netns exec` / `nsenter --net=` work on it, a container
//! joins it by path, and an `exec` task `setns`s into it.
//!
//! Legion's own listeners — upstream ports, VIPs, the resolver, the egress
//! proxy — are bound *inside* it from the worker process by entering the
//! namespace on a blocking thread for the duration of the bind and returning.
//! A socket stays in the namespace it was created in, so the accept loops run
//! on the worker's normal runtime while dialing out happens from the host
//! namespace. That asymmetry is the whole enforcement: there is no route out
//! of the namespace except through a socket Legion bound.
//!
//! Syscalls rather than shelling out to `ip`: the worker already needs the
//! privileges (`CAP_SYS_ADMIN` for the mount namespace `exec` hardening
//! unshares), and a hard dependency on iproute2 being installed would be a
//! new one.

use crate::prelude::*;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Where namespaces are bind-mounted (the `ip netns` convention).
pub const NETNS_DIR: &str = "/var/run/netns";
/// Where gate markers live: a present marker means "shut" for that
/// allocation (docs/edge-boot.md `boot` gate on a gated namespace).
pub const GATE_DIR: &str = "/var/run/legion/gate";

/// The namespace name for an allocation.
pub fn netns_name(allocation_id: i64) -> String {
    format!("legion-{allocation_id}")
}

/// The bind-mount path for an allocation's namespace.
pub fn netns_path(allocation_id: i64) -> PathBuf {
    Path::new(NETNS_DIR).join(netns_name(allocation_id))
}

/// Whether the allocation's namespace exists on this host.
pub fn netns_exists(allocation_id: i64) -> bool {
    netns_path(allocation_id).exists()
}

/// Whether this host can create and enter network namespaces at all.
pub fn platform_supports_netns() -> bool {
    cfg!(target_os = "linux")
}

/// Create the allocation's empty namespace with loopback up. Idempotent: an
/// existing namespace is left alone.
#[cfg(target_os = "linux")]
pub fn create(allocation_id: i64) -> R<PathBuf> {
    let path = netns_path(allocation_id);
    if path.exists() {
        return Ok(path);
    }
    std::fs::create_dir_all(NETNS_DIR).context("create /var/run/netns")?;
    // A fresh thread, because `unshare(CLONE_NEWNET)` moves the *calling
    // thread* into the new namespace for good; the bind mount keeps the
    // namespace alive after the thread exits.
    let target = path.clone();
    std::thread::Builder::new()
        .name(format!("netns-create-{allocation_id}"))
        .spawn(move || -> R<()> {
            // SAFETY: plain syscalls with checked return values; this thread
            // does nothing else afterwards.
            if unsafe { libc::unshare(libc::CLONE_NEWNET) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("unshare(CLONE_NEWNET) — gated networking needs CAP_SYS_ADMIN");
            }
            bring_loopback_up().context("bring loopback up in the new namespace")?;
            std::fs::File::create(&target)
                .with_context(|| format!("create mount point {}", target.display()))?;
            let source = std::ffi::CString::new("/proc/thread-self/ns/net")?;
            let dest = std::ffi::CString::new(target.as_os_str().as_encoded_bytes())?;
            if unsafe {
                libc::mount(
                    source.as_ptr(),
                    dest.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND,
                    std::ptr::null(),
                )
            } != 0
            {
                let err = std::io::Error::last_os_error();
                let _ = std::fs::remove_file(&target);
                return Err(err)
                    .with_context(|| format!("bind-mount namespace at {}", target.display()));
            }
            Ok(())
        })
        .context("spawn namespace thread")?
        .join()
        .map_err(|_| anyhow!("namespace thread panicked"))??;
    Ok(path)
}

#[cfg(not(target_os = "linux"))]
pub fn create(_allocation_id: i64) -> R<PathBuf> {
    bail!("gated networking needs Linux network namespaces; this host is not Linux");
}

/// `ifconfig lo up` for the current thread's namespace, via `SIOCSIFFLAGS`.
#[cfg(target_os = "linux")]
fn bring_loopback_up() -> R<()> {
    use std::os::fd::AsRawFd;
    /// `struct ifreq` as the flag ioctls read it: the interface name, then
    /// the `short` flags at the start of the 24-byte union.
    #[repr(C)]
    struct IfreqFlags {
        name: [u8; 16],
        flags: libc::c_short,
        _pad: [u8; 22],
    }
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").or_else(|_| {
        // Loopback is down, so binding to it fails; an unbound datagram
        // socket is enough for the ioctl.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        use std::os::fd::FromRawFd;
        Ok(unsafe { std::net::UdpSocket::from_raw_fd(fd) })
    })?;
    let mut req = IfreqFlags {
        name: *b"lo\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
        flags: 0,
        _pad: [0; 22],
    };
    // SAFETY: `req` is a correctly laid out ifreq for these two ioctls.
    if unsafe { libc::ioctl(socket.as_raw_fd(), libc::SIOCGIFFLAGS, &mut req) } != 0 {
        return Err(std::io::Error::last_os_error()).context("SIOCGIFFLAGS lo");
    }
    req.flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
    if unsafe { libc::ioctl(socket.as_raw_fd(), libc::SIOCSIFFLAGS, &req) } != 0 {
        return Err(std::io::Error::last_os_error()).context("SIOCSIFFLAGS lo up");
    }
    Ok(())
}

/// Delete the allocation's namespace mount. Idempotent. The namespace
/// itself lives on until its last process exits; this removes the path
/// Legion and the runtime use to find it.
#[cfg(target_os = "linux")]
pub fn destroy(allocation_id: i64) -> R<()> {
    let path = netns_path(allocation_id);
    if !path.exists() {
        return Ok(());
    }
    let dest = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())?;
    // SAFETY: a plain unmount of a path this module mounted.
    if unsafe { libc::umount2(dest.as_ptr(), libc::MNT_DETACH) } != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINVAL) {
            return Err(err).with_context(|| format!("unmount {}", path.display()));
        }
    }
    std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))
}

#[cfg(not(target_os = "linux"))]
pub fn destroy(_allocation_id: i64) -> R<()> {
    Ok(())
}

/// Run `f` with the current thread inside the namespace at `path`, then
/// return to the host namespace. Everything the closure creates — sockets
/// above all — stays in the target namespace.
///
/// Must run on a thread the async runtime does not share (the caller uses
/// `spawn_blocking`): `setns` changes the *thread*, and a tokio worker
/// thread that stayed inside would bind every later socket in the wrong
/// place.
#[cfg(target_os = "linux")]
pub fn within<T>(path: &Path, f: impl FnOnce() -> R<T>) -> R<T> {
    use std::os::fd::AsRawFd;
    let host =
        std::fs::File::open("/proc/thread-self/ns/net").context("open host network namespace")?;
    let target = std::fs::File::open(path)
        .with_context(|| format!("open network namespace {}", path.display()))?;
    // SAFETY: `setns` with a valid namespace fd and the matching nstype
    // flag; both fds stay open for the duration. The restore below runs on
    // every path out of the closure, including an error, so the thread never
    // stays in the target namespace.
    let rc = unsafe { libc::setns(target.as_raw_fd(), libc::CLONE_NEWNET) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("enter network namespace {}", path.display()));
    }
    let result = f();
    let rc = unsafe { libc::setns(host.as_raw_fd(), libc::CLONE_NEWNET) };
    if rc != 0 {
        // The thread is stuck in the workload's namespace. Nothing on it can
        // be trusted to bind in the host namespace again; abort loudly
        // rather than silently serving from the wrong place.
        let err = std::io::Error::last_os_error();
        error!(error = ?err, "Failed to return to the host network namespace; aborting");
        std::process::abort();
    }
    result
}

#[cfg(not(target_os = "linux"))]
pub fn within<T>(path: &Path, _f: impl FnOnce() -> R<T>) -> R<T> {
    bail!(
        "network namespaces are Linux-only (asked to enter {})",
        path.display()
    );
}

/// Bind a TCP listener at `addr` inside `netns` (or in the host namespace
/// when `None`), returning a tokio listener. Non-blocking mode is set before
/// the conversion, as `TcpListener::from_std` requires.
pub async fn bind_tcp(netns: Option<PathBuf>, addr: SocketAddr) -> R<tokio::net::TcpListener> {
    let std_listener = tokio::task::spawn_blocking(move || -> R<std::net::TcpListener> {
        let bind = || -> R<std::net::TcpListener> {
            let listener =
                std::net::TcpListener::bind(addr).with_context(|| format!("bind {addr}"))?;
            listener.set_nonblocking(true)?;
            Ok(listener)
        };
        match netns {
            Some(path) => within(&path, bind),
            None => bind(),
        }
    })
    .await
    .context("bind task panicked")??;
    tokio::net::TcpListener::from_std(std_listener).context("register listener with tokio")
}

/// Bind a UDP socket at `addr` inside `netns` (or the host namespace).
pub async fn bind_udp(netns: Option<PathBuf>, addr: SocketAddr) -> R<tokio::net::UdpSocket> {
    let std_socket = tokio::task::spawn_blocking(move || -> R<std::net::UdpSocket> {
        let bind = || -> R<std::net::UdpSocket> {
            let socket = std::net::UdpSocket::bind(addr).with_context(|| format!("bind {addr}"))?;
            socket.set_nonblocking(true)?;
            Ok(socket)
        };
        match netns {
            Some(path) => within(&path, bind),
            None => bind(),
        }
    })
    .await
    .context("bind task panicked")??;
    tokio::net::UdpSocket::from_std(std_socket).context("register socket with tokio")
}

/// Connect to `addr` from inside `netns` (or the host namespace when
/// `None`) — how an ingress listener in the host namespace reaches a gated
/// task's private port. The connect itself is blocking on a helper thread;
/// the resulting stream is registered with tokio.
pub async fn connect_tcp(netns: Option<PathBuf>, addr: SocketAddr) -> R<tokio::net::TcpStream> {
    let std_stream = tokio::task::spawn_blocking(move || -> R<std::net::TcpStream> {
        let connect = || -> R<std::net::TcpStream> {
            let stream =
                std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(2))
                    .with_context(|| format!("connect {addr}"))?;
            stream.set_nonblocking(true)?;
            Ok(stream)
        };
        match netns {
            Some(path) => within(&path, connect),
            None => connect(),
        }
    })
    .await
    .context("connect task panicked")??;
    tokio::net::TcpStream::from_std(std_stream).context("register stream with tokio")
}

/// Gate markers: the edge-boot `boot` gate for a gated allocation is
/// "policy withheld", expressed as a file the runtime creates before start
/// and removes at ratification, so the datapath (which the runtime cannot
/// reach directly) can check it per connection with one `stat`.
pub fn gate_marker(allocation_id: i64) -> PathBuf {
    Path::new(GATE_DIR).join(allocation_id.to_string())
}

pub fn set_gate(allocation_id: i64, shut: bool) -> R<()> {
    let marker = gate_marker(allocation_id);
    if shut {
        std::fs::create_dir_all(GATE_DIR).context("create gate marker directory")?;
        std::fs::write(&marker, b"shut").with_context(|| format!("write {}", marker.display()))?;
    } else if marker.exists() {
        std::fs::remove_file(&marker).with_context(|| format!("remove {}", marker.display()))?;
    }
    Ok(())
}

pub fn gate_is_shut(allocation_id: i64) -> bool {
    gate_marker(allocation_id).exists()
}

/// Whether the current process can manage namespaces here: Linux, root, and
/// `/proc` mounted. Tests skip themselves otherwise.
pub fn can_manage_netns() -> bool {
    platform_supports_netns()
        && unsafe { libc::geteuid() } == 0
        && Path::new("/proc/thread-self/ns/net").exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn listener_bound_inside_a_namespace_is_unreachable_from_the_host() {
        if !can_manage_netns() {
            eprintln!("skipping: needs root on Linux");
            return;
        }
        let allocation_id = 900_000 + i64::from(std::process::id() % 10_000);
        let _ = destroy(allocation_id);
        let path = create(allocation_id).unwrap();
        assert!(path.exists());
        assert_eq!(create(allocation_id).unwrap(), path, "idempotent");
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        // Bound inside the namespace: the host cannot reach it...
        let inside = bind_tcp(Some(path.clone()), addr).await.unwrap();
        let port = inside.local_addr().unwrap().port();
        let from_host = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            tokio::net::TcpStream::connect(("127.0.0.1", port)),
        )
        .await;
        assert!(
            !matches!(from_host, Ok(Ok(_))),
            "a listener inside the namespace must not be reachable from the host"
        );

        // ...but a process inside the namespace can (bash's /dev/tcp via
        // nsenter, which the image has; iproute2 is not required).
        let accept = tokio::spawn(async move {
            let (mut socket, _) = inside.accept().await.unwrap();
            let _ = socket.write_all(b"hi").await;
        });
        // Async, so the spawned accept task above can run while bash waits:
        // a blocking `status()` on the test runtime would starve it.
        let status = tokio::process::Command::new("nsenter")
            .args([
                &format!("--net={}", path.display()),
                "bash",
                "-c",
                &format!(
                    "exec 3<>/dev/tcp/127.0.0.1/{port} && read -t 2 -n 2 x <&3 && [ \"$x\" = hi ]"
                ),
            ])
            .status()
            .await;
        match status {
            Ok(status) => assert!(
                status.success(),
                "a process inside the namespace reaches the listener"
            ),
            Err(err) => eprintln!("nsenter unavailable ({err}); skipping the in-namespace half"),
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), accept).await;

        // The worker thread is back in the host namespace: a plain bind is
        // reachable from the host again.
        let host_listener = bind_tcp(None, addr).await.unwrap();
        let host_port = host_listener.local_addr().unwrap().port();
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", host_port))
            .await
            .is_ok());

        // UDP binds inside too, and 127/8 is all loopback in there.
        let vip: SocketAddr = "127.42.250.1:0".parse().unwrap();
        assert!(bind_udp(Some(path.clone()), vip).await.is_ok());

        set_gate(allocation_id, true).unwrap();
        assert!(gate_is_shut(allocation_id));
        set_gate(allocation_id, false).unwrap();
        assert!(!gate_is_shut(allocation_id));

        destroy(allocation_id).unwrap();
        assert!(!netns_exists(allocation_id));
        destroy(allocation_id).unwrap();
    }
}
