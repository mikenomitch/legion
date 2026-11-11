//! Where the datapath listens and how it dials.
//!
//! Every datapath listener — upstreams and their VIPs, resolvers, egress
//! proxies, ingress ports — binds a [`ListenAddr`] (a TCP address, inside a
//! gated namespace when there is one, or the Unix socket a VMM connects a
//! guest's vsock port to) and runs one accept loop, [`spawn_accept_loop`].
//! Every outbound TCP connection the datapath opens goes through
//! [`dial_tcp`]. A socket option, a new kind of socket, or a change to how
//! accept errors are handled has one place to go.

use crate::prelude::*;
use crate::worker::netd::splice::{Socket, SpliceStream};

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::task::JoinHandle;
use tracing::Instrument;

/// Pause after a failed `accept()` so a persistent failure (descriptor
/// exhaustion, most likely) cannot spin an accept loop hot.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Where one datapath listener binds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenAddr {
    /// A TCP address, inside the gated namespace at `netns` when set.
    Tcp {
        netns: Option<PathBuf>,
        addr: SocketAddr,
    },
    /// A Unix socket: the host end of a sandbox guest's vsock port, which
    /// the VMM connects the guest's streams for that port to.
    Unix(PathBuf),
    /// Every address of the host on this port, both families (an ingress
    /// port with no explicit bind; mesh-control-path plan §6.5): `[::]`
    /// with `IPV6_V6ONLY` off, so one socket takes IPv4 and IPv6, or
    /// `0.0.0.0` when the host has no IPv6.
    AnyAddress(u16),
}

impl ListenAddr {
    /// A TCP address in the host namespace.
    pub fn host(addr: SocketAddr) -> Self {
        ListenAddr::Tcp { netns: None, addr }
    }

    /// Bind it. A Unix socket replaces a stale one a previous listener (or
    /// worker) left behind, and its directory is created when missing.
    pub async fn bind(&self) -> R<Listener> {
        match self {
            ListenAddr::Tcp { netns, addr } => Ok(Listener::Tcp(
                crate::worker::netd::netns::bind_tcp(netns.clone(), *addr).await?,
            )),
            ListenAddr::Unix(path) => Ok(Listener::Unix(bind_unix(path)?)),
            ListenAddr::AnyAddress(port) => Ok(Listener::Tcp(bind_any_address(*port).await?)),
        }
    }

    /// Bind a UDP socket at this TCP address (a resolver's datagram side).
    /// A Unix socket has no datagram side: a guest's agent relays its UDP
    /// queries over the stream.
    pub async fn bind_udp(&self) -> R<Option<tokio::net::UdpSocket>> {
        match self {
            ListenAddr::Tcp { netns, addr } => Ok(Some(
                crate::worker::netd::netns::bind_udp(netns.clone(), *addr).await?,
            )),
            ListenAddr::Unix(_) | ListenAddr::AnyAddress(_) => Ok(None),
        }
    }

    /// The Unix socket path, for a listener on a guest's vsock port.
    pub fn unix_path(&self) -> Option<&Path> {
        match self {
            ListenAddr::Unix(path) => Some(path),
            ListenAddr::Tcp { .. } | ListenAddr::AnyAddress(_) => None,
        }
    }
}

impl std::fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListenAddr::Tcp { netns: None, addr } => write!(f, "{addr}"),
            ListenAddr::Tcp {
                netns: Some(netns),
                addr,
            } => write!(f, "{addr} in {}", netns.display()),
            ListenAddr::Unix(path) => write!(f, "{}", path.display()),
            ListenAddr::AnyAddress(port) => write!(f, "[::]:{port} (IPv4 and IPv6)"),
        }
    }
}

/// Bind every address on `port`, IPv4 and IPv6 on one socket, or IPv4 only
/// when the host has no IPv6 (the socket cannot be made, or `[::]` cannot be
/// bound while `0.0.0.0` can).
async fn bind_any_address(port: u16) -> R<TcpListener> {
    match bind_dual_stack(port) {
        Ok(listener) => Ok(listener),
        Err(v6) => {
            let v4 = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), port);
            let listener = crate::worker::netd::netns::bind_tcp(None, v4)
                .await
                .with_context(|| format!("bind [::]:{port} failed too: {v6}"))?;
            debug!(port, error = %v6, "No IPv6 on this host; listening on IPv4 only");
            Ok(listener)
        }
    }
}

fn bind_dual_stack(port: u16) -> io::Result<TcpListener> {
    let socket = tokio::net::TcpSocket::new_v6()?;
    socket.set_reuseaddr(true)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let off: libc::c_int = 0;
        // SAFETY: a valid socket descriptor, and a pointer to a c_int that
        // lives across the call, with its size.
        let rc = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_IPV6,
                libc::IPV6_V6ONLY,
                &off as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    socket.bind(SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        port,
    ))?;
    socket.listen(1024)
}

fn bind_unix(path: &Path) -> R<UnixListener> {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(path);
    UnixListener::bind(path).with_context(|| format!("bind {}", path.display()))
}

/// A bound datapath listener.
pub enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

impl Listener {
    /// The bound TCP address; `None` for a Unix socket.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        match self {
            Listener::Tcp(listener) => listener.local_addr().ok(),
            Listener::Unix(_) => None,
        }
    }

    /// Accept one connection. A stream from a VMM carries no peer address:
    /// the socket it arrived on is the attribution.
    pub async fn accept(&self) -> io::Result<(Stream, Option<SocketAddr>)> {
        match self {
            Listener::Tcp(listener) => {
                let (stream, peer) = listener.accept().await?;
                // An IPv4 client of a dual-stack socket arrives as
                // `::ffff:a.b.c.d`; policy and logs see `a.b.c.d`.
                let peer = SocketAddr::new(peer.ip().to_canonical(), peer.port());
                Ok((Stream::Tcp(stream), Some(peer)))
            }
            Listener::Unix(listener) => {
                let (stream, _) = listener.accept().await?;
                Ok((Stream::Unix(stream), None))
            }
        }
    }
}

/// Run `listener`'s accept loop on its own task until the task is aborted,
/// handing every connection to `handle` on a task of its own, so a slow
/// connection never blocks the next accept. `what` names the listener in
/// the log line a failed accept writes.
pub fn spawn_accept_loop<F, Fut>(
    listener: Listener,
    what: &'static str,
    handle: F,
) -> JoinHandle<()>
where
    F: Fn(Stream, Option<SocketAddr>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let span = tracing::Span::current();
    tokio::spawn(
        async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        debug!(listener = what, error = ?err, "Datapath accept failed");
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                        continue;
                    }
                };
                // A proxy forwards the application's writes as they come;
                // Nagle on our side would hold a streamed response's small
                // trailing writes until the client's delayed ACK.
                if let Stream::Tcp(tcp) = &stream {
                    let _ = tcp.set_nodelay(true);
                }
                tokio::spawn(handle(stream, peer));
            }
        }
        .instrument(span),
    )
}

/// One accepted or dialed connection: TCP, or a Unix stream to or from a
/// sandbox guest's VMM.
#[derive(Debug)]
pub enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            Stream::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            Stream::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            Stream::Unix(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            Stream::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// Either kind is a plain socket, so an allowed flow over it splices.
impl SpliceStream for Stream {
    fn socket(&self) -> Option<Socket<'_>> {
        match self {
            Stream::Tcp(stream) => Some(Socket::Tcp(stream)),
            Stream::Unix(stream) => Some(Socket::Unix(stream)),
        }
    }
}

/// Open a TCP connection for the datapath: the first of `addrs` that
/// accepts within `timeout`. Every outbound dial goes through here — to an
/// endpoint, an external upstream, a handler, an origin behind the egress
/// proxy, or a TAP guest — so a socket option or a source-address rule for
/// a future capture mode is one change. A timeout is
/// [`io::ErrorKind::TimedOut`], reading `dial timed out`.
pub async fn dial_tcp(
    addrs: impl tokio::net::ToSocketAddrs,
    timeout: Duration,
) -> io::Result<TcpStream> {
    match tokio::time::timeout(timeout, TcpStream::connect(addrs)).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "dial timed out")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn echo_once(listener: Listener) -> JoinHandle<()> {
        spawn_accept_loop(listener, "test", |mut stream, _| async move {
            let mut buf = [0u8; 4];
            if stream.read_exact(&mut buf).await.is_ok() {
                let _ = stream.write_all(&buf).await;
            }
        })
    }

    #[tokio::test]
    async fn one_accept_loop_serves_tcp_and_unix_test() {
        let tcp = ListenAddr::host("127.0.0.1:0".parse().unwrap())
            .bind()
            .await
            .unwrap();
        let addr = tcp.local_addr().unwrap();
        let tcp_task = echo_once(tcp).await;
        let mut client = dial_tcp(addr, Duration::from_secs(2)).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut back = [0u8; 4];
        client.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"ping");
        tcp_task.abort();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("vsock.sock_1080");
        // A stale socket from a previous listener is replaced.
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"stale").unwrap();
        let unix = ListenAddr::Unix(path.clone()).bind().await.unwrap();
        assert!(unix.local_addr().is_none());
        let unix_task = echo_once(unix).await;
        let mut client = UnixStream::connect(&path).await.unwrap();
        client.write_all(b"pong").await.unwrap();
        client.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"pong");
        unix_task.abort();
    }

    /// Mesh-control-path plan §6.5: an ingress port with no explicit bind
    /// takes IPv4 and, where the host has it, IPv6, and an IPv4 client is
    /// seen as IPv4, not as a mapped IPv6 address.
    #[tokio::test]
    async fn any_address_takes_both_families_and_canonicalizes_peers_test() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listener = ListenAddr::AnyAddress(port).bind().await.unwrap();
        let v4 = tokio::spawn(async move { TcpStream::connect(("127.0.0.1", port)).await });
        let (_, peer) = listener.accept().await.unwrap();
        let peer = peer.expect("a TCP peer");
        assert_eq!(peer.ip(), std::net::IpAddr::from([127, 0, 0, 1]), "{peer}");
        let _ = v4.await;
        // IPv6 only where the host has it (CI containers often do not).
        if std::net::TcpListener::bind("[::1]:0").is_ok() {
            let v6 = tokio::spawn(async move { TcpStream::connect(("::1", port)).await });
            let (_, peer) = listener.accept().await.unwrap();
            assert!(peer.expect("a TCP peer").is_ipv6());
            let _ = v6.await;
        }
    }
}
