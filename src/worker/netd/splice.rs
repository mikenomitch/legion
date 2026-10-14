//! Moving an allowed L4 flow's bytes between its two connections.
//!
//! Every L4 path in the datapath ends the same way: two connections, bytes
//! copied both ways until both sides close. [`bidirectional`] is that copy.
//!
//! - **On Linux, when both ends are plain sockets**, the bytes move with
//!   `splice(2)` through a pipe and never enter this process's memory. This
//!   is the network-handlers plan's §7.12 "the L4 path is `splice`, not a
//!   userspace copy". It covers TCP and Unix sockets (a sandbox's vsock
//!   relay), and only when nothing has to see the bytes: a TLS stream, or a
//!   stream with bytes already read off it, takes the copy path.
//! - **Everywhere else**, tokio's `copy_bidirectional`, as before.
//!
//! Either way the copy also measures the time from the first request byte
//! to the first reply byte, so a latency-aware balancer learns how fast an
//! endpoint answers without parsing anything (`balance::Outcome`).
//!
//! The behavior is identical on every platform: same bytes, same half-close
//! semantics, same counts. Only the cost differs.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};

/// A connection the datapath can copy. The default says "not a raw socket",
/// which always takes the portable copy.
pub trait SpliceStream: AsyncRead + AsyncWrite + Unpin {
    /// The socket under this stream, when every byte read from or written to
    /// the stream goes straight to it: no TLS, no buffered prefix, no framing.
    fn socket(&self) -> Option<Socket<'_>> {
        None
    }

    /// Bytes moved through this stream by the kernel, which a wrapper that
    /// counts activity would otherwise never see.
    fn spliced(&self, _bytes: usize) {}
}

/// A plain socket [`bidirectional`] can splice.
#[derive(Clone, Copy)]
pub enum Socket<'a> {
    Tcp(&'a TcpStream),
    Unix(&'a UnixStream),
}

impl SpliceStream for TcpStream {
    fn socket(&self) -> Option<Socket<'_>> {
        Some(Socket::Tcp(self))
    }
}

impl SpliceStream for UnixStream {
    fn socket(&self) -> Option<Socket<'_>> {
        Some(Socket::Unix(self))
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> SpliceStream for tokio_rustls::client::TlsStream<IO> {}
impl<IO: AsyncRead + AsyncWrite + Unpin> SpliceStream for tokio_rustls::server::TlsStream<IO> {}

/// What one finished flow moved, and how it ended.
#[derive(Debug, Default)]
pub struct Transfer {
    /// Bytes from the first stream (the client) to the second.
    pub sent: u64,
    /// Bytes from the second stream (the upstream) back to the first.
    pub received: u64,
    /// The first request byte to the first reply byte. `None` when the
    /// upstream spoke first (a server greeting), or never answered.
    pub first_reply: Option<Duration>,
    /// Why the flow ended early, if it did.
    pub error: Option<io::Error>,
}

/// Copy between `client` and `upstream` until both directions close, the
/// way `tokio::io::copy_bidirectional` does: an EOF on one side shuts down
/// the write half of the other, and the first error ends the flow.
pub async fn bidirectional<A, B>(client: &mut A, upstream: &mut B) -> Transfer
where
    A: SpliceStream + ?Sized,
    B: SpliceStream + ?Sized,
{
    let clock = ReplyClock::default();
    #[cfg(target_os = "linux")]
    if let (Some(a), Some(b)) = (client.socket(), upstream.socket()) {
        if linux::available() {
            if let Some(pipes) = linux::Pipes::new() {
                let counts = linux::Counts::default();
                let result = linux::bidirectional(a, b, pipes, &clock, &counts, |n| {
                    client.spliced(n);
                    upstream.spliced(n);
                })
                .await;
                return Transfer {
                    sent: counts.sent.load(Ordering::Relaxed),
                    received: counts.received.load(Ordering::Relaxed),
                    first_reply: clock.first_reply(),
                    error: result.err(),
                };
            }
        }
    }

    let mut a = Timed {
        inner: client,
        clock: &clock,
        side: Side::Client,
        bytes: 0,
    };
    let mut b = Timed {
        inner: upstream,
        clock: &clock,
        side: Side::Upstream,
        bytes: 0,
    };
    let error = tokio::io::copy_bidirectional(&mut a, &mut b).await.err();
    Transfer {
        sent: a.bytes,
        received: b.bytes,
        first_reply: clock.first_reply(),
        error,
    }
}

/// Times the first reply: started by the first byte read from the client,
/// stopped by the first byte read from the upstream after it. Shared by the
/// two directions of one flow, which may run on different threads.
#[derive(Default)]
struct ReplyClock {
    origin: OnceLock<Instant>,
    /// 0 nothing yet, 1 the client spoke first, 2 settled.
    state: AtomicU8,
    /// Nanoseconds from the first request byte to the first reply byte.
    reply_nanos: AtomicU64,
}

const CLOCK_IDLE: u8 = 0;
const CLOCK_RUNNING: u8 = 1;
const CLOCK_SETTLED: u8 = 2;

impl ReplyClock {
    fn client_bytes(&self) {
        if self.state.load(Ordering::Relaxed) == CLOCK_IDLE {
            let _ = self.origin.set(Instant::now());
            let _ = self.state.compare_exchange(
                CLOCK_IDLE,
                CLOCK_RUNNING,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }
    }

    fn upstream_bytes(&self) {
        match self.state.swap(CLOCK_SETTLED, Ordering::AcqRel) {
            CLOCK_RUNNING => {
                if let Some(origin) = self.origin.get() {
                    let nanos = origin.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    self.reply_nanos.store(nanos.max(1), Ordering::Relaxed);
                }
            }
            // The upstream spoke first: no request to time.
            _ => {}
        }
    }

    fn first_reply(&self) -> Option<Duration> {
        match self.reply_nanos.load(Ordering::Relaxed) {
            0 => None,
            nanos => Some(Duration::from_nanos(nanos)),
        }
    }
}

#[derive(Clone, Copy)]
enum Side {
    Client,
    Upstream,
}

/// A stream that tells the [`ReplyClock`] when its first bytes arrive and
/// counts what was read from it.
struct Timed<'a, S: ?Sized> {
    inner: &'a mut S,
    clock: &'a ReplyClock,
    side: Side,
    bytes: u64,
}

impl<S: AsyncRead + Unpin + ?Sized> AsyncRead for Timed<'_, S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut *self.inner).poll_read(cx, buf);
        let read = buf.filled().len() - before;
        if read > 0 {
            if self.bytes == 0 {
                match self.side {
                    Side::Client => self.clock.client_bytes(),
                    Side::Upstream => self.clock.upstream_bytes(),
                }
            }
            self.bytes += read as u64;
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin + ?Sized> AsyncWrite for Timed<'_, S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(cx)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{ReplyClock, Socket};
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;
    use tokio::io::Interest;

    /// The most one `splice` call moves: the pipe's default capacity.
    const CHUNK: usize = 64 * 1024;

    /// Whether this kernel splices sockets at all, probed once. A sandboxed
    /// kernel (gVisor, some seccomp profiles) may refuse it; the copy path
    /// then serves every flow.
    pub fn available() -> bool {
        static AVAILABLE: OnceLock<bool> = OnceLock::new();
        *AVAILABLE.get_or_init(|| {
            let works = probe();
            if !works {
                tracing::info!("splice(2) is unavailable; the datapath copies in user space");
            }
            works
        })
    }

    fn probe() -> bool {
        let Ok((left, right)) = std::os::unix::net::UnixStream::pair() else {
            return false;
        };
        let Some(pipe) = Pipe::new() else {
            return false;
        };
        use std::io::Write;
        if (&left).write_all(b"x").is_err() {
            return false;
        }
        // SAFETY: valid fds owned above; null offsets are allowed for
        // sockets and pipes.
        let moved = unsafe {
            libc::splice(
                right.as_raw_fd(),
                std::ptr::null_mut(),
                pipe.write.as_raw_fd(),
                std::ptr::null_mut(),
                1,
                libc::SPLICE_F_NONBLOCK,
            )
        };
        moved == 1
    }

    struct Pipe {
        read: OwnedFd,
        write: OwnedFd,
    }

    impl Pipe {
        fn new() -> Option<Pipe> {
            let mut fds = [0 as RawFd; 2];
            // SAFETY: `fds` is a valid two-element array for pipe2 to fill.
            if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) } != 0 {
                return None;
            }
            // SAFETY: pipe2 succeeded, so both fds are open and ours alone.
            Some(unsafe {
                Pipe {
                    read: OwnedFd::from_raw_fd(fds[0]),
                    write: OwnedFd::from_raw_fd(fds[1]),
                }
            })
        }
    }

    /// One pipe per direction. Creating them can fail (out of descriptors);
    /// the caller then copies instead.
    pub struct Pipes {
        to_upstream: Pipe,
        to_client: Pipe,
    }

    impl Pipes {
        pub fn new() -> Option<Pipes> {
            Some(Pipes {
                to_upstream: Pipe::new()?,
                to_client: Pipe::new()?,
            })
        }
    }

    #[derive(Default)]
    pub struct Counts {
        pub sent: AtomicU64,
        pub received: AtomicU64,
    }

    impl Socket<'_> {
        fn fd(&self) -> RawFd {
            match self {
                Socket::Tcp(s) => s.as_raw_fd(),
                Socket::Unix(s) => s.as_raw_fd(),
            }
        }

        async fn ready(&self, interest: Interest) -> io::Result<()> {
            match self {
                Socket::Tcp(s) => s.ready(interest).await.map(drop),
                Socket::Unix(s) => s.ready(interest).await.map(drop),
            }
        }

        fn try_io<R>(
            &self,
            interest: Interest,
            f: impl FnOnce() -> io::Result<R>,
        ) -> io::Result<R> {
            match self {
                Socket::Tcp(s) => s.try_io(interest, f),
                Socket::Unix(s) => s.try_io(interest, f),
            }
        }

        fn shutdown_write(&self) {
            // SAFETY: a valid socket fd; shutting down its write half is
            // what an EOF from the other side means. ENOTCONN (the peer is
            // already gone) is fine to ignore.
            unsafe {
                libc::shutdown(self.fd(), libc::SHUT_WR);
            }
        }
    }

    fn splice(from: RawFd, to: RawFd, len: usize) -> io::Result<usize> {
        // SAFETY: both fds are open for the duration of the call (borrowed
        // from live sockets and pipes); null offsets are required for them.
        let moved = unsafe {
            libc::splice(
                from,
                std::ptr::null_mut(),
                to,
                std::ptr::null_mut(),
                len,
                libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK,
            )
        };
        if moved < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(moved as usize)
        }
    }

    /// One direction: socket → pipe → socket until EOF, then half-close.
    /// The pipe is drained after every fill, so a would-block can only come
    /// from the socket, never the pipe, and the readiness bookkeeping stays
    /// honest.
    async fn one_way(
        from: Socket<'_>,
        to: Socket<'_>,
        pipe: &Pipe,
        total: &AtomicU64,
        first: &dyn Fn(),
        moved: &dyn Fn(usize),
    ) -> io::Result<()> {
        loop {
            let filled = loop {
                from.ready(Interest::READABLE).await?;
                match from.try_io(Interest::READABLE, || {
                    splice(from.fd(), pipe.write.as_raw_fd(), CHUNK)
                }) {
                    Ok(n) => break n,
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(err) => return Err(err),
                }
            };
            if filled == 0 {
                to.shutdown_write();
                return Ok(());
            }
            if total.load(Ordering::Relaxed) == 0 {
                first();
            }
            let mut left = filled;
            while left > 0 {
                to.ready(Interest::WRITABLE).await?;
                match to.try_io(Interest::WRITABLE, || {
                    splice(pipe.read.as_raw_fd(), to.fd(), left)
                }) {
                    Ok(n) => left -= n,
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(err) => return Err(err),
                }
            }
            total.fetch_add(filled as u64, Ordering::Relaxed);
            moved(filled);
        }
    }

    pub async fn bidirectional(
        client: Socket<'_>,
        upstream: Socket<'_>,
        pipes: Pipes,
        clock: &ReplyClock,
        counts: &Counts,
        moved: impl Fn(usize),
    ) -> io::Result<()> {
        let request_started = || clock.client_bytes();
        let reply_started = || clock.upstream_bytes();
        tokio::try_join!(
            one_way(
                client,
                upstream,
                &pipes.to_upstream,
                &counts.sent,
                &request_started,
                &moved,
            ),
            one_way(
                upstream,
                client,
                &pipes.to_client,
                &counts.received,
                &reply_started,
                &moved,
            ),
        )
        .map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// A loopback pair: (the end we hold, the end handed to `bidirectional`).
    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (near, far) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (near.unwrap(), far.unwrap().0)
    }

    /// Bytes go both ways intact, EOF on one side half-closes the other,
    /// and the counts and the reply clock come back right. On Linux this is
    /// the splice path; elsewhere the copy path.
    #[tokio::test]
    async fn moves_bytes_both_ways_and_half_closes() {
        let (mut client, mut proxy_client) = tcp_pair().await;
        let (mut proxy_upstream, mut upstream) = tcp_pair().await;
        let flow =
            tokio::spawn(
                async move { bidirectional(&mut proxy_client, &mut proxy_upstream).await },
            );

        let request = vec![7u8; 300_000];
        client.write_all(&request).await.unwrap();
        let mut got = vec![0u8; request.len()];
        upstream.read_exact(&mut got).await.unwrap();
        assert_eq!(got, request);

        upstream.write_all(b"reply").await.unwrap();
        let mut reply = [0u8; 5];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");

        // Client EOF reaches the upstream as EOF; the upstream can still
        // answer on its half.
        client.shutdown().await.unwrap();
        let mut rest = Vec::new();
        upstream.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
        upstream.write_all(b"bye").await.unwrap();
        upstream.shutdown().await.unwrap();
        let mut tail = Vec::new();
        client.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, b"bye");

        let transfer = flow.await.unwrap();
        assert!(transfer.error.is_none(), "{:?}", transfer.error);
        assert_eq!(transfer.sent, request.len() as u64);
        assert_eq!(transfer.received, 8);
        assert!(transfer.first_reply.is_some());
    }

    /// A server that speaks first is not a request/reply sample.
    #[tokio::test]
    async fn a_server_greeting_is_not_timed() {
        let (mut client, mut proxy_client) = tcp_pair().await;
        let (mut proxy_upstream, mut upstream) = tcp_pair().await;
        let flow =
            tokio::spawn(
                async move { bidirectional(&mut proxy_client, &mut proxy_upstream).await },
            );
        upstream.write_all(b"220 hello\r\n").await.unwrap();
        let mut greeting = [0u8; 11];
        client.read_exact(&mut greeting).await.unwrap();
        client.write_all(b"QUIT\r\n").await.unwrap();
        drop(client);
        let mut rest = Vec::new();
        upstream.read_to_end(&mut rest).await.unwrap();
        drop(upstream);
        let transfer = flow.await.unwrap();
        assert_eq!(transfer.first_reply, None);
        assert_eq!(transfer.received, 11);
    }

    /// A stream that is not a raw socket (here, an in-memory pipe) always
    /// takes the copy path, with the same results.
    #[tokio::test]
    async fn non_socket_streams_take_the_copy_path() {
        struct Duplex(tokio::io::DuplexStream);
        impl AsyncRead for Duplex {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                Pin::new(&mut self.0).poll_read(cx, buf)
            }
        }
        impl AsyncWrite for Duplex {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                data: &[u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.0).poll_write(cx, data)
            }
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut self.0).poll_flush(cx)
            }
            fn poll_shutdown(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                Pin::new(&mut self.0).poll_shutdown(cx)
            }
        }
        impl SpliceStream for Duplex {}

        let (mut client, proxy_client) = tokio::io::duplex(1024);
        let (mut proxy_upstream, mut upstream) = tcp_pair().await;
        let flow = tokio::spawn(async move {
            let mut proxy_client = Duplex(proxy_client);
            bidirectional(&mut proxy_client, &mut proxy_upstream).await
        });
        client.write_all(b"ping").await.unwrap();
        let mut got = [0u8; 4];
        upstream.read_exact(&mut got).await.unwrap();
        upstream.write_all(b"pong").await.unwrap();
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"pong");
        drop(client);
        drop(upstream);
        let transfer = flow.await.unwrap();
        assert_eq!((transfer.sent, transfer.received), (4, 4));
        assert!(transfer.first_reply.is_some());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn this_kernel_splices_sockets() {
        assert!(linux::available());
    }
}
