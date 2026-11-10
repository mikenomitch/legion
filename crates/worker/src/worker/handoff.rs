//! Connection handoff: an owner hands an inbound connection it accepted to
//! one of its children's ports, and leaves the byte path
//! (docs/child-sessions.md, "Connection handoff").
//!
//! Forwarding every byte through the owner is wrong for a large upload or a
//! long-lived stream. The owner keeps the decision (it accepted the
//! connection and chose the child); the worker moves the bytes, with the
//! datapath's own copy ([`crate::worker::netd::splice::bidirectional`]:
//! `splice(2)` on Linux once a flow turns bulk, a plain copy elsewhere).
//!
//! A file descriptor cannot ride a gRPC call, so this one workload-channel
//! call has its own socket, `legion-handoff.sock`, beside `legion.sock` in
//! the allocation's secrets directory and admitted by the same rule
//! ([`crate::worker::api_socket`]). One connection is one handoff:
//!
//! 1. The owner connects and sends one line of JSON,
//!    `{"child":"<session id>","port":"<port label>"}\n`, with the accepted
//!    connection's descriptor attached to the first byte (`SCM_RIGHTS`).
//!    It can close its own copy of the descriptor right away.
//! 2. The worker checks the child is the caller's own, on this node, and
//!    declares the port, dials it the way ingress would, and answers one
//!    line: `{"ok":true}` or `{"ok":false,"code":"...","message":"..."}`
//!    (then closes).
//! 3. The worker copies between the two until both directions close, then
//!    writes `{"closed":true,"sent":N,"received":M}` (bytes to the child
//!    and back, plus `"error"` when the flow ended early) and closes. That
//!    line is how the owner learns the connection ended.
//!
//! Only a child on the owner's own node can take a handoff: the bytes would
//! otherwise cross the worker plane, which is a proxy, not a handoff.

use crate::prelude::*;
use crate::worker::netd::listen::Stream;
use crate::worker::task_manager::WorkerTaskManager;
use serde::{Deserialize, Serialize};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;
use tokio::io::{AsyncWriteExt, Interest};
use tokio::net::UnixStream;

/// The handoff socket's file name inside `$LEGION_SECRETS_DIR`.
pub const HANDOFF_SOCKET_NAME: &str = "legion-handoff.sock";

/// The most bytes a request line may be.
const MAX_REQUEST: usize = 4096;

/// How long the owner has to send its request line.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The request line.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandoffRequest {
    /// The child's session id, as `StartChildSession` returned it.
    pub child: String,
    /// The child's port label (`network.ports[].name`).
    pub port: String,
}

/// The first answer line.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandoffAccepted {
    pub ok: bool,
    /// For a refusal: `invalid_argument`, `not_found`,
    /// `failed_precondition`, or `unavailable`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// The last line: the handed-off connection ended.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandoffClosed {
    pub closed: bool,
    /// Bytes from the client to the child.
    pub sent: u64,
    /// Bytes from the child back to the client.
    pub received: u64,
    /// Why the flow ended early, when it did.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// Serve one handoff connection for `allocation_id`, already admitted.
pub async fn serve_connection(
    control: UnixStream,
    allocation_id: i64,
    task_manager: Arc<Mutex<WorkerTaskManager>>,
) {
    // ACL: workload channel. The caller is the socket's allocation, and only
    // its own children on this node are reachable; any other id answers
    // not_found, whatever it names.
    serve_with(control, move |request| {
        task_manager
            .lock()
            .map_err(|_| tonic::Status::unavailable("task manager lock poisoned"))?
            .child_port_target(allocation_id, &request.child, &request.port)
    })
    .await;
}

/// [`serve_connection`] with the child lookup supplied: `resolve` says how
/// to dial the port a request names, or why the caller may not.
pub async fn serve_with(
    mut control: UnixStream,
    resolve: impl FnOnce(
        &HandoffRequest,
    ) -> Result<crate::worker::netd::ingress::TaskTarget, tonic::Status>,
) {
    let refuse = |code: &str, message: String| HandoffAccepted {
        ok: false,
        code: code.to_string(),
        message,
    };
    let received = tokio::time::timeout(REQUEST_TIMEOUT, read_request(&control)).await;
    let (request, descriptor) = match received {
        Ok(Ok(parts)) => parts,
        Ok(Err(err)) => {
            let answer = refuse("invalid_argument", format!("{err:#}"));
            let _ = write_line(&mut control, &answer).await;
            return;
        }
        Err(_) => {
            let answer = refuse(
                "invalid_argument",
                "no request line arrived within 10s".into(),
            );
            let _ = write_line(&mut control, &answer).await;
            return;
        }
    };
    let mut client = match adopt(descriptor) {
        Ok(client) => client,
        Err(err) => {
            let answer = refuse("invalid_argument", format!("{err:#}"));
            let _ = write_line(&mut control, &answer).await;
            return;
        }
    };
    let target = resolve(&request);
    let target = match target {
        Ok(target) => target,
        Err(status) => {
            let code = match status.code() {
                tonic::Code::NotFound => "not_found",
                tonic::Code::FailedPrecondition => "failed_precondition",
                _ => "unavailable",
            };
            let _ = write_line(&mut control, &refuse(code, status.message().to_string())).await;
            return;
        }
    };
    let mut upstream = match target.connect().await {
        Ok(upstream) => upstream,
        Err(err) => {
            let answer = refuse(
                "unavailable",
                format!(
                    "could not reach port {:?} of child {}: {err:#}",
                    request.port, request.child
                ),
            );
            let _ = write_line(&mut control, &answer).await;
            return;
        }
    };
    let accepted = HandoffAccepted {
        ok: true,
        ..Default::default()
    };
    if write_line(&mut control, &accepted).await.is_err() {
        return;
    }
    crate::telemetry::worker_metrics()
        .child_handoffs_total
        .inc();
    debug!(
        child = %request.child,
        port = %request.port,
        "Handed a connection to a child"
    );
    let transfer = crate::worker::netd::splice::bidirectional(&mut client, &mut upstream).await;
    let closed = HandoffClosed {
        closed: true,
        sent: transfer.sent,
        received: transfer.received,
        error: transfer
            .error
            .map(|err| err.to_string())
            .unwrap_or_default(),
    };
    let _ = write_line(&mut control, &closed).await;
}

async fn write_line(control: &mut UnixStream, line: &impl Serialize) -> R<()> {
    let mut bytes = serde_json::to_vec(line)?;
    bytes.push(b'\n');
    control.write_all(&bytes).await?;
    Ok(())
}

/// Read the request line and the descriptor that rides its first byte.
async fn read_request(control: &UnixStream) -> R<(HandoffRequest, OwnedFd)> {
    let mut line = Vec::new();
    let mut descriptor: Option<OwnedFd> = None;
    loop {
        control.readable().await?;
        let mut chunk = [0u8; 1024];
        let read = control.try_io(Interest::READABLE, || {
            recv_with_fd(control.as_raw_fd(), &mut chunk)
        });
        let (count, received) = match read {
            Ok(parts) => parts,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err.into()),
        };
        if descriptor.is_none() {
            descriptor = received;
        }
        if count == 0 {
            bail!("the connection closed before a request line arrived");
        }
        line.extend_from_slice(&chunk[..count]);
        if let Some(end) = line.iter().position(|byte| *byte == b'\n') {
            line.truncate(end);
            break;
        }
        if line.len() > MAX_REQUEST {
            bail!("the request line is longer than {MAX_REQUEST} bytes");
        }
    }
    let request: HandoffRequest = serde_json::from_slice(&line).context(
        "the request line must be JSON: {\"child\":\"<session id>\",\"port\":\"<port label>\"}",
    )?;
    if request.child.trim().is_empty() || request.port.trim().is_empty() {
        bail!("child and port are both required");
    }
    let descriptor = descriptor.context(
        "no file descriptor came with the request: send the accepted connection as SCM_RIGHTS \
         with the first byte of the line",
    )?;
    Ok((request, descriptor))
}

/// `recvmsg` one chunk, keeping the first descriptor sent with it and
/// closing any others.
fn recv_with_fd(fd: RawFd, buf: &mut [u8]) -> std::io::Result<(usize, Option<OwnedFd>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // Aligned for `cmsghdr`, with room for a few descriptors.
    let mut control = [0u64; 8];
    // SAFETY: zeroed is a valid `msghdr`; the pointers set below outlive the call.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = std::mem::size_of_val(&control) as _;
    #[cfg(target_os = "linux")]
    let flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = 0;
    // SAFETY: `msg` points at live buffers of the sizes it states.
    let count = unsafe { libc::recvmsg(fd, &mut msg, flags) };
    if count < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut received: Option<OwnedFd> = None;
    // SAFETY: the kernel filled `msg`'s control buffer; the CMSG macros walk it.
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&msg);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(header) as *const RawFd;
                let bytes = (*header).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                for index in 0..bytes / std::mem::size_of::<RawFd>() {
                    let owned = OwnedFd::from_raw_fd(std::ptr::read_unaligned(data.add(index)));
                    if received.is_none() {
                        received = Some(owned);
                    }
                }
            }
            header = libc::CMSG_NXTHDR(&msg, header);
        }
    }
    Ok((count as usize, received))
}

/// Turn a received descriptor into a stream the datapath can copy. Only a
/// connected stream socket (TCP or Unix) is accepted.
fn adopt(descriptor: OwnedFd) -> R<Stream> {
    let fd = descriptor.as_raw_fd();
    let mut kind: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `kind` and `len` are live and sized as stated.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut kind as *mut libc::c_int).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        bail!("the descriptor sent is not a socket");
    }
    if kind != libc::SOCK_STREAM {
        bail!(
            "the descriptor sent is not a stream socket; only TCP and Unix streams are handed off"
        );
    }
    // SAFETY: zeroed is a valid `sockaddr_storage`.
    let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut addr_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: `addr` and `addr_len` are live and sized as stated.
    let rc = unsafe {
        libc::getsockname(
            fd,
            (&mut addr as *mut libc::sockaddr_storage).cast(),
            &mut addr_len,
        )
    };
    if rc != 0 {
        bail!(
            "could not read the descriptor's address: {}",
            std::io::Error::last_os_error()
        );
    }
    match libc::c_int::from(addr.ss_family) {
        libc::AF_UNIX => {
            let stream = std::os::unix::net::UnixStream::from(descriptor);
            stream.set_nonblocking(true)?;
            Ok(Stream::Unix(UnixStream::from_std(stream)?))
        }
        libc::AF_INET | libc::AF_INET6 => {
            let stream = std::net::TcpStream::from(descriptor);
            stream.set_nonblocking(true)?;
            Ok(Stream::Tcp(tokio::net::TcpStream::from_std(stream)?))
        }
        family => bail!("the descriptor sent is a socket of family {family}, not TCP or Unix"),
    }
}

/// The owner's side: connect to the handoff socket at `socket`, send
/// `request` with `connection` attached, and return the control stream once
/// the worker accepted. The caller may close `connection` afterwards; read
/// [`wait_closed`] on the returned stream to learn when it ended. Blocking.
pub fn hand_off(
    socket: &Path,
    request: &HandoffRequest,
    connection: RawFd,
) -> R<std::os::unix::net::UnixStream> {
    use std::io::{BufRead, BufReader};
    let control = std::os::unix::net::UnixStream::connect(socket)
        .with_context(|| format!("connect {}", socket.display()))?;
    let mut line = serde_json::to_vec(request)?;
    line.push(b'\n');
    send_with_fd(control.as_raw_fd(), &line, connection)?;
    let mut answer = String::new();
    BufReader::new(&control).read_line(&mut answer)?;
    let accepted: HandoffAccepted = serde_json::from_str(answer.trim())
        .with_context(|| format!("unexpected handoff answer {answer:?}"))?;
    if !accepted.ok {
        bail!("handoff refused ({}): {}", accepted.code, accepted.message);
    }
    Ok(control)
}

/// Block until the worker reports the handed-off connection closed.
pub fn wait_closed(control: std::os::unix::net::UnixStream) -> R<HandoffClosed> {
    use std::io::{BufRead, BufReader};
    let mut line = String::new();
    BufReader::new(&control).read_line(&mut line)?;
    if line.trim().is_empty() {
        bail!("the worker closed the handoff without reporting how it ended");
    }
    serde_json::from_str(line.trim()).with_context(|| format!("unexpected handoff report {line:?}"))
}

/// `sendmsg` all of `bytes`, with `descriptor` attached to the first byte.
fn send_with_fd(socket: RawFd, bytes: &[u8], descriptor: RawFd) -> R<()> {
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr() as *mut libc::c_void,
        iov_len: bytes.len(),
    };
    let mut control = [0u64; 4];
    // SAFETY: zeroed is a valid `msghdr`; the pointers set below outlive the call.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    // SAFETY: CMSG_SPACE/LEN are pure arithmetic; the header written is inside `control`.
    let sent = unsafe {
        msg.msg_controllen = libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) as _;
        let header = libc::CMSG_FIRSTHDR(&msg);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<RawFd>() as u32) as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(header) as *mut RawFd, descriptor);
        libc::sendmsg(socket, &msg, 0)
    };
    if sent < 0 {
        return Err(std::io::Error::last_os_error()).context("sendmsg on the handoff socket");
    }
    let mut rest = &bytes[sent as usize..];
    while !rest.is_empty() {
        // SAFETY: `rest` is a live slice of the length stated.
        let more = unsafe { libc::write(socket, rest.as_ptr().cast(), rest.len()) };
        if more < 0 {
            return Err(std::io::Error::last_os_error()).context("write on the handoff socket");
        }
        rest = &rest[more as usize..];
    }
    Ok(())
}

#[cfg(test)]
mod handoff_test {
    use super::*;

    #[tokio::test]
    async fn a_descriptor_rides_the_request_line_test() -> R<()> {
        let (ours, theirs) = std::os::unix::net::UnixStream::pair()?;
        let (payload, _keep) = std::os::unix::net::UnixStream::pair()?;
        let request = HandoffRequest {
            child: "machine-abc.01".into(),
            port: "http".into(),
        };
        let mut line = serde_json::to_vec(&request)?;
        line.push(b'\n');
        send_with_fd(ours.as_raw_fd(), &line, payload.as_raw_fd())?;
        let mut buf = [0u8; 1024];
        let (count, descriptor) = recv_with_fd(theirs.as_raw_fd(), &mut buf)?;
        assert_eq!(&buf[..count], line.as_slice());
        let descriptor = descriptor.expect("the descriptor arrived");
        assert!(matches!(adopt(descriptor)?, Stream::Unix(_)));
        Ok(())
    }

    #[tokio::test]
    async fn only_stream_sockets_are_adopted_test() -> R<()> {
        let datagram = std::os::unix::net::UnixDatagram::unbound()?;
        let err = adopt(OwnedFd::from(datagram)).expect_err("a datagram socket is refused");
        assert!(err.to_string().contains("not a stream socket"), "{err:#}");
        let file = std::fs::File::open("/dev/null")?;
        let err = adopt(OwnedFd::from(file)).expect_err("a file is refused");
        assert!(err.to_string().contains("not a socket"), "{err:#}");
        Ok(())
    }

    /// The whole handoff against a real listener standing in for the
    /// child's port: the owner accepts a client, hands the connection over,
    /// closes its own copy, and the client then talks to the child with the
    /// owner out of the path. The owner learns of the close and the bytes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handed_off_stream_reaches_the_child_and_its_close_is_reported_test() -> R<()> {
        use crate::worker::netd::ingress::{TaskDial, TaskTarget};
        use std::io::{Read, Write};
        use tokio::io::AsyncReadExt;
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;

        // The child: echoes what it reads, upper-cased, until EOF.
        let child = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let child_port = child.local_addr()?.port();
        tokio::spawn(async move {
            let (mut stream, _) = child.accept().await.expect("child accept");
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let count = stream.read(&mut buf).await.expect("child read");
                if count == 0 {
                    break;
                }
                buf[..count].make_ascii_uppercase();
                stream.write_all(&buf[..count]).await.expect("child write");
            }
        });

        // The worker's handoff socket.
        let socket = dir.path().join(HANDOFF_SOCKET_NAME);
        let listener = tokio::net::UnixListener::bind(&socket)?;
        tokio::spawn(async move {
            let (control, _) = listener.accept().await.expect("handoff accept");
            serve_with(control, move |request| {
                assert_eq!(request.child, "machine-abc.01");
                Ok(TaskTarget {
                    dial: TaskDial::Loopback { netns: None },
                    label: request.port.clone(),
                    port: child_port,
                })
            })
            .await;
        });

        // The owner: accepts the outside client's connection.
        let front = std::net::TcpListener::bind("127.0.0.1:0")?;
        let front_addr = front.local_addr()?;
        let client = std::thread::spawn(move || -> R<Vec<u8>> {
            let mut stream = std::net::TcpStream::connect(front_addr)?;
            // More than one copy buffer, so the flow turns bulk.
            let payload = vec![b'a'; 300 * 1024];
            let writer = {
                let mut stream = stream.try_clone()?;
                std::thread::spawn(move || -> R<()> {
                    stream.write_all(&payload)?;
                    stream.shutdown(std::net::Shutdown::Write)?;
                    Ok(())
                })
            };
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply)?;
            writer.join().expect("writer")?;
            Ok(reply)
        });
        let (accepted, _) = tokio::task::spawn_blocking(move || front.accept()).await??;
        let request = HandoffRequest {
            child: "machine-abc.01".into(),
            port: "http".into(),
        };
        let closed = tokio::task::spawn_blocking(move || -> R<HandoffClosed> {
            let control = hand_off(&socket, &request, accepted.as_raw_fd())?;
            // The owner leaves the byte path.
            drop(accepted);
            wait_closed(control)
        })
        .await??;
        let reply = tokio::task::spawn_blocking(move || client.join().expect("client")).await??;

        assert_eq!(reply.len(), 300 * 1024);
        assert!(reply.iter().all(|byte| *byte == b'A'), "the child answered");
        assert!(closed.closed);
        assert_eq!(closed.sent, 300 * 1024);
        assert_eq!(closed.received, 300 * 1024);
        assert_eq!(closed.error, "");
        Ok(())
    }

    #[tokio::test]
    async fn a_refused_handoff_says_why_test() -> R<()> {
        let dir = tempfile::tempdir_in(crate::test_scratch::root())?;
        let socket = dir.path().join(HANDOFF_SOCKET_NAME);
        let listener = tokio::net::UnixListener::bind(&socket)?;
        tokio::spawn(async move {
            let (control, _) = listener.accept().await.expect("handoff accept");
            serve_with(control, |request| {
                Err(tonic::Status::not_found(format!(
                    "no child {} of this workload",
                    request.child
                )))
            })
            .await;
        });
        let (ours, _theirs) = std::os::unix::net::UnixStream::pair()?;
        let err = tokio::task::spawn_blocking(move || {
            hand_off(
                &socket,
                &HandoffRequest {
                    child: "someone-elses".into(),
                    port: "http".into(),
                },
                ours.as_raw_fd(),
            )
        })
        .await?
        .expect_err("refused");
        let message = format!("{err:#}");
        assert!(message.contains("not_found"), "{message}");
        assert!(message.contains("no child someone-elses"), "{message}");
        Ok(())
    }

    #[tokio::test]
    async fn a_request_without_a_descriptor_is_refused_test() -> R<()> {
        let (ours, theirs) = UnixStream::pair()?;
        let mut ours = ours;
        ours.write_all(b"{\"child\":\"c\",\"port\":\"http\"}\n")
            .await?;
        let err = read_request(&theirs).await.expect_err("no descriptor");
        assert!(err.to_string().contains("no file descriptor"), "{err:#}");
        Ok(())
    }
}
