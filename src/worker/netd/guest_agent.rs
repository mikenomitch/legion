//! The sandbox guest agent — the datapath's foothold inside a microVM
//! (docs/network-handlers.md, "Sandboxes").
//!
//! A gated sandbox has no NIC. Its only way out is a virtio-vsock device
//! whose host end is Legion's datapath: the node listens on the VMM's Unix
//! socket for guest port 1080 (the egress proxy) and 53 (the resolver), the
//! same listeners a gated container gets inside its namespace. This agent
//! runs as the first thing the guest init starts and makes those reachable
//! the way every tool expects:
//!
//! - `127.0.0.1:1080` inside the guest is a TCP listener; each connection is
//!   relayed byte-for-byte to host vsock port 1080, so `HTTP_PROXY`,
//!   `ALL_PROXY=socks5://…` and `CONNECT` all work unchanged.
//! - `127.0.0.1:53/udp` answers DNS: each query is framed as DNS-over-TCP
//!   (two-byte length prefix) over a vsock stream to host port 53 and the
//!   answer is relayed back.
//!
//! Nothing here decides anything — policy, handlers, identity and the graph
//! all run on the host side of the vsock, which is what keeps the guest
//! untrusted. The agent is small on purpose: `libc` sockets and `std`
//! threads, no async runtime, so it starts in a few milliseconds and can
//! run on a busybox rootfs.
//!
//! The host end is abstracted as [`HostDial`] so the same relay code is
//! tested over Unix sockets named `<base>_<port>` — the convention both
//! Firecracker and cloud-hypervisor use for the host side of vsock, i.e.
//! exactly what `netd` binds.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// `VMADDR_CID_HOST`: the host as seen from a guest.
pub const HOST_CID: u32 = 2;
/// Guest ports the host datapath listens on.
pub const PROXY_PORT: u32 = 1080;
pub const DNS_PORT: u32 = 53;

/// A bidirectional stream to the host.
pub trait HostStream: Read + Write + Send {
    /// A second handle for the other direction of a relay.
    fn split(&self) -> io::Result<Box<dyn HostStream>>;
    fn shutdown_write(&self) -> io::Result<()>;
}

impl HostStream for UnixStream {
    fn split(&self) -> io::Result<Box<dyn HostStream>> {
        Ok(Box::new(self.try_clone()?))
    }
    fn shutdown_write(&self) -> io::Result<()> {
        self.shutdown(std::net::Shutdown::Write)
    }
}

/// How the agent reaches a host port.
pub trait HostDial: Send + Sync {
    fn dial(&self, port: u32) -> io::Result<Box<dyn HostStream>>;
}

/// The real thing: `AF_VSOCK` to the host CID.
pub struct VsockDial {
    pub cid: u32,
}

impl HostDial for VsockDial {
    fn dial(&self, port: u32) -> io::Result<Box<dyn HostStream>> {
        Ok(Box::new(vsock_connect(self.cid, port)?))
    }
}

/// Tests and the host-side conformance probe: `<base>_<port>` Unix sockets,
/// the VMMs' host-side naming.
pub struct UnixDial {
    pub base: PathBuf,
}

impl HostDial for UnixDial {
    fn dial(&self, port: u32) -> io::Result<Box<dyn HostStream>> {
        let path = format!("{}_{port}", self.base.display());
        Ok(Box::new(UnixStream::connect(path)?))
    }
}

/// The host-side socket path for a guest port, as both VMMs name it.
pub fn host_socket_path(base: &std::path::Path, port: u32) -> PathBuf {
    PathBuf::from(format!("{}_{port}", base.display()))
}

#[cfg(target_os = "linux")]
fn vsock_connect(cid: u32, port: u32) -> io::Result<UnixStream> {
    use std::os::unix::io::FromRawFd;
    // SAFETY: plain socket(2)/connect(2) on a freshly created descriptor; the
    // sockaddr is fully initialised and its length is its size.
    unsafe {
        let fd = libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut addr: libc::sockaddr_vm = std::mem::zeroed();
        addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
        addr.svm_cid = cid;
        addr.svm_port = port;
        let rc = libc::connect(
            fd,
            &addr as *const libc::sockaddr_vm as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        );
        if rc != 0 {
            let err = io::Error::last_os_error();
            libc::close(fd);
            return Err(err);
        }
        // A vsock stream is a stream socket: read/write/shutdown/dup all
        // behave, which is everything `UnixStream` asks of the descriptor.
        Ok(UnixStream::from_raw_fd(fd))
    }
}

#[cfg(not(target_os = "linux"))]
fn vsock_connect(_cid: u32, _port: u32) -> io::Result<UnixStream> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "AF_VSOCK is Linux-only",
    ))
}

/// What to run.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Guest TCP bind for the proxy relay.
    pub proxy_listen: SocketAddr,
    /// Host port the proxy relay dials.
    pub proxy_port: u32,
    /// Guest UDP bind for the DNS relay; `None` disables it.
    pub dns_listen: Option<SocketAddr>,
    /// Host port the DNS relay dials.
    pub dns_port: u32,
}

impl Default for AgentConfig {
    fn default() -> Self {
        AgentConfig {
            proxy_listen: "127.0.0.1:1080".parse().expect("static"),
            proxy_port: PROXY_PORT,
            dns_listen: Some("127.0.0.1:53".parse().expect("static")),
            dns_port: DNS_PORT,
        }
    }
}

/// Bound listeners, handed back so a caller (a test) knows the ports.
pub struct Started {
    pub proxy_addr: SocketAddr,
    pub dns_addr: Option<SocketAddr>,
}

/// Bind the guest listeners and start relaying on background threads.
pub fn start(config: AgentConfig, dial: Arc<dyn HostDial>) -> io::Result<Started> {
    let tcp = TcpListener::bind(config.proxy_listen)?;
    let proxy_addr = tcp.local_addr()?;
    let proxy_port = config.proxy_port;
    {
        let dial = dial.clone();
        thread::Builder::new()
            .name("legion-guest-proxy".into())
            .spawn(move || relay_tcp(tcp, dial, proxy_port))?;
    }
    let dns_addr = match config.dns_listen {
        Some(bind) => {
            let udp = UdpSocket::bind(bind)?;
            let addr = udp.local_addr()?;
            let dns_port = config.dns_port;
            thread::Builder::new()
                .name("legion-guest-dns".into())
                .spawn(move || relay_dns(udp, dial, dns_port))?;
            Some(addr)
        }
        None => None,
    };
    Ok(Started {
        proxy_addr,
        dns_addr,
    })
}

/// Accept guest connections forever and relay each to the host port.
fn relay_tcp(listener: TcpListener, dial: Arc<dyn HostDial>, port: u32) {
    for accepted in listener.incoming() {
        let Ok(client) = accepted else {
            thread::sleep(Duration::from_millis(50));
            continue;
        };
        let dial = dial.clone();
        let _ = thread::Builder::new()
            .name("legion-guest-relay".into())
            .spawn(move || {
                let host = match dial.dial(port) {
                    Ok(host) => host,
                    Err(err) => {
                        eprintln!("legion-guest-agent: host port {port} unreachable: {err}");
                        return;
                    }
                };
                relay_pair(client, host);
            });
    }
}

/// Copy both directions until both sides are done.
fn relay_pair(client: TcpStream, host: Box<dyn HostStream>) {
    let _ = client.set_nodelay(true);
    let (Ok(mut client_read), Ok(mut host_write)) = (client.try_clone(), host.split()) else {
        return;
    };
    let mut client_write = client;
    let mut host_read = host;
    let up = thread::spawn(move || {
        let _ = io::copy(&mut client_read, &mut host_write);
        let _ = host_write.shutdown_write();
    });
    let _ = io::copy(&mut host_read, &mut client_write);
    let _ = client_write.shutdown(std::net::Shutdown::Write);
    let _ = up.join();
}

/// Answer each UDP query by relaying it over one host stream with
/// DNS-over-TCP framing.
fn relay_dns(udp: UdpSocket, dial: Arc<dyn HostDial>, port: u32) {
    let udp = Arc::new(udp);
    let mut buf = [0u8; 4096];
    loop {
        let (n, peer) = match udp.recv_from(&mut buf) {
            Ok(received) => received,
            Err(_) => {
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        let query = buf[..n].to_vec();
        let dial = dial.clone();
        let udp = udp.clone();
        let _ = thread::Builder::new()
            .name("legion-guest-dns-query".into())
            .spawn(move || {
                if let Ok(answer) = relay_dns_query(&*dial, port, &query) {
                    let _ = udp.send_to(&answer, peer);
                }
            });
    }
}

/// One DNS-over-TCP exchange: length-prefixed query out, length-prefixed
/// answer back.
pub fn relay_dns_query(dial: &dyn HostDial, port: u32, query: &[u8]) -> io::Result<Vec<u8>> {
    let mut host = dial.dial(port)?;
    let len = u16::try_from(query.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "query too large"))?;
    host.write_all(&len.to_be_bytes())?;
    host.write_all(query)?;
    host.flush()?;
    let mut prefix = [0u8; 2];
    host.read_exact(&mut prefix)?;
    let mut answer = vec![0u8; usize::from(u16::from_be_bytes(prefix))];
    host.read_exact(&mut answer)?;
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    /// A fake host: `<base>_1080` echoes bytes back upper-cased; `<base>_53`
    /// answers every DNS-over-TCP query with its own length reversed.
    fn fake_host(dir: &std::path::Path) -> PathBuf {
        let base = dir.join("vsock.sock");
        let proxy = UnixListener::bind(host_socket_path(&base, PROXY_PORT)).unwrap();
        thread::spawn(move || {
            for stream in proxy.incoming().flatten() {
                thread::spawn(move || {
                    let mut stream = stream;
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 256];
                    loop {
                        let n = stream.read(&mut chunk).unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if buf.ends_with(b"\n") {
                            break;
                        }
                    }
                    let _ =
                        stream.write_all(String::from_utf8_lossy(&buf).to_uppercase().as_bytes());
                });
            }
        });
        let dns = UnixListener::bind(host_socket_path(&base, DNS_PORT)).unwrap();
        thread::spawn(move || {
            for stream in dns.incoming().flatten() {
                let mut stream = stream;
                let mut prefix = [0u8; 2];
                stream.read_exact(&mut prefix).unwrap();
                let mut query = vec![0u8; usize::from(u16::from_be_bytes(prefix))];
                stream.read_exact(&mut query).unwrap();
                let mut answer = query.clone();
                answer.reverse();
                let _ = stream.write_all(&(answer.len() as u16).to_be_bytes());
                let _ = stream.write_all(&answer);
            }
        });
        base
    }

    #[test]
    fn relays_tcp_and_dns_to_the_host_sockets() -> io::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let base = fake_host(dir.path());
        let dial: Arc<dyn HostDial> = Arc::new(UnixDial { base });
        let started = start(
            AgentConfig {
                proxy_listen: "127.0.0.1:0".parse().unwrap(),
                dns_listen: Some("127.0.0.1:0".parse().unwrap()),
                ..Default::default()
            },
            dial.clone(),
        )?;

        // TCP: what the guest sends reaches the host port and the reply
        // comes back on the same connection.
        let mut client = TcpStream::connect(started.proxy_addr)?;
        client.write_all(b"connect please\n")?;
        client.shutdown(std::net::Shutdown::Write)?;
        let mut reply = String::new();
        client.read_to_string(&mut reply)?;
        assert_eq!(reply, "CONNECT PLEASE\n");

        // DNS: a UDP query is framed over a host stream and answered.
        let socket = UdpSocket::bind("127.0.0.1:0")?;
        socket.set_read_timeout(Some(Duration::from_secs(3)))?;
        socket.send_to(b"\x12\x34query", started.dns_addr.unwrap())?;
        let mut buf = [0u8; 64];
        let (n, _) = socket.recv_from(&mut buf)?;
        assert_eq!(&buf[..n], b"yreuq\x34\x12");

        // An unreachable host port closes the guest connection cleanly.
        let dead: Arc<dyn HostDial> = Arc::new(UnixDial {
            base: dir.path().join("nothing"),
        });
        assert!(relay_dns_query(&*dead, DNS_PORT, b"x").is_err());
        Ok(())
    }
}
