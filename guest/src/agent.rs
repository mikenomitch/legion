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
//!
//! **Ingress.** The other direction is [`Ingress`]: for each port the task
//! advertises, the agent listens on `AF_VSOCK` at that port number and
//! relays every connection to the workload's `127.0.0.1:<port>`. The node
//! datapath opens those connections through the VMM's base socket with the
//! host-initiated handshake (`CONNECT <port>`), after its ingress chain,
//! TLS, and accounting have run. The vsock port is the port the workload
//! binds (`LEGION_PORT_<label>`), so one number names both ends; the agent's
//! egress ports (1080, 53) are host ports, a separate space. The listener is
//! abstracted as [`GuestListen`] so the relay is tested over Unix sockets
//! too.
//!
//! **Across a memory restore.** Every relay dials a fresh vsock stream, so
//! the first connection after a restore reaches the restoring allocation's
//! listener (the worker moves the VM onto its own socket). Streams that were
//! open at capture are dead: Firecracker resets them, cloud-hypervisor v52
//! and later reset them on restore, and older cloud-hypervisor can leave
//! them half open. The agent tracks every open relay in [`Relays`], and its
//! resume watch ([`crate::resume::Watcher`]) closes them all when it sees a
//! restore, so a guest client gets a prompt close on every VMM version and
//! never a hang. The agent's own listeners, loopback and vsock alike, are
//! guest kernel state and survive the restore unchanged: a restored guest
//! keeps the ports it was captured with, so the datapath of the restoring
//! allocation dials the same vsock ports. A port the restore adds arrives
//! on the identity disk (`ResumeIdentity::ingress_ports`), and the resume
//! watch hands it to [`Ingress::ensure`].

use std::collections::{BTreeSet, HashMap};
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    /// Close both directions, which unblocks a reader on another handle.
    fn shutdown_both(&self) -> io::Result<()>;
}

impl HostStream for UnixStream {
    fn split(&self) -> io::Result<Box<dyn HostStream>> {
        Ok(Box::new(self.try_clone()?))
    }
    fn shutdown_write(&self) -> io::Result<()> {
        self.shutdown(std::net::Shutdown::Write)
    }
    fn shutdown_both(&self) -> io::Result<()> {
        self.shutdown(std::net::Shutdown::Both)
    }
}

/// The relays the agent holds open right now, so a restore can close them
/// all at once (see the module docs).
#[derive(Default)]
pub struct Relays {
    next: AtomicU64,
    open: Mutex<HashMap<u64, OpenRelay>>,
}

/// Handles on both ends of one relay, kept only to shut them down.
struct OpenRelay {
    client: TcpStream,
    host: Box<dyn HostStream>,
}

impl Relays {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, OpenRelay>> {
        self.open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Track a relay. `None` when a handle cannot be duplicated, in which
    /// case the relay still runs, untracked.
    fn register(&self, client: &TcpStream, host: &dyn HostStream) -> Option<u64> {
        let relay = OpenRelay {
            client: client.try_clone().ok()?,
            host: host.split().ok()?,
        };
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.lock().insert(id, relay);
        Some(id)
    }

    fn finish(&self, id: Option<u64>) {
        if let Some(id) = id {
            self.lock().remove(&id);
        }
    }

    /// Relays open right now.
    pub fn open_count(&self) -> usize {
        self.lock().len()
    }

    /// Close every open relay, both ends: the guest client sees its
    /// connection close, and a relay thread blocked on a dead host stream
    /// wakes up. Returns how many were open.
    pub fn close_all(&self) -> usize {
        let open: Vec<OpenRelay> = self.lock().drain().map(|(_, relay)| relay).collect();
        for relay in &open {
            let _ = relay.client.shutdown(std::net::Shutdown::Both);
            let _ = relay.host.shutdown_both();
        }
        open.len()
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

/// Where host-initiated connections arrive: the guest side of ingress.
pub trait GuestListen: Send + Sync {
    /// Listen on guest port `port`.
    fn listen(&self, port: u32) -> io::Result<Box<dyn GuestListener>>;
}

/// One bound ingress listener.
pub trait GuestListener: Send {
    /// The next connection the host opened to this port.
    fn accept(&self) -> io::Result<Box<dyn HostStream>>;
}

/// The real thing: `AF_VSOCK` on any CID. The VMM delivers a host's
/// `CONNECT <port>` on its base socket to the guest listener on `port`.
pub struct VsockListen;

impl GuestListen for VsockListen {
    fn listen(&self, port: u32) -> io::Result<Box<dyn GuestListener>> {
        vsock_listen(port)
    }
}

/// Tests: a Unix listener at `<base>_<port>` stands in for the vsock port.
pub struct UnixListen {
    pub base: PathBuf,
}

impl GuestListen for UnixListen {
    fn listen(&self, port: u32) -> io::Result<Box<dyn GuestListener>> {
        Ok(Box::new(UnixListener::bind(host_socket_path(
            &self.base, port,
        ))?))
    }
}

impl GuestListener for UnixListener {
    fn accept(&self) -> io::Result<Box<dyn HostStream>> {
        Ok(Box::new(UnixListener::accept(self)?.0))
    }
}

#[cfg(target_os = "linux")]
struct VsockListener {
    fd: std::os::unix::io::OwnedFd,
}

#[cfg(target_os = "linux")]
impl GuestListener for VsockListener {
    fn accept(&self) -> io::Result<Box<dyn HostStream>> {
        use std::os::unix::io::{AsRawFd, FromRawFd};
        loop {
            // SAFETY: accept4(2) on a listening descriptor we own; the peer
            // address is not wanted, so both out-pointers are null.
            let fd = unsafe {
                libc::accept4(
                    self.fd.as_raw_fd(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_CLOEXEC,
                )
            };
            if fd >= 0 {
                // SAFETY: a freshly accepted stream descriptor we now own. A
                // vsock stream behaves as a stream socket for read, write,
                // shutdown, and dup, which is all `UnixStream` asks of it.
                // (std's `UnixListener::accept` would reject the vsock peer
                // address, which is why the accept is done here.)
                return Ok(Box::new(unsafe { UnixStream::from_raw_fd(fd) }));
            }
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::Interrupted {
                return Err(err);
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn vsock_listen(port: u32) -> io::Result<Box<dyn GuestListener>> {
    use std::os::unix::io::{FromRawFd, OwnedFd};
    // SAFETY: plain socket(2)/bind(2)/listen(2) on a freshly created
    // descriptor, owned by `OwnedFd` from the moment it exists; the sockaddr
    // is fully initialised and its length is its size.
    unsafe {
        let raw = libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = OwnedFd::from_raw_fd(raw);
        let mut addr: libc::sockaddr_vm = std::mem::zeroed();
        addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
        addr.svm_cid = libc::VMADDR_CID_ANY;
        addr.svm_port = port;
        if libc::bind(
            raw,
            &addr as *const libc::sockaddr_vm as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        ) != 0
        {
            return Err(io::Error::last_os_error());
        }
        if libc::listen(raw, 128) != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Box::new(VsockListener { fd }))
    }
}

#[cfg(not(target_os = "linux"))]
fn vsock_listen(_port: u32) -> io::Result<Box<dyn GuestListener>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "AF_VSOCK is Linux-only",
    ))
}

/// How long the agent waits for the workload's loopback port to accept.
const WORKLOAD_DIAL_TIMEOUT: Duration = Duration::from_secs(2);

/// The agent's ingress side: one listener per port the node datapath
/// fronts, each relaying host-initiated connections to the workload (see
/// the module docs). Its relays are tracked in the same [`Relays`] as the
/// egress ones, so a restore closes both.
pub struct Ingress {
    listen: Arc<dyn GuestListen>,
    relays: Arc<Relays>,
    /// Where the workload listens: loopback in a guest.
    workload: IpAddr,
    ports: Mutex<BTreeSet<u16>>,
}

impl Ingress {
    pub fn new(listen: Arc<dyn GuestListen>, relays: Arc<Relays>) -> Arc<Self> {
        Arc::new(Ingress {
            listen,
            relays,
            workload: IpAddr::V4(Ipv4Addr::LOCALHOST),
            ports: Mutex::new(BTreeSet::new()),
        })
    }

    /// Listen on every port in `ports` that is not listened on yet. A port
    /// already served keeps its listener (a restored guest's listeners are
    /// part of its memory and survive the restore), and nothing is ever
    /// unbound. Returns the ports it could not listen on, with why.
    pub fn ensure(self: &Arc<Self>, ports: &[u16]) -> Vec<(u16, io::Error)> {
        let mut failed = Vec::new();
        for &port in ports {
            if self.lock().contains(&port) {
                continue;
            }
            let listener = match self.listen.listen(u32::from(port)) {
                Ok(listener) => listener,
                Err(err) => {
                    failed.push((port, err));
                    continue;
                }
            };
            let this = self.clone();
            match thread::Builder::new()
                .name(format!("legion-guest-ingress-{port}"))
                .spawn(move || this.serve(port, listener))
            {
                Ok(_) => {
                    self.lock().insert(port);
                }
                Err(err) => failed.push((port, err)),
            }
        }
        failed
    }

    /// The ports listened on now.
    pub fn ports(&self) -> Vec<u16> {
        self.lock().iter().copied().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeSet<u16>> {
        self.ports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Accept host connections on `port` forever, relaying each to the
    /// workload on the same port.
    fn serve(self: Arc<Self>, port: u16, listener: Box<dyn GuestListener>) {
        loop {
            let host = match listener.accept() {
                Ok(host) => host,
                Err(_) => {
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
            };
            let workload = SocketAddr::new(self.workload, port);
            let relays = self.relays.clone();
            let _ = thread::Builder::new()
                .name("legion-guest-ingress-relay".into())
                .spawn(move || {
                    // Nothing listening yet (a workload still starting, a
                    // health check that came early): close the host stream
                    // so the datapath sees the connection end at once.
                    let Ok(client) = TcpStream::connect_timeout(&workload, WORKLOAD_DIAL_TIMEOUT)
                    else {
                        let _ = host.shutdown_both();
                        return;
                    };
                    let id = relays.register(&client, &*host);
                    relay_pair(client, host);
                    relays.finish(id);
                });
        }
    }
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
    /// Ports the node datapath fronts: the agent listens on each as a vsock
    /// port and relays to the workload's loopback port of the same number.
    pub ingress_ports: Vec<u16>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        AgentConfig {
            proxy_listen: "127.0.0.1:1080".parse().expect("static"),
            proxy_port: PROXY_PORT,
            dns_listen: Some("127.0.0.1:53".parse().expect("static")),
            dns_port: DNS_PORT,
            ingress_ports: Vec::new(),
        }
    }
}

/// Bound listeners, handed back so a caller (a test) knows the ports, and
/// the open relays, so the resume watch can close them on a restore.
pub struct Started {
    pub proxy_addr: SocketAddr,
    pub dns_addr: Option<SocketAddr>,
    pub relays: Arc<Relays>,
    /// The ingress side, so the resume watch can add the ports a restore
    /// names.
    pub ingress: Arc<Ingress>,
    /// Ingress ports the agent could not listen on, with why. The rest of
    /// the agent runs regardless: egress never depends on ingress.
    pub ingress_errors: Vec<(u16, io::Error)>,
}

impl Started {
    /// What the agent does when its resume watch sees a restore: close
    /// every relay the capture left dead (egress and ingress), then listen
    /// on any ingress port the restore added. Returns how many relays it
    /// closed and the ports it could not listen on.
    pub fn restored(&self, ingress_ports: &[u16]) -> (usize, Vec<(u16, io::Error)>) {
        let closed = self.relays.close_all();
        (closed, self.ingress.ensure(ingress_ports))
    }
}

/// Bind the guest listeners and start relaying on background threads.
/// `listen` is where host-initiated (ingress) connections arrive:
/// [`VsockListen`] in a guest.
pub fn start(
    config: AgentConfig,
    dial: Arc<dyn HostDial>,
    listen: Arc<dyn GuestListen>,
) -> io::Result<Started> {
    let tcp = TcpListener::bind(config.proxy_listen)?;
    let proxy_addr = tcp.local_addr()?;
    let proxy_port = config.proxy_port;
    let relays = Arc::new(Relays::default());
    let ingress = Ingress::new(listen, relays.clone());
    let ingress_errors = ingress.ensure(&config.ingress_ports);
    {
        let dial = dial.clone();
        let relays = relays.clone();
        thread::Builder::new()
            .name("legion-guest-proxy".into())
            .spawn(move || relay_tcp(tcp, dial, proxy_port, relays))?;
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
        relays,
        ingress,
        ingress_errors,
    })
}

/// Accept guest connections forever and relay each to the host port.
fn relay_tcp(listener: TcpListener, dial: Arc<dyn HostDial>, port: u32, relays: Arc<Relays>) {
    for accepted in listener.incoming() {
        let Ok(client) = accepted else {
            thread::sleep(Duration::from_millis(50));
            continue;
        };
        let dial = dial.clone();
        let relays = relays.clone();
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
                let id = relays.register(&client, &*host);
                relay_pair(client, host);
                relays.finish(id);
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
            Arc::new(UnixListen {
                base: dir.path().join("guest.sock"),
            }),
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

    /// A workload on a loopback port that answers each line upper-cased,
    /// then closes.
    fn workload() -> io::Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                thread::spawn(move || {
                    let mut stream = stream;
                    let mut line = Vec::new();
                    let mut byte = [0u8; 1];
                    while stream.read(&mut byte).unwrap_or(0) == 1 {
                        line.push(byte[0]);
                        if byte[0] == b'\n' {
                            let _ = stream.write_all(&line.to_ascii_uppercase());
                            line.clear();
                        }
                    }
                });
            }
        });
        Ok(port)
    }

    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "{what}");
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Gated plan §6.10: a host-initiated connection to guest port P (here
    /// a Unix socket standing in for vsock port P) reaches the workload on
    /// `127.0.0.1:P`, both ways. The relay is tracked, so a restore closes
    /// it like an egress relay, and a later connection works again.
    #[test]
    fn ingress_relays_host_connections_to_the_workload_port() -> io::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let port = workload()?;
        let guest = dir.path().join("guest.sock");
        let started = start(
            AgentConfig {
                proxy_listen: "127.0.0.1:0".parse().unwrap(),
                dns_listen: None,
                ingress_ports: vec![port],
                ..Default::default()
            },
            Arc::new(UnixDial {
                base: dir.path().join("host.sock"),
            }),
            Arc::new(UnixListen {
                base: guest.clone(),
            }),
        )?;
        assert!(started.ingress_errors.is_empty());
        assert_eq!(started.ingress.ports(), vec![port]);

        // What the host sends reaches the workload, and its answer comes
        // back on the same stream.
        let mut host = UnixStream::connect(host_socket_path(&guest, u32::from(port)))?;
        host.set_read_timeout(Some(Duration::from_secs(5)))?;
        host.write_all(b"hello guest\n")?;
        let mut answer = [0u8; 12];
        host.read_exact(&mut answer)?;
        assert_eq!(&answer, b"HELLO GUEST\n");
        wait_until("the relay is tracked", || started.relays.open_count() == 1);

        // A restore closes it: the host sees the stream end promptly.
        assert_eq!(started.relays.close_all(), 1);
        let mut rest = Vec::new();
        let read = host.read_to_end(&mut rest);
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?} {rest:?}");
        wait_until("the relay finishes", || started.relays.open_count() == 0);

        // The listener itself survives, so the next connection works.
        let mut again = UnixStream::connect(host_socket_path(&guest, u32::from(port)))?;
        again.set_read_timeout(Some(Duration::from_secs(5)))?;
        again.write_all(b"again\n")?;
        let mut answer = [0u8; 6];
        again.read_exact(&mut answer)?;
        assert_eq!(&answer, b"AGAIN\n");
        Ok(())
    }

    /// The ports a restore names are added, ports already served are left
    /// alone, a port that cannot be listened on is reported, and a port
    /// with no workload behind it closes the host stream at once.
    #[test]
    fn ingress_ensure_adds_ports_and_reports_failures() -> io::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let guest = dir.path().join("guest.sock");
        let ingress = Ingress::new(
            Arc::new(UnixListen {
                base: guest.clone(),
            }),
            Arc::new(Relays::default()),
        );
        let first = workload()?;
        assert!(ingress.ensure(&[first]).is_empty());
        // Already served: no second bind (which would fail on the path).
        assert!(ingress.ensure(&[first]).is_empty());

        let second = workload()?;
        // Something already holds the second port's socket path.
        let squatter = UnixListener::bind(host_socket_path(&guest, u32::from(second)))?;
        let failed = ingress.ensure(&[first, second]);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, second);
        drop(squatter);
        std::fs::remove_file(host_socket_path(&guest, u32::from(second)))?;
        assert!(ingress.ensure(&[second]).is_empty());
        let mut ports = vec![first, second];
        ports.sort_unstable();
        assert_eq!(ingress.ports(), ports);

        // A port nobody listens on in the guest: the stream closes.
        let idle = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        assert!(ingress.ensure(&[idle]).is_empty());
        let mut host = UnixStream::connect(host_socket_path(&guest, u32::from(idle)))?;
        host.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut buf = [0u8; 4];
        let read = host.read(&mut buf);
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
        Ok(())
    }
}
