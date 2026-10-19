//! Transparent capture (mesh interfaces plan §6.A.3, phase A3; docs:
//! network-handlers.md, "The kernel classifier").
//!
//! When the kernel classifier meets a flow only netd can decide (a host rule
//! for an address no resolver answer taught it, a request-time rule, a chain
//! with hops), it rewrites the `connect()` to a capture listener and records
//! where the socket meant to go. The listener:
//!
//! 1. recovers the original destination from the peer it accepted (the
//!    classifier keyed it by netns, local address, and local port);
//! 2. peeks the first bytes for a name: the TLS SNI, or the HTTP `Host`;
//! 3. hands the flow to the egress proxy's judge (`proxy::serve_captured`),
//!    which allows a named flow only if the original address is one of that
//!    name's addresses, so a client that lies in its SNI gets nothing.
//!
//! Host-mode allocations share one listener on the node's loopback; a gated
//! allocation gets one inside its namespace, beside its egress proxy.

use crate::prelude::*;
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::listen::spawn_accept_loop;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;

/// Where a gated allocation's capture listener binds inside its namespace,
/// next to its egress proxy (`127.0.0.1:1080`).
pub const GATED_CAPTURE_PORT: u16 = 1081;

/// How long the listener waits for the client's first bytes before judging
/// the flow without a name (a protocol where the server speaks first).
pub const PEEK_TIMEOUT: Duration = Duration::from_millis(300);

/// The most the listener reads before deciding.
const PEEK_MAX: usize = 4096;

/// What the first bytes of a flow say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Peeked {
    /// A TLS ClientHello, with its SNI when it names one.
    Tls(Option<String>),
    /// An HTTP/1 request head, with its `Host` (port stripped).
    Http(Option<String>),
    /// Anything else, or nothing yet.
    Other,
}

impl Peeked {
    pub fn name(&self) -> Option<&str> {
        match self {
            Peeked::Tls(name) | Peeked::Http(name) => name.as_deref(),
            Peeked::Other => None,
        }
    }
}

/// Classify the first bytes of a flow.
pub fn peek(bytes: &[u8]) -> Peeked {
    if let Some(tls) = client_hello_sni(bytes) {
        return Peeked::Tls(tls);
    }
    if let Some(host) = http_host(bytes) {
        return Peeked::Http(host);
    }
    Peeked::Other
}

/// The SNI of a TLS ClientHello: `None` when `bytes` is not one,
/// `Some(None)` when it is one without a server name.
fn client_hello_sni(bytes: &[u8]) -> Option<Option<String>> {
    // TLS record: handshake (22), version, length; handshake: client
    // hello (1), length (3).
    if bytes.len() < 9 || bytes[0] != 22 || bytes[1] != 3 || bytes[5] != 1 {
        return None;
    }
    let mut at = 9;
    let take = |at: &mut usize, n: usize| -> Option<&[u8]> {
        let slice = bytes.get(*at..*at + n)?;
        *at += n;
        Some(slice)
    };
    let len8 = |at: &mut usize| -> Option<usize> { take(at, 1).map(|b| usize::from(b[0])) };
    let len16 = |at: &mut usize| -> Option<usize> {
        take(at, 2).map(|b| usize::from(u16::from_be_bytes([b[0], b[1]])))
    };
    // version, random
    take(&mut at, 2 + 32).map(|_| ()).unwrap_or(());
    if at > bytes.len() {
        return Some(None);
    }
    let session = len8(&mut at)?;
    take(&mut at, session)?;
    let suites = len16(&mut at)?;
    take(&mut at, suites)?;
    let compression = len8(&mut at)?;
    take(&mut at, compression)?;
    let Some(extensions) = len16(&mut at) else {
        return Some(None);
    };
    let end = (at + extensions).min(bytes.len());
    while at + 4 <= end {
        let kind = u16::from_be_bytes([bytes[at], bytes[at + 1]]);
        let len = usize::from(u16::from_be_bytes([bytes[at + 2], bytes[at + 3]]));
        at += 4;
        if kind == 0 {
            // server_name: list length, then (type, length, name).
            let list = bytes.get(at..at + len)?;
            if list.len() < 5 || list[2] != 0 {
                return Some(None);
            }
            let name_len = usize::from(u16::from_be_bytes([list[3], list[4]]));
            let name = list.get(5..5 + name_len)?;
            return Some(
                std::str::from_utf8(name)
                    .ok()
                    .filter(|name| !name.is_empty())
                    .map(|name| name.trim_end_matches('.').to_ascii_lowercase()),
            );
        }
        at += len;
    }
    Some(None)
}

/// The `Host` of an HTTP/1 request head (port stripped): `None` when the
/// bytes are not one.
fn http_host(bytes: &[u8]) -> Option<Option<String>> {
    const METHODS: &[&str] = &[
        "GET ", "POST ", "PUT ", "DELETE ", "HEAD ", "OPTIONS ", "PATCH ", "CONNECT ", "TRACE ",
    ];
    let text = std::str::from_utf8(&bytes[..bytes.len().min(PEEK_MAX)]).ok()?;
    if !METHODS.iter().any(|method| text.starts_with(method)) {
        return None;
    }
    let host = text
        .split("\r\n")
        .skip(1)
        .take_while(|line| !line.is_empty())
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("host")
                .then(|| value.trim())
        })
        .map(|host| {
            let host = match host.strip_prefix('[') {
                Some(rest) => rest.split(']').next().unwrap_or(rest),
                None => host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host),
            };
            host.trim_end_matches('.').to_ascii_lowercase()
        })
        .filter(|host| !host.is_empty());
    Some(host)
}

/// Read what the client sends first, up to [`PEEK_MAX`], waiting at most
/// [`PEEK_TIMEOUT`] for a name to appear.
pub async fn read_first<S>(client: &mut S) -> Vec<u8>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(1024);
    let deadline = tokio::time::Instant::now() + PEEK_TIMEOUT;
    let mut chunk = [0u8; 1024];
    while buf.len() < PEEK_MAX {
        let read = tokio::time::timeout_at(deadline, client.read(&mut chunk)).await;
        match read {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(n)) => {
                buf.extend_from_slice(&chunk[..n]);
                // A full ClientHello record, or a complete request head, is
                // enough.
                if buf.len() >= 5 && buf[0] == 22 {
                    let record = 5 + usize::from(u16::from_be_bytes([buf[3], buf[4]]));
                    if buf.len() >= record {
                        break;
                    }
                } else if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                } else if buf.len() >= 8 && http_host(&buf).is_none() && buf[0] != 22 {
                    // Neither TLS nor HTTP: nothing more to learn.
                    break;
                }
            }
        }
    }
    buf
}

/// The node-wide capture listener host-mode allocations are redirected to.
pub struct HostCapture {
    task: JoinHandle<()>,
    pub addr: SocketAddrV4,
}

impl HostCapture {
    /// Bind on the node's loopback and serve every captured flow.
    pub async fn start(context: Arc<ForwarderContext>) -> R<Self> {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .context("bind the capture listener")?;
        let addr = match listener.local_addr()? {
            SocketAddr::V4(addr) => addr,
            other => bail!("capture listener bound {other}, not IPv4"),
        };
        let netns = netns_cookie(&listener)?;
        let proxies: Arc<
            std::sync::Mutex<HashMap<i64, Arc<crate::worker::netd::proxy::CaptureProxy>>>,
        > = Arc::default();
        let listener = crate::worker::netd::listen::Listener::Tcp(listener);
        let task = spawn_accept_loop(listener, "capture", move |client, peer| {
            let context = context.clone();
            let proxies = proxies.clone();
            async move {
                let Some(peer) = peer else {
                    return;
                };
                let Some((original, allocation_id)) =
                    context.kernel_original_destination(netns, peer)
                else {
                    // Dialed directly, not redirected: nothing to serve.
                    debug!(%peer, "Capture listener: no original destination; closing");
                    return;
                };
                let proxy = {
                    let mut proxies = proxies.lock().unwrap_or_else(|p| p.into_inner());
                    if !proxies.contains_key(&allocation_id) {
                        // A new allocation: drop the state of ones this
                        // node no longer runs, so the map tracks the live set.
                        proxies.retain(|id, _| context.identity_string_for(*id).is_some());
                    }
                    proxies
                        .entry(allocation_id)
                        .or_insert_with(|| {
                            crate::worker::netd::proxy::CaptureProxy::new(
                                allocation_id,
                                false,
                                context.clone(),
                            )
                        })
                        .clone()
                };
                let client = context.activity().track(allocation_id, client);
                crate::worker::netd::proxy::serve_captured(proxy, client, Some(peer), original)
                    .await;
            }
        });
        Ok(HostCapture { task, addr })
    }
}

impl Drop for HostCapture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `SO_NETNS_COOKIE` of a listener: what the classifier recorded with each
/// redirected flow it sends there.
pub fn netns_cookie<T: std::os::fd::AsFd>(socket: &T) -> R<u64> {
    #[cfg(target_os = "linux")]
    {
        crate::worker::netd::kernel::runtime::netns_cookie(socket.as_fd())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = socket;
        bail!("capture needs Linux")
    }
}

#[cfg(test)]
mod capture_test {
    use super::*;

    /// A minimal ClientHello naming `name`.
    pub fn client_hello(name: &str) -> Vec<u8> {
        let mut sni = vec![0u8]; // host_name
        sni.extend_from_slice(&(name.len() as u16).to_be_bytes());
        sni.extend_from_slice(name.as_bytes());
        let mut list = (sni.len() as u16).to_be_bytes().to_vec();
        list.extend_from_slice(&sni);
        let mut ext = vec![0, 0];
        ext.extend_from_slice(&(list.len() as u16).to_be_bytes());
        ext.extend_from_slice(&list);
        let mut body = vec![3, 3];
        body.extend_from_slice(&[7u8; 32]);
        body.push(0); // session id
        body.extend_from_slice(&[0, 2, 0x13, 0x01]); // one suite
        body.extend_from_slice(&[1, 0]); // null compression
        body.extend_from_slice(&(ext.len() as u16).to_be_bytes());
        body.extend_from_slice(&ext);
        let mut handshake = vec![1];
        handshake.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
        handshake.extend_from_slice(&body);
        let mut record = vec![22, 3, 1];
        record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
        record.extend_from_slice(&handshake);
        record
    }

    #[test]
    fn peek_reads_the_sni_and_the_host_test() {
        assert_eq!(
            peek(&client_hello("API.GitHub.com.")),
            Peeked::Tls(Some("api.github.com".into()))
        );
        assert_eq!(
            peek(b"GET /simple/ HTTP/1.1\r\nUser-Agent: pip\r\nHost: files.pypi.org:80\r\n\r\n"),
            Peeked::Http(Some("files.pypi.org".into()))
        );
        assert_eq!(peek(b"POST / HTTP/1.1\r\n\r\n"), Peeked::Http(None));
        assert_eq!(
            peek(b"\x00\x00\x00\x08\x04\xd2\x16\x2f"),
            Peeked::Other,
            "a Postgres SSLRequest"
        );
        assert_eq!(peek(b""), Peeked::Other);
        // Truncated hellos never panic.
        let hello = client_hello("x.example");
        for cut in 0..hello.len() {
            let _ = peek(&hello[..cut]);
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod capture_kernel_test {
    use super::capture_test::client_hello;
    use super::*;
    use crate::structs::netpolicy::{merge_policies, ChainConfig, InlinePolicy};
    use crate::structs::AllocNetPolicy;
    use crate::worker::netd::kernel::runtime::testkit::{capable, node_ip, Scratch};
    use crate::worker::netd::kernel::runtime::KernelNet;
    use crate::worker::netd::kernel_guard::{GuardTable, GuardedAllocation};
    use crate::worker::netd::{Denials, NetPolicyCache};
    use std::net::IpAddr;

    struct Names(HashMap<String, IpAddr>);

    #[tonic::async_trait]
    impl crate::worker::netd::guard::Resolver for Names {
        async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
            match self.0.get(host) {
                Some(ip) => Ok(vec![SocketAddr::new(*ip, port)]),
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    host.to_string(),
                )),
            }
        }
    }

    /// A3 end to end on a real kernel: a host-mode task dials an address
    /// directly; the classifier redirects it (a host rule, an address it never
    /// learned); the capture listener reads the SNI, checks the name owns the
    /// address, and splices to the original destination. A lying SNI and a
    /// nameless flow are refused and recorded.
    ///
    /// ```text
    /// sudo -E cargo test --lib capture_kernel -- --ignored --nocapture
    /// ```
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires root, cgroup v2, and BPF"]
    async fn captured_flows_are_judged_by_name_test() -> R<()> {
        if !capable() {
            eprintln!("skipping: needs root, python3, and cgroup v2");
            return Ok(());
        }
        let Some(ip) = node_ip() else {
            return Ok(());
        };
        let scratch = Scratch::new("capture-e2e").context("scratch cgroup")?;
        let origin = tokio::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        let origin_port = origin.local_addr()?.port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = origin.accept().await {
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let mut buf = [0u8; 4096];
                    if stream.read(&mut buf).await.unwrap_or(0) > 0 {
                        let _ = stream.write_all(b"pong").await;
                    }
                });
            }
        });

        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let mut config: ChainConfig = serde_json::from_value(serde_json::json!({
            "policy": "deny",
            "rules": [{ "when": { "host": "allowed.test", "port": origin_port }, "action": "allow" }],
        }))?;
        config.normalize_and_validate("egress")?;
        let policy = merge_policies(
            &[],
            InlinePolicy {
                egress: Some(&config),
                ..Default::default()
            },
        );
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 5,
            policy_hash: policy.hash.clone(),
        }]);
        cache.insert_body(&policy.hash.clone(), policy)?;
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: vec![format!("{ip}:{origin_port}")],
                deny: vec![],
            },
        )?;
        let names = Names(
            [
                ("allowed.test".to_string(), IpAddr::V4(ip)),
                ("liar.test".to_string(), "192.0.2.99".parse()?),
            ]
            .into_iter()
            .collect(),
        );
        let base = GuardTable {
            cgroup_parent: scratch.parent.clone(),
            ..Default::default()
        };
        let context = Arc::new(
            ForwarderContext::new(cache, denials.clone())
                .with_host_guard(Arc::new(guard))
                .with_resolver(Arc::new(names))
                .with_kernel_classifier(base, KernelNet::open(scratch.settings(None))?),
        );
        let capture = HostCapture::start(context.clone()).await?;
        context.set_host_capture(Some(capture.addr));
        let reconcile = context.clone();
        let leaf = scratch.leaf.clone();
        tokio::task::spawn_blocking(move || {
            reconcile.reconcile_kernel_guard(vec![GuardedAllocation {
                allocation_id: 5,
                cgroup: leaf,
                vip_block: None,
                loopback_ports: vec![],
                resolver: None,
                gated: false,
                host_sockets: true,
            }])
        })
        .await?;

        let hex = |bytes: Vec<u8>| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let script = format!(
            r#"
import socket
def send(payload):
    s = socket.create_connection(('{ip}', {origin_port}), timeout=3)
    s.sendall(bytes.fromhex(payload))
    try:
        return s.recv(16).decode() or 'closed'
    except Exception as e:
        return type(e).__name__
print(send('{allowed}'), send('{liar}'), send('{raw}'))
"#,
            allowed = hex(client_hello("allowed.test")),
            liar = hex(client_hello("liar.test")),
            raw = hex(b"hello there".to_vec()),
        );
        let scratch_ref = &scratch;
        let said = tokio::task::block_in_place(|| scratch_ref.run(&script));
        assert_eq!(
            said, "pong closed closed",
            "allowed by name, lying SNI, nameless"
        );
        let drained = denials.drain();
        assert!(
            drained
                .iter()
                .any(|d| d.source == crate::structs::netpolicy::SOURCE_CAPTURE_NAME),
            "{drained:?}"
        );
        assert!(
            drained.iter().any(|d| d.source == "fallthrough"),
            "{drained:?}"
        );
        let status = context.kernel_status(5).context("a kernel status")?;
        assert!(status.enforced && status.capture, "{status:?}");
        context.shutdown_kernel();
        Ok(())
    }

    /// A gated task's capture is transparent: a tool that ignores the proxy
    /// variables still gets out, under policy, through the capture listener
    /// inside its namespace; an address nothing allows is refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires root, cgroup v2, BPF, and network namespaces"]
    async fn gated_capture_is_transparent_test() -> R<()> {
        if !capable() {
            return Ok(());
        }
        let Some(ip) = node_ip() else {
            return Ok(());
        };
        let allocation_id = 900_000 + i64::from(std::process::id() % 1000);
        let netns = crate::worker::netd::netns::create(allocation_id)?;
        let scratch = Scratch::new("gated-capture").context("scratch cgroup")?;
        let origin = tokio::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        let origin_port = origin.local_addr()?.port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = origin.accept().await {
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let mut buf = [0u8; 4096];
                    if stream.read(&mut buf).await.unwrap_or(0) > 0 {
                        let _ = stream.write_all(b"pong").await;
                    }
                });
            }
        });
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let mut config: ChainConfig = serde_json::from_value(serde_json::json!({
            "allow": [format!("{ip}:{origin_port}")],
        }))?;
        config.normalize_and_validate("egress")?;
        let policy = merge_policies(
            &[],
            InlinePolicy {
                egress: Some(&config),
                ..Default::default()
            },
        );
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id,
            policy_hash: policy.hash.clone(),
        }]);
        cache.insert_body(&policy.hash.clone(), policy)?;
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: vec![format!("{ip}:{origin_port}")],
                deny: vec![],
            },
        )?;
        let context = Arc::new(
            ForwarderContext::new(cache, denials.clone())
                .with_host_guard(Arc::new(guard))
                .with_kernel_classifier(
                    GuardTable {
                        cgroup_parent: scratch.parent.clone(),
                        ..Default::default()
                    },
                    KernelNet::open(scratch.settings(None))?,
                ),
        );
        let proxies = crate::worker::netd::proxy::EgressProxies::new(context.clone());
        proxies
            .sync(&[crate::worker::netd::proxy::ProxySpec {
                allocation_id,
                attachment: crate::worker::netd::attach::Attachment::Netns {
                    path: netns.clone(),
                },
                listen: None,
            }])
            .await;
        assert!(context.gated_capture_addr(allocation_id).is_some());
        let reconcile = context.clone();
        let leaf = scratch.leaf.clone();
        tokio::task::spawn_blocking(move || {
            reconcile.reconcile_kernel_guard(vec![GuardedAllocation {
                allocation_id,
                cgroup: leaf,
                vip_block: None,
                loopback_ports: vec![],
                resolver: None,
                gated: true,
                host_sockets: true,
            }])
        })
        .await?;
        let script = format!(
            r#"
import os, socket
fd = os.open('{netns}', os.O_RDONLY)
os.setns(fd, os.CLONE_NEWNET)
def send(host, port):
    try:
        s = socket.create_connection((host, port), timeout=3)
        s.sendall(b'ping')
        return s.recv(16).decode() or 'closed'
    except Exception as e:
        return type(e).__name__
print(send('{ip}', {origin_port}), send('{ip}', {other}))
"#,
            netns = netns.display(),
            other = origin_port.wrapping_add(1),
        );
        let scratch_ref = &scratch;
        let said = tokio::task::block_in_place(|| scratch_ref.run(&script));
        drop(proxies);
        context.shutdown_kernel();
        let _ = crate::worker::netd::netns::destroy(allocation_id);
        assert_eq!(
            said, "pong closed",
            "allowed through the capture listener; refused there"
        );
        assert!(denials
            .drain()
            .iter()
            .any(|d| d.allocation_id == allocation_id));
        Ok(())
    }
}
