//! The egress proxy inside a gated namespace.
//!
//! A gated workload's namespace has loopback and Legion's listeners, nothing
//! else — so the only way out for traffic that is not a declared upstream is
//! this proxy, at `127.0.0.1:1080` inside the namespace, named by
//! `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` / `LEGION_EGRESS_PROXY`. It
//! speaks the three things the long tail of tools speak:
//!
//! - **HTTP `CONNECT host:port`** — TLS and anything else opaque. Policy is
//!   judged at connect time (host, port); an allowed tunnel is spliced.
//! - **Absolute-URI HTTP** (`GET http://host/path HTTP/1.1`) — plaintext
//!   HTTP through a forward proxy. Policy sees the request head too. The
//!   request is rewritten to origin form and forwarded with
//!   `Connection: close`, so every request is one connection and gets its
//!   own verdict.
//! - **SOCKS5 CONNECT** (no auth) — for tools that prefer `ALL_PROXY`.
//!   Domain-name targets are judged by name; IPv4/IPv6 targets by address.
//!
//! Every denial is a `403` (HTTP) or a SOCKS `connection not allowed` reply,
//! recorded for the heartbeat with the deciding rule, exactly like a refused
//! upstream dial. Dialing happens from the host namespace — the worker's —
//! which is what makes the namespace a boundary and the proxy the door.

use crate::prelude::*;
use crate::structs::netpolicy::{ConnectFacts, RequestFacts, Verdict, PROTOCOL_HTTP, PROTOCOL_TCP};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::metrics as net_metrics;
use crate::worker::netd::netns;
use crate::worker::netd::policy::egress_connect_verdict;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// Where the proxy listens inside every gated namespace.
pub const PROXY_ADDR: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
pub const PROXY_PORT: u16 = 1080;
const DIAL_TIMEOUT: Duration = Duration::from_secs(2);
/// Largest request head read before deciding; more than this is not a
/// proxy request a task should be sending.
const MAX_HEAD: usize = 16 * 1024;

/// One gated allocation's proxy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxySpec {
    pub allocation_id: i64,
    /// The namespace to bind inside; `None` binds in the host namespace
    /// (tests, and cooperative mode on hosts without namespaces).
    pub netns: Option<PathBuf>,
    /// Bind here instead of `127.0.0.1:1080` (tests).
    pub listen: Option<SocketAddr>,
}

struct Listener {
    spec: ProxySpec,
    task: Option<JoinHandle<()>>,
    addr: Option<SocketAddr>,
}

/// The node's egress proxies, one per gated allocation, reconciled from the
/// desired set like every other datapath listener.
pub struct EgressProxies {
    context: Arc<ForwarderContext>,
    listeners: Mutex<HashMap<i64, Listener>>,
}

impl EgressProxies {
    pub fn new(context: Arc<ForwarderContext>) -> Self {
        EgressProxies {
            context,
            listeners: Mutex::new(HashMap::new()),
        }
    }

    pub async fn sync(&self, desired: &[ProxySpec]) {
        let mut wanted: HashMap<i64, ProxySpec> = HashMap::new();
        for spec in desired {
            wanted.insert(spec.allocation_id, spec.clone());
        }
        let mut stopped = Vec::new();
        let mut pending = Vec::new();
        {
            let mut listeners = lock(&self.listeners);
            listeners.retain(|id, listener| {
                if wanted.get(id).is_some_and(|spec| *spec == listener.spec) {
                    return true;
                }
                if let Some(task) = listener.task.take() {
                    stopped.push(task);
                }
                false
            });
            for (id, spec) in wanted {
                match listeners.get(&id) {
                    Some(listener) if listener.task.as_ref().is_some_and(|t| !t.is_finished()) => {}
                    _ => pending.push(spec),
                }
            }
        }
        for task in stopped {
            task.abort();
            let _ = task.await;
        }
        for spec in pending {
            self.start(spec).await;
        }
    }

    /// The bound address of an allocation's proxy, when it is serving.
    pub fn address(&self, allocation_id: i64) -> Option<SocketAddr> {
        let listeners = lock(&self.listeners);
        let listener = listeners.get(&allocation_id)?;
        let running = listener.task.as_ref().is_some_and(|t| !t.is_finished());
        running.then_some(listener.addr).flatten()
    }

    async fn start(&self, spec: ProxySpec) {
        let addr = spec
            .listen
            .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(PROXY_ADDR), PROXY_PORT));
        match netns::bind_tcp(spec.netns.clone(), addr).await {
            Ok(listener) => {
                let bound = listener.local_addr().ok();
                debug!(allocation_id = spec.allocation_id, addr = ?bound, "Started egress proxy");
                let shared = Arc::new(Proxy {
                    allocation_id: spec.allocation_id,
                    gated: spec.netns.is_some(),
                    context: self.context.clone(),
                });
                let task = tokio::spawn(run(listener, shared));
                lock(&self.listeners).insert(
                    spec.allocation_id,
                    Listener {
                        spec,
                        task: Some(task),
                        addr: bound,
                    },
                );
            }
            Err(err) => {
                warn!(
                    allocation_id = spec.allocation_id,
                    error = ?err,
                    "Failed to bind egress proxy; retrying on the next heartbeat"
                );
                lock(&self.listeners).insert(
                    spec.allocation_id,
                    Listener {
                        spec,
                        task: None,
                        addr: None,
                    },
                );
            }
        }
    }
}

impl Drop for EgressProxies {
    fn drop(&mut self) {
        for listener in lock(&self.listeners).values_mut() {
            if let Some(task) = listener.task.take() {
                task.abort();
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

struct Proxy {
    allocation_id: i64,
    /// A gated proxy fails closed while its policy is still pending or its
    /// edge-boot gate is shut.
    gated: bool,
    context: Arc<ForwarderContext>,
}

async fn run(listener: TcpListener, proxy: Arc<Proxy>) {
    loop {
        let (client, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                debug!(error = ?err, "Egress proxy accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let proxy = proxy.clone();
        tokio::spawn(async move {
            handle(client, proxy).await;
        });
    }
}

/// An HTTP request head as policy sees it: method, path, lower-cased headers.
type RequestHead<'a> = (&'a str, &'a str, &'a [(String, String)]);

/// Policy for one destination. `Err(source, reason)` means deny.
fn judge(
    proxy: &Proxy,
    host: Option<&str>,
    ip: Option<IpAddr>,
    port: u16,
    request: Option<RequestHead<'_>>,
) -> Result<(), (String, Option<String>)> {
    if proxy.gated && netns::gate_is_shut(proxy.allocation_id) {
        return Err((
            "gate".to_string(),
            Some("network gate is shut until the placement is ratified".into()),
        ));
    }
    let policy = proxy.context.policy_for(proxy.allocation_id);
    if proxy.gated && policy.is_none() && !proxy.context.policy_known(proxy.allocation_id) {
        return Err((
            "policy-pending".to_string(),
            Some("network policy has not arrived yet".into()),
        ));
    }
    let verdict = match (policy.as_ref().and_then(|p| p.egress.as_ref()), request) {
        (Some(chain), Some((method, path, headers))) if chain.is_http() => {
            crate::structs::netpolicy::evaluate_request(
                chain,
                &RequestFacts {
                    host,
                    ip,
                    port,
                    identity: None,
                    method,
                    path,
                    headers,
                },
            )
        }
        _ => egress_connect_verdict(
            policy.as_deref(),
            &ConnectFacts {
                host,
                ip,
                port,
                identity: None,
            },
        ),
    };
    match verdict {
        Verdict::Allow | Verdict::Defer => Ok(()),
        Verdict::Deny { source, reason } => Err((source, reason)),
    }
}

async fn handle(client: TcpStream, proxy: Arc<Proxy>) {
    let mut first = [0u8; 1];
    match client.peek(&mut first).await {
        Ok(1) if first[0] == 0x05 => handle_socks5(client, proxy).await,
        Ok(1) => handle_http(client, proxy).await,
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// HTTP: CONNECT and absolute-URI requests
// ---------------------------------------------------------------------------

async fn read_head(client: &mut TcpStream) -> Option<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = client.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            return Some(buf);
        }
        if buf.len() > MAX_HEAD {
            return None;
        }
    }
}

fn split_host_port(target: &str, default_port: u16) -> (String, u16) {
    let target = target.trim();
    if let Some(rest) = target.strip_prefix('[') {
        if let Some((host, tail)) = rest.split_once(']') {
            let port = tail
                .strip_prefix(':')
                .and_then(|p| p.parse().ok())
                .unwrap_or(default_port);
            return (host.to_ascii_lowercase(), port);
        }
    }
    match target.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => match port.parse::<u16>() {
            Ok(port) => (host.to_ascii_lowercase(), port),
            Err(_) => (target.to_ascii_lowercase(), default_port),
        },
        _ => (target.to_ascii_lowercase(), default_port),
    }
}

async fn deny_http(client: &mut TcpStream, source: &str, reason: Option<&str>, destination: &str) {
    let body = json!({
        "legion": {
            "denied": true,
            "direction": "egress",
            "destination": destination,
            "source": source,
            "reason": reason.unwrap_or_default(),
        }
    })
    .to_string();
    let mut response = format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nLegion-Denied-By: {source}\r\n"
    );
    if let Some(reason) = reason {
        response.push_str(&format!(
            "Legion-Denied-Reason: {}\r\n",
            reason.replace(['\r', '\n'], " ")
        ));
    }
    response.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    ));
    let _ = client.write_all(response.as_bytes()).await;
    let _ = client.shutdown().await;
}

async fn handle_http(mut client: TcpStream, proxy: Arc<Proxy>) {
    let Some(head) = read_head(&mut client).await else {
        return;
    };
    let head_end = head
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
        .unwrap_or(head.len());
    let text = String::from_utf8_lossy(&head[..head_end]).to_string();
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_ascii_uppercase();
    let target = parts.next().unwrap_or_default().to_string();
    let version = parts.next().unwrap_or("HTTP/1.1").to_string();
    let headers: Vec<(String, String)> = lines
        .filter(|l| !l.is_empty())
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();

    if method == "CONNECT" {
        let (host, port) = split_host_port(&target, 443);
        let destination = format!("{host}:{port}");
        let ip = host.parse::<IpAddr>().ok();
        if let Err((source, reason)) = judge(&proxy, Some(&host), ip, port, None) {
            net_metrics::connection("egress", PROTOCOL_TCP, "deny");
            proxy.context.record_denial(
                proxy.allocation_id,
                "egress",
                PROTOCOL_TCP,
                &destination,
                &source,
                reason.as_deref(),
                "CONNECT",
                "",
            );
            deny_http(&mut client, &source, reason.as_deref(), &destination).await;
            return;
        }
        let upstream = match tokio::time::timeout(
            DIAL_TIMEOUT,
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            _ => {
                net_metrics::connection("egress", PROTOCOL_TCP, "error");
                let _ = client
                    .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
                return;
            }
        };
        if client
            .write_all(format!("{version} 200 Connection Established\r\n\r\n").as_bytes())
            .await
            .is_err()
        {
            return;
        }
        net_metrics::connection("egress", PROTOCOL_TCP, "allow");
        splice(client, upstream, &head[head_end..]).await;
        return;
    }

    // Absolute-URI request: GET http://host[:port]/path HTTP/1.1
    let Some((scheme, rest)) = target.split_once("://") else {
        let _ = client
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await;
        return;
    };
    let (authority, path) = match rest.find('/') {
        Some(slash) => (&rest[..slash], &rest[slash..]),
        None => (rest, "/"),
    };
    let default_port = if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    };
    let (host, port) = split_host_port(authority, default_port);
    let destination = format!("{host}:{port}");
    let ip = host.parse::<IpAddr>().ok();
    let path_only = path.split('?').next().unwrap_or("/");
    if let Err((source, reason)) = judge(
        &proxy,
        Some(&host),
        ip,
        port,
        Some((&method, path_only, &headers)),
    ) {
        net_metrics::connection("egress", PROTOCOL_HTTP, "deny");
        proxy.context.record_denial(
            proxy.allocation_id,
            "egress",
            PROTOCOL_HTTP,
            &destination,
            &source,
            reason.as_deref(),
            &method,
            path_only,
        );
        deny_http(&mut client, &source, reason.as_deref(), &destination).await;
        return;
    }
    let upstream =
        match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect((host.as_str(), port))).await {
            Ok(Ok(stream)) => stream,
            _ => {
                net_metrics::connection("egress", PROTOCOL_HTTP, "error");
                let _ = client
                .write_all(
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
                return;
            }
        };
    // Origin form, one request per connection.
    let mut rewritten = format!("{method} {path} {version}\r\n");
    let mut saw_host = false;
    for (name, value) in &headers {
        if name == "proxy-connection" || name == "connection" || name == "keep-alive" {
            continue;
        }
        if name == "host" {
            saw_host = true;
        }
        rewritten.push_str(&format!("{name}: {value}\r\n"));
    }
    if !saw_host {
        rewritten.push_str(&format!("host: {authority}\r\n"));
    }
    rewritten.push_str("connection: close\r\n\r\n");
    let mut upstream = upstream;
    if upstream.write_all(rewritten.as_bytes()).await.is_err() {
        return;
    }
    net_metrics::connection("egress", PROTOCOL_HTTP, "allow");
    splice(client, upstream, &head[head_end..]).await;
}

/// Write any bytes read past the head to the upstream, then splice.
async fn splice(mut client: TcpStream, mut upstream: TcpStream, leftover: &[u8]) {
    if !leftover.is_empty() && upstream.write_all(leftover).await.is_err() {
        return;
    }
    match tokio::io::copy_bidirectional(&mut client, &mut upstream).await {
        Ok((up, down)) => {
            net_metrics::bytes("egress", up);
            net_metrics::bytes("ingress", down);
        }
        Err(err) => debug!(error = ?err, "Egress proxy tunnel ended with an error"),
    }
}

// ---------------------------------------------------------------------------
// SOCKS5 CONNECT
// ---------------------------------------------------------------------------

async fn handle_socks5(mut client: TcpStream, proxy: Arc<Proxy>) {
    // Greeting: VER NMETHODS METHODS...
    let mut hdr = [0u8; 2];
    if client.read_exact(&mut hdr).await.is_err() || hdr[0] != 0x05 {
        return;
    }
    let mut methods = vec![0u8; usize::from(hdr[1])];
    if client.read_exact(&mut methods).await.is_err() {
        return;
    }
    // No authentication.
    if client.write_all(&[0x05, 0x00]).await.is_err() {
        return;
    }
    // Request: VER CMD RSV ATYP DST.ADDR DST.PORT
    let mut req = [0u8; 4];
    if client.read_exact(&mut req).await.is_err() || req[0] != 0x05 {
        return;
    }
    if req[1] != 0x01 {
        // Only CONNECT; BIND and UDP ASSOCIATE are the UDP phase.
        let _ = client
            .write_all(&[0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await;
        return;
    }
    let (host, ip): (String, Option<IpAddr>) = match req[3] {
        0x01 => {
            let mut a = [0u8; 4];
            if client.read_exact(&mut a).await.is_err() {
                return;
            }
            let ip = IpAddr::V4(Ipv4Addr::from(a));
            (ip.to_string(), Some(ip))
        }
        0x03 => {
            let mut len = [0u8; 1];
            if client.read_exact(&mut len).await.is_err() {
                return;
            }
            let mut name = vec![0u8; usize::from(len[0])];
            if client.read_exact(&mut name).await.is_err() {
                return;
            }
            (String::from_utf8_lossy(&name).to_ascii_lowercase(), None)
        }
        0x04 => {
            let mut a = [0u8; 16];
            if client.read_exact(&mut a).await.is_err() {
                return;
            }
            let ip = IpAddr::V6(std::net::Ipv6Addr::from(a));
            (ip.to_string(), Some(ip))
        }
        _ => {
            let _ = client
                .write_all(&[0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await;
            return;
        }
    };
    let mut port_bytes = [0u8; 2];
    if client.read_exact(&mut port_bytes).await.is_err() {
        return;
    }
    let port = u16::from_be_bytes(port_bytes);
    let destination = format!("{host}:{port}");
    if let Err((source, reason)) = judge(&proxy, Some(&host), ip, port, None) {
        net_metrics::connection("egress", PROTOCOL_TCP, "deny");
        proxy.context.record_denial(
            proxy.allocation_id,
            "egress",
            PROTOCOL_TCP,
            &destination,
            &source,
            reason.as_deref(),
            "",
            "",
        );
        // 0x02: connection not allowed by ruleset.
        let _ = client
            .write_all(&[0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await;
        return;
    }
    let upstream =
        match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect((host.as_str(), port))).await {
            Ok(Ok(stream)) => stream,
            _ => {
                net_metrics::connection("egress", PROTOCOL_TCP, "error");
                let _ = client
                    .write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                    .await;
                return;
            }
        };
    if client
        .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await
        .is_err()
    {
        return;
    }
    net_metrics::connection("egress", PROTOCOL_TCP, "allow");
    splice(client, upstream, &[]).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use crate::structs::AllocNetPolicy;
    use crate::worker::netd::{Denials, NetPolicyCache};

    async fn origin() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let text = String::from_utf8_lossy(&buf[..n]).to_string();
                    let first = text.lines().next().unwrap_or_default().to_string();
                    let body = format!("saw {first}");
                    let _ = stream
                        .write_all(
                            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
                                .as_bytes(),
                        )
                        .await;
                });
            }
        });
        port
    }

    async fn proxies_with_policy(allow: &[&str]) -> (Arc<EgressProxies>, Arc<Denials>, SocketAddr) {
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let mut object: NetPolicyObject = serde_json::from_value(json!({
            "name": "p", "enforcement": "require",
            "egress": { "protocol": "http", "allow": allow }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let policy = merge_policies(&[object], InlinePolicy::default());
        let hash = policy.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 5,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
        let context = Arc::new(ForwarderContext::new(cache, denials.clone()));
        let proxies = Arc::new(EgressProxies::new(context));
        proxies
            .sync(&[ProxySpec {
                allocation_id: 5,
                netns: None,
                listen: Some("127.0.0.1:0".parse().unwrap()),
            }])
            .await;
        let addr = proxies.address(5).expect("proxy bound");
        (proxies, denials, addr)
    }

    async fn raw(addr: SocketAddr, bytes: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(bytes).await.unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        String::from_utf8_lossy(&out).to_string()
    }

    #[tokio::test]
    async fn http_connect_and_absolute_uri_are_judged_and_forwarded() {
        let origin_port = origin().await;
        let (_proxies, denials, addr) = proxies_with_policy(&["127.0.0.1/ok/**"]).await;

        // Absolute-URI GET to an allowed path is forwarded in origin form.
        let response = raw(addr, format!("GET http://127.0.0.1:{origin_port}/ok/x HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\n\r\n").as_bytes()).await;
        assert!(response.contains("200 OK"), "{response}");
        assert!(response.contains("saw GET /ok/x HTTP/1.1"), "{response}");

        // A denied path gets a 403 naming the rule.
        let response = raw(
            addr,
            format!("GET http://127.0.0.1:{origin_port}/secret HTTP/1.1\r\nHost: x\r\n\r\n")
                .as_bytes(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains("legion-denied-by: fallthrough"),
            "{response}"
        );

        // CONNECT to an allowed host:port tunnels; to a denied one is refused.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(format!("CONNECT 127.0.0.1:{origin_port} HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\n\r\n").as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 256];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("200 Connection Established"));
        stream
            .write_all(b"GET /ok/tunnel HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        assert!(String::from_utf8_lossy(&out).contains("saw GET /ok/tunnel"));

        let response = raw(
            addr,
            b"CONNECT evil.example:443 HTTP/1.1\r\nHost: evil.example:443\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");

        let drained = denials.drain();
        assert!(
            drained
                .iter()
                .any(|d| d.destination == "evil.example:443" && d.method == "CONNECT"),
            "{drained:?}"
        );
        assert!(drained.iter().any(|d| d.path == "/secret"), "{drained:?}");
    }

    #[tokio::test]
    async fn socks5_connect_is_judged_by_name() {
        let origin_port = origin().await;
        let (_proxies, _denials, addr) = proxies_with_policy(&["127.0.0.1"]).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut reply = [0u8; 2];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [0x05, 0x00]);
        // CONNECT 127.0.0.1:<origin> by IPv4.
        let mut req = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
        req.extend_from_slice(&origin_port.to_be_bytes());
        stream.write_all(&req).await.unwrap();
        let mut reply = [0u8; 10];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply[1], 0x00, "granted");
        stream
            .write_all(b"GET /via-socks HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        assert!(String::from_utf8_lossy(&out).contains("saw GET /via-socks"));

        // A domain name the policy does not allow is refused with 0x02.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut reply = [0u8; 2];
        stream.read_exact(&mut reply).await.unwrap();
        let name = b"evil.example";
        let mut req = vec![0x05, 0x01, 0x00, 0x03, name.len() as u8];
        req.extend_from_slice(name);
        req.extend_from_slice(&443u16.to_be_bytes());
        stream.write_all(&req).await.unwrap();
        let mut reply = [0u8; 10];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply[1], 0x02, "connection not allowed by ruleset");
    }
}
