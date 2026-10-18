//! The egress proxy inside a gated namespace.
//!
//! A gated workload's namespace has loopback and Legion's listeners, nothing
//! else — so the only way out for traffic that is not a declared upstream is
//! this proxy, at `127.0.0.1:1080` inside the namespace, named by
//! `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` / `LEGION_EGRESS_PROXY`. It
//! speaks the three things the long tail of tools speak (HTTP parsed by
//! hyper, SOCKS5 by hand — it is a dozen bytes):
//!
//! - **HTTP `CONNECT host:port`** — TLS and anything else opaque. Policy is
//!   judged at connect time (host, port); an allowed tunnel is spliced.
//! - **Absolute-URI HTTP** (`GET http://host/path HTTP/1.1`) — plaintext
//!   HTTP through a forward proxy. Policy sees the request head too. Every
//!   request on the client's connection — kept alive or pipelined — gets its
//!   own verdict, and goes upstream in origin form on a connection of its
//!   own.
//! - **SOCKS5 CONNECT** (no auth) — for tools that prefer `ALL_PROXY`.
//!   Domain-name targets are judged by name; IPv4/IPv6 targets by address.
//!
//! Every denial is a `403` (HTTP) or a SOCKS `connection not allowed` reply,
//! recorded for the heartbeat with the deciding rule, exactly like a refused
//! upstream dial. Every accepted connection is tracked in the activity
//! ledger (`super::activity`), so idle detection sees an open egress
//! connection and its bytes as they move, not only when the splice ends. Dialing happens from the host namespace — the worker's —
//! which is what makes the namespace a boundary and the proxy the door.

use crate::prelude::*;
use crate::structs::netpolicy::{ConnectFacts, RequestFacts, Verdict, PROTOCOL_HTTP, PROTOCOL_TCP};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::attach::Attachment;
use crate::worker::netd::guard::{self, GuardRefusal};
use crate::worker::netd::handlers::{self, HopFacts, HopOutcome, ProxyTarget};
use crate::worker::netd::http;
use crate::worker::netd::listen::{dial_tcp, spawn_accept_loop, ListenAddr};
use crate::worker::netd::metrics as net_metrics;
use crate::worker::netd::netns;
use crate::worker::netd::policy::{egress_connect_verdict, gated_egress_connect_verdict};
use crate::worker::netd::proxyproto::Prefixed;
use crate::worker::netd::splice::SpliceStream;

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::Incoming;
use hyper::{header, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
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
    /// How the allocation is attached: the proxy binds `127.0.0.1:1080`
    /// inside its namespace, or on a gated sandbox's vsock host socket for
    /// port 1080. A gated attachment fails closed while policy is pending or
    /// the edge-boot gate is shut; [`Attachment::Host`] binds in the host
    /// namespace (tests, and cooperative mode on hosts without namespaces).
    pub attachment: Attachment,
    /// Bind here instead of `127.0.0.1:1080` (tests).
    pub listen: Option<SocketAddr>,
}

impl ProxySpec {
    /// A gated listener fails closed while policy is pending or the boot
    /// gate is shut.
    pub fn gated(&self) -> bool {
        self.attachment.is_gated()
    }

    /// Where the proxy binds.
    pub fn listen_addr(&self) -> ListenAddr {
        let addr = self
            .listen
            .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(PROXY_ADDR), PROXY_PORT));
        self.attachment.listen(addr.ip(), addr.port())
    }
}

struct Listener {
    spec: ProxySpec,
    task: Option<JoinHandle<()>>,
    addr: Option<SocketAddr>,
}

impl Listener {
    fn running(&self) -> bool {
        self.task.as_ref().is_some_and(|t| !t.is_finished())
    }

    /// Running, and for a vsock host socket still at its path. A failed
    /// start removes the allocation's directory, socket and all, and a
    /// listener on an unlinked path serves nothing.
    fn serving(&self) -> bool {
        self.running()
            && match self.spec.listen_addr() {
                ListenAddr::Unix(path) => path.exists(),
                ListenAddr::Tcp { .. } => true,
            }
    }
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
                    Some(listener) if listener.running() => {}
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

    /// Bind one allocation's proxy now, ahead of the next [`Self::sync`],
    /// unless it already serves exactly this spec, at its socket path. True
    /// when it is serving afterward.
    ///
    /// The launch path calls this for a gated sandbox that arrived by push
    /// or the work stream, which starts the task before any heartbeat binds
    /// its datapath: a restored guest resumes only once this socket exists.
    /// The next `sync` keeps the listener when its desired set carries the
    /// same spec, and stops it otherwise, so the heartbeat stays the owner.
    /// Unlike `sync`, it rebinds a listener whose socket file is gone: the
    /// start it precedes is about to wait on that file. `sync` leaves such
    /// a listener alone, so it never recreates the directory of an
    /// allocation the node already tore down.
    pub async fn ensure(&self, spec: ProxySpec) -> bool {
        let allocation_id = spec.allocation_id;
        let serving = {
            let listeners = lock(&self.listeners);
            listeners
                .get(&allocation_id)
                .is_some_and(|listener| listener.spec == spec && listener.serving())
        };
        if !serving {
            self.start(spec).await;
        }
        self.is_running(allocation_id)
    }

    /// Record an allocation's listener, stopping any it replaces. A `sync`
    /// and an [`Self::ensure`] can both start the same allocation, and a
    /// replaced listener must not keep serving a socket path it no longer
    /// owns.
    fn install(&self, listener: Listener) {
        let replaced = lock(&self.listeners).insert(listener.spec.allocation_id, listener);
        if let Some(task) = replaced.and_then(|mut old| old.task.take()) {
            task.abort();
        }
    }

    /// The bound address of an allocation's proxy, when it is serving on a
    /// TCP port (a vsock-backed proxy has none; see [`Self::is_running`]).
    pub fn address(&self, allocation_id: i64) -> Option<SocketAddr> {
        let listeners = lock(&self.listeners);
        let listener = listeners.get(&allocation_id)?;
        let running = listener.task.as_ref().is_some_and(|t| !t.is_finished());
        running.then_some(listener.addr).flatten()
    }

    /// Whether an allocation's proxy is serving, on a port or a socket.
    pub fn is_running(&self, allocation_id: i64) -> bool {
        let listeners = lock(&self.listeners);
        listeners
            .get(&allocation_id)
            .is_some_and(|l| l.task.as_ref().is_some_and(|t| !t.is_finished()))
    }

    async fn start(&self, spec: ProxySpec) {
        let listen = spec.listen_addr();
        match listen.bind().await {
            Ok(listener) => {
                let bound = listener.local_addr();
                debug!(allocation_id = spec.allocation_id, listen = %listen, "Started egress proxy");
                let shared = Arc::new(Proxy {
                    allocation_id: spec.allocation_id,
                    gated: spec.gated(),
                    context: self.context.clone(),
                });
                let task = spawn_accept_loop(listener, "egress proxy", move |client, peer| {
                    // Counted as open (and its bytes as activity, as they
                    // move) for the idle policy's `connections` and
                    // `network` signals, until it closes: a quiet model call
                    // through the proxy keeps the task awake when it asks
                    // for `connections`. A gated sandbox's egress is the one
                    // datapath traffic an idle policy can see for it.
                    let client = shared
                        .context
                        .activity()
                        .track(shared.allocation_id, client);
                    handle(client, peer, shared.clone())
                });
                self.install(Listener {
                    spec,
                    task: Some(task),
                    addr: bound,
                });
            }
            Err(err) => {
                warn!(
                    allocation_id = spec.allocation_id,
                    listen = %listen,
                    error = ?err,
                    "Failed to bind egress proxy; retrying on the next heartbeat"
                );
                self.install(Listener {
                    spec,
                    task: None,
                    addr: None,
                });
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

/// An HTTP request head as policy sees it: method, path, lower-cased headers.
type RequestHead<'a> = (&'a str, &'a str, &'a [(String, String)]);

/// Why a destination may not be dialed: the verdict's source and reason,
/// which a `403` (or a SOCKS `0x02`) carries.
enum Refused {
    Denied(String, Option<String>),
}

/// Judges one destination and returns the addresses that may be dialed.
///
/// Order: the boot gate, then the host guard (which needs the name
/// resolved), then pending policy, then the egress chain, evaluated once per
/// resolved address with the name still attached, so a name rule matches
/// the name and a CIDR rule matches the address. A host-guard denial is
/// final: no rule below it can allow what it refused. The caller dials only
/// the returned addresses and never resolves the name again
/// (worker-plane-auth plan §6.11).
async fn judge(
    proxy: &Proxy,
    host: &str,
    port: u16,
    request: Option<RequestHead<'_>>,
) -> Result<Vec<SocketAddr>, Refused> {
    if proxy.gated && netns::gate_is_shut(proxy.allocation_id) {
        return Err(Refused::Denied(
            "gate".to_string(),
            Some("network gate is shut until the placement is ratified".into()),
        ));
    }
    let addrs = match guard::resolve_and_guard(
        proxy.context.resolver(),
        proxy.context.host_guard(),
        host,
        port,
    )
    .await
    {
        Ok(addrs) => addrs,
        Err(GuardRefusal::Denied(reason)) => {
            return Err(Refused::Denied(
                crate::structs::host_guard::HOST_GUARD_SOURCE.to_string(),
                Some(reason),
            ))
        }
        Err(GuardRefusal::Unresolved(reason)) => {
            debug!(allocation_id = proxy.allocation_id, %reason, "Egress proxy could not resolve the destination");
            // Nothing to dial. A name the policy refuses still reads as that
            // refusal (more useful than a 502); anything else is unreachable.
            let policy = proxy.context.policy_for(proxy.allocation_id);
            if proxy.gated && policy.is_none() && !proxy.context.policy_known(proxy.allocation_id) {
                return Err(Refused::Denied(
                    "policy-pending".to_string(),
                    Some("network policy has not arrived yet".into()),
                ));
            }
            let verdict = connect_verdict(
                proxy,
                policy.as_deref(),
                &ConnectFacts {
                    host: Some(host),
                    ip: None,
                    port,
                    identity: None,
                },
            );
            // Allowed by name: nothing this node can dial (the caller's
            // direct dial fails with a 502), but a `proxy` handler may still
            // take the flow and resolve the name on its own side.
            return match verdict {
                Verdict::Deny { source, reason } => Err(Refused::Denied(source, reason)),
                _ => Ok(Vec::new()),
            };
        }
    };
    let policy = proxy.context.policy_for(proxy.allocation_id);
    if proxy.gated && policy.is_none() && !proxy.context.policy_known(proxy.allocation_id) {
        return Err(Refused::Denied(
            "policy-pending".to_string(),
            Some("network policy has not arrived yet".into()),
        ));
    }
    let mut first_denial: Option<(String, Option<String>)> = None;
    let mut allowed = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let ip = Some(addr.ip());
        let verdict = match (policy.as_ref().and_then(|p| p.egress.as_ref()), request) {
            (Some(chain), Some((method, path, headers))) if chain.is_http() => {
                crate::structs::netpolicy::evaluate_request(
                    chain,
                    &RequestFacts {
                        host: Some(host),
                        ip,
                        port,
                        identity: None,
                        method,
                        path,
                        headers,
                    },
                )
            }
            _ => connect_verdict(
                proxy,
                policy.as_deref(),
                &ConnectFacts {
                    host: Some(host),
                    ip,
                    port,
                    identity: None,
                },
            ),
        };
        match verdict {
            Verdict::Allow | Verdict::Defer => allowed.push(addr),
            Verdict::Deny { source, reason } => {
                first_denial.get_or_insert((source, reason));
            }
        }
    }
    if allowed.is_empty() {
        let (source, reason) = first_denial.unwrap_or_else(|| ("policy".into(), None));
        return Err(Refused::Denied(source, reason));
    }
    Ok(allowed)
}

/// The connect-time verdict: a gated allocation that no egress chain
/// covers denies (`gated-default`); a cooperative one allows with
/// attribution.
fn connect_verdict(
    proxy: &Proxy,
    policy: Option<&crate::structs::netpolicy::EffectivePolicy>,
    facts: &ConnectFacts<'_>,
) -> Verdict {
    if proxy.gated {
        gated_egress_connect_verdict(policy, facts)
    } else {
        egress_connect_verdict(policy, facts)
    }
}

/// Dials the judged addresses in order, with the dial timeout. `None` for
/// an empty list (a name that did not resolve here).
async fn dial(addrs: &[SocketAddr]) -> Option<TcpStream> {
    if addrs.is_empty() {
        return None;
    }
    dial_tcp(addrs, DIAL_TIMEOUT).await.ok()
}

/// The external hops on the allocation's effective egress chain.
async fn hops(proxy: &Arc<Proxy>, facts: &HopFacts<'_>) -> HopOutcome {
    let hops = handlers::hops_for(&proxy.context, proxy.allocation_id, "egress", None, &[]);
    if hops.is_empty() {
        return HopOutcome::Allow;
    }
    handlers::run(&proxy.context, &hops, facts).await
}

/// Dial a `proxy` handler's target and start it with the PROXY v2 header.
async fn dial_proxy_target(
    proxy: &Arc<Proxy>,
    target: &ProxyTarget,
    facts: &HopFacts<'_>,
    client_addr: Option<SocketAddr>,
) -> Option<TcpStream> {
    // A `proxy` handler's target is part of the allocation's policy (a job
    // may append handlers), so the host guard judges it like any other
    // destination the allocation names.
    let mut stream = match guard::dial_guarded(&proxy.context, &target.addr, DIAL_TIMEOUT).await {
        Ok(stream) => stream,
        Err(err) => {
            proxy.context.note_handler(
                proxy.allocation_id,
                &target.source,
                crate::structs::netpolicy::HANDLER_PROXY,
                &target.addr,
                Err(format!("dial {}: {err}", target.addr)),
            );
            return None;
        }
    };
    if target.proxy_protocol {
        let destination = handlers::resolve_addr(facts.destination).await;
        let header = handlers::proxy_header(&proxy.context, facts, client_addr, destination);
        stream.write_all(&header).await.ok()?;
    }
    Some(stream)
}

async fn handle<S>(mut client: S, peer: Option<SocketAddr>, proxy: Arc<Proxy>)
where
    S: SpliceStream + Send + 'static,
{
    // The first byte says which protocol the client speaks; it is handed
    // back to the handler in front of the stream.
    let mut first = [0u8; 1];
    let n = match client.read(&mut first).await {
        Ok(n) => n,
        Err(_) => return,
    };
    if n == 0 {
        return;
    }
    let client = Prefixed::new(first.to_vec(), client);
    if first[0] == 0x05 {
        handle_socks5(client, peer, proxy).await
    } else {
        handle_http(client, peer, proxy).await
    }
}

// ---------------------------------------------------------------------------
// HTTP: CONNECT and absolute-URI requests, parsed by hyper
// ---------------------------------------------------------------------------

type ProxyBody = BoxBody<Bytes, hyper::Error>;

fn full_body(bytes: impl Into<Bytes>) -> ProxyBody {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed()
}

/// A local answer with no body; the client connection closes after it.
fn status_response(status: StatusCode) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header(header::CONNECTION, "close")
        .body(full_body(Bytes::new()))
        .expect("static response")
}

/// The `403` for a refused request. Its body and its `Legion-Denied-By` /
/// `Legion-Denied-Reason` headers name the rule, so a client (an agent in a
/// sandbox, say) can read why instead of retrying.
fn denied_response(source: &str, reason: Option<&str>, destination: &str) -> Response<ProxyBody> {
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
    let mut response = Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONNECTION, "close")
        .body(full_body(body))
        .expect("static response");
    let headers = response.headers_mut();
    if let Ok(value) = header::HeaderValue::from_str(source) {
        headers.insert("legion-denied-by", value);
    }
    if let Some(reason) = reason {
        if let Ok(value) = header::HeaderValue::from_str(&reason.replace(['\r', '\n'], " ")) {
            headers.insert("legion-denied-reason", value);
        }
    }
    response
}

/// Record a refused request — metrics, the service graph, the denial
/// ledger — and answer it with a `403`. `request` is the method and path
/// (`CONNECT` and no path for a tunnel).
fn refuse(
    proxy: &Proxy,
    host: &str,
    protocol: &'static str,
    destination: &str,
    (source, reason): (String, Option<String>),
    (method, path): (&str, &str),
) -> Response<ProxyBody> {
    net_metrics::connection("egress", protocol, "deny");
    proxy
        .context
        .edges()
        .record_denied(proxy.allocation_id, host, protocol);
    proxy.context.record_denial(
        proxy.allocation_id,
        "egress",
        protocol,
        destination,
        &source,
        reason.as_deref(),
        method,
        path,
    );
    denied_response(&source, reason.as_deref(), destination)
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

/// Serve one HTTP proxy connection. Every request on it is judged on its
/// own — a client that pipelines or keeps the connection alive gets a
/// verdict per request, never a pass for everything after the first.
async fn handle_http<S>(client: S, peer: Option<SocketAddr>, proxy: Arc<Proxy>)
where
    S: SpliceStream + Send + 'static,
{
    let service = hyper::service::service_fn(move |request: Request<Incoming>| {
        let proxy = proxy.clone();
        async move { Ok::<_, hyper::Error>(proxy_request::<S>(request, peer, proxy).await) }
    });
    if let Err(err) = hyper::server::conn::http1::Builder::new()
        .preserve_header_case(true)
        .max_buf_size(MAX_HEAD)
        .serve_connection(TokioIo::new(client), service)
        .with_upgrades()
        .await
    {
        debug!(error = ?err, "Egress proxy HTTP connection ended with an error");
    }
}

async fn proxy_request<S>(
    request: Request<Incoming>,
    peer: Option<SocketAddr>,
    proxy: Arc<Proxy>,
) -> Response<ProxyBody>
where
    S: SpliceStream + Send + 'static,
{
    if request.method() == Method::CONNECT {
        connect_tunnel::<S>(request, peer, proxy).await
    } else {
        forward_absolute(request, peer, proxy).await
    }
}

/// `CONNECT host:port`: judged at connect time, then the client's upgraded
/// connection is spliced to the destination. `S` is the client's stream,
/// which hyper hands back after the upgrade so the tunnel splices the
/// socket itself rather than a copy through hyper's buffers.
async fn connect_tunnel<S>(
    request: Request<Incoming>,
    peer: Option<SocketAddr>,
    proxy: Arc<Proxy>,
) -> Response<ProxyBody>
where
    S: SpliceStream + Send + 'static,
{
    let target = request
        .uri()
        .authority()
        .map(|authority| authority.as_str().to_string())
        .unwrap_or_else(|| request.uri().to_string());
    let (host, port) = split_host_port(&target, 443);
    let destination = format!("{host}:{port}");
    let addrs = match judge(&proxy, &host, port, None).await {
        Ok(addrs) => addrs,
        Err(Refused::Denied(source, reason)) => {
            return refuse(
                &proxy,
                &host,
                PROTOCOL_TCP,
                &destination,
                (source, reason),
                ("CONNECT", ""),
            )
        }
    };
    let ip = addrs.first().map(|addr| addr.ip());
    let hop_facts = HopFacts {
        allocation_id: proxy.allocation_id,
        direction: "egress",
        protocol: PROTOCOL_TCP,
        destination: &destination,
        host: Some(&host),
        ip,
        port,
        ..Default::default()
    };
    let upstream = match hops(&proxy, &hop_facts).await {
        HopOutcome::Allow => dial(&addrs).await,
        HopOutcome::Deny { source, reason } => {
            return refuse(
                &proxy,
                &host,
                PROTOCOL_TCP,
                &destination,
                (source, reason),
                ("CONNECT", ""),
            )
        }
        HopOutcome::Proxy(target) => dial_proxy_target(&proxy, &target, &hop_facts, peer).await,
    };
    let Some(upstream) = upstream else {
        net_metrics::connection("egress", PROTOCOL_TCP, "error");
        proxy
            .context
            .edges()
            .record_connection(proxy.allocation_id, &host, None, false);
        return status_response(StatusCode::BAD_GATEWAY);
    };
    net_metrics::connection("egress", PROTOCOL_TCP, "allow");
    proxy
        .context
        .edges()
        .record_connection(proxy.allocation_id, &host, None, true);
    let tunnel = proxy.clone();
    tokio::spawn(async move {
        let upgraded = match hyper::upgrade::on(request).await {
            Ok(upgraded) => upgraded,
            Err(err) => {
                debug!(error = ?err, "Egress proxy CONNECT upgrade failed");
                return;
            }
        };
        // Anything the client sent past the CONNECT head goes upstream
        // first; the rest flows over the client's own socket.
        match upgraded.downcast::<TokioIo<S>>() {
            Ok(parts) => {
                let client = parts.io.into_inner();
                splice(&tunnel, &host, client, upstream, &parts.read_buf).await
            }
            Err(upgraded) => splice(&tunnel, &host, TokioIo::new(upgraded), upstream, &[]).await,
        }
    });
    let mut response = Response::new(full_body(Bytes::new()));
    response
        .extensions_mut()
        .insert(hyper::ext::ReasonPhrase::from_static(
            b"Connection Established",
        ));
    response
}

/// An absolute-URI request (`GET http://host/path`): judged with its head,
/// then sent on a fresh connection in origin form (or, through a `proxy`
/// handler, in absolute form with the Legion attribution headers).
async fn forward_absolute(
    request: Request<Incoming>,
    peer: Option<SocketAddr>,
    proxy: Arc<Proxy>,
) -> Response<ProxyBody> {
    let started = std::time::Instant::now();
    let uri = request.uri().clone();
    let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
        return status_response(StatusCode::BAD_REQUEST);
    };
    let default_port = if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    };
    let (host, port) = split_host_port(authority.as_str(), default_port);
    let destination = format!("{host}:{port}");
    let method = request.method().as_str().to_ascii_uppercase();
    let path_only = if uri.path().is_empty() {
        "/"
    } else {
        uri.path()
    };
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_ascii_lowercase(),
                String::from_utf8_lossy(value.as_bytes()).trim().to_string(),
            )
        })
        .collect();
    let addrs = match judge(&proxy, &host, port, Some((&method, path_only, &headers))).await {
        Ok(addrs) => addrs,
        Err(Refused::Denied(source, reason)) => {
            log_request(
                &proxy,
                &method,
                path_only,
                &destination,
                403,
                "deny",
                Some(&source),
                started,
                0,
            );
            return refuse(
                &proxy,
                &host,
                PROTOCOL_HTTP,
                &destination,
                (source, reason),
                (&method, path_only),
            );
        }
    };
    let ip = addrs.first().map(|addr| addr.ip());
    let hop_facts = HopFacts {
        allocation_id: proxy.allocation_id,
        direction: "egress",
        protocol: PROTOCOL_HTTP,
        destination: &destination,
        host: Some(&host),
        ip,
        port,
        method: Some(&method),
        path: Some(path_only),
        headers: &headers,
        ..Default::default()
    };
    let (upstream, absolute_form, extra) = match hops(&proxy, &hop_facts).await {
        HopOutcome::Allow => match dial(&addrs).await {
            Some(stream) => (stream, false, Vec::new()),
            None => {
                net_metrics::connection("egress", PROTOCOL_HTTP, "error");
                proxy
                    .context
                    .edges()
                    .record_connection(proxy.allocation_id, &host, None, false);
                return status_response(StatusCode::BAD_GATEWAY);
            }
        },
        HopOutcome::Deny { source, reason } => {
            log_request(
                &proxy,
                &method,
                path_only,
                &destination,
                403,
                "deny",
                Some(&source),
                started,
                0,
            );
            return refuse(
                &proxy,
                &host,
                PROTOCOL_HTTP,
                &destination,
                (source, reason),
                (&method, path_only),
            );
        }
        HopOutcome::Proxy(target) => {
            let extra = handlers::legion_headers(&proxy.context, &hop_facts);
            // An HTTP proxy speaks HTTP: no PROXY header, the request itself
            // says where it is going.
            let plain = ProxyTarget {
                proxy_protocol: false,
                ..target
            };
            match dial_proxy_target(&proxy, &plain, &hop_facts, peer).await {
                Some(stream) => (stream, true, extra),
                None => {
                    net_metrics::connection("egress", PROTOCOL_HTTP, "error");
                    return status_response(StatusCode::BAD_GATEWAY);
                }
            }
        }
    };
    let (mut parts, body) = request.into_parts();
    // The chain's `dlp` hops read the body before it leaves, exactly as they
    // do on an HTTP-mode upstream.
    let dlps = chain_dlp(&proxy);
    let mut bytes_in = 0u64;
    let body: ProxyBody = if dlps.is_empty() {
        body.boxed()
    } else {
        match http_body_util::Limited::new(body, http::dlp_cap(&dlps) as usize)
            .collect()
            .await
        {
            Ok(collected) => {
                let bytes = collected.to_bytes();
                bytes_in = bytes.len() as u64;
                if let Some((source, reason)) = http::dlp_check(&dlps, &bytes) {
                    log_request(
                        &proxy,
                        &method,
                        path_only,
                        &destination,
                        403,
                        "deny",
                        Some(&source),
                        started,
                        bytes_in,
                    );
                    return refuse(
                        &proxy,
                        &host,
                        PROTOCOL_HTTP,
                        &destination,
                        (source, Some(reason)),
                        (&method, path_only),
                    );
                }
                full_body(bytes)
            }
            Err(_) => {
                // Over the cap, or the client went away mid-body.
                if dlps.iter().any(|d| d.on_error_deny()) {
                    let source = dlps
                        .first()
                        .map(|d| d.source().to_string())
                        .unwrap_or_default();
                    log_request(
                        &proxy,
                        &method,
                        path_only,
                        &destination,
                        403,
                        "deny",
                        Some(&source),
                        started,
                        0,
                    );
                    return refuse(
                        &proxy,
                        &host,
                        PROTOCOL_HTTP,
                        &destination,
                        (source, Some("request body exceeds dlp maxBytes".into())),
                        (&method, path_only),
                    );
                }
                return status_response(StatusCode::PAYLOAD_TOO_LARGE);
            }
        }
    };
    // Hop-by-hop headers stay on this hop; the upstream connection carries
    // this one request and closes.
    for name in ["proxy-connection", "connection", "keep-alive"] {
        parts.headers.remove(name);
    }
    if !absolute_form {
        if !parts.headers.contains_key(header::HOST) {
            if let Ok(value) = header::HeaderValue::from_str(authority.as_str()) {
                parts.headers.insert(header::HOST, value);
            }
        }
        parts.uri = uri
            .path_and_query()
            .map(|path| path.as_str())
            .unwrap_or("/")
            .parse()
            .unwrap_or_else(|_| hyper::Uri::from_static("/"));
    }
    for (name, value) in extra {
        if let (Ok(name), Ok(value)) = (
            header::HeaderName::from_bytes(name.as_bytes()),
            header::HeaderValue::from_str(&value),
        ) {
            parts.headers.append(name, value);
        }
    }
    parts.headers.insert(
        header::CONNECTION,
        header::HeaderValue::from_static("close"),
    );
    net_metrics::connection("egress", PROTOCOL_HTTP, "allow");
    proxy
        .context
        .edges()
        .record_connection(proxy.allocation_id, &host, None, true);
    let upstream = CountedUpstream::new(upstream, proxy.clone(), host);
    match send_one(upstream, Request::from_parts(parts, body)).await {
        Ok(response) => {
            log_request(
                &proxy,
                &method,
                path_only,
                &destination,
                response.status().as_u16(),
                "allow",
                None,
                started,
                bytes_in,
            );
            response
        }
        Err(err) => {
            debug!(allocation_id = proxy.allocation_id, destination = %destination, error = %err, "Egress proxy request failed");
            log_request(
                &proxy,
                &method,
                path_only,
                &destination,
                502,
                "error",
                None,
                started,
                bytes_in,
            );
            status_response(StatusCode::BAD_GATEWAY)
        }
    }
}

/// The egress chain's `dlp` hops, compiled once per policy body.
fn chain_dlp(proxy: &Proxy) -> Arc<Vec<http::Dlp>> {
    let Some(policy) = proxy.context.policy_for(proxy.allocation_id) else {
        return Arc::new(Vec::new());
    };
    match policy.egress.as_ref() {
        Some(chain) if chain.is_http() => http::chain_body_hops(&policy, chain, "egress").dlp,
        _ => Arc::new(Vec::new()),
    }
}

/// One access-log line for an absolute-URI request through the proxy, when
/// the egress chain asks for one (`accessLog = true` or a `log` handler).
#[allow(clippy::too_many_arguments)]
fn log_request(
    proxy: &Proxy,
    method: &str,
    path: &str,
    destination: &str,
    status: u16,
    verdict: &str,
    source: Option<&str>,
    started: std::time::Instant,
    bytes_in: u64,
) {
    let Some(policy) = proxy.context.policy_for(proxy.allocation_id) else {
        return;
    };
    let Some(chain) = policy.egress.as_ref() else {
        return;
    };
    if !chain.access_log && !http::chain_declares_log(chain) {
        return;
    }
    let Some(dir) = proxy.context.log_dir_for(proxy.allocation_id) else {
        return;
    };
    let identity = proxy.context.identity_for(proxy.allocation_id);
    let line = json!({
        "ts": time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        "allocation": proxy.allocation_id,
        "job": identity.as_ref().map(|i| i.job_id.clone()).unwrap_or_default(),
        "task": identity.as_ref().map(|i| i.task_name.clone()).unwrap_or_default(),
        "labels": identity.as_ref().map(|i| i.labels.clone()).unwrap_or_default(),
        "direction": "egress",
        "protocol": "http",
        "upstream": "egress-proxy",
        "destination": destination,
        "method": method,
        "path": path,
        "status": status,
        "verdict": verdict,
        "source": source.unwrap_or_default(),
        "bytesIn": bytes_in,
        "durationMs": started.elapsed().as_millis() as u64,
    });
    tokio::spawn(http::append_access_line(dir, line));
}

/// Send one request on a fresh upstream connection and hand its response
/// back, minus the hop-by-hop headers that belong to that connection.
async fn send_one(
    upstream: CountedUpstream,
    request: Request<ProxyBody>,
) -> Result<Response<ProxyBody>, hyper::Error> {
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(upstream)).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            debug!(error = %err, "Egress proxy upstream connection ended with an error");
        }
    });
    let response = sender.send_request(request).await?;
    let (mut parts, body) = response.into_parts();
    for name in ["connection", "keep-alive", "proxy-connection"] {
        parts.headers.remove(name);
    }
    Ok(Response::from_parts(parts, body.boxed()))
}

/// An upstream connection that counts the bytes it moves and records them
/// — metrics and the service graph — when the connection is dropped, like
/// a tunnel's splice does when it ends.
struct CountedUpstream {
    stream: TcpStream,
    proxy: Arc<Proxy>,
    host: String,
    up: u64,
    down: u64,
}

impl CountedUpstream {
    fn new(stream: TcpStream, proxy: Arc<Proxy>, host: String) -> Self {
        CountedUpstream {
            stream,
            proxy,
            host,
            up: 0,
            down: 0,
        }
    }
}

impl AsyncRead for CountedUpstream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let poll = std::pin::Pin::new(&mut self.stream).poll_read(cx, buf);
        let read = buf.filled().len().saturating_sub(before);
        self.down += read as u64;
        poll
    }
}

impl AsyncWrite for CountedUpstream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let poll = std::pin::Pin::new(&mut self.stream).poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(written)) = &poll {
            self.up += *written as u64;
        }
        poll
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl Drop for CountedUpstream {
    fn drop(&mut self) {
        net_metrics::bytes("egress", self.up);
        net_metrics::bytes("ingress", self.down);
        self.proxy.context.edges().record_bytes(
            self.proxy.allocation_id,
            &self.host,
            None,
            PROTOCOL_TCP,
            self.up,
            self.down,
        );
    }
}

/// Write any bytes read past the head to the upstream, then splice.
async fn splice<S>(
    proxy: &Proxy,
    host: &str,
    mut client: S,
    mut upstream: TcpStream,
    leftover: &[u8],
) where
    S: SpliceStream,
{
    if !leftover.is_empty() && upstream.write_all(leftover).await.is_err() {
        return;
    }
    let transfer = crate::worker::netd::splice::bidirectional(&mut client, &mut upstream).await;
    net_metrics::bytes("egress", transfer.sent);
    net_metrics::bytes("ingress", transfer.received);
    proxy.context.edges().record_bytes(
        proxy.allocation_id,
        host,
        None,
        PROTOCOL_TCP,
        transfer.sent,
        transfer.received,
    );
    if let Some(err) = transfer.error {
        debug!(error = ?err, "Egress proxy tunnel ended with an error");
    }
}

/// An upgraded connection hyper could not hand back as the client's own
/// stream: always the portable copy.
impl SpliceStream for TokioIo<hyper::upgrade::Upgraded> {}

// ---------------------------------------------------------------------------
// SOCKS5 CONNECT
// ---------------------------------------------------------------------------

async fn handle_socks5<S>(mut client: S, peer: Option<SocketAddr>, proxy: Arc<Proxy>)
where
    S: SpliceStream + Send + 'static,
{
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
    let addrs = match judge(&proxy, &host, port, None).await {
        Ok(addrs) => addrs,
        Err(Refused::Denied(source, reason)) => {
            net_metrics::connection("egress", PROTOCOL_TCP, "deny");
            proxy
                .context
                .edges()
                .record_denied(proxy.allocation_id, &host, PROTOCOL_TCP);
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
    };
    let ip = addrs.first().map(|addr| addr.ip()).or(ip);
    let hop_facts = HopFacts {
        allocation_id: proxy.allocation_id,
        direction: "egress",
        protocol: PROTOCOL_TCP,
        destination: &destination,
        host: Some(&host),
        ip,
        port,
        ..Default::default()
    };
    let upstream = match hops(&proxy, &hop_facts).await {
        HopOutcome::Allow => dial(&addrs).await,
        HopOutcome::Deny { source, reason } => {
            net_metrics::connection("egress", PROTOCOL_TCP, "deny");
            proxy
                .context
                .edges()
                .record_denied(proxy.allocation_id, &host, PROTOCOL_TCP);
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
            let _ = client
                .write_all(&[0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await;
            return;
        }
        HopOutcome::Proxy(target) => dial_proxy_target(&proxy, &target, &hop_facts, peer).await,
    };
    let Some(upstream) = upstream else {
        net_metrics::connection("egress", PROTOCOL_TCP, "error");
        proxy
            .context
            .edges()
            .record_connection(proxy.allocation_id, &host, None, false);
        let _ = client
            .write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await;
        return;
    };
    if client
        .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await
        .is_err()
    {
        return;
    }
    net_metrics::connection("egress", PROTOCOL_TCP, "allow");
    proxy
        .context
        .edges()
        .record_connection(proxy.allocation_id, &host, None, true);
    splice(&proxy, &host, client, upstream, &[]).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use crate::structs::AllocNetPolicy;
    use crate::worker::netd::{Denials, NetPolicyCache};
    use tokio::net::TcpListener;

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

    /// A guard that opens the given loopback ports, so tests can run their
    /// origins on 127.0.0.1 behind the default host guard.
    fn guard_opening(ports: &[u16]) -> Arc<crate::structs::host_guard::HostGuard> {
        let allow = ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect();
        Arc::new(
            crate::structs::host_guard::HostGuard::new(
                &crate::structs::host_guard::HostGuardConfig {
                    allow,
                    deny: vec![],
                },
            )
            .expect("guard"),
        )
    }

    async fn proxies_with(
        allow: &[&str],
        guard: Arc<crate::structs::host_guard::HostGuard>,
        resolver: Option<Arc<dyn crate::worker::netd::guard::Resolver>>,
    ) -> (Arc<EgressProxies>, Arc<Denials>, SocketAddr) {
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
        let mut context = ForwarderContext::new(cache, denials.clone()).with_host_guard(guard);
        if let Some(resolver) = resolver {
            context = context.with_resolver(resolver);
        }
        let proxies = Arc::new(EgressProxies::new(Arc::new(context)));
        proxies
            .sync(&[ProxySpec {
                allocation_id: 5,
                attachment: Attachment::Host,
                listen: Some("127.0.0.1:0".parse().unwrap()),
            }])
            .await;
        let addr = proxies.address(5).expect("proxy bound");
        (proxies, denials, addr)
    }

    /// Send one request and read until the proxy closes. The proxy keeps a
    /// client connection open for more requests, so a plain request asks
    /// for `Connection: close` here, as a one-shot client would.
    async fn raw(addr: SocketAddr, bytes: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let text = String::from_utf8_lossy(bytes);
        let bytes = if text.starts_with("CONNECT ") || text.contains("Connection: close") {
            bytes.to_vec()
        } else {
            text.replacen("\r\n\r\n", "\r\nConnection: close\r\n\r\n", 1)
                .into_bytes()
        };
        stream.write_all(&bytes).await.unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        String::from_utf8_lossy(&out).to_string()
    }

    /// A chain `dlp` handler reads an absolute-URI request's body through the
    /// gated proxy, and a chain `log` handler writes the access log there.
    #[tokio::test]
    async fn chain_dlp_and_log_run_on_absolute_uri_requests_test() {
        let origin_port = origin().await;
        let cache = Arc::new(NetPolicyCache::new());
        let mut object: NetPolicyObject = serde_json::from_value(json!({
            "name": "p", "enforcement": "require",
            "egress": {
                "protocol": "http", "allow": ["127.0.0.1"],
                "handlers": [
                    { "handler": "dlp", "config": { "patterns": ["sk-[a-z]{8,}"] } },
                    { "handler": "log" }
                ]
            }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let policy = merge_policies(&[object], InlinePolicy::default());
        let hash = policy.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 6,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
        let logs = tempfile::tempdir().unwrap();
        let context = ForwarderContext::new(cache, Arc::new(Denials::new()))
            .with_host_guard(guard_opening(&[origin_port]))
            .with_logs_base_dir(logs.path().to_path_buf());
        let proxies = Arc::new(EgressProxies::new(Arc::new(context)));
        proxies
            .sync(&[ProxySpec {
                allocation_id: 6,
                attachment: Attachment::Host,
                listen: Some("127.0.0.1:0".parse().unwrap()),
            }])
            .await;
        let addr = proxies.address(6).expect("proxy bound");

        let body = "token=sk-abcdefghij";
        let response = raw(
            addr,
            format!(
                "POST http://127.0.0.1:{origin_port}/up HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains("legion-denied-by: policy/p#0"),
            "{response}"
        );

        let response = raw(
            addr,
            format!("GET http://127.0.0.1:{origin_port}/fine HTTP/1.1\r\nHost: x\r\n\r\n")
                .as_bytes(),
        )
        .await;
        assert!(response.contains("saw GET /fine"), "{response}");

        let log = logs.path().join("6").join("net-access.log");
        let mut text = String::new();
        for _ in 0..50 {
            text = std::fs::read_to_string(&log).unwrap_or_default();
            if text.lines().count() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(text.contains("\"verdict\":\"deny\""), "{text}");
        assert!(text.contains("\"path\":\"/fine\""), "{text}");
    }

    #[tokio::test]
    async fn http_connect_and_absolute_uri_are_judged_and_forwarded() {
        let origin_port = origin().await;
        let (_proxies, denials, addr) =
            proxies_with(&["127.0.0.1/ok/**"], guard_opening(&[origin_port]), None).await;

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

    /// Bytes a client sends in the same write as its CONNECT head (a TLS
    /// ClientHello, say) reach the destination ahead of the rest of the
    /// tunnel: they are already off the socket when the tunnel takes it.
    #[tokio::test]
    async fn bytes_sent_with_the_connect_head_go_through_the_tunnel_test() {
        let origin_port = origin().await;
        let (_proxies, _denials, addr) =
            proxies_with(&["127.0.0.1/ok/**"], guard_opening(&[origin_port]), None).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                format!(
                    "CONNECT 127.0.0.1:{origin_port} HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\n\r\n\
                     GET /ok/early HTTP/1.1\r\nHost: x\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        let out = String::from_utf8_lossy(&out);
        assert!(out.contains("200 Connection Established"), "{out}");
        assert!(out.contains("saw GET /ok/early"), "{out}");
    }

    /// Every request on one client connection is judged: an allowed request
    /// followed on the same connection by one the policy denies gets a 200
    /// and then a 403, never a pass for the second.
    #[tokio::test]
    async fn every_pipelined_request_gets_its_own_verdict_test() {
        let origin_port = origin().await;
        let (_proxies, denials, addr) =
            proxies_with(&["127.0.0.1/ok/**"], guard_opening(&[origin_port]), None).await;
        let pipelined = format!(
            "GET http://127.0.0.1:{origin_port}/ok/first HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\n\r\n\
             GET http://127.0.0.1:{origin_port}/secret HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\nConnection: close\r\n\r\n"
        );
        let response = raw(addr, pipelined.as_bytes()).await;
        let first = response.find("saw GET /ok/first").expect(&response);
        let second = response.find("HTTP/1.1 403").expect(&response);
        assert!(first < second, "{response}");
        assert!(
            !response.contains("saw GET /secret"),
            "the denied request must never reach the origin: {response}"
        );
        assert!(
            denials.drain().iter().any(|d| d.path == "/secret"),
            "the second request is recorded as denied"
        );
    }

    /// A "filter" proxy: reads the PROXY v2 header, then answers an HTTP
    /// request with what the header said (or echoes the request head for an
    /// HTTP-mode hand-off), so the test can see the attribution arrive.
    async fn filter_proxy() -> SocketAddr {
        use crate::worker::netd::proxyproto::{self, tlv};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let (header, mut leftover) =
                        proxyproto::read_header(&mut stream).await.unwrap();
                    if !leftover.windows(4).any(|w| w == b"\r\n\r\n") {
                        let mut buf = vec![0u8; 4096];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        leftover.extend_from_slice(&buf[..n]);
                    }
                    let head = String::from_utf8_lossy(&leftover).to_string();
                    let body = match header {
                        Some(header) => format!(
                            "proxied identity={} destination={} direction={} alloc={} first={}",
                            header.text(tlv::IDENTITY).unwrap_or_default(),
                            header.text(tlv::DESTINATION).unwrap_or_default(),
                            header.text(tlv::DIRECTION).unwrap_or_default(),
                            header.text(tlv::ALLOCATION).unwrap_or_default(),
                            head.lines().next().unwrap_or_default()
                        ),
                        None => format!("plain head={}", head.replace("\r\n", "|")),
                    };
                    let _ = stream
                        .write_all(
                            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
                                .as_bytes(),
                        )
                        .await;
                });
            }
        });
        addr
    }

    async fn proxies_with_handler(
        egress: serde_json::Value,
        open_ports: &[u16],
    ) -> (Arc<EgressProxies>, Arc<ForwarderContext>, SocketAddr) {
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let mut object: NetPolicyObject = serde_json::from_value(json!({
            "name": "p", "enforcement": "require", "egress": egress
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
        cache.set_identities(
            [(
                5,
                crate::worker::netd::policy::AllocIdentity {
                    job_id: "web".into(),
                    task_name: "server".into(),
                    group: "default".into(),
                    labels: Default::default(),
                    ports: Default::default(),
                },
            )]
            .into_iter()
            .collect(),
        );
        let context = Arc::new(
            ForwarderContext::new(cache, denials).with_host_guard(guard_opening(open_ports)),
        );
        let proxies = Arc::new(EgressProxies::new(context.clone()));
        proxies
            .sync(&[ProxySpec {
                allocation_id: 5,
                attachment: Attachment::Host,
                listen: Some("127.0.0.1:0".parse().unwrap()),
            }])
            .await;
        let addr = proxies.address(5).expect("proxy bound");
        (proxies, context, addr)
    }

    /// A `proxy` handler takes the flow: CONNECT and SOCKS5 tunnels start
    /// at the filter with a PROXY v2 header naming the workload and the
    /// destination; an absolute-URI request reaches it in absolute form
    /// with `Legion-*` headers.
    #[tokio::test]
    async fn proxy_handler_hands_flows_off_with_attribution() {
        let filter = filter_proxy().await;
        let (_proxies, context, addr) = proxies_with_handler(
            json!({
                "protocol": "http",
                "handlers": [{ "handler": "proxy", "service": "filter" }]
            }),
            &[filter.port()],
        )
        .await;
        context.set_handler_endpoints(
            5,
            "filter",
            vec![crate::worker::balance::Endpoint {
                addr: filter.to_string(),
                allocation_id: 90,
                node_id: "node-x".into(),
                tags: Vec::new(),
                mesh_tls: false,
            }],
        );

        // CONNECT: the tunnel is to the filter, which saw the header.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"CONNECT api.example:443 HTTP/1.1\r\nHost: api.example:443\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 256];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("200 Connection Established"));
        stream
            .write_all(b"GET /x HTTP/1.1\r\nHost: api.example\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        let text = String::from_utf8_lossy(&out).to_string();
        assert!(
            text.contains("proxied identity=auto:web:default:server destination=api.example:443 direction=egress alloc=5 first=GET /x HTTP/1.1"),
            "{text}"
        );

        // Absolute URI: the filter gets the request in absolute form plus
        // the attribution headers; no PROXY header on an HTTP hand-off.
        let text = raw(
            addr,
            b"GET http://api.example/y HTTP/1.1\r\nHost: api.example\r\n\r\n",
        )
        .await;
        assert!(
            text.contains("plain head=GET http://api.example/y HTTP/1.1|"),
            "{text}"
        );
        assert!(
            text.contains("legion-identity: auto:web:default:server"),
            "{text}"
        );
        assert!(
            text.contains("legion-destination: api.example:80"),
            "{text}"
        );
        assert!(text.contains("legion-allocation: 5"), "{text}");

        // SOCKS5 CONNECT by name.
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut greet = [0u8; 2];
        stream.read_exact(&mut greet).await.unwrap();
        let name = b"api.example";
        let mut req = vec![0x05, 0x01, 0x00, 0x03, name.len() as u8];
        req.extend_from_slice(name);
        req.extend_from_slice(&443u16.to_be_bytes());
        stream.write_all(&req).await.unwrap();
        let mut reply = [0u8; 10];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply[1], 0x00, "socks connect succeeded");
        stream
            .write_all(b"GET /z HTTP/1.1\r\nHost: api.example\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        assert!(String::from_utf8_lossy(&out).contains("destination=api.example:443"));

        let statuses = context.handler_statuses(5);
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].handler, "proxy");
        assert!(statuses[0].healthy);
        assert_eq!(statuses[0].calls, 3);
    }

    /// A proxy handler with no endpoints fails the chain closed (or open
    /// with `onError = "allow"`), and the datapath reports the hop unhealthy.
    #[tokio::test]
    async fn proxy_handler_without_endpoints_fails_closed() {
        let origin_port = origin().await;
        let (_proxies, context, addr) = proxies_with_handler(
            json!({
                "protocol": "http",
                "handlers": [{ "handler": "proxy", "service": "nowhere" }]
            }),
            &[origin_port],
        )
        .await;
        let response = raw(
            addr,
            format!("GET http://127.0.0.1:{origin_port}/ok HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes(),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(response.contains("has no endpoint"), "{response}");
        let statuses = context.handler_statuses(5);
        assert!(!statuses[0].healthy);

        let (_proxies, _context, addr) = proxies_with_handler(
            json!({
                "protocol": "http",
                "handlers": [{ "handler": "proxy", "service": "nowhere", "onError": "allow" }]
            }),
            &[origin_port],
        )
        .await;
        let response = raw(
            addr,
            format!("GET http://127.0.0.1:{origin_port}/ok HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes(),
        )
        .await;
        assert!(response.contains("saw GET /ok"), "{response}");
    }

    /// The host end of a gated sandbox's vsock is a Unix socket; the proxy
    /// speaks exactly the same protocol on it.
    #[tokio::test]
    async fn the_proxy_serves_a_vsock_host_socket() {
        let origin_port = origin().await;
        let dir = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let mut object: NetPolicyObject = serde_json::from_value(json!({
            "name": "p", "enforcement": "require",
            "egress": { "protocol": "http", "allow": ["127.0.0.1/ok/**"] }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let policy = merge_policies(&[object], InlinePolicy::default());
        let hash = policy.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 9,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
        let context = Arc::new(
            ForwarderContext::new(cache, denials.clone())
                .with_host_guard(guard_opening(&[origin_port])),
        );
        let proxies = EgressProxies::new(context);
        let spec = ProxySpec {
            allocation_id: 9,
            attachment: Attachment::GuestVsock {
                scratch: dir.path().to_path_buf(),
            },
            listen: None,
        };
        let socket = crate::worker::netd::guest_agent::host_socket_path(
            &dir.path().join("vsock.sock"),
            crate::worker::netd::guest_agent::PROXY_PORT,
        );
        assert_eq!(spec.listen_addr(), ListenAddr::Unix(socket.clone()));
        proxies.sync(&[spec]).await;
        assert!(proxies.is_running(9));
        assert!(
            proxies.address(9).is_none(),
            "a socket-backed proxy has no port"
        );

        let mut stream = tokio::net::UnixStream::connect(&socket).await.unwrap();
        stream
            .write_all(
                format!("GET http://127.0.0.1:{origin_port}/ok/vsock HTTP/1.1\r\nHost: x\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("saw GET /ok/vsock"), "{text}");

        let mut stream = tokio::net::UnixStream::connect(&socket).await.unwrap();
        stream
            .write_all(
                format!("GET http://127.0.0.1:{origin_port}/secret HTTP/1.1\r\nHost: x\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        assert!(String::from_utf8_lossy(&out).starts_with("HTTP/1.1 403"));
        assert!(denials.drain().iter().any(|d| d.path == "/secret"));
    }

    /// One policy body per allocation, each allowing `allow` over HTTP.
    fn gated_context(
        policies: &[(i64, &[&str])],
        open_ports: &[u16],
    ) -> (Arc<ForwarderContext>, Arc<Denials>) {
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let mut assignments = Vec::new();
        for (allocation_id, allow) in policies {
            let mut object: NetPolicyObject = serde_json::from_value(json!({
                "name": "p", "enforcement": "require",
                "egress": { "protocol": "http", "allow": allow }
            }))
            .unwrap();
            object.normalize_and_validate().unwrap();
            let policy = merge_policies(&[object], InlinePolicy::default());
            assignments.push(AllocNetPolicy {
                allocation_id: *allocation_id,
                policy_hash: policy.hash.clone(),
            });
            let hash = policy.hash.clone();
            cache.insert_body(&hash, policy).unwrap();
        }
        cache.apply_assignments(&assignments);
        (
            Arc::new(
                ForwarderContext::new(cache, denials.clone())
                    .with_host_guard(guard_opening(open_ports)),
            ),
            denials,
        )
    }

    /// A gated sandbox's proxy on its own allocation's vsock socket.
    fn vsock_spec(root: &std::path::Path, allocation_id: i64) -> ProxySpec {
        ProxySpec {
            allocation_id,
            attachment: Attachment::GuestVsock {
                scratch: crate::task_plugins::sandbox_dir_for(
                    &root.join(allocation_id.to_string()),
                ),
            },
            listen: None,
        }
    }

    /// The vsock host socket a spec's proxy listens on.
    fn socket_of(spec: &ProxySpec) -> std::path::PathBuf {
        spec.listen_addr()
            .unix_path()
            .expect("a vsock spec listens on a socket")
            .to_path_buf()
    }

    async fn get_via(socket: &std::path::Path, origin_port: u16, path: &str) -> String {
        let mut stream = tokio::net::UnixStream::connect(socket).await.unwrap();
        stream
            .write_all(
                format!("GET http://127.0.0.1:{origin_port}{path} HTTP/1.1\r\nHost: x\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await;
        String::from_utf8_lossy(&out).to_string()
    }

    /// Gated plan §6.3, scenario 2: a resume is a new allocation, and its
    /// restored guest's traffic arrives on that allocation's own listener,
    /// so it is judged by the policy in force at resume. Here a policy that
    /// stopped allowing `/poll` while the instance slept denies the first
    /// post-resume request, attributed to the new allocation and never to
    /// the suspended one.
    #[tokio::test]
    async fn a_resumed_gated_sandbox_is_judged_by_its_new_allocations_policy() {
        let origin_port = origin().await;
        let dir = tempfile::tempdir().unwrap();
        let (context, denials) = gated_context(
            &[(7, &["127.0.0.1/**"]), (8, &["127.0.0.1/health"])],
            &[origin_port],
        );
        let proxies = EgressProxies::new(context);
        let before = vsock_spec(dir.path(), 7);
        proxies.sync(std::slice::from_ref(&before)).await;
        let response = get_via(&socket_of(&before), origin_port, "/poll").await;
        assert!(response.contains("200 OK"), "{response}");

        // Suspended: allocation 7 leaves the node. Resumed as 8.
        let after = vsock_spec(dir.path(), 8);
        proxies.sync(std::slice::from_ref(&after)).await;
        assert!(!proxies.is_running(7));
        let response = get_via(&socket_of(&after), origin_port, "/poll").await;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        let drained = denials.drain();
        assert!(!drained.is_empty());
        assert!(
            drained.iter().all(|denial| denial.allocation_id == 8),
            "{drained:?}"
        );
    }

    /// Gated plan §9, fork attribution: three forks of a gated sandbox each
    /// run on their own allocation's socket, so every connection, denial,
    /// and service-graph edge carries the fork's own allocation (which the
    /// manager maps to its instance). Stopping the origin leaves the forks
    /// working, and nothing is ever attributed to the origin.
    #[tokio::test]
    async fn forks_of_a_gated_sandbox_are_attributed_to_their_own_allocations() {
        let origin_port = origin().await;
        let dir = tempfile::tempdir().unwrap();
        let allow: &[&str] = &["127.0.0.1/ok/**"];
        let (context, denials) = gated_context(
            &[(7, allow), (11, allow), (12, allow), (13, allow)],
            &[origin_port],
        );
        let proxies = EgressProxies::new(context.clone());
        let specs: Vec<ProxySpec> = [7, 11, 12, 13]
            .into_iter()
            .map(|id| vsock_spec(dir.path(), id))
            .collect();
        proxies.sync(&specs).await;
        let forks = &specs[1..];
        for fork in forks {
            let socket = &socket_of(&fork);
            let response = get_via(socket, origin_port, "/ok/work").await;
            assert!(response.contains("200 OK"), "{response}");
            let response = get_via(socket, origin_port, "/secret").await;
            assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        }
        let edges = context.edges().drain();
        let mut edge_allocations: Vec<i64> = edges.iter().map(|edge| edge.allocation_id).collect();
        edge_allocations.dedup();
        assert_eq!(edge_allocations, vec![11, 12, 13], "{edges:?}");
        let mut denied: Vec<i64> = denials
            .drain()
            .iter()
            .map(|denial| denial.allocation_id)
            .collect();
        denied.sort();
        assert_eq!(denied, vec![11, 12, 13]);

        // The origin stops; its forks keep their doors.
        proxies.sync(forks).await;
        assert!(!proxies.is_running(7));
        for fork in forks {
            let response = get_via(&socket_of(&fork), origin_port, "/ok/after").await;
            assert!(response.contains("200 OK"), "{response}");
        }
        assert!(context
            .edges()
            .drain()
            .iter()
            .all(|edge| edge.allocation_id != 7));
    }

    /// Gated plan §6.9, Phase 3: a connection through a gated sandbox's
    /// egress proxy counts as open for the `connections` signal from accept
    /// to close, and bytes stamp the allocation's activity as they move,
    /// while the tunnel is still open, not only when the splice ends. So a
    /// long model stream is seen throughout.
    #[tokio::test]
    async fn an_open_egress_tunnel_counts_and_stamps_bytes_as_they_move() {
        // An origin that echoes for as long as the connection stays open.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if stream.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let (context, _denials) = gated_context(&[(9, &["127.0.0.1"])], &[port]);
        let proxies = EgressProxies::new(context.clone());
        let spec = vsock_spec(dir.path(), 9);
        proxies.sync(std::slice::from_ref(&spec)).await;
        let ledger = context.activity();
        let open = |ledger: &crate::worker::netd::activity::ActivityLedger| {
            ledger
                .snapshot()
                .get(&9)
                .map(|activity| activity.open_connections)
                .unwrap_or(0)
        };
        assert_eq!(open(ledger), 0);

        let mut stream = tokio::net::UnixStream::connect(&socket_of(&spec))
            .await
            .unwrap();
        stream
            .write_all(format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut buf = vec![0u8; 256];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("200 Connection Established"));
        assert_eq!(open(ledger), 1, "open while the tunnel is");

        // A quiet stretch, then bytes through the open tunnel stamp the
        // allocation at once.
        ledger.rewind(9, 1);
        stream.write_all(b"model tokens").await.unwrap();
        let mut echo = [0u8; 12];
        stream.read_exact(&mut echo).await.unwrap();
        assert_eq!(&echo, b"model tokens");
        assert!(
            ledger.snapshot()[&9].last_bytes_unix > 1,
            "stamped mid-stream, before the splice ends"
        );
        assert!(
            context
                .edges()
                .drain()
                .iter()
                .all(|edge| edge.bytes_out == 0 && edge.bytes_in == 0),
            "the service graph still counts bytes only when the splice ends"
        );

        drop(stream);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while open(ledger) != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the close was never counted"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Gated plan §6.5, calls entry 3: a gated allocation that no task
    /// block and no policy object covers denies every destination with
    /// `gated-default`, readable in the `403` and the denial. Writing
    /// `egress = { policy = "allow" }` restores allow-all with attribution.
    #[tokio::test]
    async fn a_gated_allocation_with_no_policy_denies_by_default() {
        let origin_port = origin().await;
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let empty = merge_policies(&[], InlinePolicy::default());
        let mut allow: crate::structs::netpolicy::ChainConfig =
            serde_json::from_value(json!({ "policy": "allow", "accessLog": true })).unwrap();
        crate::structs::netpolicy::validate_inline_chain(
            "workspace",
            crate::structs::netpolicy::Direction::Egress,
            &mut allow,
        )
        .unwrap();
        let open = merge_policies(
            &[],
            InlinePolicy {
                egress: Some(&allow),
                ..InlinePolicy::default()
            },
        );
        cache.apply_assignments(&[
            AllocNetPolicy {
                allocation_id: 7,
                policy_hash: empty.hash.clone(),
            },
            AllocNetPolicy {
                allocation_id: 8,
                policy_hash: open.hash.clone(),
            },
        ]);
        cache.insert_body(&empty.hash.clone(), empty).unwrap();
        cache.insert_body(&open.hash.clone(), open).unwrap();
        let context = Arc::new(
            ForwarderContext::new(cache, denials.clone())
                .with_host_guard(guard_opening(&[origin_port])),
        );
        let proxies = EgressProxies::new(context);
        let (shut, opened) = (vsock_spec(dir.path(), 7), vsock_spec(dir.path(), 8));
        proxies.sync(&[shut.clone(), opened.clone()]).await;

        let response = get_via(&socket_of(&shut), origin_port, "/anything").await;
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains("legion-denied-by: gated-default"),
            "{response}"
        );
        let drained = denials.drain();
        assert!(
            drained.iter().any(|denial| denial.allocation_id == 7
                && denial.source == "gated-default"
                && denial.reason.contains("nothing allows 127.0.0.1")),
            "{drained:?}"
        );

        let response = get_via(&socket_of(&opened), origin_port, "/anything").await;
        assert!(response.contains("200 OK"), "{response}");
    }

    #[tokio::test]
    async fn socks5_connect_is_judged_by_name() {
        let origin_port = origin().await;
        let (_proxies, _denials, addr) =
            proxies_with(&["127.0.0.1"], guard_opening(&[origin_port]), None).await;

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

    // -----------------------------------------------------------------
    // The host guard (worker-plane-auth plan §6.11)
    // -----------------------------------------------------------------

    async fn connect_status(addr: SocketAddr, target: &str) -> String {
        raw(
            addr,
            format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n").as_bytes(),
        )
        .await
    }

    async fn socks_reply(addr: SocketAddr, atyp: u8, address: &[u8], port: u16) -> u8 {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut hello = [0u8; 2];
        stream.read_exact(&mut hello).await.unwrap();
        let mut req = vec![0x05, 0x01, 0x00, atyp];
        if atyp == 0x03 {
            req.push(address.len() as u8);
        }
        req.extend_from_slice(address);
        req.extend_from_slice(&port.to_be_bytes());
        stream.write_all(&req).await.unwrap();
        let mut reply = [0u8; 10];
        stream.read_exact(&mut reply).await.unwrap();
        reply[1]
    }

    fn assert_guard_denied(response: &str, what: &str) {
        assert!(response.starts_with("HTTP/1.1 403"), "{what}: {response}");
        let lower = response.to_ascii_lowercase();
        assert!(
            lower.contains("legion-denied-by: host-guard"),
            "{what}: {response}"
        );
        assert!(
            lower.contains("legion-denied-reason:") && lower.contains("may not reach it"),
            "{what}: {response}"
        );
    }

    #[tokio::test]
    async fn every_default_class_is_refused_on_all_three_paths_test() {
        // Policy allows everything: the guard sits below it.
        let guard = guard_opening(&[]);
        guard.set_node_addrs(vec!["10.0.1.7".parse().unwrap()]);
        let (_proxies, denials, addr) = proxies_with(&["*"], guard, None).await;
        for (target, connect) in [
            ("127.0.0.1", "127.0.0.1:4747"),
            ("::1", "[::1]:4747"),
            ("::ffff:127.0.0.1", "[::ffff:127.0.0.1]:4848"),
            ("0.0.0.0", "0.0.0.0:4848"),
            ("10.0.1.7", "10.0.1.7:4848"),
            ("169.254.169.254", "169.254.169.254:80"),
            ("fd00:ec2::254", "[fd00:ec2::254]:80"),
            ("100.100.100.200", "100.100.100.200:80"),
        ] {
            assert_guard_denied(&connect_status(addr, connect).await, connect);
            let absolute = raw(
                addr,
                format!("GET http://{connect}/latest/meta-data/ HTTP/1.1\r\nHost: x\r\n\r\n")
                    .as_bytes(),
            )
            .await;
            assert_guard_denied(&absolute, &format!("GET {connect}"));
            let ip: IpAddr = target.parse().unwrap();
            let port: u16 = connect.rsplit(':').next().unwrap().parse().unwrap();
            let code = match ip {
                IpAddr::V4(v4) => socks_reply(addr, 0x01, &v4.octets(), port).await,
                IpAddr::V6(v6) => socks_reply(addr, 0x04, &v6.octets(), port).await,
            };
            assert_eq!(code, 0x02, "SOCKS {target} must be refused by the ruleset");
        }
        let drained = denials.drain();
        assert!(!drained.is_empty());
        assert!(
            drained
                .iter()
                .all(|denial| denial.source == crate::structs::host_guard::HOST_GUARD_SOURCE),
            "every denial names host-guard: {drained:?}"
        );
    }

    #[tokio::test]
    async fn names_are_judged_by_the_address_they_resolve_to_test() {
        let resolver = crate::worker::netd::guard::test_support::FakeResolver::new();
        resolver.answer(
            "instance-data.test",
            vec![vec!["169.254.169.254".parse().unwrap()]],
        );
        let (_proxies, _denials, addr) =
            proxies_with(&["*"], guard_opening(&[]), Some(resolver.clone())).await;
        for name in [
            "localhost:4747",
            "instance-data.test:80",
            "2130706433:4747",
            "0177.0.0.1:4747",
        ] {
            assert_guard_denied(&connect_status(addr, name).await, name);
        }
        let response = connect_status(addr, "instance-data.test:80").await;
        assert!(
            response.contains("instance-data.test resolves to 169.254.169.254 (link-local)"),
            "{response}"
        );
        assert_eq!(
            socks_reply(addr, 0x03, b"instance-data.test", 80).await,
            0x02
        );
    }

    #[tokio::test]
    async fn a_rebinding_name_is_resolved_once_and_the_judged_address_dialed_test() {
        let origin_port = origin_counting().await;
        let resolver = crate::worker::netd::guard::test_support::FakeResolver::new();
        // First answer: a routable address the guard passes (it will not
        // answer in a test, so the tunnel fails). Second: loopback, where the
        // origin is listening. The proxy must never ask twice.
        resolver.answer(
            "rebind.test",
            vec![
                vec!["10.255.255.1".parse().unwrap()],
                vec!["127.0.0.1".parse().unwrap()],
            ],
        );
        let (_proxies, _denials, addr) = proxies_with(
            &["*"],
            guard_opening(&[origin_port.0]),
            Some(resolver.clone()),
        )
        .await;
        let response = connect_status(addr, &format!("rebind.test:{}", origin_port.0)).await;
        assert!(
            !response.contains("200 Connection Established"),
            "{response}"
        );
        assert_eq!(resolver.call_count("rebind.test"), 1);
        assert_eq!(
            origin_port.1.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the second (loopback) answer was never dialed"
        );
    }

    /// An origin that counts connections, returning (port, count).
    async fn origin_counting() -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = count.clone();
        tokio::spawn(async move {
            while let Ok((_stream, _)) = listener.accept().await {
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
        (port, count)
    }

    #[tokio::test]
    async fn a_host_guard_allow_entry_opens_exactly_its_port_test() {
        let origin_port = origin().await;
        let (_proxies, _denials, addr) =
            proxies_with(&["*"], guard_opening(&[origin_port]), None).await;
        let open = connect_status(addr, &format!("127.0.0.1:{origin_port}")).await;
        assert!(open.contains("200 Connection Established"), "{open}");
        let next = origin_port.wrapping_add(1).max(1);
        assert_guard_denied(
            &connect_status(addr, &format!("127.0.0.1:{next}")).await,
            "the port next to the opened one",
        );
        assert_guard_denied(
            &connect_status(addr, &format!("[::1]:{origin_port}")).await,
            "the same port on another loopback address",
        );
    }
}
