//! HTTP/1.1 mode for an upstream listener.
//!
//! When an upstream is HTTP — its `to` is `http://`/`https://`, it carries an
//! `http` block, or the allocation's effective egress policy says
//! `protocol = "http"` — the listener stops splicing bytes and parses
//! requests instead. Per request, in order:
//!
//! 1. **Policy.** The effective egress chain is evaluated with the request
//!    head (method, path, headers, plus the connect-time facts). A denial is
//!    answered with a `403` whose body and `Legion-Denied-By` header name the
//!    rule and reason, so a client — an agent in a sandbox, say — can read
//!    *why* and change its approach instead of retrying.
//! 2. **Handlers.** The chain's hops and then the upstream's own run as one
//!    pipeline ([`crate::worker::netd::handlers::Pipeline`]): `authz` and
//!    `proxy` with the request head, then `dlp` on the buffered body. The
//!    chain's `accessLog` writes one NDJSON line per request.
//! 3. **Traffic management** (`connect.upstreams[].http`): a per-request
//!    deadline, bounded retries for idempotent methods under a budget, a
//!    weighted split by service tag, and outlier ejection of endpoints that
//!    keep answering 5xx.
//! 4. **Credentials** (`connect.upstreams[].credentials`): each declared
//!    header is dropped from what the task sent and set to the secret the
//!    node holds. A header with no value yet fails the request with a local
//!    `503` instead of sending it unauthenticated.
//! 5. **The dial.** An idle pooled upstream connection when the listener has
//!    one ([`ConnPool`]), else a new one: TLS originated for
//!    `https://`/`tls://` targets, mesh mTLS plus a PROXY v2 header for mesh
//!    endpoints. The `Host` header is rewritten to an external target's host
//!    so the origin accepts it.
//!
//! Everything here is bounded: body buffering has a cap, retries have a
//! budget, requests have a deadline. Per-request logging stays at `debug`.

use crate::prelude::*;
use crate::structs::hops::HopConfig;
use crate::structs::netpolicy::{
    parse_duration, EffectiveChain, RequestFacts, Verdict, PROTOCOL_HTTP,
};
use crate::structs::{UpstreamCredential, UpstreamHttpConfig, UpstreamTarget};
use crate::worker::balance::{Balancer, Endpoint, Outcome};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::handlers::{
    self, ExchangeOutcome, HopFacts, LocalResponse, PipelineCache, ProxyTarget, RequestHead,
};
use crate::worker::netd::metrics as net_metrics;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// Consecutive 5xx answers that eject an endpoint in HTTP mode.
const OUTLIER_CONSECUTIVE_5XX: u32 = 5;
/// How long an ejected endpoint is skipped.
const OUTLIER_EJECT_FOR: Duration = Duration::from_secs(30);
/// How long an upstream dial may take before it counts as a connect failure.
const DIAL_TIMEOUT: Duration = Duration::from_secs(2);
/// Retry budget window.
const BUDGET_WINDOW: Duration = Duration::from_secs(60);
/// Idempotent methods retried by default.
const IDEMPOTENT: &[&str] = &["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"];
/// How long an idle pooled upstream connection is kept. Shorter than the
/// keep-alive timeout of most servers, so the pool rarely hands out a
/// connection the server is about to close.
const POOL_IDLE: Duration = Duration::from_secs(30);
/// Idle connections kept per destination; more are closed when released.
const POOL_MAX_IDLE: usize = 32;

/// Hop-by-hop headers that must not be forwarded.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "upgrade",
];

/// A retry policy with defaults filled in.
#[derive(Clone)]
struct RetryPolicy {
    attempts: u32,
    /// The largest body kept for a retry (`retries.maxBodyBytes`).
    max_body_bytes: u64,
    on_5xx: bool,
    on_connect_failure: bool,
    on_reset: bool,
    on_retriable_4xx: bool,
    budget_percent: u32,
    methods: Vec<String>,
}

impl RetryPolicy {
    fn from(config: Option<&crate::structs::RetryConfig>) -> Option<Self> {
        let config = config?;
        let on: Vec<&str> = if config.on.is_empty() {
            vec!["connect-failure", "5xx"]
        } else {
            config.on.iter().map(String::as_str).collect()
        };
        let budget_percent = config
            .budget
            .as_deref()
            .and_then(|b| b.trim().trim_end_matches('%').parse::<u32>().ok())
            .unwrap_or(20);
        Some(RetryPolicy {
            attempts: config.attempts.unwrap_or(1).min(10),
            max_body_bytes: config.max_body_bytes(),
            on_5xx: on.contains(&"5xx"),
            on_connect_failure: on.contains(&"connect-failure"),
            on_reset: on.contains(&"reset"),
            on_retriable_4xx: on.contains(&"retriable-4xx"),
            budget_percent,
            methods: if config.methods.is_empty() {
                IDEMPOTENT.iter().map(|m| m.to_string()).collect()
            } else {
                config
                    .methods
                    .iter()
                    .map(|m| m.to_ascii_uppercase())
                    .collect()
            },
        })
    }
}

/// Retries as a share of requests over a window, so a failing backend is
/// never retried into the ground.
struct RetryBudget {
    requests: AtomicU64,
    retries: AtomicU64,
    window_start: Mutex<Instant>,
}

impl RetryBudget {
    fn new() -> Self {
        RetryBudget {
            requests: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            window_start: Mutex::new(Instant::now()),
        }
    }

    fn note_request(&self) {
        let mut start = self.window_start.lock().unwrap_or_else(|p| p.into_inner());
        if start.elapsed() > BUDGET_WINDOW {
            *start = Instant::now();
            self.requests.store(0, Ordering::Relaxed);
            self.retries.store(0, Ordering::Relaxed);
        }
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    fn try_take(&self, percent: u32) -> bool {
        let requests = self.requests.load(Ordering::Relaxed).max(1);
        let allowed = (requests * u64::from(percent) / 100).max(1);
        let used = self.retries.load(Ordering::Relaxed);
        if used >= allowed {
            return false;
        }
        self.retries.fetch_add(1, Ordering::Relaxed);
        true
    }
}

/// Per-endpoint outlier state.
#[derive(Default)]
struct OutlierState {
    consecutive_5xx: u32,
    ejected_until: Option<Instant>,
}

/// Everything an HTTP-mode listener needs, built once per listener.
pub struct HttpSettings {
    pub allocation_id: i64,
    pub key: String,
    pub target: Option<UpstreamTarget>,
    /// `"egress"` (an upstream listener) or `"ingress"` (an advertised port
    /// in front of the task): selects the chain, the metric labels, and how
    /// denials are attributed.
    pub direction: &'static str,
    /// For ingress: how a request reaches the task (its loopback, inside
    /// its gated namespace when it has one, or a sandbox guest over vsock or
    /// on its bridge). `None` dials the picked endpoint over TCP.
    pub task: Option<crate::worker::netd::ingress::TaskTarget>,
    pub context: Arc<ForwarderContext>,
    pub balancer: Arc<dyn Balancer>,
    pub endpoints: Arc<Mutex<Arc<Vec<Endpoint>>>>,
    pub inflight: Arc<Mutex<crate::worker::balance::InflightCounts>>,
    /// Idle upstream connections, reused across requests.
    pool: Arc<ConnPool>,
    /// Where the allocation's access log lives (`<logs>/<alloc>/net-access.log`).
    pub log_dir: Option<PathBuf>,
    timeout: Option<Duration>,
    retry: Option<RetryPolicy>,
    budget: RetryBudget,
    split: Vec<(String, u32)>,
    outlier_threshold: Option<u32>,
    outlier_eject: Duration,
    outliers: Mutex<HashMap<String, OutlierState>>,
    /// The upstream's own handlers (`connect.upstreams[].handlers`); they run
    /// per request after the chain's.
    pub handlers: Vec<HopConfig>,
    /// The chain's hops plus the upstream's, compiled per policy body.
    pipelines: PipelineCache,
    /// `connect.upstreams[].credentials`: headers set on every request this
    /// upstream sends, with values from the context's credential store.
    pub credentials: Vec<UpstreamCredential>,
    /// `connect.upstreams[].ca`: the trust bundle an external `https://`
    /// upstream is verified against instead of the public roots.
    pub ca: Option<crate::structs::source::Source>,
    /// The allocation is gated: its egress requests fail closed while its
    /// boot gate is shut or its policy has not arrived.
    pub gated: bool,
}

/// Headers to set on an outgoing request in place of whatever the task sent
/// under the same names. Values are marked sensitive.
type Injected = Vec<(hyper::header::HeaderName, hyper::header::HeaderValue)>;

/// What a listener knows about one accepted connection.
#[derive(Debug, Clone, Default)]
pub struct ConnInfo {
    /// The verified peer identity (ingress with mutual TLS).
    pub identity: Option<String>,
    pub peer: Option<SocketAddr>,
}

impl HttpSettings {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allocation_id: i64,
        key: String,
        target: Option<UpstreamTarget>,
        config: Option<&UpstreamHttpConfig>,
        handlers: &[HopConfig],
        context: Arc<ForwarderContext>,
        balancer: Arc<dyn Balancer>,
        endpoints: Arc<Mutex<Arc<Vec<Endpoint>>>>,
        inflight: Arc<Mutex<crate::worker::balance::InflightCounts>>,
        log_dir: Option<PathBuf>,
    ) -> Self {
        let timeout = config
            .and_then(|c| c.timeout.as_deref())
            .and_then(|t| parse_duration(t).ok())
            .filter(|d| !d.is_zero());
        let retry = RetryPolicy::from(config.and_then(|c| c.retries.as_ref()));
        let split = config
            .map(|c| {
                c.split
                    .iter()
                    .map(|s| (s.tag.clone(), s.weight))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        // Outlier ejection has no knob: an endpoint that answers 5xx this
        // many times in a row is skipped for this long, whatever the balancer.
        let outlier_threshold = Some(OUTLIER_CONSECUTIVE_5XX);
        let outlier_eject = OUTLIER_EJECT_FOR;
        HttpSettings {
            allocation_id,
            key,
            target,
            direction: "egress",
            task: None,
            context,
            balancer,
            endpoints,
            inflight,
            pool: Arc::new(ConnPool::default()),
            log_dir,
            timeout,
            retry,
            budget: RetryBudget::new(),
            split,
            outlier_threshold,
            outlier_eject,
            outliers: Mutex::new(HashMap::new()),
            handlers: handlers.to_vec(),
            pipelines: PipelineCache::default(),
            credentials: Vec::new(),
            ca: None,
            gated: false,
        }
    }

    /// The headers to inject on this request, read once so every attempt
    /// (and a proxy hand-off) sends the same value even if a rotation lands
    /// mid-request. `Err` names the first credential with no value: the
    /// request must not leave without it.
    fn injected_credentials(&self) -> Result<Injected, String> {
        let mut out = Vec::with_capacity(self.credentials.len());
        for credential in &self.credentials {
            let header = credential.header.to_ascii_lowercase();
            let value = self
                .context
                .credential_value(self.allocation_id, &self.key, credential)
                .map_err(|error| format!("header {header}: {error}"))?;
            let name = hyper::header::HeaderName::from_bytes(header.as_bytes())
                .map_err(|_| format!("header {header}: not a valid header name"))?;
            let mut value = hyper::header::HeaderValue::from_str(&format!(
                "{}{}",
                credential.prefix.as_deref().unwrap_or_default(),
                value.expose()
            ))
            .map_err(|_| format!("header {header}: the value is not a valid header value"))?;
            value.set_sensitive(true);
            out.push((name, value));
        }
        Ok(out)
    }

    /// Settings for an ingress listener: HTTP/1.1 in front of the task's
    /// private port, judged against the *ingress* chain. No traffic
    /// management (there is exactly one backend), no handlers yet.
    pub fn for_ingress(
        allocation_id: i64,
        task: crate::worker::netd::ingress::TaskTarget,
        context: Arc<ForwarderContext>,
        log_dir: Option<PathBuf>,
    ) -> Self {
        let mut settings = HttpSettings::new(
            allocation_id,
            task.label.clone(),
            Some(UpstreamTarget {
                host: "127.0.0.1".to_string(),
                port: task.port,
                tls: false,
                http: true,
            }),
            None,
            &[],
            context,
            crate::worker::balance::default_balancer(),
            Arc::new(Mutex::new(Arc::new(Vec::new()))),
            Arc::new(Mutex::new(HashMap::new())),
            log_dir,
        );
        settings.direction = "ingress";
        settings.task = Some(task);
        settings
    }

    /// Endpoints that are not currently ejected.
    fn healthy_endpoints(&self) -> Vec<Endpoint> {
        let endpoints = Vec::clone(&self.endpoints.lock().unwrap_or_else(|p| p.into_inner()));
        if self.outlier_threshold.is_none() {
            return endpoints;
        }
        let now = Instant::now();
        let mut outliers = self.outliers.lock().unwrap_or_else(|p| p.into_inner());
        endpoints
            .into_iter()
            .filter(|endpoint| match outliers.get_mut(&endpoint.addr) {
                Some(state) => match state.ejected_until {
                    Some(until) if until > now => false,
                    Some(_) => {
                        // Ejection over: give it another chance from zero.
                        *state = OutlierState::default();
                        true
                    }
                    None => true,
                },
                None => true,
            })
            .collect()
    }

    fn note_outcome(&self, addr: &str, status: Option<StatusCode>) {
        let Some(threshold) = self.outlier_threshold else {
            return;
        };
        let mut outliers = self.outliers.lock().unwrap_or_else(|p| p.into_inner());
        let state = outliers.entry(addr.to_string()).or_default();
        match status {
            Some(status) if status.is_server_error() => {
                state.consecutive_5xx += 1;
                if state.consecutive_5xx >= threshold {
                    state.ejected_until = Some(Instant::now() + self.outlier_eject);
                    state.consecutive_5xx = 0;
                    debug!(upstream = %self.key, endpoint = addr, "Ejected endpoint after consecutive 5xx");
                }
            }
            Some(_) => state.consecutive_5xx = 0,
            None => {
                // A connect failure counts like a 5xx for ejection purposes.
                state.consecutive_5xx += 1;
                if state.consecutive_5xx >= threshold {
                    state.ejected_until = Some(Instant::now() + self.outlier_eject);
                    state.consecutive_5xx = 0;
                }
            }
        }
    }

    /// Pick an endpoint for one attempt, honoring the split: a tag bucket is
    /// chosen by weight, then the balancer picks within it (falling back to
    /// everything when the bucket is empty).
    fn pick(&self, candidates: &[Endpoint]) -> Option<Endpoint> {
        if candidates.is_empty() {
            return None;
        }
        let pool: Vec<Endpoint> = if self.split.is_empty() {
            candidates.to_vec()
        } else {
            let roll = rand::random::<u32>() % 100;
            let mut acc = 0;
            let mut chosen: Option<&str> = None;
            for (tag, weight) in &self.split {
                acc += weight;
                if roll < acc {
                    chosen = Some(tag.as_str());
                    break;
                }
            }
            let listed: Vec<&str> = self.split.iter().map(|(t, _)| t.as_str()).collect();
            let bucket: Vec<Endpoint> = match chosen {
                Some(tag) => candidates
                    .iter()
                    .filter(|e| e.tags.iter().any(|t| t == tag))
                    .cloned()
                    .collect(),
                None => candidates
                    .iter()
                    .filter(|e| !e.tags.iter().any(|t| listed.contains(&t.as_str())))
                    .cloned()
                    .collect(),
            };
            if bucket.is_empty() {
                candidates.to_vec()
            } else {
                bucket
            }
        };
        let index = {
            let inflight = self.inflight.lock().unwrap_or_else(|p| p.into_inner());
            self.balancer.pick(&pool, &inflight)?
        };
        pool.get(index).cloned()
    }

    /// Count one request in flight to `addr` until the guard drops, so
    /// load-aware strategies see HTTP-mode load too.
    fn in_flight(self: &Arc<Self>, addr: &str) -> crate::worker::balance::InflightGuard {
        crate::worker::balance::acquire_inflight(&self.inflight, addr)
    }
}

/// How a pooled connection was opened. Connections are only reused for the
/// same kind of dial to the same place.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PoolKey {
    /// Plain TCP to an endpoint or an external `http://` target.
    Plain(String),
    /// TLS originated to an external `https://` target.
    Tls(String),
    /// Mesh mTLS to an endpoint, with this listener's workload named in the
    /// PROXY v2 header sent when the connection opened.
    Mesh(String),
    /// The task behind an ingress listener.
    Task,
}

type Sender = hyper::client::conn::http1::SendRequest<OutBody>;

/// Idle upstream connections of one HTTP-mode listener.
///
/// A listener belongs to one allocation, so a pool is never shared between
/// workloads: a mesh connection's PROXY v2 header, sent once when it opened,
/// names the right workload for every request it later carries.
///
/// A connection comes back once its response body has been read and the
/// upstream kept it alive, and leaves after [`POOL_IDLE`] or when the
/// upstream closes it. A reaper task closes idle connections on a listener
/// that has gone quiet, and exits once nothing is idle.
#[derive(Default)]
struct ConnPool {
    idle: Mutex<HashMap<PoolKey, Vec<IdleConn>>>,
    reaping: AtomicBool,
}

struct IdleConn {
    sender: Sender,
    since: Instant,
}

impl ConnPool {
    /// The most recently released live connection for `key`. Newest first
    /// keeps the fewest connections warm when traffic drops.
    fn checkout(&self, key: &PoolKey) -> Option<Sender> {
        let mut idle = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        let list = idle.get_mut(key)?;
        while let Some(conn) = list.pop() {
            if conn.since.elapsed() < POOL_IDLE && conn.sender.is_ready() {
                return Some(conn.sender);
            }
        }
        None
    }

    fn checkin(self: &Arc<Self>, key: PoolKey, sender: Sender) {
        if !sender.is_ready() {
            return;
        }
        {
            let mut idle = self.idle.lock().unwrap_or_else(|p| p.into_inner());
            let list = idle.entry(key).or_default();
            if list.len() >= POOL_MAX_IDLE {
                return;
            }
            list.push(IdleConn {
                sender,
                since: Instant::now(),
            });
        }
        if !self.reaping.swap(true, Ordering::AcqRel) {
            let pool = Arc::downgrade(self);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(POOL_IDLE / 2).await;
                    let Some(pool) = pool.upgrade() else {
                        return;
                    };
                    if pool.reap() == 0 {
                        pool.reaping.store(false, Ordering::Release);
                        // A connection released between the sweep and the
                        // flag store would otherwise wait for the next one.
                        if pool.reap() == 0 || pool.reaping.swap(true, Ordering::AcqRel) {
                            return;
                        }
                    }
                }
            });
        }
    }

    /// Close expired and dead idle connections; returns how many remain.
    fn reap(&self) -> usize {
        let mut idle = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        idle.retain(|_, list| {
            list.retain(|conn| conn.since.elapsed() < POOL_IDLE && !conn.sender.is_closed());
            !list.is_empty()
        });
        idle.values().map(Vec::len).sum()
    }

    #[cfg(test)]
    fn idle_count(&self) -> usize {
        self.idle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .map(Vec::len)
            .sum()
    }
}

/// Serve one accepted connection in HTTP mode until the client closes it.
pub async fn serve(client: TcpStream, settings: Arc<HttpSettings>) {
    serve_with(client, settings, ConnInfo::default()).await
}

/// Serve one accepted connection — plain or TLS-terminated — knowing who is
/// on the other end.
pub async fn serve_with<S>(client: S, settings: Arc<HttpSettings>, conn: ConnInfo)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(client);
    let conn = Arc::new(conn);
    let service = hyper::service::service_fn(move |req: Request<Incoming>| {
        let settings = settings.clone();
        let conn = conn.clone();
        async move { Ok::<_, hyper::Error>(handle(req, settings, conn).await) }
    });
    if let Err(err) = hyper::server::conn::http1::Builder::new()
        .preserve_header_case(true)
        .half_close(true)
        .serve_connection(io, service)
        .await
    {
        debug!(error = ?err, "HTTP upstream connection ended with an error");
    }
}

type OutBody = handlers::HopBody;

fn full(bytes: impl Into<Bytes>) -> OutBody {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed()
}

fn denied_response(
    direction: &str,
    upstream: &str,
    destination: &str,
    source: &str,
    reason: Option<&str>,
) -> Response<OutBody> {
    let body = json!({
        "legion": {
            "denied": true,
            "direction": direction,
            "upstream": upstream,
            "destination": destination,
            "source": source,
            "reason": reason.unwrap_or_default(),
        }
    });
    let mut response = Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header("content-type", "application/json")
        .header("legion-denied-by", source)
        .body(full(body.to_string()))
        .expect("static response");
    if let Some(reason) = reason {
        if let Ok(value) = hyper::header::HeaderValue::from_str(reason) {
            response.headers_mut().insert("legion-denied-reason", value);
        }
    }
    response
}

/// The local answer for a request whose credential the node does not hold:
/// a `503` that names the upstream, the header, and why, so the caller sees
/// a configuration problem rather than the origin's `401`.
fn credential_unavailable_response(upstream: &str, problem: &str) -> Response<OutBody> {
    let reason = format!("credential-unavailable: upstream {upstream} {problem}");
    let body = json!({
        "legion": {
            "error": reason,
            "upstream": upstream,
            "source": "credential-unavailable",
        }
    });
    let mut response = Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("content-type", "application/json")
        .header("legion-denied-by", "credential-unavailable")
        .body(full(body.to_string()))
        .expect("static response");
    if let Ok(value) = hyper::header::HeaderValue::from_str(&reason) {
        response.headers_mut().insert("legion-denied-reason", value);
    }
    response
}

fn error_response(status: StatusCode, message: String) -> Response<OutBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full(json!({ "legion": { "error": message } }).to_string()))
        .expect("static response")
}

/// One request end to end.
async fn handle(
    req: Request<Incoming>,
    settings: Arc<HttpSettings>,
    conn: Arc<ConnInfo>,
) -> Response<OutBody> {
    let started = Instant::now();
    let method = req.method().as_str().to_ascii_uppercase();
    let path = req
        .uri()
        .path_and_query()
        .map(|pq| pq.to_string())
        .unwrap_or_else(|| "/".to_string());
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_ascii_lowercase(),
                value.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    let host_header = headers
        .iter()
        .find(|(name, _)| name == "host")
        .map(|(_, value)| value.clone());

    // Connect-time facts: an external target's host and port, or the service
    // name and the endpoint that will be dialed (decided per attempt).
    let (fact_host, fact_port): (String, u16) = match settings.target.as_ref() {
        Some(target) => (target.host.clone(), target.port),
        None => (settings.key.clone(), 0),
    };
    let destination = match settings.target.as_ref() {
        Some(target) => target.addr(),
        None => settings.key.clone(),
    };
    settings.budget.note_request();

    // 1. Policy. An egress request to an external target runs the shared
    // judge: the gate, the host guard on every resolved address, then the
    // chain per address with the request head, so a CIDR rule sees what the
    // name resolves to. A catalog service's endpoints are judged at the
    // gate and by the service name; ingress by its own chain.
    let direction = settings.direction;
    let policy = settings.context.policy_for(settings.allocation_id);
    let chain: Option<&EffectiveChain> = policy.as_ref().and_then(|p| {
        if direction == "ingress" {
            p.ingress.as_ref()
        } else {
            p.egress.as_ref()
        }
    });
    let path_only = path.split('?').next().unwrap_or("/");
    let mut judged: Option<Vec<std::net::SocketAddr>> = None;
    let denial = match (direction, settings.target.as_ref()) {
        ("egress", Some(target)) => match crate::worker::netd::egress::judge(
            &settings.context,
            settings.allocation_id,
            settings.gated,
            true,
            &target.host,
            target.port,
            Some((&method, path_only, &headers)),
        )
        .await
        {
            Ok(addrs) => {
                judged = Some(addrs);
                None
            }
            Err(denial) => Some(denial),
        },
        _ => crate::worker::netd::egress::gate_block(
            &settings.context,
            settings.allocation_id,
            settings.gated && direction == "egress",
            policy.as_deref(),
        )
        .or_else(|| {
            let facts = RequestFacts {
                host: Some(fact_host.as_str()),
                ip: fact_host.parse().ok(),
                port: fact_port,
                identity: conn.identity.as_deref(),
                method: &method,
                path: path_only,
                headers: &headers,
            };
            match crate::structs::netpolicy::evaluate_request(chain?, &facts) {
                Verdict::Deny { source, reason } => {
                    Some(crate::worker::netd::egress::Denial::new(source, reason))
                }
                _ => None,
            }
        }),
    };
    if let Some(crate::worker::netd::egress::Denial { source, reason }) = denial {
        net_metrics::connection(direction, PROTOCOL_HTTP, "deny");
        settings.context.record_denial(
            settings.allocation_id,
            direction,
            PROTOCOL_HTTP,
            &destination,
            &source,
            reason.as_deref(),
            &method,
            path_only,
        );
        if let Some(chain) = chain {
            access_log(
                &settings,
                chain,
                &method,
                &path,
                host_header.as_deref(),
                403,
                "deny",
                Some(&source),
                started,
                0,
                0,
            )
            .await;
        }
        note_edge_denied(&settings);
        return denied_response(
            direction,
            &settings.key,
            &destination,
            &source,
            reason.as_deref(),
        );
    }

    // 1b. Handlers, with the request head: a webhook may deny, a proxy may
    // take the request, a plugin may answer or rewrite it.
    let hop_facts = HopFacts {
        allocation_id: settings.allocation_id,
        direction,
        protocol: PROTOCOL_HTTP,
        destination: &destination,
        host: Some(fact_host.as_str()),
        ip: fact_host.parse().ok(),
        port: fact_port,
        identity: conn.identity.as_deref(),
        upstream: (direction == "egress").then_some(settings.key.as_str()),
        method: Some(&method),
        path: Some(path_only),
        headers: &headers,
    };
    let pipeline = settings.pipelines.get(
        policy.as_deref(),
        direction,
        Some(&settings.key),
        &settings.handlers,
    );
    let mut exchange = pipeline.exchange(&settings.context, &hop_facts);
    let mut head = RequestHead {
        method: method.clone(),
        path: path.clone(),
        authority: host_header.clone().unwrap_or_else(|| destination.clone()),
        scheme: if settings.target.as_ref().is_some_and(|t| t.tls) {
            "https".into()
        } else {
            "http".into()
        },
        headers: headers.clone(),
        end_of_stream: RequestHead::no_body(&headers),
        changed: false,
    };
    let mut hand_off: Option<ProxyTarget> = None;
    if !exchange.is_empty() {
        match exchange.request(&hop_facts, &mut head).await {
            ExchangeOutcome::Continue => {}
            ExchangeOutcome::Deny { source, reason } => {
                net_metrics::connection(direction, PROTOCOL_HTTP, "deny");
                settings.context.record_denial_for(
                    settings.allocation_id,
                    conn.identity.as_deref(),
                    direction,
                    PROTOCOL_HTTP,
                    &destination,
                    &source,
                    reason.as_deref(),
                    &method,
                    path_only,
                );
                if let Some(chain) = chain {
                    access_log(
                        &settings,
                        chain,
                        &method,
                        &path,
                        host_header.as_deref(),
                        403,
                        "deny",
                        Some(&source),
                        started,
                        0,
                        0,
                    )
                    .await;
                }
                note_edge_denied(&settings);
                exchange.answered(403);
                return denied_response(
                    direction,
                    &settings.key,
                    &destination,
                    &source,
                    reason.as_deref(),
                );
            }
            ExchangeOutcome::Respond(local) => {
                return answer_locally(
                    &settings,
                    &conn,
                    chain,
                    exchange,
                    local,
                    &destination,
                    &method,
                    &path,
                    host_header.as_deref(),
                    started,
                    0,
                )
                .await;
            }
            ExchangeOutcome::Proxy(target) => hand_off = Some(target),
        }
    }

    // 1c. Credentials. Read once, before the body and before any dial: a
    // header the node holds no value for fails the request here, so it never
    // reaches the origin (or a proxy) without its credential.
    let injected = match settings.injected_credentials() {
        Ok(injected) => injected,
        Err(problem) => {
            net_metrics::connection(direction, PROTOCOL_HTTP, "error");
            note_edge_request(&settings, None, Some(503), started, 0);
            debug!(
                allocation_id = settings.allocation_id,
                upstream = %settings.key,
                problem = %problem,
                "Credential unavailable; answering 503 without dialing"
            );
            if let Some(chain) = chain {
                access_log(
                    &settings,
                    chain,
                    &method,
                    &path,
                    host_header.as_deref(),
                    503,
                    "error",
                    Some("credential-unavailable"),
                    started,
                    0,
                    0,
                )
                .await;
            }
            return credential_unavailable_response(&settings.key, &problem);
        }
    };

    // Split the head from the body (mesh-control-path plan §6.4). The body
    // is streamed to the upstream as it arrives unless something must read
    // or replay it: the body hops (`dlp`, a plugin's `body` stage) read it
    // whole, up to their cap; a request that may be retried keeps a copy up
    // to `retries.maxBodyBytes`, and a larger one streams and is sent once.
    // Nothing buffers an unbounded body.
    let retry = settings.retry.clone();
    let retriable_method = retry
        .as_ref()
        .is_some_and(|r| r.methods.iter().any(|m| m == &method));
    let replays =
        hand_off.is_none() && retriable_method && retry.as_ref().is_some_and(|r| r.attempts > 0);
    let (mut parts, body) = req.into_parts();
    if head.changed {
        apply_head(&mut parts, &head);
    }
    let mut request_body: RequestBody = if !exchange.reads_body() {
        if replays {
            let cap = retry.as_ref().map(|r| r.max_body_bytes).unwrap_or(0);
            match RequestBody::buffer_for_replay(body, cap).await {
                Ok(request_body) => request_body,
                Err(err) => {
                    debug!(upstream = %settings.key, error = %err, "Failed to read request body");
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "failed to read request body".into(),
                    );
                }
            }
        } else {
            RequestBody::streaming(body)
        }
    } else {
        match handlers::read_capped(body, exchange.body_cap()).await {
            Ok(handlers::CappedBody::Whole(mut bytes)) => {
                match exchange.body(&mut bytes).await {
                    ExchangeOutcome::Continue | ExchangeOutcome::Proxy(_) => {}
                    ExchangeOutcome::Deny { source, reason } => {
                        net_metrics::connection(direction, PROTOCOL_HTTP, "deny");
                        settings.context.record_denial(
                            settings.allocation_id,
                            direction,
                            PROTOCOL_HTTP,
                            &destination,
                            &source,
                            reason.as_deref(),
                            &method,
                            &path,
                        );
                        if let Some(chain) = chain {
                            access_log(
                                &settings,
                                chain,
                                &method,
                                &path,
                                host_header.as_deref(),
                                403,
                                "deny",
                                Some(&source),
                                started,
                                bytes.len() as u64,
                                0,
                            )
                            .await;
                        }
                        note_edge_denied(&settings);
                        exchange.answered(403);
                        return denied_response(
                            direction,
                            &settings.key,
                            &destination,
                            &source,
                            reason.as_deref(),
                        );
                    }
                    ExchangeOutcome::Respond(local) => {
                        let bytes_in = bytes.len() as u64;
                        return answer_locally(
                            &settings,
                            &conn,
                            chain,
                            exchange,
                            local,
                            &destination,
                            &method,
                            &path,
                            host_header.as_deref(),
                            started,
                            bytes_in,
                        )
                        .await;
                    }
                }
                RequestBody::buffered(bytes)
            }
            Ok(handlers::CappedBody::Over(prefix, rest)) => {
                // Over the cap: a body hop that fails closed refuses it; when
                // every body hop fails open (`onError = "allow"`), it goes
                // on unread, streamed once, as if no hop read bodies.
                if let Some(source) = exchange.body_unreadable_denied_by() {
                    net_metrics::connection(direction, PROTOCOL_HTTP, "deny");
                    note_edge_denied(&settings);
                    return denied_response(
                        direction,
                        &settings.key,
                        &destination,
                        &source,
                        Some("request body exceeds the body hop's maxBytes"),
                    );
                }
                debug!(
                    upstream = %settings.key,
                    cap = exchange.body_cap(),
                    "Request body is over the body hops' maxBytes and they fail open; streaming it unread"
                );
                RequestBody::streaming_after(prefix, rest)
            }
            Err(err) => {
                // The client went away mid-body.
                debug!(upstream = %settings.key, error = %err, "Failed to read request body");
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "failed to read request body".into(),
                );
            }
        }
    };
    // Bytes the client sent: known up front for a buffered body, counted
    // as they stream otherwise (a log line written before the upload ends
    // reports what had arrived).
    let bytes_counter = request_body.counter();
    let bytes_in = || bytes_counter.load(Ordering::Relaxed);

    // A `proxy` hop: one attempt, to the proxy, in absolute form with the
    // Legion attribution headers; the proxy answers for the origin.
    if let Some(target) = hand_off {
        let scheme = if settings.target.as_ref().is_some_and(|t| t.tls) {
            "https"
        } else {
            "http"
        };
        let authority = match settings.target.as_ref() {
            Some(target) => target.addr(),
            None => host_header.clone().unwrap_or_else(|| settings.key.clone()),
        };
        let hand = HandOff {
            scheme,
            authority,
            headers: handlers::legion_headers(&settings.context, &hop_facts),
        };
        // The request may carry an injected credential, or be bound for a
        // TLS origin this node would have encrypted to: it leaves the node
        // only over the mesh. A mesh endpoint is dialed with mTLS wherever
        // it runs (its port refuses plaintext); a plain endpoint is used
        // only on this node.
        let outcome = match hand_off_route(&target) {
            Ok(mesh) => {
                attempt_request(
                    &settings,
                    &target.addr,
                    &parts,
                    &mut request_body,
                    host_header.as_deref(),
                    Some(&hand),
                    mesh.as_ref(),
                    &injected,
                )
                .await
            }
            Err(err) => Err(err),
        };
        return match outcome {
            Ok(response) => {
                let status = response.status();
                net_metrics::connection(direction, PROTOCOL_HTTP, "allow");
                note_edge_request(&settings, None, Some(status.as_u16()), started, bytes_in());
                if let Some(chain) = chain {
                    access_log(
                        &settings,
                        chain,
                        &method,
                        &path,
                        host_header.as_deref(),
                        status.as_u16(),
                        "allow",
                        Some(&target.source),
                        started,
                        bytes_in(),
                        0,
                    )
                    .await;
                }
                exchange.respond(response).await
            }
            Err(err) => {
                settings.context.note_handler(
                    settings.allocation_id,
                    &target.source,
                    "proxy",
                    &target.addr,
                    Err(err.to_string()),
                );
                net_metrics::connection(direction, PROTOCOL_HTTP, "error");
                note_edge_request(&settings, None, None, started, bytes_in());
                error_response(
                    StatusCode::BAD_GATEWAY,
                    format!("proxy handler {}: {err}", target.source),
                )
            }
        };
    }

    // An external target was resolved once and judged above; every attempt
    // dials the first address that passed, never the name again
    // (worker-plane-auth plan §6.11).
    let judged_target: Option<String> = match (judged, settings.target.as_ref()) {
        (Some(addrs), Some(target)) => match addrs.first() {
            Some(addr) => Some(addr.to_string()),
            None => {
                net_metrics::connection(direction, PROTOCOL_HTTP, "error");
                note_edge_request(&settings, None, None, started, bytes_in());
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    format!("{} did not resolve", target.host),
                );
            }
        },
        _ => None,
    };

    // 2. Attempts.
    let max_attempts = 1 + retry.as_ref().map(|r| r.attempts).unwrap_or(0);
    let mut candidates = settings.healthy_endpoints();
    let mut last_error: Option<String> = None;
    let deadline = settings.timeout.map(|t| started + t);

    for attempt in 0..max_attempts {
        if attempt > 0 {
            let Some(retry) = retry.as_ref() else { break };
            // A streamed body went with the first attempt: nothing to send
            // again.
            if !retriable_method
                || !request_body.replayable()
                || !settings.budget.try_take(retry.budget_percent)
            {
                break;
            }
        }
        let (addr, mesh_endpoint, edge_tag) = match settings.target.as_ref() {
            Some(target) => (
                judged_target.clone().unwrap_or_else(|| target.addr()),
                None,
                None,
            ),
            None => match settings.pick(&candidates) {
                Some(endpoint) => {
                    candidates.retain(|e| e.addr != endpoint.addr);
                    let mesh = endpoint.mesh_tls.then(|| endpoint.clone());
                    let tag = endpoint.tags.first().cloned();
                    (endpoint.addr, mesh, tag)
                }
                None => {
                    last_error = Some("no healthy endpoints".into());
                    break;
                }
            },
        };
        // Per-attempt policy for service upstreams, against the endpoint.
        if settings.target.is_none() {
            if let Some(chain) = chain {
                let socket: Option<std::net::SocketAddr> = addr.parse().ok();
                let facts = RequestFacts {
                    host: Some(settings.key.as_str()),
                    ip: socket.map(|s| s.ip()),
                    port: socket.map(|s| s.port()).unwrap_or(0),
                    identity: conn.identity.as_deref(),
                    method: &method,
                    path: path.split('?').next().unwrap_or("/"),
                    headers: &headers,
                };
                if let Verdict::Deny { source, reason } =
                    crate::structs::netpolicy::evaluate_request(chain, &facts)
                {
                    net_metrics::connection(direction, PROTOCOL_HTTP, "deny");
                    settings.context.record_denial(
                        settings.allocation_id,
                        direction,
                        PROTOCOL_HTTP,
                        &addr,
                        &source,
                        reason.as_deref(),
                        &method,
                        facts.path,
                    );
                    note_edge_denied(&settings);
                    return denied_response(
                        direction,
                        &settings.key,
                        &addr,
                        &source,
                        reason.as_deref(),
                    );
                }
            }
        }

        let remaining = deadline.map(|d| d.saturating_duration_since(Instant::now()));
        if remaining.is_some_and(|r| r.is_zero()) {
            last_error = Some("request deadline exceeded".into());
            break;
        }
        // A service endpoint's load and latency feed its balancer; an
        // external target has one destination and nothing to balance.
        let balanced = settings.target.is_none();
        let _in_flight = balanced.then(|| settings.in_flight(&addr));
        let attempt_started = Instant::now();
        let attempt_future = attempt_request(
            &settings,
            &addr,
            &parts,
            &mut request_body,
            host_header.as_deref(),
            None,
            mesh_endpoint.as_ref(),
            &injected,
        );
        let outcome = match remaining {
            Some(remaining) => match tokio::time::timeout(remaining, attempt_future).await {
                Ok(outcome) => outcome,
                Err(_) => Err(AttemptError::Timeout),
            },
            None => attempt_future.await,
        };
        match outcome {
            Ok(response) => {
                let status = response.status();
                settings.note_outcome(&addr, Some(status));
                if balanced {
                    let outcome = if status.is_server_error() {
                        Outcome::Failure
                    } else {
                        Outcome::Latency(attempt_started.elapsed())
                    };
                    settings.balancer.observe(&addr, outcome);
                }
                let retry_this = retry.as_ref().is_some_and(|r| {
                    (r.on_5xx && status.is_server_error())
                        || (r.on_retriable_4xx && matches!(status.as_u16(), 408 | 425 | 429))
                });
                if retry_this
                    && attempt + 1 < max_attempts
                    && retriable_method
                    && request_body.replayable()
                {
                    last_error = Some(format!("upstream answered {}", status.as_u16()));
                    continue;
                }
                net_metrics::connection(direction, PROTOCOL_HTTP, "allow");
                note_edge_request(
                    &settings,
                    edge_tag.as_deref(),
                    Some(status.as_u16()),
                    started,
                    bytes_in(),
                );
                if let Some(chain) = chain {
                    access_log(
                        &settings,
                        chain,
                        &method,
                        &path,
                        host_header.as_deref(),
                        status.as_u16(),
                        "allow",
                        None,
                        started,
                        bytes_in(),
                        0,
                    )
                    .await;
                }
                return exchange.respond(response).await;
            }
            Err(err) => {
                settings.note_outcome(&addr, None);
                if balanced {
                    settings.balancer.observe(&addr, Outcome::Failure);
                }
                let retriable = match &err {
                    AttemptError::Connect(_) => {
                        retry.as_ref().is_some_and(|r| r.on_connect_failure)
                    }
                    AttemptError::Reset(_) => retry.as_ref().is_some_and(|r| r.on_reset),
                    AttemptError::Timeout => false,
                };
                last_error = Some(err.to_string());
                if !retriable {
                    break;
                }
            }
        }
    }

    net_metrics::connection(direction, PROTOCOL_HTTP, "error");
    note_edge_request(&settings, None, None, started, bytes_in());
    let message = last_error.unwrap_or_else(|| "upstream unavailable".into());
    let status = if message.contains("deadline") {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::BAD_GATEWAY
    };
    if let Some(chain) = chain {
        access_log(
            &settings,
            chain,
            &method,
            &path,
            host_header.as_deref(),
            status.as_u16(),
            "error",
            None,
            started,
            bytes_in(),
            0,
        )
        .await;
    }
    error_response(status, message)
}

/// A request a hop answered instead of the upstream (a plugin's local
/// response, or the `503` of a failed plugin): recorded as a denial when it
/// refuses, logged, and sent.
#[allow(clippy::too_many_arguments)]
async fn answer_locally(
    settings: &Arc<HttpSettings>,
    conn: &ConnInfo,
    chain: Option<&EffectiveChain>,
    mut exchange: handlers::Exchange,
    local: LocalResponse,
    destination: &str,
    method: &str,
    path: &str,
    host_header: Option<&str>,
    started: Instant,
    bytes_in: u64,
) -> Response<OutBody> {
    let direction = settings.direction;
    let verdict = if local.denies() { "deny" } else { "allow" };
    net_metrics::connection(direction, PROTOCOL_HTTP, verdict);
    if local.denies() {
        settings.context.record_denial_for(
            settings.allocation_id,
            conn.identity.as_deref(),
            direction,
            PROTOCOL_HTTP,
            destination,
            &local.source,
            local.reason.as_deref(),
            method,
            path.split('?').next().unwrap_or("/"),
        );
        note_edge_denied(settings);
    } else {
        note_edge_request(settings, None, Some(local.status), started, bytes_in);
    }
    if let Some(chain) = chain {
        access_log(
            settings,
            chain,
            method,
            path,
            host_header,
            local.status,
            verdict,
            Some(&local.source),
            started,
            bytes_in,
            local.body.len() as u64,
        )
        .await;
    }
    exchange.answered(local.status);
    local.into_response()
}

/// Apply a rewritten request head (a plugin's) to the request that goes on.
fn apply_head(parts: &mut http::request::Parts, head: &RequestHead) {
    parts.headers.clear();
    for (name, value) in &head.headers {
        if let (Ok(name), Ok(value)) = (
            hyper::header::HeaderName::from_bytes(name.as_bytes()),
            hyper::header::HeaderValue::from_str(value),
        ) {
            parts.headers.append(name, value);
        }
    }
    if let Ok(uri) = head.path.parse::<hyper::Uri>() {
        parts.uri = uri;
    }
}

/// Service-graph sample for one egress request (docs/network-handlers.md,
/// "Service graph"). Ingress listeners are not edges: the graph is who calls
/// whom, recorded on the calling side.
fn note_edge_request(
    settings: &Arc<HttpSettings>,
    tag: Option<&str>,
    status: Option<u16>,
    started: Instant,
    bytes_in: u64,
) {
    if settings.direction != "egress" {
        return;
    }
    settings.context.edges().record_request(
        settings.allocation_id,
        &settings.key,
        tag,
        status,
        started.elapsed().as_millis() as u64,
        bytes_in,
        0,
    );
}

fn note_edge_denied(settings: &Arc<HttpSettings>) {
    if settings.direction != "egress" {
        return;
    }
    settings
        .context
        .edges()
        .record_denied(settings.allocation_id, &settings.key, PROTOCOL_HTTP);
}

enum AttemptError {
    Connect(String),
    Reset(String),
    Timeout,
}

impl std::fmt::Display for AttemptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttemptError::Connect(err) => write!(f, "connect failed: {err}"),
            AttemptError::Reset(err) => write!(f, "upstream reset: {err}"),
            AttemptError::Timeout => write!(f, "request deadline exceeded"),
        }
    }
}

/// A `proxy` hop in HTTP mode: send the request to the proxy in absolute
/// form with the attribution headers.
struct HandOff {
    scheme: &'static str,
    authority: String,
    headers: Vec<(String, String)>,
}

/// How an HTTP hand-off reaches its proxy: over the mesh (the endpoint to
/// dial with mTLS) when the proxy's port is a mesh port, in plaintext when
/// it is on this node, and not at all when it is a plain endpoint on
/// another node. The hand-off carries any injected credential and, for a
/// TLS origin, what the task trusted this node to encrypt, so it never
/// crosses the network in plaintext.
fn hand_off_route(target: &ProxyTarget) -> Result<Option<Endpoint>, AttemptError> {
    if let Some(node_id) = target.mesh_node.as_ref() {
        return Ok(Some(Endpoint {
            addr: target.addr.clone(),
            allocation_id: 0,
            node_id: node_id.clone(),
            tags: Vec::new(),
            mesh_tls: true,
        }));
    }
    if target.remote {
        return Err(AttemptError::Connect(format!(
            "endpoint {} is on another node and not on the mesh; an HTTP hand-off \
             leaves the node only over the mesh (set `mesh = true` on the proxy's \
             service, or use `local = true`)",
            target.addr
        )));
    }
    Ok(None)
}

/// Dial `addr`, send the request, and return the response with its body
/// streamed through. `hand_off` sends it to a proxy instead of the origin;
/// `mesh` dials a mesh endpoint with this node's certificate and names the
/// workload in a PROXY v2 header before the request. On an ingress listener
/// with neither, the request goes to the task itself ([`HttpSettings::task`]).
/// `injected` replaces the task's own headers of the same names (credential
/// injection).
#[allow(clippy::too_many_arguments)]
async fn attempt_request(
    settings: &Arc<HttpSettings>,
    addr: &str,
    parts: &http::request::Parts,
    body: &mut RequestBody,
    host_header: Option<&str>,
    hand_off: Option<&HandOff>,
    mesh: Option<&Endpoint>,
    injected: &Injected,
) -> Result<Response<OutBody>, AttemptError> {
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let uri = match hand_off {
        Some(hand) => format!("{}://{}{}", hand.scheme, hand.authority, path_and_query),
        None => path_and_query.to_string(),
    };
    let mut builder = Request::builder().method(parts.method.clone()).uri(uri);
    for (name, value) in parts.headers.iter() {
        let lower = name.as_str().to_ascii_lowercase();
        if HOP_BY_HOP.contains(&lower.as_str()) || lower == "host" {
            continue;
        }
        // The task's own value for an injected header (a placeholder its
        // SDK insisted on, or anything else) never leaves the node.
        if injected.iter().any(|(injected, _)| *injected == *name) {
            continue;
        }
        builder = builder.header(name, value);
    }
    for (name, value) in injected {
        builder = builder.header(name, value);
    }
    // The origin must see its own name, not the loopback the task dialed.
    let host = match settings.target.as_ref() {
        Some(target) => {
            let default_port = if target.tls { 443 } else { 80 };
            if target.port == default_port && !target.host.contains(':') {
                target.host.clone()
            } else if target.port == default_port {
                format!("[{}]", target.host)
            } else {
                crate::structs::hostport::join(&target.host, target.port)
            }
        }
        None => host_header.unwrap_or(addr).to_string(),
    };
    builder = builder.header("host", host);
    if let Some(hand) = hand_off {
        for (name, value) in &hand.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
    }
    let (head, ()) = builder
        .body(())
        .map_err(|err| AttemptError::Reset(err.to_string()))?
        .into_parts();

    let key = pool_key(settings, addr, hand_off, mesh);
    // Dial before taking the body, so a connect failure leaves a streamed
    // body unsent.
    let pooled = key.as_ref().and_then(|key| settings.pool.checkout(key));
    let mut request = None;
    if let (Some(key), Some(mut sender)) = (key.as_ref(), pooled) {
        let sent = Request::from_parts(head.clone(), body.take()?);
        match sender.try_send_request(sent).await {
            Ok(response) => return Ok(finish(settings, Some(key.clone()), sender, response)),
            Err(mut err) => {
                // The upstream closed the pooled connection under us. A
                // request that never left goes again as it is (its body
                // untouched); one that left goes again only when it is safe
                // to repeat and its body was kept.
                request = err.take_message();
                let unsent = request.is_some();
                if !unsent {
                    if !IDEMPOTENT.contains(&parts.method.as_str()) || !body.replayable() {
                        return Err(AttemptError::Reset(err.into_error().to_string()));
                    }
                    request = Some(Request::from_parts(head.clone(), body.take()?));
                }
                debug!(
                    upstream = %settings.key,
                    destination = addr,
                    unsent,
                    "Pooled upstream connection was closed; retrying on a new one"
                );
            }
        }
    }

    let mut sender = open(settings, addr, hand_off, mesh).await?;
    let request = match request {
        Some(request) => request,
        None => Request::from_parts(head, body.take()?),
    };
    let response = sender
        .send_request(request)
        .await
        .map_err(|err| AttemptError::Reset(err.to_string()))?;
    Ok(finish(settings, key, sender, response))
}

/// A request's body on its way upstream (mesh-control-path plan §6.4):
/// kept whole, so every attempt can send it, or streamed, so it goes once;
/// with the count of client bytes it has carried so far.
struct RequestBody {
    kind: BodyKind,
    counter: Arc<AtomicU64>,
}

enum BodyKind {
    /// Small enough to keep (or read whole by a body hop): replayable.
    Buffered(Bytes),
    /// Streamed as it arrives; taken by the first attempt.
    Streaming(Option<OutBody>),
}

impl RequestBody {
    fn buffered(bytes: Bytes) -> Self {
        RequestBody {
            counter: Arc::new(AtomicU64::new(bytes.len() as u64)),
            kind: BodyKind::Buffered(bytes),
        }
    }

    /// Stream `incoming` (after `prefix`, the part already read) to the
    /// upstream, counting what passes.
    fn streaming_after(prefix: Bytes, incoming: Incoming) -> Self {
        let counter = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&counter);
        let body = Prefixed::new(prefix, incoming)
            .map_frame(move |frame| {
                if let Some(data) = frame.data_ref() {
                    seen.fetch_add(data.len() as u64, Ordering::Relaxed);
                }
                frame
            })
            .map_err(|err| -> handlers::BoxError { Box::new(err) })
            .boxed();
        RequestBody {
            kind: BodyKind::Streaming(Some(body)),
            counter,
        }
    }

    fn streaming(incoming: Incoming) -> Self {
        Self::streaming_after(Bytes::new(), incoming)
    }

    /// Read `incoming` whole when it is at most `cap` bytes, so a retry can
    /// send it again; past `cap`, stream what was read and the rest, once.
    async fn buffer_for_replay(mut incoming: Incoming, cap: u64) -> Result<Self, hyper::Error> {
        let mut kept = bytes::BytesMut::new();
        while let Some(frame) = incoming.frame().await {
            let Ok(data) = frame?.into_data() else {
                // Trailers: not forwarded, as when a body is read whole.
                continue;
            };
            kept.extend_from_slice(&data);
            if kept.len() as u64 > cap {
                let read = kept.len() as u64;
                debug!(
                    read,
                    cap, "Request body is larger than retries.maxBodyBytes; streaming it once"
                );
                return Ok(Self::streaming_after(kept.freeze(), incoming));
            }
        }
        Ok(Self::buffered(kept.freeze()))
    }

    fn counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.counter)
    }

    fn replayable(&self) -> bool {
        matches!(self.kind, BodyKind::Buffered(_))
    }

    /// The body for one attempt: a copy of a kept body, or the stream the
    /// first time only.
    fn take(&mut self) -> Result<OutBody, AttemptError> {
        match &mut self.kind {
            BodyKind::Buffered(bytes) => Ok(full(bytes.clone())),
            BodyKind::Streaming(body) => body.take().ok_or_else(|| {
                AttemptError::Reset("the streamed request body was already sent".into())
            }),
        }
    }
}

/// A body that yields bytes already read, then the rest of the client's
/// stream.
pub(super) struct Prefixed {
    prefix: Option<Bytes>,
    rest: Incoming,
}

impl Prefixed {
    pub(super) fn new(prefix: Bytes, rest: Incoming) -> Self {
        Prefixed {
            prefix: (!prefix.is_empty()).then_some(prefix),
            rest,
        }
    }
}

impl hyper::body::Body for Prefixed {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
        let this = self.get_mut();
        if let Some(prefix) = this.prefix.take() {
            return std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(prefix))));
        }
        std::pin::Pin::new(&mut this.rest).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.prefix.is_none() && self.rest.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        let prefix = self.prefix.as_ref().map(|p| p.len() as u64).unwrap_or(0);
        let rest = self.rest.size_hint();
        let mut hint = hyper::body::SizeHint::new();
        hint.set_lower(rest.lower() + prefix);
        if let Some(upper) = rest.upper() {
            hint.set_upper(upper + prefix);
        }
        hint
    }
}

/// Which pool a request's connection belongs to. A `proxy` hop is never
/// pooled: its requests carry per-request attribution headers in absolute
/// form to a proxy the policy names.
fn pool_key(
    settings: &HttpSettings,
    addr: &str,
    hand_off: Option<&HandOff>,
    mesh: Option<&Endpoint>,
) -> Option<PoolKey> {
    if hand_off.is_some() {
        return None;
    }
    if mesh.is_some() {
        return Some(PoolKey::Mesh(addr.to_string()));
    }
    if settings.task.is_some() {
        return Some(PoolKey::Task);
    }
    match settings.target.as_ref() {
        Some(target) if target.tls => Some(PoolKey::Tls(addr.to_string())),
        _ => Some(PoolKey::Plain(addr.to_string())),
    }
}

/// Open a new upstream connection for one request: to the task (ingress),
/// through a `proxy` hop, to a mesh endpoint with mTLS and a PROXY v2
/// header, or to an endpoint or external target, with TLS when it asks.
async fn open(
    settings: &Arc<HttpSettings>,
    addr: &str,
    hand_off: Option<&HandOff>,
    mesh: Option<&Endpoint>,
) -> Result<Sender, AttemptError> {
    // Ingress: the task itself, through its namespace, its VMM's vsock, or
    // its bridge. A `proxy` hop and a mesh endpoint are dialed on the host,
    // like any endpoint.
    if hand_off.is_none() && mesh.is_none() {
        if let Some(task) = settings.task.as_ref() {
            let stream = task
                .connect()
                .await
                .map_err(|err| AttemptError::Connect(format!("{err:#}")))?;
            return handshake(TokioIo::new(stream)).await;
        }
    }
    // A `proxy` handler's target comes from policy a job may append to, so
    // it is dialed through the host guard. (An external target was already
    // resolved and judged before the attempts; `addr` is the address that
    // passed.)
    let stream = if hand_off.is_some() {
        crate::worker::netd::guard::dial_guarded(&settings.context, addr, DIAL_TIMEOUT)
            .await
            .map_err(AttemptError::Connect)?
    } else {
        crate::worker::netd::listen::dial_tcp(addr, DIAL_TIMEOUT)
            .await
            .map_err(|err| AttemptError::Connect(err.to_string()))?
    };
    let _ = stream.set_nodelay(true);

    if let Some(endpoint) = mesh {
        let connector = settings
            .context
            .mesh_connector()
            .ok_or_else(|| AttemptError::Connect("no mesh certificate yet".into()))?;
        let server_name = crate::worker::netd::tls::server_name(&crate::mesh_ca::node_dns_name(
            &endpoint.node_id,
        ))
        .map_err(|err| AttemptError::Connect(err.to_string()))?;
        let mut tls = connector
            .connect(server_name, stream)
            .await
            .map_err(|err| AttemptError::Connect(format!("mesh tls: {err}")))?;
        if crate::worker::netd::mesh_tls::resumed_with_expired_peer(tls.get_ref().1) {
            return Err(AttemptError::Connect(
                "mesh tls: resumed a session whose peer certificate expired".into(),
            ));
        }
        let header = handlers::proxy_header(
            &settings.context,
            &HopFacts {
                allocation_id: settings.allocation_id,
                direction: "egress",
                protocol: PROTOCOL_HTTP,
                destination: addr,
                ..Default::default()
            },
            None,
            addr.parse().ok(),
        );
        tls.write_all(&header)
            .await
            .map_err(|err| AttemptError::Reset(err.to_string()))?;
        return handshake(TokioIo::new(tls)).await;
    }

    match settings.target.as_ref() {
        Some(target) if target.tls && hand_off.is_none() => {
            let server_name = rustls_pki_types::ServerName::try_from(target.host.clone())
                .map_err(|err| AttemptError::Connect(err.to_string()))?;
            let tls = settings
                .context
                .upstream_tls_connector(settings.allocation_id, &settings.key, settings.ca.as_ref())
                .map_err(AttemptError::Connect)?
                .connect(server_name, stream)
                .await
                .map_err(|err| AttemptError::Connect(format!("tls: {err}")))?;
            handshake(TokioIo::new(tls)).await
        }
        _ => handshake(TokioIo::new(stream)).await,
    }
}

/// Start an HTTP/1.1 client connection over `io`. The connection runs on
/// its own task until the upstream closes it or its sender is dropped.
async fn handshake<I>(io: I) -> Result<Sender, AttemptError>
where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|err| AttemptError::Connect(err.to_string()))?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            debug!(error = %err, "Upstream HTTP connection ended with an error");
        }
    });
    sender
        .ready()
        .await
        .map_err(|err| AttemptError::Reset(err.to_string()))?;
    Ok(sender)
}

/// Strip hop-by-hop headers from an upstream response, and hand its
/// connection back to the pool once the body has been read and the
/// upstream kept the connection alive.
fn finish(
    settings: &Arc<HttpSettings>,
    key: Option<PoolKey>,
    mut sender: Sender,
    response: Response<Incoming>,
) -> Response<OutBody> {
    if let Some(key) = key {
        let pool = settings.pool.clone();
        tokio::spawn(async move {
            // Ready again only after this response's body is done; an error
            // means the upstream closed the connection instead.
            if sender.ready().await.is_ok() {
                pool.checkin(key, sender);
            }
        });
    }
    let (mut parts, body) = response.into_parts();
    for name in HOP_BY_HOP {
        parts.headers.remove(*name);
    }
    Response::from_parts(
        parts,
        body.map_err(|err| Box::new(err) as handlers::BoxError)
            .boxed(),
    )
}

#[allow(clippy::too_many_arguments)]
async fn access_log(
    settings: &Arc<HttpSettings>,
    chain: &EffectiveChain,
    method: &str,
    path: &str,
    host: Option<&str>,
    status: u16,
    verdict: &str,
    source: Option<&str>,
    started: Instant,
    bytes_in: u64,
    bytes_out: u64,
) {
    if !chain.access_log {
        return;
    }
    write_access_line(
        settings, method, path, host, status, verdict, source, started, bytes_in, bytes_out,
    )
    .await;
}

/// Append one NDJSON line to `<dir>/net-access.log`, off the async runtime.
pub(crate) async fn append_access_line(dir: PathBuf, line: serde_json::Value) {
    let path = dir.join("net-access.log");
    let text = format!("{line}\n");
    let _ = tokio::task::spawn_blocking(move || {
        use std::io::Write;
        let _ = std::fs::create_dir_all(&dir);
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
        {
            let _ = file.write_all(text.as_bytes());
        }
    })
    .await;
}

/// One NDJSON line in the allocation's log directory.
#[allow(clippy::too_many_arguments)]
async fn write_access_line(
    settings: &Arc<HttpSettings>,
    method: &str,
    path: &str,
    host: Option<&str>,
    status: u16,
    verdict: &str,
    source: Option<&str>,
    started: Instant,
    bytes_in: u64,
    bytes_out: u64,
) {
    let Some(dir) = settings.log_dir.as_ref() else {
        return;
    };
    let identity = settings.context.identity_for(settings.allocation_id);
    let line = json!({
        "ts": time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        "allocation": settings.allocation_id,
        "job": identity.as_ref().map(|i| i.job_id.clone()).unwrap_or_default(),
        "task": identity.as_ref().map(|i| i.task_name.clone()).unwrap_or_default(),
        "labels": identity.as_ref().map(|i| i.labels.clone()).unwrap_or_default(),
        "direction": settings.direction,
        "protocol": "http",
        "upstream": settings.key,
        "destination": settings.target.as_ref().map(|t| t.addr()).unwrap_or_else(|| settings.key.clone()),
        "method": method,
        "path": path,
        "host": host.unwrap_or_default(),
        "status": status,
        "verdict": verdict,
        "source": source.unwrap_or_default(),
        "bytesIn": bytes_in,
        "bytesOut": bytes_out,
        "durationMs": started.elapsed().as_millis() as u64,
    });
    append_access_line(dir.clone(), line).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_defaults_and_budget() {
        let config: crate::structs::RetryConfig =
            serde_json::from_value(json!({ "attempts": 2 })).unwrap();
        let policy = RetryPolicy::from(Some(&config)).unwrap();
        assert!(policy.on_5xx && policy.on_connect_failure && !policy.on_reset);
        assert_eq!(policy.budget_percent, 20);
        assert!(policy.methods.contains(&"GET".to_string()));
        assert!(!policy.methods.contains(&"POST".to_string()));

        let budget = RetryBudget::new();
        for _ in 0..10 {
            budget.note_request();
        }
        // 20% of 10 requests = 2 retries.
        assert!(budget.try_take(20));
        assert!(budget.try_take(20));
        assert!(!budget.try_take(20));
    }

    #[test]
    fn denied_response_carries_the_rule() {
        let response = denied_response(
            "egress",
            "github",
            "api.github.com:443",
            "policy/p#2",
            Some("nope"),
        );
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response.headers().get("legion-denied-by").unwrap(),
            "policy/p#2"
        );
        assert_eq!(
            response.headers().get("legion-denied-reason").unwrap(),
            "nope"
        );
    }

    /// A raw origin: answers every request on a connection with a short
    /// body, counting connections and requests. With `close_after_reply`
    /// it closes each connection after its first reply, the way a server
    /// with a short keep-alive timeout does to an idle connection.
    async fn counting_origin(close_after_reply: bool) -> (String, Arc<AtomicU64>, Arc<AtomicU64>) {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let connections = Arc::new(AtomicU64::new(0));
        let requests = Arc::new(AtomicU64::new(0));
        let (conns, reqs) = (connections.clone(), requests.clone());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                conns.fetch_add(1, Ordering::SeqCst);
                let reqs = reqs.clone();
                tokio::spawn(async move {
                    let mut stream = BufReader::new(stream);
                    loop {
                        let mut length = 0usize;
                        let mut line = String::new();
                        // Request head, then a Content-Length body.
                        loop {
                            line.clear();
                            match stream.read_line(&mut line).await {
                                Ok(0) | Err(_) => return,
                                Ok(_) => {}
                            }
                            let lower = line.to_ascii_lowercase();
                            if let Some(value) = lower.strip_prefix("content-length:") {
                                length = value.trim().parse().unwrap_or(0);
                            }
                            if line == "\r\n" {
                                break;
                            }
                        }
                        let mut body = vec![0u8; length];
                        if stream.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        reqs.fetch_add(1, Ordering::SeqCst);
                        let reply = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
                        if stream.get_mut().write_all(reply).await.is_err() || close_after_reply {
                            return;
                        }
                    }
                });
            }
        });
        (addr, connections, requests)
    }

    /// An HTTP-mode egress listener for one service endpoint, served on a
    /// loopback port the test dials.
    async fn http_listener(endpoint: &str) -> (Arc<HttpSettings>, u16) {
        let settings = Arc::new(HttpSettings::new(
            1,
            "svc".into(),
            None,
            Some(&UpstreamHttpConfig::default()),
            &[],
            Arc::new(ForwarderContext::default()),
            crate::worker::balance::default_balancer(),
            Arc::new(Mutex::new(Arc::new(vec![Endpoint {
                addr: endpoint.to_string(),
                allocation_id: 2,
                node_id: "node-a".into(),
                tags: Vec::new(),
                mesh_tls: false,
            }]))),
            Arc::new(Mutex::new(HashMap::new())),
            None,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let serving = settings.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, serving.clone()));
            }
        });
        (settings, port)
    }

    /// One request on a new client connection, so only the upstream side
    /// can be reused.
    async fn send(port: u16, method: &str) -> u16 {
        use tokio::io::AsyncReadExt;
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let raw = format!(
            "{method} /x HTTP/1.1\r\nHost: svc\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi"
        );
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut reply = Vec::new();
        stream.read_to_end(&mut reply).await.unwrap();
        String::from_utf8_lossy(&reply)
            .split_whitespace()
            .nth(1)
            .and_then(|status| status.parse().ok())
            .unwrap_or(0)
    }

    async fn wait_for_idle(settings: &HttpSettings, want: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while settings.pool.idle_count() != want {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("pool never reached the expected idle count");
    }

    /// Requests from separate client connections share one upstream
    /// connection: the pool hands it back once each reply is read.
    #[tokio::test]
    async fn requests_reuse_a_pooled_upstream_connection_test() {
        let (origin, connections, requests) = counting_origin(false).await;
        let (settings, port) = http_listener(&origin).await;
        for _ in 0..5 {
            assert_eq!(send(port, "GET").await, 200);
            wait_for_idle(&settings, 1).await;
        }
        assert_eq!(requests.load(Ordering::SeqCst), 5);
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "one upstream connection"
        );
        // Nothing is left counted as in flight once the replies are back.
        assert!(
            crate::worker::balance::inflight_snapshot(&settings.inflight.lock().unwrap())
                .is_empty()
        );
    }

    /// A pooled connection the upstream has since closed is never handed
    /// out; the request goes on a fresh connection and succeeds, POST
    /// included.
    #[tokio::test]
    async fn a_connection_the_upstream_closed_is_not_reused_test() {
        let (origin, connections, requests) = counting_origin(true).await;
        let (settings, port) = http_listener(&origin).await;
        for method in ["GET", "POST", "POST"] {
            assert_eq!(send(port, method).await, 200, "{method}");
        }
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        assert_eq!(connections.load(Ordering::SeqCst), 3);
        // Closed connections leave the pool rather than lingering in it.
        assert_eq!(settings.pool.reap(), 0);
    }

    /// Pools are keyed by how a connection was dialed: a plain connection
    /// to an address is never handed out for TLS or mesh to the same one.
    #[test]
    fn pool_keys_separate_dial_kinds() {
        let plain = PoolKey::Plain("10.0.0.1:80".into());
        let mesh = PoolKey::Mesh("10.0.0.1:80".into());
        let tls = PoolKey::Tls("10.0.0.1:80".into());
        assert_ne!(plain, mesh);
        assert_ne!(plain, tls);
        assert_ne!(mesh, tls);
    }
}

#[cfg(test)]
mod integration {
    //! HTTP mode end to end: a real loopback origin behind an external
    //! upstream, driven through the forwarder's public API.

    use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use crate::structs::{AllocNetPolicy, UpstreamCredential};
    use crate::worker::forwarder::{
        ForwarderContext, SecretValue, UpstreamForwarder, UpstreamSpec,
    };
    use crate::worker::netd::{Denials, NetPolicyCache};
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    static FLAKY_HITS: AtomicUsize = AtomicUsize::new(0);

    async fn free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    }

    /// An origin that echoes method, path, host, and body; answers 503 for
    /// the first `flaky` requests to `/flaky`.
    async fn start_origin(flaky: usize) -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_server = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let hits = hits_for_server.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let hits = hits.clone();
                        async move {
                            hits.fetch_add(1, Ordering::SeqCst);
                            let path = req.uri().path().to_string();
                            // Only `/flaky` counts toward the 503s so the
                            // earlier requests in a test do not use them up.
                            let n = if path == "/flaky" {
                                FLAKY_HITS.fetch_add(1, Ordering::SeqCst)
                            } else {
                                usize::MAX
                            };
                            let host = req
                                .headers()
                                .get("host")
                                .and_then(|h| h.to_str().ok())
                                .unwrap_or_default()
                                .to_string();
                            let method = req.method().to_string();
                            // Every value of the two credential-shaped
                            // headers, so a test can see both what was
                            // injected and that nothing else survived.
                            let auth = ["authorization", "x-api-key"]
                                .iter()
                                .flat_map(|name| {
                                    req.headers().get_all(*name).iter().map(move |v| {
                                        format!("{name}:{}", v.to_str().unwrap_or_default())
                                    })
                                })
                                .collect::<Vec<_>>()
                                .join(",");
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            if path == "/flaky" && n < flaky {
                                return Ok::<_, hyper::Error>(
                                    Response::builder()
                                        .status(503)
                                        .body(Full::new(Bytes::from_static(b"flaky")))
                                        .unwrap(),
                                );
                            }
                            let text = format!(
                                "{method} {path} host={host} auth=[{auth}] body={}",
                                String::from_utf8_lossy(&body)
                            );
                            Ok(Response::new(Full::new(Bytes::from(text))))
                        }
                    });
                    let _ = http1::Builder::new()
                        .half_close(true)
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (port, hits)
    }

    /// Send one raw HTTP/1.1 request and return (status, headers, body).
    async fn request(port: u16, raw: &str) -> (u16, String, String) {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        // `Connection: close` so the response is delimited by EOF; the
        // client never half-closes before reading (neither does curl).
        let raw = raw.replacen("\r\n\r\n", "\r\nConnection: close\r\n\r\n", 1);
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf).to_string();
        let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let status: u16 = head
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        (status, head.to_ascii_lowercase(), body.to_string())
    }

    fn http_policy() -> crate::structs::netpolicy::EffectivePolicy {
        let mut object: NetPolicyObject = serde_json::from_value(serde_json::json!({
            "name": "t",
            "enforcement": "require",
            "egress": {
                "protocol": "http",
                "policy": "deny",
                "rules": [
                    { "when": { "path": "/ok/**" }, "action": "allow" },
                    { "when": { "path": "/flaky" }, "action": "allow" },
                    { "when": { "path": "/secret" }, "action": "deny", "reason": "not for you" }
                ]
            }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        merge_policies(&[object], InlinePolicy::default())
    }

    #[tokio::test]
    async fn http_mode_enforces_policy_dlp_retries_and_host_rewrite() {
        let (origin, hits) = start_origin(1).await;
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let policy = http_policy();
        let hash = policy.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 7,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
        // The origin is on loopback, which the host guard refuses; an
        // operator `allow` entry opens exactly its port.
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: vec![format!("127.0.0.1:{origin}")],
                deny: vec![],
                ..Default::default()
            },
        )
        .unwrap();
        let context = Arc::new(
            ForwarderContext::new(cache, denials.clone()).with_host_guard(Arc::new(guard)),
        );
        let forwarder = UpstreamForwarder::with_context(context);

        let port = free_port().await;
        let http: crate::structs::UpstreamHttpConfig = serde_json::from_value(serde_json::json!({
            "retries": { "attempts": 1, "on": ["5xx"] },
            "timeout": "5s"
        }))
        .unwrap();
        let dlp: crate::structs::hops::HopConfig = serde_json::from_value(serde_json::json!({
            "dlp": { "maxBytes": "64Ki", "patterns": ["sk-[A-Za-z0-9]{8,}"] }
        }))
        .unwrap();
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: 7,
                service: "origin".into(),
                local_port: Some(port),
                http_protocol: false,
                balance: "round_robin".into(),
                to: Some(format!("http://127.0.0.1:{origin}")),
                http: Some(http),
                handlers: vec![dlp],
                credentials: Vec::new(),
                ca: None,
                vip: None,
                canonical_port: None,
                attachment: crate::worker::netd::attach::Attachment::Host,
            }])
            .await;
        assert_eq!(forwarder.active().len(), 1, "listener bound");

        // Allowed path: forwarded, Host rewritten to the origin.
        let (status, _, body) =
            request(port, "GET /ok/x HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").await;
        assert_eq!(status, 200, "{body}");
        assert!(body.starts_with("GET /ok/x host=127.0.0.1:"), "{body}");

        // Denied by a rule: 403 with the rule and reason in headers and body.
        let (status, head, body) = request(port, "GET /secret HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 403);
        assert!(head.contains("legion-denied-by: policy/t#2"), "{head}");
        assert!(head.contains("legion-denied-reason: not for you"), "{head}");
        assert!(body.contains("\"source\":\"policy/t#2\""), "{body}");

        // Fallthrough deny.
        let (status, head, _) = request(port, "GET /elsewhere HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 403);
        assert!(head.contains("legion-denied-by: fallthrough"), "{head}");

        // dlp: a credential-shaped body is rejected before it leaves.
        let payload = "{\"key\":\"sk-abcdefghijk\"}";
        let raw = format!(
            "POST /ok/upload HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{payload}",
            payload.len()
        );
        let (status, head, _) = request(port, &raw).await;
        assert_eq!(status, 403);
        assert!(
            head.contains("legion-denied-by: upstream/origin#0"),
            "{head}"
        );
        let clean = "{\"key\":\"fine\"}";
        let raw = format!(
            "POST /ok/upload HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{clean}",
            clean.len()
        );
        let (status, _, body) = request(port, &raw).await;
        assert_eq!(status, 200, "{body}");
        assert!(body.ends_with(&format!("body={clean}")), "{body}");

        // Retry: the origin's first /flaky answer is 503; one retry gets 200.
        let before = hits.load(Ordering::SeqCst);
        let (status, _, body) = request(port, "GET /flaky HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            hits.load(Ordering::SeqCst) - before,
            2,
            "one attempt plus one retry"
        );

        // Denials were folded for the heartbeat, with method and path.
        let drained = denials.drain();
        assert!(
            drained
                .iter()
                .any(|d| d.source == "policy/t#2" && d.method == "GET" && d.path == "/secret"),
            "{drained:?}"
        );
        assert!(
            drained.iter().any(|d| d.source == "upstream/origin#0"),
            "{drained:?}"
        );
    }

    /// A `dlp` handler declared on the *chain* (here on a policy object) runs
    /// on an HTTP-mode upstream that declares no handlers of its own, and the
    /// chain's `accessLog` writes both requests.
    #[tokio::test]
    async fn chain_dlp_and_log_handlers_run_on_an_http_upstream_test() {
        let (origin, _hits) = start_origin(0).await;
        let mut object: NetPolicyObject = serde_json::from_value(serde_json::json!({
            "name": "t",
            "enforcement": "require",
            "egress": {
                "protocol": "http",
                "policy": "allow",
                "accessLog": true,
                "handlers": [
                    { "dlp": { "patterns": ["sk-[A-Za-z0-9]{8,}"] } }
                ]
            }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let policy = merge_policies(&[object], InlinePolicy::default());
        let hash = policy.hash.clone();
        let cache = Arc::new(NetPolicyCache::new());
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id: 8,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: vec![format!("127.0.0.1:{origin}")],
                deny: vec![],
                ..Default::default()
            },
        )
        .unwrap();
        let logs = tempfile::tempdir().unwrap();
        let context = Arc::new(
            ForwarderContext::new(cache, Arc::new(Denials::new()))
                .with_host_guard(Arc::new(guard))
                .with_logs_base_dir(logs.path().to_path_buf()),
        );
        let forwarder = UpstreamForwarder::with_context(context);
        let port = free_port().await;
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: 8,
                service: "origin".into(),
                local_port: Some(port),
                http_protocol: false,
                balance: "round_robin".into(),
                to: Some(format!("http://127.0.0.1:{origin}")),
                http: None,
                handlers: Vec::new(),
                credentials: Vec::new(),
                ca: None,
                vip: None,
                canonical_port: None,
                attachment: crate::worker::netd::attach::Attachment::Host,
            }])
            .await;

        let payload = "{\"key\":\"sk-abcdefghijk\"}";
        let raw = format!(
            "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{payload}",
            payload.len()
        );
        let (status, head, _) = request(port, &raw).await;
        assert_eq!(status, 403, "{head}");
        assert!(head.contains("legion-denied-by: policy/t#0"), "{head}");

        let (status, _, body) = request(port, "GET /fine HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 200, "{body}");

        // The chain's access log has both requests.
        let log = logs.path().join("8").join("net-access.log");
        let mut text = String::new();
        for _ in 0..50 {
            text = std::fs::read_to_string(&log).unwrap_or_default();
            if text.lines().count() >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(text.contains("\"verdict\":\"deny\""), "{text}");
        assert!(text.contains("\"path\":\"/fine\""), "{text}");
    }

    /// A plugin on an HTTP-mode upstream, end to end: one hop rewrites the
    /// path before the origin sees it, a second (the same module, another
    /// config) rewrites the origin's answer; a trapping one answers `503`.
    #[tokio::test]
    async fn plugins_rewrite_requests_and_responses_on_an_http_upstream_test() {
        let (origin, _hits) = start_origin(0).await;
        let mut object: NetPolicyObject = serde_json::from_value(serde_json::json!({
            "name": "t",
            "enforcement": "require",
            "egress": {
                "protocol": "http",
                "policy": "allow",
                "handlers": [
                    { "plugin": { "name": "c", "config": { "mode": "rewrite" } } },
                    { "plugin": { "name": "c", "config": { "mode": "response" } } }
                ]
            }
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let mut trapping = object.clone();
        trapping.name = "trap".into();
        trapping.egress.as_mut().unwrap().handlers = vec![serde_json::from_value(
            serde_json::json!({ "plugin": { "name": "c", "config": { "mode": "trap" } } }),
        )
        .unwrap()];
        let cache = Arc::new(NetPolicyCache::new());
        let mut assignments = Vec::new();
        for (allocation_id, policy) in [(8, object), (9, trapping)] {
            let policy = merge_policies(&[policy], InlinePolicy::default());
            assignments.push(AllocNetPolicy {
                allocation_id,
                policy_hash: policy.hash.clone(),
            });
            cache.insert_body(&policy.hash.clone(), policy).unwrap();
        }
        cache.apply_assignments(&assignments);
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: vec![format!("127.0.0.1:{origin}")],
                deny: vec![],
                ..Default::default()
            },
        )
        .unwrap();
        let context = Arc::new(
            ForwarderContext::new(cache, Arc::new(Denials::new())).with_host_guard(Arc::new(guard)),
        );
        let mut plugin: crate::structs::netplugin::NetPluginObject =
            serde_json::from_value(serde_json::json!({
                "name": "c",
                "module": { "source": "file:///unused", "checksum": format!("sha256:{}", "0".repeat(64)) },
                "stages": ["request", "response"],
            }))
            .unwrap();
        plugin.normalize_and_validate().unwrap();
        let bytes = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/netplugins/conformance.wasm"
        ))
        .unwrap();
        context
            .plugins()
            .install_compiled(plugin, 1, &bytes)
            .unwrap();
        let forwarder = UpstreamForwarder::with_context(context);
        let (ok_port, trap_port) = (free_port().await, free_port().await);
        let spec = |allocation_id, port| UpstreamSpec {
            allocation_id,
            service: "origin".into(),
            local_port: Some(port),
            http_protocol: false,
            balance: "round_robin".into(),
            to: Some(format!("http://127.0.0.1:{origin}")),
            http: None,
            handlers: Vec::new(),
            credentials: Vec::new(),
            ca: None,
            vip: None,
            canonical_port: None,
            attachment: crate::worker::netd::attach::Attachment::Host,
        };
        forwarder
            .sync(&[spec(8, ok_port), spec(9, trap_port)])
            .await;

        let (status, head, body) =
            request(ok_port, "GET /old/thing HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 200, "{head}");
        assert!(head.contains("x-plugin: seen"), "{head}");
        // Chunked: the plugin may change the length, so it is not declared.
        assert!(body.contains("GET /NEW/THING HOST="), "{body}");

        let (status, head, body) = request(trap_port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 503, "{head}");
        assert!(head.contains("legion-denied-by: policy/trap#0"), "{head}");
        assert!(body.contains("plugin c trapped"), "{body}");
    }

    /// Credential injection: the node sets each declared header from the
    /// value it holds, the task's own value never reaches the origin, and a
    /// header with no value fails closed with a local 503 without dialing.
    /// Submit validation only admits `https://` targets; the forwarder is
    /// built directly here so a plaintext origin can see the headers.
    #[tokio::test]
    async fn credentials_are_injected_and_fail_closed_without_a_value() {
        let (origin, hits) = start_origin(0).await;
        let forwarder = UpstreamForwarder::new();
        let context = forwarder.context();
        let port = free_port().await;
        let credentials: Vec<UpstreamCredential> = serde_json::from_value(serde_json::json!([
            { "header": "authorization", "prefix": "Bearer ", "from": { "kv": "secrets/openai" } },
            { "header": "x-api-key", "from": { "kv": "secrets/key" } },
        ]))
        .unwrap();
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: 9,
                service: "openai".into(),
                local_port: Some(port),
                http_protocol: false,
                balance: "round_robin".into(),
                to: Some(format!("http://127.0.0.1:{origin}")),
                http: None,
                handlers: Vec::new(),
                credentials,
                ca: None,
                vip: None,
                canonical_port: None,
                attachment: crate::worker::netd::attach::Attachment::Host,
            }])
            .await;
        let send = "GET /v1/models HTTP/1.1\r\nHost: x\r\nAuthorization: placeholder\r\nX-Api-Key: dummy\r\nx-api-key: dummy-2\r\n\r\n";

        // Never fetched: 503, named, and the origin is never dialed.
        let (status, head, body) = request(port, send).await;
        assert_eq!(status, 503, "{body}");
        assert!(
            head.contains("legion-denied-by: credential-unavailable"),
            "{head}"
        );
        assert!(
            body.contains("header authorization: kv secrets/openai not fetched yet"),
            "{body}"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "no dial without a credential"
        );

        // One of two held is still not enough.
        context.set_credential(
            9,
            "openai",
            "authorization",
            "secrets/openai",
            1,
            SecretValue::new("sk-live-1"),
        );
        let (status, _, body) = request(port, send).await;
        assert_eq!(status, 503, "{body}");
        assert!(body.contains("kv secrets/key not fetched yet"), "{body}");
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        // Both held: injected, the task's own values dropped.
        context.set_credential(
            9,
            "openai",
            "x-api-key",
            "secrets/key",
            1,
            SecretValue::new("k-1"),
        );
        let (status, _, body) = request(port, send).await;
        assert_eq!(status, 200, "{body}");
        assert!(
            body.contains("auth=[authorization:Bearer sk-live-1,x-api-key:k-1]"),
            "{body}"
        );

        // Rotation: the next request carries the new value.
        context.set_credential(
            9,
            "openai",
            "x-api-key",
            "secrets/key",
            2,
            SecretValue::new("k-2"),
        );
        let (_, _, body) = request(port, send).await;
        assert!(body.contains("x-api-key:k-2]"), "{body}");

        // A transient fetch problem keeps the last good value, and says so.
        context.set_credential_error(
            9,
            "openai",
            "x-api-key",
            "secrets/key",
            "kv secrets/key could not be read".into(),
            false,
        );
        let (status, _, body) = request(port, send).await;
        assert_eq!(status, 200, "{body}");
        assert!(
            context.credential_problems(9)[0].contains("injecting the previous value"),
            "{:?}",
            context.credential_problems(9)
        );

        // A deleted key revokes it: 503 again, naming the key.
        context.set_credential_error(
            9,
            "openai",
            "x-api-key",
            "secrets/key",
            "kv secrets/key not found".into(),
            true,
        );
        let before = hits.load(Ordering::SeqCst);
        let (status, _, body) = request(port, send).await;
        assert_eq!(status, 503, "{body}");
        assert!(body.contains("kv secrets/key not found"), "{body}");
        assert_eq!(hits.load(Ordering::SeqCst), before);
        assert!(
            context.credential_problems(9)[0].contains("requests on this upstream get 503"),
            "{:?}",
            context.credential_problems(9)
        );

        // Allocations that leave take their values with them.
        context.retain_allocations(&std::collections::HashSet::new());
        assert_eq!(context.credential_version(9, "openai", "authorization"), 0);
        assert!(format!("{:?}", SecretValue::new("sk-live-1")).contains("redacted"));
    }

    /// A "proxy task": answers every request with its request line and the
    /// credential it received, and counts the connections it accepted.
    async fn start_forward_proxy() -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                count.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&buf).to_string();
                    let line = head.lines().next().unwrap_or_default().to_string();
                    let auth = head
                        .lines()
                        .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
                        .unwrap_or_default()
                        .to_string();
                    let body = format!("proxy saw {line} | {auth}");
                    let _ = stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await;
                });
            }
        });
        (port, accepted)
    }

    /// An HTTP hand-off carries the injected credential, so it leaves the
    /// node only over the mesh: a mesh endpoint of the proxy's service on
    /// another node is dialed with mTLS and receives the request; a plain
    /// endpoint on another node is never dialed (`502`); a plain endpoint on
    /// this node is used in plaintext.
    #[tokio::test]
    async fn proxy_hand_offs_cross_nodes_only_over_the_mesh_test() {
        use crate::mesh_ca::{generate_ca, issue_node_cert};
        use crate::worker::balance::Endpoint;
        use crate::worker::forwarder::PortTermination;
        use crate::worker::netd::ingress::{IngressListeners, IngressSpec, TaskDial};
        use crate::worker::netd::tls::MeshIdentity;
        use std::net::{IpAddr, Ipv4Addr};

        let (ca_pem, ca_key) = generate_ca().unwrap();
        let (a_cert, a_key, a_exp) = issue_node_cert(&ca_pem, &ca_key, "node-a").unwrap();
        let (b_cert, b_key, b_exp) = issue_node_cert(&ca_pem, &ca_key, "node-b").unwrap();

        // Node B runs the proxy as a `mesh = true` service: the datapath
        // owns the advertised port and terminates mTLS in front of it.
        let (proxy_private, proxy_hits) = start_forward_proxy().await;
        let mesh_port = free_port().await;
        let cache_b = Arc::new(NetPolicyCache::new());
        // `allow = ["*/*"]`: the consumer here has no workload identity to
        // name, and who may call is not what this test is about.
        let anyone = vec!["*/*".to_string()];
        let effective = merge_policies(
            &[],
            InlinePolicy {
                mesh: Some(true),
                callers: &anyone,
                owner: "filter",
                ..Default::default()
            },
        );
        let hash = effective.hash.clone();
        cache_b.apply_assignments(&[AllocNetPolicy {
            allocation_id: 40,
            policy_hash: hash.clone(),
        }]);
        cache_b.insert_body(&hash, effective).unwrap();
        let context_b = Arc::new(
            ForwarderContext::new(cache_b, Arc::new(Denials::new())).with_node_id("node-b".into()),
        );
        context_b
            .set_mesh_identity(
                MeshIdentity::from_pem("node-b", &b_cert, &b_key, &ca_pem, b_exp).unwrap(),
            )
            .unwrap();
        let ingress = IngressListeners::new(context_b);
        ingress
            .sync(&[IngressSpec {
                allocation_id: 40,
                label: "http".into(),
                host_port: mesh_port,
                private_port: proxy_private,
                dial: TaskDial::Loopback { netns: None },
                bind: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                termination: Some(PortTermination::Mesh),
            }])
            .await;

        // Node A: an HTTP-mode upstream with a credential and a proxy hop.
        let (origin, origin_hits) = start_origin(0).await;
        // Everything here is on loopback, which the host guard refuses; an
        // operator `allow` entry opens exactly these ports.
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: vec![
                    format!("127.0.0.1:{origin}"),
                    format!("127.0.0.1:{mesh_port}"),
                    format!("127.0.0.1:{proxy_private}"),
                ],
                deny: vec![],
                ..Default::default()
            },
        )
        .unwrap();
        let context_a = Arc::new(
            ForwarderContext::new(Arc::new(NetPolicyCache::new()), Arc::new(Denials::new()))
                .with_node_id("node-a".into())
                .with_host_guard(Arc::new(guard)),
        );
        context_a
            .set_mesh_identity(
                MeshIdentity::from_pem("node-a", &a_cert, &a_key, &ca_pem, a_exp).unwrap(),
            )
            .unwrap();
        let forwarder = UpstreamForwarder::with_context(context_a.clone());
        let port = free_port().await;
        let credentials: Vec<UpstreamCredential> = serde_json::from_value(serde_json::json!([
            { "header": "authorization", "prefix": "Bearer ", "from": { "kv": "secrets/openai" } },
        ]))
        .unwrap();
        let proxy_hop: crate::structs::hops::HopConfig =
            serde_json::from_value(serde_json::json!({ "proxy": { "service": "filter" } }))
                .unwrap();
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: 9,
                service: "openai".into(),
                local_port: Some(port),
                http_protocol: false,
                balance: "round_robin".into(),
                to: Some(format!("http://127.0.0.1:{origin}")),
                http: None,
                handlers: vec![proxy_hop],
                credentials,
                ca: None,
                vip: None,
                canonical_port: None,
                attachment: crate::worker::netd::attach::Attachment::Host,
            }])
            .await;
        context_a.set_credential(
            9,
            "openai",
            "authorization",
            "secrets/openai",
            1,
            SecretValue::new("sk-live-1"),
        );
        let endpoint = |addr: String, node: &str, mesh_tls: bool| Endpoint {
            addr,
            allocation_id: 40,
            node_id: node.into(),
            tags: Vec::new(),
            mesh_tls,
        };
        let send = "GET /v1/models HTTP/1.1\r\nHost: x\r\n\r\n";

        // The proxy on node B over the mesh: absolute form, credential in.
        context_a.set_handler_endpoints(
            9,
            "filter",
            vec![endpoint(format!("127.0.0.1:{mesh_port}"), "node-b", true)],
        );
        let (status, _, body) = request(port, send).await;
        assert_eq!(status, 200, "{body}");
        assert!(
            body.contains(&format!(
                "proxy saw GET http://127.0.0.1:{origin}/v1/models HTTP/1.1"
            )),
            "{body}"
        );
        assert!(body.contains("Bearer sk-live-1"), "{body}");
        assert_eq!(proxy_hits.load(Ordering::SeqCst), 1);

        // The same proxy as a plain endpoint on node B: never dialed.
        context_a.set_handler_endpoints(
            9,
            "filter",
            vec![endpoint(
                format!("127.0.0.1:{proxy_private}"),
                "node-b",
                false,
            )],
        );
        let (status, _, body) = request(port, send).await;
        assert_eq!(status, 502, "{body}");
        assert!(body.contains("not on the mesh"), "{body}");
        assert_eq!(proxy_hits.load(Ordering::SeqCst), 1, "no plaintext dial");

        // A plain endpoint on this node stays plaintext on the host.
        context_a.set_handler_endpoints(
            9,
            "filter",
            vec![endpoint(
                format!("127.0.0.1:{proxy_private}"),
                "node-a",
                false,
            )],
        );
        let (status, _, body) = request(port, send).await;
        assert_eq!(status, 200, "{body}");
        assert!(body.contains("Bearer sk-live-1"), "{body}");
        assert_eq!(proxy_hits.load(Ordering::SeqCst), 2);
        assert_eq!(origin_hits.load(Ordering::SeqCst), 0, "the proxy answers");
    }

    /// A gated sandbox reaches its upstream over the VMM's vsock host
    /// socket, and the same listener state injects the credential there, so
    /// the key stays out of the guest and its memory checkpoints.
    #[tokio::test]
    async fn credentials_are_injected_on_a_gated_sandboxs_vsock_upstream() {
        use tokio::net::UnixStream;
        let (origin, _hits) = start_origin(0).await;
        let forwarder = UpstreamForwarder::new();
        let context = forwarder.context();
        let dir = tempfile::tempdir().unwrap();
        // The guest's scratch dir: its VMM's socket for local port 20010.
        let path = dir.path().join("vsock.sock_20010");
        let credentials: Vec<UpstreamCredential> = serde_json::from_value(serde_json::json!([
            { "header": "authorization", "prefix": "Bearer ", "from": { "identity": "api" } },
        ]))
        .unwrap();
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: 9,
                service: "api".into(),
                local_port: Some(20010),
                http_protocol: false,
                balance: "round_robin".into(),
                to: Some(format!("http://127.0.0.1:{origin}")),
                http: None,
                handlers: Vec::new(),
                credentials,
                ca: None,
                vip: None,
                canonical_port: None,
                attachment: crate::worker::netd::attach::Attachment::GuestVsock {
                    scratch: dir.path().to_path_buf(),
                },
            }])
            .await;
        context.set_credential(
            9,
            "api",
            "authorization",
            "identity api",
            1_900_000_000,
            SecretValue::new("eyJ.token"),
        );

        let mut stream = UnixStream::connect(&path).await.unwrap();
        stream
            .write_all(
                b"GET /v1/x HTTP/1.1\r\nHost: x\r\nAuthorization: placeholder\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream.read_to_end(&mut buf),
        )
        .await
        .expect("the vsock upstream answered in time")
        .unwrap();
        let text = String::from_utf8_lossy(&buf);
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");
        assert!(
            text.contains("auth=[authorization:Bearer eyJ.token]"),
            "{text}"
        );
    }

    /// A forwarder whose context knows allocation 9 and resolves
    /// `origin.test` to loopback, where the host guard opens only `origin`.
    fn judged_context(origin: u16) -> (Arc<NetPolicyCache>, Arc<Denials>, UpstreamForwarder) {
        let cache = Arc::new(NetPolicyCache::new());
        let denials = Arc::new(Denials::new());
        let resolver = crate::worker::netd::guard::test_support::FakeResolver::new();
        resolver.answer("origin.test", vec![vec!["127.0.0.1".parse().unwrap()]]);
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: vec![format!("127.0.0.1:{origin}")],
                deny: vec![],
                ..Default::default()
            },
        )
        .unwrap();
        let context = ForwarderContext::new(cache.clone(), denials.clone())
            .with_resolver(resolver)
            .with_host_guard(Arc::new(guard));
        (
            cache,
            denials,
            UpstreamForwarder::with_context(Arc::new(context)),
        )
    }

    fn install(cache: &NetPolicyCache, allocation_id: i64, egress: serde_json::Value) {
        let mut object: NetPolicyObject = serde_json::from_value(serde_json::json!({
            "name": "t", "enforcement": "require", "egress": egress,
        }))
        .unwrap();
        object.normalize_and_validate().unwrap();
        let policy = merge_policies(&[object], InlinePolicy::default());
        let hash = policy.hash.clone();
        cache.apply_assignments(&[AllocNetPolicy {
            allocation_id,
            policy_hash: hash.clone(),
        }]);
        cache.insert_body(&hash, policy).unwrap();
    }

    fn external_spec(
        allocation_id: i64,
        port: u16,
        to: String,
        attachment: crate::worker::netd::attach::Attachment,
    ) -> UpstreamSpec {
        UpstreamSpec {
            allocation_id,
            service: "ext".into(),
            local_port: Some(port),
            http_protocol: false,
            balance: "round_robin".into(),
            to: Some(to),
            http: None,
            handlers: Vec::new(),
            credentials: Vec::new(),
            ca: None,
            vip: None,
            canonical_port: None,
            attachment,
        }
    }

    /// A gated allocation's HTTP-mode upstream fails closed like its L4
    /// path: nothing is served until its policy arrives.
    #[tokio::test]
    async fn a_gated_http_upstream_fails_closed_until_its_policy_arrives_test() {
        use tokio::net::UnixStream;
        let (origin, hits) = start_origin(0).await;
        let (cache, denials, forwarder) = judged_context(origin);
        let dir = tempfile::tempdir().unwrap();
        forwarder
            .sync(&[external_spec(
                9,
                20011,
                format!("http://origin.test:{origin}"),
                crate::worker::netd::attach::Attachment::GuestVsock {
                    scratch: dir.path().to_path_buf(),
                },
            )])
            .await;
        let get = || async {
            let mut stream = UnixStream::connect(dir.path().join("vsock.sock_20011"))
                .await
                .unwrap();
            stream
                .write_all(b"GET /x HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf).to_ascii_lowercase()
        };
        let pending = get().await;
        assert!(pending.starts_with("http/1.1 403"), "{pending}");
        assert!(
            pending.contains("legion-denied-by: policy-pending"),
            "{pending}"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0, "nothing reached the origin");
        assert_eq!(denials.drain()[0].source, "policy-pending");

        install(
            &cache,
            9,
            serde_json::json!({ "protocol": "http", "policy": "allow" }),
        );
        let served = get().await;
        assert!(served.starts_with("http/1.1 200"), "{served}");
    }

    /// A CIDR rule judges the address an HTTP-mode upstream's name resolves
    /// to, as it does on the L4 path: the name alone does not get past it.
    #[tokio::test]
    async fn cidr_rules_judge_an_http_upstreams_resolved_address_test() {
        let (origin, hits) = start_origin(0).await;
        let (cache, denials, forwarder) = judged_context(origin);
        install(
            &cache,
            7,
            serde_json::json!({
                "protocol": "http",
                "policy": "allow",
                "rules": [{ "when": { "cidr": "127.0.0.0/8" }, "action": "deny", "reason": "no loopback" }]
            }),
        );
        let port = free_port().await;
        forwarder
            .sync(&[external_spec(
                7,
                port,
                format!("http://origin.test:{origin}"),
                crate::worker::netd::attach::Attachment::Host,
            )])
            .await;
        let (status, head, _) = request(port, "GET /x HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 403, "{head}");
        assert!(head.contains("legion-denied-by: policy/t#0"), "{head}");
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        let recorded = denials.drain();
        assert_eq!(recorded[0].reason, "no loopback", "{recorded:?}");
    }

    #[tokio::test]
    async fn l4_path_is_untouched_without_http() {
        // An external tcp:// upstream with no policy stays a byte splice.
        let (origin, _) = start_origin(0).await;
        let forwarder = UpstreamForwarder::new();
        let port = free_port().await;
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: 8,
                service: "raw".into(),
                local_port: Some(port),
                http_protocol: false,
                balance: "round_robin".into(),
                to: Some(format!("tcp://127.0.0.1:{origin}")),
                http: None,
                handlers: Vec::new(),
                credentials: Vec::new(),
                ca: None,
                vip: None,
                canonical_port: None,
                attachment: crate::worker::netd::attach::Attachment::Host,
            }])
            .await;
        let (status, _, body) =
            request(port, "GET /anything HTTP/1.1\r\nHost: keep.me\r\n\r\n").await;
        assert_eq!(status, 200, "{body}");
        assert!(
            body.contains("host=keep.me"),
            "L4 splice does not touch headers: {body}"
        );
    }

    /// An origin that counts requests, answers 503 on `/fail`, and otherwise
    /// echoes how many body bytes it read.
    async fn start_counting_origin() -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(AtomicUsize::new(0));
        let counted = hits.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let hits = counted.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let hits = hits.clone();
                        async move {
                            hits.fetch_add(1, Ordering::SeqCst);
                            let fail = req.uri().path() == "/fail";
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let status = if fail { 503 } else { 200 };
                            Ok::<_, hyper::Error>(
                                Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from(format!("read={}", body.len()))))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (port, hits)
    }

    /// An HTTP-mode upstream to `origin` with `http` and `handlers`, on a
    /// node whose host guard opens exactly the origin's port.
    async fn http_upstream(
        origin: u16,
        http: serde_json::Value,
        handlers: Vec<crate::structs::hops::HopConfig>,
    ) -> (UpstreamForwarder, u16) {
        let guard = crate::structs::host_guard::HostGuard::new(
            &crate::structs::host_guard::HostGuardConfig {
                allow: vec![format!("127.0.0.1:{origin}")],
                deny: vec![],
                ..Default::default()
            },
        )
        .unwrap();
        let context = Arc::new(
            ForwarderContext::new(Arc::new(NetPolicyCache::new()), Arc::new(Denials::new()))
                .with_host_guard(Arc::new(guard)),
        );
        let forwarder = UpstreamForwarder::with_context(context);
        let port = free_port().await;
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: 7,
                service: "origin".into(),
                local_port: Some(port),
                http_protocol: false,
                balance: "round_robin".into(),
                to: Some(format!("http://127.0.0.1:{origin}")),
                http: Some(serde_json::from_value(http).unwrap()),
                handlers,
                credentials: Vec::new(),
                ca: None,
                vip: None,
                canonical_port: None,
                attachment: crate::worker::netd::attach::Attachment::Host,
            }])
            .await;
        assert_eq!(forwarder.active().len(), 1, "listener bound");
        (forwarder, port)
    }

    fn post(path: &str, bytes: usize) -> String {
        format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {bytes}\r\n\r\n{}",
            "a".repeat(bytes)
        )
    }

    /// Mesh-control-path plan §6.4: with nothing to replay, a body streams
    /// through whatever its size; with retries, a body within
    /// `retries.maxBodyBytes` is kept and retried, and a larger one streams
    /// once and is not retried.
    #[tokio::test]
    async fn request_bodies_stream_unless_a_retry_needs_them_test() {
        let (origin, hits) = start_counting_origin().await;

        // No retries: a 3 MiB upload goes through untouched.
        let (_plain, port) = http_upstream(origin, serde_json::json!({}), Vec::new()).await;
        let big = 3 * 1024 * 1024;
        let (status, _, body) = request(port, &post("/upload", big)).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body, format!("read={big}"));

        let (_retrying, port) = http_upstream(
            origin,
            serde_json::json!({ "retries": {
                "attempts": 1, "on": ["5xx"], "methods": ["POST"], "maxBodyBytes": "1Ki"
            } }),
            Vec::new(),
        )
        .await;
        // Within the cap: kept, and retried once.
        let before = hits.load(Ordering::SeqCst);
        let (status, _, _) = request(port, &post("/fail", 512)).await;
        assert_eq!(status, 503);
        assert_eq!(hits.load(Ordering::SeqCst) - before, 2, "retried");
        // Over the cap: streamed, sent once, and the answer passes through.
        let before = hits.load(Ordering::SeqCst);
        let (status, _, body) = request(port, &post("/fail", 4096)).await;
        assert_eq!(status, 503, "{body}");
        assert_eq!(hits.load(Ordering::SeqCst) - before, 1, "not retried");
        let (status, _, body) = request(port, &post("/upload", 4096)).await;
        assert_eq!(status, 200);
        assert_eq!(body, "read=4096", "the part read for the cap is sent too");
    }

    /// A body hop reads at most its `maxBytes`. Over it, a hop that fails
    /// closed denies, and so does a chain with any such hop; when every
    /// body hop fails open, the body goes on unread, whole (the part read
    /// for the cap and the rest), and no hop judges it.
    #[tokio::test]
    async fn a_body_over_a_body_hops_cap_is_refused_unless_every_hop_fails_open_test() {
        let (origin, hits) = start_counting_origin().await;
        let dlp = |on_error: &str| -> crate::structs::hops::HopConfig {
            serde_json::from_value(serde_json::json!({
                "dlp": { "maxBytes": "1Ki", "patterns": ["sk-[A-Za-z0-9]{8,}"] },
                "onError": on_error
            }))
            .unwrap()
        };
        let (_open, port) = http_upstream(origin, serde_json::json!({}), vec![dlp("allow")]).await;
        let (status, _, body) = request(port, &post("/upload", 4096)).await;
        assert_eq!((status, body.as_str()), (200, "read=4096"));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // Unread means unchecked: a match past the cap is not seen.
        let secret = format!("{}sk-abcdefghijkl", "a".repeat(2048));
        let raw = format!(
            "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{secret}",
            secret.len()
        );
        let (status, _, _) = request(port, &raw).await;
        assert_eq!(status, 200);
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        let (_closed, port) = http_upstream(origin, serde_json::json!({}), vec![dlp("deny")]).await;
        let (status, _, _) = request(port, &post("/upload", 4096)).await;
        assert_eq!(status, 403);
        let (_mixed, mixed_port) = http_upstream(
            origin,
            serde_json::json!({}),
            vec![dlp("allow"), dlp("deny")],
        )
        .await;
        let (status, _, _) = request(mixed_port, &post("/upload", 4096)).await;
        assert_eq!(status, 403);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "no refused body reached the origin"
        );
        // Within the cap, the hop reads it and it goes on.
        let (status, _, body) = request(port, &post("/upload", 100)).await;
        assert_eq!((status, body.as_str()), (200, "read=100"));
    }
}
