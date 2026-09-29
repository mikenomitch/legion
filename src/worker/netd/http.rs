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
//! 2. **Handlers.** `dlp` buffers the request body up to `maxBytes` and
//!    rejects it when a pattern matches; `log` and the chain's `accessLog`
//!    write one NDJSON line per request. (`authz` and `proxy` land with the
//!    external-handlers phase and are skipped here with a debug log.)
//! 3. **Traffic management** (`connect.upstreams[].http`): a per-request
//!    deadline, bounded retries for idempotent methods under a budget, a
//!    weighted split by service tag, and outlier ejection of endpoints that
//!    keep answering 5xx.
//! 4. **The dial.** One upstream connection per request (no pooling yet),
//!    TLS originated for `https://`/`tls://` targets, the `Host` header
//!    rewritten to an external target's host so the origin accepts it.
//!
//! Everything here is bounded: body buffering has a cap, retries have a
//! budget, requests have a deadline. Per-request logging stays at `debug`.

use crate::prelude::*;
use crate::structs::netpolicy::{
    parse_bytes, parse_duration, EffectiveChain, HandlerConfig, RequestFacts, Verdict, HANDLER_DLP,
    HANDLER_LOG, PROTOCOL_HTTP,
};
use crate::structs::{UpstreamHttpConfig, UpstreamTarget};
use crate::worker::balance::{Balancer, Endpoint};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::handlers::{self, HopFacts, HopOutcome, ProxyTarget};
use crate::worker::netd::metrics as net_metrics;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

/// How long an upstream dial may take before it counts as a connect failure.
const DIAL_TIMEOUT: Duration = Duration::from_secs(2);
/// Bodies buffered for `dlp` when the handler names no `maxBytes`.
const DEFAULT_DLP_MAX_BYTES: u64 = 1024 * 1024;
/// Retry budget window.
const BUDGET_WINDOW: Duration = Duration::from_secs(60);
/// Idempotent methods retried by default.
const IDEMPOTENT: &[&str] = &["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"];

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

/// A compiled `dlp` handler.
struct Dlp {
    max_bytes: u64,
    patterns: Vec<regex::Regex>,
    on_error_deny: bool,
    source: String,
}

/// A retry policy with defaults filled in.
#[derive(Clone)]
struct RetryPolicy {
    attempts: u32,
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
    /// For ingress into a gated task: dial the private port inside this
    /// namespace.
    pub netns: Option<PathBuf>,
    pub context: Arc<ForwarderContext>,
    pub balancer: Arc<dyn Balancer>,
    pub endpoints: Arc<Mutex<Vec<Endpoint>>>,
    pub inflight: Arc<Mutex<HashMap<String, usize>>>,
    /// Where the allocation's access log lives (`<logs>/<alloc>/net-access.log`).
    pub log_dir: Option<PathBuf>,
    timeout: Option<Duration>,
    retry: Option<RetryPolicy>,
    budget: RetryBudget,
    split: Vec<(String, u32)>,
    outlier_threshold: Option<u32>,
    outlier_eject: Duration,
    outliers: Mutex<HashMap<String, OutlierState>>,
    dlp: Vec<Dlp>,
    log_handler: bool,
    /// The upstream's own handlers (`connect.upstreams[].handlers`); the
    /// external ones (`authz`, `proxy`) run per request after the chain's.
    pub handlers: Vec<HandlerConfig>,
}

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
        handlers: &[HandlerConfig],
        context: Arc<ForwarderContext>,
        balancer: Arc<dyn Balancer>,
        endpoints: Arc<Mutex<Vec<Endpoint>>>,
        inflight: Arc<Mutex<HashMap<String, usize>>>,
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
        let outlier_threshold = config
            .and_then(|c| c.outlier.as_ref())
            .map(|o| o.consecutive_5xx.unwrap_or(5).max(1));
        let outlier_eject = config
            .and_then(|c| c.outlier.as_ref())
            .and_then(|o| o.eject_for.as_deref())
            .and_then(|d| parse_duration(d).ok())
            .unwrap_or(Duration::from_secs(30));
        let mut dlp = Vec::new();
        let mut log_handler = false;
        for (index, handler) in handlers.iter().enumerate() {
            match handler.handler.as_str() {
                HANDLER_DLP => {
                    let max_bytes = handler
                        .config
                        .get("maxBytes")
                        .and_then(|m| parse_bytes(m).ok())
                        .unwrap_or(DEFAULT_DLP_MAX_BYTES);
                    let patterns = handler
                        .config
                        .get("patterns")
                        .and_then(|p| p.as_array())
                        .map(|p| {
                            p.iter()
                                .filter_map(|v| v.as_str())
                                .filter_map(|v| regex::Regex::new(v).ok())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    dlp.push(Dlp {
                        max_bytes,
                        patterns,
                        on_error_deny: handler.on_error.as_deref() != Some("allow"),
                        source: format!("upstream/{key}#{index}"),
                    });
                }
                HANDLER_LOG => log_handler = true,
                other => debug!(
                    upstream = %key,
                    handler = other,
                    "Handler kind is not served on the datapath yet; skipping"
                ),
            }
        }
        HttpSettings {
            allocation_id,
            key,
            target,
            direction: "egress",
            netns: None,
            context,
            balancer,
            endpoints,
            inflight,
            log_dir,
            timeout,
            retry,
            budget: RetryBudget::new(),
            split,
            outlier_threshold,
            outlier_eject,
            outliers: Mutex::new(HashMap::new()),
            dlp,
            log_handler,
            handlers: handlers.to_vec(),
        }
    }

    /// Settings for an ingress listener: HTTP/1.1 in front of the task's
    /// private port, judged against the *ingress* chain. No traffic
    /// management (there is exactly one backend), no handlers yet.
    pub fn for_ingress(
        allocation_id: i64,
        label: String,
        private_port: u16,
        netns: Option<PathBuf>,
        context: Arc<ForwarderContext>,
        log_dir: Option<PathBuf>,
    ) -> Self {
        let mut settings = HttpSettings::new(
            allocation_id,
            label,
            Some(UpstreamTarget {
                host: "127.0.0.1".to_string(),
                port: private_port,
                tls: false,
                http: true,
            }),
            None,
            &[],
            context,
            crate::worker::balance::default_balancer(),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(HashMap::new())),
            log_dir,
        );
        settings.direction = "ingress";
        settings.netns = netns;
        settings
    }

    /// Endpoints that are not currently ejected.
    fn healthy_endpoints(&self) -> Vec<Endpoint> {
        let endpoints = self
            .endpoints
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
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
        let inflight = self
            .inflight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
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
        let index = self.balancer.pick(&pool, &inflight)?;
        pool.get(index).cloned()
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

type OutBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

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

    // 1. Policy.
    let direction = settings.direction;
    let policy = settings.context.policy_for(settings.allocation_id);
    let chain: Option<&EffectiveChain> = policy.as_ref().and_then(|p| {
        if direction == "ingress" {
            p.ingress.as_ref()
        } else {
            p.egress.as_ref()
        }
    });
    if let Some(chain) = chain {
        let facts = RequestFacts {
            host: Some(fact_host.as_str()),
            ip: fact_host.parse().ok(),
            port: fact_port,
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
                &destination,
                &source,
                reason.as_deref(),
                &method,
                facts.path,
            );
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
            note_edge_denied(&settings);
            return denied_response(
                direction,
                &settings.key,
                &destination,
                &source,
                reason.as_deref(),
            );
        }
    }

    // 1b. External handlers, with the request head: a webhook may deny, a
    // proxy may take the request.
    let path_only = path.split('?').next().unwrap_or("/").to_string();
    let hop_facts = HopFacts {
        allocation_id: settings.allocation_id,
        direction,
        protocol: PROTOCOL_HTTP,
        destination: &destination,
        host: Some(fact_host.as_str()),
        ip: fact_host.parse().ok(),
        port: fact_port,
        identity: conn.identity.as_deref(),
        method: Some(&method),
        path: Some(&path_only),
        headers: &headers,
    };
    let hops = handlers::hops_for(
        &settings.context,
        settings.allocation_id,
        direction,
        Some(&settings.key),
        &settings.handlers,
    );
    let mut hand_off: Option<ProxyTarget> = None;
    if !hops.is_empty() {
        match handlers::run(&settings.context, &hops, &hop_facts).await {
            HopOutcome::Allow => {}
            HopOutcome::Deny { source, reason } => {
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
                    &path_only,
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
            HopOutcome::Proxy(target) => hand_off = Some(target),
        }
    }

    // Split the head from the body. The body is read fully either way (one
    // upstream connection per request, no streaming yet); `dlp` caps it.
    let (parts, body) = req.into_parts();
    let body_bytes: Bytes = if settings.dlp.is_empty() {
        match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(err) => {
                debug!(upstream = %settings.key, error = %err, "Failed to read request body");
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "failed to read request body".into(),
                );
            }
        }
    } else {
        let cap = settings
            .dlp
            .iter()
            .map(|d| d.max_bytes)
            .max()
            .unwrap_or(DEFAULT_DLP_MAX_BYTES);
        match Limited::new(body, cap as usize).collect().await {
            Ok(collected) => {
                let bytes = collected.to_bytes();
                for dlp in &settings.dlp {
                    if bytes.len() as u64 > dlp.max_bytes {
                        if dlp.on_error_deny {
                            net_metrics::connection(direction, PROTOCOL_HTTP, "deny");
                            settings.context.record_denial(
                                settings.allocation_id,
                                direction,
                                PROTOCOL_HTTP,
                                &destination,
                                &dlp.source,
                                Some("body larger than dlp maxBytes"),
                                &method,
                                &path,
                            );
                            note_edge_denied(&settings);
                            return denied_response(
                                direction,
                                &settings.key,
                                &destination,
                                &dlp.source,
                                Some("body larger than dlp maxBytes"),
                            );
                        }
                        continue;
                    }
                    let text = String::from_utf8_lossy(&bytes);
                    if let Some(pattern) = dlp.patterns.iter().find(|p| p.is_match(&text)) {
                        let reason = format!("body matches dlp pattern {}", pattern.as_str());
                        net_metrics::connection(direction, PROTOCOL_HTTP, "deny");
                        settings.context.record_denial(
                            settings.allocation_id,
                            direction,
                            PROTOCOL_HTTP,
                            &destination,
                            &dlp.source,
                            Some(&reason),
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
                                Some(&dlp.source),
                                started,
                                bytes.len() as u64,
                                0,
                            )
                            .await;
                        }
                        note_edge_denied(&settings);
                        return denied_response(
                            direction,
                            &settings.key,
                            &destination,
                            &dlp.source,
                            Some(&reason),
                        );
                    }
                }
                bytes
            }
            Err(err) => {
                // Over the cap, or the client went away mid-body.
                let deny = settings.dlp.iter().any(|d| d.on_error_deny);
                debug!(upstream = %settings.key, error = %err, "dlp could not buffer the request body");
                if deny {
                    net_metrics::connection(direction, PROTOCOL_HTTP, "deny");
                    let source = settings
                        .dlp
                        .first()
                        .map(|d| d.source.clone())
                        .unwrap_or_default();
                    note_edge_denied(&settings);
                    return denied_response(
                        direction,
                        &settings.key,
                        &destination,
                        &source,
                        Some("request body exceeds dlp maxBytes"),
                    );
                }
                return error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body exceeds dlp maxBytes".into(),
                );
            }
        }
    };
    let bytes_in = body_bytes.len() as u64;

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
        let outcome = attempt_request(
            &settings,
            &target.addr,
            &parts,
            body_bytes.clone(),
            host_header.as_deref(),
            Some(&hand),
            None,
        )
        .await;
        return match outcome {
            Ok(response) => {
                let status = response.status();
                net_metrics::connection(direction, PROTOCOL_HTTP, "allow");
                note_edge_request(&settings, None, Some(status.as_u16()), started, bytes_in);
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
                        bytes_in,
                        0,
                    )
                    .await;
                }
                response
            }
            Err(err) => {
                settings.context.note_handler(
                    settings.allocation_id,
                    &target.source,
                    crate::structs::netpolicy::HANDLER_PROXY,
                    &target.addr,
                    Err(err.to_string()),
                );
                net_metrics::connection(direction, PROTOCOL_HTTP, "error");
                note_edge_request(&settings, None, None, started, bytes_in);
                error_response(
                    StatusCode::BAD_GATEWAY,
                    format!("proxy handler {}: {err}", target.source),
                )
            }
        };
    }

    // 2. Attempts.
    let retry = settings.retry.clone();
    let max_attempts = 1 + retry.as_ref().map(|r| r.attempts).unwrap_or(0);
    let retriable_method = retry
        .as_ref()
        .is_some_and(|r| r.methods.iter().any(|m| m == &method));
    let mut candidates = settings.healthy_endpoints();
    let mut last_error: Option<String> = None;
    let deadline = settings.timeout.map(|t| started + t);

    for attempt in 0..max_attempts {
        if attempt > 0 {
            let Some(retry) = retry.as_ref() else { break };
            if !retriable_method || !settings.budget.try_take(retry.budget_percent) {
                break;
            }
        }
        let (addr, mesh_endpoint, edge_tag) = match settings.target.as_ref() {
            Some(target) => (target.addr(), None, None),
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
        let attempt_future = attempt_request(
            &settings,
            &addr,
            &parts,
            body_bytes.clone(),
            host_header.as_deref(),
            None,
            mesh_endpoint.as_ref(),
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
                let retry_this = retry.as_ref().is_some_and(|r| {
                    (r.on_5xx && status.is_server_error())
                        || (r.on_retriable_4xx && matches!(status.as_u16(), 408 | 425 | 429))
                });
                if retry_this && attempt + 1 < max_attempts && retriable_method {
                    last_error = Some(format!("upstream answered {}", status.as_u16()));
                    continue;
                }
                net_metrics::connection(direction, PROTOCOL_HTTP, "allow");
                note_edge_request(
                    &settings,
                    edge_tag.as_deref(),
                    Some(status.as_u16()),
                    started,
                    bytes_in,
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
                        bytes_in,
                        0,
                    )
                    .await;
                } else if settings.log_handler {
                    write_access_line(
                        &settings,
                        &method,
                        &path,
                        host_header.as_deref(),
                        status.as_u16(),
                        "allow",
                        None,
                        started,
                        bytes_in,
                        0,
                    )
                    .await;
                }
                return response;
            }
            Err(err) => {
                settings.note_outcome(&addr, None);
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
    note_edge_request(&settings, None, None, started, bytes_in);
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
            bytes_in,
            0,
        )
        .await;
    }
    error_response(status, message)
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

/// Dial `addr`, send the request, and return the response with its body
/// streamed through. `hand_off` sends it to a proxy instead of the origin;
/// `mesh` dials a mesh endpoint with this node's certificate and names the
/// workload in a PROXY v2 header before the request.
async fn attempt_request(
    settings: &Arc<HttpSettings>,
    addr: &str,
    parts: &http::request::Parts,
    body: Bytes,
    host_header: Option<&str>,
    hand_off: Option<&HandOff>,
    mesh: Option<&Endpoint>,
) -> Result<Response<OutBody>, AttemptError> {
    let stream = match settings.netns.clone() {
        Some(netns) => {
            let socket: std::net::SocketAddr = addr
                .parse()
                .map_err(|_| AttemptError::Connect(format!("invalid address {addr}")))?;
            crate::worker::netd::netns::connect_tcp(Some(netns), socket)
                .await
                .map_err(|err| AttemptError::Connect(err.to_string()))?
        }
        None => match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => return Err(AttemptError::Connect(err.to_string())),
            Err(_) => return Err(AttemptError::Connect("dial timed out".into())),
        },
    };
    let _ = stream.set_nodelay(true);

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
        builder = builder.header(name, value);
    }
    // The origin must see its own name, not the loopback the task dialed.
    let host = match settings.target.as_ref() {
        Some(target) => {
            let default_port = if target.tls { 443 } else { 80 };
            if target.port == default_port {
                target.host.clone()
            } else {
                format!("{}:{}", target.host, target.port)
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
    let request = builder
        .body(full(body))
        .map_err(|err| AttemptError::Reset(err.to_string()))?;

    if let Some(endpoint) = mesh {
        let connector = settings
            .context
            .mesh_connector()
            .ok_or_else(|| AttemptError::Connect("no mesh certificate yet".into()))?;
        let server_name = crate::worker::netd::tls::server_name(
            &crate::manager::mesh_ca::node_dns_name(&endpoint.node_id),
        )
        .map_err(|err| AttemptError::Connect(err.to_string()))?;
        let mut tls = connector
            .connect(server_name, stream)
            .await
            .map_err(|err| AttemptError::Connect(format!("mesh tls: {err}")))?;
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
        return send_over(TokioIo::new(tls), request).await;
    }

    let response = match settings.target.as_ref() {
        Some(target) if target.tls && hand_off.is_none() => {
            let server_name = rustls_pki_types::ServerName::try_from(target.host.clone())
                .map_err(|err| AttemptError::Connect(err.to_string()))?;
            let tls = settings
                .context
                .tls_connector()
                .connect(server_name, stream)
                .await
                .map_err(|err| AttemptError::Connect(format!("tls: {err}")))?;
            send_over(TokioIo::new(tls), request).await?
        }
        _ => send_over(TokioIo::new(stream), request).await?,
    };
    Ok(response)
}

async fn send_over<I>(io: I, request: Request<OutBody>) -> Result<Response<OutBody>, AttemptError>
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
    let response = sender
        .send_request(request)
        .await
        .map_err(|err| AttemptError::Reset(err.to_string()))?;
    let (parts, body) = response.into_parts();
    let mut parts = parts;
    for name in HOP_BY_HOP {
        parts.headers.remove(*name);
    }
    Ok(Response::from_parts(parts, body.boxed()))
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
    if !chain.access_log && !settings.log_handler {
        return;
    }
    write_access_line(
        settings, method, path, host, status, verdict, source, started, bytes_in, bytes_out,
    )
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
    let path = dir.join("net-access.log");
    let dir = dir.clone();
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
}

#[cfg(test)]
mod integration {
    //! HTTP mode end to end: a real loopback origin behind an external
    //! upstream, driven through the forwarder's public API.

    use crate::structs::netpolicy::{merge_policies, InlinePolicy, NetPolicyObject};
    use crate::structs::AllocNetPolicy;
    use crate::worker::forwarder::{ForwarderContext, UpstreamForwarder, UpstreamSpec};
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
                                "{method} {path} host={host} body={}",
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
                    { "match": { "path": "/ok/**" }, "action": "allow" },
                    { "match": { "path": "/flaky" }, "action": "allow" },
                    { "match": { "path": "/secret" }, "action": "deny", "reason": "not for you" }
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
        let context = Arc::new(ForwarderContext::new(cache, denials.clone()));
        let forwarder = UpstreamForwarder::with_context(context);

        let port = free_port().await;
        let http: crate::structs::UpstreamHttpConfig = serde_json::from_value(serde_json::json!({
            "retries": { "attempts": 1, "on": ["5xx"] },
            "timeout": "5s"
        }))
        .unwrap();
        let dlp: crate::structs::netpolicy::HandlerConfig =
            serde_json::from_value(serde_json::json!({
                "handler": "dlp",
                "config": { "maxBytes": "64Ki", "patterns": ["sk-[A-Za-z0-9]{8,}"] }
            }))
            .unwrap();
        forwarder
            .sync(&[UpstreamSpec {
                allocation_id: 7,
                service: "origin".into(),
                local_port: port,
                balance: "round_robin".into(),
                to: Some(format!("http://127.0.0.1:{origin}")),
                http: Some(http),
                handlers: vec![dlp],
                vip: None,
                canonical_port: None,
                netns: None,
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
                local_port: port,
                balance: "round_robin".into(),
                to: Some(format!("tcp://127.0.0.1:{origin}")),
                http: None,
                handlers: Vec::new(),
                vip: None,
                canonical_port: None,
                netns: None,
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
}
