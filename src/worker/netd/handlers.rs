//! Handlers — code on the path, only when a chain or an upstream names it
//! (docs/network-handlers.md, "Handlers").
//!
//! Every hop is one [`Handler`]: a kind from the registry in
//! [`crate::structs::hops`] with its declared stages and placement. A chain's
//! hops, then an upstream's own, compile once per policy body into one
//! [`Pipeline`], and every path runs that same pipeline at the stages it can
//! see:
//!
//! - **connect** — an L4 upstream splice, the gated proxy's `CONNECT` and
//!   an L4 ingress splice: [`Pipeline::run_connect`] with the
//!   connect-time facts.
//! - **request** — an HTTP-mode upstream, an absolute-URI request through the
//!   gated proxy, HTTP-mode ingress: [`Pipeline::run_request`] with the
//!   request head.
//! - **body** — the same HTTP paths: [`Pipeline::check_body`] on the buffered
//!   body, up to [`Pipeline::body_cap`].
//!
//! The kinds: `authz` (a decision webhook), `proxy` (hand the flow to another
//! proxy), and `dlp` (reject matching request bodies). Every hop has a
//! `timeout` and an `onError`; the default is `deny`, so a dead webhook or a
//! proxy with no endpoints fails the chain closed unless the hop says
//! `onError = "allow"`. Health per hop is reported on the heartbeat
//! (`NetHandlerStatus`) and read by `net doctor`.
//!
//! There is one list of hops per flow, so a hop a chain declares cannot be
//! accepted at submit and then skipped by one path.

mod authz;
mod dlp;
mod plugin;
mod proxy;

pub use authz::DEFAULT_AUTHZ_CACHE;
pub use dlp::DEFAULT_DLP_MAX_BYTES;

use crate::prelude::*;
use crate::structs::hops::{HandlerEntry, HandlerKind, HopConfig};
use crate::structs::netpolicy::{EffectiveChain, EffectivePolicy};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::proxyproto::{self, tlv};

use std::net::{IpAddr, SocketAddr};

/// Cap on cached verdicts per node; past it the cache is cleared.
pub const AUTHZ_CACHE_MAX: usize = 4096;

/// What a hop is told about the flow.
#[derive(Debug, Clone, Default)]
pub struct HopFacts<'a> {
    pub allocation_id: i64,
    /// `egress` or `ingress`.
    pub direction: &'a str,
    /// `tcp` or `http`.
    pub protocol: &'a str,
    /// `host:port` — the egress destination or the ingress peer.
    pub destination: &'a str,
    pub host: Option<&'a str>,
    pub ip: Option<IpAddr>,
    pub port: u16,
    /// Verified peer identity (ingress with mTLS).
    pub identity: Option<&'a str>,
    /// The upstream key, on an upstream's path.
    pub upstream: Option<&'a str>,
    /// HTTP mode only.
    pub method: Option<&'a str>,
    pub path: Option<&'a str>,
    pub headers: &'a [(String, String)],
}

/// A `proxy` hop's resolved target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyTarget {
    pub source: String,
    /// `host:port` to dial.
    pub addr: String,
    /// Prepend a PROXY v2 header (TCP) — `proxyProtocol`, default true.
    pub proxy_protocol: bool,
}

/// What the handlers decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HopOutcome {
    Allow,
    Deny {
        source: String,
        reason: Option<String>,
    },
    /// Hand the flow to this proxy instead of the destination.
    Proxy(ProxyTarget),
}

/// One hop on the path. Each kind implements the stages it acts at; the
/// rest default to doing nothing.
#[async_trait::async_trait]
pub trait Handler: Send + Sync {
    /// The kind's registry row: its name, stages, and placement.
    fn entry(&self) -> &'static HandlerEntry;

    /// Where the hop came from: `policy/<name>#i`, `task#i`, or
    /// `upstream/<key>#i`.
    fn source(&self) -> &str;

    /// Whether a failure of this hop lets the flow through (`onError`).
    fn fails_open(&self) -> bool {
        false
    }

    /// When the flow opens, with the connect-time facts.
    async fn on_connect(
        &self,
        _context: &Arc<ForwarderContext>,
        _facts: &HopFacts<'_>,
    ) -> HopOutcome {
        HopOutcome::Allow
    }

    /// Per HTTP request, with the request head.
    async fn on_request(
        &self,
        _context: &Arc<ForwarderContext>,
        _facts: &HopFacts<'_>,
    ) -> HopOutcome {
        HopOutcome::Allow
    }

    /// How much of a request body this hop reads; `None` when it reads none.
    fn body_cap(&self) -> Option<u64> {
        None
    }

    /// Per HTTP request, with the buffered body: `Some(reason)` to deny.
    fn on_body(&self, _body: &[u8]) -> Option<String> {
        None
    }

    /// Per-exchange state for an HTTP path, for kinds that keep any; `None`
    /// runs the stateless stage methods above.
    fn exchange(
        &self,
        _context: &Arc<ForwarderContext>,
        _facts: &HopFacts<'_>,
    ) -> Option<Box<dyn HopExchange>> {
        None
    }
}

/// The body type the HTTP paths answer with.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type HopBody = http_body_util::combinators::BoxBody<bytes::Bytes, BoxError>;

/// The request head the HTTP-stage hops see, and a plugin may rewrite.
#[derive(Debug, Clone, Default)]
pub struct RequestHead {
    pub method: String,
    /// The path with its query.
    pub path: String,
    pub authority: String,
    /// `http` or `https`.
    pub scheme: String,
    /// Lowercase names, in order.
    pub headers: Vec<(String, String)>,
    /// The request carries no body.
    pub end_of_stream: bool,
    /// A hop changed `path` or `headers`; the path applies them.
    pub changed: bool,
}

impl RequestHead {
    pub fn path_only(&self) -> &str {
        self.path.split('?').next().unwrap_or("/")
    }

    /// Whether a request with these headers has no body.
    pub fn no_body(headers: &[(String, String)]) -> bool {
        let length = headers
            .iter()
            .find(|(name, _)| name == "content-length")
            .map(|(_, value)| value.trim() == "0");
        let chunked = headers.iter().any(|(name, _)| name == "transfer-encoding");
        !chunked && length.unwrap_or(true)
    }
}

/// The response head the `response`-stage hops see, and may rewrite.
#[derive(Debug, Clone, Default)]
pub struct ResponseHead {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub changed: bool,
}

/// A response a hop sends instead of the upstream's: a plugin's
/// `send_local_response`, or the `503` a failed plugin answers with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalResponse {
    /// The hop that answered (`policy/<name>#<i>`, `task#<i>`, ...).
    pub source: String,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Why, for `Legion-Denied-Reason`, the denial ledger, and the access
    /// log.
    pub reason: Option<String>,
}

impl LocalResponse {
    /// Whether this answer refuses the request (a 4xx or 5xx), so it is
    /// recorded as a denial and carries `Legion-Denied-By`.
    pub fn denies(&self) -> bool {
        self.status >= 400
    }

    pub fn into_response(self) -> hyper::Response<HopBody> {
        use http_body_util::BodyExt;
        let status = hyper::StatusCode::from_u16(self.status)
            .unwrap_or(hyper::StatusCode::SERVICE_UNAVAILABLE);
        let mut builder = hyper::Response::builder().status(status);
        let has = |name: &str| {
            self.headers
                .iter()
                .any(|(n, _)| n.eq_ignore_ascii_case(name))
        };
        let denied_by = !has("legion-denied-by") && self.denies();
        let reason = !has("legion-denied-reason") && self.denies();
        let typed = has("content-type");
        for (name, value) in &self.headers {
            if let (Ok(name), Ok(value)) = (
                hyper::header::HeaderName::from_bytes(name.as_bytes()),
                hyper::header::HeaderValue::from_str(value),
            ) {
                builder = builder.header(name, value);
            }
        }
        if denied_by {
            builder = builder.header("legion-denied-by", self.source.as_str());
        }
        if reason {
            if let Some(Ok(value)) = self
                .reason
                .as_deref()
                .map(hyper::header::HeaderValue::from_str)
            {
                builder = builder.header("legion-denied-reason", value);
            }
        }
        if !typed && !self.body.is_empty() && self.body.first() == Some(&b'{') {
            builder = builder.header("content-type", "application/json");
        }
        builder
            .body(
                http_body_util::Full::new(bytes::Bytes::from(self.body))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .unwrap_or_else(|_| {
                hyper::Response::builder()
                    .status(hyper::StatusCode::SERVICE_UNAVAILABLE)
                    .body(
                        http_body_util::Empty::new()
                            .map_err(|never| match never {})
                            .boxed(),
                    )
                    .expect("static response")
            })
    }
}

/// What the HTTP-stage hops decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeOutcome {
    Continue,
    Deny {
        source: String,
        reason: Option<String>,
    },
    /// Hand the request to this proxy instead of the destination.
    Proxy(ProxyTarget),
    /// Answer with this instead of sending the request on.
    Respond(LocalResponse),
}

/// A hop's state for one HTTP exchange, for kinds that keep any (plugins:
/// their stream context lives across the request, body, response, and
/// done stages).
#[async_trait::async_trait]
pub trait HopExchange: Send {
    async fn on_request(
        &mut self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
        head: &mut RequestHead,
    ) -> ExchangeOutcome;

    /// How much of the request body this hop wants buffered.
    fn body_cap(&self) -> Option<u64> {
        None
    }

    /// The buffered request body; the hop may replace it.
    async fn on_body(
        &mut self,
        _context: &Arc<ForwarderContext>,
        _body: &mut bytes::Bytes,
    ) -> ExchangeOutcome {
        ExchangeOutcome::Continue
    }

    /// Whether the hop acts on the response (head or body) or after it.
    fn reads_response(&self) -> bool {
        false
    }

    /// The response head; `Respond` replaces the response.
    async fn on_response(
        &mut self,
        _context: &Arc<ForwarderContext>,
        _head: &mut ResponseHead,
    ) -> ExchangeOutcome {
        ExchangeOutcome::Continue
    }

    /// Whether the hop reads the response body.
    fn reads_response_body(&self) -> bool {
        false
    }

    /// One chunk of the response body as it streams (`end` on the last).
    /// Returns the bytes to pass on, which may be fewer (the hop is holding
    /// them to see more) or rewritten. `Err` aborts the response.
    async fn on_response_chunk(
        &mut self,
        _context: &Arc<ForwarderContext>,
        chunk: bytes::Bytes,
        _end: bool,
    ) -> Result<bytes::Bytes, String> {
        Ok(chunk)
    }

    /// The exchange is over (answered, refused, or abandoned): `status` is
    /// what the client got, 0 when it never got one.
    fn on_done(&mut self, _status: u16) {}
}

/// Build the runtime hop for one configured hop.
fn build(hop: &HopConfig, source: String) -> Arc<dyn Handler> {
    match &hop.kind {
        HandlerKind::Authz(config) => {
            Arc::new(authz::Authz::new(source, config.clone(), hop.clone()))
        }
        HandlerKind::Proxy(config) => {
            Arc::new(proxy::Proxy::new(source, config.clone(), hop.clone()))
        }
        HandlerKind::Dlp(config) => Arc::new(dlp::Dlp::new(source, config.clone(), hop.clone())),
        HandlerKind::Plugin(config) => {
            Arc::new(plugin::Plugin::new(source, config.clone(), hop.clone()))
        }
    }
}

/// The hops one flow runs: the chain's, then the upstream's own.
#[derive(Default)]
pub struct Pipeline {
    hops: Vec<Arc<dyn Handler>>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.hops
                    .iter()
                    .map(|hop| (hop.entry().name, hop.source().to_string())),
            )
            .finish()
    }
}

impl Pipeline {
    /// Compile a chain's hops (sources already set by the merge) and then an
    /// upstream's own (`upstream/<key>#i`).
    pub fn compile(
        chain: Option<&EffectiveChain>,
        upstream_key: Option<&str>,
        upstream: &[HopConfig],
    ) -> Pipeline {
        let mut hops: Vec<Arc<dyn Handler>> = Vec::new();
        if let Some(chain) = chain {
            for hop in &chain.handlers {
                hops.push(build(&hop.handler, hop.source.clone()));
            }
        }
        for (index, hop) in upstream.iter().enumerate() {
            hops.push(build(
                hop,
                format!("upstream/{}#{index}", upstream_key.unwrap_or("?")),
            ));
        }
        Pipeline { hops }
    }

    pub fn is_empty(&self) -> bool {
        self.hops.is_empty()
    }

    /// The `(kind, source)` of every hop, in order.
    pub fn describe(&self) -> Vec<(&'static str, String)> {
        self.hops
            .iter()
            .map(|hop| (hop.entry().name, hop.source().to_string()))
            .collect()
    }

    /// Run the hops at `connect`. The first `deny` ends the walk; the first
    /// `proxy` takes the flow (hops after it could not see the bytes anyway).
    pub async fn run_connect(
        &self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> HopOutcome {
        for hop in &self.hops {
            match hop.on_connect(context, facts).await {
                HopOutcome::Allow => {}
                decided => return decided,
            }
        }
        HopOutcome::Allow
    }

    /// Run the hops at `request`, with the request head in `facts`.
    pub async fn run_request(
        &self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> HopOutcome {
        for hop in &self.hops {
            match hop.on_request(context, facts).await {
                HopOutcome::Allow => {}
                decided => return decided,
            }
        }
        HopOutcome::Allow
    }

    /// Whether any hop reads the request body.
    pub fn reads_body(&self) -> bool {
        self.hops.iter().any(|hop| hop.body_cap().is_some())
    }

    /// The most any body-reading hop wants buffered.
    pub fn body_cap(&self) -> u64 {
        self.hops
            .iter()
            .filter_map(|hop| hop.body_cap())
            .max()
            .unwrap_or(DEFAULT_DLP_MAX_BYTES)
    }

    /// Run the body hops: `Some((source, reason))` to deny.
    pub fn check_body(&self, body: &[u8]) -> Option<(String, String)> {
        self.hops.iter().find_map(|hop| {
            hop.on_body(body)
                .map(|reason| (hop.source().to_string(), reason))
        })
    }

    /// When the body could not be buffered (over the cap, or the client went
    /// away): the first body hop that fails closed, which decides the denial.
    pub fn body_unreadable_denied_by(&self) -> Option<String> {
        self.hops
            .iter()
            .find(|hop| hop.body_cap().is_some() && !hop.fails_open())
            .map(|hop| hop.source().to_string())
    }
}

/// One HTTP exchange through a pipeline: every hop at every HTTP stage, in
/// order, with the per-exchange state of the kinds that keep any. The
/// request, body, response, and done stages all run through it, so a plugin
/// sees one stream context for the whole exchange.
pub struct Exchange {
    context: Arc<ForwarderContext>,
    pipeline: Arc<Pipeline>,
    states: Vec<Option<Box<dyn HopExchange>>>,
    status: u16,
    done: bool,
}

impl Pipeline {
    /// Start an exchange (one HTTP request) through this pipeline.
    pub fn exchange(
        self: &Arc<Self>,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> Exchange {
        Exchange {
            context: context.clone(),
            states: self
                .hops
                .iter()
                .map(|hop| hop.exchange(context, facts))
                .collect(),
            pipeline: self.clone(),
            status: 0,
            done: false,
        }
    }
}

impl Exchange {
    pub fn is_empty(&self) -> bool {
        self.pipeline.is_empty()
    }

    /// The `request` stage. `facts` are the connect-time facts; the head is
    /// what the hops read and may rewrite.
    pub async fn request(
        &mut self,
        facts: &HopFacts<'_>,
        head: &mut RequestHead,
    ) -> ExchangeOutcome {
        for (index, hop) in self.pipeline.hops.iter().enumerate() {
            let outcome = match self.states[index].as_mut() {
                Some(state) => state.on_request(&self.context, facts, head).await,
                None => {
                    let view = HopFacts {
                        method: Some(&head.method),
                        path: Some(head.path_only()),
                        headers: &head.headers,
                        ..facts.clone()
                    };
                    match hop.on_request(&self.context, &view).await {
                        HopOutcome::Allow => ExchangeOutcome::Continue,
                        HopOutcome::Deny { source, reason } => {
                            ExchangeOutcome::Deny { source, reason }
                        }
                        HopOutcome::Proxy(target) => ExchangeOutcome::Proxy(target),
                    }
                }
            };
            if outcome != ExchangeOutcome::Continue {
                return outcome;
            }
        }
        ExchangeOutcome::Continue
    }

    /// Whether any hop reads the request body.
    pub fn reads_body(&self) -> bool {
        self.pipeline.reads_body() || self.states.iter().flatten().any(|s| s.body_cap().is_some())
    }

    /// The most any body-reading hop wants buffered.
    pub fn body_cap(&self) -> u64 {
        let stateful = self
            .states
            .iter()
            .flatten()
            .filter_map(|s| s.body_cap())
            .max();
        let stateless = self
            .pipeline
            .hops
            .iter()
            .filter_map(|hop| hop.body_cap())
            .max();
        stateful
            .into_iter()
            .chain(stateless)
            .max()
            .unwrap_or(DEFAULT_DLP_MAX_BYTES)
    }

    /// The `body` stage, on the buffered body; a hop may replace it.
    pub async fn body(&mut self, body: &mut bytes::Bytes) -> ExchangeOutcome {
        for (index, hop) in self.pipeline.hops.iter().enumerate() {
            let outcome = match self.states[index].as_mut() {
                Some(state) if state.body_cap().is_some() => {
                    state.on_body(&self.context, body).await
                }
                Some(_) => ExchangeOutcome::Continue,
                None => match hop.on_body(body) {
                    Some(reason) => ExchangeOutcome::Deny {
                        source: hop.source().to_string(),
                        reason: Some(reason),
                    },
                    None => ExchangeOutcome::Continue,
                },
            };
            if outcome != ExchangeOutcome::Continue {
                return outcome;
            }
        }
        ExchangeOutcome::Continue
    }

    /// When the body could not be buffered: the first body-reading hop that
    /// fails closed.
    pub fn body_unreadable_denied_by(&self) -> Option<String> {
        self.pipeline
            .hops
            .iter()
            .enumerate()
            .find(|(index, hop)| {
                let reads = match self.states[*index].as_ref() {
                    Some(state) => state.body_cap().is_some(),
                    None => hop.body_cap().is_some(),
                };
                reads && !hop.fails_open()
            })
            .map(|(_, hop)| hop.source().to_string())
    }

    /// Remember the status the client got, for the `done` stage.
    pub fn answered(&mut self, status: u16) {
        self.status = status;
    }

    /// The `response` and `done` stages around the upstream's response: the
    /// hops that read the head run now (a `Respond` replaces the response);
    /// a hop that reads the body sees it chunk by chunk as it streams, and
    /// `done` runs when the last byte has gone (or the client went away).
    pub async fn respond(mut self, response: hyper::Response<HopBody>) -> hyper::Response<HopBody> {
        if !self.states.iter().flatten().any(|s| s.reads_response()) {
            self.status = response.status().as_u16();
            if self.states.iter().all(Option::is_none) {
                return response;
            }
            // Stateful hops still end their exchange (a plugin's `on_done`),
            // after the response is on its way, not before.
            let (parts, body) = response.into_parts();
            return hyper::Response::from_parts(
                parts,
                http_body_util::BodyExt::boxed(DoneOnEnd {
                    inner: body,
                    exchange: Some(std::sync::Mutex::new(self)),
                }),
            );
        }
        let (mut parts, body) = response.into_parts();
        let mut head = ResponseHead {
            status: parts.status.as_u16(),
            headers: parts
                .headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_ascii_lowercase(),
                        String::from_utf8_lossy(value.as_bytes()).into_owned(),
                    )
                })
                .collect(),
            changed: false,
        };
        for state in self.states.iter_mut().flatten() {
            match state.on_response(&self.context, &mut head).await {
                ExchangeOutcome::Continue => {}
                ExchangeOutcome::Respond(local) => {
                    self.status = local.status;
                    self.finish();
                    return local.into_response();
                }
                ExchangeOutcome::Deny { source, reason } => {
                    let local = LocalResponse {
                        source,
                        status: 403,
                        headers: Vec::new(),
                        body: Vec::new(),
                        reason,
                    };
                    self.status = local.status;
                    self.finish();
                    return local.into_response();
                }
                ExchangeOutcome::Proxy(_) => {}
            }
        }
        if head.changed {
            if let Ok(status) = hyper::StatusCode::from_u16(head.status) {
                parts.status = status;
            }
            parts.headers.clear();
            for (name, value) in &head.headers {
                if let (Ok(name), Ok(value)) = (
                    hyper::header::HeaderName::from_bytes(name.as_bytes()),
                    hyper::header::HeaderValue::from_str(value),
                ) {
                    parts.headers.append(name, value);
                }
            }
        }
        self.status = parts.status.as_u16();
        let reads_body = self
            .states
            .iter()
            .flatten()
            .any(|s| s.reads_response_body());
        if reads_body {
            // A hop may change the body's length.
            parts.headers.remove(hyper::header::CONTENT_LENGTH);
        }
        let body = if reads_body {
            streamed(self, body)
        } else {
            // Only `done` waits on the body: run it when the body ends,
            // with no task or channel in the way.
            http_body_util::BodyExt::boxed(DoneOnEnd {
                inner: body,
                exchange: Some(std::sync::Mutex::new(self)),
            })
        };
        hyper::Response::from_parts(parts, body)
    }

    /// Run `done` once.
    fn finish(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        let status = self.status;
        for state in self.states.iter_mut().flatten() {
            state.on_done(status);
        }
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        self.finish();
    }
}

/// A response body that runs `done` when it ends (or is dropped unread).
struct DoneOnEnd {
    inner: HopBody,
    /// In a mutex only so the body is `Sync`; nothing contends for it.
    exchange: Option<std::sync::Mutex<Exchange>>,
}

impl hyper::body::Body for DoneOnEnd {
    type Data = bytes::Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let polled = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        // Dropping the exchange runs `done`: at the end of the body, or when
        // hyper drops a body it has finished writing.
        if matches!(polled, std::task::Poll::Ready(None)) {
            self.exchange.take();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

/// The response body, passed through the hops that read it, chunk by
/// chunk, inside `poll_frame`: no task or channel between the upstream and
/// the client. `done` runs when hyper drops the body, after the last byte.
fn streamed(exchange: Exchange, body: HopBody) -> HopBody {
    http_body_util::BodyExt::boxed(StreamedBody {
        state: std::sync::Mutex::new(Streamed {
            inner: body,
            exchange: Some(exchange),
            pending: None,
            ended: false,
            trailers: None,
        }),
    })
}

type ChunkFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = (Exchange, Result<bytes::Bytes, String>)> + Send>,
>;

/// In a mutex only so the body is `Sync`; only hyper polls it.
struct StreamedBody {
    state: std::sync::Mutex<Streamed>,
}

struct Streamed {
    inner: HopBody,
    exchange: Option<Exchange>,
    /// The hops working on one chunk (usually done on the first poll).
    pending: Option<ChunkFuture>,
    ended: bool,
    trailers: Option<hyper::body::Frame<bytes::Bytes>>,
}

/// One chunk through every body-reading hop in order.
fn pump(mut exchange: Exchange, mut chunk: bytes::Bytes, end: bool) -> ChunkFuture {
    Box::pin(async move {
        let context = exchange.context.clone();
        for state in exchange.states.iter_mut().flatten() {
            if !state.reads_response_body() {
                continue;
            }
            match state.on_response_chunk(&context, chunk, end).await {
                Ok(out) => chunk = out,
                Err(reason) => return (exchange, Err(reason)),
            }
        }
        (exchange, Ok(chunk))
    })
}

impl hyper::body::Body for StreamedBody {
    type Data = bytes::Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let state = &mut *state;
        loop {
            if let Some(pending) = state.pending.as_mut() {
                let (exchange, result) = match pending.as_mut().poll(cx) {
                    Poll::Ready(done) => done,
                    Poll::Pending => return Poll::Pending,
                };
                state.pending = None;
                state.exchange = Some(exchange);
                match result {
                    Ok(chunk) if !chunk.is_empty() => {
                        return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))))
                    }
                    Ok(_) => continue,
                    // A hop failed mid-stream: abort the response rather
                    // than end it short.
                    Err(reason) => return Poll::Ready(Some(Err(reason.into()))),
                }
            }
            if state.ended {
                return Poll::Ready(state.trailers.take().map(Ok));
            }
            let Some(exchange) = state.exchange.take() else {
                return Poll::Ready(None);
            };
            match std::pin::Pin::new(&mut state.inner).poll_frame(cx) {
                Poll::Pending => {
                    state.exchange = Some(exchange);
                    return Poll::Pending;
                }
                Poll::Ready(Some(Err(err))) => {
                    state.exchange = Some(exchange);
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(chunk) => state.pending = Some(pump(exchange, chunk, false)),
                    Err(trailers) => {
                        // Trailers end the data: the hops see the end, then
                        // the trailers go out as they are.
                        state.trailers = Some(trailers);
                        state.ended = true;
                        state.pending = Some(pump(exchange, bytes::Bytes::new(), true));
                    }
                },
                Poll::Ready(None) => {
                    state.ended = true;
                    state.pending = Some(pump(exchange, bytes::Bytes::new(), true));
                }
            }
        }
    }
}

/// One listener's compiled pipeline, rebuilt when the policy body it runs
/// under changes (a new hash) and reused otherwise.
#[derive(Default)]
pub struct PipelineCache {
    slot: Mutex<Option<(String, Arc<Pipeline>)>>,
}

impl PipelineCache {
    /// The pipeline for `direction` under `policy`, plus the upstream's own
    /// hops.
    pub fn get(
        &self,
        policy: Option<&EffectivePolicy>,
        direction: &str,
        upstream_key: Option<&str>,
        upstream: &[HopConfig],
    ) -> Arc<Pipeline> {
        let chain = policy.and_then(|p| {
            if direction == "ingress" {
                p.ingress.as_ref()
            } else {
                p.egress.as_ref()
            }
        });
        // No policy compiles to the upstream's own hops under the key "";
        // a policy without a hash (tests) is compiled every time.
        let key = match policy {
            None => Some(String::new()),
            Some(p) if p.hash.is_empty() => None,
            Some(p) => Some(p.hash.clone()),
        };
        let mut slot = self.slot.lock().unwrap_or_else(|p| p.into_inner());
        if let (Some(key), Some((hash, pipeline))) = (key.as_ref(), slot.as_ref()) {
            if hash == key {
                return pipeline.clone();
            }
        }
        let pipeline = Arc::new(Pipeline::compile(chain, upstream_key, upstream));
        if let Some(key) = key {
            *slot = Some((key, pipeline.clone()));
        }
        pipeline
    }
}

/// The PROXY v2 header a hand-off (or a mesh hop) starts with.
pub fn proxy_header(
    context: &ForwarderContext,
    facts: &HopFacts<'_>,
    client: Option<SocketAddr>,
    destination: Option<SocketAddr>,
) -> Vec<u8> {
    let who = context.identity_for(facts.allocation_id);
    let identity = context
        .identity_string_for(facts.allocation_id)
        .unwrap_or_default();
    let allocation = facts.allocation_id.to_string();
    let labels = who
        .as_ref()
        .map(|w| {
            w.labels
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(";")
        })
        .unwrap_or_default();
    let job = who.as_ref().map(|w| w.job_id.clone()).unwrap_or_default();
    let task = who
        .as_ref()
        .map(|w| w.task_name.clone())
        .unwrap_or_default();
    proxyproto::encode(
        client,
        destination,
        &[
            (tlv::IDENTITY, identity.as_bytes()),
            (tlv::DESTINATION, facts.destination.as_bytes()),
            (tlv::ALLOCATION, allocation.as_bytes()),
            (tlv::LABELS, labels.as_bytes()),
            (tlv::DIRECTION, facts.direction.as_bytes()),
            (tlv::JOB, job.as_bytes()),
            (tlv::TASK, task.as_bytes()),
        ],
    )
}

/// The `Legion-*` headers an HTTP hand-off carries.
pub fn legion_headers(context: &ForwarderContext, facts: &HopFacts<'_>) -> Vec<(String, String)> {
    let who = context.identity_for(facts.allocation_id);
    let mut out = vec![
        (
            "legion-identity".to_string(),
            context
                .identity_string_for(facts.allocation_id)
                .unwrap_or_default(),
        ),
        (
            "legion-allocation".to_string(),
            facts.allocation_id.to_string(),
        ),
        ("legion-direction".to_string(), facts.direction.to_string()),
        (
            "legion-destination".to_string(),
            facts.destination.to_string(),
        ),
    ];
    if let Some(who) = who {
        out.push(("legion-job".to_string(), who.job_id));
        out.push(("legion-task".to_string(), who.task_name));
        if !who.labels.is_empty() {
            out.push((
                "legion-labels".to_string(),
                who.labels
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(";"),
            ));
        }
    }
    if let Some(identity) = facts.identity {
        out.push(("legion-peer-identity".to_string(), identity.to_string()));
    }
    out
}

/// Resolve `host:port` to one socket address (for the PROXY header's fixed
/// part); `None` when the host is a name that does not resolve.
pub async fn resolve_addr(destination: &str) -> Option<SocketAddr> {
    if let Ok(addr) = destination.parse::<SocketAddr>() {
        return Some(addr);
    }
    tokio::net::lookup_host(destination)
        .await
        .ok()
        .and_then(|mut addrs| addrs.next())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::netd::{Denials, NetPolicyCache};
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    static HITS: AtomicUsize = AtomicUsize::new(0);

    /// A webhook that denies paths under /secret, allows the rest, and
    /// counts calls (so the cache is observable).
    async fn webhook() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let service = service_fn(
                        |req: hyper::Request<hyper::body::Incoming>| async move {
                            use http_body_util::BodyExt;
                            HITS.fetch_add(1, Ordering::Relaxed);
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let facts: serde_json::Value = serde_json::from_slice(&body).unwrap();
                            let deny = facts
                                .get("path")
                                .and_then(|p| p.as_str())
                                .is_some_and(|p| p.starts_with("/secret"))
                                || facts.get("destination").and_then(|d| d.as_str())
                                    == Some("evil.example:443");
                            let reply = if deny {
                                json!({ "verdict": "deny", "reason": "webhook said no", "ttl": "1s" })
                            } else {
                                json!({ "verdict": "allow" })
                            };
                            Ok::<_, hyper::Error>(hyper::Response::new(http_body_util::Full::new(
                                bytes::Bytes::from(reply.to_string()),
                            )))
                        },
                    );
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        format!("http://{addr}/authz")
    }

    fn context() -> Arc<ForwarderContext> {
        Arc::new(ForwarderContext::new(
            Arc::new(NetPolicyCache::new()),
            Arc::new(Denials::new()),
        ))
    }

    fn hop(value: serde_json::Value) -> HopConfig {
        serde_json::from_value(value).unwrap()
    }

    /// A pipeline of chain hops, each sourced `policy/p#<i>`.
    fn chain_pipeline(hops: Vec<HopConfig>) -> Pipeline {
        let chain = EffectiveChain {
            protocol: "tcp".into(),
            policy: "allow".into(),
            access_log: false,
            callers: None,
            rules: Vec::new(),
            handlers: hops
                .into_iter()
                .enumerate()
                .map(|(i, handler)| crate::structs::netpolicy::EffectiveHandler {
                    handler,
                    source: format!("policy/p#{i}"),
                })
                .collect(),
        };
        Pipeline::compile(Some(&chain), None, &[])
    }

    fn authz_hop(url: &str, on_error: Option<&str>) -> HopConfig {
        let mut value = json!({ "authz": { "url": url, "cache": "30s" }, "timeout": "500ms" });
        if let Some(on_error) = on_error {
            value["onError"] = json!(on_error);
        }
        hop(value)
    }

    #[tokio::test]
    async fn authz_webhook_allows_denies_and_caches() {
        let url = webhook().await;
        let context = context();
        let pipeline = chain_pipeline(vec![authz_hop(&url, None)]);
        let facts = HopFacts {
            allocation_id: 1,
            direction: "egress",
            protocol: "tcp",
            destination: "good.example:443",
            host: Some("good.example"),
            port: 443,
            ..Default::default()
        };
        let before = HITS.load(Ordering::Relaxed);
        assert_eq!(
            pipeline.run_connect(&context, &facts).await,
            HopOutcome::Allow
        );
        assert_eq!(
            pipeline.run_connect(&context, &facts).await,
            HopOutcome::Allow
        );
        assert_eq!(
            HITS.load(Ordering::Relaxed) - before,
            1,
            "the second identical flow is answered from the cache"
        );
        let denied = HopFacts {
            destination: "evil.example:443",
            host: Some("evil.example"),
            ..facts.clone()
        };
        assert_eq!(
            pipeline.run_connect(&context, &denied).await,
            HopOutcome::Deny {
                source: "policy/p#0".into(),
                reason: Some("webhook said no".into())
            }
        );
        let statuses = context.handler_statuses(1);
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].healthy);
        assert_eq!(statuses[0].calls, 2);
    }

    #[tokio::test]
    async fn a_dead_webhook_fails_closed_unless_on_error_allow() {
        let context = context();
        let facts = HopFacts {
            allocation_id: 2,
            direction: "egress",
            protocol: "tcp",
            destination: "x:1",
            ..Default::default()
        };
        // Nothing listens on this port.
        let port = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dead = format!("http://127.0.0.1:{port}/authz");
        match chain_pipeline(vec![authz_hop(&dead, None)])
            .run_connect(&context, &facts)
            .await
        {
            HopOutcome::Deny { source, reason } => {
                assert_eq!(source, "policy/p#0");
                assert!(reason.unwrap().starts_with("authz http://"));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            chain_pipeline(vec![authz_hop(&dead, Some("allow"))])
                .run_connect(&context, &facts)
                .await,
            HopOutcome::Allow
        );
        let statuses = context.handler_statuses(2);
        assert!(!statuses[0].healthy);
        assert!(statuses[0].last_error.is_some());
    }

    #[tokio::test]
    async fn proxy_hop_resolves_service_local_and_port_targets() {
        let context = Arc::new(
            ForwarderContext::new(Arc::new(NetPolicyCache::new()), Arc::new(Denials::new()))
                .with_node_id("node-a".into()),
        );
        context.set_handler_endpoints(
            3,
            "filter",
            vec![
                crate::worker::balance::Endpoint {
                    addr: "10.0.0.1:9000".into(),
                    allocation_id: 30,
                    node_id: "node-b".into(),
                    tags: Vec::new(),
                    mesh_tls: false,
                },
                crate::worker::balance::Endpoint {
                    addr: "10.0.0.2:9000".into(),
                    allocation_id: 31,
                    node_id: "node-a".into(),
                    tags: Vec::new(),
                    mesh_tls: false,
                },
            ],
        );
        let facts = HopFacts {
            allocation_id: 3,
            direction: "egress",
            protocol: "tcp",
            destination: "api.example:443",
            ..Default::default()
        };
        let upstream = |hops: Vec<HopConfig>| Pipeline::compile(None, Some("api"), &hops);
        match upstream(vec![hop(json!({ "proxy": { "service": "filter" } }))])
            .run_connect(&context, &facts)
            .await
        {
            HopOutcome::Proxy(target) => {
                assert!(target.addr.starts_with("10.0.0."));
                assert!(target.proxy_protocol);
                assert_eq!(target.source, "upstream/api#0");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            upstream(vec![hop(
                json!({ "proxy": { "service": "filter", "local": true, "proxyProtocol": false } })
            )])
            .run_connect(&context, &facts)
            .await,
            HopOutcome::Proxy(ProxyTarget {
                source: "upstream/api#0".into(),
                addr: "10.0.0.2:9000".into(),
                proxy_protocol: false
            })
        );
        assert!(matches!(
            upstream(vec![hop(json!({ "proxy": { "service": "nowhere" } }))])
                .run_connect(&context, &facts)
                .await,
            HopOutcome::Deny { .. }
        ));
        assert_eq!(
            upstream(vec![hop(
                json!({ "proxy": { "service": "nowhere" }, "onError": "allow" })
            )])
            .run_connect(&context, &facts)
            .await,
            HopOutcome::Allow
        );
    }

    #[test]
    fn body_hops_run_in_order_and_cap_the_buffer_test() {
        let pipeline = chain_pipeline(vec![
            hop(json!({ "dlp": { "patterns": ["sk-[a-z]{4,}"], "maxBytes": 16 } })),
            hop(json!({ "dlp": { "patterns": ["AKIA"], "maxBytes": 64 }, "onError": "allow" })),
        ]);
        assert!(pipeline.reads_body());
        assert_eq!(pipeline.body_cap(), 64);
        assert_eq!(pipeline.check_body(b"hello"), None);
        assert_eq!(
            pipeline.check_body(b"x sk-abcd").map(|(s, _)| s),
            Some("policy/p#0".to_string())
        );
        // Over the first hop's cap, which fails closed.
        assert_eq!(
            pipeline.check_body(&[b'a'; 32]).map(|(s, _)| s),
            Some("policy/p#0".to_string())
        );
        assert_eq!(
            pipeline.body_unreadable_denied_by().as_deref(),
            Some("policy/p#0")
        );
    }

    #[test]
    fn the_cache_recompiles_only_when_the_policy_hash_moves_test() {
        let cache = PipelineCache::default();
        let mut policy = EffectivePolicy {
            hash: "a".into(),
            ..Default::default()
        };
        let first = cache.get(Some(&policy), "egress", None, &[]);
        let again = cache.get(Some(&policy), "egress", None, &[]);
        assert!(Arc::ptr_eq(&first, &again));
        policy.hash = "b".into();
        let moved = cache.get(Some(&policy), "egress", None, &[]);
        assert!(!Arc::ptr_eq(&first, &moved));
    }

    #[test]
    fn proxy_header_carries_the_legion_tlvs() {
        let context = context();
        let facts = HopFacts {
            allocation_id: 9,
            direction: "egress",
            protocol: "tcp",
            destination: "api.example:443",
            ..Default::default()
        };
        let bytes = proxy_header(&context, &facts, "10.0.0.5:4444".parse().ok(), None);
        match proxyproto::parse(&bytes) {
            proxyproto::Parsed::Header(header, _) => {
                assert_eq!(
                    header.text(tlv::DESTINATION).as_deref(),
                    Some("api.example:443")
                );
                assert_eq!(header.text(tlv::ALLOCATION).as_deref(), Some("9"));
                assert_eq!(header.text(tlv::DIRECTION).as_deref(), Some("egress"));
            }
            other => panic!("{other:?}"),
        }
    }
}
