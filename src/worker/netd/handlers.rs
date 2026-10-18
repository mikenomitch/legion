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
//!   SOCKS5, an L4 ingress splice: [`Pipeline::run_connect`] with the
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
