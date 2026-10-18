//! External handlers — code on the path, only when a chain names it
//! (docs/network-handlers.md, "Handlers").
//!
//! Two kinds run here; `log` and `dlp` are built in and live in HTTP mode.
//!
//! - **`authz`** — a decision webhook. `netd` POSTs the flow's facts as JSON
//!   (who, where to, and in HTTP mode the request head) and reads back
//!   `{ "verdict": "allow" | "deny", "reason"?, "ttl"? }`. The verdict is
//!   cached per (hop, identity, destination[, method, path]) for `cache`
//!   (default [`DEFAULT_AUTHZ_CACHE`], at most `MAX_AUTHZ_CACHE`), so a
//!   webhook costs one round trip per new pair, not one per connection. It
//!   never sees bytes.
//! - **`proxy`** — hands the flow to something that does see bytes: any
//!   proxy reachable as a catalog `service`, its `node`-local endpoints, or a
//!   sidecar task's `port` label in the same job. On TCP the hand-off starts
//!   with a PROXY protocol v2 header carrying Legion TLVs
//!   ([`super::proxyproto`]); on HTTP the request goes out in absolute form
//!   with `Legion-*` headers. To deny, the proxy closes or answers 4xx.
//!
//! Every hop has a `timeout` and an `onError`; the default `onError` is
//! `deny`, so a dead webhook or a proxy with no endpoints fails the chain
//! closed unless the operator wrote `onError = "allow"`. Health per hop is
//! reported on the heartbeat (`NetHandlerStatus`) and read by `net doctor`.

use crate::prelude::*;
use crate::structs::netpolicy::{
    parse_duration, HandlerConfig, DEFAULT_HANDLER_TIMEOUT, HANDLER_AUTHZ, HANDLER_PROXY,
    MAX_AUTHZ_CACHE, ON_ERROR_ALLOW,
};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::metrics as net_metrics;
use crate::worker::netd::proxyproto::{self, tlv};

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

/// Verdict cache TTL when a hop names no `cache`.
pub const DEFAULT_AUTHZ_CACHE: &str = "10s";
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

/// One hop with where it came from (`policy/<name>#i`, `task#i`,
/// `upstream/<key>#i`).
#[derive(Debug, Clone)]
pub struct Hop {
    pub source: String,
    pub config: HandlerConfig,
}

fn is_external(handler: &str) -> bool {
    handler == HANDLER_AUTHZ || handler == HANDLER_PROXY
}

/// The external hops that apply to a flow: the effective chain's for the
/// direction, then the upstream's own (`connect.upstreams[].handlers`).
pub fn hops_for(
    context: &ForwarderContext,
    allocation_id: i64,
    direction: &str,
    upstream_key: Option<&str>,
    upstream_handlers: &[HandlerConfig],
) -> Vec<Hop> {
    let mut hops = Vec::new();
    if let Some(policy) = context.policy_for(allocation_id) {
        let chain = if direction == "ingress" {
            policy.ingress.as_ref()
        } else {
            policy.egress.as_ref()
        };
        if let Some(chain) = chain {
            for hop in &chain.handlers {
                if is_external(&hop.handler.handler) {
                    hops.push(Hop {
                        source: hop.source.clone(),
                        config: hop.handler.clone(),
                    });
                }
            }
        }
    }
    for (index, handler) in upstream_handlers.iter().enumerate() {
        if is_external(&handler.handler) {
            hops.push(Hop {
                source: format!("upstream/{}#{index}", upstream_key.unwrap_or("?")),
                config: handler.clone(),
            });
        }
    }
    hops
}

fn hop_timeout(config: &HandlerConfig) -> Duration {
    config
        .timeout
        .as_deref()
        .and_then(|t| parse_duration(t).ok())
        .unwrap_or_else(|| parse_duration(DEFAULT_HANDLER_TIMEOUT).expect("constant"))
}

fn fails_open(config: &HandlerConfig) -> bool {
    config.on_error.as_deref() == Some(ON_ERROR_ALLOW)
}

/// Run the hops in order. The first `deny` ends the walk; the first `proxy`
/// takes the flow (hops after it never run — they could not see the bytes
/// anyway); a failed hop is a deny unless it says `onError = "allow"`.
pub async fn run(
    context: &Arc<ForwarderContext>,
    hops: &[Hop],
    facts: &HopFacts<'_>,
) -> HopOutcome {
    for hop in hops {
        match hop.config.handler.as_str() {
            HANDLER_AUTHZ => {
                if let Some((source, reason)) = authz(context, hop, facts).await {
                    return HopOutcome::Deny { source, reason };
                }
            }
            HANDLER_PROXY => match resolve_proxy(context, hop, facts) {
                Ok(target) => return HopOutcome::Proxy(target),
                Err(reason) => {
                    if fails_open(&hop.config) {
                        continue;
                    }
                    return HopOutcome::Deny {
                        source: hop.source.clone(),
                        reason: Some(reason),
                    };
                }
            },
            _ => {}
        }
    }
    HopOutcome::Allow
}

/// The webhook's request body.
fn authz_body(context: &ForwarderContext, facts: &HopFacts<'_>) -> serde_json::Value {
    let who = context.identity_for(facts.allocation_id);
    let mut body = json!({
        "allocation": facts.allocation_id,
        "job": who.as_ref().map(|w| w.job_id.clone()).unwrap_or_default(),
        "task": who.as_ref().map(|w| w.task_name.clone()).unwrap_or_default(),
        "identity": context.identity_string_for(facts.allocation_id),
        "labels": who.as_ref().map(|w| w.labels.clone()).unwrap_or_default(),
        "node": context.node_id().unwrap_or_default(),
        "direction": facts.direction,
        "protocol": facts.protocol,
        "destination": facts.destination,
        "host": facts.host,
        "ip": facts.ip.map(|ip| ip.to_string()),
        "port": facts.port,
        "peerIdentity": facts.identity,
    });
    if let Some(method) = facts.method {
        body["method"] = json!(method);
        body["path"] = json!(facts.path.unwrap_or("/"));
        body["headers"] = json!(facts
            .headers
            .iter()
            .filter(|(name, _)| name != "authorization"
                && name != "cookie"
                && name != "proxy-authorization")
            .cloned()
            .collect::<Vec<_>>());
    }
    body
}

/// `Some((source, reason))` to deny.
async fn authz(
    context: &Arc<ForwarderContext>,
    hop: &Hop,
    facts: &HopFacts<'_>,
) -> Option<(String, Option<String>)> {
    let url = hop.config.url.clone().unwrap_or_default();
    // Everything the webhook is told that can change its answer: a verdict
    // for one allocation (or one webhook) must never answer for another on
    // this node. The cache is node-wide.
    let key = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
        hop.source,
        url,
        facts.allocation_id,
        facts.direction,
        facts.protocol,
        facts.identity.unwrap_or(""),
        facts.destination,
        facts.method.unwrap_or(""),
        facts.path.unwrap_or("")
    );
    if let Some((allowed, reason)) = context.authz_cached(&key) {
        net_metrics::handler(HANDLER_AUTHZ, if allowed { "allow" } else { "deny" }, 0.0);
        return (!allowed).then(|| (hop.source.clone(), reason));
    }
    let timeout = hop_timeout(&hop.config);
    let cap = parse_duration(MAX_AUTHZ_CACHE).expect("constant");
    let configured_ttl = hop
        .config
        .cache
        .as_deref()
        .and_then(|c| parse_duration(c).ok())
        .unwrap_or_else(|| parse_duration(DEFAULT_AUTHZ_CACHE).expect("constant"))
        .min(cap);
    let body = authz_body(context, facts).to_string();
    let started = Instant::now();
    let outcome: Result<(bool, Option<String>, Option<Duration>), String> = async {
        let response = context
            .http_client()
            .post(&url)
            .timeout(timeout)
            .header("content-type", "application/json")
            .header("legion-handler", "authz")
            .body(body)
            .send()
            .await
            .map_err(|err| format!("{err}"))?;
        let status = response.status();
        let text = response.text().await.map_err(|err| format!("{err}"))?;
        if !status.is_success() {
            return Err(format!("webhook answered {}", status.as_u16()));
        }
        let value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|err| format!("webhook reply is not JSON: {err}"))?;
        let verdict = value
            .get("verdict")
            .and_then(|v| v.as_str())
            .map(|v| v.trim().to_ascii_lowercase())
            .ok_or_else(|| "webhook reply has no \"verdict\"".to_string())?;
        let allowed = match verdict.as_str() {
            "allow" => true,
            "deny" => false,
            other => return Err(format!("webhook verdict {other:?} is not allow|deny")),
        };
        let reason = value
            .get("reason")
            .and_then(|r| r.as_str())
            .map(str::to_string);
        let ttl = value
            .get("ttl")
            .and_then(|t| t.as_str())
            .and_then(|t| parse_duration(t).ok());
        Ok((allowed, reason, ttl))
    }
    .await;
    let elapsed = started.elapsed().as_secs_f64();
    match outcome {
        Ok((allowed, reason, ttl)) => {
            let ttl = ttl.unwrap_or(configured_ttl).min(cap);
            context.authz_store(key, allowed, reason.clone(), ttl);
            context.note_handler(
                facts.allocation_id,
                &hop.source,
                HANDLER_AUTHZ,
                &url,
                Ok(()),
            );
            net_metrics::handler(
                HANDLER_AUTHZ,
                if allowed { "allow" } else { "deny" },
                elapsed,
            );
            (!allowed).then(|| (hop.source.clone(), reason))
        }
        Err(err) => {
            debug!(source = %hop.source, url = %url, error = %err, "authz handler failed");
            context.note_handler(
                facts.allocation_id,
                &hop.source,
                HANDLER_AUTHZ,
                &url,
                Err(err.clone()),
            );
            net_metrics::handler(HANDLER_AUTHZ, "error", elapsed);
            if fails_open(&hop.config) {
                None
            } else {
                Some((hop.source.clone(), Some(format!("authz {url}: {err}"))))
            }
        }
    }
}

/// Where a `proxy` hop sends the flow.
fn resolve_proxy(
    context: &ForwarderContext,
    hop: &Hop,
    facts: &HopFacts<'_>,
) -> Result<ProxyTarget, String> {
    let proxy_protocol = hop.config.proxy_protocol.unwrap_or(true);
    let (label, candidates): (String, Vec<String>) =
        if let Some(service) = hop.config.service.as_deref() {
            (
                format!("service {service}"),
                context
                    .handler_endpoints_for(facts.allocation_id, service)
                    .into_iter()
                    .map(|e| e.addr)
                    .collect(),
            )
        } else if let Some(service) = hop.config.node.as_deref() {
            let node = context.node_id();
            (
                format!("node-local service {service}"),
                context
                    .handler_endpoints_for(facts.allocation_id, service)
                    .into_iter()
                    .filter(|e| node.as_deref().is_none_or(|n| n == e.node_id))
                    .map(|e| e.addr)
                    .collect(),
            )
        } else if let Some(port) = hop.config.port.as_deref() {
            (
                format!("port {port}"),
                context
                    .sidecar_port(facts.allocation_id, port)
                    .map(|p| vec![format!("127.0.0.1:{p}")])
                    .unwrap_or_default(),
            )
        } else {
            (String::from("?"), Vec::new())
        };
    let target = format!("{label} ({} endpoint(s))", candidates.len());
    if candidates.is_empty() {
        let reason = format!("proxy handler {} has no endpoint", label);
        context.note_handler(
            facts.allocation_id,
            &hop.source,
            HANDLER_PROXY,
            &target,
            Err(reason.clone()),
        );
        net_metrics::handler(HANDLER_PROXY, "error", 0.0);
        return Err(reason);
    }
    let pick = context.next_handler_pick() % candidates.len();
    context.note_handler(
        facts.allocation_id,
        &hop.source,
        HANDLER_PROXY,
        &target,
        Ok(()),
    );
    net_metrics::handler(HANDLER_PROXY, "proxy", 0.0);
    Ok(ProxyTarget {
        source: hop.source.clone(),
        addr: candidates[pick].clone(),
        proxy_protocol,
    })
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

    fn authz_hop(url: &str, on_error: Option<&str>) -> Hop {
        Hop {
            source: "policy/p#0".into(),
            config: HandlerConfig {
                handler: HANDLER_AUTHZ.into(),
                url: Some(url.into()),
                cache: Some("30s".into()),
                timeout: Some("500ms".into()),
                on_error: on_error.map(str::to_string),
                ..Default::default()
            },
        }
    }

    #[tokio::test]
    async fn authz_webhook_allows_denies_and_caches() {
        let url = webhook().await;
        let context = context();
        let hops = vec![authz_hop(&url, None)];
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
        assert_eq!(run(&context, &hops, &facts).await, HopOutcome::Allow);
        assert_eq!(run(&context, &hops, &facts).await, HopOutcome::Allow);
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
            run(&context, &hops, &denied).await,
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
        match run(&context, &[authz_hop(&dead, None)], &facts).await {
            HopOutcome::Deny { source, reason } => {
                assert_eq!(source, "policy/p#0");
                assert!(reason.unwrap().starts_with("authz http://"));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            run(&context, &[authz_hop(&dead, Some("allow"))], &facts).await,
            HopOutcome::Allow
        );
        let statuses = context.handler_statuses(2);
        assert!(!statuses[0].healthy);
        assert!(statuses[0].last_error.is_some());
    }

    #[tokio::test]
    async fn proxy_hop_resolves_service_node_and_port_targets() {
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
        let hop = |config: HandlerConfig| Hop {
            source: "task#0".into(),
            config,
        };
        let service = hop(HandlerConfig {
            handler: HANDLER_PROXY.into(),
            service: Some("filter".into()),
            ..Default::default()
        });
        match run(&context, &[service], &facts).await {
            HopOutcome::Proxy(target) => {
                assert!(target.addr.starts_with("10.0.0."));
                assert!(target.proxy_protocol);
            }
            other => panic!("{other:?}"),
        }
        let node = hop(HandlerConfig {
            handler: HANDLER_PROXY.into(),
            node: Some("filter".into()),
            proxy_protocol: Some(false),
            ..Default::default()
        });
        assert_eq!(
            run(&context, &[node], &facts).await,
            HopOutcome::Proxy(ProxyTarget {
                source: "task#0".into(),
                addr: "10.0.0.2:9000".into(),
                proxy_protocol: false
            })
        );
        let missing = hop(HandlerConfig {
            handler: HANDLER_PROXY.into(),
            service: Some("nowhere".into()),
            ..Default::default()
        });
        assert!(matches!(
            run(&context, std::slice::from_ref(&missing), &facts).await,
            HopOutcome::Deny { .. }
        ));
        let open = hop(HandlerConfig {
            on_error: Some("allow".into()),
            ..missing.config
        });
        assert_eq!(run(&context, &[open], &facts).await, HopOutcome::Allow);
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
