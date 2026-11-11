//! `authz` — a decision webhook. netd POSTs the flow's facts as JSON (who,
//! where to, and in HTTP mode the request head) and reads back
//! `{ "verdict": "allow" | "deny", "reason"?, "ttl"? }`. The verdict is cached
//! per (hop, identity, destination[, method, path]) for `cache` (default
//! [`DEFAULT_AUTHZ_CACHE`], at most `MAX_AUTHZ_CACHE`), so a webhook costs one
//! round trip per new pair, not one per connection. It never sees bytes.

use super::{Handler, HopFacts, HopOutcome};
use crate::prelude::*;
use crate::structs::hops::{handler_entry, AuthzConfig, HandlerEntry, HopConfig};
use crate::structs::netpolicy::{parse_duration, MAX_AUTHZ_CACHE};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::metrics as net_metrics;

use std::time::{Duration, Instant};

/// Verdict cache TTL when a hop names no `cache`.
pub const DEFAULT_AUTHZ_CACHE: &str = "10s";

pub(super) struct Authz {
    source: String,
    config: AuthzConfig,
    hop: HopConfig,
}

impl Authz {
    pub(super) fn new(source: String, config: AuthzConfig, hop: HopConfig) -> Self {
        Authz {
            source,
            config,
            hop,
        }
    }

    /// `Some((source, reason))` to deny.
    async fn decide(
        &self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> Option<(String, Option<String>)> {
        let url = self.config.url.clone();
        // Everything the webhook is told that can change its answer: a
        // verdict for one allocation (or one webhook) must never answer for
        // another on this node. The cache is node-wide.
        let key = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.source,
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
            net_metrics::handler("authz", if allowed { "allow" } else { "deny" }, 0.0);
            return (!allowed).then(|| (self.source.clone(), reason));
        }
        let timeout = self.hop.timeout();
        let cap = parse_duration(MAX_AUTHZ_CACHE).expect("constant");
        let configured_ttl = self
            .config
            .cache
            .as_deref()
            .and_then(|c| parse_duration(c).ok())
            .unwrap_or_else(|| parse_duration(DEFAULT_AUTHZ_CACHE).expect("constant"))
            .min(cap);
        let body = body(context, facts).to_string();
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
                context.note_handler(facts.allocation_id, &self.source, "authz", &url, Ok(()));
                net_metrics::handler("authz", if allowed { "allow" } else { "deny" }, elapsed);
                (!allowed).then(|| (self.source.clone(), reason))
            }
            Err(err) => {
                debug!(source = %self.source, url = %url, error = %err, "authz handler failed");
                context.note_handler(
                    facts.allocation_id,
                    &self.source,
                    "authz",
                    &url,
                    Err(err.clone()),
                );
                net_metrics::handler("authz", "error", elapsed);
                if self.hop.fails_open() {
                    None
                } else {
                    Some((self.source.clone(), Some(format!("authz {url}: {err}"))))
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler for Authz {
    fn entry(&self) -> &'static HandlerEntry {
        handler_entry("authz").expect("registered")
    }

    fn source(&self) -> &str {
        &self.source
    }

    async fn on_connect(
        &self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> HopOutcome {
        match self.decide(context, facts).await {
            Some((source, reason)) => HopOutcome::Deny { source, reason },
            None => HopOutcome::Allow,
        }
    }

    async fn on_request(
        &self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> HopOutcome {
        self.on_connect(context, facts).await
    }
}

/// The webhook's request body.
fn body(context: &ForwarderContext, facts: &HopFacts<'_>) -> serde_json::Value {
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
