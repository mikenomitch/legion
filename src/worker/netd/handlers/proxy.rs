//! `proxy` — hands the flow to something that does see bytes: any proxy
//! reachable as a catalog `service` (optionally its node-local endpoints
//! only), or a sidecar task's `port` label in the same job. On TCP the
//! hand-off starts with a PROXY protocol v2 header carrying Legion TLVs
//! ([`crate::worker::netd::proxyproto`]); on HTTP the request goes out in
//! absolute form with `Legion-*` headers. To deny, the proxy closes or answers
//! 4xx.

use super::{Handler, HopFacts, HopOutcome, ProxyTarget};
use crate::prelude::*;
use crate::structs::hops::{handler_entry, HandlerEntry, HopConfig, ProxyConfig};
use crate::worker::forwarder::ForwarderContext;
use crate::worker::netd::metrics as net_metrics;

pub(super) struct Proxy {
    source: String,
    config: ProxyConfig,
    hop: HopConfig,
}

impl Proxy {
    pub(super) fn new(source: String, config: ProxyConfig, hop: HopConfig) -> Self {
        Proxy {
            source,
            config,
            hop,
        }
    }

    /// Where this hop sends the flow.
    fn resolve(
        &self,
        context: &ForwarderContext,
        facts: &HopFacts<'_>,
    ) -> Result<ProxyTarget, String> {
        let proxy_protocol = self.config.proxy_protocol.unwrap_or(true);
        let (label, candidates): (String, Vec<String>) =
            if let Some(service) = self.config.service.as_deref() {
                let node = self.config.local.then(|| context.node_id()).flatten();
                let label = if self.config.local {
                    format!("node-local service {service}")
                } else {
                    format!("service {service}")
                };
                (
                    label,
                    context
                        .handler_endpoints_for(facts.allocation_id, service)
                        .into_iter()
                        .filter(|e| {
                            !self.config.local || node.as_deref().is_none_or(|n| n == e.node_id)
                        })
                        .map(|e| e.addr)
                        .collect(),
                )
            } else if let Some(port) = self.config.port.as_deref() {
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
            let reason = format!("proxy handler {label} has no endpoint");
            context.note_handler(
                facts.allocation_id,
                &self.source,
                "proxy",
                &target,
                Err(reason.clone()),
            );
            net_metrics::handler("proxy", "error", 0.0);
            return Err(reason);
        }
        let pick = context.next_handler_pick() % candidates.len();
        context.note_handler(facts.allocation_id, &self.source, "proxy", &target, Ok(()));
        net_metrics::handler("proxy", "proxy", 0.0);
        Ok(ProxyTarget {
            source: self.source.clone(),
            addr: candidates[pick].clone(),
            proxy_protocol,
        })
    }
}

#[async_trait::async_trait]
impl Handler for Proxy {
    fn entry(&self) -> &'static HandlerEntry {
        handler_entry("proxy").expect("registered")
    }

    fn source(&self) -> &str {
        &self.source
    }

    async fn on_connect(
        &self,
        context: &Arc<ForwarderContext>,
        facts: &HopFacts<'_>,
    ) -> HopOutcome {
        match self.resolve(context, facts) {
            Ok(target) => HopOutcome::Proxy(target),
            // A failed hop that fails open lets the walk continue.
            Err(_) if self.hop.fails_open() => HopOutcome::Allow,
            Err(reason) => HopOutcome::Deny {
                source: self.source.clone(),
                reason: Some(reason),
            },
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
