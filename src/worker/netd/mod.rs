//! `netd` — the node datapath's policy and telemetry half.
//!
//! The bytes still move through [`crate::worker::forwarder`] (and, from the
//! ingress phase on, its ingress twin); this module is what those listeners
//! consult before they dial, and what they report to afterwards:
//!
//! - [`policy::NetPolicyCache`] — the merged policy per allocation, keyed by
//!   the content hash the heartbeat reply named, fetched once per hash and
//!   kept last-known-good across heartbeat failures (fail-static).
//! - [`policy::Denials`] — denials recorded by listeners, folded per
//!   `(allocation, direction, destination, rule)`, drained onto the next
//!   heartbeat so the manager can emit `net_denied` events.
//! - [`metrics`] — the bounded-label worker metric families the datapath
//!   increments. Attribution (labels, identity, destination) never becomes a
//!   metric label; it goes on events and the access log.
//!
//! The interface between the worker and this module is deliberately the one
//! `legion netd` will have when it becomes its own process (service-mesh-plan
//! §3.1 / network-handlers-plan §7.8): policy in, telemetry out, capture
//! setup and teardown. Nothing here reaches into the heartbeat loop.

pub mod activity;
pub mod dns;
pub mod graph;
pub mod guard;
pub mod guest_agent;
pub mod handlers;
pub mod http;
pub mod ingress;
pub mod kernel_guard;
pub mod netns;
pub mod policy;
pub mod proxy;
pub mod proxyproto;
pub mod state;
pub mod tls;

pub use policy::{Denials, NetPolicyCache, PolicyLookup};

/// Bounded-label metric families for the datapath, on the worker registry.
pub mod metrics {
    /// Increment the connection counter for one verdict.
    pub fn connection(direction: &str, protocol: &str, verdict: &str) {
        crate::manager::telemetry::worker_metrics()
            .net_conns_total
            .inc(&[direction, protocol, verdict]);
        if verdict == "deny" {
            crate::manager::telemetry::worker_metrics()
                .net_denied_total
                .inc(&[direction]);
        }
    }

    /// Add forwarded bytes for one direction.
    pub fn bytes(direction: &str, count: u64) {
        crate::manager::telemetry::worker_metrics()
            .net_bytes_total
            .inc_by(&[direction], count);
    }

    /// One external handler call: `outcome` is allow|deny|proxy|error.
    pub fn handler(handler: &str, outcome: &str, seconds: f64) {
        let metrics = crate::manager::telemetry::worker_metrics();
        metrics.net_handler_total.inc(&[handler, outcome]);
        if seconds > 0.0 {
            metrics.net_handler_seconds.observe(seconds);
        }
    }
}
