//! The service graph's raw material (docs/network-handlers.md, "Service
//! graph"): what each allocation's datapath saw per (upstream service,
//! endpoint tag) since the last heartbeat — connections or requests,
//! errors, denials, latency, bytes.
//!
//! Recorded on the *calling* side, because that is where Legion owns the
//! bytes for every runtime and platform: an upstream listener, the gated
//! egress proxy, an HTTP-mode request. The ledger holds deltas, drained
//! into `HeartbeatRequest.net_edges` and restored if the heartbeat fails,
//! and the manager folds them into per-(caller, service) windows. Bounded:
//! past [`MAX_EDGES`] distinct keys, new keys are dropped until the next
//! drain — a sandbox that scans the internet cannot grow the node's memory.

use crate::structs::NetEdgeSample;
use std::collections::HashMap;
use std::sync::Mutex;

/// Distinct (allocation, service, tag, protocol) keys held between beats.
pub const MAX_EDGES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EdgeKey {
    allocation_id: i64,
    service: String,
    tag: String,
    protocol: &'static str,
}

#[derive(Debug, Default, Clone)]
struct Counters {
    requests: u64,
    errors: u64,
    denied: u64,
    latency_ms_sum: u64,
    latency_ms_max: u64,
    bytes_out: u64,
    bytes_in: u64,
}

/// One node's edge deltas since the last heartbeat.
#[derive(Default)]
pub struct EdgeLedger {
    pending: Mutex<HashMap<EdgeKey, Counters>>,
}

impl EdgeLedger {
    pub fn new() -> Self {
        Self::default()
    }

    fn with<F: FnOnce(&mut Counters)>(
        &self,
        allocation_id: i64,
        service: &str,
        tag: Option<&str>,
        protocol: &'static str,
        f: F,
    ) {
        let key = EdgeKey {
            allocation_id,
            service: service.to_string(),
            tag: tag.unwrap_or_default().to_string(),
            protocol,
        };
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        if !pending.contains_key(&key) && pending.len() >= MAX_EDGES {
            return;
        }
        f(pending.entry(key).or_default());
    }

    /// An L4 dial: reached an endpoint (`ok`) or failed to reach any.
    pub fn record_connection(
        &self,
        allocation_id: i64,
        service: &str,
        tag: Option<&str>,
        ok: bool,
    ) {
        self.with(allocation_id, service, tag, "tcp", |c| {
            if ok {
                c.requests += 1;
            } else {
                c.errors += 1;
            }
        });
    }

    /// Bytes an L4 flow moved (`out` toward the service, `in` back).
    pub fn record_bytes(
        &self,
        allocation_id: i64,
        service: &str,
        tag: Option<&str>,
        protocol: &'static str,
        out: u64,
        r#in: u64,
    ) {
        self.with(allocation_id, service, tag, protocol, |c| {
            c.bytes_out += out;
            c.bytes_in += r#in;
        });
    }

    /// One HTTP-mode request: `status` is the origin's answer (`None` when
    /// no endpoint answered), `latency_ms` end to end.
    #[allow(clippy::too_many_arguments)]
    pub fn record_request(
        &self,
        allocation_id: i64,
        service: &str,
        tag: Option<&str>,
        status: Option<u16>,
        latency_ms: u64,
        bytes_out: u64,
        bytes_in: u64,
    ) {
        self.with(allocation_id, service, tag, "http", |c| {
            match status {
                Some(status) if status < 500 => c.requests += 1,
                Some(_) => {
                    c.requests += 1;
                    c.errors += 1;
                }
                None => c.errors += 1,
            }
            c.latency_ms_sum += latency_ms;
            c.latency_ms_max = c.latency_ms_max.max(latency_ms);
            c.bytes_out += bytes_out;
            c.bytes_in += bytes_in;
        });
    }

    /// A connection or request denied by policy or a handler.
    pub fn record_denied(&self, allocation_id: i64, service: &str, protocol: &'static str) {
        self.with(allocation_id, service, None, protocol, |c| c.denied += 1);
    }

    /// Everything since the last drain, sorted for stable heartbeats.
    pub fn drain(&self) -> Vec<NetEdgeSample> {
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<NetEdgeSample> = pending
            .drain()
            .map(|(key, c)| NetEdgeSample {
                allocation_id: key.allocation_id,
                service: key.service,
                tag: key.tag,
                protocol: key.protocol.to_string(),
                requests: c.requests,
                errors: c.errors,
                denied: c.denied,
                latency_ms_sum: c.latency_ms_sum,
                latency_ms_max: c.latency_ms_max,
                bytes_out: c.bytes_out,
                bytes_in: c.bytes_in,
            })
            .collect();
        out.sort_by(|a, b| {
            a.allocation_id
                .cmp(&b.allocation_id)
                .then_with(|| a.service.cmp(&b.service))
                .then_with(|| a.tag.cmp(&b.tag))
        });
        out
    }

    /// Put drained samples back after a failed heartbeat, merging with
    /// anything recorded since.
    pub fn restore(&self, samples: Vec<NetEdgeSample>) {
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        for sample in samples {
            let protocol: &'static str = if sample.protocol == "http" {
                "http"
            } else {
                "tcp"
            };
            let key = EdgeKey {
                allocation_id: sample.allocation_id,
                service: sample.service,
                tag: sample.tag,
                protocol,
            };
            if !pending.contains_key(&key) && pending.len() >= MAX_EDGES {
                continue;
            }
            let c = pending.entry(key).or_default();
            c.requests += sample.requests;
            c.errors += sample.errors;
            c.denied += sample.denied;
            c.latency_ms_sum += sample.latency_ms_sum;
            c.latency_ms_max = c.latency_ms_max.max(sample.latency_ms_max);
            c.bytes_out += sample.bytes_out;
            c.bytes_in += sample.bytes_in;
        }
    }

    /// Distinct keys pending, for tests.
    #[cfg(test)]
    pub fn pending_len(&self) -> usize {
        self.pending.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds_and_drains_per_edge() {
        let ledger = EdgeLedger::new();
        ledger.record_connection(1, "db", None, true);
        ledger.record_connection(1, "db", None, true);
        ledger.record_connection(1, "db", None, false);
        ledger.record_bytes(1, "db", None, "tcp", 100, 2000);
        ledger.record_request(1, "api", Some("canary"), Some(200), 12, 10, 0);
        ledger.record_request(1, "api", Some("canary"), Some(503), 40, 10, 0);
        ledger.record_request(1, "api", Some("canary"), None, 2000, 10, 0);
        ledger.record_denied(2, "evil.example", "tcp");
        let samples = ledger.drain();
        assert_eq!(samples.len(), 3);
        let db = samples.iter().find(|s| s.service == "db").unwrap();
        assert_eq!(
            (db.requests, db.errors, db.bytes_out, db.bytes_in),
            (2, 1, 100, 2000)
        );
        let api = samples.iter().find(|s| s.service == "api").unwrap();
        assert_eq!(api.tag, "canary");
        assert_eq!((api.requests, api.errors), (2, 2));
        assert_eq!(api.latency_ms_max, 2000);
        assert_eq!(api.latency_ms_sum, 2052);
        let denied = samples.iter().find(|s| s.allocation_id == 2).unwrap();
        assert_eq!(denied.denied, 1);
        assert!(ledger.drain().is_empty(), "drain empties the ledger");

        ledger.restore(samples);
        ledger.record_connection(1, "db", None, true);
        let again = ledger.drain();
        let db = again.iter().find(|s| s.service == "db").unwrap();
        assert_eq!(db.requests, 3, "restored counters merge with new ones");
    }

    #[test]
    fn the_ledger_is_bounded() {
        let ledger = EdgeLedger::new();
        for i in 0..(MAX_EDGES + 100) {
            ledger.record_connection(1, &format!("host-{i}"), None, true);
        }
        assert_eq!(ledger.pending_len(), MAX_EDGES);
    }
}
