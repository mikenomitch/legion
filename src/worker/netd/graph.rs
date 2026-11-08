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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Distinct (allocation, service, tag, protocol) keys held between beats.
pub const MAX_EDGES: usize = 4096;

/// Lock shards, by allocation (mesh-control-path plan §6.8, review NET8):
/// connections of different allocations rarely contend on one lock.
const SHARDS: usize = 16;

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

/// One allocation's edges: service → tag → per protocol. Nested so a hit
/// looks up by `&str` and allocates nothing; only a new key allocates.
type AllocationEdges = HashMap<String, HashMap<String, Vec<(&'static str, Counters)>>>;

/// One node's edge deltas since the last heartbeat.
pub struct EdgeLedger {
    shards: Vec<Mutex<HashMap<i64, AllocationEdges>>>,
    /// Distinct keys held, across shards, for the bound.
    keys: AtomicUsize,
}

impl Default for EdgeLedger {
    fn default() -> Self {
        EdgeLedger {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            keys: AtomicUsize::new(0),
        }
    }
}

fn protocol_name(protocol: &str) -> &'static str {
    match protocol {
        "http" => "http",
        "udp" => "udp",
        _ => "tcp",
    }
}

impl EdgeLedger {
    pub fn new() -> Self {
        Self::default()
    }

    fn shard(
        &self,
        allocation_id: i64,
    ) -> std::sync::MutexGuard<'_, HashMap<i64, AllocationEdges>> {
        let index = (allocation_id.unsigned_abs() as usize) % SHARDS;
        self.shards[index].lock().unwrap_or_else(|p| p.into_inner())
    }

    fn with<F: FnOnce(&mut Counters)>(
        &self,
        allocation_id: i64,
        service: &str,
        tag: Option<&str>,
        protocol: &'static str,
        f: F,
    ) {
        let tag = tag.unwrap_or_default();
        let mut shard = self.shard(allocation_id);
        // The common case: the key exists. No allocation, no bound check.
        if let Some(counters) = shard
            .get_mut(&allocation_id)
            .and_then(|services| services.get_mut(service))
            .and_then(|tags| tags.get_mut(tag))
            .and_then(|protocols| {
                protocols
                    .iter_mut()
                    .find(|(name, _)| *name == protocol)
                    .map(|(_, counters)| counters)
            })
        {
            f(counters);
            return;
        }
        // A new key: counted against the bound before it is made.
        if self
            .keys
            .try_update(Ordering::AcqRel, Ordering::Acquire, |keys| {
                (keys < MAX_EDGES).then_some(keys + 1)
            })
            .is_err()
        {
            return;
        }
        let protocols = shard
            .entry(allocation_id)
            .or_default()
            .entry(service.to_string())
            .or_default()
            .entry(tag.to_string())
            .or_default();
        let mut counters = Counters::default();
        f(&mut counters);
        protocols.push((protocol, counters));
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

    /// `count` L4 dials at once (the kernel classifier's counters, read once
    /// per heartbeat).
    pub fn record_connections(&self, allocation_id: i64, service: &str, ok: bool, count: u64) {
        self.with(allocation_id, service, None, "tcp", |c| {
            if ok {
                c.requests += count;
            } else {
                c.errors += count;
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
        let mut out: Vec<NetEdgeSample> = Vec::new();
        for shard in &self.shards {
            let drained = std::mem::take(&mut *shard.lock().unwrap_or_else(|p| p.into_inner()));
            for (allocation_id, services) in drained {
                for (service, tags) in services {
                    for (tag, protocols) in tags {
                        for (protocol, c) in protocols {
                            self.keys.fetch_sub(1, Ordering::AcqRel);
                            out.push(NetEdgeSample {
                                allocation_id,
                                service: service.clone(),
                                tag: tag.clone(),
                                protocol: protocol.to_string(),
                                requests: c.requests,
                                errors: c.errors,
                                denied: c.denied,
                                latency_ms_sum: c.latency_ms_sum,
                                latency_ms_max: c.latency_ms_max,
                                bytes_out: c.bytes_out,
                                bytes_in: c.bytes_in,
                            });
                        }
                    }
                }
            }
        }
        out.sort_by(|a, b| {
            a.allocation_id
                .cmp(&b.allocation_id)
                .then_with(|| a.service.cmp(&b.service))
                .then_with(|| a.tag.cmp(&b.tag))
                .then_with(|| a.protocol.cmp(&b.protocol))
        });
        out
    }

    /// Put drained samples back after a failed heartbeat, merging with
    /// anything recorded since.
    pub fn restore(&self, samples: Vec<NetEdgeSample>) {
        for sample in samples {
            let tag = (!sample.tag.is_empty()).then_some(sample.tag.as_str());
            self.with(
                sample.allocation_id,
                &sample.service,
                tag,
                protocol_name(&sample.protocol),
                |c| {
                    c.requests += sample.requests;
                    c.errors += sample.errors;
                    c.denied += sample.denied;
                    c.latency_ms_sum += sample.latency_ms_sum;
                    c.latency_ms_max = c.latency_ms_max.max(sample.latency_ms_max);
                    c.bytes_out += sample.bytes_out;
                    c.bytes_in += sample.bytes_in;
                },
            );
        }
    }

    /// Distinct keys pending, for tests.
    #[cfg(test)]
    pub fn pending_len(&self) -> usize {
        self.keys.load(Ordering::Acquire)
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
        // A drain frees the bound.
        assert_eq!(ledger.drain().len(), MAX_EDGES);
        assert_eq!(ledger.pending_len(), 0);
        ledger.record_connection(1, "after", None, true);
        assert_eq!(ledger.pending_len(), 1);
    }

    /// Recording from many threads at once loses nothing (the shards are
    /// independent locks).
    #[test]
    fn concurrent_recording_across_allocations_adds_up() {
        let ledger = std::sync::Arc::new(EdgeLedger::new());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let ledger = ledger.clone();
                std::thread::spawn(move || {
                    for i in 0..1000 {
                        ledger.record_connection(i64::from(i % 32), "db", Some("v1"), t % 2 == 0);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let samples = ledger.drain();
        assert_eq!(samples.len(), 32);
        let total: u64 = samples.iter().map(|s| s.requests + s.errors).sum();
        assert_eq!(total, 8000);
    }
}
