//! Scaling inputs: one cluster-wide read of how busy each task is
//! (docs/autoscaling.md, "Scaling inputs"; autoscaler-v2 plan §6.7, calls
//! entry 11).
//!
//! Usage reports and service-graph samples reach whichever manager a worker
//! heartbeats to, and stay there in memory (`src/manager/usage.rs`). This
//! module gets them to one place without touching raft:
//!
//! 1. **Roll up.** Every [`ROLLUP_INTERVAL`] each manager folds the latest
//!    usage report of every node it hears from, and the edge samples its
//!    workers sent since the last fold, into one [`Rollup`] per
//!    (node, task) — sums, maxima, and a fixed-bucket histogram of
//!    per-replica CPU utilization against the reservation
//!    ([`build_rollups`]).
//! 2. **Ship.** A manager that does not lead the status lane sends the
//!    rollups that changed to the leader on the liveness digest RPC
//!    ([`RollupSender`]), delta-encoded under an epoch and a sequence number.
//! 3. **Merge.** The leader keeps the newest rollup per (node, task),
//!    whichever manager sent it, so a node that moves between managers is
//!    never counted twice ([`RollupMerge`]); sums them per task on read; and
//!    keeps an hour of 10-second per-task totals.
//!
//! Everything here is pure over its inputs (no store, no clock reads except
//! through arguments), so the merge rules are tested without a cluster. The
//! store reads and the RPC live in [`RollupTicker`] and in the manager's
//! `GetScalingInputs` handler.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::structs::{NetEdgeSample, RollupSync, TargetRollup, UsageReport};

/// How often a manager folds and ships its rollups: the worker usage report
/// interval, so every rollup carries at most one new sample per node.
pub const ROLLUP_INTERVAL: Duration = Duration::from_secs(10);

/// An unchanged rollup is re-sent at least this often (when its node keeps
/// reporting), so a node that came back to a manager whose last word on it
/// was stale reclaims its (node, task) within a minute.
pub const ROLLUP_REFRESH: Duration = Duration::from_secs(60);

/// A node whose latest usage report is older than this contributes nothing:
/// three missed report intervals. A manager keeps a report for two minutes
/// (`USAGE_STALE_AFTER`), which is too long to call it current.
pub const REPORT_MAX_AGE: Duration = Duration::from_secs(30);

/// The leader drops a manager's rollups when that manager has not confirmed
/// them (a sync, even an empty one) for this long: three intervals and a
/// margin.
pub const PEER_ROLLUP_GRACE: Duration = Duration::from_secs(35);

/// For this long after a manager takes the status lane, every target reads
/// `resyncing`: peers' full syncs land on their next tick, at most one
/// interval away.
pub const RESYNC_WINDOW: Duration = ROLLUP_INTERVAL;

/// Points of history kept per target: one hour at 10 seconds.
pub const HISTORY_POINTS: usize = 360;

/// How far back history reaches.
pub const HISTORY_SPAN: Duration = Duration::from_secs(3600);

/// At most this many targets carry history (about 14 KB each), so a cluster
/// with tens of thousands of tasks cannot grow the leader by gigabytes. A
/// target past the cap still has current inputs, only no history.
pub const HISTORY_MAX_TARGETS: usize = 4096;

/// Upper bounds of the per-replica CPU utilization histogram buckets; one
/// more, unbounded, bucket follows. Chosen around the 70% default target the
/// autoscaler plan uses (§6.3).
pub const CPU_HISTOGRAM_BOUNDS: [f64; 7] = [0.1, 0.25, 0.5, 0.7, 0.85, 1.0, 1.5];

/// Buckets in the histogram: one per bound, plus the overflow bucket.
pub const CPU_HISTOGRAM_BUCKETS: usize = CPU_HISTOGRAM_BOUNDS.len() + 1;

/// Distinct (node, caller allocation, upstream) edge keys held between two
/// folds. A worker already caps its own ledger at 4096 keys per heartbeat;
/// this bounds the sum over every node a manager hears from.
pub const MAX_PENDING_EDGE_KEYS: usize = 65_536;

const BYTES_PER_MIB: f64 = 1024.0 * 1024.0;

/// A task: the unit `legion job scale` sizes and `GetScalingInputs` reports.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TargetKey {
    pub job_id: String,
    pub task_group: String,
    pub task_name: String,
}

impl TargetKey {
    pub fn new(job_id: &str, task_group: &str, task_name: &str) -> Self {
        Self {
            job_id: job_id.to_string(),
            task_group: task_group.to_string(),
            task_name: task_name.to_string(),
        }
    }
}

/// One task on one node: what a rollup is keyed by.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RollupKey {
    pub node_id: String,
    pub target: TargetKey,
}

impl RollupKey {
    pub fn new(node_id: &str, target: TargetKey) -> Self {
        Self {
            node_id: node_id.to_string(),
            target,
        }
    }
}

/// A replica's CPU reservation, as its stored resources normalize.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CpuReservation {
    /// No CPU reserved: utilization cannot be computed.
    None,
    /// `cpu` in MHz.
    Mhz(f64),
    /// `cores = N`: N x 1000 millicores, N x the node's clock in MHz.
    Cores(f64),
}

impl CpuReservation {
    /// From a normalized resource map's `cpu` and `cores` amounts. `cores`
    /// wins when both are present, as normalization keeps only one.
    pub fn from_amounts(cpu_mhz: Option<f64>, cores: Option<f64>) -> Self {
        match (cores, cpu_mhz) {
            (Some(cores), _) if cores > 0.0 => CpuReservation::Cores(cores),
            (_, Some(mhz)) if mhz > 0.0 => CpuReservation::Mhz(mhz),
            _ => CpuReservation::None,
        }
    }

    /// The reservation in millicores on a node with `mhz_per_core`, when it
    /// can be known.
    pub fn millicores(self, mhz_per_core: Option<f64>) -> Option<f64> {
        let millicores = match self {
            CpuReservation::None => return None,
            CpuReservation::Cores(cores) => cores * 1000.0,
            CpuReservation::Mhz(mhz) => mhz / mhz_per_core.filter(|clock| *clock > 0.0)? * 1000.0,
        };
        (millicores.is_finite() && millicores > 0.0).then_some(millicores)
    }

    /// The reservation in MHz on a node with `mhz_per_core`, when it can be
    /// known.
    pub fn mhz(self, mhz_per_core: Option<f64>) -> Option<f64> {
        match self {
            CpuReservation::None => None,
            CpuReservation::Mhz(mhz) => Some(mhz),
            CpuReservation::Cores(cores) => mhz_per_core.map(|clock| cores * clock),
        }
        .filter(|mhz| mhz.is_finite() && *mhz > 0.0)
    }
}

/// What a rollup needs to know about one allocation, looked up once per
/// allocation (its task and reservation never change while it lives).
#[derive(Debug, Clone, PartialEq)]
pub struct AllocMeta {
    pub target: TargetKey,
    pub cpu: CpuReservation,
    pub memory_mib: f64,
    /// The allocation's declared upstreams: the key its datapath records
    /// edges under, mapped to the canonical service reference it dials.
    pub upstreams: HashMap<String, String>,
}

/// Requests one node's callers sent to one service over a window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EdgeTotals {
    pub requests: u64,
    pub errors: u64,
    pub latency_ms_sum: u64,
    pub latency_ms_max: u64,
    /// The window the counts cover; 0 means no caller on the node reached
    /// the service.
    pub window_ms: u32,
}

impl EdgeTotals {
    fn add(&mut self, other: &EdgeCounts) {
        self.requests = self.requests.saturating_add(other.requests);
        self.errors = self.errors.saturating_add(other.errors);
        self.latency_ms_sum = self.latency_ms_sum.saturating_add(other.latency_ms_sum);
        self.latency_ms_max = self.latency_ms_max.max(other.latency_ms_max);
    }
}

/// One task's usage and request totals on one node.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rollup {
    /// The newest worker sample folded in (unix ms, worker clock); for a
    /// request-only rollup, the fold time on the sender's clock. The leader
    /// keeps whichever manager's rollup has the larger value.
    pub sampled_at_unix_ms: i64,
    pub replicas: u32,
    pub cpu_used_millicores: u64,
    pub cpu_used_mhz: f64,
    pub cpu_reserved_mhz: f64,
    pub cpu_util_used_millicores: u64,
    pub cpu_util_reserved_millicores: f64,
    pub cpu_utilization_max: f64,
    pub cpu_histogram: [u32; CPU_HISTOGRAM_BUCKETS],
    pub cpu_unmeasurable: u32,
    pub memory_used_bytes: u64,
    pub memory_reserved_mib: f64,
    pub memory_utilization_max: f64,
    pub edge: EdgeTotals,
}

/// The histogram bucket a utilization falls in.
pub fn cpu_bucket(utilization: f64) -> usize {
    CPU_HISTOGRAM_BOUNDS
        .iter()
        .position(|bound| utilization < *bound)
        .unwrap_or(CPU_HISTOGRAM_BOUNDS.len())
}

impl Rollup {
    /// Fold one replica's sample in: `cpu_millicores` and `memory_bytes` as
    /// the worker measured them, against `meta`'s reservation, on a node
    /// with `mhz_per_core` (`None` when the node reports no clock speed).
    ///
    /// MHz uses `GetNodeUsage`'s arithmetic (`millicores / 1000 x
    /// mhz_per_core`); utilization divides in millicores, where a `cores = N`
    /// reservation is N x 1000 whatever the clock.
    pub fn fold_replica(
        &mut self,
        cpu_millicores: u64,
        memory_bytes: u64,
        meta: &AllocMeta,
        mhz_per_core: Option<f64>,
    ) {
        self.replicas = self.replicas.saturating_add(1);
        self.cpu_used_millicores = self.cpu_used_millicores.saturating_add(cpu_millicores);
        if let Some(clock) = mhz_per_core {
            self.cpu_used_mhz += cpu_millicores as f64 / 1000.0 * clock;
        }
        if let Some(reserved) = meta.cpu.mhz(mhz_per_core) {
            self.cpu_reserved_mhz += reserved;
        }
        match meta.cpu.millicores(mhz_per_core) {
            Some(reserved) => {
                let utilization = cpu_millicores as f64 / reserved;
                self.cpu_util_used_millicores =
                    self.cpu_util_used_millicores.saturating_add(cpu_millicores);
                self.cpu_util_reserved_millicores += reserved;
                self.cpu_utilization_max = self.cpu_utilization_max.max(utilization);
                self.cpu_histogram[cpu_bucket(utilization)] += 1;
            }
            None => self.cpu_unmeasurable = self.cpu_unmeasurable.saturating_add(1),
        }
        self.memory_used_bytes = self.memory_used_bytes.saturating_add(memory_bytes);
        if meta.memory_mib > 0.0 {
            self.memory_reserved_mib += meta.memory_mib;
            let utilization = memory_bytes as f64 / BYTES_PER_MIB / meta.memory_mib;
            self.memory_utilization_max = self.memory_utilization_max.max(utilization);
        }
    }

    /// Nothing to report: no replica sampled and no caller seen.
    pub fn is_empty(&self) -> bool {
        self.replicas == 0 && self.edge.window_ms == 0
    }

    /// Equal in everything but the sample time: what the sender's delta
    /// compares, so a steady rollup is not re-sent every interval.
    pub fn same_values(&self, other: &Rollup) -> bool {
        let mut stamped = other.clone();
        stamped.sampled_at_unix_ms = self.sampled_at_unix_ms;
        *self == stamped
    }

    pub fn to_proto(&self, key: &RollupKey) -> TargetRollup {
        TargetRollup {
            node_id: key.node_id.clone(),
            job_id: key.target.job_id.clone(),
            task_group: key.target.task_group.clone(),
            task_name: key.target.task_name.clone(),
            sampled_at_unix_ms: self.sampled_at_unix_ms,
            removed: false,
            replicas: self.replicas,
            cpu_used_millicores: self.cpu_used_millicores,
            cpu_used_mhz: self.cpu_used_mhz,
            cpu_reserved_mhz: self.cpu_reserved_mhz,
            cpu_util_used_millicores: self.cpu_util_used_millicores,
            cpu_util_reserved_millicores: self.cpu_util_reserved_millicores,
            cpu_utilization_max: self.cpu_utilization_max,
            cpu_utilization_histogram: self.cpu_histogram.to_vec(),
            cpu_unmeasurable: self.cpu_unmeasurable,
            memory_used_bytes: self.memory_used_bytes,
            memory_reserved_mib: self.memory_reserved_mib,
            memory_utilization_max: self.memory_utilization_max,
            edge_requests: self.edge.requests,
            edge_errors: self.edge.errors,
            edge_latency_ms_sum: self.edge.latency_ms_sum,
            edge_latency_ms_max: self.edge.latency_ms_max,
            edge_window_ms: self.edge.window_ms,
        }
    }

    /// The key and, unless the wire form is a removal, the rollup.
    pub fn from_proto(wire: &TargetRollup) -> (RollupKey, Option<Rollup>) {
        let key = RollupKey::new(
            &wire.node_id,
            TargetKey::new(&wire.job_id, &wire.task_group, &wire.task_name),
        );
        if wire.removed {
            return (key, None);
        }
        let mut histogram = [0u32; CPU_HISTOGRAM_BUCKETS];
        for (slot, count) in histogram.iter_mut().zip(&wire.cpu_utilization_histogram) {
            *slot = *count;
        }
        let rollup = Rollup {
            sampled_at_unix_ms: wire.sampled_at_unix_ms,
            replicas: wire.replicas,
            cpu_used_millicores: wire.cpu_used_millicores,
            cpu_used_mhz: wire.cpu_used_mhz,
            cpu_reserved_mhz: wire.cpu_reserved_mhz,
            cpu_util_used_millicores: wire.cpu_util_used_millicores,
            cpu_util_reserved_millicores: wire.cpu_util_reserved_millicores,
            cpu_utilization_max: wire.cpu_utilization_max,
            cpu_histogram: histogram,
            cpu_unmeasurable: wire.cpu_unmeasurable,
            memory_used_bytes: wire.memory_used_bytes,
            memory_reserved_mib: wire.memory_reserved_mib,
            memory_utilization_max: wire.memory_utilization_max,
            edge: EdgeTotals {
                requests: wire.edge_requests,
                errors: wire.edge_errors,
                latency_ms_sum: wire.edge_latency_ms_sum,
                latency_ms_max: wire.edge_latency_ms_max,
                window_ms: wire.edge_window_ms,
            },
        };
        (key, Some(rollup))
    }
}

/// A removal on the wire.
fn removed_proto(key: &RollupKey) -> TargetRollup {
    TargetRollup {
        node_id: key.node_id.clone(),
        job_id: key.target.job_id.clone(),
        task_group: key.target.task_group.clone(),
        task_name: key.target.task_name.clone(),
        removed: true,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Edge accumulation (heartbeat path)
// ---------------------------------------------------------------------------

/// Counters for one (node, calling allocation, upstream key) since the last
/// fold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EdgeCounts {
    pub requests: u64,
    pub errors: u64,
    pub latency_ms_sum: u64,
    pub latency_ms_max: u64,
}

/// (node, calling allocation, upstream key).
pub type EdgeAccumulatorKey = (String, i64, String);

/// Edge samples since the last fold, keyed the cheapest way the heartbeat
/// path can: no store reads, no parsing, one map entry per sample. The fold
/// resolves each key to the task behind the service.
#[derive(Debug, Default)]
pub struct EdgeAccumulator {
    pending: HashMap<EdgeAccumulatorKey, EdgeCounts>,
}

impl EdgeAccumulator {
    /// Fold one heartbeat's samples. Denials are not requests the service
    /// served and are left out; samples with nothing in them are skipped.
    /// Past [`MAX_PENDING_EDGE_KEYS`] new keys are dropped until the next
    /// fold.
    pub fn record(&mut self, node_id: &str, samples: &[NetEdgeSample]) {
        for sample in samples {
            if sample.requests == 0 && sample.errors == 0 {
                continue;
            }
            let key = (
                node_id.to_string(),
                sample.allocation_id,
                sample.service.clone(),
            );
            if !self.pending.contains_key(&key) && self.pending.len() >= MAX_PENDING_EDGE_KEYS {
                continue;
            }
            let counts = self.pending.entry(key).or_default();
            counts.requests = counts.requests.saturating_add(sample.requests);
            counts.errors = counts.errors.saturating_add(sample.errors);
            counts.latency_ms_sum = counts.latency_ms_sum.saturating_add(sample.latency_ms_sum);
            counts.latency_ms_max = counts.latency_ms_max.max(sample.latency_ms_max);
        }
    }

    /// Everything since the last drain.
    pub fn drain(&mut self) -> HashMap<EdgeAccumulatorKey, EdgeCounts> {
        std::mem::take(&mut self.pending)
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Building rollups (every manager, every interval)
// ---------------------------------------------------------------------------

/// One node's current usage, as the fold reads it: the report's sample time
/// and, per allocation, `(allocation id, cpu millicores, memory bytes)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSample {
    pub node_id: String,
    pub sampled_at_unix_ms: i64,
    pub allocations: Vec<(i64, u64, u64)>,
}

impl NodeSample {
    pub fn from_report(node_id: &str, report: &UsageReport) -> Self {
        Self {
            node_id: node_id.to_string(),
            sampled_at_unix_ms: report.sampled_at_unix_ms,
            allocations: report
                .allocations
                .iter()
                .map(|usage| {
                    (
                        usage.allocation_id,
                        usage.cpu_millicores,
                        usage.memory_current_bytes,
                    )
                })
                .collect(),
        }
    }
}

/// What [`build_rollups`] reads besides the samples.
pub struct BuildContext<'a> {
    /// Per allocation: its task, reservation, and upstreams. Allocations
    /// missing here (gone from the store) are skipped.
    pub allocations: &'a HashMap<i64, Arc<AllocMeta>>,
    /// Per node: average clock per core, `None` when the node reports none.
    pub mhz_per_core: &'a HashMap<String, Option<f64>>,
    /// Per canonical service reference: the tasks that back it.
    pub services: &'a HashMap<String, Vec<TargetKey>>,
    /// The window the edge counts cover.
    pub edge_window_ms: u32,
    /// The fold time, for request-only rollups on nodes with no usage.
    pub now_unix_ms: i64,
}

/// Fold current node samples and the edge counts since the last fold into
/// one rollup per (node, task). Bounded by the number of (node, task) pairs,
/// never by the number of samples.
pub fn build_rollups(
    samples: &[NodeSample],
    edges: &HashMap<EdgeAccumulatorKey, EdgeCounts>,
    context: &BuildContext<'_>,
) -> HashMap<RollupKey, Rollup> {
    let mut rollups: HashMap<RollupKey, Rollup> = HashMap::new();
    let mut node_sampled_at: HashMap<&str, i64> = HashMap::new();
    for sample in samples {
        node_sampled_at.insert(sample.node_id.as_str(), sample.sampled_at_unix_ms);
        let clock = context.mhz_per_core.get(&sample.node_id).copied().flatten();
        for (allocation_id, millicores, memory_bytes) in &sample.allocations {
            let Some(meta) = context.allocations.get(allocation_id) else {
                continue;
            };
            let rollup = rollups
                .entry(RollupKey::new(&sample.node_id, meta.target.clone()))
                .or_default();
            rollup.fold_replica(*millicores, *memory_bytes, meta, clock);
            rollup.sampled_at_unix_ms = rollup.sampled_at_unix_ms.max(sample.sampled_at_unix_ms);
        }
    }
    for ((node_id, caller, upstream), counts) in edges {
        let Some(meta) = context.allocations.get(caller) else {
            continue;
        };
        // The caller's declaration says which service a key dials; an
        // undeclared key is a service in the caller's own namespace, as
        // `ListServiceEdges` reads it. An external host resolves to no
        // backing task below and is dropped.
        let service = meta.upstreams.get(upstream).cloned().unwrap_or_else(|| {
            crate::structs::namespace::qualify_in(
                crate::structs::namespace::job_namespace(&meta.target.job_id),
                upstream,
            )
        });
        let Some(targets) = context.services.get(&service) else {
            continue;
        };
        for target in targets {
            let rollup = rollups
                .entry(RollupKey::new(node_id, target.clone()))
                .or_default();
            rollup.edge.add(counts);
            rollup.edge.window_ms = context.edge_window_ms.max(1);
            if rollup.sampled_at_unix_ms == 0 {
                rollup.sampled_at_unix_ms = node_sampled_at
                    .get(node_id.as_str())
                    .copied()
                    .unwrap_or(context.now_unix_ms);
            }
        }
    }
    rollups.retain(|_, rollup| !rollup.is_empty());
    rollups
}

// ---------------------------------------------------------------------------
// Shipping rollups (a manager that does not lead the status lane)
// ---------------------------------------------------------------------------

/// The sending half of the rollup protocol: what the leader holds from this
/// manager, and the next sync to bring it up to date.
#[derive(Debug)]
pub struct RollupSender {
    epoch: u64,
    seq: u64,
    /// The last sync the leader applied; `None` sends a full set next.
    acked: Option<u64>,
    /// What the leader holds from this manager as of `acked`.
    sent: HashMap<RollupKey, Rollup>,
    /// The sync in flight and the state it leaves the leader in.
    in_flight: Option<(u64, HashMap<RollupKey, Rollup>)>,
}

impl RollupSender {
    /// A sender for a fresh process: `epoch` should be random so the leader
    /// can tell a restarted sender from a continuing one.
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            seq: 0,
            acked: None,
            sent: HashMap::new(),
            in_flight: None,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether the next sync is a full one.
    pub fn needs_full(&self) -> bool {
        self.acked.is_none()
    }

    /// The sync that brings the leader from what it holds to `current`.
    /// Always produced, even with nothing changed: an empty delta still
    /// confirms every rollup this manager holds, which is what keeps them
    /// from aging out on the leader.
    pub fn next_sync(&mut self, current: &HashMap<RollupKey, Rollup>) -> RollupSync {
        self.seq += 1;
        let refresh_ms = ROLLUP_REFRESH.as_millis() as i64;
        let (full, base_seq, rollups, after) = match self.acked {
            None => {
                let mut keys: Vec<&RollupKey> = current.keys().collect();
                keys.sort();
                let rollups = keys
                    .into_iter()
                    .map(|key| current[key].to_proto(key))
                    .collect();
                (true, 0, rollups, current.clone())
            }
            Some(base) => {
                let mut after = self.sent.clone();
                let mut changed: Vec<TargetRollup> = Vec::new();
                let mut keys: Vec<&RollupKey> = current.keys().collect();
                keys.sort();
                for key in keys {
                    let rollup = &current[key];
                    let resend = match self.sent.get(key) {
                        None => true,
                        Some(previous) => {
                            !previous.same_values(rollup)
                                || (rollup.sampled_at_unix_ms - previous.sampled_at_unix_ms).abs()
                                    >= refresh_ms
                        }
                    };
                    if resend {
                        changed.push(rollup.to_proto(key));
                        after.insert(key.clone(), rollup.clone());
                    }
                }
                let mut gone: Vec<&RollupKey> = self
                    .sent
                    .keys()
                    .filter(|key| !current.contains_key(*key))
                    .collect();
                gone.sort();
                for key in gone {
                    changed.push(removed_proto(key));
                    after.remove(key);
                }
                (false, base, changed, after)
            }
        };
        self.in_flight = Some((self.seq, after));
        RollupSync {
            epoch: self.epoch,
            seq: self.seq,
            base_seq,
            full,
            rollups,
        }
    }

    /// The leader applied the sync last produced.
    pub fn acked(&mut self) {
        if let Some((seq, after)) = self.in_flight.take() {
            self.acked = Some(seq);
            self.sent = after;
        }
    }

    /// The sync failed, or the leader asked for a resync, or leadership
    /// moved: whatever the leader holds is unknown, so send everything next.
    pub fn reset(&mut self) {
        self.in_flight = None;
        self.acked = None;
        self.sent.clear();
    }
}

// ---------------------------------------------------------------------------
// Merging rollups (the status-lane leader)
// ---------------------------------------------------------------------------

/// What the leader did with a sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncVerdict {
    Applied,
    /// The delta did not build on what the leader holds: send a full set.
    Resync,
}

#[derive(Debug, Clone)]
struct PeerSync {
    epoch: u64,
    seq: u64,
    last_sync: Instant,
}

#[derive(Debug, Clone)]
struct MergedEntry {
    owner: String,
    rollup: Rollup,
}

/// One 10-second history point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HistoryPoint {
    pub at_unix_ms: i64,
    pub measured_replicas: u32,
    pub cpu_used_mhz: f32,
    pub cpu_utilization: f32,
    pub memory_used_mib: f32,
    pub memory_utilization: f32,
    pub requests_per_second: f32,
    pub errors_per_second: f32,
    pub latency_ms_mean: f32,
}

/// One task's totals across every node, as the leader holds them now.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TargetTotals {
    pub replicas: u32,
    pub cpu_used_millicores: u64,
    pub cpu_used_mhz: f64,
    pub cpu_reserved_mhz: f64,
    pub cpu_util_used_millicores: u64,
    pub cpu_util_reserved_millicores: f64,
    pub cpu_utilization_max: f64,
    pub cpu_histogram: [u32; CPU_HISTOGRAM_BUCKETS],
    pub cpu_unmeasurable: u32,
    pub memory_used_bytes: u64,
    pub memory_reserved_mib: f64,
    pub memory_utilization_max: f64,
    pub requests_per_second: f64,
    pub errors_per_second: f64,
    pub edge_requests: u64,
    pub edge_latency_ms_sum: u64,
    pub edge_latency_ms_max: u64,
    pub calling_nodes: u32,
    /// Nodes with a rollup for the task.
    pub reporting_nodes: u32,
    pub sampled_at_unix_ms: i64,
    /// How long ago the least recently confirmed rollup was confirmed.
    pub age: Duration,
}

impl TargetTotals {
    /// Sum of used over sum of reserved millicores, over measurable replicas.
    pub fn cpu_utilization(&self) -> f64 {
        if self.cpu_util_reserved_millicores > 0.0 {
            self.cpu_util_used_millicores as f64 / self.cpu_util_reserved_millicores
        } else {
            0.0
        }
    }

    pub fn memory_used_mib(&self) -> f64 {
        self.memory_used_bytes as f64 / BYTES_PER_MIB
    }

    pub fn memory_utilization(&self) -> f64 {
        if self.memory_reserved_mib > 0.0 {
            self.memory_used_mib() / self.memory_reserved_mib
        } else {
            0.0
        }
    }

    /// Failed attempts over all attempts, as `ListServiceEdges` computes it.
    pub fn error_ratio(&self) -> f64 {
        let attempts = self.requests_per_second + self.errors_per_second;
        if attempts > 0.0 {
            self.errors_per_second / attempts
        } else {
            0.0
        }
    }

    pub fn latency_ms_mean(&self) -> f64 {
        if self.edge_requests > 0 {
            self.edge_latency_ms_sum as f64 / self.edge_requests as f64
        } else {
            0.0
        }
    }

    fn point(&self, at_unix_ms: i64) -> HistoryPoint {
        HistoryPoint {
            at_unix_ms,
            measured_replicas: self.replicas,
            cpu_used_mhz: self.cpu_used_mhz as f32,
            cpu_utilization: self.cpu_utilization() as f32,
            memory_used_mib: self.memory_used_mib() as f32,
            memory_utilization: self.memory_utilization() as f32,
            requests_per_second: self.requests_per_second as f32,
            errors_per_second: self.errors_per_second as f32,
            latency_ms_mean: self.latency_ms_mean() as f32,
        }
    }
}

/// Whether a target's numbers can be acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataState {
    Current,
    Missing,
    Resyncing,
}

impl DataState {
    pub fn as_str(self) -> &'static str {
        match self {
            DataState::Current => "current",
            DataState::Missing => "missing",
            DataState::Resyncing => "resyncing",
        }
    }
}

/// The status-lane leader's merged view: the newest rollup per (node, task)
/// from every manager, and an hour of per-task history.
#[derive(Debug, Default)]
pub struct RollupMerge {
    peers: HashMap<String, PeerSync>,
    entries: HashMap<RollupKey, MergedEntry>,
    /// Nodes with an entry, per task: the read index.
    by_target: HashMap<TargetKey, HashSet<String>>,
    history: HashMap<TargetKey, VecDeque<HistoryPoint>>,
    /// When this manager took the status lane; `None` while it does not
    /// lead it.
    responsible_since: Option<Instant>,
    /// Until when peers' full resyncs may still be landing: targets read
    /// `resyncing` before this. `None` when no peer can hold rollups this
    /// manager lacks (a single manager).
    resync_until: Option<Instant>,
}

impl RollupMerge {
    /// Take the status lane: start empty, so every peer's next delta is
    /// refused and answered with a full resync. With `peers`, targets read
    /// `resyncing` for [`RESYNC_WINDOW`], while those resyncs land; a single
    /// manager has nobody to wait for.
    pub fn become_responsible(&mut self, now: Instant, peers: bool) {
        *self = RollupMerge {
            responsible_since: Some(now),
            resync_until: peers.then(|| now + RESYNC_WINDOW),
            ..RollupMerge::default()
        };
    }

    /// Lose the status lane: the merged view belongs to the new leader now.
    pub fn relinquish(&mut self) {
        *self = RollupMerge::default();
    }

    pub fn responsible_since(&self) -> Option<Instant> {
        self.responsible_since
    }

    /// Rollups held, from every manager.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Targets with history.
    pub fn history_targets(&self) -> usize {
        self.history.len()
    }

    fn insert(&mut self, key: RollupKey, owner: &str, rollup: Rollup) {
        self.by_target
            .entry(key.target.clone())
            .or_default()
            .insert(key.node_id.clone());
        self.entries.insert(
            key,
            MergedEntry {
                owner: owner.to_string(),
                rollup,
            },
        );
    }

    fn remove(&mut self, key: &RollupKey) {
        if self.entries.remove(key).is_some() {
            if let Some(nodes) = self.by_target.get_mut(&key.target) {
                nodes.remove(&key.node_id);
                if nodes.is_empty() {
                    self.by_target.remove(&key.target);
                }
            }
        }
    }

    /// The newest rollup wins, whichever manager sent it; a sender always
    /// replaces its own.
    fn upsert(&mut self, key: RollupKey, owner: &str, rollup: Rollup) {
        let replace = match self.entries.get(&key) {
            None => true,
            Some(existing) => {
                existing.owner == owner
                    || rollup.sampled_at_unix_ms > existing.rollup.sampled_at_unix_ms
            }
        };
        if replace {
            self.insert(key, owner, rollup);
        }
    }

    /// A removal only drops the sender's own rollup: when another manager
    /// already holds a newer one for the (node, task), the node moved there
    /// and that one stands.
    fn remove_owned(&mut self, key: &RollupKey, owner: &str) {
        if self
            .entries
            .get(key)
            .is_some_and(|existing| existing.owner == owner)
        {
            self.remove(key);
        }
    }

    fn drop_owner(&mut self, owner: &str, keep: Option<&HashSet<RollupKey>>) {
        let gone: Vec<RollupKey> = self
            .entries
            .iter()
            .filter(|(key, entry)| {
                entry.owner == owner && keep.is_none_or(|keep| !keep.contains(*key))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in &gone {
            self.remove(key);
        }
    }

    /// Apply one peer's sync. A full sync replaces everything the peer sent
    /// before; a delta applies only on top of the sync it names.
    pub fn apply(&mut self, sender: &str, sync: &RollupSync, now: Instant) -> SyncVerdict {
        // A peer's sync can reach a new leader before its own rollup loop
        // has noticed the takeover; the takeover starts here then.
        if self.responsible_since.is_none() {
            self.responsible_since = Some(now);
            self.resync_until = Some(now + RESYNC_WINDOW);
        }
        if !sync.full {
            let builds_on_ours = self
                .peers
                .get(sender)
                .is_some_and(|peer| peer.epoch == sync.epoch && peer.seq == sync.base_seq);
            if !builds_on_ours {
                return SyncVerdict::Resync;
            }
        }
        let decoded: Vec<(RollupKey, Option<Rollup>)> =
            sync.rollups.iter().map(Rollup::from_proto).collect();
        if sync.full {
            let keep: HashSet<RollupKey> = decoded
                .iter()
                .filter(|(_, rollup)| rollup.is_some())
                .map(|(key, _)| key.clone())
                .collect();
            self.drop_owner(sender, Some(&keep));
        }
        for (key, rollup) in decoded {
            match rollup {
                Some(rollup) => self.upsert(key, sender, rollup),
                None => self.remove_owned(&key, sender),
            }
        }
        self.peers.insert(
            sender.to_string(),
            PeerSync {
                epoch: sync.epoch,
                seq: sync.seq,
                last_sync: now,
            },
        );
        SyncVerdict::Applied
    }

    /// Apply the leader's own rollups: a full replacement every interval,
    /// with no wire in between.
    pub fn apply_local(
        &mut self,
        self_id: &str,
        current: &HashMap<RollupKey, Rollup>,
        now: Instant,
    ) {
        let keep: HashSet<RollupKey> = current.keys().cloned().collect();
        self.drop_owner(self_id, Some(&keep));
        for (key, rollup) in current {
            self.upsert(key.clone(), self_id, rollup.clone());
        }
        self.peers.insert(
            self_id.to_string(),
            PeerSync {
                epoch: 0,
                seq: 0,
                last_sync: now,
            },
        );
    }

    /// Drop every rollup of a manager that stopped confirming them.
    pub fn prune(&mut self, now: Instant) {
        let stale: Vec<String> = self
            .peers
            .iter()
            .filter(|(_, peer)| now.saturating_duration_since(peer.last_sync) > PEER_ROLLUP_GRACE)
            .map(|(name, _)| name.clone())
            .collect();
        for owner in stale {
            self.peers.remove(&owner);
            self.drop_owner(&owner, None);
        }
    }

    /// One task's totals across every node, or `None` with no rollup.
    pub fn totals(&self, target: &TargetKey, now: Instant) -> Option<TargetTotals> {
        let nodes = self.by_target.get(target)?;
        let mut totals = TargetTotals::default();
        let mut oldest: Option<Instant> = None;
        for node_id in nodes {
            let key = RollupKey::new(node_id, target.clone());
            let Some(entry) = self.entries.get(&key) else {
                continue;
            };
            let rollup = &entry.rollup;
            totals.reporting_nodes += 1;
            totals.replicas += rollup.replicas;
            totals.cpu_used_millicores += rollup.cpu_used_millicores;
            totals.cpu_used_mhz += rollup.cpu_used_mhz;
            totals.cpu_reserved_mhz += rollup.cpu_reserved_mhz;
            totals.cpu_util_used_millicores += rollup.cpu_util_used_millicores;
            totals.cpu_util_reserved_millicores += rollup.cpu_util_reserved_millicores;
            totals.cpu_utilization_max = totals.cpu_utilization_max.max(rollup.cpu_utilization_max);
            for (sum, count) in totals.cpu_histogram.iter_mut().zip(rollup.cpu_histogram) {
                *sum += count;
            }
            totals.cpu_unmeasurable += rollup.cpu_unmeasurable;
            totals.memory_used_bytes += rollup.memory_used_bytes;
            totals.memory_reserved_mib += rollup.memory_reserved_mib;
            totals.memory_utilization_max = totals
                .memory_utilization_max
                .max(rollup.memory_utilization_max);
            if rollup.edge.window_ms > 0 {
                let seconds = f64::from(rollup.edge.window_ms) / 1000.0;
                totals.calling_nodes += 1;
                totals.requests_per_second += rollup.edge.requests as f64 / seconds;
                totals.errors_per_second += rollup.edge.errors as f64 / seconds;
                totals.edge_requests += rollup.edge.requests;
                totals.edge_latency_ms_sum += rollup.edge.latency_ms_sum;
                totals.edge_latency_ms_max =
                    totals.edge_latency_ms_max.max(rollup.edge.latency_ms_max);
            }
            totals.sampled_at_unix_ms = totals.sampled_at_unix_ms.max(rollup.sampled_at_unix_ms);
            if let Some(peer) = self.peers.get(&entry.owner) {
                oldest = Some(oldest.map_or(peer.last_sync, |at| at.min(peer.last_sync)));
            }
        }
        if totals.reporting_nodes == 0 {
            return None;
        }
        totals.age = oldest.map_or(Duration::ZERO, |at| now.saturating_duration_since(at));
        Some(totals)
    }

    /// Whether `target`'s numbers can be acted on now.
    pub fn data_state(&self, has_totals: bool, now: Instant) -> DataState {
        let resyncing = self.resync_until.is_some_and(|until| now < until);
        if resyncing {
            DataState::Resyncing
        } else if has_totals {
            DataState::Current
        } else {
            DataState::Missing
        }
    }

    /// Append one point per task with rollups, and age out history older
    /// than [`HISTORY_SPAN`]. New tasks past [`HISTORY_MAX_TARGETS`] get no
    /// history.
    pub fn record_history(&mut self, now: Instant, now_unix_ms: i64) {
        let targets: Vec<TargetKey> = self.by_target.keys().cloned().collect();
        for target in targets {
            let Some(totals) = self.totals(&target, now) else {
                continue;
            };
            if !self.history.contains_key(&target) && self.history.len() >= HISTORY_MAX_TARGETS {
                continue;
            }
            let ring = self.history.entry(target).or_default();
            ring.push_back(totals.point(now_unix_ms));
            while ring.len() > HISTORY_POINTS {
                ring.pop_front();
            }
        }
        let cutoff = now_unix_ms - HISTORY_SPAN.as_millis() as i64;
        self.history.retain(|_, ring| {
            while ring.front().is_some_and(|point| point.at_unix_ms < cutoff) {
                ring.pop_front();
            }
            !ring.is_empty()
        });
    }

    /// A task's history points at or after `since_unix_ms`, oldest first.
    pub fn history(&self, target: &TargetKey, since_unix_ms: i64) -> Vec<HistoryPoint> {
        self.history
            .get(target)
            .map(|ring| {
                ring.iter()
                    .filter(|point| point.at_unix_ms >= since_unix_ms)
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// The scaling-input soft state a manager holds, shared between the
/// heartbeat handler (edge samples in), the rollup loop (fold, ship, merge),
/// the raft transport (peers' syncs in), and `GetScalingInputs` (reads).
/// Never persisted: a restart or a failover rebuilds it within an interval.
#[derive(Debug, Default)]
pub struct ScalingRollups {
    edges: Mutex<EdgeAccumulator>,
    merge: Mutex<RollupMerge>,
}

impl ScalingRollups {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one heartbeat's edge samples. A map insert per sample; no store
    /// reads.
    pub fn record_edges(&self, node_id: &str, samples: &[NetEdgeSample]) {
        if samples.is_empty() {
            return;
        }
        self.edges
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .record(node_id, samples);
    }

    /// Every edge sample since the last call.
    pub fn take_edges(&self) -> HashMap<EdgeAccumulatorKey, EdgeCounts> {
        self.edges.lock().unwrap_or_else(|p| p.into_inner()).drain()
    }

    /// The leader's merged view.
    pub fn merge(&self) -> std::sync::MutexGuard<'_, RollupMerge> {
        self.merge.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Apply a peer's sync (the raft transport's `ReportNodeLiveness`).
    pub fn apply_peer_sync(&self, sender: &str, sync: &RollupSync) -> SyncVerdict {
        self.merge().apply(sender, sync, Instant::now())
    }
}

// ---------------------------------------------------------------------------
// The rollup loop
// ---------------------------------------------------------------------------

/// How long a node's clock speed is trusted before it is read again. A
/// node's fingerprint changes only when its worker re-registers.
const CLOCK_REFRESH: Duration = Duration::from_secs(300);

/// What one [`RollupTicker::tick`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TickOutcome {
    /// This manager leads the status lane and merged its own rollups.
    Merged { rollups: usize },
    /// Shipped to the leader, which applied it.
    Sent { rollups: usize, full: bool },
    /// The leader asked for a full set and took it.
    Resynced { rollups: usize },
    /// Not delivered (no leader, an error, a refused resync); the next tick
    /// sends a full set.
    NotSent { reason: String },
}

impl TickOutcome {
    /// The `outcome` label on `legion_scaling_rollup_syncs_total`.
    pub fn metric_label(&self) -> &'static str {
        match self {
            TickOutcome::Merged { .. } => "local",
            TickOutcome::Sent { .. } => "ok",
            TickOutcome::Resynced { .. } => "resync",
            TickOutcome::NotSent { .. } => "error",
        }
    }
}

/// One manager's rollup loop state: the sender half of the protocol, and
/// caches that keep the steady-state fold free of store reads.
#[derive(Debug)]
pub struct RollupTicker {
    manager_id: String,
    sender: RollupSender,
    allocations: HashMap<i64, Arc<AllocMeta>>,
    clocks: HashMap<String, Option<f64>>,
    clocks_read_at: HashMap<String, Instant>,
    was_leader: Option<bool>,
    last_fold: Option<Instant>,
    last_local: usize,
}

impl RollupTicker {
    /// A ticker for the manager the digest identifies as `manager_id`, with a
    /// fresh random epoch.
    pub fn new(manager_id: &str) -> Self {
        Self {
            manager_id: manager_id.to_string(),
            sender: RollupSender::new(rand::random::<u64>()),
            allocations: HashMap::new(),
            clocks: HashMap::new(),
            clocks_read_at: HashMap::new(),
            was_leader: None,
            last_fold: None,
            last_local: 0,
        }
    }

    /// Rollups the last tick built from this manager's own reports.
    pub fn last_local(&self) -> usize {
        self.last_local
    }

    /// Fold this manager's current usage and the edge samples since the last
    /// tick, then merge them (status-lane leader) or ship them to the leader
    /// (everyone else). Run every [`ROLLUP_INTERVAL`].
    pub async fn tick(
        &mut self,
        store: &crate::storage::store::Store,
        usage: &Mutex<crate::manager::usage::UsageTable>,
    ) -> TickOutcome {
        let now = Instant::now();
        let now_unix_ms = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let now_unix_ms = i64::try_from(now_unix_ms).unwrap_or(i64::MAX);
        let window = self
            .last_fold
            .map_or(ROLLUP_INTERVAL, |at| now.saturating_duration_since(at))
            .clamp(Duration::from_secs(1), Duration::from_secs(60));
        self.last_fold = Some(now);
        let samples = usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .current_samples(now, REPORT_MAX_AGE);
        let edges = store.scaling_rollups().take_edges();
        let current = self.fold(store, &samples, &edges, window, now, now_unix_ms);
        self.last_local = current.len();

        let leader = store.is_raft_leader(crate::storage::Lane::Status);
        if self.was_leader != Some(leader) {
            // Whatever the leader holds from this manager is unknown across
            // a leadership change, in either direction: start with a full
            // set.
            self.sender.reset();
            self.was_leader = Some(leader);
        }
        if leader {
            let mut merge = store.scaling_rollups().merge();
            if merge.responsible_since().is_none() {
                // Any other manager may hold rollups for nodes that dial it;
                // only a manager without raft is sure to be alone.
                merge.become_responsible(now, store.raft_node_id().is_some());
            }
            merge.apply_local(&self.manager_id, &current, now);
            merge.prune(now);
            merge.record_history(now, now_unix_ms);
            return TickOutcome::Merged {
                rollups: current.len(),
            };
        }
        {
            let mut merge = store.scaling_rollups().merge();
            if merge.responsible_since().is_some() {
                merge.relinquish();
            }
        }
        self.ship(store, &current).await
    }

    async fn ship(
        &mut self,
        store: &crate::storage::store::Store,
        current: &HashMap<RollupKey, Rollup>,
    ) -> TickOutcome {
        use crate::storage::liveness::DigestOutcome;
        let sync = self.sender.next_sync(current);
        let (full, rollups) = (sync.full, sync.rollups.len());
        let (outcome, resync) = store.report_scaling_rollups(&self.manager_id, sync).await;
        match outcome {
            DigestOutcome::Accepted { .. } if !resync => {
                self.sender.acked();
                TickOutcome::Sent { rollups, full }
            }
            DigestOutcome::Accepted { .. } => {
                // The leader holds nothing it can apply this delta to (it
                // just took the lane): answer with everything now rather than
                // leave its targets missing for another interval.
                self.sender.reset();
                let full = self.sender.next_sync(current);
                let rollups = full.rollups.len();
                let (outcome, again) = store.report_scaling_rollups(&self.manager_id, full).await;
                if matches!(outcome, DigestOutcome::Accepted { .. }) && !again {
                    self.sender.acked();
                    TickOutcome::Resynced { rollups }
                } else {
                    self.sender.reset();
                    TickOutcome::NotSent {
                        reason: format!("full resync not applied: {outcome:?}"),
                    }
                }
            }
            other => {
                self.sender.reset();
                TickOutcome::NotSent {
                    reason: format!("{other:?}"),
                }
            }
        }
    }

    /// Resolve what the samples reference (allocations, node clocks, the
    /// service catalog) and build the rollups. Store reads happen only for
    /// allocations and nodes this manager has not seen before.
    fn fold(
        &mut self,
        store: &crate::storage::store::Store,
        samples: &[NodeSample],
        edges: &HashMap<EdgeAccumulatorKey, EdgeCounts>,
        window: Duration,
        now: Instant,
        now_unix_ms: i64,
    ) -> HashMap<RollupKey, Rollup> {
        let wanted: HashSet<i64> = samples
            .iter()
            .flat_map(|sample| sample.allocations.iter().map(|(id, _, _)| *id))
            .chain(edges.keys().map(|(_, caller, _)| *caller))
            .collect();
        self.allocations.retain(|id, _| wanted.contains(id));
        let mut missing: Vec<i64> = wanted
            .iter()
            .filter(|id| !self.allocations.contains_key(id))
            .copied()
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            match store.scaling_alloc_rows(&missing) {
                Ok(rows) => {
                    for row in rows {
                        let meta = alloc_meta(&store.resource_policy, row);
                        self.allocations.insert(meta.0, Arc::new(meta.1));
                    }
                }
                Err(err) => {
                    tracing::debug!(error = ?err, "Scaling rollup: allocation lookup failed; retrying next tick")
                }
            }
        }

        let reporting: HashSet<&str> = samples.iter().map(|s| s.node_id.as_str()).collect();
        self.clocks
            .retain(|node, _| reporting.contains(node.as_str()));
        self.clocks_read_at
            .retain(|node, _| reporting.contains(node.as_str()));
        for node_id in reporting {
            let fresh = self
                .clocks_read_at
                .get(node_id)
                .is_some_and(|at| now.saturating_duration_since(*at) < CLOCK_REFRESH);
            if fresh {
                continue;
            }
            let clock = store
                .get_node(node_id)
                .ok()
                .and_then(|node| crate::resources::NodeCapacity::of(&node).ok())
                .and_then(|capacity| capacity.mhz_per_core);
            self.clocks.insert(node_id.to_string(), clock);
            self.clocks_read_at.insert(node_id.to_string(), now);
        }

        // The service catalog is read only when a caller reached something:
        // a fleet with no service traffic pays nothing for it.
        let services = if edges.is_empty() {
            HashMap::new()
        } else {
            store.scaling_service_targets().unwrap_or_else(|err| {
                tracing::debug!(error = ?err, "Scaling rollup: service lookup failed");
                HashMap::new()
            })
        };
        build_rollups(
            samples,
            edges,
            &BuildContext {
                allocations: &self.allocations,
                mhz_per_core: &self.clocks,
                services: &services,
                edge_window_ms: u32::try_from(window.as_millis()).unwrap_or(u32::MAX),
                now_unix_ms,
            },
        )
    }
}

/// An allocation's rollup facts from its row: the task, the reservation as
/// the scheduler normalizes it, and its declared upstreams.
pub fn alloc_meta(
    policy: &crate::resources::ResourcePolicy,
    row: crate::storage::store::ScalingAllocRow,
) -> (i64, AllocMeta) {
    let request = policy.usage_request(&row.resources).ok();
    let amount = |name: &str| request.as_ref().and_then(|request| request.amount(name));
    let upstreams = row
        .connect
        .as_deref()
        .and_then(|raw| serde_json::from_str::<crate::structs::ConnectConfig>(raw).ok())
        .map(|connect| {
            connect
                .upstreams
                .iter()
                .filter_map(|upstream| {
                    upstream
                        .service_ref(&row.job_id)
                        .map(|service| (upstream.key(), service))
                })
                .collect()
        })
        .unwrap_or_default();
    (
        row.allocation_id,
        AllocMeta {
            target: TargetKey::new(&row.job_id, &row.task_group, &row.task_name),
            cpu: CpuReservation::from_amounts(amount("cpu"), amount("cores")),
            memory_mib: amount("memory").unwrap_or(0.0),
            upstreams,
        },
    )
}

/// Run the rollup loop forever, one tick per [`ROLLUP_INTERVAL`].
pub async fn run_rollup_loop(
    store: crate::storage::store::Store,
    usage: Arc<Mutex<crate::manager::usage::UsageTable>>,
    manager_id: String,
) {
    let mut ticker = RollupTicker::new(&manager_id);
    let mut interval = tokio::time::interval(ROLLUP_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let outcome = ticker.tick(&store, &usage).await;
        let telemetry = crate::manager::telemetry::metrics();
        telemetry
            .scaling_rollup_syncs_total
            .inc(&[outcome.metric_label()]);
        let merged = store.scaling_rollups().merge().len();
        telemetry
            .scaling_rollup_rows
            .set(&["merged"], i64::try_from(merged).unwrap_or(i64::MAX));
        telemetry.scaling_rollup_rows.set(
            &["local"],
            i64::try_from(ticker.last_local()).unwrap_or(i64::MAX),
        );
        match &outcome {
            TickOutcome::Merged { .. } | TickOutcome::Sent { .. } => {}
            TickOutcome::Resynced { rollups } => {
                tracing::info!(
                    rollups,
                    "The status-lane leader asked for a full scaling-input resync; sent"
                );
            }
            TickOutcome::NotSent { reason } => {
                tracing::debug!(%reason, "Scaling rollups not delivered; the next tick sends a full set");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn target(task: &str) -> TargetKey {
        TargetKey::new("web", "default", task)
    }

    fn meta(task: &str, cpu: CpuReservation, memory_mib: f64) -> Arc<AllocMeta> {
        Arc::new(AllocMeta {
            target: target(task),
            cpu,
            memory_mib,
            upstreams: HashMap::new(),
        })
    }

    fn rollup_for(node: &str, task: &str, sampled_at: i64, millicores: u64) -> (RollupKey, Rollup) {
        let mut rollup = Rollup::default();
        rollup.fold_replica(
            millicores,
            100 * MIB,
            &meta(task, CpuReservation::Mhz(1000.0), 256.0),
            Some(2000.0),
        );
        rollup.sampled_at_unix_ms = sampled_at;
        (RollupKey::new(node, target(task)), rollup)
    }

    fn set(entries: Vec<(RollupKey, Rollup)>) -> HashMap<RollupKey, Rollup> {
        entries.into_iter().collect()
    }

    /// GetNodeUsage's arithmetic: millicores / 1000 x the node's MHz per
    /// core; utilization against an MHz reservation goes through the same
    /// clock.
    #[test]
    fn mhz_conversion_uses_the_nodes_clock() {
        let mut rollup = Rollup::default();
        // Half a core at 2400 MHz/core = 1200 MHz, against a 1000 MHz
        // reservation (= 416.7 millicores at that clock): 120%.
        rollup.fold_replica(
            500,
            0,
            &meta("api", CpuReservation::Mhz(1000.0), 0.0),
            Some(2400.0),
        );
        assert_eq!(rollup.cpu_used_millicores, 500);
        assert!((rollup.cpu_used_mhz - 1200.0).abs() < 1e-9);
        assert!((rollup.cpu_reserved_mhz - 1000.0).abs() < 1e-9);
        assert!((rollup.cpu_utilization_max - 1.2).abs() < 1e-9);
        assert_eq!(rollup.cpu_histogram[cpu_bucket(1.2)], 1);
        assert_eq!(cpu_bucket(1.2), 6, "1.0 <= 1.2 < 1.5");

        // A node with no clock: MHz and an MHz-reservation utilization are
        // unknowable, but the millicores still count.
        let mut unclocked = Rollup::default();
        unclocked.fold_replica(500, 0, &meta("api", CpuReservation::Mhz(1000.0), 0.0), None);
        assert_eq!(unclocked.cpu_used_millicores, 500);
        assert_eq!(unclocked.cpu_used_mhz, 0.0);
        assert_eq!(unclocked.cpu_unmeasurable, 1);
        assert_eq!(unclocked.cpu_histogram.iter().sum::<u32>(), 0);
    }

    /// `cores = N` is N x 1000 millicores whatever the clock, and N x the
    /// clock in MHz when the node reports one.
    #[test]
    fn cores_reservations_count_as_thousand_millicores_per_core() {
        let cores = meta("worker", CpuReservation::Cores(2.0), 512.0);
        let mut rollup = Rollup::default();
        rollup.fold_replica(1500, 256 * MIB, &cores, Some(3000.0));
        assert!((rollup.cpu_util_reserved_millicores - 2000.0).abs() < 1e-9);
        assert_eq!(rollup.cpu_util_used_millicores, 1500);
        assert!((rollup.cpu_utilization_max - 0.75).abs() < 1e-9);
        assert!((rollup.cpu_reserved_mhz - 6000.0).abs() < 1e-9);
        assert!((rollup.cpu_used_mhz - 4500.0).abs() < 1e-9);
        assert!((rollup.memory_utilization_max - 0.5).abs() < 1e-9);

        // On an unclocked node (Apple Silicon) the utilization still holds.
        let mut unclocked = Rollup::default();
        unclocked.fold_replica(1500, 0, &cores, None);
        assert_eq!(unclocked.cpu_unmeasurable, 0);
        assert!((unclocked.cpu_utilization_max - 0.75).abs() < 1e-9);
        assert_eq!(unclocked.cpu_reserved_mhz, 0.0);

        assert_eq!(
            CpuReservation::from_amounts(Some(500.0), Some(2.0)),
            CpuReservation::Cores(2.0)
        );
        assert_eq!(
            CpuReservation::from_amounts(Some(500.0), None),
            CpuReservation::Mhz(500.0)
        );
        assert_eq!(
            CpuReservation::from_amounts(Some(0.0), Some(0.0)),
            CpuReservation::None
        );
    }

    #[test]
    fn histogram_buckets_follow_the_fixed_bounds() {
        assert_eq!(cpu_bucket(0.0), 0);
        assert_eq!(cpu_bucket(0.1), 1, "a bound belongs to the bucket above it");
        assert_eq!(cpu_bucket(0.69), 3);
        assert_eq!(cpu_bucket(0.7), 4);
        assert_eq!(cpu_bucket(0.99), 5);
        assert_eq!(cpu_bucket(9.0), CPU_HISTOGRAM_BUCKETS - 1);
    }

    /// Rollups are per (node, task) however many replicas and samples feed
    /// them, and allocations the store no longer knows are skipped.
    #[test]
    fn build_folds_replicas_per_node_and_task() {
        let mut allocations = HashMap::new();
        allocations.insert(1, meta("api", CpuReservation::Mhz(1000.0), 256.0));
        allocations.insert(2, meta("api", CpuReservation::Mhz(1000.0), 256.0));
        allocations.insert(3, meta("db", CpuReservation::Cores(1.0), 1024.0));
        let clocks: HashMap<String, Option<f64>> = [
            ("n1".to_string(), Some(2000.0)),
            ("n2".to_string(), Some(2000.0)),
        ]
        .into();
        let services = HashMap::new();
        let context = BuildContext {
            allocations: &allocations,
            mhz_per_core: &clocks,
            services: &services,
            edge_window_ms: 10_000,
            now_unix_ms: 99,
        };
        let samples = vec![
            NodeSample {
                node_id: "n1".into(),
                sampled_at_unix_ms: 50,
                allocations: vec![(1, 250, 100 * MIB), (2, 750, 200 * MIB), (9, 999, 1)],
            },
            NodeSample {
                node_id: "n2".into(),
                sampled_at_unix_ms: 60,
                allocations: vec![(3, 500, 512 * MIB)],
            },
        ];
        let rollups = build_rollups(&samples, &HashMap::new(), &context);
        assert_eq!(rollups.len(), 2, "one per (node, task); id 9 is unknown");
        let api = &rollups[&RollupKey::new("n1", target("api"))];
        assert_eq!(api.replicas, 2);
        assert_eq!(api.cpu_used_millicores, 1000);
        assert_eq!(api.sampled_at_unix_ms, 50);
        assert_eq!(api.memory_used_bytes, 300 * MIB);
        assert!((api.memory_reserved_mib - 512.0).abs() < 1e-9);
        // 250 mc of 500 = 0.5, 750 of 500 = 1.5.
        assert!((api.cpu_utilization_max - 1.5).abs() < 1e-9);
        assert_eq!(api.cpu_histogram[cpu_bucket(0.5)], 1);
        assert_eq!(api.cpu_histogram[cpu_bucket(1.5)], 1);
        let db = &rollups[&RollupKey::new("n2", target("db"))];
        assert!((db.cpu_utilization_max - 0.5).abs() < 1e-9);
    }

    /// Edge samples land on the task behind the service, on the CALLER's
    /// node, resolved through the caller's declared upstream key; external
    /// hosts resolve to nothing and are dropped.
    #[test]
    fn edges_attribute_to_the_backing_task_on_the_callers_node() {
        let mut caller = AllocMeta {
            target: TargetKey::new("payments/front", "default", "web"),
            cpu: CpuReservation::None,
            memory_mib: 0.0,
            upstreams: HashMap::new(),
        };
        caller
            .upstreams
            .insert("billing".into(), "payments/billing-api".into());
        let allocations: HashMap<i64, Arc<AllocMeta>> = [(7, Arc::new(caller))].into();
        let backing = TargetKey::new("payments/billing", "default", "api");
        let services: HashMap<String, Vec<TargetKey>> = [
            ("payments/billing-api".to_string(), vec![backing.clone()]),
            (
                "payments/ledger".to_string(),
                vec![TargetKey::new("payments/ledger", "default", "db")],
            ),
        ]
        .into();
        let clocks = HashMap::new();
        let context = BuildContext {
            allocations: &allocations,
            mhz_per_core: &clocks,
            services: &services,
            edge_window_ms: 10_000,
            now_unix_ms: 1_234,
        };
        let mut accumulator = EdgeAccumulator::default();
        let sample = |service: &str, requests: u64| NetEdgeSample {
            allocation_id: 7,
            service: service.into(),
            requests,
            errors: 1,
            latency_ms_sum: requests * 20,
            latency_ms_max: 90,
            ..Default::default()
        };
        accumulator.record(
            "caller-node",
            &[sample("billing", 100), sample("example.com", 5)],
        );
        accumulator.record(
            "caller-node",
            &[sample("billing", 50), sample("ledger", 10)],
        );
        let rollups = build_rollups(&[], &accumulator.drain(), &context);
        assert_eq!(rollups.len(), 2, "billing via its alias, ledger undeclared");
        let billing = &rollups[&RollupKey::new("caller-node", backing)];
        assert_eq!(billing.edge.requests, 150);
        assert_eq!(billing.edge.errors, 2);
        assert_eq!(billing.edge.latency_ms_sum, 3000);
        assert_eq!(billing.edge.window_ms, 10_000);
        assert_eq!(billing.replicas, 0, "no replica on the caller's node");
        assert_eq!(billing.sampled_at_unix_ms, 1_234);
        assert!(accumulator.is_empty(), "drained");
    }

    /// The pending-edge map stops growing at its bound.
    #[test]
    fn the_edge_accumulator_is_bounded() {
        let mut accumulator = EdgeAccumulator::default();
        let samples: Vec<NetEdgeSample> = (0..(MAX_PENDING_EDGE_KEYS + 10))
            .map(|n| NetEdgeSample {
                allocation_id: n as i64,
                service: "api".into(),
                requests: 1,
                ..Default::default()
            })
            .collect();
        accumulator.record("n1", &samples);
        assert_eq!(accumulator.len(), MAX_PENDING_EDGE_KEYS);
        // An existing key still accumulates past the bound.
        accumulator.record("n1", &samples[..1]);
        assert_eq!(
            accumulator.drain()[&("n1".to_string(), 0, "api".to_string())].requests,
            2
        );
    }

    /// Size bounds: a rollup's wire form is a small fixed-shape row whatever
    /// fed it, and the set is bounded by (node, task) pairs: 1000
    /// allocations of one task on one node are ONE row.
    #[test]
    fn rollups_are_bounded_by_node_task_pairs_not_samples() {
        use prost::Message;
        let api = meta("api", CpuReservation::Mhz(500.0), 256.0);
        let allocations: HashMap<i64, Arc<AllocMeta>> =
            (0..1000).map(|id| (id, api.clone())).collect();
        let clocks: HashMap<String, Option<f64>> = [("node-0001".to_string(), Some(2400.0))].into();
        let services = HashMap::new();
        let context = BuildContext {
            allocations: &allocations,
            mhz_per_core: &clocks,
            services: &services,
            edge_window_ms: 10_000,
            now_unix_ms: 0,
        };
        let sample = NodeSample {
            node_id: "node-0001".into(),
            sampled_at_unix_ms: 1_700_000_000_000,
            allocations: (0..1000)
                .map(|id| (id, 400 + id as u64, 200 * MIB))
                .collect(),
        };
        let rollups = build_rollups(&[sample], &HashMap::new(), &context);
        assert_eq!(rollups.len(), 1);
        let (key, rollup) = rollups.iter().next().expect("one rollup");
        let wire = rollup.to_proto(key);
        let bytes = wire.encoded_len();
        assert!(bytes < 192, "one rollup row encodes in {bytes} bytes");

        // The plan's sizing: 10,000 tasks on 3 nodes each fit well inside
        // the raft transport's message limit in one full sync.
        let mut sender = RollupSender::new(1);
        let current: HashMap<RollupKey, Rollup> = (0..30_000)
            .map(|n| {
                (
                    RollupKey::new(
                        &format!("node-{:04}", n % 1000),
                        TargetKey::new(&format!("job-{:05}", n / 3), "default", "task"),
                    ),
                    rollup.clone(),
                )
            })
            .collect();
        let full = sender.next_sync(&current);
        let total = full.encoded_len();
        assert!(
            total < 6 * 1024 * 1024,
            "a 30,000-row full sync encodes in {total} bytes"
        );
        assert!(total < crate::storage::raft::network::RAFT_MAX_MESSAGE_BYTES);
    }

    #[test]
    fn wire_round_trip_keeps_every_field() {
        let (key, mut rollup) = rollup_for("n1", "api", 42, 300);
        rollup.edge = EdgeTotals {
            requests: 10,
            errors: 1,
            latency_ms_sum: 200,
            latency_ms_max: 50,
            window_ms: 9_000,
        };
        let (back_key, back) = Rollup::from_proto(&rollup.to_proto(&key));
        assert_eq!(back_key, key);
        assert_eq!(back, Some(rollup));
        let (_, removed) = Rollup::from_proto(&removed_proto(&key));
        assert_eq!(removed, None);
    }

    /// Delta encoding: unchanged rollups are not re-sent, changed ones are,
    /// vanished ones are sent as removals, and an unchanged rollup is
    /// refreshed once its sample time moves a refresh interval on.
    #[test]
    fn the_sender_ships_only_what_changed() {
        let mut sender = RollupSender::new(7);
        let (a, ra) = rollup_for("n1", "api", 1_000, 300);
        let (b, rb) = rollup_for("n2", "api", 1_000, 300);
        let first = sender.next_sync(&set(vec![(a.clone(), ra.clone()), (b.clone(), rb.clone())]));
        assert!(first.full);
        assert_eq!(first.rollups.len(), 2);
        sender.acked();

        // Same values, new sample time inside the refresh interval: nothing
        // to send, but the sync still goes (it confirms).
        let mut ra2 = ra.clone();
        ra2.sampled_at_unix_ms = 11_000;
        let quiet = sender.next_sync(&set(vec![
            (a.clone(), ra2.clone()),
            (b.clone(), rb.clone()),
        ]));
        assert!(!quiet.full);
        assert_eq!(quiet.base_seq, first.seq);
        assert!(quiet.rollups.is_empty());
        sender.acked();

        // b changed, a vanished.
        let (_, rb3) = rollup_for("n2", "api", 21_000, 900);
        let delta = sender.next_sync(&set(vec![(b.clone(), rb3.clone())]));
        assert_eq!(delta.base_seq, quiet.seq);
        let removed: Vec<bool> = delta.rollups.iter().map(|r| r.removed).collect();
        assert_eq!(removed, vec![false, true], "change first, then the removal");
        sender.acked();

        // b unchanged for a refresh interval: re-sent with its new stamp.
        let mut rb4 = rb3.clone();
        rb4.sampled_at_unix_ms = 21_000 + ROLLUP_REFRESH.as_millis() as i64;
        let refresh = sender.next_sync(&set(vec![(b.clone(), rb4)]));
        assert_eq!(refresh.rollups.len(), 1);

        // A failure sends everything next.
        sender.reset();
        assert!(sender.needs_full());
        let again = sender.next_sync(&set(vec![(b, rb3)]));
        assert!(again.full);
    }

    fn sync(
        epoch: u64,
        seq: u64,
        base_seq: u64,
        full: bool,
        rollups: Vec<(RollupKey, Option<Rollup>)>,
    ) -> RollupSync {
        RollupSync {
            epoch,
            seq,
            base_seq,
            full,
            rollups: rollups
                .into_iter()
                .map(|(key, rollup)| match rollup {
                    Some(rollup) => rollup.to_proto(&key),
                    None => removed_proto(&key),
                })
                .collect(),
        }
    }

    /// Three managers, one of them stale: the leader sums the two that keep
    /// confirming, and drops the third's rollups once it has been silent past
    /// the grace.
    #[test]
    fn merge_across_three_managers_drops_the_stale_one() {
        let start = Instant::now();
        let mut merge = RollupMerge::default();
        merge.become_responsible(start, true);
        let (k1, r1) = rollup_for("n1", "api", 1_000, 200);
        let (k2, r2) = rollup_for("n2", "api", 1_000, 300);
        let (k3, r3) = rollup_for("n3", "api", 1_000, 400);
        let (local_key, local) = rollup_for("n0", "api", 1_000, 100);
        merge.apply_local("leader", &set(vec![(local_key, local)]), start);
        for (peer, key, rollup) in [
            ("mgr-b", &k1, &r1),
            ("mgr-c", &k2, &r2),
            ("mgr-d", &k3, &r3),
        ] {
            let verdict = merge.apply(
                peer,
                &sync(1, 1, 0, true, vec![(key.clone(), Some(rollup.clone()))]),
                start,
            );
            assert_eq!(verdict, SyncVerdict::Applied);
        }
        let all = merge.totals(&target("api"), start).expect("totals");
        assert_eq!(all.replicas, 4);
        assert_eq!(all.cpu_used_millicores, 1000);
        assert_eq!(all.reporting_nodes, 4);

        // mgr-b and mgr-c (and the leader) keep confirming; mgr-d goes quiet.
        let later = start + PEER_ROLLUP_GRACE + Duration::from_secs(1);
        for (peer, seq) in [("mgr-b", 2), ("mgr-c", 2)] {
            assert_eq!(
                merge.apply(peer, &sync(1, seq, 1, false, vec![]), later),
                SyncVerdict::Applied
            );
        }
        merge.apply_local(
            "leader",
            &set(vec![rollup_for("n0", "api", 11_000, 100)]),
            later,
        );
        merge.prune(later);
        let fresh = merge.totals(&target("api"), later).expect("totals");
        assert_eq!(fresh.replicas, 3, "the stale manager's node is gone");
        assert_eq!(fresh.cpu_used_millicores, 600);
        assert_eq!(fresh.age, Duration::ZERO);
        assert_eq!(merge.len(), 3);

        // The quiet manager comes back with a delta on its old base: the
        // leader no longer knows it, so it asks for a full set.
        assert_eq!(
            merge.apply("mgr-d", &sync(1, 2, 1, false, vec![]), later),
            SyncVerdict::Resync
        );
    }

    /// A node that moves from one manager to another is counted once: the
    /// newer sample replaces the older, and the old manager's removal does
    /// not take the new one's rollup with it.
    #[test]
    fn a_node_switching_managers_is_counted_once() {
        let now = Instant::now();
        let mut merge = RollupMerge::default();
        merge.become_responsible(now, true);
        let (key, old) = rollup_for("n1", "api", 1_000, 300);
        merge.apply(
            "mgr-a",
            &sync(1, 1, 0, true, vec![(key.clone(), Some(old.clone()))]),
            now,
        );

        // n1 now heartbeats to mgr-b: its newer sample wins.
        let (_, newer) = rollup_for("n1", "api", 11_000, 500);
        merge.apply(
            "mgr-b",
            &sync(2, 1, 0, true, vec![(key.clone(), Some(newer.clone()))]),
            now,
        );
        let totals = merge.totals(&target("api"), now).expect("totals");
        assert_eq!(totals.replicas, 1, "one node, one rollup");
        assert_eq!(totals.cpu_used_millicores, 500);

        // mgr-a still holds n1's last report for a while and refreshes it:
        // older, so it is ignored.
        merge.apply(
            "mgr-a",
            &sync(1, 2, 1, false, vec![(key.clone(), Some(old))]),
            now,
        );
        assert_eq!(
            merge
                .totals(&target("api"), now)
                .expect("totals")
                .cpu_used_millicores,
            500
        );

        // mgr-a drops n1 and says so: mgr-b's rollup stands.
        merge.apply(
            "mgr-a",
            &sync(1, 3, 2, false, vec![(key.clone(), None)]),
            now,
        );
        assert_eq!(
            merge.totals(&target("api"), now).expect("totals").replicas,
            1
        );

        // mgr-b's own removal does take it.
        merge.apply("mgr-b", &sync(2, 2, 1, false, vec![(key, None)]), now);
        assert!(merge.totals(&target("api"), now).is_none());
    }

    /// A new leader starts empty, refuses deltas it cannot place, and is
    /// whole again after each peer's full resync. Until the resync window
    /// passes, targets read `resyncing`.
    #[test]
    fn a_new_leader_resyncs_in_full() {
        let start = Instant::now();
        let mut sender = RollupSender::new(9);
        let (key, rollup) = rollup_for("n1", "api", 1_000, 300);
        let current = set(vec![(key, rollup)]);

        // Steady state with the old leader.
        let mut old_leader = RollupMerge::default();
        old_leader.become_responsible(start, true);
        let first = sender.next_sync(&current);
        assert_eq!(
            old_leader.apply("mgr-b", &first, start),
            SyncVerdict::Applied
        );
        sender.acked();
        let delta = sender.next_sync(&current);
        assert_eq!(
            old_leader.apply("mgr-b", &delta, start),
            SyncVerdict::Applied
        );
        sender.acked();

        // The lane moves. The new leader holds nothing for mgr-b, so the
        // steady-state delta is refused...
        let took_over = start + Duration::from_secs(30);
        let mut new_leader = RollupMerge::default();
        new_leader.become_responsible(took_over, true);
        assert!(new_leader.totals(&target("api"), took_over).is_none());
        assert_eq!(
            new_leader.data_state(false, took_over),
            DataState::Resyncing
        );
        let refused = sender.next_sync(&current);
        assert!(!refused.full);
        assert_eq!(
            new_leader.apply("mgr-b", &refused, took_over),
            SyncVerdict::Resync
        );

        // ...and the sender answers with everything at once.
        sender.reset();
        let full = sender.next_sync(&current);
        assert!(full.full);
        assert_eq!(
            new_leader.apply("mgr-b", &full, took_over),
            SyncVerdict::Applied
        );
        sender.acked();
        let totals = new_leader
            .totals(&target("api"), took_over)
            .expect("resynced");
        assert_eq!(totals.cpu_used_millicores, 300);
        assert_eq!(
            new_leader.data_state(true, took_over + RESYNC_WINDOW),
            DataState::Current
        );
        assert_eq!(
            new_leader.data_state(false, took_over + RESYNC_WINDOW),
            DataState::Missing
        );

        // A full sync replaces the sender's whole contribution.
        let (other_key, other) = rollup_for("n2", "api", 2_000, 100);
        let replace = RollupSync {
            epoch: 9,
            seq: 99,
            base_seq: 0,
            full: true,
            rollups: vec![other.to_proto(&other_key)],
        };
        new_leader.apply("mgr-b", &replace, took_over);
        let totals = new_leader
            .totals(&target("api"), took_over)
            .expect("totals");
        assert_eq!(totals.reporting_nodes, 1);
        assert_eq!(totals.cpu_used_millicores, 100);
    }

    /// A different epoch from the same sender (it restarted) voids the
    /// leader's state for it.
    #[test]
    fn a_new_epoch_needs_a_full_sync() {
        let now = Instant::now();
        let mut merge = RollupMerge::default();
        merge.become_responsible(now, true);
        merge.apply("mgr-b", &sync(1, 1, 0, true, vec![]), now);
        assert_eq!(
            merge.apply("mgr-b", &sync(2, 2, 1, false, vec![]), now),
            SyncVerdict::Resync
        );
        assert_eq!(
            merge.apply("mgr-b", &sync(1, 2, 1, false, vec![]), now),
            SyncVerdict::Applied
        );
    }

    /// Edge windows become rates on read, summed over calling nodes.
    #[test]
    fn totals_turn_edge_windows_into_rates() {
        let now = Instant::now();
        let mut merge = RollupMerge::default();
        merge.become_responsible(now, true);
        let edge = |node: &str, requests: u64, errors: u64, max: u64| {
            let rollup = Rollup {
                sampled_at_unix_ms: 1,
                edge: EdgeTotals {
                    requests,
                    errors,
                    latency_ms_sum: requests * 10,
                    latency_ms_max: max,
                    window_ms: 10_000,
                },
                ..Default::default()
            };
            (RollupKey::new(node, target("api")), rollup)
        };
        merge.apply_local(
            "leader",
            &set(vec![edge("n1", 100, 0, 40), edge("n2", 300, 100, 90)]),
            now,
        );
        let totals = merge.totals(&target("api"), now).expect("totals");
        assert!((totals.requests_per_second - 40.0).abs() < 1e-9);
        assert!((totals.errors_per_second - 10.0).abs() < 1e-9);
        assert!((totals.error_ratio() - 0.2).abs() < 1e-9);
        assert!((totals.latency_ms_mean() - 10.0).abs() < 1e-9);
        assert_eq!(totals.edge_latency_ms_max, 90);
        assert_eq!(totals.calling_nodes, 2);
        assert_eq!(totals.replicas, 0);
    }

    /// History holds one point per interval for an hour, per target, and is
    /// capped in targets.
    #[test]
    fn history_keeps_an_hour_of_points() {
        let start = Instant::now();
        let mut merge = RollupMerge::default();
        merge.become_responsible(start, true);
        let mut now = start;
        for tick in 0..(HISTORY_POINTS as i64 + 20) {
            let at_ms = tick * 10_000;
            merge.apply_local(
                "leader",
                &set(vec![rollup_for("n1", "api", at_ms + 1, 300)]),
                now,
            );
            merge.record_history(now, 1_000_000_000 + at_ms);
            now += ROLLUP_INTERVAL;
        }
        let points = merge.history(&target("api"), 0);
        assert_eq!(points.len(), HISTORY_POINTS);
        assert!(points
            .windows(2)
            .all(|pair| pair[0].at_unix_ms < pair[1].at_unix_ms));
        assert!((points[0].cpu_utilization - 300.0 / 500.0).abs() < 1e-6);
        let last_minute = merge.history(
            &target("api"),
            points[HISTORY_POINTS - 1].at_unix_ms - 59_000,
        );
        assert_eq!(last_minute.len(), 6);

        // Once the task stops reporting, its history ages out after an hour.
        merge.apply_local("leader", &HashMap::new(), now);
        merge.record_history(now, 1_000_000_000 + 10_000 * (HISTORY_POINTS as i64 * 3));
        assert_eq!(merge.history_targets(), 0);
    }

    #[test]
    fn history_is_capped_in_targets() {
        let now = Instant::now();
        let mut merge = RollupMerge::default();
        merge.become_responsible(now, true);
        let current: HashMap<RollupKey, Rollup> = (0..(HISTORY_MAX_TARGETS + 5))
            .map(|n| rollup_for("n1", &format!("task-{n}"), 1, 10))
            .collect();
        merge.apply_local("leader", &current, now);
        merge.record_history(now, 1);
        assert_eq!(merge.history_targets(), HISTORY_MAX_TARGETS);
        assert_eq!(
            merge.len(),
            HISTORY_MAX_TARGETS + 5,
            "inputs are not capped"
        );
    }

    /// Relinquishing the lane clears the view; taking it starts the resync
    /// window afresh.
    #[test]
    fn leadership_changes_reset_the_merged_view() {
        let now = Instant::now();
        let mut merge = RollupMerge::default();
        merge.become_responsible(now, true);
        merge.apply_local("leader", &set(vec![rollup_for("n1", "api", 1, 10)]), now);
        assert_eq!(merge.len(), 1);
        merge.relinquish();
        assert!(merge.is_empty());
        assert!(merge.responsible_since().is_none());
        let later = now + Duration::from_secs(60);
        merge.become_responsible(later, true);
        assert_eq!(merge.responsible_since(), Some(later));
        assert_eq!(merge.data_state(true, later), DataState::Resyncing);
    }
}
