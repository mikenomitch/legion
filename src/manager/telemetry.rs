//! Legion telemetry: a dependency-free metrics registry plus the HTTP
//! listeners that expose it. Hosts BOTH process registries — the manager's
//! (`legion_*`) and the worker's (`legion_worker_*`, served by its own
//! opt-in listener) — and the structured snapshot API the OTLP metrics
//! push (`otel.rs`) exports from.
//!
//! The registry is hand-rolled on atomics (no prometheus crate) and renders
//! the Prometheus text exposition format (version 0.0.4). Metric families are
//! created once via [`metrics`] (a `OnceLock` global) and are cheap to update
//! from hot paths: counters and gauges are single atomic ops, histograms are
//! one atomic per observation plus a CAS loop for the running sum.
//!
//! Label support is deliberately simple: families declare their label names
//! up front and series are materialized per distinct label-value tuple.
//! Cardinality must stay low — a family caps its series at
//! [`MAX_SERIES_PER_FAMILY`] and folds any overflow into a single series with
//! every label set to `"other"`, so a misbehaving caller (or a client sending
//! garbage URI paths at the gRPC listener) cannot balloon memory.
//!
//! The listener (same hyper http/1.1 pattern as `exec_ws`) serves:
//! - `GET /metrics`  — Prometheus text format (`text/plain; version=0.0.4`)
//! - `GET /healthz`  — liveness: `200 ok` whenever the listener is serving
//! - `GET /readyz`   — readiness: `200` only when a store read probe
//!   (`SELECT 1`) succeeds and, under raft persistence, a leader is known;
//!   otherwise `503` with the reason in the body.
//!
//! By convention the listener binds `server.port + 2` (the exec WebSocket
//! holds `+ 1`); `telemetry.port` overrides it. See docs/metrics.md.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use time::OffsetDateTime;

use crate::prelude::*;
use crate::storage::store::Store;

/// Hard cap on distinct label-value tuples per family. Beyond this, updates
/// fold into a single overflow series (all labels `"other"`).
pub const MAX_SERIES_PER_FAMILY: usize = 256;

/// Bucket bounds (seconds) shared by the duration histograms.
const DURATION_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Bucket bounds (seconds) for suspend and resume, which run from tens of
/// milliseconds (a disk suspend, a small restore) to a minute (a multi-GiB
/// memory capture on slow disk).
const CHECKPOINT_DURATION_BUCKETS: &[f64] = &[
    0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Bucket bounds (seconds) for publishing a checkpoint to a content store
/// (Phase 7b): an upload of a multi-GiB memory file over a WAN runs to
/// minutes, a re-publish of bytes the store already holds to milliseconds.
const CHECKPOINT_PUBLISH_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];

/// Bucket bounds (bytes) for checkpoint sizes: 1 MiB to 64 GiB.
const CHECKPOINT_BYTES_BUCKETS: &[f64] = &[
    1_048_576.0,
    16_777_216.0,
    134_217_728.0,
    268_435_456.0,
    536_870_912.0,
    1_073_741_824.0,
    2_147_483_648.0,
    4_294_967_296.0,
    8_589_934_592.0,
    17_179_869_184.0,
    68_719_476_736.0,
];

/// Bucket bounds (seconds) for runtime-scheduling delay, which is
/// microseconds on a healthy runtime — [`DURATION_BUCKETS`] would put every
/// healthy sample in one bucket and lose the signal.
const SCHEDULER_DELAY_BUCKETS: &[f64] = &[
    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0,
];

/// Bucket bounds (seconds) for how long an eval waited blocked on capacity
/// (sandbox-scaling plan, Phase 3): from a node joining a moment later to an
/// hour-long wait for a pool to scale out.
const BLOCKED_WAIT_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0,
];

/// Bucket bounds for candidate-count histograms (scheduling indexes).
const COUNT_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 4096.0, 16384.0,
];

// ---------------------------------------------------------------------------
// Primitive metric cells
// ---------------------------------------------------------------------------

/// Monotonic counter.
#[derive(Default)]
pub struct CounterCell(AtomicU64);

impl CounterCell {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Set-to-current-value gauge.
#[derive(Default)]
pub struct GaugeCell(AtomicI64);

impl GaugeCell {
    pub fn set(&self, v: i64) {
        self.0.store(v, Ordering::Relaxed);
    }

    /// Relative update (`+1` / `-1`), for gauges maintained by paired
    /// increment/decrement rather than by a periodic refresh — in-flight
    /// requests, queue depths. One atomic, safe from any thread.
    pub fn add(&self, delta: i64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Fixed-bucket histogram: per-bucket atomic counts plus running sum/count.
pub struct HistogramCell {
    bounds: &'static [f64],
    /// One slot per bound plus a final `+Inf` slot.
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    /// f64 bits of the running sum, updated via CAS.
    sum_bits: AtomicU64,
}

impl HistogramCell {
    fn new(bounds: &'static [f64]) -> Self {
        let mut buckets = Vec::with_capacity(bounds.len() + 1);
        for _ in 0..=bounds.len() {
            buckets.push(AtomicU64::new(0));
        }
        HistogramCell {
            bounds,
            buckets,
            count: AtomicU64::new(0),
            sum_bits: AtomicU64::new(0f64.to_bits()),
        }
    }

    pub fn observe(&self, v: f64) {
        let idx = self
            .bounds
            .iter()
            .position(|bound| v <= *bound)
            .unwrap_or(self.bounds.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        let mut cur = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let new = (f64::from_bits(cur) + v).to_bits();
            match self.sum_bits.compare_exchange_weak(
                cur,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    fn sum(&self) -> f64 {
        f64::from_bits(self.sum_bits.load(Ordering::Relaxed))
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Families (labeled and unlabeled) + rendering
// ---------------------------------------------------------------------------

struct FamilyMeta {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
}

/// A labeled set of series sharing one family name/help/type.
struct Series<T> {
    map: RwLock<Vec<(Vec<String>, Arc<T>)>>,
}

impl<T> Series<T> {
    fn new() -> Self {
        Series {
            map: RwLock::new(Vec::new()),
        }
    }

    /// Fetch (or create) the cell for a label-value tuple. Past
    /// [`MAX_SERIES_PER_FAMILY`], folds into the all-`"other"` series.
    fn cell(&self, labels: &[&str], make: impl Fn() -> T) -> Arc<T> {
        if let Some(found) = self
            .map
            .read()
            .ok()
            .and_then(|map| Self::find(&map, labels))
        {
            return found;
        }
        let Ok(mut map) = self.map.write() else {
            // Lock poisoned (a panic mid-update elsewhere): metrics are
            // best-effort, hand back a detached cell rather than propagate.
            return Arc::new(make());
        };
        if let Some(found) = Self::find(&map, labels) {
            return found;
        }
        let key: Vec<String> = if map.len() >= MAX_SERIES_PER_FAMILY {
            let overflow: Vec<String> = labels.iter().map(|_| "other".to_string()).collect();
            if let Some(found) = Self::find(&map, &overflow) {
                return found;
            }
            overflow
        } else {
            labels.iter().map(|s| s.to_string()).collect()
        };
        let cell = Arc::new(make());
        map.push((key, Arc::clone(&cell)));
        cell
    }

    fn find(map: &[(Vec<String>, Arc<T>)], labels: &[impl AsRef<str>]) -> Option<Arc<T>> {
        map.iter()
            .find(|(key, _)| {
                key.len() == labels.len()
                    && key
                        .iter()
                        .zip(labels.iter())
                        .all(|(a, b)| a.as_str() == b.as_ref())
            })
            .map(|(_, cell)| Arc::clone(cell))
    }

    /// Sorted snapshot for deterministic rendering.
    fn snapshot(&self) -> Vec<(Vec<String>, Arc<T>)> {
        let mut snap: Vec<(Vec<String>, Arc<T>)> = self
            .map
            .read()
            .map(|map| {
                map.iter()
                    .map(|(k, v)| (k.clone(), Arc::clone(v)))
                    .collect()
            })
            .unwrap_or_default();
        snap.sort_by(|a, b| a.0.cmp(&b.0));
        snap
    }
}

/// Escapes a label value per the exposition format.
fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn render_label_block(names: &[&str], values: &[String], extra: Option<(&str, &str)>) -> String {
    let mut parts: Vec<String> = names
        .iter()
        .zip(values.iter())
        .map(|(name, value)| format!("{name}=\"{}\"", escape_label_value(value)))
        .collect();
    if let Some((name, value)) = extra {
        parts.push(format!("{name}=\"{}\"", escape_label_value(value)));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", parts.join(","))
    }
}

fn render_header(out: &mut String, meta: &FamilyMeta, kind: &str) {
    out.push_str(&format!("# HELP {} {}\n", meta.name, meta.help));
    out.push_str(&format!("# TYPE {} {}\n", meta.name, kind));
}

trait RenderMetric: Send + Sync {
    fn render_into(&self, out: &mut String);
    /// Structured snapshot of the family, for exporters that need data
    /// rather than exposition text (the OTLP metrics push in `otel.rs`).
    fn snapshot_family(&self) -> FamilySnapshot;
}

/// What kind of series a [`FamilySnapshot`] carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotKind {
    Counter,
    Gauge,
    Histogram,
}

/// One family's structured snapshot: identity plus every live series.
pub struct FamilySnapshot {
    pub name: &'static str,
    pub help: &'static str,
    pub kind: SnapshotKind,
    pub label_names: &'static [&'static str],
    pub samples: Vec<SampleSnapshot>,
}

/// One series' snapshot within a family.
pub struct SampleSnapshot {
    /// One value per `label_names` entry; empty for unlabeled families.
    pub label_values: Vec<String>,
    pub value: SampleValue,
}

pub enum SampleValue {
    Counter(u64),
    Gauge(i64),
    /// A gauge whose value is genuinely fractional — a ratio, not a count.
    /// Kept separate rather than scaling into the integer gauge so the metric
    /// an operator alerts on reads in the units the docs state
    /// (`legion_heartbeat_freshness_ratio` is 0.22, never 220).
    GaugeF64(f64),
    Histogram {
        bounds: &'static [f64],
        /// Per-bucket (NON-cumulative) counts, one per bound plus a final
        /// overflow slot — the OTLP `bucket_counts` shape.
        bucket_counts: Vec<u64>,
        count: u64,
        sum: f64,
    },
}

impl HistogramCell {
    fn snapshot_value(&self) -> SampleValue {
        SampleValue::Histogram {
            bounds: self.bounds,
            bucket_counts: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            count: self.count(),
            sum: self.sum(),
        }
    }
}

/// Counter family with labels.
pub struct CounterVec {
    meta: FamilyMeta,
    series: Series<CounterCell>,
}

impl CounterVec {
    /// Increment the series identified by `labels` (one value per declared
    /// label name, in order).
    pub fn inc(&self, labels: &[&str]) {
        self.inc_by(labels, 1);
    }

    /// Add `count` to the series identified by `labels`. For batched events
    /// (a plan evicting several allocations of one job) where one increment
    /// per item would be pure overhead.
    pub fn inc_by(&self, labels: &[&str], count: u64) {
        debug_assert_eq!(labels.len(), self.meta.labels.len());
        let cell = self.series.cell(labels, CounterCell::default);
        for _ in 0..count {
            cell.inc();
        }
    }

    /// Current value of a series (0 when it does not exist). Test/inspection
    /// helper — not needed on hot paths.
    pub fn get(&self, labels: &[&str]) -> u64 {
        Series::find(&self.series.snapshot(), labels)
            .map(|c| c.get())
            .unwrap_or(0)
    }
}

impl RenderMetric for CounterVec {
    fn render_into(&self, out: &mut String) {
        render_header(out, &self.meta, "counter");
        for (values, cell) in self.series.snapshot() {
            let labels = render_label_block(self.meta.labels, &values, None);
            out.push_str(&format!("{}{} {}\n", self.meta.name, labels, cell.get()));
        }
    }

    fn snapshot_family(&self) -> FamilySnapshot {
        FamilySnapshot {
            name: self.meta.name,
            help: self.meta.help,
            kind: SnapshotKind::Counter,
            label_names: self.meta.labels,
            samples: self
                .series
                .snapshot()
                .into_iter()
                .map(|(label_values, cell)| SampleSnapshot {
                    label_values,
                    value: SampleValue::Counter(cell.get()),
                })
                .collect(),
        }
    }
}

/// Gauge family with labels.
pub struct GaugeVec {
    meta: FamilyMeta,
    series: Series<GaugeCell>,
}

impl GaugeVec {
    pub fn set(&self, labels: &[&str], v: i64) {
        debug_assert_eq!(labels.len(), self.meta.labels.len());
        self.series.cell(labels, GaugeCell::default).set(v);
    }

    /// Relative update of one series. Callers must pair their `+1` with a
    /// `-1` on every exit path (including cancellation — see
    /// [`InFlightGuard`]), or the series drifts upward forever.
    pub fn add(&self, labels: &[&str], delta: i64) {
        debug_assert_eq!(labels.len(), self.meta.labels.len());
        self.series.cell(labels, GaugeCell::default).add(delta);
    }

    /// Current value of a series (0 when it does not exist). Test/inspection
    /// helper — not needed on hot paths.
    pub fn get(&self, labels: &[&str]) -> i64 {
        Series::find(&self.series.snapshot(), labels)
            .map(|c| c.get())
            .unwrap_or(0)
    }

    /// Replace the whole family: series present in `entries` get their new
    /// value, previously-seen series absent from `entries` drop to zero (so a
    /// status that empties out reads 0 instead of a stale count).
    pub fn set_all(&self, entries: &[(Vec<String>, i64)]) {
        for (values, cell) in self.series.snapshot() {
            if !entries.iter().any(|(k, _)| *k == values) {
                cell.set(0);
            }
        }
        for (values, v) in entries {
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            self.set(&refs, *v);
        }
    }
}

impl RenderMetric for GaugeVec {
    fn render_into(&self, out: &mut String) {
        render_header(out, &self.meta, "gauge");
        for (values, cell) in self.series.snapshot() {
            let labels = render_label_block(self.meta.labels, &values, None);
            out.push_str(&format!("{}{} {}\n", self.meta.name, labels, cell.get()));
        }
    }

    fn snapshot_family(&self) -> FamilySnapshot {
        FamilySnapshot {
            name: self.meta.name,
            help: self.meta.help,
            kind: SnapshotKind::Gauge,
            label_names: self.meta.labels,
            samples: self
                .series
                .snapshot()
                .into_iter()
                .map(|(label_values, cell)| SampleSnapshot {
                    label_values,
                    value: SampleValue::Gauge(cell.get()),
                })
                .collect(),
        }
    }
}

/// Histogram family with labels.
pub struct HistogramVec {
    meta: FamilyMeta,
    bounds: &'static [f64],
    series: Series<HistogramCell>,
}

impl HistogramVec {
    pub fn observe(&self, labels: &[&str], v: f64) {
        debug_assert_eq!(labels.len(), self.meta.labels.len());
        self.series
            .cell(labels, || HistogramCell::new(self.bounds))
            .observe(v);
    }
}

fn render_histogram_series(
    out: &mut String,
    meta: &FamilyMeta,
    values: &[String],
    cell: &HistogramCell,
) {
    let mut cumulative = 0u64;
    for (idx, bound) in cell.bounds.iter().enumerate() {
        cumulative += cell.buckets[idx].load(Ordering::Relaxed);
        let labels = render_label_block(meta.labels, values, Some(("le", &format!("{bound}"))));
        out.push_str(&format!("{}_bucket{} {}\n", meta.name, labels, cumulative));
    }
    cumulative += cell.buckets[cell.bounds.len()].load(Ordering::Relaxed);
    let labels = render_label_block(meta.labels, values, Some(("le", "+Inf")));
    out.push_str(&format!("{}_bucket{} {}\n", meta.name, labels, cumulative));
    let plain = render_label_block(meta.labels, values, None);
    out.push_str(&format!("{}_sum{} {}\n", meta.name, plain, cell.sum()));
    out.push_str(&format!("{}_count{} {}\n", meta.name, plain, cell.count()));
}

impl RenderMetric for HistogramVec {
    fn render_into(&self, out: &mut String) {
        render_header(out, &self.meta, "histogram");
        for (values, cell) in self.series.snapshot() {
            render_histogram_series(out, &self.meta, &values, &cell);
        }
    }

    fn snapshot_family(&self) -> FamilySnapshot {
        FamilySnapshot {
            name: self.meta.name,
            help: self.meta.help,
            kind: SnapshotKind::Histogram,
            label_names: self.meta.labels,
            samples: self
                .series
                .snapshot()
                .into_iter()
                .map(|(label_values, cell)| SampleSnapshot {
                    label_values,
                    value: cell.snapshot_value(),
                })
                .collect(),
        }
    }
}

/// Unlabeled counter.
pub struct Counter {
    meta: FamilyMeta,
    cell: CounterCell,
}

impl Counter {
    pub fn inc(&self) {
        self.cell.inc();
    }

    pub fn add(&self, n: u64) {
        self.cell.add(n);
    }

    pub fn get(&self) -> u64 {
        self.cell.get()
    }
}

impl RenderMetric for Counter {
    fn render_into(&self, out: &mut String) {
        render_header(out, &self.meta, "counter");
        out.push_str(&format!("{} {}\n", self.meta.name, self.cell.get()));
    }

    fn snapshot_family(&self) -> FamilySnapshot {
        FamilySnapshot {
            name: self.meta.name,
            help: self.meta.help,
            kind: SnapshotKind::Counter,
            label_names: self.meta.labels,
            samples: vec![SampleSnapshot {
                label_values: Vec::new(),
                value: SampleValue::Counter(self.cell.get()),
            }],
        }
    }
}

/// Unlabeled gauge.
pub struct Gauge {
    meta: FamilyMeta,
    cell: GaugeCell,
}

impl Gauge {
    pub fn set(&self, v: i64) {
        self.cell.set(v);
    }

    pub fn get(&self) -> i64 {
        self.cell.get()
    }
}

impl RenderMetric for Gauge {
    fn render_into(&self, out: &mut String) {
        render_header(out, &self.meta, "gauge");
        out.push_str(&format!("{} {}\n", self.meta.name, self.cell.get()));
    }

    fn snapshot_family(&self) -> FamilySnapshot {
        FamilySnapshot {
            name: self.meta.name,
            help: self.meta.help,
            kind: SnapshotKind::Gauge,
            label_names: self.meta.labels,
            samples: vec![SampleSnapshot {
                label_values: Vec::new(),
                value: SampleValue::Gauge(self.cell.get()),
            }],
        }
    }
}

/// Unlabeled gauge with a fractional value (a ratio, a rate).
pub struct FloatGauge {
    meta: FamilyMeta,
    bits: AtomicU64,
}

impl FloatGauge {
    pub fn set(&self, v: f64) {
        self.bits.store(v.to_bits(), Ordering::Relaxed);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
}

impl RenderMetric for FloatGauge {
    fn render_into(&self, out: &mut String) {
        render_header(out, &self.meta, "gauge");
        out.push_str(&format!("{} {}\n", self.meta.name, self.get()));
    }

    fn snapshot_family(&self) -> FamilySnapshot {
        FamilySnapshot {
            name: self.meta.name,
            help: self.meta.help,
            kind: SnapshotKind::Gauge,
            label_names: self.meta.labels,
            samples: vec![SampleSnapshot {
                label_values: Vec::new(),
                value: SampleValue::GaugeF64(self.get()),
            }],
        }
    }
}

/// Unlabeled histogram.
pub struct Histogram {
    meta: FamilyMeta,
    cell: HistogramCell,
}

impl Histogram {
    pub fn observe(&self, v: f64) {
        self.cell.observe(v);
    }
}

impl RenderMetric for Histogram {
    fn render_into(&self, out: &mut String) {
        render_header(out, &self.meta, "histogram");
        render_histogram_series(out, &self.meta, &[], &self.cell);
    }

    fn snapshot_family(&self) -> FamilySnapshot {
        FamilySnapshot {
            name: self.meta.name,
            help: self.meta.help,
            kind: SnapshotKind::Histogram,
            label_names: self.meta.labels,
            samples: vec![SampleSnapshot {
                label_values: Vec::new(),
                value: self.cell.snapshot_value(),
            }],
        }
    }
}

// ---------------------------------------------------------------------------
// Global registry
// ---------------------------------------------------------------------------

/// Every metric family the manager exports. Obtained via [`metrics`]; fields
/// are updated directly at the instrumented call sites.
///
/// Naming note: the `legion_raft_*` prefix belongs to the raft engine and
/// no other subsystem may claim it. The quorum-shape gauges
/// (`voters`/`quorum`, plan §7-C3) and the engine series (term, indexes,
/// apply lag, leadership, capture/propose latency, op outcomes) all live
/// here; see docs/metrics.md for the full list.
pub struct Metrics {
    /// `legion_grpc_requests_total{method,code}`
    pub grpc_requests_total: Arc<CounterVec>,
    /// `legion_grpc_request_duration_seconds{method}`
    pub grpc_request_duration_seconds: Arc<HistogramVec>,
    /// `legion_grpc_in_flight{method}`
    pub grpc_in_flight: Arc<GaugeVec>,
    /// `legion_evals_processed_total{kind,status}`
    pub evals_processed_total: Arc<CounterVec>,
    /// `legion_eval_queue_depth`
    pub eval_queue_depth: Arc<Gauge>,
    /// `legion_plan_apply_total{outcome}`
    pub plan_apply_total: Arc<CounterVec>,
    /// `legion_eval_claim_total{outcome}`
    pub eval_claim_total: Arc<CounterVec>,
    /// `legion_eval_handle_seconds{kind}`
    pub eval_handle_seconds: Arc<HistogramVec>,
    /// `legion_dispatch_send_seconds`
    pub dispatch_send_seconds: Arc<Histogram>,
    /// `legion_scheduler_plan_candidate_rank`
    pub scheduler_plan_candidate_rank: Arc<Histogram>,
    /// `legion_scheduler_plan_bind_pass{pass}`
    pub scheduler_plan_bind_pass: Arc<CounterVec>,
    /// `legion_eval_conflict_requeues_total`
    pub eval_conflict_requeues_total: Arc<Counter>,
    /// `legion_evals_blocked{key_kind}`
    pub evals_blocked: Arc<GaugeVec>,
    /// `legion_evals_unblocked_total{event}`
    pub evals_unblocked_total: Arc<CounterVec>,
    /// `legion_blocked_wait_seconds`
    pub blocked_wait_seconds: Arc<Histogram>,
    /// `legion_scheduler_concurrency_limit`
    pub scheduler_concurrency_limit: Arc<Gauge>,
    /// `legion_scheduler_concurrency_in_flight`
    pub scheduler_concurrency_in_flight: Arc<Gauge>,
    /// `legion_scheduler_concurrency_state{state}`
    pub scheduler_concurrency_state: Arc<GaugeVec>,
    /// `legion_scheduler_concurrency_conflict_rate`
    pub scheduler_concurrency_conflict_rate: Arc<FloatGauge>,
    /// `legion_scheduler_concurrency_fallback_rate`
    pub scheduler_concurrency_fallback_rate: Arc<FloatGauge>,
    /// `legion_scheduler_concurrency_goodput`
    pub scheduler_concurrency_goodput: Arc<FloatGauge>,
    /// `legion_scheduler_concurrency_adjustments_total{direction}`
    pub scheduler_concurrency_adjustments_total: Arc<CounterVec>,
    /// `legion_scheduler_admission_skips_total`
    pub scheduler_admission_skips_total: Arc<Counter>,
    /// `legion_preemptions_total{pool,band}` — victims actually evicted.
    pub preemptions_total: Arc<CounterVec>,
    /// `legion_preemption_plans_total{outcome}` — preemption passes that
    /// reached a decision: `placed`, `observe`, or `no_victims`.
    pub preemption_plans_total: Arc<CounterVec>,
    /// `legion_heartbeats_total`
    pub heartbeats_total: Arc<Counter>,
    /// `legion_nodes{status}`
    pub nodes: Arc<GaugeVec>,
    /// `legion_read_model_nodes{set}`
    pub read_model_nodes: Arc<GaugeVec>,
    /// `legion_node_memory_physical_bytes{node}` (docs/metrics.md, "Node usage")
    pub node_memory_physical_bytes: Arc<GaugeVec>,
    /// `legion_node_memory_available_bytes{node}` (docs/metrics.md, "Node usage")
    pub node_memory_available_bytes: Arc<GaugeVec>,
    /// `legion_node_memory_used_bytes{node}` (docs/metrics.md, "Node usage")
    pub node_memory_used_bytes: Arc<GaugeVec>,
    /// `legion_node_cpu_used_millicores{node}` (docs/metrics.md, "Node usage")
    pub node_cpu_used_millicores: Arc<GaugeVec>,
    /// `legion_node_pressure_actions_total{node,step}` (docs/metrics.md,
    /// "Node usage")
    pub node_pressure_actions_total: Arc<CounterVec>,
    /// `legion_instance_suspend_seconds{kind}` (docs/metrics.md, "Suspend and
    /// resume")
    pub instance_suspend_seconds: Arc<HistogramVec>,
    /// `legion_instance_suspends_total{reason}` (docs/metrics.md, "Suspend
    /// and resume")
    pub instance_suspends_total: Arc<CounterVec>,
    /// `legion_idle_sweep_suspends_total{outcome}` (docs/metrics.md, "Suspend
    /// and resume")
    pub idle_sweep_suspends_total: Arc<CounterVec>,
    /// `legion_make_room_suspends_total{outcome}` (docs/metrics.md, "Suspend
    /// and resume")
    pub make_room_suspends_total: Arc<CounterVec>,
    /// `legion_instance_resume_seconds{kind}` (docs/metrics.md, "Suspend and
    /// resume")
    pub instance_resume_seconds: Arc<HistogramVec>,
    /// `legion_checkpoint_bytes{kind}` (docs/metrics.md, "Suspend and resume")
    pub checkpoint_bytes: Arc<HistogramVec>,
    /// `legion_checkpoint_publish_bytes{kind}` (docs/metrics.md, "Suspend and
    /// resume"; Phase 7b)
    pub checkpoint_publish_bytes: Arc<HistogramVec>,
    /// `legion_checkpoint_publish_seconds{kind}` (docs/metrics.md, "Suspend
    /// and resume"; Phase 7b)
    pub checkpoint_publish_seconds: Arc<HistogramVec>,
    /// `legion_checkpoint_publish_total{outcome}` (docs/metrics.md, "Suspend
    /// and resume"; Phase 7b)
    pub checkpoint_publish_total: Arc<CounterVec>,
    /// `legion_instance_archived_total{mode}` (docs/metrics.md, "Suspend
    /// and resume"; sandbox-scaling Phase 5)
    pub instance_archived_total: Arc<CounterVec>,
    /// `legion_session_starts_total{outcome}` (docs/metrics.md, "Sessions";
    /// sandbox-scaling Phase 6)
    pub session_starts_total: Arc<CounterVec>,
    /// `legion_session_stops_total{reason}` (docs/metrics.md, "Sessions")
    pub session_stops_total: Arc<CounterVec>,
    /// `legion_session_promotions_total` (docs/metrics.md, "Sessions")
    pub session_promotions_total: Arc<Counter>,
    /// `legion_allocations`
    pub allocations: Arc<Gauge>,
    /// `legion_store_write_lock_wait_seconds`
    pub store_write_lock_wait_seconds: Arc<Histogram>,
    /// `legion_store_read_lock_wait_seconds`
    pub store_read_lock_wait_seconds: Arc<Histogram>,
    /// `legion_store_coalesce_batch_size{path}`
    pub store_coalesce_batch_size: Arc<HistogramVec>,
    /// `legion_store_coalesce_wait_seconds{path,role}`
    pub store_coalesce_wait_seconds: Arc<HistogramVec>,
    /// `legion_store_coalesce_commit_seconds{path}`
    pub store_coalesce_commit_seconds: Arc<HistogramVec>,
    /// `legion_store_coalesce_wait_rounds_total{path}`
    pub store_coalesce_wait_rounds_total: Arc<CounterVec>,
    /// `legion_runtime_spawn_delay_seconds`
    pub runtime_spawn_delay_seconds: Arc<Histogram>,
    /// `legion_runtime_worker_threads`
    pub runtime_worker_threads: Arc<Gauge>,
    /// `legion_keyring_unlocked`
    pub keyring_unlocked: Arc<Gauge>,
    /// `legion_keyring_keys{state}`
    pub keyring_keys: Arc<GaugeVec>,
    /// `legion_keyring_active_key_age_seconds`
    pub keyring_active_key_age_seconds: Arc<Gauge>,
    /// `legion_kv_rows{encryption}`
    pub kv_rows: Arc<GaugeVec>,
    /// `legion_kv_decrypt_failures_total{reason}`
    pub kv_decrypt_failures_total: Arc<CounterVec>,
    /// `legion_keyring_provider_calls_total{op,outcome}`
    pub keyring_provider_calls_total: Arc<CounterVec>,
    /// `legion_raft_voters{lane}`
    pub raft_voters: Arc<GaugeVec>,
    /// `legion_raft_quorum{lane}`
    pub raft_quorum: Arc<GaugeVec>,
    /// `legion_raft_term{lane}`
    pub raft_term: Arc<GaugeVec>,
    /// `legion_raft_commit_index{lane}`
    pub raft_commit_index: Arc<GaugeVec>,
    /// `legion_raft_applied_index{lane}`
    pub raft_applied_index: Arc<GaugeVec>,
    /// `legion_raft_apply_lag{lane}`
    pub raft_apply_lag: Arc<GaugeVec>,
    /// `legion_raft_is_leader{lane}`
    pub raft_is_leader: Arc<GaugeVec>,
    /// `legion_raft_capture_seconds{lane}`
    pub raft_capture_seconds: Arc<HistogramVec>,
    /// `legion_raft_propose_apply_seconds{lane}`
    pub raft_propose_apply_seconds: Arc<HistogramVec>,
    /// `legion_raft_ops_total{lane,op,outcome}`
    pub raft_ops_total: Arc<CounterVec>,
    /// `legion_raft_pipeline_wait_seconds{lane}`
    pub raft_pipeline_wait_seconds: Arc<HistogramVec>,
    /// `legion_raft_pipeline_depth{lane}`
    pub raft_pipeline_depth: Arc<GaugeVec>,
    /// `legion_scheduler_index_queries_total{index,outcome}`
    pub scheduler_index_queries_total: Arc<CounterVec>,
    /// `legion_scheduler_locality_placements_total{outcome}`
    pub scheduler_locality_placements_total: Arc<CounterVec>,
    /// `legion_scheduler_index_candidates{index}`
    pub scheduler_index_candidates: Arc<HistogramVec>,
    /// `legion_scheduler_index_shadow_total{index,result}`
    pub scheduler_index_shadow_total: Arc<CounterVec>,
    /// `legion_scheduler_index_maintenance_seconds`
    pub scheduler_index_maintenance_seconds: Arc<Histogram>,
    /// `legion_scheduler_plan_phase_seconds{phase}`
    pub scheduler_plan_phase_seconds: Arc<HistogramVec>,
    /// `legion_scheduler_lazy_sample_fallback_total`
    pub scheduler_lazy_sample_fallback_total: Arc<Counter>,
    /// `legion_scheduler_node_list_rebuilds_total`
    pub scheduler_node_list_rebuilds_total: Arc<Counter>,
    /// `legion_eval_phase_seconds{kind,phase}`
    pub eval_phase_seconds: Arc<HistogramVec>,
    /// `legion_eval_queue_round_seconds{op}`
    pub eval_queue_round_seconds: Arc<HistogramVec>,
    /// `legion_store_read_model_lock_wait_seconds{mode}`
    pub store_read_model_lock_wait_seconds: Arc<HistogramVec>,
    /// `legion_heartbeat_freshness_ratio` — **the number to alert on.**
    pub heartbeat_freshness_ratio: Arc<FloatGauge>,
    /// `legion_heartbeat_served_interval_seconds`
    pub heartbeat_served_interval_seconds: Arc<Gauge>,
    /// `legion_heartbeat_lost_window_seconds`
    pub heartbeat_lost_window_seconds: Arc<Gauge>,
    /// `legion_heartbeat_sustainable_nodes`
    pub heartbeat_sustainable_nodes: Arc<Gauge>,
    /// `legion_nodes_marked_lost_total`
    pub nodes_marked_lost_total: Arc<Counter>,
    /// `legion_nodes_recovered_total{path}`
    pub nodes_recovered_total: Arc<CounterVec>,
    /// `legion_read_model_drift_repaired_total{source}`
    pub read_model_drift_repaired_total: Arc<CounterVec>,
    /// `legion_read_model_external_writes_total`
    pub read_model_external_writes_total: Arc<Counter>,
    /// `legion_liveness_tracked_nodes`
    pub liveness_tracked_nodes: Arc<Gauge>,
    /// `legion_liveness_responsible_seconds`
    pub liveness_responsible_seconds: Arc<Gauge>,
    /// `legion_liveness_digest_rpcs_total{outcome}`
    pub liveness_digest_rpcs_total: Arc<CounterVec>,
    /// `legion_liveness_digest_nodes`
    pub liveness_digest_nodes: Arc<Histogram>,
    /// `legion_liveness_peer_stale{peer}`
    pub liveness_peer_stale: Arc<GaugeVec>,
    /// `legion_liveness_loss_suppressed_total{reason}`
    pub liveness_loss_suppressed_total: Arc<CounterVec>,
    /// `legion_liveness_checkpoint_seconds`
    pub liveness_checkpoint_seconds: Arc<Histogram>,
    /// Render order for the exposition endpoint.
    families: Vec<Arc<dyn RenderMetric>>,
}

impl Metrics {
    fn new() -> Self {
        let grpc_requests_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_grpc_requests_total",
                help: "gRPC requests handled by the manager, by method and result code.",
                labels: &["method", "code"],
            },
            series: Series::new(),
        });
        let grpc_request_duration_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_grpc_request_duration_seconds",
                help: "gRPC request duration in seconds (until response headers), by method.",
                labels: &["method"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });
        let grpc_in_flight = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_grpc_in_flight",
                help: "gRPC requests currently executing in the manager, by method. Read against legion_runtime_worker_threads: a value that sits at or above the worker-thread count means handlers are blocking the runtime, not that the cluster is busy.",
                labels: &["method"],
            },
            series: Series::new(),
        });
        let evals_processed_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_evals_processed_total",
                help: "Scheduler evals completed, by eval kind and outcome status (ok|error).",
                labels: &["kind", "status"],
            },
            series: Series::new(),
        });
        let eval_queue_depth = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_eval_queue_depth",
                help: "Pending (unclaimed) evals in the scheduler queue, including delayed ones.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let plan_apply_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_plan_apply_total",
                help: "Scheduler plan submissions applied to the store, by outcome (applied|rejected).",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let eval_claim_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_eval_claim_total",
                help: "Eval claim attempts by an eval worker, by outcome (claimed|empty). An empty attempt produces an empty changeset, which skips the raft entry — its cost is the capture (a savepoint on the write connection) plus a wasted wake, so a high `empty` share means workers are being woken for evals that do not exist and the write path is contending for it.",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let eval_handle_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_eval_handle_seconds",
                help: "Wall time to handle one claimed eval end to end: planning, applying, dispatching the allocations, and the post-apply bookkeeping. Read it AGAINST legion_scheduler_plan_phase_seconds{phase=\"total\"} — the difference is everything an eval does outside planning, which is otherwise invisible. Throughput is roughly evalThreads divided by this.",
                labels: &["kind"],
            },
            series: Series::new(),
            bounds: DURATION_BUCKETS,
        });
        let dispatch_send_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_dispatch_send_seconds",
                help: "One successful PostWork push to a worker, from send to ack. Dispatch is detached from the eval loop, so this latency no longer appears in legion_eval_handle_seconds - this histogram is where it went. Rising values mean slow or overloaded workers, not a slow scheduler.",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        });
        let scheduler_plan_candidate_rank = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_scheduler_plan_candidate_rank",
                help: "Candidate rank each applied placement bound at (0 = the planner's first choice; higher ranks mean a conflict was absorbed by falling back).",
                labels: &[],
            },
            cell: HistogramCell::new(COUNT_BUCKETS),
        });
        let scheduler_plan_bind_pass = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_scheduler_plan_bind_pass",
                help: "Placements bound by the apply walk, by pass: `clean` (the candidate's node epoch was unchanged since the plan snapshot) or `revalidated` (its epoch had moved, and the in-transaction recheck admitted it anyway).",
                labels: &["pass"],
            },
            series: Series::new(),
        });
        let eval_conflict_requeues_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_eval_conflict_requeues_total",
                help: "Evals released back to the pending queue after exhausting plan-conflict retries, instead of failing terminally.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });
        let evals_blocked = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_evals_blocked",
                help: "Evals waiting blocked on capacity, by the kind of key they wait under: `class` (a pool and pinned attributes), `node` (a resume pinned to the node holding its checkpoint), `escaped` (no schedulable node at all). Set by the scheduler-lane leader's wake pass; zero elsewhere.",
                labels: &["key_kind"],
            },
            series: Series::new(),
        });
        let evals_unblocked_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_evals_unblocked_total",
                help: "Blocked evals returned to the pending queue, by what woke them: `register` (a node joined), `node` (a node's pool, availability, or attributes changed), `ready` (a node became ready), `freed` (allocations left a node), `missed` (capacity appeared while the eval was running, so it never blocked), `startup` (a manager starting as the scheduler's writer gives every blocked eval one more look).",
                labels: &["event"],
            },
            series: Series::new(),
        });
        let blocked_wait_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_blocked_wait_seconds",
                help: "How long a blocked eval waited before capacity woke it, from the entry that blocked it to the one that returned it to the queue.",
                labels: &[],
            },
            cell: HistogramCell::new(BLOCKED_WAIT_BUCKETS),
        });
        // Adaptive scheduler concurrency
        // (docs/adaptive-scheduler-concurrency.md). All of these are
        // scheduler-lane-leader-only: a follower does no planning, so its
        // window is empty and its state would be noise.
        let scheduler_concurrency_limit = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_scheduler_concurrency_limit",
                help: "Planners the adaptive concurrency controller currently admits. In `observe` mode this is the SHADOW limit — what the controller would enforce — which is what makes observe mode useful on its own.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let scheduler_concurrency_in_flight = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_scheduler_concurrency_in_flight",
                help: "Admission permits currently held by eval workers running a planning eval. 0 when the controller is off, which does not consult the gate at all.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let scheduler_concurrency_state = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_scheduler_concurrency_state",
                help: "1 on the controller's current state, 0 on the others: `steady` (running at the ceiling), `throttled` (backed off), `probing` (calm, climbing back), `saturated` (backing off did not improve goodput, so the conflicts are capacity-driven and the actuator has stopped being applied).",
                labels: &["state"],
            },
            series: Series::new(),
        });
        let scheduler_concurrency_conflict_rate = Arc::new(FloatGauge {
            meta: FamilyMeta {
                name: "legion_scheduler_concurrency_conflict_rate",
                help: "Smoothed plan rejection rate the controller is deciding on: rejected / (applied + fallback + rejected). Fallbacks count as progress, not as conflict.",
                labels: &[],
            },
            bits: AtomicU64::new(0),
        });
        let scheduler_concurrency_fallback_rate = Arc::new(FloatGauge {
            meta: FamilyMeta {
                name: "legion_scheduler_concurrency_fallback_rate",
                help: "Smoothed fraction of plans that bound but slid down a candidate list — races ABSORBED by late binding. Reported for diagnosis; never fed into the controller's decision.",
                labels: &[],
            },
            bits: AtomicU64::new(0),
        });
        let scheduler_concurrency_goodput = Arc::new(FloatGauge {
            meta: FamilyMeta {
                name: "legion_scheduler_concurrency_goodput",
                help: "Smoothed plans that actually placed, per second. The objective the controller is guarding; conflict rate is only its leading indicator.",
                labels: &[],
            },
            bits: AtomicU64::new(0),
        });
        let scheduler_concurrency_adjustments_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_scheduler_concurrency_adjustments_total",
                help: "Limit changes the controller made, by direction: `increase`, `decrease`, or `revert` (a decrease undone because it did not improve goodput). A sustained two-value sawtooth here is hunting, which is a failure and not a cosmetic issue.",
                labels: &["direction"],
            },
            series: Series::new(),
        });
        let scheduler_admission_skips_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_scheduler_admission_skips_total",
                help: "Times a throttled eval worker claimed a non-planning eval (today: `job_stop`) instead of the queue head. This counts the one cross-class priority inversion the admission gate introduces.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });
        let preemptions_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_preemptions_total",
                help: "Allocations evicted by scheduler preemption, by the victim's node pool and priority band.",
                labels: &["pool", "band"],
            },
            series: Series::new(),
        });
        let preemption_plans_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_preemption_plans_total",
                help: "Preemption passes that reached a decision, by outcome (placed|observe|no_victims).",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let heartbeats_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_heartbeats_total",
                help: "Worker heartbeat RPCs accepted by the manager.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });
        let nodes = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_nodes",
                help: "Registered worker nodes, by status.",
                labels: &["status"],
            },
            series: Series::new(),
        });
        let node_memory_physical_bytes = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_node_memory_physical_bytes",
                help: "Physical memory (MemTotal) on a node, from its latest usage report. Reported by workers with resourceEnforcement.enabled; 0 once a node stops reporting.",
                labels: &["node"],
            },
            series: Series::new(),
        });
        let node_memory_available_bytes = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_node_memory_available_bytes",
                help: "MemAvailable on a node, from its latest usage report: the kernel's estimate of memory new work can use without swapping. The number to alert on for a node running short.",
                labels: &["node"],
            },
            series: Series::new(),
        });
        let node_memory_used_bytes = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_node_memory_used_bytes",
                help: "Sum of memory.current over a node's governed allocations, from its latest usage report. Compare with reservations from `legion node usage`.",
                labels: &["node"],
            },
            series: Series::new(),
        });
        let node_pressure_actions_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_node_pressure_actions_total",
                help: "Actions a node's memory pressure response reported, by step (reclaim|suspend|evict-bursting|evict-reserved). Counted when the heartbeat carrying them reaches this manager.",
                labels: &["node", "step"],
            },
            series: Series::new(),
        });
        let instance_suspend_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_instance_suspend_seconds",
                help: "How long a successful suspend's capture took on the node, from the checkpoint request to the stopped task, by checkpoint kind (memory|disk). Observed when the worker's report reaches this manager.",
                labels: &["kind"],
            },
            series: Series::new(),
            bounds: CHECKPOINT_DURATION_BUCKETS,
        });
        let instance_suspends_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_instance_suspends_total",
                help: "Suspends applied, by reason: manual (SuspendInstance), idle (the task's suspend.idle policy), or pressure (a node's suspend pressure step). Counted when the node's capture report reaches this manager.",
                labels: &["reason"],
            },
            series: Series::new(),
        });
        let idle_sweep_suspends_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_idle_sweep_suspends_total",
                help: "Suspends the idle sweep tried to start, by outcome: started, or skipped (the instance changed, an operation was in flight, or its node cannot checkpoint).",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let make_room_suspends_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_make_room_suspends_total",
                help: "Suspends the manager started to make room for a resume waiting on its checkpoint's node, by outcome: started, failed (the suspend could not start), or no_room (suspending every eligible instance would not have made room, so none was suspended).",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let instance_resume_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_instance_resume_seconds",
                help: "How long a successful resume's restore took on the node, from starting the allocation to the running task, by checkpoint kind (memory|disk). Placement time is not included.",
                labels: &["kind"],
            },
            series: Series::new(),
            bounds: CHECKPOINT_DURATION_BUCKETS,
        });
        let checkpoint_bytes = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_checkpoint_bytes",
                help: "Size of each captured checkpoint (snapshots and suspends), by kind (memory|disk). A disk checkpoint taken by suspend retains its volume in place and reports 0.",
                labels: &["kind"],
            },
            series: Series::new(),
            bounds: CHECKPOINT_BYTES_BUCKETS,
        });
        let checkpoint_publish_bytes = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_checkpoint_publish_bytes",
                help: "Bytes a node uploaded to its contentStore to publish one checkpoint (durability = \"store\"), by kind (memory|disk). 0 when the store already held every part.",
                labels: &["kind"],
            },
            series: Series::new(),
            bounds: CHECKPOINT_BYTES_BUCKETS,
        });
        let checkpoint_publish_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_checkpoint_publish_seconds",
                help: "How long a node took to publish one checkpoint to its contentStore, by kind (memory|disk). Observed when the successful publish report reaches this manager.",
                labels: &["kind"],
            },
            series: Series::new(),
            bounds: CHECKPOINT_PUBLISH_BUCKETS,
        });
        let checkpoint_publish_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_checkpoint_publish_total",
                help: "Checkpoint publishes reported by nodes, by outcome (published|retrying|failed). A failed publish leaves the snapshot local to its node; retrying means the manager asks the node again after a backoff.",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let instance_archived_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_instance_archived_total",
                help: "Suspended instances archived as their checkpoint was published (suspend.archive), by mode (slim|manifestOnly). An archived instance keeps no snapshot row in the store; manifestOnly keeps no row at all.",
                labels: &["mode"],
            },
            series: Series::new(),
        });
        let session_starts_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_session_starts_total",
                help: "Session creates this manager's session router answered, by outcome (started|deduplicated|no_envelope|no_room|refused). None writes the replicated store.",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let session_stops_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_session_stops_total",
                help: "Sessions this manager ended on their nodes, by reason (stop|destroy|duplicate).",
                labels: &["reason"],
            },
            series: Series::new(),
        });
        let session_promotions_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_session_promotions_total",
                help: "Running sessions promoted to keyed instances (PromoteInstance), one scheduler-lane entry each.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });
        let node_cpu_used_millicores = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_node_cpu_used_millicores",
                help: "CPU used by a node's governed allocations over the last report interval, in thousandths of a core.",
                labels: &["node"],
            },
            series: Series::new(),
        });
        let read_model_nodes = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_read_model_nodes",
                help: "Nodes in the in-memory scheduling read model, by set: 'all' is every cached node, 'available' is the planner's candidate universe. A persistent gap against legion_nodes means the scheduler is planning over a truncated fleet.",
                labels: &["set"],
            },
            series: Series::new(),
        });
        let allocations = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_allocations",
                help: "Live allocations tracked by the manager.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let store_write_lock_wait_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_store_write_lock_wait_seconds",
                help: "Time spent waiting for the store's SQLite write-connection lock.",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        });
        let store_read_lock_wait_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_store_read_lock_wait_seconds",
                help: "Time spent waiting for one of the store's SQLite read connections. Reads that found a free connection immediately are recorded as 0, so this is the queueing cost of the read pool and not the cost of the query. A rising tail here shows up inside whichever scheduler plan phase happened to be running — historically `task_fetch` — so read it alongside legion_scheduler_plan_phase_seconds.",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        });
        // Coalescer (group commit) instrumentation. Three questions, three
        // families: does batching engage at all (batch_size), is a submitter
        // waiting or committing (wait_seconds by role vs commit_seconds), and
        // how many extra commits it sat through before its own (wait_rounds).
        let store_coalesce_batch_size = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_store_coalesce_batch_size",
                help: "Requests drained into one group commit, by coalesced path (heartbeat|plan_apply). The le=\"1\" bucket is how often batching did not engage at all; a distribution pinned at 1 under load means requests are not arriving while a commit is in flight.",
                labels: &["path"],
            },
            bounds: COUNT_BUCKETS,
            series: Series::new(),
        });
        let store_coalesce_wait_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_store_coalesce_wait_seconds",
                help: "Time a submitter waited before being served, by path and role. role=\"leader\": queued until it won the commit lock (pure contention, commit not yet started). role=\"follower\": queued until another submitter's batch returned its result (contention plus that batch's commit).",
                labels: &["path", "role"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });
        let store_coalesce_commit_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_store_coalesce_commit_seconds",
                help: "Wall-clock of one group commit (the batch runner itself), by path. Compare against legion_store_coalesce_wait_seconds to split 'the commit is slow' from 'submitters are queued behind it'.",
                labels: &["path"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });
        let store_coalesce_wait_rounds_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_store_coalesce_wait_rounds_total",
                help: "Times a waiting submitter was woken by a finished commit that was neither its own result nor its turn to lead, i.e. it sat through an extra commit. Divided by the wait_seconds count this is the mean number of EXTRA commits a request waits out; 0 means every submitter was served by the first commit it queued behind.",
                labels: &["path"],
            },
            series: Series::new(),
        });
        let runtime_spawn_delay_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_runtime_spawn_delay_seconds",
                help: "Probe: wall-clock from tokio::spawn to that task's first poll, sampled once a second. Microseconds on a healthy runtime; milliseconds mean every worker thread is busy or blocked, which shows up as latency on EVERY handler including trivial ones.",
                labels: &[],
            },
            cell: HistogramCell::new(SCHEDULER_DELAY_BUCKETS),
        });
        let runtime_worker_threads = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_runtime_worker_threads",
                help: "Tokio worker threads in this manager's runtime — the ceiling on concurrent blocking handlers, and the denominator for legion_grpc_in_flight.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let raft_voters = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_voters",
                help: "Voting members of each raft lane's group, as known by this manager.",
                labels: &["lane"],
            },
            series: Series::new(),
        });
        // KV encryption at rest (docs/kv-encryption.md).
        let keyring_unlocked = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_keyring_unlocked",
                help: "1 when this manager can encrypt and decrypt every KV value (every data key unwrapped), 0 when the keyring is locked, disabled, or not initialized yet. Alert on 0 while encryption is enabled.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let keyring_keys = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_keyring_keys",
                help: "Keys in the KV keyring by state (active|retired data keys, checkpoint for the key that seals published checkpoints). A retired key for longer than a rewrap means values still sit under it.",
                labels: &["state"],
            },
            series: Series::new(),
        });
        let keyring_active_key_age_seconds = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_keyring_active_key_age_seconds",
                help: "Age of the active KV data key. Rotates automatically at encryption.dataKeyRotationAge (default 30 days).",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let kv_rows = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_kv_rows",
                help: "KV entries by storage form (encrypted|plaintext). Plaintext rows after enabling encryption are pre-upgrade values a rewrap has not converted yet.",
                labels: &["encryption"],
            },
            series: Series::new(),
        });
        let kv_decrypt_failures_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_kv_decrypt_failures_total",
                help: "KV reads that could not be decrypted, by reason: locked (the data key is not unwrapped on this manager — root key missing) or corrupt (the stored ciphertext failed authentication).",
                labels: &["reason"],
            },
            series: Series::new(),
        });
        let keyring_provider_calls_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_keyring_provider_calls_total",
                help: "Calls to the root key provider (describe|wrap|unwrap) by outcome (ok|error). Never on a KV read or write path.",
                labels: &["op", "outcome"],
            },
            series: Series::new(),
        });
        let raft_quorum = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_quorum",
                help: "Majority (quorum) size required to commit on each raft lane (voters/2 + 1).",
                labels: &["lane"],
            },
            series: Series::new(),
        });
        let raft_term = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_term",
                help: "Current raft term of each lane, as this manager sees it. A climbing term means elections (leadership churn).",
                labels: &["lane"],
            },
            series: Series::new(),
        });
        let raft_commit_index = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_commit_index",
                help: "Highest quorum-committed log index of each lane, as this manager sees it.",
                labels: &["lane"],
            },
            series: Series::new(),
        });
        let raft_applied_index = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_applied_index",
                help: "Highest log index this manager's state machine has applied, per lane.",
                labels: &["lane"],
            },
            series: Series::new(),
        });
        let raft_apply_lag = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_apply_lag",
                help: "Committed-but-not-yet-applied log entries per lane (commit_index - applied_index). Sustained growth means the state machine cannot keep up.",
                labels: &["lane"],
            },
            series: Series::new(),
        });
        let raft_is_leader = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_is_leader",
                help: "1 when this manager leads the lane, else 0. Sum across managers should be 1 per lane; flapping means leadership churn.",
                labels: &["lane"],
            },
            series: Series::new(),
        });
        let raft_capture_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_raft_capture_seconds",
                help: "Local capture phase of a replicated op (run the mutation, collect its changeset, roll back), per lane.",
                labels: &["lane"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });
        let raft_propose_apply_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_raft_propose_apply_seconds",
                help: "Propose -> quorum commit -> local apply round-trip of a replicated op, per lane. Compare against legion_raft_capture_seconds to attribute write latency to the local store vs the replication pipeline.",
                labels: &["lane"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });
        let raft_ops_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_raft_ops_total",
                help: "Replicated store ops by lane, op name, and outcome (applied|empty|not_leader|error). empty = the mutation touched no rows and skipped the raft entry. The op name is what makes a busy lane attributable: a lane's op count is its quorum-round count, so the top ops by rate are the ones setting its cost.",
                labels: &["lane", "op", "outcome"],
            },
            series: Series::new(),
        });
        let raft_pipeline_wait_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_raft_pipeline_wait_seconds",
                help: "Time a replicated op sat in a lane's pipeline queue before its (single, per-lane) pipeline thread began executing it. This is queueing, not replication: when it grows while legion_raft_propose_apply_seconds does not, the lane's serialized pipeline is the bottleneck.",
                labels: &["lane"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });
        let raft_pipeline_depth = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_pipeline_depth",
                help: "Replicated ops queued to a lane's pipeline thread and not yet started. Sustained depth > 1 means writes on that lane are serialized behind each other.",
                labels: &["lane"],
            },
            series: Series::new(),
        });

        let scheduler_index_queries_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_scheduler_index_queries_total",
                help: "Scheduling-index candidate queries, by index and outcome (used|shadow|fell_back|refilled|unusable).",
                labels: &["index", "outcome"],
            },
            series: Series::new(),
        });
        let scheduler_locality_placements_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_scheduler_locality_placements_total",
                help: "Placements whose task had a content set, by how much of it the chosen node already held (warm = all, partial = some, cold = none). Hit ratio = warm / total.",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let scheduler_index_candidates = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_scheduler_index_candidates",
                help: "Candidates handed to placement per index-served task.",
                labels: &["index"],
            },
            bounds: COUNT_BUCKETS,
            series: Series::new(),
        });
        let scheduler_index_shadow_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_scheduler_index_shadow_total",
                help: "Shadow-mode comparisons between index candidates and the scan, by result (agree|disagree).",
                labels: &["index", "result"],
            },
            series: Series::new(),
        });
        let scheduler_index_maintenance_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_scheduler_index_maintenance_seconds",
                help: "Time spent re-projecting nodes into the scheduling indexes on read-model mutations.",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        });
        let scheduler_plan_phase_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_scheduler_plan_phase_seconds",
                help: "Per-phase wall-clock of scheduler plan_and_apply (job_lookup|available_nodes|task_fetch|capacity_snapshot|build_plan|version_sample|speculative_dispatch|apply|post_apply|total). Rejected plans are observed too, up to apply.",
                labels: &["phase"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });

        let scheduler_lazy_sample_fallback_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_scheduler_lazy_sample_fallback_total",
                help: "Plans that re-ran with the eager epoch sample because the read model changed mid-plan.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });
        let scheduler_node_list_rebuilds_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_scheduler_node_list_rebuilds_total",
                help: "Rebuilds of the scheduler's cached available-node set (the sorted list and its by-id lookup), one per plan that found the cache invalidated by a node change. Near zero in steady state; a rate tracking the plan rate means nodes are changing under every plan.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });
        let eval_phase_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_eval_phase_seconds",
                help: "Per-phase wall-clock of one placing eval's handler (queue_wait|scheduler_init|prepare|plan|conflict_backoff|dispatch|finish), by eval kind. The phases partition legion_eval_handle_seconds for the kinds that place work; `plan` covers every plan attempt, including rejected ones, and `conflict_backoff` the sleeps between them.",
                labels: &["kind", "phase"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });
        let eval_queue_round_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_eval_queue_round_seconds",
                help: "Wall time of one eval-queue round on the scheduler lane, by op (claim|settle). `claim` is a standalone claim; `settle` writes a batch's outcomes and claims the next batch in one replicated op. Both happen outside legion_eval_handle_seconds.",
                labels: &["op"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });
        let store_read_model_lock_wait_seconds = Arc::new(HistogramVec {
            meta: FamilyMeta {
                name: "legion_store_read_model_lock_wait_seconds",
                help: "Time spent acquiring the scheduler read model's lock, by mode (read|write). Planners and heartbeats read it; every allocation publish and node change writes it. A rising read tail means writers hold it long or often; a rising write tail means readers do.",
                labels: &["mode"],
            },
            bounds: DURATION_BUCKETS,
            series: Series::new(),
        });

        // Heartbeat cadence (docs/heartbeat-cadence-and-scale.md). All on the
        // status-lane leader; a manager that does not own liveness reports 0,
        // which is why the freshness ratio is derived from the store's view
        // rather than from a per-process RPC counter — under `lanes` topology
        // an RPC rate is only ever one manager's share.
        let heartbeat_freshness_ratio = Arc::new(FloatGauge {
            meta: FamilyMeta {
                name: "legion_heartbeat_freshness_ratio",
                help: "Fraction of active nodes that checked in within one heartbeat interval. 1.0 = the fleet is keeping up; false node loss begins at served_interval/lost_window (0.20 at the defaults). 0 on a manager that does not own liveness or a fleet too small to measure.",
                labels: &[],
            },
            bits: AtomicU64::new(0),
        });
        let heartbeat_served_interval_seconds = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_heartbeat_served_interval_seconds",
                help: "Heartbeat interval this manager is currently telling workers to use.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let heartbeat_lost_window_seconds = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_heartbeat_lost_window_seconds",
                help: "Lost-node window derived from the served interval (missedBeatsBeforeLost x interval).",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let heartbeat_sustainable_nodes = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_heartbeat_sustainable_nodes",
                help: "Nodes this control plane is observed to sustain at the current interval. A LOWER BOUND while the fleet is keeping up: an unsaturated control plane never reveals its ceiling.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let nodes_marked_lost_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_nodes_marked_lost_total",
                help: "Nodes marked lost after missed heartbeats. Distinguishes 'nodes went away' from legion_nodes{status=\"lost\"}'s 'nodes are away'.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });
        let nodes_recovered_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_nodes_recovered_total",
                help: "Lost nodes that came back, by path: 'heartbeat' (the node beat again), 'reregistration' (the worker re-registered), or 'reconcile' (an active, heartbeating node whose stored availability still said unavailable was returned to the scheduling pool by the liveness owner's reconcile pass). marked_lost minus recovered is the net loss, and a fleet that flaps shows here instead of showing nothing.",
                labels: &["path"],
            },
            series: Series::new(),
        });

        // Read-model coherence (docs/state-store.md).
        let read_model_drift_repaired_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_read_model_drift_repaired_total",
                help: "Nodes whose cached scheduling state disagreed with the state database and was reloaded, by source: 'scrub' (the timed sweep) or 'delta' (a committed change contradicted the cache). Should stay at zero; any increase is a bug worth reporting.",
                labels: &["source"],
            },
            series: Series::new(),
        });
        let read_model_external_writes_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_read_model_external_writes_total",
                help: "Times another process or connection committed to the state database behind the manager's back, forcing a full rebuild of the cached scheduling state.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });

        // Node liveness (docs/node-liveness.md).
        let liveness_tracked_nodes = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_liveness_tracked_nodes",
                help: "Nodes in this manager's in-memory liveness table. Should equal legion_nodes{status=\"active\"} on the status-lane leader; 0 elsewhere.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let liveness_responsible_seconds = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_liveness_responsible_seconds",
                help: "Seconds since this manager became responsible for liveness (process start OR status-lane leadership acquisition, whichever is later). Resets at every acquisition, which is what makes 'we just took over' visible.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let liveness_digest_rpcs_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_liveness_digest_rpcs_total",
                help: "Liveness digests sent to the status-lane leader, by outcome (ok|not_leader|error|unimplemented). 'unimplemented' means a peer that predates digests leads the lane and heartbeats have fallen back to replicated writes.",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let liveness_digest_nodes = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_liveness_digest_nodes",
                help: "Node ids carried by one liveness digest.",
                labels: &[],
            },
            cell: HistogramCell::new(COUNT_BUCKETS),
        });
        let liveness_peer_stale = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_liveness_peer_stale",
                help: "1 while a peer manager has missed several liveness digests. THE metric to alert on for a manager-to-manager problem: without it, one quiet manager looks like its share of the fleet dying.",
                labels: &["peer"],
            },
            series: Series::new(),
        });
        let liveness_loss_suppressed_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_liveness_loss_suppressed_total",
                help: "Would-be node losses the manager declined to act on, by reason (failover_grace|peer_digest_gap|mass_disruption). 'Nothing was marked lost' must never be indistinguishable from 'nothing was wrong'.",
                labels: &["reason"],
            },
            series: Series::new(),
        });
        let liveness_checkpoint_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_liveness_checkpoint_seconds",
                help: "Wall-clock of one replicated liveness checkpoint (the display-only batched write of observed heartbeat times).",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        });

        let families: Vec<Arc<dyn RenderMetric>> = vec![
            Arc::clone(&heartbeat_freshness_ratio) as Arc<dyn RenderMetric>,
            Arc::clone(&heartbeat_served_interval_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&heartbeat_lost_window_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&heartbeat_sustainable_nodes) as Arc<dyn RenderMetric>,
            Arc::clone(&nodes_marked_lost_total) as Arc<dyn RenderMetric>,
            Arc::clone(&nodes_recovered_total) as Arc<dyn RenderMetric>,
            Arc::clone(&read_model_drift_repaired_total) as Arc<dyn RenderMetric>,
            Arc::clone(&read_model_external_writes_total) as Arc<dyn RenderMetric>,
            Arc::clone(&liveness_tracked_nodes) as Arc<dyn RenderMetric>,
            Arc::clone(&liveness_responsible_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&liveness_digest_rpcs_total) as Arc<dyn RenderMetric>,
            Arc::clone(&liveness_digest_nodes) as Arc<dyn RenderMetric>,
            Arc::clone(&liveness_peer_stale) as Arc<dyn RenderMetric>,
            Arc::clone(&liveness_loss_suppressed_total) as Arc<dyn RenderMetric>,
            Arc::clone(&liveness_checkpoint_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&allocations) as Arc<dyn RenderMetric>,
            Arc::clone(&eval_queue_depth) as Arc<dyn RenderMetric>,
            Arc::clone(&evals_processed_total) as Arc<dyn RenderMetric>,
            Arc::clone(&grpc_in_flight) as Arc<dyn RenderMetric>,
            Arc::clone(&grpc_request_duration_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&grpc_requests_total) as Arc<dyn RenderMetric>,
            Arc::clone(&heartbeats_total) as Arc<dyn RenderMetric>,
            Arc::clone(&nodes) as Arc<dyn RenderMetric>,
            Arc::clone(&read_model_nodes) as Arc<dyn RenderMetric>,
            Arc::clone(&node_memory_physical_bytes) as Arc<dyn RenderMetric>,
            Arc::clone(&node_memory_available_bytes) as Arc<dyn RenderMetric>,
            Arc::clone(&node_memory_used_bytes) as Arc<dyn RenderMetric>,
            Arc::clone(&node_cpu_used_millicores) as Arc<dyn RenderMetric>,
            Arc::clone(&node_pressure_actions_total) as Arc<dyn RenderMetric>,
            Arc::clone(&instance_suspend_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&instance_suspends_total) as Arc<dyn RenderMetric>,
            Arc::clone(&idle_sweep_suspends_total) as Arc<dyn RenderMetric>,
            Arc::clone(&make_room_suspends_total) as Arc<dyn RenderMetric>,
            Arc::clone(&instance_resume_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&checkpoint_bytes) as Arc<dyn RenderMetric>,
            Arc::clone(&checkpoint_publish_bytes) as Arc<dyn RenderMetric>,
            Arc::clone(&checkpoint_publish_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&checkpoint_publish_total) as Arc<dyn RenderMetric>,
            Arc::clone(&instance_archived_total) as Arc<dyn RenderMetric>,
            Arc::clone(&session_starts_total) as Arc<dyn RenderMetric>,
            Arc::clone(&session_stops_total) as Arc<dyn RenderMetric>,
            Arc::clone(&session_promotions_total) as Arc<dyn RenderMetric>,
            Arc::clone(&eval_conflict_requeues_total) as Arc<dyn RenderMetric>,
            Arc::clone(&evals_blocked) as Arc<dyn RenderMetric>,
            Arc::clone(&evals_unblocked_total) as Arc<dyn RenderMetric>,
            Arc::clone(&blocked_wait_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&plan_apply_total) as Arc<dyn RenderMetric>,
            Arc::clone(&eval_claim_total) as Arc<dyn RenderMetric>,
            Arc::clone(&eval_handle_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&dispatch_send_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_concurrency_limit) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_concurrency_in_flight) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_concurrency_state) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_concurrency_conflict_rate) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_concurrency_fallback_rate) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_concurrency_goodput) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_concurrency_adjustments_total) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_admission_skips_total) as Arc<dyn RenderMetric>,
            Arc::clone(&preemption_plans_total) as Arc<dyn RenderMetric>,
            Arc::clone(&preemptions_total) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_applied_index) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_apply_lag) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_capture_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_commit_index) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_is_leader) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_ops_total) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_pipeline_depth) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_pipeline_wait_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_propose_apply_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_quorum) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_term) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_voters) as Arc<dyn RenderMetric>,
            Arc::clone(&keyring_unlocked) as Arc<dyn RenderMetric>,
            Arc::clone(&keyring_keys) as Arc<dyn RenderMetric>,
            Arc::clone(&keyring_active_key_age_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&kv_rows) as Arc<dyn RenderMetric>,
            Arc::clone(&kv_decrypt_failures_total) as Arc<dyn RenderMetric>,
            Arc::clone(&keyring_provider_calls_total) as Arc<dyn RenderMetric>,
            Arc::clone(&runtime_spawn_delay_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&runtime_worker_threads) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_index_candidates) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_index_maintenance_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_index_queries_total) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_locality_placements_total) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_index_shadow_total) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_plan_bind_pass) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_plan_candidate_rank) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_lazy_sample_fallback_total) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_node_list_rebuilds_total) as Arc<dyn RenderMetric>,
            Arc::clone(&eval_phase_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&eval_queue_round_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&store_read_model_lock_wait_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&scheduler_plan_phase_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&store_coalesce_batch_size) as Arc<dyn RenderMetric>,
            Arc::clone(&store_coalesce_commit_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&store_coalesce_wait_rounds_total) as Arc<dyn RenderMetric>,
            Arc::clone(&store_coalesce_wait_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&store_write_lock_wait_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&store_read_lock_wait_seconds) as Arc<dyn RenderMetric>,
        ];

        Metrics {
            grpc_requests_total,
            grpc_request_duration_seconds,
            grpc_in_flight,
            evals_processed_total,
            eval_queue_depth,
            plan_apply_total,
            eval_claim_total,
            eval_handle_seconds,
            dispatch_send_seconds,
            scheduler_plan_bind_pass,
            scheduler_plan_candidate_rank,
            eval_conflict_requeues_total,
            evals_blocked,
            evals_unblocked_total,
            blocked_wait_seconds,
            scheduler_concurrency_limit,
            scheduler_concurrency_in_flight,
            scheduler_concurrency_state,
            scheduler_concurrency_conflict_rate,
            scheduler_concurrency_fallback_rate,
            scheduler_concurrency_goodput,
            scheduler_concurrency_adjustments_total,
            scheduler_admission_skips_total,
            preemptions_total,
            preemption_plans_total,
            heartbeats_total,
            nodes,
            read_model_nodes,
            node_memory_physical_bytes,
            node_memory_available_bytes,
            node_memory_used_bytes,
            node_cpu_used_millicores,
            node_pressure_actions_total,
            instance_suspend_seconds,
            instance_suspends_total,
            idle_sweep_suspends_total,
            make_room_suspends_total,
            instance_resume_seconds,
            checkpoint_bytes,
            checkpoint_publish_bytes,
            checkpoint_publish_seconds,
            checkpoint_publish_total,
            instance_archived_total,
            session_starts_total,
            session_stops_total,
            session_promotions_total,
            allocations,
            store_write_lock_wait_seconds,
            store_read_lock_wait_seconds,
            store_coalesce_batch_size,
            store_coalesce_wait_seconds,
            store_coalesce_commit_seconds,
            store_coalesce_wait_rounds_total,
            runtime_spawn_delay_seconds,
            runtime_worker_threads,
            keyring_unlocked,
            keyring_keys,
            keyring_active_key_age_seconds,
            kv_rows,
            kv_decrypt_failures_total,
            keyring_provider_calls_total,
            raft_voters,
            raft_quorum,
            raft_term,
            raft_commit_index,
            raft_applied_index,
            raft_apply_lag,
            raft_is_leader,
            raft_capture_seconds,
            raft_propose_apply_seconds,
            raft_ops_total,
            raft_pipeline_wait_seconds,
            raft_pipeline_depth,
            scheduler_index_queries_total,
            scheduler_locality_placements_total,
            scheduler_index_candidates,
            scheduler_index_shadow_total,
            scheduler_index_maintenance_seconds,
            scheduler_plan_phase_seconds,
            scheduler_lazy_sample_fallback_total,
            scheduler_node_list_rebuilds_total,
            eval_phase_seconds,
            eval_queue_round_seconds,
            store_read_model_lock_wait_seconds,
            heartbeat_freshness_ratio,
            heartbeat_served_interval_seconds,
            heartbeat_lost_window_seconds,
            heartbeat_sustainable_nodes,
            nodes_marked_lost_total,
            nodes_recovered_total,
            read_model_drift_repaired_total,
            read_model_external_writes_total,
            liveness_tracked_nodes,
            liveness_responsible_seconds,
            liveness_digest_rpcs_total,
            liveness_digest_nodes,
            liveness_peer_stale,
            liveness_loss_suppressed_total,
            liveness_checkpoint_seconds,
            families,
        }
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// The process-global metric registry (created on first use).
pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
}

/// Renders every registered family in the Prometheus text exposition format.
pub fn render() -> String {
    let mut out = String::with_capacity(4096);
    for family in &metrics().families {
        family.render_into(&mut out);
    }
    out
}

/// Structured snapshots of every family in every registry this process has
/// actually initialized (manager and/or worker) — the OTLP metrics push
/// input. Registries are lazily created at their first instrumented call
/// site, so a worker-only process exports no manager families and vice
/// versa.
pub fn snapshot_initialized_families() -> Vec<FamilySnapshot> {
    let mut snapshots = Vec::new();
    if let Some(manager) = METRICS.get() {
        snapshots.extend(manager.families.iter().map(|f| f.snapshot_family()));
    }
    if let Some(worker) = WORKER_METRICS.get() {
        snapshots.extend(worker.families.iter().map(|f| f.snapshot_family()));
    }
    snapshots
}

// ---------------------------------------------------------------------------
// Worker registry
// ---------------------------------------------------------------------------

/// Metric families the WORKER process exports (`legion_worker_*`). Same
/// machinery as the manager registry, separate global: a combined
/// manager+worker process serves both registries from their own listeners,
/// and the OTLP push exports whichever registries the process initialized.
///
/// A simulated fleet (`legion start worker --simulate N`) reports through
/// this same registry rather than a parallel one — the process *is* a
/// worker, just carrying N nodes. Everything here is therefore per-process:
/// in a fleet the shared families aggregate over every simulated node, and
/// the `simulated_*` families below describe the fleet itself. Per-node
/// series are deliberately not exported; at 1,250 nodes per host that is a
/// cardinality bomb, and the manager already holds the per-node view.
pub struct WorkerMetrics {
    /// `legion_worker_heartbeats_total{outcome}`
    pub heartbeats_total: Arc<CounterVec>,
    /// `legion_worker_heartbeat_duration_seconds`
    pub heartbeat_duration_seconds: Arc<Histogram>,
    /// `legion_worker_allocations_running`
    pub allocations_running: Arc<Gauge>,
    /// `legion_worker_allocation_failures_reported_total`
    pub allocation_failures_reported_total: Arc<Counter>,
    /// `legion_worker_pressure_actions_total{step}` — actions this node's
    /// memory pressure response took (docs/resource-enforcement.md).
    pub pressure_actions_total: Arc<CounterVec>,
    /// `legion_provisional_starts_total{origin,gate}` — provisional
    /// allocations admitted on this node (docs/edge-boot.md).
    pub provisional_starts_total: Arc<CounterVec>,
    /// `legion_provisional_refused_total{reason}` — ratifications refused.
    pub provisional_refused_total: Arc<CounterVec>,
    /// `legion_provisional_killed_total{cause}` — provisional work killed
    /// (`refused`, `expired`, `failed`).
    pub provisional_killed_total: Arc<CounterVec>,
    /// `legion_provisional_ratify_seconds` — admission to verdict.
    pub provisional_ratify_seconds: Arc<Histogram>,
    /// `legion_provisional_active` — unratified claims held right now, so
    /// started-but-unconfirmed capacity is visible rather than invisible.
    pub provisional_active: Arc<Gauge>,
    /// `legion_worker_launch_refusals_total` — starts refused because every
    /// launch slot stayed busy for `edgeBoot.queueTimeout`.
    pub launch_pool_refusals_total: Arc<Counter>,
    /// `legion_worker_edge_session_starts_total` — sessions started on this
    /// node from a session ticket (`Worker.StartSession`).
    pub edge_session_starts_total: Arc<Counter>,
    /// `legion_session_checkpoints_skipped_total` — periodic session
    /// checkpoints (`suspend.checkpointEvery`) a node skipped because its
    /// backlog would not let it capture them within one interval.
    pub session_checkpoints_skipped_total: Arc<Counter>,
    /// `legion_session_evictions_total{reason}` — suspended sessions whose
    /// local checkpoint this node evicted, by reason (keep_for |
    /// disk_pressure | envelope_gone | other).
    pub session_evictions_total: Arc<CounterVec>,
    /// `legion_worker_simulated_nodes` — simulated nodes registered by this
    /// process (`--simulate N`); 0 in a real worker.
    pub simulated_nodes: Arc<Gauge>,
    /// `legion_worker_simulated_nodes_target` — the fleet size this process
    /// was asked for, so a stalled ramp is visible as a gap.
    pub simulated_nodes_target: Arc<Gauge>,
    /// `legion_worker_simulated_registrations_total{outcome}`
    pub simulated_registrations_total: Arc<CounterVec>,
    /// `legion_net_conns_total{direction,protocol,verdict}` — connections the
    /// node datapath decided (docs/network-handlers.md). Bounded labels only:
    /// attribution (job, labels, destination) goes on events and the access
    /// log, never here.
    pub net_conns_total: Arc<CounterVec>,
    /// `legion_net_denied_total{direction}` — connections and requests denied
    /// by policy.
    pub net_denied_total: Arc<CounterVec>,
    /// `legion_net_bytes_total{direction}` — bytes forwarded through the
    /// datapath (`egress` = task to destination, `ingress` = peer to task).
    pub net_bytes_total: Arc<CounterVec>,
    /// `legion_net_handler_total{handler,outcome}` — external handler calls
    /// (`authz`, `proxy`) by outcome (allow|deny|proxy|error).
    pub net_handler_total: Arc<CounterVec>,
    /// `legion_net_handler_seconds` — wall-clock of one `authz` webhook call.
    pub net_handler_seconds: Arc<Histogram>,
    families: Vec<Arc<dyn RenderMetric>>,
}

impl WorkerMetrics {
    fn new() -> Self {
        let heartbeats_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_worker_heartbeats_total",
                help: "Heartbeat round-trips attempted by this worker process, by outcome (ok|error|unauthenticated). Summed over every node in a simulated fleet.",
                labels: &["outcome"],
            },
            series: Series::new(),
        });
        let heartbeat_duration_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_worker_heartbeat_duration_seconds",
                help: "Wall-clock of one heartbeat round-trip (report + reconcile against the manager's reply).",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        });
        let allocations_running = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_worker_allocations_running",
                help: "Allocations this worker currently runs, as reported on its last heartbeat. Summed over every node in a simulated fleet (where they are acked, not run).",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let allocation_failures_reported_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_worker_allocation_failures_reported_total",
                help: "Allocation failures this worker has reported to the manager.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });

        let provisional_starts_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_provisional_starts_total",
                help: "Provisional allocations admitted on this node, by origin (manager|edge) and gate.",
                labels: &["origin", "gate"],
            },
            series: Series::new(),
        });
        let provisional_refused_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_provisional_refused_total",
                help: "Ratifications refused, by reason (instance_claimed|no_capacity|ineligible|expired|ticket_invalid|unknown_target|disabled|internal).",
                labels: &["reason"],
            },
            series: Series::new(),
        });
        let provisional_killed_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_provisional_killed_total",
                help: "Provisional allocations killed, by cause (refused|expired|failed).",
                labels: &["cause"],
            },
            series: Series::new(),
        });
        let provisional_ratify_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_provisional_ratify_seconds",
                help:
                    "Wall-clock from admitting a provisional allocation to receiving its verdict.",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        });
        let provisional_active = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_provisional_active",
                help: "Unratified provisional allocations this node is holding right now.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });

        let launch_pool_refusals_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_worker_launch_refusals_total",
                help: "Session and claim starts refused because every launch slot (edgeBoot.launchConcurrency) stayed busy for edgeBoot.queueTimeout. Refused starts are retryable on another node.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });

        let edge_session_starts_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_worker_edge_session_starts_total",
                help: "Sessions started on this node from a session ticket (Worker.StartSession), with no manager on the path.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });

        let session_checkpoints_skipped_total = Arc::new(Counter {
            meta: FamilyMeta {
                name: "legion_session_checkpoints_skipped_total",
                help: "Periodic session checkpoints (suspend.checkpointEvery) skipped because the node's checkpoint backlog would not let it capture them within one interval. A rising rate means checkpointEvery is too short for the node's session count and memory.",
                labels: &[],
            },
            cell: CounterCell::default(),
        });

        let session_evictions_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_session_evictions_total",
                help: "Suspended sessions whose local-only checkpoint this node evicted, by reason (keep_for, disk_pressure, envelope_gone, other). An evicted session answers `evicted` for another suspend.keepFor and cannot be resumed.",
                labels: &["reason"],
            },
            series: Series::new(),
        });

        // Simulated-fleet families. A `--simulate N` process is a worker
        // process that happens to carry N nodes, so it reports through this
        // same registry: the families above become fleet-wide aggregates and
        // these three say how big the fleet is and whether it got there. All
        // three stay at zero in a real worker.
        let simulated_nodes = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_worker_simulated_nodes",
                help: "Simulated nodes in this process that have completed registration (0 in a real worker).",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let simulated_nodes_target = Arc::new(Gauge {
            meta: FamilyMeta {
                name: "legion_worker_simulated_nodes_target",
                help: "Simulated nodes this process was asked to run (`--simulate N`); 0 in a real worker.",
                labels: &[],
            },
            cell: GaugeCell::default(),
        });
        let pressure_actions_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_worker_pressure_actions_total",
                help: "Actions this node's memory pressure response took, by step (reclaim|evict-bursting|evict-reserved).",
                labels: &["step"],
            },
            series: Series::new(),
        });
        let simulated_registrations_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_worker_simulated_registrations_total",
                help: "Simulated-node registration attempts, by outcome (ok|error). Counts re-registrations, so it outruns the node count over a long run.",
                labels: &["outcome"],
            },
            series: Series::new(),
        });

        let net_conns_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_net_conns_total",
                help: "Connections decided by the node datapath, by direction (egress|ingress), protocol (tcp|http|dns), and verdict (allow|deny|error).",
                labels: &["direction", "protocol", "verdict"],
            },
            series: Series::new(),
        });
        let net_denied_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_net_denied_total",
                help: "Connections and requests denied by network policy, by direction. Which rule, which destination, and for whom are on the net_denied event, not here.",
                labels: &["direction"],
            },
            series: Series::new(),
        });
        let net_bytes_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_net_bytes_total",
                help: "Bytes forwarded through the node datapath, by direction.",
                labels: &["direction"],
            },
            series: Series::new(),
        });
        let net_handler_total = Arc::new(CounterVec {
            meta: FamilyMeta {
                name: "legion_net_handler_total",
                help: "External network handler calls (authz webhooks, proxy hand-offs) by handler and outcome (allow|deny|proxy|error).",
                labels: &["handler", "outcome"],
            },
            series: Series::new(),
        });
        let net_handler_seconds = Arc::new(Histogram {
            meta: FamilyMeta {
                name: "legion_net_handler_seconds",
                help: "Wall-clock of one authz webhook call, cache misses only.",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        });

        let families: Vec<Arc<dyn RenderMetric>> = vec![
            Arc::clone(&allocation_failures_reported_total) as Arc<dyn RenderMetric>,
            Arc::clone(&allocations_running) as Arc<dyn RenderMetric>,
            Arc::clone(&net_bytes_total) as Arc<dyn RenderMetric>,
            Arc::clone(&net_conns_total) as Arc<dyn RenderMetric>,
            Arc::clone(&net_denied_total) as Arc<dyn RenderMetric>,
            Arc::clone(&net_handler_total) as Arc<dyn RenderMetric>,
            Arc::clone(&net_handler_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&heartbeat_duration_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&heartbeats_total) as Arc<dyn RenderMetric>,
            Arc::clone(&launch_pool_refusals_total) as Arc<dyn RenderMetric>,
            Arc::clone(&edge_session_starts_total) as Arc<dyn RenderMetric>,
            Arc::clone(&session_checkpoints_skipped_total) as Arc<dyn RenderMetric>,
            Arc::clone(&session_evictions_total) as Arc<dyn RenderMetric>,
            Arc::clone(&provisional_active) as Arc<dyn RenderMetric>,
            Arc::clone(&provisional_killed_total) as Arc<dyn RenderMetric>,
            Arc::clone(&provisional_ratify_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&provisional_refused_total) as Arc<dyn RenderMetric>,
            Arc::clone(&provisional_starts_total) as Arc<dyn RenderMetric>,
            Arc::clone(&pressure_actions_total) as Arc<dyn RenderMetric>,
            Arc::clone(&simulated_nodes) as Arc<dyn RenderMetric>,
            Arc::clone(&simulated_nodes_target) as Arc<dyn RenderMetric>,
            Arc::clone(&simulated_registrations_total) as Arc<dyn RenderMetric>,
        ];

        WorkerMetrics {
            heartbeats_total,
            heartbeat_duration_seconds,
            allocations_running,
            allocation_failures_reported_total,
            pressure_actions_total,
            provisional_starts_total,
            provisional_refused_total,
            provisional_killed_total,
            provisional_ratify_seconds,
            provisional_active,
            launch_pool_refusals_total,
            edge_session_starts_total,
            session_checkpoints_skipped_total,
            session_evictions_total,
            simulated_nodes,
            simulated_nodes_target,
            simulated_registrations_total,
            net_conns_total,
            net_denied_total,
            net_bytes_total,
            net_handler_total,
            net_handler_seconds,
            families,
        }
    }
}

static WORKER_METRICS: OnceLock<WorkerMetrics> = OnceLock::new();

/// The process-global WORKER metric registry (created on first use).
pub fn worker_metrics() -> &'static WorkerMetrics {
    WORKER_METRICS.get_or_init(WorkerMetrics::new)
}

/// Renders the worker registry in the Prometheus text exposition format.
pub fn render_worker() -> String {
    let mut out = String::with_capacity(1024);
    for family in &worker_metrics().families {
        family.render_into(&mut out);
    }
    out
}

// ---------------------------------------------------------------------------
// Runtime saturation probe
// ---------------------------------------------------------------------------

/// How often [`start_runtime_probe`] samples spawn delay. One spawn a second
/// is free; the signal it carries (are there any idle worker threads?) is
/// not obtainable any other way once handlers block.
const RUNTIME_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Start the runtime saturation probe on the current tokio runtime.
///
/// Every second it spawns a task that does nothing but record how long it
/// waited to be polled (`legion_runtime_spawn_delay_seconds`), and it
/// publishes the runtime's worker-thread count once
/// (`legion_runtime_worker_threads`).
///
/// Why this exists: a handler that makes a *blocking* call from an async
/// context occupies a worker thread without yielding. With every worker
/// occupied, latency appears on unrelated handlers — even ones that do no
/// work at all — which reads like "everything is slow" while CPU sits idle
/// and no single operation is slow. Spawn delay names that condition
/// directly. Cheap enough to leave on permanently; see docs/metrics.md.
pub fn start_runtime_probe() {
    metrics()
        .runtime_worker_threads
        .set(tokio::runtime::Handle::current().metrics().num_workers() as i64);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RUNTIME_PROBE_INTERVAL);
        // Delay (not burst) after a missed tick: a saturated runtime must
        // not be handed a backlog of probe spawns to catch up on.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let spawned_at = Instant::now();
            let probe = tokio::spawn(async move {
                metrics()
                    .runtime_spawn_delay_seconds
                    .observe(spawned_at.elapsed().as_secs_f64());
            });
            if probe.await.is_err() {
                debug!("runtime saturation probe task failed; stopping probe");
                return;
            }
        }
    });
}

// ---------------------------------------------------------------------------
// gRPC instrumentation layer
// ---------------------------------------------------------------------------

/// Holds one `legion_grpc_in_flight` increment for the life of a request.
/// A `Drop` guard rather than a decrement at the end of the future because
/// a client that hangs up mid-call leaves the future *dropped*, not
/// completed — decrementing only on the happy path would let the gauge
/// drift upward forever and turn the saturation signal into noise.
struct InFlightGuard {
    method: String,
}

impl InFlightGuard {
    fn enter(method: &str) -> Self {
        metrics().grpc_in_flight.add(&[method], 1);
        InFlightGuard {
            method: method.to_string(),
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        metrics().grpc_in_flight.add(&[&self.method], -1);
    }
}

/// Tower layer recording `legion_grpc_requests_total`,
/// `legion_grpc_request_duration_seconds` and `legion_grpc_in_flight`
/// around the tonic service.
///
/// The `code` label is the `grpc-status` value when tonic surfaces it in the
/// response *headers* (trailers-only responses, i.e. every error path);
/// successful responses carry `grpc-status` in HTTP trailers, which this
/// layer does not buffer, so they are labeled by HTTP status class
/// (`http_2xx` etc.). Durations are measured to response headers, not to the
/// end of streaming bodies.
#[derive(Clone, Copy, Default)]
pub struct GrpcMetricsLayer;

impl<S> tower::Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService { inner }
    }
}

/// Service produced by [`GrpcMetricsLayer`].
#[derive(Clone)]
pub struct GrpcMetricsService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> tower::Service<Request<ReqBody>> for GrpcMetricsService<S>
where
    S: tower::Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let method = grpc_method_label(req.uri().path());
        let started = Instant::now();
        // Taken here, not inside the future: the request is in flight from
        // the moment the service accepts it, including any time it spends
        // waiting for a runtime worker to poll it — which is exactly the
        // saturation this gauge exists to show.
        let in_flight = InFlightGuard::enter(&method);
        let fut = self.inner.call(req);
        Box::pin(async move {
            let result = fut.await;
            drop(in_flight);
            let code = match &result {
                Ok(resp) => grpc_code_label(resp.headers(), resp.status()),
                Err(_) => "transport_error".to_string(),
            };
            let m = metrics();
            m.grpc_requests_total.inc(&[&method, &code]);
            m.grpc_request_duration_seconds
                .observe(&[&method], started.elapsed().as_secs_f64());
            result
        })
    }
}

/// Last two URI path segments (`/manager.Manager/StopJob` ->
/// `manager.Manager/StopJob`), which is service/method for gRPC.
fn grpc_method_label(path: &str) -> String {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match segments.as_slice() {
        [] => "/".to_string(),
        [one] => (*one).to_string(),
        [.., service, method] => format!("{service}/{method}"),
    }
}

fn grpc_code_label(headers: &http::HeaderMap, status: http::StatusCode) -> String {
    if let Some(code) = headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i32>().ok())
    {
        return grpc_code_name(code).to_string();
    }
    format!("http_{}xx", status.as_u16() / 100)
}

fn grpc_code_name(code: i32) -> &'static str {
    match code {
        0 => "ok",
        1 => "cancelled",
        2 => "unknown",
        3 => "invalid_argument",
        4 => "deadline_exceeded",
        5 => "not_found",
        6 => "already_exists",
        7 => "permission_denied",
        8 => "resource_exhausted",
        9 => "failed_precondition",
        10 => "aborted",
        11 => "out_of_range",
        12 => "unimplemented",
        13 => "internal",
        14 => "unavailable",
        15 => "data_loss",
        16 => "unauthenticated",
        _ => "unknown",
    }
}

// ---------------------------------------------------------------------------
// HTTP listener (/metrics, /healthz, /readyz)
// ---------------------------------------------------------------------------

/// Shared state for the telemetry listener.
#[derive(Clone)]
pub struct TelemetryState {
    pub store: Store,
    /// True when persistence runs in raft mode: readiness then additionally
    /// requires `raft_status()` to report a known leader.
    pub raft_required: bool,
    /// When this node first observed the current lane-leadership divergence
    /// (unix ms), or `0` while converged. Owned by the readiness probe: the
    /// divergence grace window is measured from this timestamp, so it is
    /// "how long has THIS split stood continuously" — immune to an
    /// unrelated lane flapping its leader (which used to refresh the old
    /// `election_epoch_ms`-based grace forever and mask a standing split as
    /// ready), and it grants a full grace window after a manager restart
    /// (where `election_epoch_ms` started at 0 and denied any grace).
    pub diverged_since_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

/// Content type for the Prometheus text exposition format.
pub const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// Run the telemetry listener until the process exits. Bound on its own port
/// (`server.port + 2` by convention; `telemetry.port` overrides).
pub async fn serve_telemetry(addr: SocketAddr, state: TelemetryState) -> R<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind telemetry listener on {addr}"))?;
    info!(%addr, "Telemetry listener started (/metrics, /healthz, /readyz)");
    serve_telemetry_on(listener, state).await
}

/// Serve telemetry on an already-bound listener (tests bind port 0 and read
/// back the local address before calling this).
pub async fn serve_telemetry_on(listener: tokio::net::TcpListener, state: TelemetryState) -> R<()> {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = ?err, "telemetry accept failed");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let state = state.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| handle(req, state.clone()));
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                debug!(error = ?err, "telemetry connection closed");
            }
        });
    }
}

/// Run the WORKER telemetry listener until the process exits: `/metrics`
/// (the `legion_worker_*` registry) and `/healthz`. No `/readyz` — a worker
/// has no store or quorum to probe; liveness is what its supervisor needs,
/// and the manager's view of readiness is the node's health/heartbeat state.
/// Opt-in via `LEGION_WORKER_TELEMETRY_PORT` (see docs/metrics.md).
pub async fn serve_worker_telemetry(addr: SocketAddr) -> R<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind worker telemetry listener on {addr}"))?;
    let local = listener.local_addr()?;
    info!(addr = %local, "Worker telemetry listener started (/metrics, /healthz)");
    serve_worker_telemetry_on(listener).await
}

/// Serve worker telemetry on an already-bound listener (tests bind port 0
/// and read back the local address before calling this).
pub async fn serve_worker_telemetry_on(listener: tokio::net::TcpListener) -> R<()> {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = ?err, "worker telemetry accept failed");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            let service = hyper::service::service_fn(handle_worker);
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                debug!(error = ?err, "worker telemetry connection closed");
            }
        });
    }
}

async fn handle_worker(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    if req.method() != hyper::Method::GET {
        return Ok(text_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain",
            "method not allowed\n".into(),
        ));
    }
    let resp = match req.uri().path() {
        "/metrics" => text_response(StatusCode::OK, METRICS_CONTENT_TYPE, render_worker()),
        "/healthz" => text_response(StatusCode::OK, "text/plain", "ok\n".into()),
        _ => text_response(StatusCode::NOT_FOUND, "text/plain", "not found\n".into()),
    };
    Ok(resp)
}

fn text_response(status: StatusCode, content_type: &str, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(Full::new(Bytes::from(body)))
        .expect("static response is valid")
}

async fn handle(
    req: Request<Incoming>,
    state: TelemetryState,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if req.method() != hyper::Method::GET {
        return Ok(text_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain",
            "method not allowed\n".into(),
        ));
    }
    let resp = match req.uri().path() {
        "/metrics" => text_response(StatusCode::OK, METRICS_CONTENT_TYPE, render()),
        "/healthz" => text_response(StatusCode::OK, "text/plain", "ok\n".into()),
        "/readyz" => match readiness(&state) {
            Ok(notes) => {
                // Advisory notes (e.g. even raft voter parity) do NOT break
                // readiness — the manager serves fine today; the note names
                // the availability risk and the fix (plan §7-C3, JC-29).
                let mut body = String::from("ok\n");
                for note in notes {
                    body.push_str(&format!("note: {note}\n"));
                }
                text_response(StatusCode::OK, "text/plain", body)
            }
            Err(reason) => {
                debug!(%reason, "readiness probe failing");
                text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "text/plain",
                    format!("not ready: {reason}\n"),
                )
            }
        },
        _ => text_response(StatusCode::NOT_FOUND, "text/plain", "not found\n".into()),
    };
    Ok(resp)
}

/// Readiness: the store must answer a trivial read (`SELECT 1` on a read
/// connection) and, under raft persistence, a leader must be known.
///
/// `Ok` carries advisory note lines that do NOT affect the 200 status: when
/// a leader is known but a lane's voter count is unhealthy (even parity, or
/// below `raft.expectedVoters`), the lane's plain-language advisory is
/// surfaced in the body — readiness is about serving traffic today, and an
/// even cluster serves fine; it just survives fewer failures than it could.
fn readiness(state: &TelemetryState) -> Result<Vec<String>, String> {
    let probe = state.store.read_connection().and_then(|conn| {
        conn.query_row("SELECT 1;", [], |row| row.get::<_, i64>(0))
            .map_err(anyhow::Error::from)
    });
    match probe {
        Ok(1) => {}
        Ok(other) => return Err(format!("store probe returned unexpected value {other}")),
        Err(err) => return Err(format!("store probe failed: {err}")),
    }
    let mut notes = Vec::new();
    if state.raft_required {
        match state.store.raft_status() {
            Some(status) => {
                let now_ms = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
                let diverged_for_ms =
                    observe_lane_divergence(&state.diverged_since_ms, &status, now_ms);
                if let Some(reason) = raft_readiness_failure(&status, diverged_for_ms) {
                    return Err(with_lane_detail(reason, &status));
                }
                // Per-lane detail in the body on the healthy path too: an
                // operator debugging a lanes cluster should not have to
                // reach for `legion raft status` to see which node leads
                // which lane (JC-40).
                notes.extend(lane_detail_lines(&status));
                for lane in &status.lanes {
                    if let Some(warning) = &lane.warning {
                        let note = if status.lanes.len() > 1 {
                            format!("lane {}: {warning}", lane.lane)
                        } else {
                            warning.clone()
                        };
                        if !notes.contains(&note) {
                            notes.push(note);
                        }
                    }
                }
            }
            None => return Err("raft status unavailable".to_string()),
        }
    }
    Ok(notes)
}

/// How long per-lane leaders may DISAGREE before this manager reports
/// itself not ready (JC-40). Elections are independent per lane, so a
/// failover legitimately leaves the lanes split for a moment while the
/// anchor leader's convergence loop reacquires them; that window must stay
/// 200 or every failover would flap every load balancer. Beyond it, the
/// split is standing, which is an abnormal leadership shape an operator
/// should look at — even though the write path now degrades rather than
/// dies (plan §8: submits are accepted by the control leader and completed
/// by lane reconcilers, worker liveness is forwarded to the status leader).
///
/// Note that convergence is BEST EFFORT (JC-40 correction, 2026-07-28): the
/// anchor leader's campaign is a race raft's election safety can refuse
/// forever, so this probe can legitimately stay `503` indefinitely. See
/// `docs/multi-raft.md`, "Lane leadership: co-located, best effort".
const LANE_LEADER_DIVERGENCE_GRACE_MS: i64 = 10_000;

/// Track how long the current lane-leadership divergence has stood, from
/// this probe's own observations. Returns `Some(ms)` while diverged, `None`
/// while converged (resetting the tracker).
///
/// "Diverged" is any not-fully-converged multi-lane shape: lanes led by
/// different managers, or some lane leaderless while others are led. Both
/// keep the timer running so a lane that flaps its leader (through elected
/// and leaderless states) cannot re-grant the grace window to a standing
/// split of the OTHER lanes — the defect in the previous
/// `election_epoch_ms`-based grace, which any leader change refreshed. Only
/// genuine convergence resets the clock. A restarted manager starts
/// converged (tracker 0), so its first observed divergence gets the full
/// grace window — the old scheme started at "no grace" after every restart
/// and failed rolling restarts straight into 503.
fn observe_lane_divergence(
    diverged_since_ms: &std::sync::atomic::AtomicI64,
    status: &crate::storage::persistence::RaftStatus,
    now_ms: i64,
) -> Option<i64> {
    use std::sync::atomic::Ordering;
    let converged = status.lanes.len() < 2 || {
        let mut leaders = std::collections::BTreeSet::new();
        let mut all_led = true;
        for lane in &status.lanes {
            match lane.leader_id.as_deref() {
                Some(leader) => {
                    leaders.insert(leader);
                }
                None => all_led = false,
            }
        }
        all_led && leaders.len() == 1
    };
    if converged {
        diverged_since_ms.store(0, Ordering::Relaxed);
        return None;
    }
    let mut since = diverged_since_ms.load(Ordering::Relaxed);
    if since == 0 {
        // First observation of this divergence: the clock starts now. On a
        // race between two probes the earlier store wins; either way the
        // value is within one probe interval of the truth.
        let _ = diverged_since_ms.compare_exchange(0, now_ms, Ordering::Relaxed, Ordering::Relaxed);
        since = diverged_since_ms.load(Ordering::Relaxed);
    }
    Some(now_ms.saturating_sub(since))
}

/// Why raft state makes this manager NOT ready, if anything.
///
/// Three failure modes, all lanes-aware:
///
/// 1. no leader at all;
/// 2. some lane has no leader — the aggregate `leader_id` becomes a
///    `control=…,scheduler=-,…` summary string whenever per-lane leaders
///    differ, INCLUDING a lane with no leader, so gating on
///    `leader_id.is_some()` alone reported 200 while, say, the scheduler
///    lane's quorum (and with it all placement) was down (adversarial
///    finding, 2026-07-24);
/// 3. every lane has a leader but they are DIFFERENT nodes, and have been
///    for longer than [`LANE_LEADER_DIVERGENCE_GRACE_MS`] (JC-40).
///
/// `diverged_for_ms` comes from [`observe_lane_divergence`]: how long the
/// current divergence has stood continuously, by this probe's own clock.
fn raft_readiness_failure(
    status: &crate::storage::persistence::RaftStatus,
    diverged_for_ms: Option<i64>,
) -> Option<String> {
    if status.leader_id.is_none() {
        return Some("raft leader unknown".to_string());
    }
    let leaderless: Vec<&str> = status
        .lanes
        .iter()
        .filter(|lane| lane.leader_id.is_none())
        .map(|lane| lane.lane.as_str())
        .collect();
    if !leaderless.is_empty() {
        return Some(format!(
            "raft leader unknown for lane(s): {}",
            leaderless.join(", ")
        ));
    }
    let distinct: std::collections::BTreeSet<&str> = status
        .lanes
        .iter()
        .filter_map(|lane| lane.leader_id.as_deref())
        .collect();
    if status.lanes.len() > 1 && distinct.len() > 1 {
        let split_for_ms = diverged_for_ms.unwrap_or(0);
        if split_for_ms < LANE_LEADER_DIVERGENCE_GRACE_MS {
            // Brief post-election convergence: still ready.
            return None;
        }
        return Some(format!(
            "raft lane leadership has not converged for {}s: lanes are led by different \
             managers ({}). Single-lane operations and reads are unaffected; job submits \
             are accepted by the control leader and completed by lane reconcilers; worker \
             liveness is forwarded — but scheduling latency is degraded and state backup \
             is unavailable until leadership converges. Check `legion raft status` and \
             restart the stray lane's leader if it is unhealthy",
            split_for_ms.max(0) / 1000,
            lane_leader_summary(status)
        ));
    }
    None
}

/// `control=mgr-a,scheduler=mgr-b,status=-` — the same shape `raft status`
/// uses for its aggregate leader column when lanes disagree.
fn lane_leader_summary(status: &crate::storage::persistence::RaftStatus) -> String {
    status
        .lanes
        .iter()
        .map(|lane| format!("{}={}", lane.lane, lane.leader_id.as_deref().unwrap_or("-")))
        .collect::<Vec<_>>()
        .join(",")
}

/// One body line per lane (multi-lane topologies only): who leads it and
/// what this node's role on it is.
fn lane_detail_lines(status: &crate::storage::persistence::RaftStatus) -> Vec<String> {
    if status.lanes.len() < 2 {
        return Vec::new();
    }
    status
        .lanes
        .iter()
        .map(|lane| {
            format!(
                "lane {}: leader {} (this node: {})",
                lane.lane,
                lane.leader_id.as_deref().unwrap_or("-"),
                lane.role.as_str()
            )
        })
        .collect()
}

/// Appends the per-lane detail lines to a readiness failure reason so the
/// non-200 body names which lane is wrong, not just that something is.
fn with_lane_detail(reason: String, status: &crate::storage::persistence::RaftStatus) -> String {
    let detail = lane_detail_lines(status);
    if detail.is_empty() {
        return reason;
    }
    format!("{reason}\n{}", detail.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Adversarial regression (readyz vs. partial lane leadership): a lane
    /// with NO leader must fail readiness even while the aggregate
    /// `leader_id` carries the non-empty split summary string.
    #[test]
    fn readiness_requires_a_leader_on_every_lane() {
        use crate::storage::persistence::{PersistenceRole, RaftLaneStatus, RaftStatus};
        const NOW_MS: i64 = 1_000_000;
        let lane = |name: &str, leader: Option<&str>| RaftLaneStatus {
            lane: name.to_string(),
            role: PersistenceRole::Standby,
            leader_id: leader.map(str::to_string),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            voters: 3,
            quorum: 2,
            survives: 1,
            warning: None,
        };
        let mut status = RaftStatus {
            enabled: true,
            backend: "raft".into(),
            node_id: "mgr-a".into(),
            cluster_id: "legion-raft-test".into(),
            role: PersistenceRole::Primary,
            leader_id: Some("control=mgr-a,scheduler=-,status=mgr-a".into()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            lease_held: true,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 0,
            raft_library: "openraft-0.9".into(),
            lanes: vec![
                lane("control", Some("mgr-a")),
                lane("scheduler", None),
                lane("status", Some("mgr-a")),
            ],
        };
        let reason =
            raft_readiness_failure(&status, Some(NOW_MS)).expect("a leaderless lane must fail");
        assert!(reason.contains("scheduler"), "{reason}");

        // Every lane led by the SAME node (JC-40 steady state): ready.
        status.lanes[1].leader_id = Some("mgr-a".into());
        status.leader_id = Some("mgr-a".into());
        assert_eq!(raft_readiness_failure(&status, None), None);

        status.leader_id = None;
        assert!(raft_readiness_failure(&status, None).is_some());
    }

    /// JC-40: lanes led by DIFFERENT managers are ready during the brief
    /// post-election convergence window and NOT ready once the split has
    /// stood still past the grace period — the aggregate leader summary is
    /// non-empty in both states, so it must not be what readiness reads.
    #[test]
    fn readiness_fails_when_lane_leadership_stays_diverged() {
        use crate::storage::persistence::{PersistenceRole, RaftLaneStatus, RaftStatus};
        let lane = |name: &str, leader: &str| RaftLaneStatus {
            lane: name.to_string(),
            role: PersistenceRole::Standby,
            leader_id: Some(leader.to_string()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            voters: 3,
            quorum: 2,
            survives: 1,
            warning: None,
        };
        let mut status = RaftStatus {
            enabled: true,
            backend: "raft".into(),
            node_id: "mgr-a".into(),
            cluster_id: "legion-raft-test".into(),
            role: PersistenceRole::Primary,
            leader_id: Some("control=mgr-a,scheduler=mgr-b,status=mgr-a".into()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            lease_held: true,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 0,
            raft_library: "openraft-0.9".into(),
            lanes: vec![
                lane("control", "mgr-a"),
                lane("scheduler", "mgr-b"),
                lane("status", "mgr-a"),
            ],
        };
        // A split observed one second ago: convergence is still in flight.
        assert_eq!(
            raft_readiness_failure(&status, Some(1_000)),
            None,
            "a split within the grace window must stay ready"
        );

        // The same split, standing continuously past the grace window.
        let reason = raft_readiness_failure(&status, Some(LANE_LEADER_DIVERGENCE_GRACE_MS + 5_000))
            .expect("a persistent lane-leadership split must fail readiness");
        assert!(
            reason.contains("scheduler=mgr-b") && reason.contains("control=mgr-a"),
            "the reason must name the per-lane leaders: {reason}"
        );

        // Converged onto one manager: ready again, regardless of age.
        status.lanes[1].leader_id = Some("mgr-a".into());
        assert_eq!(raft_readiness_failure(&status, None), None);

        // Per-lane detail rides the body in both directions.
        let detail = lane_detail_lines(&status);
        assert_eq!(detail.len(), 3);
        assert!(
            detail[0].contains("lane control: leader mgr-a"),
            "{detail:?}"
        );
    }

    /// The divergence tracker measures how long the CURRENT divergence has
    /// stood continuously, by this probe's own observations:
    ///
    /// - a fresh (restarted) manager grants the full grace window to the
    ///   first divergence it sees (the old `election_epoch_ms` scheme
    ///   started at "no grace" and failed rolling restarts straight into
    ///   503);
    /// - an unrelated lane flapping its leader — including through
    ///   leaderless moments — must NOT reset the clock on a standing split
    ///   (the old scheme refreshed the grace on any leader change, masking
    ///   the split forever);
    /// - genuine convergence resets it.
    #[test]
    fn divergence_tracker_measures_continuous_divergence() {
        use crate::storage::persistence::{PersistenceRole, RaftLaneStatus, RaftStatus};
        use std::sync::atomic::AtomicI64;
        let lane = |name: &str, leader: Option<&str>| RaftLaneStatus {
            lane: name.to_string(),
            role: PersistenceRole::Standby,
            leader_id: leader.map(str::to_string),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            voters: 3,
            quorum: 2,
            survives: 1,
            warning: None,
        };
        let mut status = RaftStatus {
            enabled: true,
            backend: "raft".into(),
            node_id: "mgr-a".into(),
            cluster_id: "legion-raft-test".into(),
            role: PersistenceRole::Primary,
            leader_id: Some("control=mgr-a,scheduler=mgr-b,status=mgr-a".into()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            lease_held: true,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 0,
            raft_library: "openraft-0.9".into(),
            lanes: vec![
                lane("control", Some("mgr-a")),
                lane("scheduler", Some("mgr-b")),
                lane("status", Some("mgr-a")),
            ],
        };
        let tracker = AtomicI64::new(0);
        // Restart-fresh tracker: the first observation starts the clock at 0.
        assert_eq!(
            observe_lane_divergence(&tracker, &status, 100_000),
            Some(0),
            "the first observed divergence gets the full grace window"
        );
        // The split stands: the clock runs.
        assert_eq!(
            observe_lane_divergence(&tracker, &status, 108_000),
            Some(8_000)
        );
        // The status lane flaps through a leaderless moment: still diverged,
        // clock keeps running.
        status.lanes[2].leader_id = None;
        assert_eq!(
            observe_lane_divergence(&tracker, &status, 111_000),
            Some(11_000)
        );
        // It re-elects a DIFFERENT leader: still diverged, clock still runs.
        status.lanes[2].leader_id = Some("mgr-c".into());
        assert_eq!(
            observe_lane_divergence(&tracker, &status, 115_000),
            Some(15_000)
        );
        // Genuine convergence resets.
        status.lanes[1].leader_id = Some("mgr-a".into());
        status.lanes[2].leader_id = Some("mgr-a".into());
        assert_eq!(observe_lane_divergence(&tracker, &status, 120_000), None);
        // A later, new split starts a fresh clock.
        status.lanes[1].leader_id = Some("mgr-b".into());
        assert_eq!(observe_lane_divergence(&tracker, &status, 130_000), Some(0));
    }

    /// `single` topology reports one lane; a co-location check must never
    /// fire there (there is nothing to diverge from).
    #[test]
    fn readiness_ignores_lane_divergence_in_single_topology() {
        use crate::storage::persistence::{PersistenceRole, RaftLaneStatus, RaftStatus};
        let status = RaftStatus {
            enabled: true,
            backend: "raft".into(),
            node_id: "mgr-a".into(),
            cluster_id: "legion-raft-test".into(),
            role: PersistenceRole::Primary,
            leader_id: Some("mgr-a".into()),
            term: 1,
            commit_index: 0,
            applied_index: 0,
            lease_held: true,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            election_epoch_ms: 0,
            join_token_ttl_seconds: 0,
            raft_library: "openraft-0.9".into(),
            lanes: vec![RaftLaneStatus {
                lane: "control".into(),
                role: PersistenceRole::Primary,
                leader_id: Some("mgr-a".into()),
                term: 1,
                commit_index: 0,
                applied_index: 0,
                voters: 3,
                quorum: 2,
                survives: 1,
                warning: None,
            }],
        };
        assert_eq!(raft_readiness_failure(&status, None), None);
        assert!(lane_detail_lines(&status).is_empty());
    }

    #[test]
    fn counter_vec_renders_and_escapes_labels() {
        let counter = CounterVec {
            meta: FamilyMeta {
                name: "test_counter_total",
                help: "help text",
                labels: &["method"],
            },
            series: Series::new(),
        };
        counter.inc(&["a\"b\\c\nd"]);
        counter.inc(&["plain"]);
        counter.inc(&["plain"]);
        let mut out = String::new();
        counter.render_into(&mut out);
        assert!(out.contains("# HELP test_counter_total help text\n"));
        assert!(out.contains("# TYPE test_counter_total counter\n"));
        assert!(out.contains("test_counter_total{method=\"a\\\"b\\\\c\\nd\"} 1\n"));
        assert!(out.contains("test_counter_total{method=\"plain\"} 2\n"));
    }

    #[test]
    fn histogram_renders_cumulative_buckets_sum_and_count() {
        let hist = Histogram {
            meta: FamilyMeta {
                name: "test_hist_seconds",
                help: "h",
                labels: &[],
            },
            cell: HistogramCell::new(DURATION_BUCKETS),
        };
        hist.observe(0.0004); // first bucket
        hist.observe(0.3); // le=0.5
        hist.observe(60.0); // +Inf only
        let mut out = String::new();
        hist.render_into(&mut out);
        assert!(out.contains("test_hist_seconds_bucket{le=\"0.0005\"} 1\n"));
        assert!(out.contains("test_hist_seconds_bucket{le=\"0.5\"} 2\n"));
        assert!(out.contains("test_hist_seconds_bucket{le=\"10\"} 2\n"));
        assert!(out.contains("test_hist_seconds_bucket{le=\"+Inf\"} 3\n"));
        assert!(out.contains("test_hist_seconds_count 3\n"));
        let sum_line = out
            .lines()
            .find(|l| l.starts_with("test_hist_seconds_sum"))
            .expect("sum line rendered");
        let sum: f64 = sum_line.split_whitespace().nth(1).unwrap().parse().unwrap();
        assert!((sum - 60.3004).abs() < 1e-9);
    }

    #[test]
    fn gauge_vec_set_all_zeroes_absent_series() {
        let gauge = GaugeVec {
            meta: FamilyMeta {
                name: "test_nodes",
                help: "g",
                labels: &["status"],
            },
            series: Series::new(),
        };
        gauge.set_all(&[(vec!["active".into()], 3), (vec!["lost".into()], 1)]);
        gauge.set_all(&[(vec!["active".into()], 2)]);
        let mut out = String::new();
        gauge.render_into(&mut out);
        assert!(out.contains("test_nodes{status=\"active\"} 2\n"));
        assert!(out.contains("test_nodes{status=\"lost\"} 0\n"));
    }

    #[test]
    fn series_cap_folds_overflow_into_other() {
        let counter = CounterVec {
            meta: FamilyMeta {
                name: "test_overflow_total",
                help: "c",
                labels: &["path"],
            },
            series: Series::new(),
        };
        for i in 0..(MAX_SERIES_PER_FAMILY + 25) {
            counter.inc(&[&format!("path-{i}")]);
        }
        assert_eq!(counter.get(&["other"]), 25);
        // The map holds exactly the cap plus the single overflow series.
        assert_eq!(counter.series.snapshot().len(), MAX_SERIES_PER_FAMILY + 1);
    }

    #[test]
    fn grpc_method_label_takes_last_two_segments() {
        assert_eq!(
            grpc_method_label("/manager.Manager/StopJob"),
            "manager.Manager/StopJob"
        );
        assert_eq!(grpc_method_label("/foo"), "foo");
        assert_eq!(grpc_method_label("/"), "/");
        assert_eq!(grpc_method_label("/a/b/c"), "b/c");
    }

    #[test]
    fn grpc_code_label_prefers_header_then_status_class() {
        let mut headers = http::HeaderMap::new();
        assert_eq!(grpc_code_label(&headers, http::StatusCode::OK), "http_2xx");
        headers.insert("grpc-status", "7".parse().unwrap());
        assert_eq!(
            grpc_code_label(&headers, http::StatusCode::OK),
            "permission_denied"
        );
        headers.insert("grpc-status", "99".parse().unwrap());
        assert_eq!(grpc_code_label(&headers, http::StatusCode::OK), "unknown");
    }

    #[test]
    fn global_render_includes_every_family() {
        // Touch a couple of metrics so labeled families have series too.
        metrics().plan_apply_total.inc(&["applied"]);
        metrics().store_write_lock_wait_seconds.observe(0.002);
        let out = render();
        for family in [
            "legion_allocations",
            "legion_eval_queue_depth",
            "legion_evals_processed_total",
            "legion_grpc_in_flight",
            "legion_grpc_request_duration_seconds",
            "legion_grpc_requests_total",
            "legion_heartbeats_total",
            "legion_nodes",
            "legion_node_memory_physical_bytes",
            "legion_node_memory_available_bytes",
            "legion_node_memory_used_bytes",
            "legion_node_cpu_used_millicores",
            "legion_node_pressure_actions_total",
            "legion_plan_apply_total",
            "legion_raft_pipeline_depth",
            "legion_raft_pipeline_wait_seconds",
            "legion_read_model_nodes",
            "legion_runtime_spawn_delay_seconds",
            "legion_runtime_worker_threads",
            "legion_store_coalesce_batch_size",
            "legion_store_coalesce_commit_seconds",
            "legion_store_coalesce_wait_rounds_total",
            "legion_store_coalesce_wait_seconds",
            "legion_store_write_lock_wait_seconds",
        ] {
            assert!(
                out.contains(&format!("# TYPE {family} ")),
                "missing family {family} in render output"
            );
        }
        assert!(out.contains("legion_plan_apply_total{outcome=\"applied\"}"));
    }

    #[test]
    fn gauge_vec_add_is_relative_per_series() {
        let gauge = GaugeVec {
            meta: FamilyMeta {
                name: "test_depth",
                help: "g",
                labels: &["lane"],
            },
            series: Series::new(),
        };
        gauge.add(&["status"], 1);
        gauge.add(&["status"], 1);
        gauge.add(&["control"], 1);
        gauge.add(&["status"], -1);
        assert_eq!(gauge.get(&["status"]), 1);
        assert_eq!(gauge.get(&["control"]), 1);
        assert_eq!(gauge.get(&["scheduler"]), 0);
    }

    #[test]
    fn in_flight_guard_decrements_when_a_request_is_dropped_unfinished() {
        let method = "legion.Manager/InFlightGuardTest";
        let started = metrics().grpc_in_flight.get(&[method]);
        {
            let _guard = InFlightGuard::enter(method);
            assert_eq!(metrics().grpc_in_flight.get(&[method]), started + 1);
            // No completion path runs here — this is the cancelled-request
            // case (client hung up, tonic dropped the future).
        }
        assert_eq!(metrics().grpc_in_flight.get(&[method]), started);
    }
}
