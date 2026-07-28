//! Manager telemetry: a dependency-free metrics registry plus the HTTP
//! listener that exposes it.
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
        debug_assert_eq!(labels.len(), self.meta.labels.len());
        self.series.cell(labels, CounterCell::default).inc();
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

    pub fn get(&self) -> u64 {
        self.cell.get()
    }
}

impl RenderMetric for Counter {
    fn render_into(&self, out: &mut String) {
        render_header(out, &self.meta, "counter");
        out.push_str(&format!("{} {}\n", self.meta.name, self.cell.get()));
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
}

// ---------------------------------------------------------------------------
// Global registry
// ---------------------------------------------------------------------------

/// Every metric family the manager exports. Obtained via [`metrics`]; fields
/// are updated directly at the instrumented call sites.
///
/// Naming note: the `legion_raft_*` prefix is reserved for the raft engine.
/// The first two engine series (`legion_raft_voters{lane}`,
/// `legion_raft_quorum{lane}` — plan §7-C3) live here; the rest of the
/// prefix (term, indexes, apply lag, ...) stays reserved (docs/metrics.md).
pub struct Metrics {
    /// `legion_grpc_requests_total{method,code}`
    pub grpc_requests_total: Arc<CounterVec>,
    /// `legion_grpc_request_duration_seconds{method}`
    pub grpc_request_duration_seconds: Arc<HistogramVec>,
    /// `legion_evals_processed_total{kind,status}`
    pub evals_processed_total: Arc<CounterVec>,
    /// `legion_eval_queue_depth`
    pub eval_queue_depth: Arc<Gauge>,
    /// `legion_plan_apply_total{outcome}`
    pub plan_apply_total: Arc<CounterVec>,
    /// `legion_heartbeats_total`
    pub heartbeats_total: Arc<Counter>,
    /// `legion_nodes{status}`
    pub nodes: Arc<GaugeVec>,
    /// `legion_allocations`
    pub allocations: Arc<Gauge>,
    /// `legion_store_write_lock_wait_seconds`
    pub store_write_lock_wait_seconds: Arc<Histogram>,
    /// `legion_raft_voters{lane}`
    pub raft_voters: Arc<GaugeVec>,
    /// `legion_raft_quorum{lane}`
    pub raft_quorum: Arc<GaugeVec>,
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
        let raft_voters = Arc::new(GaugeVec {
            meta: FamilyMeta {
                name: "legion_raft_voters",
                help: "Voting members of each raft lane's group, as known by this manager.",
                labels: &["lane"],
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

        let families: Vec<Arc<dyn RenderMetric>> = vec![
            Arc::clone(&allocations) as Arc<dyn RenderMetric>,
            Arc::clone(&eval_queue_depth) as Arc<dyn RenderMetric>,
            Arc::clone(&evals_processed_total) as Arc<dyn RenderMetric>,
            Arc::clone(&grpc_request_duration_seconds) as Arc<dyn RenderMetric>,
            Arc::clone(&grpc_requests_total) as Arc<dyn RenderMetric>,
            Arc::clone(&heartbeats_total) as Arc<dyn RenderMetric>,
            Arc::clone(&nodes) as Arc<dyn RenderMetric>,
            Arc::clone(&plan_apply_total) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_quorum) as Arc<dyn RenderMetric>,
            Arc::clone(&raft_voters) as Arc<dyn RenderMetric>,
            Arc::clone(&store_write_lock_wait_seconds) as Arc<dyn RenderMetric>,
        ];

        Metrics {
            grpc_requests_total,
            grpc_request_duration_seconds,
            evals_processed_total,
            eval_queue_depth,
            plan_apply_total,
            heartbeats_total,
            nodes,
            allocations,
            store_write_lock_wait_seconds,
            raft_voters,
            raft_quorum,
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

// ---------------------------------------------------------------------------
// gRPC instrumentation layer
// ---------------------------------------------------------------------------

/// Tower layer recording `legion_grpc_requests_total` and
/// `legion_grpc_request_duration_seconds` around the tonic service.
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
        let fut = self.inner.call(req);
        Box::pin(async move {
            let result = fut.await;
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
            "legion_grpc_request_duration_seconds",
            "legion_grpc_requests_total",
            "legion_heartbeats_total",
            "legion_nodes",
            "legion_plan_apply_total",
            "legion_store_write_lock_wait_seconds",
        ] {
            assert!(
                out.contains(&format!("# TYPE {family} ")),
                "missing family {family} in render output"
            );
        }
        assert!(out.contains("legion_plan_apply_total{outcome=\"applied\"}"));
    }
}
