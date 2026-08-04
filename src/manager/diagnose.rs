//! Scheduler diagnosis: the pure detector engine behind
//! `legion scheduler diagnose` (docs/diagnosing-scheduler-performance.md).
//!
//! The whole module is deliberately I/O-free. The RPC handler takes two
//! [`SchedulerSnapshot`]s `sample_seconds` apart, hands them plus the
//! resolved manager knobs to [`diagnose`], and renders whatever comes back.
//! Everything that decides *anything* lives here, so a real incident's
//! numbers become a unit test by typing them into a snapshot literal.
//!
//! Two invariants hold across every detector, and both exist because the
//! failure this feature was built for was single-number reading:
//!
//! 1. **Every failing finding carries what it ruled out.** A finding with an
//!    empty `ruled_out` is a bug, not a terse finding — see the
//!    `every_failing_finding_carries_a_discriminator` test.
//! 2. **Passing checks are reported too**, with the number that made them
//!    pass. Silence on a passing check is what makes the next person
//!    re-investigate a cause somebody already excluded.
//!
//! Every number a detector states is a delta over the stated window unless
//! it is genuinely a gauge; gauges are read from the *later* snapshot.

use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Thresholds
//
// Constants, never learned baselines (plan §4, N2). Each carries the
// derivation that produced it — mostly the AWS scale campaign of
// 2026-08-02/03, whose numbers are also the fixtures at the bottom of this
// file.
// ---------------------------------------------------------------------------

/// Plan-rejection share that earns a `warning`. Measured: a 4-thread / 1k-node
/// cluster sat at 15.9% rejected while still gaining throughput, so the warn
/// line is below that but above the low single digits a healthy contended
/// cluster produces.
const REJECTION_WARN_SHARE: f64 = 0.05;

/// Plan-rejection share that earns a `critical`. Measured: 16 threads / 5k
/// nodes reached 73.6% rejected and throughput *fell* (354.6 → 310.4
/// alloc/sec). Anything past a fifth of all plans is paying more for retries
/// than for placements.
const REJECTION_CRITICAL_SHARE: f64 = 0.20;

/// Absolute floor for the lane-queueing detector. Below 1ms of pipeline wait
/// the lane is not the bottleneck no matter how lopsided the ratio looks —
/// two idle lanes can differ by 20x and mean nothing.
const LANE_WAIT_FLOOR_SECONDS: f64 = 0.001;

/// How much busier the scheduler lane must be than the quietest lane before
/// "the lane is queueing" beats "the whole control plane is busy". Measured:
/// 3.07ms against 0.09ms, a 34x spread.
const LANE_WAIT_RATIO: f64 = 5.0;

/// Per-lane `propose_apply` spread that still counts as "flat". If commit
/// cost itself differs by more than this across lanes, the lanes are doing
/// different work and the wait comparison is not apples to apples. Measured:
/// 1.51ms on both lanes while wait differed 34x.
const PROPOSE_APPLY_FLAT_RATIO: f64 = 2.0;

/// `runtime_spawn_delay_seconds` p99 above which the tokio runtime really is
/// starved and "not CPU-bound" may not be said. Measured healthy: 41µs, with
/// the box at 29% CPU. A whole order of magnitude of headroom over that.
const SPAWN_DELAY_STARVED_SECONDS: f64 = 0.001;

/// Share of `total` one plan phase must hold before it is called out.
const PHASE_DOMINANCE_SHARE: f64 = 0.50;

/// How far `total` may exceed the sum of its phases before the gap itself is
/// the finding (lock waits and glue outside any timed phase).
const PHASE_UNACCOUNTED_SHARE: f64 = 0.20;

/// Share of processed evals one kind must hold to count as starvation.
/// Measured: 5,760 `node_lost_reschedule` against 2,503 `job_submit` — 70%.
const STARVATION_KIND_SHARE: f64 = 0.60;

/// Queue depth below which a lopsided kind mix is just a quiet cluster doing
/// one thing, not starvation. Measured: the starved cluster sat at 764.
const STARVATION_QUEUE_DEPTH: i64 = 100;

/// Heartbeat freshness below which the fleet is falling behind regardless of
/// the lost window. Mirrors `legion node cadence`'s own verdict line.
const FRESHNESS_FALLING_BEHIND: f64 = 0.50;

/// Fleet size below which `heartbeat_freshness_ratio` is noise. Matches
/// `HEARTBEAT_FRESHNESS_MIN_FLEET` in constants.rs — restated rather than
/// imported so this module stays a pure function of its inputs.
const FRESHNESS_MIN_FLEET: f64 = 20.0;

/// The public page every finding points into.
const DOC: &str = "docs/diagnosing-scheduler-performance.md";

// ---------------------------------------------------------------------------
// Metric family names the detectors read
// ---------------------------------------------------------------------------

const M_PLAN_APPLY: &str = "legion_plan_apply_total";
const M_PLAN_REJECTED: &str = "legion_plan_rejected_total";
const M_CANDIDATE_RANK: &str = "legion_scheduler_plan_candidate_rank";
const M_INDEX_CANDIDATES: &str = "legion_scheduler_index_candidates";
const M_PIPELINE_WAIT: &str = "legion_raft_pipeline_wait_seconds";
const M_PROPOSE_APPLY: &str = "legion_raft_propose_apply_seconds";
const M_SPAWN_DELAY: &str = "legion_runtime_spawn_delay_seconds";
const M_WORKER_THREADS: &str = "legion_runtime_worker_threads";
const M_GRPC_IN_FLIGHT: &str = "legion_grpc_in_flight";
const M_COALESCE_BATCH: &str = "legion_store_coalesce_batch_size";
const M_PLAN_PHASE: &str = "legion_scheduler_plan_phase_seconds";
const M_EVALS_PROCESSED: &str = "legion_evals_processed_total";
const M_EVALS_FAILED: &str = "legion_evals_failed_total";
const M_EVAL_QUEUE_DEPTH: &str = "legion_eval_queue_depth";
const M_FRESHNESS: &str = "legion_heartbeat_freshness_ratio";
const M_SERVED_INTERVAL: &str = "legion_heartbeat_served_interval_seconds";
const M_LOST_WINDOW: &str = "legion_heartbeat_lost_window_seconds";
const M_NODES: &str = "legion_nodes";
const M_NODES_LOST: &str = "legion_nodes_marked_lost_total";
const M_NODES_RECOVERED: &str = "legion_nodes_recovered_total";

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// One histogram series, in the registry's own shape: non-cumulative bucket
/// counts, one per bound plus a final `+Inf` slot.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistogramSample {
    pub bounds: Vec<f64>,
    pub bucket_counts: Vec<u64>,
    pub count: u64,
    pub sum: f64,
}

impl HistogramSample {
    /// Build a sample from `(upper_bound, count_in_bucket)` pairs plus an
    /// overflow count. Test/fixture ergonomics — production builds these
    /// from the registry snapshot.
    pub fn from_buckets(buckets: &[(f64, u64)], overflow: u64, sum: f64) -> Self {
        let bounds: Vec<f64> = buckets.iter().map(|(bound, _)| *bound).collect();
        let mut bucket_counts: Vec<u64> = buckets.iter().map(|(_, count)| *count).collect();
        bucket_counts.push(overflow);
        let count = bucket_counts.iter().sum();
        HistogramSample {
            bounds,
            bucket_counts,
            count,
            sum,
        }
    }

    /// What happened *during the window*: `self - earlier`, per bucket.
    ///
    /// Saturating throughout. A counter that went backwards means the
    /// manager restarted mid-sample, and the honest reading of "the process
    /// I was measuring is gone" is zero, never a negative rate.
    pub fn delta(&self, earlier: &HistogramSample) -> HistogramSample {
        if earlier.bounds != self.bounds || earlier.count > self.count {
            return self.clone();
        }
        let bucket_counts = self
            .bucket_counts
            .iter()
            .enumerate()
            .map(|(idx, count)| {
                count.saturating_sub(earlier.bucket_counts.get(idx).copied().unwrap_or(0))
            })
            .collect();
        HistogramSample {
            bounds: self.bounds.clone(),
            bucket_counts,
            count: self.count.saturating_sub(earlier.count),
            sum: (self.sum - earlier.sum).max(0.0),
        }
    }

    /// Linear-interpolated quantile over the bucket bounds, the same
    /// estimate `histogram_quantile()` produces in Prometheus. `None` on an
    /// empty histogram — "no observations" is not "zero seconds".
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let target = q * self.count as f64;
        let mut cumulative = 0f64;
        let mut lower = 0.0;
        for (idx, bound) in self.bounds.iter().enumerate() {
            let bucket = self.bucket_counts.get(idx).copied().unwrap_or(0) as f64;
            if cumulative + bucket >= target {
                if bucket == 0.0 {
                    return Some(*bound);
                }
                let within = ((target - cumulative) / bucket).clamp(0.0, 1.0);
                return Some(lower + (bound - lower) * within);
            }
            cumulative += bucket;
            lower = *bound;
        }
        // Everything past the last finite bound: the best honest answer is
        // "at least this", so report the bound rather than inventing a tail.
        Some(lower)
    }

    pub fn mean(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }

    /// Share of observations at or below `bound`. Used for the
    /// `coalesce_batch_size{le=1}` reading: how often plan applies commit
    /// alone instead of riding a batch.
    pub fn share_at_or_below(&self, bound: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let mut cumulative = 0u64;
        for (idx, b) in self.bounds.iter().enumerate() {
            cumulative += self.bucket_counts.get(idx).copied().unwrap_or(0);
            if *b >= bound {
                return Some(cumulative as f64 / self.count as f64);
            }
        }
        Some(1.0)
    }
}

/// Free capacity for one node pool, over available nodes only.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PoolFreeCapacity {
    pub pool: String,
    /// Available (schedulable) nodes in the pool.
    pub nodes: u64,
    /// Of those, how many still have both CPU and memory left.
    pub nodes_with_free_capacity: u64,
    pub free_cpu: f64,
    pub free_memory_mib: f64,
}

/// What the store contributes to a snapshot: the shapes the metric registry
/// cannot express yet. Phase 2 moves the eval halves onto labelled metric
/// families; the pool capacity stays here, because it is state and not a
/// rate.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StoreSnapshot {
    /// `SELECT type, COUNT(*) FROM eval WHERE status = 'pending' GROUP BY type`.
    pub pending_evals_by_kind: BTreeMap<String, i64>,
    /// The same for `status = 'failed'` — terminally failed rows, which is
    /// the population `evals_processed_total{status="error"}` famously does
    /// not describe.
    pub failed_evals_by_kind: BTreeMap<String, i64>,
    pub pools: Vec<PoolFreeCapacity>,
}

impl StoreSnapshot {
    pub fn pending_total(&self) -> i64 {
        self.pending_evals_by_kind.values().sum()
    }

    pub fn failed_total(&self) -> i64 {
        self.failed_evals_by_kind.values().sum()
    }
}

/// One reading of everything a detector may look at: the manager's metric
/// registry plus the store shapes it cannot express.
///
/// Series are keyed by `(family, label values in declared order)` — the
/// registry's own key — with a label-name list per family so lookups can be
/// written by name.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SchedulerSnapshot {
    counters: BTreeMap<String, Vec<(Vec<String>, u64)>>,
    gauges: BTreeMap<String, Vec<(Vec<String>, f64)>>,
    histograms: BTreeMap<String, Vec<(Vec<String>, HistogramSample)>>,
    label_names: BTreeMap<String, Vec<String>>,
    pub store: StoreSnapshot,
}

impl SchedulerSnapshot {
    pub fn new() -> Self {
        Self::default()
    }

    fn remember_labels(&mut self, family: &str, names: &[&str]) {
        self.label_names
            .entry(family.to_string())
            .or_insert_with(|| names.iter().map(|n| n.to_string()).collect());
    }

    /// Record a counter series. `labels` are `(name, value)` pairs in the
    /// family's declared order.
    pub fn counter(&mut self, family: &str, labels: &[(&str, &str)], value: u64) -> &mut Self {
        let names: Vec<&str> = labels.iter().map(|(name, _)| *name).collect();
        self.remember_labels(family, &names);
        let values: Vec<String> = labels.iter().map(|(_, v)| v.to_string()).collect();
        self.counters
            .entry(family.to_string())
            .or_default()
            .push((values, value));
        self
    }

    pub fn gauge(&mut self, family: &str, labels: &[(&str, &str)], value: f64) -> &mut Self {
        let names: Vec<&str> = labels.iter().map(|(name, _)| *name).collect();
        self.remember_labels(family, &names);
        let values: Vec<String> = labels.iter().map(|(_, v)| v.to_string()).collect();
        self.gauges
            .entry(family.to_string())
            .or_default()
            .push((values, value));
        self
    }

    pub fn histogram(
        &mut self,
        family: &str,
        labels: &[(&str, &str)],
        sample: HistogramSample,
    ) -> &mut Self {
        let names: Vec<&str> = labels.iter().map(|(name, _)| *name).collect();
        self.remember_labels(family, &names);
        let values: Vec<String> = labels.iter().map(|(_, v)| v.to_string()).collect();
        self.histograms
            .entry(family.to_string())
            .or_default()
            .push((values, sample));
        self
    }

    pub fn with_store(&mut self, store: StoreSnapshot) -> &mut Self {
        self.store = store;
        self
    }

    /// Register a family and its label names even when it currently has no
    /// series, so "the family exists and is empty" stays distinguishable
    /// from "this manager predates the family". The distinction matters:
    /// treating an absent counter as a zero reading is how a health check
    /// once reported a cluster healthy that had never run a step.
    pub fn declare(&mut self, family: &str, label_names: &[&str]) -> &mut Self {
        self.remember_labels(family, label_names);
        self
    }

    pub fn has_family(&self, family: &str) -> bool {
        self.label_names.contains_key(family)
    }

    /// Build a snapshot from the manager registry's structured snapshot
    /// (`telemetry::snapshot_manager_families`) plus the store half.
    ///
    /// The only bridge between this module and the outside world, and it
    /// takes its input by value — the sampling itself stays in the handler,
    /// so everything below is still a pure function of two snapshots.
    pub fn from_families(
        families: &[crate::manager::telemetry::FamilySnapshot],
        store: StoreSnapshot,
    ) -> Self {
        use crate::manager::telemetry::SampleValue;

        let mut snapshot = SchedulerSnapshot::new();
        for family in families {
            // Declared even when empty: an existing-but-idle family and a
            // family this build does not have are different answers.
            snapshot.declare(family.name, family.label_names);
            for sample in &family.samples {
                let labels = sample.label_values.clone();
                match &sample.value {
                    SampleValue::Counter(value) => {
                        snapshot
                            .counters
                            .entry(family.name.to_string())
                            .or_default()
                            .push((labels, *value));
                    }
                    SampleValue::Gauge(value) => {
                        snapshot
                            .gauges
                            .entry(family.name.to_string())
                            .or_default()
                            .push((labels, *value as f64));
                    }
                    SampleValue::GaugeF64(value) => {
                        snapshot
                            .gauges
                            .entry(family.name.to_string())
                            .or_default()
                            .push((labels, *value));
                    }
                    SampleValue::Histogram {
                        bounds,
                        bucket_counts,
                        count,
                        sum,
                    } => {
                        snapshot
                            .histograms
                            .entry(family.name.to_string())
                            .or_default()
                            .push((
                                labels,
                                HistogramSample {
                                    bounds: bounds.to_vec(),
                                    bucket_counts: bucket_counts.clone(),
                                    count: *count,
                                    sum: *sum,
                                },
                            ));
                    }
                }
            }
        }
        snapshot.store = store;
        snapshot
    }

    fn index_of(&self, family: &str, label: &str) -> Option<usize> {
        self.label_names
            .get(family)?
            .iter()
            .position(|name| name == label)
    }

    /// Sum of a counter family's series whose `label` equals `value`; the
    /// whole family when `label` is empty.
    pub fn counter_where(&self, family: &str, label: &str, value: &str) -> u64 {
        let Some(series) = self.counters.get(family) else {
            return 0;
        };
        if label.is_empty() {
            return series.iter().map(|(_, v)| *v).sum();
        }
        let Some(idx) = self.index_of(family, label) else {
            return 0;
        };
        series
            .iter()
            .filter(|(labels, _)| labels.get(idx).map(String::as_str) == Some(value))
            .map(|(_, v)| *v)
            .sum()
    }

    /// A counter family broken out by one label: `{label value -> sum}`.
    pub fn counter_by(&self, family: &str, label: &str) -> BTreeMap<String, u64> {
        let mut out = BTreeMap::new();
        let (Some(series), Some(idx)) = (self.counters.get(family), self.index_of(family, label))
        else {
            return out;
        };
        for (labels, value) in series {
            let Some(key) = labels.get(idx) else { continue };
            *out.entry(key.clone()).or_insert(0) += value;
        }
        out
    }

    pub fn gauge_where(&self, family: &str, label: &str, value: &str) -> Option<f64> {
        let series = self.gauges.get(family)?;
        if label.is_empty() {
            return series.first().map(|(_, v)| *v);
        }
        let idx = self.index_of(family, label)?;
        series
            .iter()
            .find(|(labels, _)| labels.get(idx).map(String::as_str) == Some(value))
            .map(|(_, v)| *v)
    }

    pub fn gauge_sum(&self, family: &str) -> Option<f64> {
        let series = self.gauges.get(family)?;
        Some(series.iter().map(|(_, v)| *v).sum())
    }

    pub fn gauge_by(&self, family: &str, label: &str) -> BTreeMap<String, f64> {
        let mut out = BTreeMap::new();
        let (Some(series), Some(idx)) = (self.gauges.get(family), self.index_of(family, label))
        else {
            return out;
        };
        for (labels, value) in series {
            let Some(key) = labels.get(idx) else { continue };
            *out.entry(key.clone()).or_insert(0.0) += value;
        }
        out
    }

    pub fn histogram_where(
        &self,
        family: &str,
        label: &str,
        value: &str,
    ) -> Option<&HistogramSample> {
        let series = self.histograms.get(family)?;
        if label.is_empty() {
            return series.first().map(|(_, sample)| sample);
        }
        let idx = self.index_of(family, label)?;
        series
            .iter()
            .find(|(labels, _)| labels.get(idx).map(String::as_str) == Some(value))
            .map(|(_, sample)| sample)
    }

    /// Every series of a histogram family, keyed by one label's value.
    pub fn histogram_by(&self, family: &str, label: &str) -> BTreeMap<String, HistogramSample> {
        let mut out = BTreeMap::new();
        let (Some(series), Some(idx)) = (self.histograms.get(family), self.index_of(family, label))
        else {
            return out;
        };
        for (labels, sample) in series {
            let Some(key) = labels.get(idx) else { continue };
            out.insert(key.clone(), sample.clone());
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Inputs the report needs beyond the two snapshots
// ---------------------------------------------------------------------------

/// The resolved manager knobs a remedy names, so "reduce `evalThreads`
/// (currently 16)" needs no second lookup. Same principle as the cadence
/// command computing `heartbeatSeconds` rather than printing the formula.
#[derive(Clone, Debug, PartialEq)]
pub struct SchedulerKnobs {
    /// `evalThreads` — scheduler worker count.
    pub eval_threads: u32,
    /// `schedulerSettings.plan_candidates` count. `0` is the kill switch.
    pub plan_candidates: u32,
    /// `persistence.raft.topology` (`single` / `lanes`), empty when the
    /// manager is not replicated.
    pub raft_topology: String,
    /// Whether the manager that answered leads the scheduler lane. Its
    /// scheduler counters are near zero when false, so the report says so
    /// instead of diagnosing an idle process.
    pub is_scheduler_lane_leader: bool,
}

impl Default for SchedulerKnobs {
    fn default() -> Self {
        SchedulerKnobs {
            eval_threads: 1,
            plan_candidates: 0,
            raft_topology: String::new(),
            is_scheduler_lane_leader: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// A check that passed, carrying the number that made it pass.
    Ok,
    Warning,
    Critical,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Ok => "ok",
            Severity::Warning => "warning",
            Severity::Critical => "critical",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Healthy,
    Degraded,
    Unhealthy,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Healthy => "healthy",
            Verdict::Degraded => "degraded",
            Verdict::Unhealthy => "unhealthy",
        }
    }
}

/// A cause this finding excluded, with the number that excluded it. The
/// point of the whole feature: the report carries the discriminator even for
/// the causes it ruled *out*, so the next person does not chase them.
#[derive(Clone, Debug, PartialEq)]
pub struct RuledOut {
    pub cause: &'static str,
    pub evidence: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionKind {
    /// Change a manager-config field.
    Config,
    /// Add hardware. No knob applies.
    Capacity,
    /// Go read something specific — another command, a table.
    Investigate,
}

impl ActionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActionKind::Config => "config",
            ActionKind::Capacity => "capacity",
            ActionKind::Investigate => "investigate",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Decrease,
    Increase,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Decrease => "decrease",
            Direction::Increase => "increase",
        }
    }
}

/// What to do about a finding. Names the field and the direction — an action
/// a human can check before it lands and an agent can propose without
/// inventing one.
#[derive(Clone, Debug, PartialEq)]
pub struct Action {
    pub kind: ActionKind,
    pub field: Option<String>,
    pub current: Option<String>,
    pub direction: Option<Direction>,
    pub suggested: Option<String>,
    pub text: String,
}

impl Action {
    fn investigate(text: impl Into<String>) -> Self {
        Action {
            kind: ActionKind::Investigate,
            field: None,
            current: None,
            direction: None,
            suggested: None,
            text: text.into(),
        }
    }

    fn capacity(text: impl Into<String>) -> Self {
        Action {
            kind: ActionKind::Capacity,
            field: None,
            current: None,
            direction: None,
            suggested: None,
            text: text.into(),
        }
    }

    fn config(
        field: &str,
        current: impl ToString,
        direction: Direction,
        suggested: impl ToString,
        text: impl Into<String>,
    ) -> Self {
        Action {
            kind: ActionKind::Config,
            field: Some(field.to_string()),
            current: Some(current.to_string()),
            direction: Some(direction),
            suggested: Some(suggested.to_string()),
            text: text.into(),
        }
    }
}

/// One detector's verdict. `code` is a **stable API**: additive only, never
/// renamed, because runbooks and agents branch on it.
#[derive(Clone, Debug, PartialEq)]
pub struct Finding {
    pub code: &'static str,
    pub severity: Severity,
    /// One line, the headline number.
    pub title: String,
    /// The sentences under it: what it means and what it costs.
    pub detail: String,
    /// Ordered so the rendering matches the reasoning.
    pub evidence: Vec<(String, String)>,
    pub ruled_out: Vec<RuledOut>,
    pub action: Option<Action>,
    pub doc_anchor: String,
    /// Set on every scheduler finding when `heartbeat_ceiling` fired: the
    /// fleet not keeping up produces scheduler symptoms downstream, and
    /// tuning the scheduler first is the expensive wrong turn.
    pub secondary: bool,
}

impl Finding {
    fn new(code: &'static str, severity: Severity, anchor: &str) -> Self {
        Finding {
            code,
            severity,
            title: String::new(),
            detail: String::new(),
            evidence: Vec::new(),
            ruled_out: Vec::new(),
            action: None,
            doc_anchor: format!("{DOC}{anchor}"),
            secondary: false,
        }
    }

    fn severity(mut self, severity: Severity) -> Self {
        self.severity = severity;
        self
    }

    fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = detail.into();
        self
    }

    fn evidence(mut self, key: &str, value: impl ToString) -> Self {
        self.evidence.push((key.to_string(), value.to_string()));
        self
    }

    fn ruled_out(mut self, cause: &'static str, evidence: Vec<(String, String)>) -> Self {
        self.ruled_out.push(RuledOut { cause, evidence });
        self
    }

    fn action(mut self, action: Action) -> Self {
        self.action = Some(action);
        self
    }

    pub fn is_failing(&self) -> bool {
        self.severity != Severity::Ok
    }
}

/// The whole report. `findings` are the failing checks, worst first;
/// `passed` are the checks that ran and were fine, each with the number that
/// made it fine.
#[derive(Clone, Debug, PartialEq)]
pub struct Report {
    pub verdict: Verdict,
    pub sample_seconds: u32,
    pub is_scheduler_lane_leader: bool,
    pub findings: Vec<Finding>,
    pub passed: Vec<Finding>,
}

// ---------------------------------------------------------------------------
// Small helpers shared by the detectors
// ---------------------------------------------------------------------------

fn ev(key: &str, value: impl ToString) -> (String, String) {
    (key.to_string(), value.to_string())
}

fn pct(share: f64) -> String {
    format!("{:.1}%", share * 100.0)
}

fn ms(seconds: f64) -> String {
    format!("{:.2}ms", seconds * 1000.0)
}

fn us(seconds: f64) -> String {
    format!("{:.0}us", seconds * 1_000_000.0)
}

/// `after - before`, saturating. A counter that went backwards means the
/// manager restarted mid-sample; zero is the honest reading, and a negative
/// rate would be a lie in the direction that hides problems.
fn counter_delta(before: &SchedulerSnapshot, after: &SchedulerSnapshot, family: &str) -> u64 {
    after
        .counter_where(family, "", "")
        .saturating_sub(before.counter_where(family, "", ""))
}

fn counter_delta_where(
    before: &SchedulerSnapshot,
    after: &SchedulerSnapshot,
    family: &str,
    label: &str,
    value: &str,
) -> u64 {
    after
        .counter_where(family, label, value)
        .saturating_sub(before.counter_where(family, label, value))
}

fn counter_delta_by(
    before: &SchedulerSnapshot,
    after: &SchedulerSnapshot,
    family: &str,
    label: &str,
) -> BTreeMap<String, u64> {
    let mut out = after.counter_by(family, label);
    let earlier = before.counter_by(family, label);
    for (key, value) in out.iter_mut() {
        *value = value.saturating_sub(earlier.get(key).copied().unwrap_or(0));
    }
    out.retain(|_, value| *value > 0);
    out
}

fn hist_delta(
    before: &SchedulerSnapshot,
    after: &SchedulerSnapshot,
    family: &str,
    label: &str,
    value: &str,
) -> Option<HistogramSample> {
    let later = after.histogram_where(family, label, value)?;
    Some(match before.histogram_where(family, label, value) {
        Some(earlier) => later.delta(earlier),
        None => later.clone(),
    })
}

fn hist_delta_by(
    before: &SchedulerSnapshot,
    after: &SchedulerSnapshot,
    family: &str,
    label: &str,
) -> BTreeMap<String, HistogramSample> {
    let earlier = before.histogram_by(family, label);
    after
        .histogram_by(family, label)
        .into_iter()
        .map(|(key, sample)| {
            let delta = match earlier.get(&key) {
                Some(before) => sample.delta(before),
                None => sample,
            };
            (key, delta)
        })
        .collect()
}

/// Store counts are levels, not counters, so a "how many happened during the
/// window" reading is the rise in the level. Rows pruned mid-window make
/// this an undercount, never an overcount — the safe direction for a
/// detector that fires on "> 0".
fn store_delta(
    before: &BTreeMap<String, i64>,
    after: &BTreeMap<String, i64>,
) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    for (kind, count) in after {
        let earlier = before.get(kind).copied().unwrap_or(0);
        let delta = count - earlier;
        if delta > 0 {
            out.insert(kind.clone(), delta);
        }
    }
    out
}

fn render_mix<V: std::fmt::Display>(mix: &BTreeMap<String, V>) -> String {
    if mix.is_empty() {
        return "none".to_string();
    }
    mix.iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// The entry point
// ---------------------------------------------------------------------------

/// Diagnose the scheduler from two snapshots and the manager's knobs.
///
/// Pure: no clock, no I/O, no globals. `before` is the earlier snapshot;
/// pass a default-constructed one with `sample_seconds = 0` to read
/// since-boot values instead of rates.
///
/// `heartbeat_ceiling` is evaluated first on purpose. A fleet that cannot
/// sustain its cadence generates scheduler load (reschedule evals, plan
/// churn) that looks exactly like a scheduler problem, so when it fires
/// every other finding is marked `secondary` rather than presented as a
/// peer.
pub fn diagnose(
    before: &SchedulerSnapshot,
    after: &SchedulerSnapshot,
    knobs: &SchedulerKnobs,
    sample_seconds: u32,
) -> Report {
    let window = sample_seconds as f64;
    let mut findings = Vec::new();
    let mut passed = Vec::new();

    let push = |finding: Finding, findings: &mut Vec<Finding>, passed: &mut Vec<Finding>| {
        if finding.is_failing() {
            findings.push(finding);
        } else {
            passed.push(finding);
        }
    };

    let heartbeat = heartbeat_ceiling(before, after);
    let ceiling_fired = heartbeat.is_failing();
    push(heartbeat, &mut findings, &mut passed);

    for detector in [
        plan_rejection_storm(before, after, knobs, window),
        scheduler_lane_queued(before, after, knobs),
        phase_growth(before, after),
        eval_starvation(before, after),
        invisible_eval_failures(before, after),
        capacity_exhausted(before, after),
    ] {
        push(detector, &mut findings, &mut passed);
    }

    if ceiling_fired {
        for finding in findings.iter_mut() {
            if finding.code != HEARTBEAT_CEILING {
                finding.secondary = true;
            }
        }
    }

    // Worst first, then by the order the detectors ran, which is the order
    // the doc walks the symptoms in. `heartbeat_ceiling` is pinned to the
    // top whatever its severity: a fleet that cannot sustain its cadence
    // makes the scheduler findings below it downstream symptoms, and a
    // critical-looking rejection storm printed above a "warning" cadence
    // problem sends the reader straight back to the wrong subsystem.
    findings.sort_by(|a, b| {
        (a.code != HEARTBEAT_CEILING)
            .cmp(&(b.code != HEARTBEAT_CEILING))
            .then(b.severity.cmp(&a.severity))
    });

    let verdict = if findings.iter().any(|f| f.severity == Severity::Critical) {
        Verdict::Unhealthy
    } else if findings.is_empty() {
        Verdict::Healthy
    } else {
        Verdict::Degraded
    };

    Report {
        verdict,
        sample_seconds,
        is_scheduler_lane_leader: knobs.is_scheduler_lane_leader,
        findings,
        passed,
    }
}

// ---------------------------------------------------------------------------
// Detectors
// ---------------------------------------------------------------------------

pub const HEARTBEAT_CEILING: &str = "heartbeat_ceiling";
pub const PLAN_REJECTION_STORM: &str = "plan_rejection_storm";
pub const SCHEDULER_LANE_QUEUED: &str = "scheduler_lane_queued";
pub const PHASE_GROWTH: &str = "phase_growth";
pub const EVAL_STARVATION: &str = "eval_starvation";
pub const INVISIBLE_EVAL_FAILURES: &str = "invisible_eval_failures";
pub const CAPACITY_EXHAUSTED: &str = "capacity_exhausted";

/// Every finding code this build can emit, in evaluation order. The CLI and
/// the docs both enumerate from here so a new detector cannot ship
/// undocumented.
pub const FINDING_CODES: &[&str] = &[
    HEARTBEAT_CEILING,
    PLAN_REJECTION_STORM,
    SCHEDULER_LANE_QUEUED,
    PHASE_GROWTH,
    EVAL_STARVATION,
    INVISIBLE_EVAL_FAILURES,
    CAPACITY_EXHAUSTED,
];

/// Is the fleet's heartbeat cadence, rather than the scheduler, the ceiling?
///
/// The reason this is a scheduler detector at all: a previous campaign
/// published 0.9 placements/sec as a scheduler throughput number when it was
/// the heartbeat ceiling, and the discriminator was in the same scrape.
fn heartbeat_ceiling(before: &SchedulerSnapshot, after: &SchedulerSnapshot) -> Finding {
    let anchor = "#symptom-slow-placements-and-nodes-flapping-lost";
    let active = after
        .gauge_where(M_NODES, "status", "active")
        .or_else(|| after.gauge_sum(M_NODES))
        .unwrap_or(0.0);
    let served = after.gauge_where(M_SERVED_INTERVAL, "", "").unwrap_or(0.0);
    let lost_window = after.gauge_where(M_LOST_WINDOW, "", "").unwrap_or(0.0);
    let freshness = after.gauge_where(M_FRESHNESS, "", "");

    // Below the minimum fleet the ratio is noise, and on a manager that does
    // not own liveness it is not published at all. Both are "cannot say",
    // which is reported as a pass carrying the reason rather than a silent
    // omission.
    let Some(freshness) = freshness.filter(|_| active >= FRESHNESS_MIN_FLEET) else {
        return Finding::new(HEARTBEAT_CEILING, Severity::Ok, anchor)
            .title("heartbeat freshness not measurable")
            .detail(format!(
                "Fleet is {active:.0} nodes; the ratio is noise below \
                 {FRESHNESS_MIN_FLEET:.0} nodes, and is not published at all by a \
                 manager that does not own liveness."
            ))
            .evidence("activeNodes", format!("{active:.0}"))
            .evidence("freshnessMeasurable", false);
    };

    let false_loss_below = if lost_window > 0.0 {
        served / lost_window
    } else {
        0.0
    };

    let severity = if freshness < false_loss_below {
        Severity::Critical
    } else if freshness < FRESHNESS_FALLING_BEHIND {
        Severity::Warning
    } else {
        Severity::Ok
    };

    // `marked_lost` alone cannot distinguish a fleet that shrank from a
    // fleet that is flapping; the recovery counter next to it can. Both are
    // new families, so a manager that has lost nothing yet reports zeros
    // rather than nothing — "the counter does not exist" and "it never
    // happened" being indistinguishable is a trap this repo has been bitten
    // by before.
    let marked_lost = counter_delta(before, after, M_NODES_LOST);
    let recovered = counter_delta(before, after, M_NODES_RECOVERED);

    let base = Finding::new(HEARTBEAT_CEILING, severity, anchor)
        .evidence("freshnessRatio", format!("{freshness:.2}"))
        .evidence("nodesMarkedLost", marked_lost)
        .evidence("nodesRecovered", recovered)
        .evidence("falseLossBelow", format!("{false_loss_below:.2}"))
        .evidence("servedIntervalSeconds", format!("{served:.0}"))
        .evidence("lostWindowSeconds", format!("{lost_window:.0}"))
        .evidence("activeNodes", format!("{active:.0}"));

    if severity == Severity::Ok {
        return base
            .title(format!(
                "freshness {freshness:.2} (false loss below {false_loss_below:.2})"
            ))
            .detail(
                "The fleet is sustaining its cadence, so a slow scheduler is a slow \
                 scheduler and not a liveness artifact."
                    .to_string(),
            );
    }

    base.title(format!(
        "heartbeat freshness {freshness:.2}, below the {false_loss_below:.2} false-loss line"
    ))
    .detail(
        "The fleet is not keeping up with its own heartbeat cadence. Nodes are being \
         marked lost that are alive, each loss enqueues a reschedule eval, and the \
         scheduler load you are looking at is downstream of that. Fix cadence first."
            .to_string(),
    )
    .ruled_out(
        "scheduler_is_the_ceiling",
        vec![
            ev("freshnessRatio", format!("{freshness:.2}")),
            ev("falseLossBelow", format!("{false_loss_below:.2}")),
        ],
    )
    .ruled_out(
        "fleet_actually_shrank",
        vec![
            ev("nodesMarkedLost", marked_lost),
            ev("nodesRecovered", recovered),
        ],
    )
    .action(Action::investigate(
        "Run `legion node cadence` — it computes the `heartbeatSeconds` to set for a \
         fleet this size on this control plane. It remains the authority on liveness.",
    ))
}

/// Are plans being thrown away faster than they are being applied?
///
/// The signature that started this: 73.6% rejected, and the correct remedy
/// was to *reduce* concurrency, which is the opposite of every operator
/// instinct.
fn plan_rejection_storm(
    before: &SchedulerSnapshot,
    after: &SchedulerSnapshot,
    knobs: &SchedulerKnobs,
    window: f64,
) -> Finding {
    let anchor = "#symptom-throughput-fell-when-i-scaled-up";
    let applied = counter_delta_where(before, after, M_PLAN_APPLY, "outcome", "applied");
    let rejected = counter_delta_where(before, after, M_PLAN_APPLY, "outcome", "rejected");
    let fallback = counter_delta_where(before, after, M_PLAN_APPLY, "outcome", "fallback");
    let total = applied + rejected + fallback;

    if total == 0 {
        return Finding::new(PLAN_REJECTION_STORM, Severity::Ok, anchor)
            .title("no plans applied in the window")
            .detail(
                "Nothing to reject. An idle scheduler is not a healthy one — check \
                 `eval_starvation` and `capacity_exhausted` below before concluding the \
                 cluster is fine."
                    .to_string(),
            )
            .evidence("planApplyTotal", 0);
    }

    let share = rejected as f64 / total as f64;
    let severity = if share > REJECTION_CRITICAL_SHARE {
        Severity::Critical
    } else if share > REJECTION_WARN_SHARE {
        Severity::Warning
    } else {
        Severity::Ok
    };

    let rank = hist_delta(before, after, M_CANDIDATE_RANK, "", "");
    let rank_p50 = rank.as_ref().and_then(|h| h.quantile(0.5));
    let index_candidates = hist_delta_by(before, after, M_INDEX_CANDIDATES, "index");
    let index_mean = index_candidates
        .values()
        .filter_map(|h| h.mean())
        .fold(None::<f64>, |acc, mean| {
            Some(acc.map_or(mean, |acc: f64| acc.min(mean)))
        });

    let mut finding = Finding::new(PLAN_REJECTION_STORM, severity, anchor)
        .evidence("rejectionShare", format!("{share:.3}"))
        .evidence("planApplyApplied", applied)
        .evidence("planApplyRejected", rejected)
        .evidence("planApplyFallback", fallback);

    if window > 0.0 {
        finding = finding
            .evidence(
                "appliedPerSecond",
                format!("{:.1}", applied as f64 / window),
            )
            .evidence(
                "rejectedPerSecond",
                format!("{:.1}", rejected as f64 / window),
            );
    }

    // The reason mix is what turns "reduce concurrency" from folklore into a
    // reading: contention and fleet churn produce the same rejection share
    // and take opposite remedies. Absent on a manager that predates the
    // family, in which case the advice stays the concurrency one and says so.
    let reasons = counter_delta_by(before, after, M_PLAN_REJECTED, "reason");
    let reason_total: u64 = reasons.values().sum();
    let churn: u64 = reasons
        .iter()
        .filter(|(reason, _)| {
            matches!(
                reason.as_str(),
                "node_unavailable" | "node_gone" | "capacity_shrank"
            )
        })
        .map(|(_, count)| *count)
        .sum();
    let churn_dominated = reason_total > 0 && churn as f64 / reason_total as f64 > 0.5;
    if reason_total > 0 {
        finding = finding.evidence("rejectionReasons", render_mix(&reasons));
    }

    if severity == Severity::Ok {
        return finding
            .title(format!("{} of plans rejected", pct(share)))
            .detail(format!(
                "{applied} applied against {rejected} rejected over the window. Plan \
                 contention is normal; it is the share, not the count, that matters."
            ));
    }

    let mut finding = finding
        .title(format!(
            "{} of plans rejected ({rejected} rejected / {total} total)",
            pct(share)
        ))
        .detail(
            "Each rejection is a discarded plan plus a full eval retry: the work is done \
             twice and placed once. Past this share the control plane is spending more on \
             losing races than on placing work."
                .to_string(),
        );

    // Ruled out, in the order an operator would otherwise chase them.
    finding = finding.ruled_out(
        "candidate_lists_disabled",
        vec![ev("planCandidates", knobs.plan_candidates)],
    );
    if let Some(p50) = rank_p50 {
        finding = finding.ruled_out(
            "pool_full",
            vec![ev("candidateRankP50", format!("{p50:.1}"))],
        );
    }
    if let Some(mean) = index_mean {
        finding = finding.ruled_out(
            "index_starved",
            vec![ev("indexCandidatesPerTaskMin", format!("{mean:.0}"))],
        );
    }

    let suggested = (knobs.eval_threads / 2).max(1);
    if churn_dominated {
        finding.detail = format!(
            "{} Rejections are dominated by fleet churn ({}), not by concurrency — \
             nodes went away between the plan and its apply. Reducing `evalThreads` \
             will not help here.",
            finding.detail,
            render_mix(&reasons)
        );
        finding.action(Action::investigate(format!(
            "Investigate node churn: {churn} of {reason_total} rejections cite a node that \
             was gone, cordoned, draining, or shrunk. `legion node cadence` and \
             `legion_nodes_marked_lost_total` against `legion_nodes_recovered_total` say \
             whether the fleet is flapping."
        )))
    } else {
        finding.action(Action::config(
            "evalThreads",
            knobs.eval_threads,
            Direction::Decrease,
            suggested,
            format!(
                "Reduce evalThreads from {} to {suggested} and re-measure. Fewer planners \
                 racing means fewer discarded plans, and measured throughput goes up, not \
                 down.",
                knobs.eval_threads
            ),
        ))
    }
}

/// Is one raft lane queueing behind itself while the box is idle?
///
/// The comparison no single metric expresses: `pipeline_wait` on the
/// scheduler lane against the quietest lane, with `propose_apply` flat
/// across both to prove commit cost is not the difference.
fn scheduler_lane_queued(
    before: &SchedulerSnapshot,
    after: &SchedulerSnapshot,
    knobs: &SchedulerKnobs,
) -> Finding {
    let anchor = "#symptom-the-box-is-idle-and-throughput-is-still-capped";
    let waits = hist_delta_by(before, after, M_PIPELINE_WAIT, "lane");
    let applies = hist_delta_by(before, after, M_PROPOSE_APPLY, "lane");

    let scheduler_wait = waits
        .get("scheduler")
        .and_then(|h| h.quantile(0.99))
        .filter(|_| waits.get("scheduler").is_some_and(|h| h.count > 0));

    let quietest = waits
        .iter()
        .filter(|(lane, sample)| lane.as_str() != "scheduler" && sample.count > 0)
        .filter_map(|(lane, sample)| sample.quantile(0.99).map(|p99| (lane.clone(), p99)))
        .min_by(|a, b| a.1.total_cmp(&b.1));

    let spawn = hist_delta(before, after, M_SPAWN_DELAY, "", "");
    let spawn_p99 = spawn.as_ref().and_then(|h| h.quantile(0.99));
    let worker_threads = after.gauge_where(M_WORKER_THREADS, "", "").unwrap_or(0.0);
    let in_flight = after.gauge_sum(M_GRPC_IN_FLIGHT).unwrap_or(0.0);

    let (Some(scheduler_wait), Some((quiet_lane, quiet_wait))) = (scheduler_wait, quietest) else {
        return Finding::new(SCHEDULER_LANE_QUEUED, Severity::Ok, anchor)
            .title("no per-lane replication to compare")
            .detail(format!(
                "This manager is not running two or more busy raft lanes \
                 (topology {}), so there is no lane comparison to make. On a \
                 single-lane or non-replicated manager the scheduler's commit cost \
                 shows up in `phase_growth`'s `apply` phase instead.",
                if knobs.raft_topology.is_empty() {
                    "not replicated"
                } else {
                    knobs.raft_topology.as_str()
                }
            ))
            .evidence("raftTopology", &knobs.raft_topology)
            .evidence("lanesObserved", waits.len());
    };

    let scheduler_apply = applies.get("scheduler").and_then(|h| h.mean());
    let quiet_apply = applies.get(&quiet_lane).and_then(|h| h.mean());
    let apply_flat = match (scheduler_apply, quiet_apply) {
        (Some(a), Some(b)) if a > 0.0 && b > 0.0 => {
            let ratio = if a > b { a / b } else { b / a };
            ratio <= PROPOSE_APPLY_FLAT_RATIO
        }
        // Without both sides the flatness test cannot run. Requiring it
        // would silently disable the detector, so it defaults to "flat" and
        // the missing number is reported rather than assumed.
        _ => true,
    };

    let ratio = if quiet_wait > 0.0 {
        scheduler_wait / quiet_wait
    } else {
        f64::INFINITY
    };
    let fires = scheduler_wait > LANE_WAIT_FLOOR_SECONDS && ratio > LANE_WAIT_RATIO && apply_flat;

    let batch = hist_delta(before, after, M_COALESCE_BATCH, "path", "plan_apply");
    let alone_share = batch.as_ref().and_then(|h| h.share_at_or_below(1.0));

    let mut finding = Finding::new(
        SCHEDULER_LANE_QUEUED,
        if fires {
            Severity::Warning
        } else {
            Severity::Ok
        },
        anchor,
    )
    .evidence("pipelineWaitSchedulerP99", ms(scheduler_wait))
    .evidence(
        format!("pipelineWait{quiet_lane}P99").as_str(),
        ms(quiet_wait),
    );
    if let Some(mean) = scheduler_apply {
        finding = finding.evidence("proposeApplySchedulerMean", ms(mean));
    }
    if let Some(mean) = quiet_apply {
        finding = finding.evidence(format!("proposeApply{quiet_lane}Mean").as_str(), ms(mean));
    }
    if let Some(share) = alone_share {
        finding = finding.evidence("planApplyCommitsAloneShare", pct(share));
    }

    if !fires {
        return finding
            .title(format!(
                "scheduler lane wait {} against {} on {quiet_lane}",
                ms(scheduler_wait),
                ms(quiet_wait)
            ))
            .detail(
                "The scheduler lane is not queueing behind itself: either its wait is \
                 below the noise floor or every lane is equally busy, which is a \
                 whole-control-plane reading and not a lane one."
                    .to_string(),
            );
    }

    let mut finding = finding
        .title(format!(
            "scheduler lane queueing: {} wait against {} on {quiet_lane}",
            ms(scheduler_wait),
            ms(quiet_wait)
        ))
        .detail(
            "Proposals are waiting to enter the pipeline, not waiting to commit — commit \
             cost is flat across lanes. The scheduler lane is submitting faster than it \
             can pipeline, which is a concurrency shape, not a capacity one."
                .to_string(),
        );

    let mut cpu_evidence = vec![ev("workerThreads", format!("{worker_threads:.0}"))];
    if let Some(p99) = spawn_p99 {
        cpu_evidence.insert(0, ev("spawnDelayP99", us(p99)));
    }
    cpu_evidence.push(ev("grpcInFlight", format!("{in_flight:.0}")));
    // Only claim "not CPU-bound" when the runtime actually says so. Without
    // a spawn-delay reading the claim is unsupported, and an unsupported
    // ruled-out line is exactly what this feature exists to stop.
    if spawn_p99.is_none_or(|p99| p99 < SPAWN_DELAY_STARVED_SECONDS) {
        finding = finding.ruled_out("cpu_bound", cpu_evidence);
        finding.detail = format!("{} A larger instance will not move this.", finding.detail);
    }
    if let Some(mean) = scheduler_apply {
        finding = finding.ruled_out(
            "commit_cost",
            vec![
                ev("proposeApplySchedulerMean", ms(mean)),
                ev(
                    "proposeApplyQuietestMean",
                    quiet_apply.map(ms).unwrap_or_else(|| "n/a".to_string()),
                ),
            ],
        );
    }

    let suggested = (knobs.eval_threads / 2).max(1);
    finding.action(Action::config(
        "evalThreads",
        knobs.eval_threads,
        Direction::Decrease,
        suggested,
        format!(
            "Reduce evalThreads from {} to {suggested} so fewer proposals per second reach \
             the scheduler lane, and confirm `persistence.raft.topology` is \"lanes\" \
             (currently {}) so the scheduler is not sharing a pipeline with status \
             traffic.",
            knobs.eval_threads,
            if knobs.raft_topology.is_empty() {
                "not replicated"
            } else {
                knobs.raft_topology.as_str()
            }
        ),
    ))
}

/// Where a plan's wall clock goes, phase by phase.
fn phase_growth(before: &SchedulerSnapshot, after: &SchedulerSnapshot) -> Finding {
    let anchor = "#symptom-it-got-slower-as-the-cluster-grew";
    let phases = hist_delta_by(before, after, M_PLAN_PHASE, "phase");
    let total = phases.get("total").and_then(|h| h.mean());

    let Some(total) = total.filter(|total| *total > 0.0) else {
        return Finding::new(PHASE_GROWTH, Severity::Ok, anchor)
            .title("no plans timed in the window")
            .detail(
                "`legion_scheduler_plan_phase_seconds` recorded nothing, so there is no \
                 phase mix to read. On an idle cluster this is expected."
                    .to_string(),
            )
            .evidence("plansTimed", phases.get("total").map_or(0, |h| h.count));
    };

    let mut mix: Vec<(String, f64)> = phases
        .iter()
        .filter(|(phase, _)| phase.as_str() != "total")
        .filter_map(|(phase, sample)| sample.mean().map(|mean| (phase.clone(), mean)))
        .collect();
    mix.sort_by(|a, b| b.1.total_cmp(&a.1));

    let accounted: f64 = mix.iter().map(|(_, mean)| *mean).sum();
    let unaccounted = (total - accounted).max(0.0);
    let unaccounted_share = unaccounted / total;
    let dominant = mix.first().cloned();

    let mut finding = Finding::new(PHASE_GROWTH, Severity::Ok, anchor)
        .evidence("planTotalMean", ms(total))
        .evidence("plansTimed", phases.get("total").map_or(0, |h| h.count));
    for (phase, mean) in &mix {
        finding = finding.evidence(
            format!("phase.{phase}").as_str(),
            format!("{} ({})", ms(*mean), pct(mean / total)),
        );
    }
    finding = finding.evidence(
        "phase.unaccounted",
        format!("{} ({})", ms(unaccounted), pct(unaccounted_share)),
    );

    let dominant_fires = dominant
        .as_ref()
        .is_some_and(|(_, mean)| mean / total > PHASE_DOMINANCE_SHARE);
    let unaccounted_fires = unaccounted_share > PHASE_UNACCOUNTED_SHARE;

    if !dominant_fires && !unaccounted_fires {
        let headline = dominant
            .as_ref()
            .map(|(phase, mean)| format!("{phase} {} of {} total", ms(*mean), ms(total)))
            .unwrap_or_else(|| format!("total {}", ms(total)));
        return finding.title(headline).detail(
            "No single phase dominates and the phases account for the total, so plan \
                 cost is spread the way it should be."
                .to_string(),
        );
    }

    if unaccounted_fires && !dominant_fires {
        return finding
            .severity(Severity::Warning)
            .title(format!(
                "{} of plan time is outside any timed phase",
                pct(unaccounted_share)
            ))
            .detail(format!(
                "Phases sum to {} of a {} total. The gap is time spent waiting for the \
                 store write lock or blocked behind another plan — contention outside the \
                 planner, not planning cost.",
                ms(accounted),
                ms(total)
            ))
            .ruled_out(
                "planning_cost",
                mix.iter()
                    .map(|(phase, mean)| ev(phase, ms(*mean)))
                    .collect(),
            )
            .action(Action::investigate(
                "Read `legion_store_write_lock_wait_seconds` and \
                 `legion_store_coalesce_wait_seconds`: the time is being spent before the \
                 planner runs, so no scheduler knob moves it.",
            ));
    }

    let (phase, mean) = dominant.expect("dominant_fires implies a dominant phase");
    let share = mean / total;
    let action = phase_remedy(&phase);
    finding
        .severity(Severity::Warning)
        .title(format!(
            "{phase} holds {} of plan time ({} of {})",
            pct(share),
            ms(mean),
            ms(total)
        ))
        .detail(format!(
            "One phase owns the plan. {}",
            phase_explanation(&phase)
        ))
        .ruled_out(
            "other_phases",
            mix.iter()
                .filter(|(name, _)| name != &phase)
                .map(|(name, mean)| ev(name, ms(*mean)))
                .collect(),
        )
        .action(action)
}

fn phase_explanation(phase: &str) -> &'static str {
    match phase {
        "available_nodes" => {
            "The node snapshot itself is the cost: the planner is copying the fleet on \
             every plan."
        }
        "version_sample" => {
            "Optimistic-concurrency epoch sampling dominates, which grows with the \
             candidate set rather than with the work."
        }
        "task_fetch" => {
            "Task-config loading dominates, which grows with the number of task configs \
             a plan touches rather than with fleet size."
        }
        "capacity_snapshot" => {
            "Rebuilding free capacity dominates, which grows with allocations per node."
        }
        "build_plan" => {
            "Candidate construction and scoring dominate — this is real planning cost and \
             scales with candidates considered per placement."
        }
        "apply" => {
            "The replicated write dominates: the plan is cheap and committing it is not. \
             This is a control-plane cost, not a planner one."
        }
        _ => "This phase dominates the plan.",
    }
}

fn phase_remedy(phase: &str) -> Action {
    match phase {
        "build_plan" => Action::config(
            "schedulerSettings.score_sample",
            "unset",
            Direction::Decrease,
            "64",
            "Bound candidate scoring with `schedulerSettings.score_sample` so placement \
             scores the best of N eligible nodes instead of every one of them.",
        ),
        "available_nodes" | "version_sample" => Action::config(
            "schedulerSettings.plan_candidates",
            "current",
            Direction::Decrease,
            "4",
            "Shrink `schedulerSettings.plan_candidates`: the epoch sample and the node \
             snapshot both scale with the candidate set, and a shorter list samples less.",
        ),
        "apply" => Action::investigate(
            "The cost is the replicated commit, not the plan. Check \
             `legion_raft_propose_apply_seconds` and `legion_raft_pipeline_wait_seconds` \
             per lane, and confirm `persistence.raft.topology` is \"lanes\".",
        ),
        "capacity_snapshot" => Action::investigate(
            "Free-capacity reconstruction scales with allocations per node. Check \
             allocation density before adding scheduler concurrency, which makes this \
             phase run more often rather than faster.",
        ),
        _ => Action::investigate(
            "Read the phase table in docs/diagnosing-scheduler-performance.md for this \
             phase's remedy.",
        ),
    }
}

/// Is one kind of work crowding everything else out of the queue?
fn eval_starvation(before: &SchedulerSnapshot, after: &SchedulerSnapshot) -> Finding {
    let anchor = "#symptom-new-work-waits-behind-a-backlog-of-other-work";
    let processed = counter_delta_by(before, after, M_EVALS_PROCESSED, "kind");
    let processed_total: u64 = processed.values().sum();

    // The eval table is the authority, not `legion_eval_queue_depth{kind}`.
    // The gauge is maintained by the writes this PROCESS performs, while
    // the queue is a replicated table: on a manager that is not leading the
    // scheduler lane the gauge is stale by construction. The labelled gauge
    // (phase 2) is what makes the depth readable on a dashboard; the
    // detector reads the table, and reports the gauge beside it so a
    // disagreement between them is visible rather than silently resolved.
    let depth_by_kind = after.store.pending_evals_by_kind.clone();
    let depth: i64 = depth_by_kind.values().sum();
    let gauge_depth: i64 = after
        .gauge_by(M_EVAL_QUEUE_DEPTH, "kind")
        .values()
        .sum::<f64>() as i64;

    let dominant = processed
        .iter()
        .max_by_key(|(_, count)| **count)
        .map(|(kind, count)| (kind.clone(), *count));
    let share = dominant
        .as_ref()
        .filter(|_| processed_total > 0)
        .map(|(_, count)| *count as f64 / processed_total as f64);

    let fires = matches!(share, Some(share) if share > STARVATION_KIND_SHARE)
        && depth > STARVATION_QUEUE_DEPTH;

    let mut finding = Finding::new(
        EVAL_STARVATION,
        if fires {
            Severity::Warning
        } else {
            Severity::Ok
        },
        anchor,
    )
    .evidence("queueDepth", depth)
    .evidence("queueDepthGauge", gauge_depth)
    .evidence("queueByKind", render_mix(&depth_by_kind))
    .evidence("processedByKind", render_mix(&processed));

    if !fires {
        let headline = match (&dominant, share) {
            (Some((kind, _)), Some(share)) => {
                format!("{kind} {} of processed evals", pct(share))
            }
            _ => format!("queue depth {depth}, nothing processed in the window"),
        };
        return finding.title(headline).detail(format!(
            "No kind is crowding the queue: depth {depth} against a \
             {STARVATION_QUEUE_DEPTH}-deep floor, and no kind holds more than \
             {}.",
            pct(STARVATION_KIND_SHARE)
        ));
    }

    let (kind, count) = dominant.expect("fires implies a dominant kind");
    let share = share.expect("fires implies a share");
    finding = finding
        .title(format!(
            "{kind} is {} of processed evals with {depth} queued",
            pct(share)
        ))
        .detail(format!(
            "{count} of {processed_total} evals processed in the window were {kind}. \
             Everything else in the queue is waiting behind them. Expect this shape after \
             node events: one lost node enqueues one reschedule eval per allocation it \
             held."
        ))
        .ruled_out(
            "queue_is_empty",
            vec![ev("queueDepth", depth), ev("floor", STARVATION_QUEUE_DEPTH)],
        )
        .ruled_out(
            "mixed_workload",
            vec![ev("processedByKind", render_mix(&processed))],
        );

    if kind == "node_lost_reschedule" {
        finding = finding.action(Action::investigate(
            "The queue is full of node-loss reschedules, so the fleet is losing nodes \
             faster than the scheduler drains the consequences. Run `legion node cadence` \
             first — this is usually a liveness symptom wearing a scheduler costume.",
        ));
    } else {
        finding = finding.action(Action::investigate(format!(
            "Raise `priority` on the work that matters: the queue is drained in priority \
             order, so higher-priority jobs overtake the {kind} backlog without waiting \
             for it to clear."
        )));
    }
    finding
}

/// Evals that terminally failed, which the counters historically did not say.
///
/// 264 evals ended `status: failed` in the eval table while
/// `evals_processed_total{status="error"}` never moved, and every affected
/// job reported `done`. A dashboard built on the counter showed health.
fn invisible_eval_failures(before: &SchedulerSnapshot, after: &SchedulerSnapshot) -> Finding {
    let anchor = "#symptom-nothing-is-running-and-nothing-reports-an-error";

    // Two independent readings, deliberately not collapsed into one.
    //
    // `legion_evals_failed_total{kind,reason}` (phase 2) is recorded inside
    // the store write that sets `status = 'failed'`, so no caller on THIS
    // process can fail an eval without counting it — but the counter is
    // per-process and the eval table is replicated, so a failure written by
    // another manager in the group never touches it. The rise in the table's
    // own failed-row count catches those. Whichever is larger is the
    // reading, and both appear in the evidence.
    let failed_metric: BTreeMap<String, i64> =
        counter_delta_by(before, after, M_EVALS_FAILED, "kind")
            .into_iter()
            .map(|(kind, count)| (kind, count as i64))
            .collect();
    let failed_rows = store_delta(
        &before.store.failed_evals_by_kind,
        &after.store.failed_evals_by_kind,
    );
    let metric_total: i64 = failed_metric.values().sum();
    let rows_total: i64 = failed_rows.values().sum();
    let (failed_by_kind, source) = if metric_total >= rows_total {
        (failed_metric, "counter")
    } else {
        (failed_rows, "eval table")
    };
    let failed = metric_total.max(rows_total);

    let processed_status = counter_delta_by(before, after, M_EVALS_PROCESSED, "status");
    let reasons = counter_delta_by(before, after, M_EVALS_FAILED, "reason");

    let mut finding = Finding::new(
        INVISIBLE_EVAL_FAILURES,
        if failed > 0 {
            Severity::Critical
        } else {
            Severity::Ok
        },
        anchor,
    )
    .evidence("failedEvals", failed)
    .evidence("failedEvalsSource", source)
    .evidence("failedEvalsCounter", metric_total)
    .evidence("failedEvalRowsAppeared", rows_total)
    .evidence("processedByStatus", render_mix(&processed_status));

    if failed == 0 {
        return finding.title("0 failed evals in the window").detail(
            "No eval reached a terminal failure. This is read from the population that \
             actually fails — not from `evals_processed_total{status}`, which counts \
             handler returns and includes retries."
                .to_string(),
        );
    }

    finding = finding
        .evidence("failedByKind", render_mix(&failed_by_kind))
        .title(format!("{failed} evals failed terminally in the window"))
        .detail(
            "A failed eval is never retried, so its work never happens. The job it \
             belonged to still reports its own lifecycle status — `done` is a lifecycle \
             flag, not a placement verdict — so nothing else in the product will tell you \
             about this."
                .to_string(),
        )
        .ruled_out(
            "counter_says_healthy",
            vec![ev("processedByStatus", render_mix(&processed_status))],
        );
    if !reasons.is_empty() {
        finding = finding.ruled_out(
            "unknown_cause",
            vec![ev("failedByReason", render_mix(&reasons))],
        );
    }
    finding.action(Action::investigate(
        "List them: `legion state query \"SELECT id, type, status, job_id FROM eval WHERE \
         status = 'failed' ORDER BY id DESC LIMIT 50\"` (or the saved query \
         `agent:recent-failed-evals`). Do not trust `legion job info`'s status field for \
         this.",
    ))
}

/// Is the cluster simply full? The most expensive wrong turn available is
/// tuning a scheduler that is placing correctly onto a fleet with no room:
/// every knob in the doc makes a full cluster slower, not faster.
fn capacity_exhausted(before: &SchedulerSnapshot, after: &SchedulerSnapshot) -> Finding {
    let anchor = "#symptom-the-cluster-is-full-and-no-knob-helps";
    let pools = &after.store.pools;
    let pending = after.store.pending_total();

    let total_nodes: u64 = pools.iter().map(|pool| pool.nodes).sum();
    let with_room: u64 = pools.iter().map(|pool| pool.nodes_with_free_capacity).sum();
    let mix = render_mix(
        &pools
            .iter()
            .map(|pool| {
                (
                    pool.pool.clone(),
                    format!(
                        "{}/{} nodes with room, {:.0} cpu / {:.0} MiB free",
                        pool.nodes_with_free_capacity,
                        pool.nodes,
                        pool.free_cpu,
                        pool.free_memory_mib
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    );

    // Phase 2's rejection reasons make this precise: a plan rejected because
    // the node's totals shrank or its recheck budget ran out is a capacity
    // story, not a contention one.
    let reasons = counter_delta_by(before, after, M_PLAN_REJECTED, "reason");
    let reason_total: u64 = reasons.values().sum();
    let capacity_reasons: u64 = reasons
        .iter()
        .filter(|(reason, _)| {
            matches!(
                reason.as_str(),
                "capacity_shrank" | "capacity_recheck_budget" | "late_constraint_failure"
            )
        })
        .map(|(_, count)| *count)
        .sum();
    let capacity_dominated =
        reason_total > 0 && capacity_reasons as f64 / reason_total as f64 > 0.5;

    let full = total_nodes > 0 && with_room == 0 && pending > 0;
    let fires = full || (capacity_dominated && pending > 0);

    let mut finding = Finding::new(
        CAPACITY_EXHAUSTED,
        if fires {
            Severity::Critical
        } else {
            Severity::Ok
        },
        anchor,
    )
    .evidence("nodesWithFreeCapacity", with_room)
    .evidence("availableNodes", total_nodes)
    .evidence("pendingEvals", pending)
    .evidence("freeCapacityByPool", mix);
    if reason_total > 0 {
        finding = finding.evidence("rejectionReasons", render_mix(&reasons));
    }

    if !fires {
        return finding
            .title(format!(
                "{with_room} of {total_nodes} available nodes still have room"
            ))
            .detail(
                "The fleet has capacity, so a placement that is not happening is not \
                 being blocked by a full cluster."
                    .to_string(),
            );
    }

    finding
        .title(format!(
            "no available node has room, with {pending} evals pending"
        ))
        .detail(
            "This is not a tuning problem. Every scheduler knob makes a genuinely full \
             cluster slower rather than faster: more concurrency means more plans racing \
             for the same absent capacity, and more candidates per plan means longer \
             walks that still find nothing."
                .to_string(),
        )
        .ruled_out(
            "scheduler_tuning",
            vec![
                ev("nodesWithFreeCapacity", with_room),
                ev("availableNodes", total_nodes),
            ],
        )
        .action(Action::capacity(
            "Add nodes, or free capacity by stopping work. No `evalThreads`, \
             `plan_candidates`, or topology change places work onto a fleet with no room.",
        ))
}

#[cfg(test)]
mod diagnose_test {
    use super::*;

    /// Hand the detectors a fleet that is comfortably keeping up, so the
    /// heartbeat check passes and does not mark everything secondary.
    fn healthy_fleet(snapshot: &mut SchedulerSnapshot) {
        snapshot
            .gauge(M_NODES, &[("status", "active")], 1000.0)
            .gauge(M_FRESHNESS, &[], 0.98)
            .gauge(M_SERVED_INTERVAL, &[], 2.0)
            .gauge(M_LOST_WINDOW, &[], 10.0);
    }

    fn knobs(eval_threads: u32) -> SchedulerKnobs {
        SchedulerKnobs {
            eval_threads,
            plan_candidates: 8,
            raft_topology: "lanes".to_string(),
            is_scheduler_lane_leader: true,
        }
    }

    fn find<'a>(report: &'a Report, code: &str) -> Option<&'a Finding> {
        report
            .findings
            .iter()
            .chain(report.passed.iter())
            .find(|finding| finding.code == code)
    }

    fn failing<'a>(report: &'a Report, code: &str) -> &'a Finding {
        report
            .findings
            .iter()
            .find(|finding| finding.code == code)
            .unwrap_or_else(|| panic!("expected {code} to fire; got {:?}", codes(report)))
    }

    fn codes(report: &Report) -> Vec<&str> {
        report.findings.iter().map(|f| f.code).collect()
    }

    /// A plan-apply counter pair, `before` then `after`.
    fn plan_apply(
        before: &mut SchedulerSnapshot,
        after: &mut SchedulerSnapshot,
        applied: u64,
        rejected: u64,
    ) {
        before
            .counter(M_PLAN_APPLY, &[("outcome", "applied")], 0)
            .counter(M_PLAN_APPLY, &[("outcome", "rejected")], 0);
        after
            .counter(M_PLAN_APPLY, &[("outcome", "applied")], applied)
            .counter(M_PLAN_APPLY, &[("outcome", "rejected")], rejected);
    }

    // -- Fixtures from the AWS scale campaign of 2026-08-02/03 -------------

    #[test]
    fn four_threads_at_15_9_percent_rejected_warns_and_says_decrease_test() {
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        // 15.9% of 3,912 plans.
        plan_apply(&mut before, &mut after, 3290, 622);
        after.histogram(
            M_CANDIDATE_RANK,
            &[],
            HistogramSample::from_buckets(&[(1.0, 600), (2.0, 20), (4.0, 2)], 0, 40.0),
        );

        let report = diagnose(&before, &after, &knobs(4), 30);
        let finding = failing(&report, PLAN_REJECTION_STORM);
        assert_eq!(finding.severity, Severity::Warning);
        let action = finding.action.as_ref().expect("an action");
        assert_eq!(action.field.as_deref(), Some("evalThreads"));
        assert_eq!(action.direction, Some(Direction::Decrease));
        assert_eq!(action.suggested.as_deref(), Some("2"));
        assert_eq!(report.verdict, Verdict::Degraded);
    }

    #[test]
    fn sixteen_threads_at_73_6_percent_rejected_is_critical_test() {
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        plan_apply(&mut before, &mut after, 9312, 25988);

        let report = diagnose(&before, &after, &knobs(16), 30);
        let finding = failing(&report, PLAN_REJECTION_STORM);
        assert_eq!(finding.severity, Severity::Critical);
        assert_eq!(report.verdict, Verdict::Unhealthy);
        assert_eq!(
            finding.action.as_ref().and_then(|a| a.suggested.clone()),
            Some("8".to_string())
        );
        // The discriminator is required, not decorative.
        assert!(finding
            .ruled_out
            .iter()
            .any(|ruled| ruled.cause == "candidate_lists_disabled"));
    }

    /// 29% CPU, `pipeline_wait` 3.07ms against 0.09ms, `propose_apply` 1.5ms
    /// flat on both lanes, spawn delay entirely under 50µs.
    #[test]
    fn idle_box_with_a_queued_scheduler_lane_names_the_lane_not_the_cpu_test() {
        let (before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        after
            .histogram(
                M_PIPELINE_WAIT,
                &[("lane", "scheduler")],
                HistogramSample::from_buckets(
                    &[(0.0005, 0), (0.001, 0), (0.0025, 10), (0.005, 990)],
                    0,
                    3.07,
                ),
            )
            .histogram(
                M_PIPELINE_WAIT,
                &[("lane", "status")],
                HistogramSample::from_buckets(&[(0.0005, 900), (0.001, 100)], 0, 0.09),
            )
            .histogram(
                M_PROPOSE_APPLY,
                &[("lane", "scheduler")],
                HistogramSample::from_buckets(&[(0.0025, 1000)], 0, 1.51),
            )
            .histogram(
                M_PROPOSE_APPLY,
                &[("lane", "status")],
                HistogramSample::from_buckets(&[(0.0025, 1000)], 0, 1.51),
            )
            .histogram(
                M_SPAWN_DELAY,
                &[],
                HistogramSample::from_buckets(&[(0.00005, 1000)], 0, 0.041),
            )
            .gauge(M_WORKER_THREADS, &[], 16.0)
            .gauge(M_GRPC_IN_FLIGHT, &[("method", "RunJob")], 3.0);

        let report = diagnose(&before, &after, &knobs(16), 30);
        let finding = failing(&report, SCHEDULER_LANE_QUEUED);
        assert!(
            finding.ruled_out.iter().any(|r| r.cause == "cpu_bound"),
            "the whole point is ruling the CPU out: {:?}",
            finding.ruled_out
        );
        assert!(finding
            .detail
            .contains("larger instance will not move this"));
        // And no CPU-shaped finding anywhere in the report.
        assert!(!codes(&report).iter().any(|code| code.contains("cpu")));
    }

    #[test]
    fn phase_mix_names_the_phase_that_grew_test() {
        let (before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        // task_fetch 19.8ms of a 30ms total: the phase that grew.
        for (phase, mean) in [
            ("available_nodes", 0.00964),
            ("task_fetch", 0.0198),
            ("build_plan", 0.00701),
            ("apply", 0.0),
        ] {
            after.histogram(
                M_PLAN_PHASE,
                &[("phase", phase)],
                HistogramSample::from_buckets(&[(1.0, 100)], 0, mean * 100.0),
            );
        }
        after.histogram(
            M_PLAN_PHASE,
            &[("phase", "total")],
            HistogramSample::from_buckets(&[(1.0, 100)], 0, 0.0365 * 100.0),
        );

        let report = diagnose(&before, &after, &knobs(4), 30);
        let finding = failing(&report, PHASE_GROWTH);
        assert!(
            finding.title.contains("task_fetch"),
            "expected task_fetch in {:?}",
            finding.title
        );
    }

    #[test]
    fn one_eval_kind_crowding_a_deep_queue_is_starvation_test() {
        let (before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        after
            .counter(
                M_EVALS_PROCESSED,
                &[("kind", "node_lost_reschedule"), ("status", "ok")],
                5760,
            )
            .counter(
                M_EVALS_PROCESSED,
                &[("kind", "job_submit"), ("status", "ok")],
                2503,
            );
        after.with_store(StoreSnapshot {
            pending_evals_by_kind: BTreeMap::from([
                ("node_lost_reschedule".to_string(), 700),
                ("job_submit".to_string(), 64),
            ]),
            ..StoreSnapshot::default()
        });

        let report = diagnose(&before, &after, &knobs(4), 30);
        let finding = failing(&report, EVAL_STARVATION);
        assert!(finding.title.contains("node_lost_reschedule"));
        assert!(finding
            .evidence
            .iter()
            .any(|(key, value)| key == "queueDepth" && value == "764"));
    }

    #[test]
    fn heartbeat_ceiling_fires_first_and_marks_the_rest_secondary_test() {
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        after
            .gauge(M_NODES, &[("status", "active")], 5000.0)
            .gauge(M_FRESHNESS, &[], 0.22)
            .gauge(M_SERVED_INTERVAL, &[], 2.0)
            .gauge(M_LOST_WINDOW, &[], 10.0);
        plan_apply(&mut before, &mut after, 1000, 900);

        let report = diagnose(&before, &after, &knobs(16), 30);
        assert_eq!(
            report.findings.first().map(|f| f.code),
            Some(HEARTBEAT_CEILING)
        );
        let storm = failing(&report, PLAN_REJECTION_STORM);
        assert!(
            storm.secondary,
            "scheduler findings must be marked secondary when the fleet is behind"
        );
        assert!(!failing(&report, HEARTBEAT_CEILING).secondary);
    }

    #[test]
    fn failed_evals_fire_even_when_the_processed_counter_never_moved_test() {
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        // `status="error"` at zero for the whole window — the dashboard's view.
        before.counter(
            M_EVALS_PROCESSED,
            &[("kind", "job_submit"), ("status", "ok")],
            1000,
        );
        after.counter(
            M_EVALS_PROCESSED,
            &[("kind", "job_submit"), ("status", "ok")],
            2000,
        );
        before.with_store(StoreSnapshot {
            failed_evals_by_kind: BTreeMap::from([("job_submit".to_string(), 0)]),
            ..StoreSnapshot::default()
        });
        after.with_store(StoreSnapshot {
            failed_evals_by_kind: BTreeMap::from([("job_submit".to_string(), 264)]),
            ..StoreSnapshot::default()
        });

        let report = diagnose(&before, &after, &knobs(4), 30);
        let finding = failing(&report, INVISIBLE_EVAL_FAILURES);
        assert_eq!(finding.severity, Severity::Critical);
        assert!(finding.title.contains("264"));
        assert_eq!(report.verdict, Verdict::Unhealthy);
    }

    #[test]
    fn healthy_snapshot_puts_every_check_in_passed_with_evidence_test() {
        let (before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        after
            .counter(M_PLAN_APPLY, &[("outcome", "applied")], 3000)
            .counter(M_PLAN_APPLY, &[("outcome", "rejected")], 20);
        after.with_store(StoreSnapshot {
            pools: vec![PoolFreeCapacity {
                pool: "default".to_string(),
                nodes: 10,
                nodes_with_free_capacity: 9,
                free_cpu: 40000.0,
                free_memory_mib: 65536.0,
            }],
            ..StoreSnapshot::default()
        });

        let report = diagnose(&before, &after, &knobs(4), 30);
        assert_eq!(report.verdict, Verdict::Healthy, "{:?}", codes(&report));
        for code in FINDING_CODES {
            let finding = find(&report, code).unwrap_or_else(|| panic!("{code} missing"));
            assert_eq!(finding.severity, Severity::Ok);
            assert!(
                !finding.evidence.is_empty(),
                "{code} passed without evidence, which is the failure mode this feature exists to fix"
            );
        }
    }

    // -- Invariants --------------------------------------------------------

    #[test]
    fn every_failing_finding_carries_a_discriminator_and_an_action_test() {
        // One snapshot that trips every detector at once.
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        after
            .gauge(M_NODES, &[("status", "active")], 5000.0)
            .gauge(M_FRESHNESS, &[], 0.10)
            .gauge(M_SERVED_INTERVAL, &[], 2.0)
            .gauge(M_LOST_WINDOW, &[], 10.0);
        plan_apply(&mut before, &mut after, 100, 900);
        after
            .histogram(
                M_PIPELINE_WAIT,
                &[("lane", "scheduler")],
                HistogramSample::from_buckets(&[(0.005, 1000)], 0, 3.07),
            )
            .histogram(
                M_PIPELINE_WAIT,
                &[("lane", "status")],
                HistogramSample::from_buckets(&[(0.0005, 1000)], 0, 0.09),
            )
            .histogram(
                M_SPAWN_DELAY,
                &[],
                HistogramSample::from_buckets(&[(0.00005, 1000)], 0, 0.041),
            )
            .histogram(
                M_PLAN_PHASE,
                &[("phase", "apply")],
                HistogramSample::from_buckets(&[(1.0, 100)], 0, 0.9),
            )
            .histogram(
                M_PLAN_PHASE,
                &[("phase", "total")],
                HistogramSample::from_buckets(&[(1.0, 100)], 0, 1.0),
            )
            .counter(
                M_EVALS_PROCESSED,
                &[("kind", "node_lost_reschedule"), ("status", "ok")],
                5000,
            );
        after.with_store(StoreSnapshot {
            pending_evals_by_kind: BTreeMap::from([("node_lost_reschedule".to_string(), 764)]),
            failed_evals_by_kind: BTreeMap::from([("job_submit".to_string(), 12)]),
            pools: vec![PoolFreeCapacity {
                pool: "default".to_string(),
                nodes: 5000,
                nodes_with_free_capacity: 0,
                free_cpu: 0.0,
                free_memory_mib: 0.0,
            }],
        });

        let report = diagnose(&before, &after, &knobs(16), 30);
        assert_eq!(
            report.findings.len(),
            FINDING_CODES.len(),
            "expected every detector to fire: {:?}",
            codes(&report)
        );
        for finding in &report.findings {
            assert!(
                !finding.ruled_out.is_empty(),
                "{} fired without naming what it excluded",
                finding.code
            );
            assert!(
                finding.action.is_some(),
                "{} fired without an action",
                finding.code
            );
            assert!(
                !finding.evidence.is_empty(),
                "{} fired without evidence",
                finding.code
            );
        }
    }

    #[test]
    fn findings_are_ordered_worst_first_test() {
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        plan_apply(&mut before, &mut after, 900, 100); // 10% — warning
        after.with_store(StoreSnapshot {
            failed_evals_by_kind: BTreeMap::from([("job_submit".to_string(), 3)]), // critical
            ..StoreSnapshot::default()
        });

        let report = diagnose(&before, &after, &knobs(8), 30);
        assert_eq!(report.findings[0].severity, Severity::Critical);
        assert_eq!(report.findings[0].code, INVISIBLE_EVAL_FAILURES);
        assert_eq!(report.findings[1].code, PLAN_REJECTION_STORM);
    }

    #[test]
    fn a_counter_that_went_backwards_produces_no_negative_rate_test() {
        // Manager restarted mid-sample: `after` is smaller than `before`.
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        before
            .counter(M_PLAN_APPLY, &[("outcome", "applied")], 100_000)
            .counter(M_PLAN_APPLY, &[("outcome", "rejected")], 90_000);
        after
            .counter(M_PLAN_APPLY, &[("outcome", "applied")], 5)
            .counter(M_PLAN_APPLY, &[("outcome", "rejected")], 1);

        let report = diagnose(&before, &after, &knobs(4), 30);
        let finding = find(&report, PLAN_REJECTION_STORM).expect("present");
        assert_eq!(finding.severity, Severity::Ok);
        for (_, value) in &finding.evidence {
            assert!(!value.starts_with('-'), "negative reading: {value}");
        }
    }

    #[test]
    fn sample_seconds_zero_reads_since_boot_and_omits_rates_test() {
        let mut after = SchedulerSnapshot::new();
        healthy_fleet(&mut after);
        after
            .counter(M_PLAN_APPLY, &[("outcome", "applied")], 100)
            .counter(M_PLAN_APPLY, &[("outcome", "rejected")], 900);

        let report = diagnose(&SchedulerSnapshot::new(), &after, &knobs(8), 0);
        assert_eq!(report.sample_seconds, 0);
        let finding = failing(&report, PLAN_REJECTION_STORM);
        assert!(
            !finding
                .evidence
                .iter()
                .any(|(key, _)| key == "appliedPerSecond"),
            "a rate needs a window"
        );
    }

    #[test]
    fn thresholds_are_exclusive_at_the_boundary_test() {
        // Exactly 20% rejected is a warning, not a critical.
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        plan_apply(&mut before, &mut after, 800, 200);
        let report = diagnose(&before, &after, &knobs(8), 30);
        assert_eq!(
            failing(&report, PLAN_REJECTION_STORM).severity,
            Severity::Warning
        );

        // Exactly 5% is a pass.
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        plan_apply(&mut before, &mut after, 950, 50);
        let report = diagnose(&before, &after, &knobs(8), 30);
        assert_eq!(
            find(&report, PLAN_REJECTION_STORM).map(|f| f.severity),
            Some(Severity::Ok)
        );
    }

    #[test]
    fn lane_detector_stays_quiet_when_both_lanes_are_quick_test() {
        let (before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        after
            .histogram(
                M_PIPELINE_WAIT,
                &[("lane", "scheduler")],
                // 20x the quiet lane, but well under the 1ms floor.
                HistogramSample::from_buckets(&[(0.0005, 1000)], 0, 0.4),
            )
            .histogram(
                M_PIPELINE_WAIT,
                &[("lane", "status")],
                HistogramSample::from_buckets(&[(0.0005, 1000)], 0, 0.02),
            );
        let report = diagnose(&before, &after, &knobs(8), 30);
        assert_eq!(
            find(&report, SCHEDULER_LANE_QUEUED).map(|f| f.severity),
            Some(Severity::Ok)
        );
    }

    #[test]
    fn a_full_cluster_says_no_knob_applies_test() {
        let (before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        after.with_store(StoreSnapshot {
            pending_evals_by_kind: BTreeMap::from([("job_submit".to_string(), 40)]),
            pools: vec![PoolFreeCapacity {
                pool: "default".to_string(),
                nodes: 200,
                nodes_with_free_capacity: 0,
                free_cpu: 0.0,
                free_memory_mib: 0.0,
            }],
            ..StoreSnapshot::default()
        });

        let report = diagnose(&before, &after, &knobs(8), 30);
        let finding = failing(&report, CAPACITY_EXHAUSTED);
        assert_eq!(
            finding.action.as_ref().map(|a| a.kind),
            Some(ActionKind::Capacity)
        );
        assert!(finding.detail.contains("not a tuning problem"));
    }

    #[test]
    fn fleet_churn_rejections_change_the_remedy_test() {
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        plan_apply(&mut before, &mut after, 1000, 900);
        after
            .counter(M_PLAN_REJECTED, &[("reason", "node_unavailable")], 800)
            .counter(M_PLAN_REJECTED, &[("reason", "node_epoch_moved")], 100);

        let report = diagnose(&before, &after, &knobs(16), 30);
        let finding = failing(&report, PLAN_REJECTION_STORM);
        let action = finding.action.as_ref().expect("an action");
        assert_eq!(action.kind, ActionKind::Investigate);
        assert!(action.text.contains("churn"));

        // The contention-dominated mix keeps the concurrency remedy.
        let (mut before, mut after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        healthy_fleet(&mut after);
        plan_apply(&mut before, &mut after, 1000, 900);
        after.counter(M_PLAN_REJECTED, &[("reason", "node_epoch_moved")], 880);
        let report = diagnose(&before, &after, &knobs(16), 30);
        let action = failing(&report, PLAN_REJECTION_STORM)
            .action
            .clone()
            .expect("an action");
        assert_eq!(action.kind, ActionKind::Config);
        assert_eq!(action.field.as_deref(), Some("evalThreads"));
    }

    #[test]
    fn quantile_interpolates_within_the_bucket_test() {
        let sample = HistogramSample::from_buckets(&[(0.001, 50), (0.01, 50)], 0, 0.0);
        assert_eq!(sample.quantile(0.5), Some(0.001));
        let p99 = sample.quantile(0.99).expect("a p99");
        assert!(p99 > 0.008 && p99 <= 0.01, "{p99}");
        assert_eq!(HistogramSample::default().quantile(0.5), None);
    }

    #[test]
    fn histogram_delta_saturates_on_a_restart_test() {
        let later = HistogramSample::from_buckets(&[(0.001, 5)], 0, 0.002);
        let earlier = HistogramSample::from_buckets(&[(0.001, 500)], 0, 0.9);
        let delta = later.delta(&earlier);
        assert_eq!(delta.count, later.count);
        assert!(delta.sum >= 0.0);
    }

    #[test]
    fn finding_codes_are_unique_and_complete_test() {
        let mut seen = std::collections::HashSet::new();
        for code in FINDING_CODES {
            assert!(seen.insert(*code), "duplicate finding code {code}");
        }
        let (before, after) = (SchedulerSnapshot::new(), SchedulerSnapshot::new());
        let report = diagnose(&before, &after, &SchedulerKnobs::default(), 30);
        let emitted: std::collections::HashSet<&str> = report
            .findings
            .iter()
            .chain(report.passed.iter())
            .map(|f| f.code)
            .collect();
        assert_eq!(emitted, seen, "FINDING_CODES must list exactly what runs");
    }
}
