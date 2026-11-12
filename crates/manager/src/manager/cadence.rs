//! Heartbeat cadence and the lost-node decision, as pure functions.
//!
//! Everything here is arithmetic over a snapshot: no store access, no clock
//! reads, no leadership checks. The status checker
//! ([`crate::manager::daemon::start_node_status_checker`]) collects the
//! snapshot and applies the verdict; this module decides what the verdict is,
//! which is what makes both plans testable without a cluster.
//!
//! Two plans meet here:
//!
//! - `internal_docs/plans/heartbeat-cadence-at-scale-plan.md` owns the
//!   cadence itself — the interval workers are served, the per-node lost
//!   window derived from it, the freshness ratio, and the sizing arithmetic.
//! - `internal_docs/plans/liveness-off-the-raft-path-plan.md` owns *where*
//!   liveness lives, and with it the failover grace clock and peer-gap
//!   suppression that guard the decision.
//!
//! The public page for both is `docs/heartbeat-cadence-and-scale.md` and
//! `docs/node-liveness.md`.

use crate::prelude::*;
pub use crate::structs::worker_protocol::clamp_heartbeat_seconds;
use std::collections::HashSet;
use std::time::Duration;

/// The rungs a manager-served interval may take, in seconds.
///
/// Roughly Fibonacci (~1.6x per rung — Consul's octave trick with finer
/// steps): a fleet has to grow ~60% to move a rung, so the per-node lost
/// window is not rewritten on almost every tick the way a continuous `N/R`
/// would rewrite it. An operator can memorize the ladder and predict what the
/// manager will serve, which a continuous function does not allow.
pub const CADENCE_LADDER: [u64; 10] = [2, 3, 5, 8, 13, 21, 34, 55, 89, 120];

/// How long a `target` below the current rung must persist before the served
/// interval steps DOWN. Stepping up is immediate (backing off is always safe);
/// stepping down narrows every node's window, so it waits.
pub const LADDER_STEP_DOWN_DWELL: Duration = Duration::from_secs(300);

/// Slack on the freshness comparison: the +-10% steady-state jitter plus a
/// beat's worth of slop. A node that beat within `1.2 x interval` is "on time".
pub const FRESHNESS_SLACK: f64 = 1.2;

/// One active node, as the lost decision sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLiveness {
    pub id: String,
    /// The node's pool, for the per-pool freshness breakdown.
    pub pool: String,
    /// How long since this manager last observed a beat (a ping or a full
    /// heartbeat) from the node.
    pub age: Duration,
    /// This node's own lost window: `missedBeatsBeforeLost x its interval`.
    pub window: Duration,
}

impl NodeLiveness {
    /// The liveness interval this node is on, recovered from its window:
    /// the window is `missed x interval`, so this is exact for every window
    /// the manager wrote and rounds a hand-made one down to whole seconds.
    pub fn interval(&self, missed_beats: u32) -> Duration {
        let seconds = self.window.as_secs() / u64::from(missed_beats.max(1));
        Duration::from_secs(seconds.max(1))
    }

    /// Whether the node beat within `FRESHNESS_SLACK` of its OWN interval.
    fn is_fresh(&self, missed_beats: u32) -> bool {
        self.age <= self.interval(missed_beats).mul_f64(FRESHNESS_SLACK)
    }
}

/// Why a would-be loss was not acted on. Every suppression is counted
/// (`legion_liveness_loss_suppressed_total{reason}`) because "nothing was
/// marked lost" must never be indistinguishable from "nothing was wrong".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SuppressionReason {
    /// Inside `liveness.failoverGrace` of becoming responsible for liveness.
    FailoverGrace,
    /// The peer manager that last claimed this node has gone quiet, so its
    /// nodes look silent to us even though they are beating to it.
    PeerDigestGap,
    /// More than `LIVENESS_MASS_DISRUPTION_FRACTION` of the fleet would go at
    /// once — the control plane broke, not the fleet.
    MassDisruption,
}

impl SuppressionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            SuppressionReason::FailoverGrace => "failover_grace",
            SuppressionReason::PeerDigestGap => "peer_digest_gap",
            SuppressionReason::MassDisruption => "mass_disruption",
        }
    }
}

/// Everything the decision needs beyond the node snapshot.
#[derive(Debug, Clone)]
pub struct LossContext {
    /// How long this manager has been responsible for liveness — process
    /// start OR status-lane leadership acquisition, whichever is LATER.
    pub responsible_for: Duration,
    /// Mark nothing lost inside this window after becoming responsible.
    pub failover_grace: Duration,
    /// Nodes last claimed by a peer manager whose digests have stopped.
    pub peer_gap_nodes: HashSet<String>,
    /// Fleet sizes below this are too small for the circuit breaker to mean
    /// anything (a 3-node cluster losing 2 nodes is not a control-plane bug).
    pub min_fleet_for_breaker: usize,
    /// Fraction of the fleet that, going at once, trips the breaker.
    pub disruption_fraction: f64,
}

impl Default for LossContext {
    fn default() -> Self {
        Self {
            responsible_for: Duration::MAX,
            failover_grace: Duration::from_secs(DEF_LIVENESS_FAILOVER_GRACE_SEC),
            peer_gap_nodes: HashSet::new(),
            min_fleet_for_breaker: HEARTBEAT_FRESHNESS_MIN_FLEET,
            disruption_fraction: LIVENESS_MASS_DISRUPTION_FRACTION,
        }
    }
}

/// The verdict: who to mark lost, and every reason something was held back.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LossDecision {
    pub lost: Vec<String>,
    /// `(reason, node count)`, sorted by reason. Feeds the suppression
    /// counter and the operator-facing log line.
    pub suppressed: Vec<(SuppressionReason, usize)>,
}

impl LossDecision {
    pub fn suppressed_count(&self, reason: SuppressionReason) -> usize {
        self.suppressed
            .iter()
            .find(|(r, _)| *r == reason)
            .map(|(_, n)| *n)
            .unwrap_or(0)
    }
}

/// Which active nodes to mark lost.
///
/// The order of the guards is the safety argument, and it is deliberate:
///
/// 1. **Failover grace.** A manager that just became responsible for liveness
///    has an empty (or freshly seeded) picture and must let the fleet check in
///    before it believes anything. This supersedes the old process-uptime
///    grace, which never fired on a mid-life status-lane leadership
///    acquisition (liveness plan §5.4).
/// 2. **Per-node windows.** A node is late against `missedBeatsBeforeLost x
///    its own interval`, never against a global constant — a 2s
///    latency-sensitive pool and a 60s batch pool cannot share a window.
/// 3. **Peer-gap suppression.** A manager-to-manager problem must not present
///    as fleet-wide node loss (liveness plan §6.3).
/// 4. **The mass-disruption circuit breaker.** A control plane does not lose
///    half its fleet in one tick; if it looks like it did, believe the control
///    plane broke (Kubernetes' `stateFullDisruption`, applied at detection
///    because Legion has no zone concept).
pub fn decide_lost_nodes(nodes: &[NodeLiveness], ctx: &LossContext) -> LossDecision {
    let candidates: Vec<&NodeLiveness> =
        nodes.iter().filter(|node| node.age > node.window).collect();
    if candidates.is_empty() {
        return LossDecision::default();
    }

    if ctx.responsible_for < ctx.failover_grace {
        return LossDecision {
            lost: Vec::new(),
            suppressed: vec![(SuppressionReason::FailoverGrace, candidates.len())],
        };
    }

    let mut suppressed = Vec::new();
    let (peer_gapped, mut eligible): (Vec<&NodeLiveness>, Vec<&NodeLiveness>) = candidates
        .into_iter()
        .partition(|node| ctx.peer_gap_nodes.contains(&node.id));
    if !peer_gapped.is_empty() {
        suppressed.push((SuppressionReason::PeerDigestGap, peer_gapped.len()));
    }

    let fleet = nodes.len();
    if fleet >= ctx.min_fleet_for_breaker
        && (eligible.len() as f64) > ctx.disruption_fraction * fleet as f64
    {
        suppressed.push((SuppressionReason::MassDisruption, eligible.len()));
        eligible = Vec::new();
    }

    suppressed.sort();
    LossDecision {
        lost: eligible.into_iter().map(|node| node.id.clone()).collect(),
        suppressed,
    }
}

/// Fraction of `nodes` that checked in within one interval, each node judged
/// against its OWN liveness interval.
///
/// Per node, not against one served interval: a 60s batch pool beside a 2s
/// latency pool is on time at 50s of silence, and judging it against 2s
/// would read a healthy fleet as falling behind. `None` below `min_fleet`
/// nodes: the ratio is pure noise on a handful of nodes and would fire the
/// warning on a laptop cluster. `1.0` means everyone is on time; false loss
/// begins at `interval / lost_window`, which at the defaults is
/// `2s / 10s = 0.20`.
pub fn freshness_ratio(nodes: &[NodeLiveness], missed_beats: u32, min_fleet: usize) -> Option<f64> {
    if nodes.is_empty() || nodes.len() < min_fleet {
        return None;
    }
    let fresh = nodes
        .iter()
        .filter(|node| node.is_fresh(missed_beats))
        .count();
    Some(fresh as f64 / nodes.len() as f64)
}

/// One pool's line in the cadence report.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolCadence {
    pub pool: String,
    pub active_nodes: usize,
    /// The median liveness interval of the pool's nodes.
    pub interval: Duration,
    pub lost_window: Duration,
    /// `None` on a pool below `min_fleet` nodes.
    pub freshness_ratio: Option<f64>,
}

/// The cadence report broken down by pool, sorted by pool name.
///
/// A fleet-wide ratio blends populations: a 5,000-node batch pool keeping up
/// can hide a 200-node latency pool that is falling behind, because the
/// blended figure stays above the warning line while every node in the small
/// pool is a few beats from being marked lost.
pub fn pool_cadence(
    nodes: &[NodeLiveness],
    missed_beats: u32,
    min_fleet: usize,
) -> Vec<PoolCadence> {
    let mut by_pool: std::collections::BTreeMap<&str, Vec<&NodeLiveness>> =
        std::collections::BTreeMap::new();
    for node in nodes {
        by_pool.entry(node.pool.as_str()).or_default().push(node);
    }
    by_pool
        .into_iter()
        .map(|(pool, members)| {
            let mut intervals: Vec<Duration> = members
                .iter()
                .map(|node| node.interval(missed_beats))
                .collect();
            intervals.sort();
            let interval = intervals[intervals.len() / 2];
            let fresh = members
                .iter()
                .filter(|node| node.is_fresh(missed_beats))
                .count();
            PoolCadence {
                pool: pool.to_string(),
                active_nodes: members.len(),
                interval,
                lost_window: lost_window_for(interval, missed_beats),
                freshness_ratio: (members.len() >= min_fleet)
                    .then(|| fresh as f64 / members.len() as f64),
            }
        })
        .collect()
}

/// The fleet's cadence report — what `legion node cadence`, the
/// `GetHeartbeatCadence` RPC and the runtime warning all read.
#[derive(Debug, Clone, PartialEq)]
pub struct CadenceReport {
    pub active_nodes: usize,
    /// The liveness interval: how often a node proves it is alive, with a
    /// ping or a full heartbeat. Lost windows derive from it.
    pub served_interval: Duration,
    /// The full-heartbeat interval. Never below `served_interval`; equal to
    /// it means workers send no pings.
    pub sync_interval: Duration,
    pub lost_window: Duration,
    pub missed_beats_before_lost: u32,
    /// `None` on a fleet too small to measure (below `min_fleet`).
    pub freshness_ratio: Option<f64>,
    /// Heartbeat commits per second this control plane is observed to sustain
    /// (`active x freshness / interval`). A LOWER BOUND while the fleet is
    /// keeping up: an unsaturated control plane never reveals its ceiling.
    pub observed_rate: Option<f64>,
    /// `observed_rate x served_interval` — nodes sustainable at this cadence.
    pub sustainable_nodes: Option<u64>,
    /// `ceil(H x N / observed_rate)`, the value to put in `heartbeatSeconds`.
    pub suggested_heartbeat_seconds: Option<u64>,
    pub adaptive_enabled: bool,
    /// True when the controller wanted to back off further but hit
    /// `maxInterval` — backing off is not infinite, and past this point the
    /// answer is more control plane.
    pub at_max_interval: bool,
    /// The same report per pool.
    pub pools: Vec<PoolCadence>,
}

impl CadenceReport {
    /// The measurable pool furthest behind, when it is behind the fleet as a
    /// whole: what the warning names, because a struggling small pool is
    /// exactly what the fleet-wide ratio hides.
    pub fn worst_pool(&self) -> Option<&PoolCadence> {
        self.pools
            .iter()
            .filter(|pool| pool.freshness_ratio.is_some())
            .min_by(|a, b| {
                a.freshness_ratio
                    .partial_cmp(&b.freshness_ratio)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    /// The lowest measurable ratio, fleet-wide or in any one pool — what the
    /// warning compares with its threshold.
    pub fn lowest_freshness(&self) -> Option<f64> {
        let pool_low = self.worst_pool().and_then(|pool| pool.freshness_ratio);
        match (self.freshness_ratio, pool_low) {
            (Some(fleet), Some(pool)) => Some(fleet.min(pool)),
            (fleet, pool) => fleet.or(pool),
        }
    }
}

/// The served intervals a report describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServedCadence {
    /// The liveness interval.
    pub liveness: Duration,
    /// The full-heartbeat interval; raised to `liveness` when below it.
    pub sync: Duration,
}

impl ServedCadence {
    /// One interval for both: every beat a full heartbeat, no pings.
    pub fn uniform(interval: Duration) -> Self {
        Self {
            liveness: interval,
            sync: interval,
        }
    }
}

/// Build the cadence report from one liveness snapshot.
pub fn cadence_report(
    nodes: &[NodeLiveness],
    served: ServedCadence,
    missed_beats: u32,
    headroom: f64,
    min_fleet: usize,
    adaptive_enabled: bool,
    at_max_interval: bool,
) -> CadenceReport {
    let active = nodes.len();
    let served_interval = served.liveness;
    let ratio = freshness_ratio(nodes, missed_beats, min_fleet);
    let interval_secs = served_interval.as_secs_f64().max(f64::EPSILON);
    // Beats per second the fleet is observed to sustain: every on-time node
    // contributes one beat per its OWN interval, so a slow pool is not
    // credited with the fast pool's rate (for a uniform fleet this is
    // `active x ratio / interval`, as it always was).
    let observed_rate = ratio.map(|_| {
        nodes
            .iter()
            .filter(|node| node.is_fresh(missed_beats))
            .map(|node| 1.0 / node.interval(missed_beats).as_secs_f64())
            .sum::<f64>()
            .max(f64::EPSILON)
    });
    let sustainable = observed_rate.map(|rate| (rate * interval_secs).round() as u64);
    let suggested = observed_rate.map(|rate| (headroom * active as f64 / rate).ceil() as u64);
    CadenceReport {
        active_nodes: active,
        served_interval,
        sync_interval: served.sync.max(served_interval),
        lost_window: served_interval * missed_beats,
        missed_beats_before_lost: missed_beats,
        freshness_ratio: ratio,
        observed_rate,
        sustainable_nodes: sustainable,
        suggested_heartbeat_seconds: suggested,
        adaptive_enabled,
        at_max_interval,
        pools: pool_cadence(nodes, missed_beats, min_fleet),
    }
}

/// The interval the manager wants a fleet of `active_nodes` to heartbeat at,
/// before the ladder quantizes it: `H x N / targetRatePerSecond`, clamped.
///
/// The input is FLEET SIZE, not saturation. That is the single most important
/// choice in the cadence plan (§6.4): deriving cadence from observed latency
/// would be a closed control loop that oscillates, and the lost window would
/// breathe with it. Fleet size is slow and externally driven, so this stays a
/// pure function an operator can evaluate on paper.
pub fn target_interval(
    active_nodes: usize,
    target_rate_per_second: f64,
    headroom: f64,
    min: Duration,
    max: Duration,
) -> Duration {
    if target_rate_per_second <= 0.0 {
        return min;
    }
    let seconds = (headroom * active_nodes as f64 / target_rate_per_second).ceil();
    let seconds = if seconds.is_finite() && seconds > 0.0 {
        seconds as u64
    } else {
        min.as_secs()
    };
    Duration::from_secs(seconds).clamp(min, max)
}

/// The lowest ladder rung at or above `target`, clamped into `[min, max]`.
/// `max` itself is always a legal answer even when it is not on the ladder,
/// so an operator's ceiling is honored exactly as written.
pub fn ladder_rung(target: Duration, min: Duration, max: Duration) -> Duration {
    let target = target.clamp(min, max);
    for rung in CADENCE_LADDER {
        let rung = Duration::from_secs(rung);
        if rung >= target && rung >= min && rung <= max {
            return rung;
        }
    }
    max
}

/// The controller's memory between ticks: which rung is being served, and how
/// long the target has been below it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LadderState {
    pub served: Duration,
    /// Consecutive time the target has justified a LOWER rung. Reset by any
    /// tick whose target does not.
    pub below_for: Duration,
}

impl LadderState {
    pub fn new(served: Duration) -> Self {
        Self {
            served,
            below_for: Duration::ZERO,
        }
    }
}

/// Advance the ladder one tick.
///
/// Steps **up** the moment the target exceeds the served rung — backing off is
/// always safe, and a fleet that just grew must not spend five minutes over
/// its ceiling. Steps **down** only after the target has justified a lower
/// rung for `LADDER_STEP_DOWN_DWELL`, because narrowing every node's lost
/// window is the direction that can cost a healthy node.
pub fn advance_ladder(
    state: &LadderState,
    target: Duration,
    elapsed: Duration,
    min: Duration,
    max: Duration,
) -> LadderState {
    let wanted = ladder_rung(target, min, max);
    if wanted > state.served {
        return LadderState::new(wanted);
    }
    if wanted < state.served {
        let below_for = state.below_for.saturating_add(elapsed);
        if below_for >= LADDER_STEP_DOWN_DWELL {
            return LadderState::new(wanted);
        }
        return LadderState {
            served: state.served,
            below_for,
        };
    }
    LadderState {
        served: state.served,
        below_for: Duration::ZERO,
    }
}

/// The runtime warning body, when the fleet is not sustaining its cadence.
///
/// The remedy is COMPUTED, not generic: an operator staring at a collapsing
/// cluster is handed the number to set, not the formula to evaluate.
pub fn freshness_warning(report: &CadenceReport) -> String {
    let fleet_ratio = report.freshness_ratio.unwrap_or(1.0);
    let false_loss_at =
        report.served_interval.as_secs_f64() / report.lost_window.as_secs_f64().max(f64::EPSILON);
    let remedy = match report.suggested_heartbeat_seconds {
        Some(seconds) => format!(
            "Raise heartbeatSeconds to >= {seconds} for a fleet this size, or add \
             control-plane capacity."
        ),
        None => "Add control-plane capacity.".to_string(),
    };
    // A pool well behind a fleet that looks fine is the case the fleet-wide
    // ratio hides, so the warning says which pool when it is the pool that
    // tripped it.
    let behind = match report.worst_pool() {
        Some(pool)
            if pool.freshness_ratio.is_some_and(|ratio| {
                ratio < fleet_ratio && ratio < HEARTBEAT_FRESHNESS_WARN_RATIO
            }) =>
        {
            format!(
                "{percent:.0}% of pool {name} ({nodes} nodes, {interval}s interval) checked in \
                 within one interval ({fleet:.0}% of the whole fleet)",
                percent = pool.freshness_ratio.unwrap_or(0.0) * 100.0,
                name = pool.pool,
                nodes = pool.active_nodes,
                interval = pool.interval.as_secs(),
                fleet = fleet_ratio * 100.0,
            )
        }
        _ => format!(
            "{percent:.0}% of this fleet checked in within one interval",
            percent = fleet_ratio * 100.0
        ),
    };
    format!(
        "Heartbeat cadence is not being sustained: {behind}. Healthy nodes will be marked \
         lost when freshness falls below {false_loss_at:.2}. {remedy} See \
         docs/heartbeat-cadence-and-scale.md"
    )
}

/// A node's lost window from the interval it is actually beating at.
pub fn lost_window_for(interval: Duration, missed_beats: u32) -> Duration {
    interval * missed_beats.max(1)
}

#[cfg(test)]
mod cadence_test {
    use super::*;

    fn node(id: &str, age_secs: u64, window_secs: u64) -> NodeLiveness {
        pooled("default", id, age_secs, window_secs)
    }

    fn pooled(pool: &str, id: &str, age_secs: u64, window_secs: u64) -> NodeLiveness {
        NodeLiveness {
            id: id.to_string(),
            pool: pool.to_string(),
            age: Duration::from_secs(age_secs),
            window: Duration::from_secs(window_secs),
        }
    }

    fn settled() -> LossContext {
        LossContext {
            responsible_for: Duration::from_secs(3600),
            ..LossContext::default()
        }
    }

    #[test]
    fn per_node_windows_are_compared_independently_test() {
        // The test that would have failed under one global cutoff: node A
        // beats at 2s (10s window), node B at 60s (300s window). At 55s of
        // silence A is long gone and B is perfectly healthy.
        let nodes = vec![node("a", 55, 10), node("b", 55, 300)];
        let decision = decide_lost_nodes(&nodes, &settled());
        assert_eq!(decision.lost, vec!["a".to_string()]);

        let nodes = vec![node("b", 305, 300)];
        assert_eq!(
            decide_lost_nodes(&nodes, &settled()).lost,
            vec!["b".to_string()]
        );
    }

    #[test]
    fn a_node_inside_its_window_is_never_lost_test() {
        let nodes = vec![node("a", 9, 10), node("b", 10, 10)];
        assert!(decide_lost_nodes(&nodes, &settled()).lost.is_empty());
    }

    #[test]
    fn failover_grace_suppresses_every_loss_test() {
        // Acquisition on a process that has been up for hours: the grace runs
        // from RESPONSIBILITY, not uptime, so nothing is marked lost inside it.
        let nodes = vec![node("a", 3600, 10), node("b", 3600, 10)];
        let ctx = LossContext {
            responsible_for: Duration::from_secs(59),
            failover_grace: Duration::from_secs(60),
            ..LossContext::default()
        };
        let decision = decide_lost_nodes(&nodes, &ctx);
        assert!(decision.lost.is_empty());
        assert_eq!(
            decision.suppressed_count(SuppressionReason::FailoverGrace),
            2
        );

        let ctx = LossContext {
            responsible_for: Duration::from_secs(61),
            ..ctx
        };
        assert_eq!(decide_lost_nodes(&nodes, &ctx).lost.len(), 2);
    }

    #[test]
    fn peer_gap_suppresses_only_that_peers_nodes_test() {
        let nodes = vec![node("a", 60, 10), node("c-1", 60, 10)];
        let ctx = LossContext {
            peer_gap_nodes: HashSet::from(["c-1".to_string()]),
            ..settled()
        };
        let decision = decide_lost_nodes(&nodes, &ctx);
        assert_eq!(decision.lost, vec!["a".to_string()]);
        assert_eq!(
            decision.suppressed_count(SuppressionReason::PeerDigestGap),
            1
        );
    }

    #[test]
    fn mass_disruption_breaker_marks_none_test() {
        // 51% would go: mark none. 49%: mark exactly those.
        let mut nodes: Vec<NodeLiveness> = (0..100)
            .map(|index| node(&format!("n-{index}"), 1, 10))
            .collect();
        for node in nodes.iter_mut().take(51) {
            node.age = Duration::from_secs(60);
        }
        let decision = decide_lost_nodes(&nodes, &settled());
        assert!(decision.lost.is_empty());
        assert_eq!(
            decision.suppressed_count(SuppressionReason::MassDisruption),
            51
        );

        nodes[50].age = Duration::from_secs(1);
        assert_eq!(decide_lost_nodes(&nodes, &settled()).lost.len(), 50);
    }

    #[test]
    fn breaker_does_not_fire_on_a_small_fleet_test() {
        let nodes = vec![node("a", 60, 10), node("b", 60, 10), node("c", 1, 10)];
        assert_eq!(decide_lost_nodes(&nodes, &settled()).lost.len(), 2);
    }

    #[test]
    fn freshness_ratio_arithmetic_test() {
        let mut nodes: Vec<NodeLiveness> = (0..100)
            .map(|index| node(&format!("n-{index}"), 1, 10))
            .collect();
        for node in nodes.iter_mut().take(78) {
            node.age = Duration::from_secs(9);
        }
        let ratio = freshness_ratio(&nodes, 5, 20).expect("measurable");
        assert!((ratio - 0.22).abs() < 1e-9, "ratio was {ratio}");
    }

    #[test]
    fn freshness_is_suppressed_on_a_tiny_fleet_test() {
        let nodes: Vec<NodeLiveness> = (0..19)
            .map(|index| node(&format!("n-{index}"), 1, 10))
            .collect();
        assert!(freshness_ratio(&nodes, 5, 20).is_none());
    }

    #[test]
    fn a_slow_pool_is_judged_against_its_own_interval_test() {
        // 50 nodes on a 2s cadence (10s window), 50 on a 60s batch cadence
        // (300s window), the batch nodes 40s into their interval. Judged
        // against one 2s served interval, half the fleet read as late; judged
        // against each node's own interval, everyone is on time.
        let mut nodes: Vec<NodeLiveness> = (0..50)
            .map(|index| pooled("fast", &format!("f-{index}"), 1, 10))
            .collect();
        nodes.extend((0..50).map(|index| pooled("batch", &format!("b-{index}"), 40, 300)));
        assert_eq!(freshness_ratio(&nodes, 5, 20), Some(1.0));
        let report = cadence_report(
            &nodes,
            ServedCadence::uniform(Duration::from_secs(2)),
            5,
            2.0,
            20,
            true,
            false,
        );
        let pools: Vec<(&str, u64, u64)> = report
            .pools
            .iter()
            .map(|pool| {
                (
                    pool.pool.as_str(),
                    pool.interval.as_secs(),
                    pool.lost_window.as_secs(),
                )
            })
            .collect();
        assert_eq!(pools, vec![("batch", 60, 300), ("fast", 2, 10)]);
        // The observed rate credits each pool with its own cadence:
        // 50 / 2s + 50 / 60s.
        let rate = report.observed_rate.expect("measurable");
        assert!((rate - (25.0 + 50.0 / 60.0)).abs() < 1e-9, "{rate}");
    }

    #[test]
    fn a_struggling_pool_shows_through_a_healthy_fleet_test() {
        // A 1,000-node batch pool keeping up and a 40-node latency pool where
        // 30 nodes are late: the blend reads 97% and would never warn.
        let mut nodes: Vec<NodeLiveness> = (0..1000)
            .map(|index| pooled("batch", &format!("b-{index}"), 1, 10))
            .collect();
        nodes.extend((0..40).map(|index| {
            let age = if index < 30 { 7 } else { 1 };
            pooled("latency", &format!("l-{index}"), age, 10)
        }));
        let report = cadence_report(
            &nodes,
            ServedCadence::uniform(Duration::from_secs(2)),
            5,
            2.0,
            20,
            true,
            false,
        );
        let fleet = report.freshness_ratio.expect("measurable");
        assert!(fleet > 0.95, "{fleet}");
        let worst = report.worst_pool().expect("a measurable pool");
        assert_eq!(worst.pool, "latency");
        assert_eq!(worst.freshness_ratio, Some(0.25));
        assert_eq!(report.lowest_freshness(), Some(0.25));
        let warning = freshness_warning(&report);
        assert!(warning.contains("pool latency"), "{warning}");
        assert!(warning.contains("25%"), "{warning}");

        // A pool under the floor is reported but never measured.
        let small: Vec<NodeLiveness> = (0..5)
            .map(|index| pooled("tiny", &format!("t-{index}"), 9, 10))
            .collect();
        let pools = pool_cadence(&small, 5, 20);
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].freshness_ratio, None);
    }

    #[test]
    fn controller_is_monotone_and_bounded_test() {
        let min = Duration::from_secs(2);
        let max = Duration::from_secs(120);
        let mut previous = Duration::ZERO;
        for nodes in [0, 10, 100, 160, 250, 500, 1000, 2000, 10_000] {
            let target = target_interval(nodes, 80.0, 2.0, min, max);
            assert!(target >= min && target <= max);
            assert!(target >= previous, "not monotone at {nodes} nodes");
            previous = target;
        }
        // The sizing table: 1000 nodes at R=80, H=2 wants 25s.
        assert_eq!(
            target_interval(1000, 80.0, 2.0, min, max),
            Duration::from_secs(25)
        );
        // And the ladder rounds that up to the 34s rung.
        assert_eq!(
            ladder_rung(Duration::from_secs(25), min, max),
            Duration::from_secs(34)
        );
    }

    #[test]
    fn default_rate_keeps_small_fleets_on_the_floor_and_backs_off_large_ones_test() {
        // The published table in docs/heartbeat-cadence-and-scale.md, at the
        // shipped defaults: rate 2,500/s, headroom 2, 2s..120s, 5 missed beats.
        let rate = DEF_HEARTBEAT_TARGET_RATE_PER_SECOND;
        let headroom = DEF_HEARTBEAT_HEADROOM;
        let min = Duration::from_secs(DEF_HEARTBEAT_MIN_INTERVAL_SEC);
        let max = Duration::from_secs(DEF_HEARTBEAT_MAX_INTERVAL_SEC);
        let missed = DEF_MISSED_BEATS_BEFORE_LOST;
        // (nodes, served interval, lost window)
        let table = [
            (10, 2, 10),
            (1_000, 2, 10),
            (2_500, 2, 10),
            (2_501, 3, 15),
            (5_000, 5, 25),
            (10_000, 8, 40),
            (20_000, 21, 105),
            (50_000, 55, 275),
        ];
        for (nodes, interval, window) in table {
            let served = ladder_rung(target_interval(nodes, rate, headroom, min, max), min, max);
            assert_eq!(served, Duration::from_secs(interval), "{nodes} nodes");
            assert_eq!(
                lost_window_for(served, missed),
                Duration::from_secs(window),
                "{nodes} nodes"
            );
            // Steady-state load never exceeds rate / headroom.
            assert!(nodes as f64 / served.as_secs_f64() <= rate / headroom);
        }
    }

    #[test]
    fn default_ping_rate_keeps_detection_fast_while_sync_backs_off_test() {
        // The "What the defaults serve" table in
        // docs/heartbeat-cadence-and-scale.md: the lost window follows the
        // liveness interval (sized from the ping rate), the full heartbeat
        // follows the sync interval (sized from the target rate).
        let headroom = DEF_HEARTBEAT_HEADROOM;
        let min = Duration::from_secs(DEF_HEARTBEAT_MIN_INTERVAL_SEC);
        let max = Duration::from_secs(DEF_HEARTBEAT_MAX_INTERVAL_SEC);
        let missed = DEF_MISSED_BEATS_BEFORE_LOST;
        let served = |nodes: usize, rate: f64| {
            ladder_rung(target_interval(nodes, rate, headroom, min, max), min, max)
        };
        // (nodes, liveness interval, lost window, sync interval)
        let table = [
            (1_000, 2, 10, 2),
            (2_500, 2, 10, 2),
            (5_000, 2, 10, 5),
            (10_000, 2, 10, 8),
            (10_001, 3, 15, 13),
            (20_000, 5, 25, 21),
            (50_000, 13, 65, 55),
        ];
        for (nodes, liveness, window, sync) in table {
            let ping = served(nodes, DEF_HEARTBEAT_PING_RATE_PER_SECOND);
            let full = served(nodes, DEF_HEARTBEAT_TARGET_RATE_PER_SECOND).max(ping);
            assert_eq!(ping, Duration::from_secs(liveness), "{nodes} nodes");
            assert_eq!(
                lost_window_for(ping, missed),
                Duration::from_secs(window),
                "{nodes} nodes"
            );
            assert_eq!(full, Duration::from_secs(sync), "{nodes} nodes");
            // Neither load exceeds its budget at steady state.
            assert!(
                nodes as f64 / ping.as_secs_f64() <= DEF_HEARTBEAT_PING_RATE_PER_SECOND / headroom
            );
            assert!(
                nodes as f64 / full.as_secs_f64()
                    <= DEF_HEARTBEAT_TARGET_RATE_PER_SECOND / headroom
            );
        }
    }

    #[test]
    fn ladder_steps_up_at_once_and_down_only_after_its_dwell_test() {
        let min = Duration::from_secs(2);
        let max = Duration::from_secs(120);
        let state = LadderState::new(Duration::from_secs(2));
        let up = advance_ladder(
            &state,
            Duration::from_secs(12),
            Duration::from_secs(2),
            min,
            max,
        );
        assert_eq!(up.served, Duration::from_secs(13));

        // A target back down at 2s does not shrink the window immediately.
        let mut state = up;
        for _ in 0..149 {
            state = advance_ladder(&state, min, Duration::from_secs(2), min, max);
            assert_eq!(state.served, Duration::from_secs(13));
        }
        state = advance_ladder(&state, min, Duration::from_secs(2), min, max);
        assert_eq!(state.served, min, "dwell elapsed, step down");
    }

    #[test]
    fn a_target_that_holds_steady_clears_the_dwell_test() {
        let min = Duration::from_secs(2);
        let max = Duration::from_secs(120);
        let mut state = LadderState::new(Duration::from_secs(13));
        state = advance_ladder(&state, min, Duration::from_secs(60), min, max);
        assert_eq!(state.below_for, Duration::from_secs(60));
        state = advance_ladder(
            &state,
            Duration::from_secs(13),
            Duration::from_secs(60),
            min,
            max,
        );
        assert_eq!(
            state.below_for,
            Duration::ZERO,
            "dwell reset by a steady tick"
        );
    }

    #[test]
    fn max_interval_is_servable_even_off_ladder_test() {
        let min = Duration::from_secs(2);
        let max = Duration::from_secs(45);
        assert_eq!(ladder_rung(Duration::from_secs(40), min, max), max);
        assert_eq!(
            ladder_rung(Duration::from_secs(34), min, max),
            Duration::from_secs(34)
        );
    }

    #[test]
    fn heartbeat_seconds_clamp_widened_to_300_test() {
        assert_eq!(clamp_heartbeat_seconds(None), 2);
        assert_eq!(clamp_heartbeat_seconds(Some(0)), 1);
        assert_eq!(clamp_heartbeat_seconds(Some(60)), 60);
        assert_eq!(clamp_heartbeat_seconds(Some(300)), 300);
        assert_eq!(clamp_heartbeat_seconds(Some(4000)), 300);
    }

    #[test]
    fn report_names_the_remedy_test() {
        let nodes: Vec<NodeLiveness> = (0..1000)
            .map(|index| {
                let late = index >= 220;
                node(&format!("n-{index}"), if late { 9 } else { 1 }, 10)
            })
            .collect();
        let report = cadence_report(
            &nodes,
            ServedCadence::uniform(Duration::from_secs(2)),
            5,
            2.0,
            20,
            false,
            false,
        );
        assert_eq!(report.lost_window, Duration::from_secs(10));
        let ratio = report.freshness_ratio.expect("measurable");
        assert!((ratio - 0.22).abs() < 1e-9);
        // 1000 nodes beating at an observed 110/sec want >= 19s at H=2.
        assert_eq!(report.suggested_heartbeat_seconds, Some(19));
        assert_eq!(report.sustainable_nodes, Some(220));
        let warning = freshness_warning(&report);
        assert!(warning.contains("22%"), "{warning}");
        assert!(warning.contains(">= 19"), "{warning}");
    }
}
