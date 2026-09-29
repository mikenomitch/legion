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
    /// How long since this manager last observed a beat from the node.
    pub age: Duration,
    /// This node's own lost window: `missedBeatsBeforeLost x its interval`.
    pub window: Duration,
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

/// Fraction of the active fleet that checked in within one interval.
///
/// `None` below `min_fleet` nodes: the ratio is pure noise on a handful of
/// nodes and would fire the warning on a laptop cluster. `1.0` means everyone
/// is on time; false loss begins at `served_interval / lost_window`, which at
/// the defaults is `2s / 10s = 0.20`.
pub fn freshness_ratio(
    nodes: &[NodeLiveness],
    served_interval: Duration,
    min_fleet: usize,
) -> Option<f64> {
    if nodes.len() < min_fleet {
        return None;
    }
    let cutoff = served_interval.mul_f64(FRESHNESS_SLACK);
    let fresh = nodes.iter().filter(|node| node.age <= cutoff).count();
    Some(fresh as f64 / nodes.len() as f64)
}

/// The fleet's cadence report — what `legion node cadence`, the
/// `GetHeartbeatCadence` RPC and the runtime warning all read.
#[derive(Debug, Clone, PartialEq)]
pub struct CadenceReport {
    pub active_nodes: usize,
    pub served_interval: Duration,
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
}

/// Build the cadence report from one liveness snapshot.
pub fn cadence_report(
    nodes: &[NodeLiveness],
    served_interval: Duration,
    missed_beats: u32,
    headroom: f64,
    min_fleet: usize,
    adaptive_enabled: bool,
    at_max_interval: bool,
) -> CadenceReport {
    let active = nodes.len();
    let ratio = freshness_ratio(nodes, served_interval, min_fleet);
    let interval_secs = served_interval.as_secs_f64().max(f64::EPSILON);
    let observed_rate =
        ratio.map(|ratio| (active as f64 * ratio / interval_secs).max(f64::EPSILON));
    let sustainable = observed_rate.map(|rate| (rate * interval_secs).round() as u64);
    let suggested = observed_rate.map(|rate| (headroom * active as f64 / rate).ceil() as u64);
    CadenceReport {
        active_nodes: active,
        served_interval,
        lost_window: served_interval * missed_beats,
        missed_beats_before_lost: missed_beats,
        freshness_ratio: ratio,
        observed_rate,
        sustainable_nodes: sustainable,
        suggested_heartbeat_seconds: suggested,
        adaptive_enabled,
        at_max_interval,
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
    let ratio = report.freshness_ratio.unwrap_or(1.0);
    let false_loss_at =
        report.served_interval.as_secs_f64() / report.lost_window.as_secs_f64().max(f64::EPSILON);
    let remedy = match report.suggested_heartbeat_seconds {
        Some(seconds) => format!(
            "Raise heartbeatSeconds to >= {seconds} for a fleet this size, or add \
             control-plane capacity."
        ),
        None => "Add control-plane capacity.".to_string(),
    };
    format!(
        "Heartbeat cadence is not being sustained: {percent:.0}% of this fleet checked in \
         within one interval. Healthy nodes will be marked lost when freshness falls below \
         {false_loss_at:.2}. {remedy} See docs/heartbeat-cadence-and-scale.md",
        percent = ratio * 100.0,
    )
}

/// A worker type's `heartbeatSeconds`, clamped to the legal range.
/// `None` means the built-in default.
pub fn clamp_heartbeat_seconds(configured: Option<u32>) -> u64 {
    configured
        .map(|seconds| u64::from(seconds).clamp(1, u64::from(MAX_WORKER_HEARTBEAT_SECONDS)))
        .unwrap_or(WORKER_HEARTBEAT_DURATION_SEC)
}

/// A node's lost window from the interval it is actually beating at.
pub fn lost_window_for(interval: Duration, missed_beats: u32) -> Duration {
    interval * missed_beats.max(1)
}

#[cfg(test)]
mod cadence_test {
    use super::*;

    fn node(id: &str, age_secs: u64, window_secs: u64) -> NodeLiveness {
        NodeLiveness {
            id: id.to_string(),
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
        let ratio = freshness_ratio(&nodes, Duration::from_secs(2), 20).expect("measurable");
        assert!((ratio - 0.22).abs() < 1e-9, "ratio was {ratio}");
    }

    #[test]
    fn freshness_is_suppressed_on_a_tiny_fleet_test() {
        let nodes: Vec<NodeLiveness> = (0..19)
            .map(|index| node(&format!("n-{index}"), 1, 10))
            .collect();
        assert!(freshness_ratio(&nodes, Duration::from_secs(2), 20).is_none());
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
        let report = cadence_report(&nodes, Duration::from_secs(2), 5, 2.0, 20, false, false);
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
