//! Adaptive scheduler concurrency — congestion control for the plan-apply
//! path, as pure functions plus the two pieces of shared state they drive.
//!
//! Legion's plan-apply path is optimistic: a planner works against a snapshot
//! and the applier rejects the plan if a node epoch it depended on moved. A
//! rejection is a replan, and replans are offered load, so like every
//! optimistic-concurrency system this one has a knee: past it, more
//! concurrent planners place *less*. Where the knee is depends on fleet size,
//! job shape and arrival rate, and on an uncontended cluster there is no knee
//! in reach at all — throughput simply rises with `evalThreads`
//! (`docs/diagnosing-scheduler-performance.md`).
//!
//! This module measures the manager's own conflict rate and decides how many
//! evals it will PLAN at once: back off hard when conflicts climb, probe back
//! up when they subside (AIMD), with a goodput guard so a physically full
//! cluster is never throttled for conflicts it would have had anyway — and
//! with the explicit design goal that on a cluster that is not conflicting it
//! does nothing, cheaply.
//!
//! The architecture deliberately copies [`crate::manager::cadence`]:
//!
//! - [`advance`] is **pure** — arithmetic over a snapshot, no store access, no
//!   clock reads, no leadership checks. `start_concurrency_controller`
//!   ([`crate::manager::daemon`]) collects the snapshot and applies the
//!   verdict; this module decides what the verdict is, which is what makes the
//!   rule testable without a cluster.
//! - The limit is an **integer**, not a continuous function, so the served
//!   value is memorizable and loggable.
//! - The dwell is **asymmetric**. For cadence, backing off (widening) is safe
//!   and narrowing dwells; here backing off (throttling) is safe and
//!   *increasing* dwells.
//!
//! The plan is `internal_docs/plans/adaptive-scheduler-concurrency-plan.md`;
//! the public page is `docs/adaptive-scheduler-concurrency.md`.

use crate::prelude::*;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

//
// CONSTANTS
//
// Every constant the rule uses is public and lives here, so the decision can
// be reasoned about (and replayed) without reading the daemon.
//

/// How often the controller looks at its window.
///
/// Fast enough to react inside a burst (the measured collapse is fully
/// developed in seconds), slow enough that a tick carries a usable sample at
/// realistic rates (~350 placements/sec is ~175 attempts per tick).
pub const TICK_PERIOD: Duration = Duration::from_millis(500);

/// Smoothing factor on the conflict and goodput EWMAs (~a 3-tick time
/// constant). One unlucky tick must not trip a decrease.
pub const EWMA_ALPHA: f64 = 0.3;

/// Attempts a tick needs before it means anything.
///
/// At low volume a single rejection reads as a 100% conflict rate, and a
/// controller that believes it throttles an idle cluster. Ticks below this are
/// **inert**: they update no average, advance no dwell, and trigger no action.
pub const MIN_ATTEMPTS: u64 = 16;

/// Conflict rate above which the limit is halved.
///
/// A rejection costs one wasted plan plus one eval retry, so a share below one
/// in five is a tax and above it the planners spend more of their time losing
/// races than placing — the boundary `docs/diagnosing-scheduler-performance.md`
/// draws between "worth acting on" and "collapse". The trip point sits on
/// that boundary. It is a fitted starting value, not a derived one: the
/// campaign that would settle it (plan §11) has not been run, and the
/// pre-two-pass-bind numbers it was first chosen against are retracted
/// (`internal_docs/aws-phase4-findings.md`), which is why `observe` is the
/// default.
pub const HIGH_WATERMARK: f64 = 0.20;

/// Conflict rate below which the limit is probed back up, after a dwell.
///
/// The 15-point gap to [`HIGH_WATERMARK`] is the dead band, and it is the
/// primary anti-hunting mechanism: with a single threshold the controller
/// oscillates across it forever by construction.
pub const LOW_WATERMARK: f64 = 0.05;

/// Multiplicative-decrease factor (classic AIMD). Over the realistic ceiling
/// range this is 16 -> 8 -> 4 -> 2 -> 1: four steps and ~6s to the floor
/// including cooldowns. A gentler 0.7 takes eight steps and ~12s, which is
/// slower than the collapse it is fighting.
pub const DECREASE_FACTOR: f64 = 0.5;

/// Refractory period after a decrease, in ticks.
///
/// The single most important damping term: a decrease takes ~1 tick to show up
/// in the window and ~1 more to work through the EWMA, so without it the
/// controller cuts to the floor on the first burst before its first cut has
/// been measured. Same instinct as TCP not halving twice for losses in one
/// window.
pub const COOLDOWN_TICKS: u32 = 3;

/// Consecutive calm ticks required before the limit is increased (2s).
///
/// The cadence controller's asymmetry ([`crate::manager::cadence::LADDER_STEP_DOWN_DWELL`]),
/// inverted: here the *safe* direction is down, so down is immediate and up
/// must dwell.
pub const PROBE_DWELL_TICKS: u32 = 4;

/// Ticks after a decrease before its effect on goodput is judged (3s).
pub const GOODPUT_VERIFY_TICKS: u32 = 2 * COOLDOWN_TICKS;

/// Relative goodput improvement a decrease must produce to be judged to have
/// helped. Below this, the conflicts were not concurrency-caused.
pub const GOODPUT_EPSILON: f64 = 0.05;

/// How long the controller stops decreasing after the goodput guard trips
/// (30s). Increases are still permitted throughout — the pool may drain.
pub const SATURATION_HOLD: Duration = Duration::from_secs(30);

/// Eval kinds that never invoke the planner, and so are never throttled.
///
/// An **allowlist**, deliberately: a kind added later is treated as planning
/// (and throttled) until someone proves otherwise, which is the safe default.
/// Every other kind reaches [`crate::manager::scheduler::Scheduler`] on at
/// least one path — `rollout_tick` through
/// `crate::manager::rollout::handle_rollout_tick`, and the four `node_*` kinds
/// through `Store::reconcile_job_config_allocations` — so all of them can
/// move node epochs and all of them hold a permit.
///
/// `job_stop` is the one kind that cannot: it removes allocations and stops
/// instances, touching no epoch. It is also exactly the kind that must not
/// queue behind a congestion storm, because a `job stop` is frequently the
/// *remedy* for one.
pub const NON_PLANNING_EVAL_KINDS: &[&str] = &["job_stop"];

/// Whether an eval of this kind can reach the planner, and so must hold an
/// admission permit.
pub fn is_planning_kind(kind: &str) -> bool {
    !NON_PLANNING_EVAL_KINDS.contains(&kind)
}

//
// SETTINGS
//

/// `schedulerSettings.adaptiveConcurrency.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ConcurrencyMode {
    /// Do not run the controller at all — today's behavior, exactly.
    Off = 0,
    /// Measure, publish what it WOULD do, actuate nothing. The default:
    /// evidence before action, the same posture as `preemption.mode`.
    #[default]
    Observe = 1,
    /// Measure and actuate.
    Enforce = 2,
}

impl ConcurrencyMode {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "enforce" => Some(Self::Enforce),
            "observe" => Some(Self::Observe),
            "off" => Some(Self::Off),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enforce => "enforce",
            Self::Observe => "observe",
            Self::Off => "off",
        }
    }
}

/// Parsed `schedulerSettings.adaptiveConcurrency`.
#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveConcurrencySettings {
    pub mode: ConcurrencyMode,
    /// Floor on admitted planners. Never 0 — a floor of 0 is a stuck
    /// scheduler, not a throttled one.
    pub min: usize,
    /// Ceiling override. `None` means `evalThreads`.
    pub max: Option<usize>,
    pub high_watermark: f64,
    pub low_watermark: f64,
}

impl Default for AdaptiveConcurrencySettings {
    fn default() -> Self {
        Self {
            mode: ConcurrencyMode::default(),
            min: 1,
            max: None,
            high_watermark: HIGH_WATERMARK,
            low_watermark: LOW_WATERMARK,
        }
    }
}

impl AdaptiveConcurrencySettings {
    /// Parse `schedulerSettings.adaptiveConcurrency`. Unknown or malformed
    /// values are a **hard error**, for the same reason `preemption` makes
    /// them one: a mistyped `mode = "enforc"` silently becoming the default
    /// would leave an operator believing they had turned something on (or
    /// off) that they had not.
    pub fn from_scheduler_settings(settings: Option<&Value>) -> R<Self> {
        let block = settings.and_then(|settings| {
            settings
                .get("adaptiveConcurrency")
                .or_else(|| settings.get("adaptive_concurrency"))
        });
        let Some(block) = block else {
            return Ok(Self::default());
        };
        if !block.is_object() {
            bail!("schedulerSettings.adaptiveConcurrency must be an object");
        }

        let mode = match block.get("mode") {
            None => ConcurrencyMode::default(),
            Some(Value::String(raw)) => ConcurrencyMode::parse(raw).ok_or_else(|| {
                anyhow!(
                    "schedulerSettings.adaptiveConcurrency.mode {raw:?} must be one of \
                     enforce, observe, off"
                )
            })?,
            Some(_) => bail!("schedulerSettings.adaptiveConcurrency.mode must be a string"),
        };

        let positive = |key: &str| -> R<Option<usize>> {
            match block.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(value) => {
                    let number = value.as_u64().ok_or_else(|| {
                        anyhow!(
                            "schedulerSettings.adaptiveConcurrency.{key} must be a positive integer"
                        )
                    })?;
                    if number == 0 {
                        bail!(
                            "schedulerSettings.adaptiveConcurrency.{key} must be at least 1 \
                             (a floor of 0 is a stuck scheduler, not a throttled one)"
                        );
                    }
                    Ok(Some(number as usize))
                }
            }
        };
        let min = positive("min")?.unwrap_or(1);
        let max = positive("max")?;
        if let Some(max) = max {
            if max < min {
                bail!(
                    "schedulerSettings.adaptiveConcurrency.max ({max}) must be at least \
                     .min ({min})"
                );
            }
        }

        let fraction = |key: &str, default: f64| -> R<f64> {
            match block.get(key) {
                None | Some(Value::Null) => Ok(default),
                Some(value) => {
                    let number = value.as_f64().ok_or_else(|| {
                        anyhow!("schedulerSettings.adaptiveConcurrency.{key} must be a number")
                    })?;
                    if !(number.is_finite() && (0.0..=1.0).contains(&number)) {
                        bail!(
                            "schedulerSettings.adaptiveConcurrency.{key} ({number}) must be a \
                             conflict rate between 0.0 and 1.0"
                        );
                    }
                    Ok(number)
                }
            }
        };
        let high_watermark = fraction("highWatermark", HIGH_WATERMARK)?;
        let low_watermark = fraction("lowWatermark", LOW_WATERMARK)?;
        if low_watermark >= high_watermark {
            bail!(
                "schedulerSettings.adaptiveConcurrency.lowWatermark ({low_watermark}) must be \
                 below .highWatermark ({high_watermark}) — the gap between them is the dead \
                 band that stops the controller hunting"
            );
        }

        Ok(Self {
            mode,
            min,
            max,
            high_watermark,
            low_watermark,
        })
    }

    /// The `[floor, ceiling]` this controller may move within, given the
    /// operator's `evalThreads`. `evalThreads` always caps: `max` may lower
    /// the ceiling but never raise it above the worker pool.
    pub fn bounds(&self, eval_threads: usize) -> (usize, usize) {
        let ceiling = self
            .max
            .unwrap_or(eval_threads)
            .min(eval_threads.max(1))
            .max(1);
        let floor = self.min.clamp(1, ceiling);
        (floor, ceiling)
    }

    /// The trip points, as [`advance`] takes them.
    pub fn tuning(&self) -> ControllerTuning {
        ControllerTuning {
            high_watermark: self.high_watermark,
            low_watermark: self.low_watermark,
        }
    }
}

/// The two trip points, split out so [`advance`] never sees a mode or a
/// config shape.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ControllerTuning {
    pub high_watermark: f64,
    pub low_watermark: f64,
}

impl Default for ControllerTuning {
    fn default() -> Self {
        Self {
            high_watermark: HIGH_WATERMARK,
            low_watermark: LOW_WATERMARK,
        }
    }
}

//
// THE RULE
//

/// What the controller is doing, for the operator and for
/// `legion_scheduler_concurrency_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ControllerState {
    /// Running at the ceiling; nothing is being held back.
    #[default]
    Steady,
    /// Below the ceiling, and conflicts have not yet subsided.
    Throttled,
    /// Below the ceiling and accumulating calm ticks toward an increase.
    Probing,
    /// A decrease was tried and did not improve goodput. The conflicts are
    /// capacity-driven, so the actuator has stopped being applied.
    Saturated,
}

impl ControllerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::Throttled => "throttled",
            Self::Probing => "probing",
            Self::Saturated => "saturated",
        }
    }
}

/// Which way the last adjustment went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjustmentDirection {
    Increase,
    Decrease,
    /// A decrease that did not pay for itself, undone.
    Revert,
}

impl AdjustmentDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Increase => "increase",
            Self::Decrease => "decrease",
            Self::Revert => "revert",
        }
    }
}

/// One limit change, kept so the diagnostic can say *what* happened and *when*
/// rather than only where the limit ended up.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Adjustment {
    pub direction: AdjustmentDirection,
    pub from: usize,
    pub to: usize,
    /// Ticks since the change. Multiply by [`TICK_PERIOD`] for wall clock.
    pub ticks_ago: u32,
    /// The smoothed conflict rate that justified it.
    pub conflict_rate: f64,
    /// Smoothed goodput at the moment of the change, so the report can show
    /// what the change bought.
    pub goodput_before: f64,
}

/// A decrease awaiting its goodput verdict (§7.4).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PendingDecrease {
    /// The limit to restore if the decrease did not pay for itself.
    pub restore_to: usize,
    /// Smoothed goodput at the moment of the decrease.
    pub goodput_before: f64,
    /// Ticks left before the verdict.
    pub ticks_left: u32,
    /// Measured (non-inert) ticks seen since the decrease. With none, there is
    /// no evidence either way and the pending decrease is dropped rather than
    /// reverted.
    pub samples: u32,
}

/// The controller's memory between ticks.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcurrencyState {
    /// Planners currently admitted.
    pub limit: usize,
    /// Smoothed conflict rate. `None` until the first measured tick.
    pub ewma_conflict: Option<f64>,
    /// Smoothed rate at which races were ABSORBED by candidate lists.
    /// Tracked for the diagnostic only — [`decide`] never reads it, because a
    /// fallback is late binding working and throttling on it would throttle
    /// the success case.
    pub ewma_fallback: Option<f64>,
    /// Smoothed goodput, in placements per second.
    pub ewma_goodput: Option<f64>,
    /// Ticks left in the post-decrease refractory period.
    pub refractory_ticks: u32,
    /// Consecutive measured ticks below the low watermark.
    pub calm_ticks: u32,
    pub pending_decrease: Option<PendingDecrease>,
    /// Ticks left in the saturation hold. Non-zero implies
    /// [`ControllerState::Saturated`].
    pub saturation_ticks: u32,
    /// Goodput at the moment the saturation hold began: the hold also lifts
    /// early if goodput climbs more than [`GOODPUT_EPSILON`] above it, because
    /// that means the pool drained.
    pub saturation_goodput: f64,
    pub state: ControllerState,
    /// Ticks the controller has been in `state`.
    pub ticks_in_state: u32,
    pub last_adjustment: Option<Adjustment>,
}

impl ConcurrencyState {
    /// A fresh controller for the given bounds, starting AT THE CEILING. Also
    /// the reset a manager performs on acquiring scheduler-lane leadership:
    /// a follower does no planning, so its window is empty and its state
    /// would be noise.
    ///
    /// At the ceiling, not below it, because the controller backs off on
    /// evidence and a cold start has none: on an uncontended cluster
    /// throughput rises with planners (`docs/diagnosing-scheduler-performance.md`),
    /// and starting low would spend `(ceiling - start) x 2s` climbing back
    /// for nothing. If the first burst does congest, the descent is the fast
    /// direction — halving per refractory window — so starting high costs at
    /// most one short collapse, and only when there was one to have.
    pub fn new(ceiling: usize) -> Self {
        Self {
            limit: ceiling.max(1),
            ewma_conflict: None,
            ewma_fallback: None,
            ewma_goodput: None,
            refractory_ticks: 0,
            calm_ticks: 0,
            pending_decrease: None,
            saturation_ticks: 0,
            saturation_goodput: 0.0,
            state: ControllerState::Steady,
            ticks_in_state: 0,
            last_adjustment: None,
        }
    }

    pub fn seconds_in_state(&self) -> u64 {
        (u64::from(self.ticks_in_state) * TICK_PERIOD.as_millis() as u64) / 1_000
    }
}

/// One tick's worth of evidence.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TickInput {
    /// Plans that bound every placement at rank 0.
    pub applied: u64,
    /// Plans that bound, but slid down at least one candidate list. **Counted
    /// as progress, not as conflict**: that is the whole point of late
    /// binding — a race that cost a rank is a race that was absorbed, and
    /// throttling to avoid absorbed races would be throttling to avoid the
    /// feature working.
    pub fallback: u64,
    /// Plans that lost: every candidate was stale or invalid.
    pub rejected: u64,
    pub elapsed: Duration,
    pub floor: usize,
    pub ceiling: usize,
    pub tuning: ControllerTuning,
}

impl TickInput {
    /// Plans that actually placed.
    pub fn progress(&self) -> u64 {
        self.applied.saturating_add(self.fallback)
    }

    /// Plans that reached the applier at all.
    pub fn attempts(&self) -> u64 {
        self.progress().saturating_add(self.rejected)
    }

    /// `rejected / attempts`, or `None` on an empty window.
    pub fn conflict_rate(&self) -> Option<f64> {
        let attempts = self.attempts();
        (attempts > 0).then(|| self.rejected as f64 / attempts as f64)
    }

    /// `fallback / attempts` — races absorbed rather than lost.
    pub fn fallback_rate(&self) -> Option<f64> {
        let attempts = self.attempts();
        (attempts > 0).then(|| self.fallback as f64 / attempts as f64)
    }

    /// Placements per second this tick.
    pub fn goodput(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds <= 0.0 {
            return 0.0;
        }
        self.progress() as f64 / seconds
    }

    /// Whether this tick carries enough volume to mean anything.
    pub fn is_measurable(&self) -> bool {
        self.attempts() >= MIN_ATTEMPTS
    }
}

/// Advance the controller one tick.
///
/// The rule, in precedence order:
///
/// | Condition | Action |
/// | --- | --- |
/// | `attempts < MIN_ATTEMPTS` | inert: no average moves, no dwell advances, nothing acts |
/// | `progress == 0` | hold — nothing placed, so there is no evidence that fewer planners would have placed more. This is the pure-saturation signature |
/// | saturation hold active | no decrease; increases still permitted |
/// | `c > high` and not in refractory | halve the limit, enter refractory, and arm the goodput verification |
/// | `c < low` for `PROBE_DWELL_TICKS` | add one back |
/// | otherwise | hold, and reset the dwell |
///
/// Four independent damping terms keep the loop from hunting, and none of them
/// is sufficient alone: the dead band between the watermarks, the EWMA, the
/// refractory period, and the asymmetric dwell.
pub fn advance(state: &ConcurrencyState, input: &TickInput) -> ConcurrencyState {
    let ceiling = input.ceiling.max(1);
    let floor = input.floor.clamp(1, ceiling);
    let mut next = state.clone();
    next.limit = next.limit.clamp(floor, ceiling);

    // Timers are wall-clock guards, so they run on every tick — including
    // inert ones. Otherwise an idle cluster would hold a refractory period or
    // a saturation hold open forever.
    if !input.is_measurable() {
        // Inert. The pending verification's clock still runs (it is a
        // wall-clock promise), but with no sample to judge it by, its expiry
        // drops it rather than reverting — see `resolve_pending`.
        next.refractory_ticks = next.refractory_ticks.saturating_sub(1);
        next.saturation_ticks = next.saturation_ticks.saturating_sub(1);
        next.pending_decrease = tick_pending(next.pending_decrease, false);
        return finish(next, state, floor, ceiling, None);
    }

    let conflict = input.conflict_rate().unwrap_or(0.0);
    next.ewma_conflict = Some(smooth(next.ewma_conflict, conflict));
    next.ewma_fallback = Some(smooth(
        next.ewma_fallback,
        input.fallback_rate().unwrap_or(0.0),
    ));
    next.ewma_goodput = Some(smooth(next.ewma_goodput, input.goodput()));
    let smoothed_conflict = next.ewma_conflict.unwrap_or(conflict);
    let smoothed_goodput = next.ewma_goodput.unwrap_or(0.0);
    next.pending_decrease = tick_pending(next.pending_decrease, true);

    // The saturation hold lifts early when goodput climbs MEANINGFULLY above
    // where it was when the guard tripped: that means the pool drained, and
    // the conflicts are open to being concurrency-caused again. The margin is
    // the same `GOODPUT_EPSILON` the guard itself uses — a bare `>` would lift
    // a 30s hold on one noisy tick, which is exactly the churn the hold exists
    // to prevent.
    if next.saturation_ticks > 0
        && smoothed_goodput > next.saturation_goodput * (1.0 + GOODPUT_EPSILON)
    {
        next.saturation_ticks = 0;
    }

    // Both guards are read BEFORE this tick consumes one of their ticks, so
    // `COOLDOWN_TICKS = 3` means three ticks of holding rather than two.
    let guards = Guards {
        in_refractory: next.refractory_ticks > 0,
        in_saturation_hold: next.saturation_ticks > 0,
    };
    next.refractory_ticks = next.refractory_ticks.saturating_sub(1);
    next.saturation_ticks = next.saturation_ticks.saturating_sub(1);

    // Resolve an armed verification before deciding anything new, so a revert
    // and a fresh decrease can never land in the same tick.
    let mut adjustment = resolve_pending(&mut next, smoothed_goodput, smoothed_conflict);

    if adjustment.is_none() {
        adjustment = decide(
            &mut next,
            input,
            floor,
            ceiling,
            guards,
            smoothed_conflict,
            smoothed_goodput,
        );
    }

    finish(next, state, floor, ceiling, adjustment)
}

/// The two "not right now" guards, sampled before this tick consumes them.
#[derive(Debug, Clone, Copy)]
struct Guards {
    in_refractory: bool,
    in_saturation_hold: bool,
}

fn smooth(previous: Option<f64>, sample: f64) -> f64 {
    match previous {
        // Seed with the first real sample rather than with zero: a controller
        // that starts every average at 0 spends its first few ticks believing
        // a calm cluster it has not observed.
        None => sample,
        Some(previous) => EWMA_ALPHA * sample + (1.0 - EWMA_ALPHA) * previous,
    }
}

fn tick_pending(pending: Option<PendingDecrease>, measured: bool) -> Option<PendingDecrease> {
    pending.map(|pending| PendingDecrease {
        ticks_left: pending.ticks_left.saturating_sub(1),
        samples: pending.samples + u32::from(measured),
        ..pending
    })
}

/// The goodput guard (§7.4). Conflict rate is a fast leading indicator;
/// goodput is the thing we actually want, and they diverge in exactly the case
/// that matters. So every decrease is verified, and one that did not pay for
/// itself is undone — otherwise a physically full pool, where the losers were
/// doomed regardless, gets throttled for conflicts fewer planners cannot fix.
fn resolve_pending(
    state: &mut ConcurrencyState,
    goodput: f64,
    conflict: f64,
) -> Option<Adjustment> {
    let pending = state.pending_decrease?;
    if pending.ticks_left > 0 {
        return None;
    }
    state.pending_decrease = None;
    // No measured tick since the decrease: no evidence either way, and no load
    // for the limit to matter to. Drop it rather than reverting on silence.
    if pending.samples == 0 {
        return None;
    }
    if goodput >= pending.goodput_before * (1.0 + GOODPUT_EPSILON) {
        return None;
    }

    let from = state.limit;
    state.limit = pending.restore_to;
    state.saturation_ticks = saturation_hold_ticks();
    state.saturation_goodput = goodput;
    state.calm_ticks = 0;
    Some(Adjustment {
        direction: AdjustmentDirection::Revert,
        from,
        to: state.limit,
        ticks_ago: 0,
        conflict_rate: conflict,
        goodput_before: pending.goodput_before,
    })
}

fn decide(
    state: &mut ConcurrencyState,
    input: &TickInput,
    floor: usize,
    ceiling: usize,
    guards: Guards,
    conflict: f64,
    goodput: f64,
) -> Option<Adjustment> {
    // Nothing placed. A tick with no progress is the pure-saturation
    // signature, and it is the one shape where backing off is strictly
    // harmful: the losers were doomed either way, so fewer planners does not
    // convert them into winners, it only makes the one winner per round
    // arrive later.
    if input.progress() == 0 {
        state.calm_ticks = 0;
        return None;
    }

    if conflict > input.tuning.high_watermark {
        if guards.in_refractory || guards.in_saturation_hold {
            state.calm_ticks = 0;
            return None;
        }
        let from = state.limit;
        let to = (((from as f64) * DECREASE_FACTOR).floor() as usize).clamp(floor, ceiling);
        state.calm_ticks = 0;
        if to >= from {
            // Already at the floor. Still refractory-guarded so the log does
            // not repeat every tick.
            state.refractory_ticks = COOLDOWN_TICKS;
            return None;
        }
        state.limit = to;
        state.refractory_ticks = COOLDOWN_TICKS;
        state.pending_decrease = Some(PendingDecrease {
            restore_to: from,
            goodput_before: goodput,
            ticks_left: GOODPUT_VERIFY_TICKS,
            samples: 0,
        });
        return Some(Adjustment {
            direction: AdjustmentDirection::Decrease,
            from,
            to,
            ticks_ago: 0,
            conflict_rate: conflict,
            goodput_before: goodput,
        });
    }

    if conflict < input.tuning.low_watermark {
        state.calm_ticks = state.calm_ticks.saturating_add(1);
        if state.calm_ticks < PROBE_DWELL_TICKS {
            return None;
        }
        state.calm_ticks = 0;
        let from = state.limit;
        let to = from.saturating_add(1).min(ceiling);
        if to == from {
            return None;
        }
        state.limit = to;
        return Some(Adjustment {
            direction: AdjustmentDirection::Increase,
            from,
            to,
            ticks_ago: 0,
            conflict_rate: conflict,
            goodput_before: goodput,
        });
    }

    // Inside the dead band: hold, and forget any partial dwell.
    state.calm_ticks = 0;
    None
}

fn saturation_hold_ticks() -> u32 {
    (SATURATION_HOLD.as_millis() / TICK_PERIOD.as_millis().max(1)) as u32
}

/// Clamp, classify, and age the bookkeeping the report reads.
fn finish(
    mut next: ConcurrencyState,
    previous: &ConcurrencyState,
    floor: usize,
    ceiling: usize,
    adjustment: Option<Adjustment>,
) -> ConcurrencyState {
    next.limit = next.limit.clamp(floor, ceiling);

    next.last_adjustment = match adjustment {
        Some(adjustment) => Some(adjustment),
        None => next.last_adjustment.map(|last| Adjustment {
            ticks_ago: last.ticks_ago.saturating_add(1),
            ..last
        }),
    };

    next.state = if next.saturation_ticks > 0 {
        ControllerState::Saturated
    } else if next.limit >= ceiling {
        ControllerState::Steady
    } else if next.calm_ticks > 0 {
        ControllerState::Probing
    } else {
        ControllerState::Throttled
    };
    next.ticks_in_state = if next.state == previous.state {
        previous.ticks_in_state.saturating_add(1)
    } else {
        0
    };
    next
}

//
// THE WINDOW
//

/// The counters the two `legion_plan_apply_total` call sites already move,
/// mirrored into relaxed atomics the controller can drain per tick.
///
/// Process-global and lock-free on purpose: the apply path pays two relaxed
/// adds and nothing else, so `mode = "observe"` — and `mode = "off"` — cost
/// nothing measurable on the plan path.
#[derive(Debug, Default)]
pub struct ConflictWindow {
    applied: AtomicU64,
    fallback: AtomicU64,
    rejected: AtomicU64,
}

/// One drained window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WindowCounts {
    pub applied: u64,
    pub fallback: u64,
    pub rejected: u64,
}

impl ConflictWindow {
    pub const fn new() -> Self {
        Self {
            applied: AtomicU64::new(0),
            fallback: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    /// A plan that bound every placement at rank 0.
    pub fn record_applied(&self) {
        self.applied.fetch_add(1, Ordering::Relaxed);
    }

    /// A plan that bound, but slid down at least one candidate list.
    pub fn record_fallback(&self) {
        self.fallback.fetch_add(1, Ordering::Relaxed);
    }

    /// A plan that lost every candidate.
    pub fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Take and zero the window.
    pub fn drain(&self) -> WindowCounts {
        WindowCounts {
            applied: self.applied.swap(0, Ordering::Relaxed),
            fallback: self.fallback.swap(0, Ordering::Relaxed),
            rejected: self.rejected.swap(0, Ordering::Relaxed),
        }
    }
}

static WINDOW: ConflictWindow = ConflictWindow::new();

/// The process-wide plan-outcome window.
pub fn conflict_window() -> &'static ConflictWindow {
    &WINDOW
}

//
// THE GATE
//

/// Admission control for planning: a mode, a limit, an in-flight count, and
/// a wakeup.
///
/// The pool is never resized. Eval workers, their supervision, and their
/// raft-leadership idling are untouched; a change of `limit` takes effect on
/// the next [`PlannerGate::admit`], with no thread churn and nothing to
/// unwind. That is the whole reason the actuator is an admission gate rather
/// than a variable worker count.
#[derive(Debug)]
pub struct PlannerGate {
    mode: AtomicU8,
    limit: AtomicUsize,
    in_flight: AtomicUsize,
    release: Notify,
}

/// What a worker got when it asked to plan.
#[derive(Debug)]
pub enum Admission {
    /// The controller is off: no counting, no limit, today's behavior exactly.
    Unrestricted,
    /// A permit, held until dropped.
    Granted(PlannerPermit),
    /// The budget is full. Take work that cannot conflict, or park.
    Throttled,
}

impl Admission {
    pub fn is_throttled(&self) -> bool {
        matches!(self, Self::Throttled)
    }

    /// The permit to hold, if there is one to hold.
    pub fn into_permit(self) -> Option<PlannerPermit> {
        match self {
            Self::Granted(permit) => Some(permit),
            Self::Unrestricted | Self::Throttled => None,
        }
    }
}

/// An admitted planner. Released on drop, which is what makes the permit
/// leak-proof across the error, conflict-requeue, and panic paths — a leaked
/// permit is a permanently wedged scheduler, so this is deliberately not a
/// manual release.
#[derive(Debug)]
pub struct PlannerPermit {
    gate: &'static PlannerGate,
}

impl Drop for PlannerPermit {
    fn drop(&mut self) {
        self.gate.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.gate.release.notify_waiters();
    }
}

impl PlannerGate {
    const fn new() -> Self {
        Self {
            mode: AtomicU8::new(ConcurrencyMode::Off as u8),
            // Until the controller publishes a limit, admit everyone.
            limit: AtomicUsize::new(usize::MAX),
            in_flight: AtomicUsize::new(0),
            release: Notify::const_new(),
        }
    }

    /// Tell the gate which mode the controller runs in. Set once at startup;
    /// `off` keeps [`PlannerGate::admit`] on exactly today's path.
    pub fn set_mode(&self, mode: ConcurrencyMode) {
        self.mode.store(mode as u8, Ordering::Relaxed);
    }

    fn mode(&self) -> ConcurrencyMode {
        match self.mode.load(Ordering::Relaxed) {
            m if m == ConcurrencyMode::Enforce as u8 => ConcurrencyMode::Enforce,
            m if m == ConcurrencyMode::Observe as u8 => ConcurrencyMode::Observe,
            _ => ConcurrencyMode::Off,
        }
    }

    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Publish a new limit. Workers see it on their next [`PlannerGate::admit`];
    /// a raised limit also wakes anyone parked on the gate.
    pub fn set_limit(&self, limit: usize) {
        let previous = self.limit.swap(limit.max(1), Ordering::Relaxed);
        if limit > previous {
            self.release.notify_waiters();
        }
    }

    /// Ask to plan.
    ///
    /// - `off`: one relaxed load and [`Admission::Unrestricted`].
    /// - `observe`: the permit is counted (so `in_flight` is real and the
    ///   shadow limit is comparable) but never refused.
    /// - `enforce`: a CAS against the published limit.
    pub fn admit(&'static self) -> Admission {
        match self.mode() {
            ConcurrencyMode::Off => Admission::Unrestricted,
            ConcurrencyMode::Observe => {
                self.in_flight.fetch_add(1, Ordering::AcqRel);
                Admission::Granted(PlannerPermit { gate: self })
            }
            ConcurrencyMode::Enforce => loop {
                let limit = self.limit.load(Ordering::Relaxed);
                let in_flight = self.in_flight.load(Ordering::Acquire);
                if in_flight >= limit {
                    return Admission::Throttled;
                }
                if self
                    .in_flight
                    .compare_exchange_weak(
                        in_flight,
                        in_flight + 1,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return Admission::Granted(PlannerPermit { gate: self });
                }
            },
        }
    }

    /// Park until a permit is released (or the limit rises). Raced against the
    /// eval notifier and the delayed-eval timer by the worker loop, so a
    /// throttled worker with no non-planning work parks exactly as it does
    /// today, with one more wakeup source.
    pub async fn released(&self) {
        self.release.notified().await;
    }
}

static GATE: PlannerGate = PlannerGate::new();

/// The process-wide planner gate.
pub fn gate() -> &'static PlannerGate {
    &GATE
}

//
// THE PUBLISHED REPORT
//

/// Everything `legion scheduler concurrency` and `GetSchedulerConcurrency`
/// answer with. Published by the tick loop, read by the RPC handler.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcurrencyReport {
    pub mode: ConcurrencyMode,
    /// Admitted planners. In `observe` this is the **shadow** limit — what the
    /// controller would be enforcing — which is what makes observe mode useful
    /// on its own.
    pub limit: usize,
    pub floor: usize,
    pub ceiling: usize,
    pub eval_threads: usize,
    pub in_flight: usize,
    pub state: ControllerState,
    pub seconds_in_state: u64,
    /// Smoothed conflict rate, or `None` before the first measured tick.
    pub conflict_rate: Option<f64>,
    /// Smoothed rate at which races were ABSORBED by candidate lists. Tracked
    /// separately from `conflict_rate` and never fed into the decision.
    pub fallback_rate: Option<f64>,
    /// Smoothed placements per second.
    pub goodput: Option<f64>,
    /// The last tick's raw counts, so the percentage can be checked by hand.
    pub window: WindowCounts,
    pub last_adjustment: Option<Adjustment>,
    /// Times a throttled worker took a non-planning eval instead of the queue
    /// head.
    pub admission_skips: u64,
}

static REPORT: Mutex<Option<ConcurrencyReport>> = Mutex::new(None);
static ADMISSION_SKIPS: AtomicU64 = AtomicU64::new(0);

/// Publish this tick's report.
pub fn publish_report(report: ConcurrencyReport) {
    if let Ok(mut slot) = REPORT.lock() {
        *slot = Some(report);
    }
}

/// The last published report, if this manager has ever run the controller.
pub fn latest_report() -> Option<ConcurrencyReport> {
    REPORT.lock().ok().and_then(|slot| slot.clone())
}

/// Clear the report — the manager stopped leading the scheduler lane, so its
/// numbers describe a stint that is over.
pub fn clear_report() {
    if let Ok(mut slot) = REPORT.lock() {
        *slot = None;
    }
}

/// A throttled worker claimed `count` non-planning evals instead of the
/// queue head. Feeds both the report and
/// `legion_scheduler_admission_skips_total`.
pub fn record_admission_skips(count: u64) {
    ADMISSION_SKIPS.fetch_add(count, Ordering::Relaxed);
    crate::manager::telemetry::metrics()
        .scheduler_admission_skips_total
        .add(count);
}

pub fn admission_skips() -> u64 {
    ADMISSION_SKIPS.load(Ordering::Relaxed)
}

#[cfg(test)]
mod concurrency_test {
    use super::*;

    fn tuning() -> ControllerTuning {
        ControllerTuning::default()
    }

    /// A tick at a given conflict rate with a given placement count, sized so
    /// it is always measurable.
    fn tick(progress: u64, rejected: u64, ceiling: usize) -> TickInput {
        TickInput {
            applied: progress,
            fallback: 0,
            rejected,
            elapsed: TICK_PERIOD,
            floor: 1,
            ceiling,
            tuning: tuning(),
        }
    }

    /// A tick whose conflict rate is `rate`, with ~200 attempts.
    fn tick_at_rate(rate: f64, ceiling: usize) -> TickInput {
        let attempts = 200.0;
        let rejected = (attempts * rate).round() as u64;
        tick(attempts as u64 - rejected, rejected, ceiling)
    }

    fn with_floor(input: TickInput, floor: usize) -> TickInput {
        TickInput { floor, ..input }
    }

    fn run(mut state: ConcurrencyState, input: &TickInput, ticks: usize) -> ConcurrencyState {
        for _ in 0..ticks {
            state = advance(&state, input);
        }
        state
    }

    #[test]
    fn a_fresh_controller_starts_at_the_ceiling_test() {
        // No evidence yet means nothing to back off from.
        let fresh = ConcurrencyState::new(16);
        assert_eq!(fresh.limit, 16);
        assert_eq!(fresh.state, ControllerState::Steady);
    }

    #[test]
    fn a_decrease_halves_once_per_refractory_window_test() {
        let mut state = ConcurrencyState::new(16);
        state.limit = 4;
        let hot = tick_at_rate(0.60, 16);

        let after = advance(&state, &hot);
        assert_eq!(after.limit, 2, "one MD from 4");
        assert_eq!(after.refractory_ticks, COOLDOWN_TICKS);

        // The refractory window absorbs the next ticks, however hot.
        let mut held = after.clone();
        for _ in 0..COOLDOWN_TICKS {
            held = advance(&held, &hot);
            assert_eq!(held.limit, 2, "no second cut inside the refractory window");
        }
        let cut = advance(&held, &hot);
        assert_eq!(cut.limit, 1, "and one more once it expires");
    }

    #[test]
    fn an_increase_needs_the_full_dwell_test() {
        let mut state = ConcurrencyState::new(16);
        state.limit = 2;
        let calm = tick_at_rate(0.0, 16);

        let mut probing = state.clone();
        for tick in 1..PROBE_DWELL_TICKS {
            probing = advance(&probing, &calm);
            assert_eq!(
                probing.limit, 2,
                "no increase after only {tick} calm tick(s)"
            );
            assert_eq!(probing.state, ControllerState::Probing);
        }
        let raised = advance(&probing, &calm);
        assert_eq!(raised.limit, 3, "one step up after the dwell");
        assert_eq!(raised.calm_ticks, 0, "and the dwell restarts");
    }

    #[test]
    fn the_dead_band_holds_the_limit_test() {
        // 12% is above LOW and below HIGH: neither direction, forever.
        let state = ConcurrencyState::new(16);
        let banded = tick_at_rate(0.12, 16);
        let settled = run(state.clone(), &banded, 200);
        assert_eq!(settled.limit, state.limit);
        assert_eq!(settled.calm_ticks, 0);
        assert!(settled.last_adjustment.is_none(), "nothing ever fired");
    }

    #[test]
    fn the_floor_and_ceiling_are_never_violated_test() {
        // Hostile forever, at a configured floor of 2. Checked every tick, not
        // just at the end: the floor is a liveness parameter, and one tick
        // below it is one tick of a scheduler that admits fewer planners than
        // the operator said it must.
        let hot = with_floor(tick_at_rate(0.90, 16), 2);
        let mut state = ConcurrencyState::new(16);
        for _ in 0..400 {
            state = advance(&state, &hot);
            assert!(
                (2..=16).contains(&state.limit),
                "limit left its bounds: {}",
                state.limit
            );
        }

        let calm = tick_at_rate(0.0, 6);
        let capped = run(ConcurrencyState::new(6), &calm, 400);
        assert_eq!(capped.limit, 6, "and the ceiling caps the climb");
        assert_eq!(capped.state, ControllerState::Steady);
    }

    #[test]
    fn a_thin_tick_is_completely_inert_test() {
        // 15 attempts, all rejected: a 100% conflict rate on no evidence.
        let state = ConcurrencyState::new(16);
        let thin = tick(0, MIN_ATTEMPTS - 1, 16);
        let after = run(state.clone(), &thin, 50);
        assert_eq!(after.limit, state.limit, "an idle cluster is not throttled");
        assert!(after.ewma_conflict.is_none(), "and nothing was averaged in");
    }

    #[test]
    fn a_tick_with_no_progress_never_decreases_test() {
        // The pure-saturation signature: everything rejected, nothing placed.
        // Backing off here is strictly harmful, so it does not happen.
        let state = ConcurrencyState::new(16);
        let stalled = tick(0, 500, 16);
        let after = run(state.clone(), &stalled, 20);
        assert_eq!(after.limit, state.limit);
        assert!(after.last_adjustment.is_none());
    }

    #[test]
    fn a_decrease_that_does_not_help_is_reverted_once_and_saturates_test() {
        let mut state = ConcurrencyState::new(16);
        state.limit = 4;
        // Hot, and goodput never improves: 40 placements against 160 rejects.
        let hot = TickInput {
            applied: 40,
            fallback: 0,
            rejected: 160,
            elapsed: TICK_PERIOD,
            floor: 1,
            ceiling: 16,
            tuning: tuning(),
        };
        let cut = advance(&state, &hot);
        assert_eq!(cut.limit, 2);
        assert!(cut.pending_decrease.is_some());

        // Sustained congestion keeps cutting on the refractory clock while the
        // verification runs — that is the "back off hard" bias, and it is why
        // the guard exists rather than replacing it. What must happen is that
        // the guard eventually notices the cuts bought nothing, undoes one,
        // and stops.
        let verified = run(cut, &hot, 20);
        assert_eq!(
            verified.last_adjustment.map(|a| a.direction),
            Some(AdjustmentDirection::Revert),
            "the decrease bought nothing, so it was undone"
        );
        assert_eq!(verified.state, ControllerState::Saturated);
        assert!(
            verified.limit > 1,
            "a saturated cluster must not be left pinned at the floor: \
             the losers were doomed either way, so fewer planners only makes \
             the one winner per round arrive later"
        );

        // And the hold means no further decrease, however hot it gets.
        let limit = verified.limit;
        let held = run(verified, &hot, 20);
        assert_eq!(
            held.limit, limit,
            "saturated: the actuator stops being applied"
        );
        assert_eq!(held.state, ControllerState::Saturated);
    }

    #[test]
    fn a_decrease_that_helps_is_kept_test() {
        let mut state = ConcurrencyState::new(16);
        state.limit = 4;
        // Just over the watermark, so one decrease is all the rule wants.
        let hot = TickInput {
            applied: 150,
            fallback: 0,
            rejected: 50,
            elapsed: TICK_PERIOD,
            floor: 1,
            ceiling: 16,
            tuning: tuning(),
        };
        let cut = advance(&state, &hot);
        assert_eq!(cut.limit, 2);

        // Backing off worked: goodput doubles and conflicts collapse.
        let better = TickInput {
            applied: 400,
            fallback: 0,
            rejected: 4,
            ..hot
        };
        let verified = run(cut, &better, GOODPUT_VERIFY_TICKS as usize + 1);
        assert!(verified.limit >= 2, "no revert");
        assert_ne!(verified.state, ControllerState::Saturated);
        assert_ne!(
            verified.last_adjustment.map(|a| a.direction),
            Some(AdjustmentDirection::Revert)
        );
    }

    #[test]
    fn saturation_permits_increases_and_expires_test() {
        let mut state = ConcurrencyState::new(16);
        state.limit = 2;
        state.saturation_ticks = saturation_hold_ticks();
        state.saturation_goodput = 1_000.0;

        // Calm and low-goodput: the hold does not lift early, but the dwell
        // still runs and the limit still climbs.
        let calm = TickInput {
            applied: 200,
            fallback: 0,
            rejected: 0,
            elapsed: TICK_PERIOD,
            floor: 1,
            ceiling: 16,
            tuning: tuning(),
        };
        let climbed = run(state.clone(), &calm, PROBE_DWELL_TICKS as usize);
        assert_eq!(climbed.limit, 3, "increases are never blocked by the hold");
        assert_eq!(climbed.state, ControllerState::Saturated);

        // And it expires on its own.
        let expired = run(state, &calm, saturation_hold_ticks() as usize + 1);
        assert_ne!(expired.state, ControllerState::Saturated);
    }

    #[test]
    fn saturation_lifts_early_when_the_pool_drains_test() {
        let mut state = ConcurrencyState::new(16);
        state.saturation_ticks = saturation_hold_ticks();
        state.saturation_goodput = 10.0;
        state.ewma_goodput = Some(10.0);
        // 400 placements in 500ms is 800/sec, well above the saturation mark.
        let draining = tick(400, 10, 16);
        let after = advance(&state, &draining);
        assert_eq!(
            after.saturation_ticks, 0,
            "the pool drained; the hold lifts"
        );
    }

    #[test]
    fn fallbacks_count_as_progress_not_as_conflict_test() {
        // 200 plans, every one of them absorbed a race by falling back a rank.
        // That is late binding WORKING; throttling on it would be throttling to
        // avoid the feature working.
        let absorbed = TickInput {
            applied: 0,
            fallback: 200,
            rejected: 0,
            elapsed: TICK_PERIOD,
            floor: 1,
            ceiling: 16,
            tuning: tuning(),
        };
        assert_eq!(absorbed.conflict_rate(), Some(0.0));
        // From a throttled 4, 4 -> 16 is twelve increases at one dwell (4 ticks) each.
        let mut throttled = ConcurrencyState::new(16);
        throttled.limit = 4;
        let climbed = run(throttled, &absorbed, 12 * 4);
        assert_eq!(climbed.limit, 16, "it climbed to the ceiling, not away");
    }

    /// The design's retrodiction of the three measured AWS points (plan
    /// §11.1), driven through the pure rule. The first row is the one that can
    /// fail quietly and matters most: a controller that throttles the
    /// best-performing configuration ever measured is worse than no
    /// controller.
    #[test]
    fn the_measured_aws_points_replay_to_their_expected_limits_test() {
        // evalThreads = 4, 1,000 nodes, 15.9% rejected, 354.6 alloc/sec.
        let good = TickInput {
            applied: 149,
            fallback: 0,
            rejected: 28,
            elapsed: TICK_PERIOD,
            floor: 1,
            ceiling: 4,
            tuning: tuning(),
        };
        let settled = run(ConcurrencyState::new(4), &good, 120);
        assert_eq!(
            settled.limit, 4,
            "15.9% is inside the healthy regime; do not throttle the best \
             operating point ever measured"
        );

        // evalThreads = 4, through 5,000 nodes, 31.9% rejected.
        let stretched = TickInput {
            applied: 136,
            fallback: 0,
            rejected: 64,
            elapsed: TICK_PERIOD,
            floor: 1,
            ceiling: 4,
            tuning: tuning(),
        };
        // 20 ticks is 10s: long enough for the descent and the goodput verdict
        // that follows it, and inside the saturation hold the verdict opens.
        let settled = run(ConcurrencyState::new(4), &stretched, 20);
        assert_eq!(settled.limit, 2, "one MD's worth of backing off, then hold");

        // evalThreads = 16, through 5,000 nodes, 73.6% rejected. Must be at or
        // below 3 within 10s (20 ticks) of the burst starting.
        let collapsed = TickInput {
            applied: 53,
            fallback: 0,
            rejected: 147,
            elapsed: TICK_PERIOD,
            floor: 1,
            ceiling: 16,
            tuning: tuning(),
        };
        let mut state = ConcurrencyState::new(16);
        state.limit = 16;
        let settled = run(state, &collapsed, 20);
        assert!(
            settled.limit <= 3,
            "16 planners at a 73.6% conflict rate must be down to <= 3 within \
             10s, was {}",
            settled.limit
        );
    }

    /// Hunting is a failure, not a cosmetic issue: over a 10-minute soak on a
    /// bimodal trace the controller must make at most 6 adjustments per minute
    /// in steady state (plan §11.5).
    #[test]
    fn a_bimodal_trace_does_not_hunt_test() {
        // Alternating 4-tick bursts of conflict-prone and conflict-free work —
        // the resonant case §8.1 admits will sawtooth, bounded to +-1 by the
        // integer quantization.
        let hot = tick_at_rate(0.35, 16);
        let calm = tick_at_rate(0.01, 16);
        let mut state = ConcurrencyState::new(16);
        let mut adjustments = 0usize;
        // 10 minutes at 500ms ticks.
        for tick in 0..1_200 {
            let input = if (tick / 4) % 2 == 0 { &hot } else { &calm };
            let next = advance(&state, input);
            if next
                .last_adjustment
                .is_some_and(|adjustment| adjustment.ticks_ago == 0)
            {
                adjustments += 1;
            }
            state = next;
        }
        // 6 per minute over 10 minutes.
        assert!(
            adjustments <= 60,
            "controller hunted: {adjustments} adjustments in 10 minutes"
        );
    }

    #[test]
    fn settings_reject_what_would_silently_do_the_wrong_thing_test() {
        let parse = |raw: Value| {
            AdaptiveConcurrencySettings::from_scheduler_settings(Some(&json!({
                "adaptiveConcurrency": raw
            })))
        };

        assert_eq!(
            AdaptiveConcurrencySettings::from_scheduler_settings(None).unwrap(),
            AdaptiveConcurrencySettings::default()
        );
        assert_eq!(
            parse(json!({"mode": "enforce", "min": 2, "max": 8})).unwrap(),
            AdaptiveConcurrencySettings {
                mode: ConcurrencyMode::Enforce,
                min: 2,
                max: Some(8),
                ..AdaptiveConcurrencySettings::default()
            }
        );

        for bad in [
            json!({"mode": "enforc"}),
            json!({"mode": true}),
            json!({"min": 0}),
            json!({"min": 8, "max": 4}),
            json!({"highWatermark": 1.5}),
            json!({"lowWatermark": 0.5, "highWatermark": 0.2}),
            json!([]),
        ] {
            assert!(
                parse(bad.clone()).is_err(),
                "must be rejected rather than defaulted: {bad}"
            );
        }
    }

    #[test]
    fn eval_threads_always_caps_the_ceiling_test() {
        let settings = AdaptiveConcurrencySettings {
            max: Some(64),
            min: 2,
            ..AdaptiveConcurrencySettings::default()
        };
        assert_eq!(settings.bounds(8), (2, 8), "max may not exceed evalThreads");

        let settings = AdaptiveConcurrencySettings {
            max: Some(4),
            min: 8,
            ..AdaptiveConcurrencySettings::default()
        };
        assert_eq!(settings.bounds(16), (4, 4), "and the floor clamps into it");
    }

    #[test]
    fn only_job_stop_bypasses_the_gate_test() {
        // Every other kind reaches the planner on at least one path, so the
        // classification is an allowlist and a new kind defaults to throttled.
        assert!(!is_planning_kind("job_stop"));
        for kind in [
            "job_submit",
            "job_update",
            "job_reschedule",
            "job_scale",
            "rollout_tick",
            "node_join",
            "node_drop",
            "node_drain",
            "node_lost_reschedule",
            "some_future_kind",
        ] {
            assert!(is_planning_kind(kind), "{kind} must hold a permit");
        }
    }

    /// A gate of its own, so these tests never touch the process-wide one.
    fn private_gate(mode: ConcurrencyMode, limit: usize) -> &'static PlannerGate {
        let gate: &'static PlannerGate = Box::leak(Box::new(PlannerGate::new()));
        gate.set_mode(mode);
        gate.set_limit(limit);
        gate
    }

    #[test]
    fn the_gate_admits_exactly_its_limit_test() {
        let gate = private_gate(ConcurrencyMode::Enforce, 2);

        let first = gate.admit().into_permit().expect("under the limit");
        let second = gate.admit().into_permit().expect("at the limit");
        assert!(gate.admit().is_throttled(), "over the limit");
        assert_eq!(gate.in_flight(), 2);

        // A permit is released by dropping it, on every path.
        drop(first);
        assert_eq!(gate.in_flight(), 1);
        let third = gate
            .admit()
            .into_permit()
            .expect("the freed slot is reusable");

        // And a raised limit takes effect on the next admit, with no churn.
        gate.set_limit(4);
        let fourth = gate
            .admit()
            .into_permit()
            .expect("the raised limit admits more");
        drop((second, third, fourth));
        assert_eq!(gate.in_flight(), 0, "no permit leaked");
    }

    #[test]
    fn observe_counts_but_never_refuses_and_off_never_counts_test() {
        let gate = private_gate(ConcurrencyMode::Observe, 1);
        let first = gate.admit().into_permit().expect("counted");
        let second = gate
            .admit()
            .into_permit()
            .expect("never refused, even over the limit");
        assert_eq!(
            gate.in_flight(),
            2,
            "the shadow limit has a real in-flight to compare to"
        );
        drop((first, second));

        let gate = private_gate(ConcurrencyMode::Off, 1);
        assert!(matches!(gate.admit(), Admission::Unrestricted));
        assert!(matches!(gate.admit(), Admission::Unrestricted));
        assert_eq!(
            gate.in_flight(),
            0,
            "off is today's path: nothing is counted"
        );
    }

    #[test]
    fn a_permit_is_released_even_when_its_holder_panics_test() {
        let gate = private_gate(ConcurrencyMode::Enforce, 1);
        let permit = gate.admit().into_permit().expect("admitted");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _permit = permit;
            panic!("the handler blew up");
        }));
        assert!(result.is_err());
        assert_eq!(
            gate.in_flight(),
            0,
            "a leaked permit is a permanently wedged scheduler"
        );
        assert!(
            gate.admit().into_permit().is_some(),
            "and the slot is usable again"
        );
    }

    #[test]
    fn the_window_drains_to_zero_test() {
        let window = ConflictWindow::new();
        window.record_applied();
        window.record_applied();
        window.record_fallback();
        window.record_rejected();
        assert_eq!(
            window.drain(),
            WindowCounts {
                applied: 2,
                fallback: 1,
                rejected: 1
            }
        );
        assert_eq!(window.drain(), WindowCounts::default());
    }
}
