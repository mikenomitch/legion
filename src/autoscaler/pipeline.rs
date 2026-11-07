//! The safeguard pipeline: one fixed order of stages that turns checks into
//! a decision, and records why (plan §6.5, calls entry 6).
//!
//! ```text
//!  checks ──▶ recommendations ──▶ merge (max) ──▶ headroom ──▶ stabilize ──▶ hold ──▶ step
//!   ──▶ bounds (min/max, schedule) ──▶ quota ──▶ capacity ──▶ rollout ──▶ pin ──▶ pause ──▶ mode
//! ```
//!
//! | Stage | What it does |
//! | --- | --- |
//! | recommendations | each check's strategy turns its reading into a desired count (tolerance band and the warmup rule live here, see [`strategies`](super::strategies)); a strategy plugin's answer arrives in the observation ([`Observation::recommendations`]); a check with no data contributes nothing |
//! | [`Stage::Merge`] | the max over checks with data; a `scaleIn = false` check counts only when it wants more than the current count. With `activation` checks: at zero only an active one can wake the target, and a `min = 0` target goes idle (0) once every activation check reads at or below its activation |
//! | [`Stage::Headroom`] | adds `headroom` replicas |
//! | [`Stage::Stabilize`] | per direction (HPA): scale-out takes the *lowest* recommendation in `up.stabilize`, scale-in the *highest* in `down.stabilize` |
//! | [`Stage::Hold`] | scale-in holds: cold start (no scale-in until one `down.stabilize` of samples exists since the engine started), missing data (`onMissing = hold`), and the flap guard (3 direction changes in 10 minutes hold scale-in for `2 × down.stabilize`) |
//! | [`Stage::Step`] | the rolling step limit per direction: the larger of `percent` of the count at the start of the `every` window and `replicas`, minus what was applied in the window in that direction |
//! | [`Stage::Bounds`] | clamps into `min..max`, with open schedule windows raising the floor or lowering the ceiling. After the step limit on purpose: a raised floor or a lowered ceiling applies at once |
//! | [`Stage::Quota`] | caps an increase at the namespace quota headroom (an input) |
//! | [`Stage::Capacity`] | holds an increase while more than half of the last scale-out is unplaced, unless a node group can host it (inputs) |
//! | [`Stage::Rollout`] | defers scale-in while a rollout is active (an input) |
//! | [`Stage::Pin`] | an operator's pin replaces the result (and may exceed `max`); a pinned floor or ceiling clamps it |
//! | [`Stage::Pause`] | a pause keeps the current count |
//! | [`Stage::Mode`] | `dry-run` turns a change into a "would" |
//!
//! Every stage records a [`StageNote`] with the number it received, the
//! number it passed on, and a note. The notes *are* the explanation: the
//! decision's one-line [`Decision::summary`] is built from the winning check
//! and the notes of the stages that changed the number, so it can never
//! drift from what happened.
//!
//! The pipeline is pure apart from [`PolicyState`], which holds the
//! per-policy windows (recommendation history, applied changes, flap
//! guard). Time is always passed in (`now_ms`, unix milliseconds).

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::defaults::{EffectiveCheck, EffectivePolicy};
use super::schedule::scheduled_bounds;
use super::spec::{format_duration, Mode, OnMissing};
use super::strategies::StrategyInput;

/// The flap guard's window: direction changes are counted over this long.
pub const FLAP_WINDOW: Duration = Duration::from_secs(600);
/// Direction changes within [`FLAP_WINDOW`] that count as flapping.
pub const FLAP_REVERSALS: u32 = 3;

fn ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX / 4)
}

fn clock(at_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(at_ms)
        .map(|at| at.format("%H:%M UTC").to_string())
        .unwrap_or_else(|| at_ms.to_string())
}

/// One check's reading for one evaluation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Reading {
    /// The value the source returned.
    Value(f64),
    /// No data, and why (`timeout`, `no series`, ...).
    Missing(String),
}

/// A strategy plugin's answer for one check, which the autoscaler fetches
/// before the pipeline runs (a plugin does I/O; the pipeline is pure).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRecommendation {
    /// The desired count, when the plugin answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired: Option<u32>,
    /// What the plugin said it saw.
    #[serde(default)]
    pub note: String,
    /// Why there is no answer (the plugin failed, timed out, or refused).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// An operator pin (`legion job scale --count 12 --for 2h`). Active until
/// `until_ms`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pin {
    /// Replaces the result outright, even above `max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
    /// A pinned floor (`--min`): still autoscaled above it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<u32>,
    /// A pinned ceiling (`--max`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
    /// When the pin expires (unix ms).
    pub until_ms: i64,
    /// Who pinned.
    pub by: String,
    /// Why.
    #[serde(default)]
    pub reason: String,
}

/// A pause (`legion scaling pause`): nothing is written while it holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pause {
    /// When the pause ends (unix ms); `None` holds until resumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_ms: Option<i64>,
    /// Who paused.
    pub by: String,
    /// Why.
    #[serde(default)]
    pub reason: String,
}

/// The namespace quota guard's input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaGuard {
    /// How many more replicas the namespace quota admits for this target's
    /// band.
    pub headroom: u32,
    /// What the limit is, for the note
    /// (`namespace quota payments-prod (normal: 12/12 cpu)`).
    pub detail: String,
}

/// The capacity guard's input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapacityGuard {
    /// Replicas the scheduler could not place.
    pub unplaced: u32,
    /// Whether some node-group policy could host the unplaced work.
    pub node_group_fits: bool,
    /// The size of the last scale-out, if the caller knows it better than
    /// the engine's own history (after a restart, from `scale_history`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scale_out: Option<u32>,
}

/// Cluster-aware guards, computed by the caller and passed in.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Guards {
    /// The namespace quota, if the namespace has one for this band.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<QuotaGuard>,
    /// Placement state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<CapacityGuard>,
    /// The id of the target's active rollout, if one is running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout: Option<String>,
}

/// Everything one evaluation sees about its target.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    /// The target's current desired count.
    pub current: u32,
    /// Ready replicas (healthy and past warmup).
    pub ready: u32,
    /// Replicas that are healthy but still warming.
    pub warming: u32,
    /// Readings keyed by check name (`cpu`, `memory`, or an explicit
    /// check's name). A check with no entry has no data.
    pub readings: BTreeMap<String, Reading>,
    /// Cluster-aware guards.
    pub guards: Guards,
    /// The active pin, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<Pin>,
    /// The active pause, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<Pause>,
    /// Strategy plugins' answers, keyed by check name. A check whose
    /// strategy is a plugin and has no entry has no data.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub recommendations: BTreeMap<String, PluginRecommendation>,
}

impl Observation {
    /// A target at `current` with every replica ready and no readings.
    pub fn new(current: u32) -> Self {
        Observation {
            current,
            ready: current,
            ..Default::default()
        }
    }

    /// Add a reading.
    pub fn reading(mut self, check: &str, value: f64) -> Self {
        self.readings
            .insert(check.to_string(), Reading::Value(value));
        self
    }

    /// Mark a check as having no data, with a reason.
    pub fn missing(mut self, check: &str, reason: &str) -> Self {
        self.readings
            .insert(check.to_string(), Reading::Missing(reason.to_string()));
        self
    }

    /// Set the ready and warming counts.
    pub fn readiness(mut self, ready: u32, warming: u32) -> Self {
        self.ready = ready;
        self.warming = warming;
        self
    }
}

/// The pipeline stages after the per-check recommendations, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Stage {
    /// Max over checks.
    Merge,
    /// `headroom` replicas added.
    Headroom,
    /// Per-direction stabilization windows.
    Stabilize,
    /// Scale-in holds: cold start, missing data, flap guard.
    Hold,
    /// The rolling step limit.
    Step,
    /// `min`/`max` and schedules.
    Bounds,
    /// Namespace quota headroom.
    Quota,
    /// Unplaced replicas.
    Capacity,
    /// Active rollout defers scale-in.
    Rollout,
    /// Operator pin.
    Pin,
    /// Pause.
    Pause,
    /// `enforce` or `dry-run`.
    Mode,
}

impl Stage {
    /// Every stage in pipeline order.
    pub const ORDER: [Stage; 12] = [
        Stage::Merge,
        Stage::Headroom,
        Stage::Stabilize,
        Stage::Hold,
        Stage::Step,
        Stage::Bounds,
        Stage::Quota,
        Stage::Capacity,
        Stage::Rollout,
        Stage::Pin,
        Stage::Pause,
        Stage::Mode,
    ];
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Stage::Merge => "merge",
            Stage::Headroom => "headroom",
            Stage::Stabilize => "stabilize",
            Stage::Hold => "hold",
            Stage::Step => "step",
            Stage::Bounds => "bounds",
            Stage::Quota => "quota",
            Stage::Capacity => "capacity",
            Stage::Rollout => "rollout",
            Stage::Pin => "pin",
            Stage::Pause => "pause",
            Stage::Mode => "mode",
        })
    }
}

/// What one stage did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StageNote {
    /// The stage.
    pub stage: Stage,
    /// The number it received.
    pub before: u32,
    /// The number it passed on.
    pub after: u32,
    /// Why (always set when the number changed; informative otherwise).
    pub note: String,
}

impl StageNote {
    /// Whether the stage changed the number.
    pub fn changed(&self) -> bool {
        self.before != self.after
    }
}

/// One check's part in a decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckOutcome {
    /// The check's name.
    pub name: String,
    /// The reading, when there was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reading: Option<f64>,
    /// Why there was no data, when there was none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<String>,
    /// What the check wants, when it has data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired: Option<u32>,
    /// What the strategy saw (`0.91 vs 0.70 (4 ready)`).
    pub detail: String,
    /// The strategy held inside its tolerance band.
    pub within_tolerance: bool,
    /// Whether the check took part in the merge.
    pub counted: bool,
    /// For a check with `activation`: whether it read above it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

/// What became of a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DecisionKind {
    /// `enforce` and the count changes: the caller writes it, and calls
    /// [`PolicyState::record_applied`] once the write succeeds.
    Applied,
    /// The count stays where it is.
    Held,
    /// `dry-run` and the count would have changed; nothing is written.
    Would,
}

impl fmt::Display for DecisionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DecisionKind::Applied => "applied",
            DecisionKind::Held => "held",
            DecisionKind::Would => "would",
        })
    }
}

/// A state change worth an event (plan §6.6). Each is emitted once per
/// episode, never per tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "flag")]
pub enum Flag {
    /// `scale.limited`: a bound or guard started holding the count back.
    Limited {
        /// What held it, with a hint (`at max 8 (raise autoscale.max)`).
        reason: String,
    },
    /// `scale.flapping`: the flap guard engaged.
    Flapping {
        /// Direction changes seen in the window.
        reversals: u32,
        /// When the scale-in hold ends (unix ms).
        until_ms: i64,
    },
}

/// The outcome of one evaluation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Decision {
    /// When it was made (unix ms).
    pub at_ms: i64,
    /// The count before.
    pub from: u32,
    /// The count after (equal to `from` when held).
    pub to: u32,
    /// Applied, held, or would.
    pub kind: DecisionKind,
    /// Ready replicas the decision saw.
    pub ready: u32,
    /// Warming replicas the decision saw.
    pub warming: u32,
    /// Each check's reading and recommendation.
    pub checks: Vec<CheckOutcome>,
    /// Every stage, in order.
    pub stages: Vec<StageNote>,
    /// The one-line why: `cpu 0.91 vs 0.70 (4 ready); rate-limited from 7`.
    pub summary: String,
    /// The digest of the effective policy that decided.
    pub policy_digest: String,
    /// Events to emit.
    pub flags: Vec<Flag>,
}

impl Decision {
    /// The note of one stage.
    pub fn stage(&self, stage: Stage) -> Option<&StageNote> {
        self.stages.iter().find(|note| note.stage == stage)
    }
}

/// One applied change, for step windows and the flap guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppliedChange {
    /// When the write succeeded (unix ms).
    pub at_ms: i64,
    /// The count before.
    pub from: u32,
    /// The count after.
    pub to: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Sample {
    at_ms: i64,
    desired: u32,
}

/// The per-policy memory the pipeline keeps between evaluations. It lives
/// in the autoscaler app, so it survives a manager failover; an app restart
/// starts it fresh, which is what the cold-start hold covers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyState {
    started_at_ms: Option<i64>,
    recommendations: VecDeque<Sample>,
    applied: VecDeque<AppliedChange>,
    last_scale_out: Option<u32>,
    flap_hold_until_ms: Option<i64>,
    flap_trigger_ms: Option<i64>,
    limited: Option<Stage>,
}

impl PolicyState {
    /// State for an engine that started at `started_at_ms` (the cold-start
    /// hold counts from here). A default state starts at its first
    /// evaluation.
    pub fn new(started_at_ms: i64) -> Self {
        PolicyState {
            started_at_ms: Some(started_at_ms),
            ..Default::default()
        }
    }

    /// Record a change that was written. Call it when a write succeeds
    /// (never for a `would`), and to seed the step and flap windows from
    /// `scale_history` after a restart. Writes by people count too: they
    /// spend the same step budget. Calls must come in time order.
    pub fn record_applied(&mut self, at_ms: i64, from: u32, to: u32) {
        if from == to {
            return;
        }
        if to > from {
            self.last_scale_out = Some(to - from);
        }
        self.applied.push_back(AppliedChange { at_ms, from, to });
    }

    /// The applied changes still inside a window.
    pub fn applied(&self) -> impl Iterator<Item = &AppliedChange> {
        self.applied.iter()
    }

    /// The size of the last applied scale-out.
    pub fn last_scale_out(&self) -> Option<u32> {
        self.last_scale_out
    }

    /// When the flap guard's scale-in hold ends, if it is engaged.
    pub fn flap_hold_until(&self, now_ms: i64) -> Option<i64> {
        self.flap_hold_until_ms.filter(|until| *until > now_ms)
    }

    fn prune(&mut self, policy: &EffectivePolicy, now_ms: i64) {
        let keep_samples = ms(policy.up.stabilize.max(policy.down.stabilize));
        while self
            .recommendations
            .front()
            .is_some_and(|s| s.at_ms <= now_ms - keep_samples && s.at_ms != now_ms)
        {
            self.recommendations.pop_front();
        }
        let keep_applied = ms(policy.up.every.max(policy.down.every).max(FLAP_WINDOW));
        while self
            .applied
            .front()
            .is_some_and(|c| c.at_ms <= now_ms - keep_applied)
        {
            self.applied.pop_front();
        }
    }

    /// Engage the flap guard if the applied changes in the last
    /// [`FLAP_WINDOW`] reversed direction [`FLAP_REVERSALS`] times since
    /// the last episode.
    fn detect_flapping(&mut self, policy: &EffectivePolicy, now_ms: i64) -> Option<Flag> {
        let since = now_ms - ms(FLAP_WINDOW);
        let changes: Vec<&AppliedChange> =
            self.applied.iter().filter(|c| c.at_ms > since).collect();
        let mut reversals = 0;
        let mut previous: Option<bool> = None;
        for change in &changes {
            let up = change.to > change.from;
            if previous.is_some_and(|was_up| was_up != up) {
                reversals += 1;
            }
            previous = Some(up);
        }
        let newest = changes.last()?.at_ms;
        let fresh = self.flap_trigger_ms.is_none_or(|trigger| newest > trigger);
        if reversals >= FLAP_REVERSALS && fresh && self.flap_hold_until(now_ms).is_none() {
            let until_ms = now_ms + 2 * ms(policy.down.stabilize);
            self.flap_hold_until_ms = Some(until_ms);
            self.flap_trigger_ms = Some(newest);
            return Some(Flag::Flapping {
                reversals,
                until_ms,
            });
        }
        None
    }

    fn step_window(&self, every: Duration, now_ms: i64) -> impl Iterator<Item = &AppliedChange> {
        let since = now_ms - ms(every);
        self.applied.iter().filter(move |c| c.at_ms > since)
    }
}

/// Builds the stage list as the number moves through the pipeline.
struct Trace {
    value: u32,
    stages: Vec<StageNote>,
}

impl Trace {
    fn record(&mut self, stage: Stage, after: u32, note: impl Into<String>) {
        self.stages.push(StageNote {
            stage,
            before: self.value,
            after,
            note: note.into(),
        });
        self.value = after;
    }
}

fn outcome_for(
    policy: &EffectivePolicy,
    check: &EffectiveCheck,
    obs: &Observation,
) -> CheckOutcome {
    let strategy = check.strategy.strategy();
    let mut outcome = CheckOutcome {
        name: check.name.clone(),
        reading: None,
        missing: None,
        desired: None,
        detail: String::new(),
        within_tolerance: false,
        counted: false,
        active: None,
    };
    if let Some(plugin) = check.strategy.plugin() {
        // A strategy plugin: the reading when the check has a query, and
        // the answer the autoscaler fetched for it.
        let reading = if check.query.trim().is_empty() {
            None
        } else {
            match obs.readings.get(&check.name) {
                Some(Reading::Value(value)) if value.is_finite() => Some(*value),
                Some(Reading::Value(value)) => {
                    let why = format!("reading {value} is not a number");
                    outcome.detail = format!("no data ({why})");
                    outcome.missing = Some(why);
                    return outcome;
                }
                Some(Reading::Missing(why)) => {
                    outcome.detail = format!("no data ({why})");
                    outcome.missing = Some(why.clone());
                    return outcome;
                }
                None => {
                    outcome.detail = "no data (no data)".to_string();
                    outcome.missing = Some("no data".to_string());
                    return outcome;
                }
            }
        };
        match obs.recommendations.get(&check.name) {
            Some(PluginRecommendation {
                desired: Some(desired),
                note,
                ..
            }) => {
                outcome.reading = reading;
                outcome.desired = Some(*desired);
                outcome.detail = if note.is_empty() {
                    format!("plugin {} wants {desired}", plugin.name)
                } else {
                    note.clone()
                };
                outcome.active = check
                    .activation
                    .map(|activation| reading.is_some_and(|value| value > activation));
            }
            other => {
                let why = other
                    .and_then(|answer| answer.error.clone())
                    .unwrap_or_else(|| format!("strategy plugin {} did not answer", plugin.name));
                outcome.reading = reading;
                outcome.detail = format!("no data ({why})");
                outcome.missing = Some(why);
            }
        }
        return outcome;
    }
    let reading = if strategy.needs_reading() {
        let value = match obs.readings.get(&check.name) {
            Some(Reading::Value(value)) if value.is_finite() => Ok(*value),
            Some(Reading::Value(value)) => Err(format!("reading {value} is not a number")),
            Some(Reading::Missing(reason)) => Err(reason.clone()),
            None => Err("no data".to_string()),
        };
        let value = value.and_then(|value| {
            if check.per_ready_replica() && obs.ready == 0 {
                Err("no ready replicas to measure".to_string())
            } else {
                Ok(value)
            }
        });
        match value {
            Ok(value) => value,
            Err(reason) => {
                outcome.detail = format!("no data ({reason})");
                outcome.missing = Some(reason);
                return outcome;
            }
        }
    } else {
        0.0
    };
    let recommendation = strategy.recommend(&StrategyInput {
        current: obs.current,
        reading,
        ready: obs.ready,
        tolerance: policy.tolerance,
        per_ready_replica: check.per_ready_replica(),
    });
    if strategy.needs_reading() {
        outcome.reading = Some(reading);
    }
    outcome.desired = Some(recommendation.desired);
    outcome.detail = recommendation.detail;
    outcome.within_tolerance = recommendation.within_tolerance;
    outcome.active = check.activation.map(|activation| reading > activation);
    outcome
}

/// Run one evaluation of `policy` for the target described by `obs`.
///
/// Mutates `state`: records this evaluation's recommendation for the
/// stabilization windows, prunes old samples, and engages the flap guard.
/// It does **not** record the decision as applied: call
/// [`PolicyState::record_applied`] once the write succeeds.
pub fn evaluate(
    policy: &EffectivePolicy,
    state: &mut PolicyState,
    obs: &Observation,
    now_ms: i64,
) -> Decision {
    let started_ms = *state.started_at_ms.get_or_insert(now_ms);
    state.prune(policy, now_ms);
    let mut flags = Vec::new();
    if let Some(flag) = state.detect_flapping(policy, now_ms) {
        flags.push(flag);
    }
    let current = obs.current;
    let checks = policy.all_checks();
    let mut outcomes: Vec<CheckOutcome> = checks
        .iter()
        .map(|check| outcome_for(policy, check, obs))
        .collect();

    // Missing data: `ignore` drops the check; `hold` blocks scale-in.
    let missing: Vec<String> = outcomes
        .iter()
        .filter_map(|o| {
            o.missing
                .as_ref()
                .map(|why| format!("{} has no data ({why})", o.name))
        })
        .collect();
    let missing_hold =
        (policy.on_missing == OnMissing::Hold && !missing.is_empty()).then(|| missing.join("; "));

    // ── merge ────────────────────────────────────────────────────────────
    let has_activation = checks.iter().any(|check| check.activation.is_some());
    let mut merge_parts = Vec::new();
    let mut merged: Option<u32> = None;
    let mut driver: Option<usize> = None;
    if current == 0 && has_activation {
        // Waking from zero always takes the activation path.
        for (index, (check, outcome)) in checks.iter().zip(outcomes.iter_mut()).enumerate() {
            if check.activation.is_some() && outcome.active == Some(true) {
                let wants = outcome.desired.unwrap_or(1).max(1);
                outcome.counted = true;
                if merged.is_none_or(|m| wants > m) {
                    merged = Some(wants);
                    driver = Some(index);
                }
            }
        }
        if merged.is_none() {
            merged = Some(0);
            merge_parts.push("asleep: no activation check is above its activation".to_string());
        } else if let Some(index) = driver {
            merge_parts.push(format!("woken by {}", outcomes[index].name));
        }
    } else {
        for (index, (check, outcome)) in checks.iter().zip(outcomes.iter_mut()).enumerate() {
            let Some(wants) = outcome.desired else {
                continue;
            };
            if !check.scale_in && wants <= current {
                merge_parts.push(format!(
                    "{} (scaleIn = false) wants {wants}: not counted",
                    outcome.name
                ));
                continue;
            }
            outcome.counted = true;
            if merged.is_none_or(|m| wants > m) {
                merged = Some(wants);
                driver = Some(index);
            }
        }
        let voters: Vec<String> = outcomes
            .iter()
            .filter(|o| o.counted)
            .map(|o| format!("{} wants {}", o.name, o.desired.unwrap_or(0)))
            .collect();
        if voters.len() > 1 {
            merge_parts.insert(
                0,
                format!("max of {} checks: {}", voters.len(), voters.join(", ")),
            );
        } else if let Some(only) = voters.first() {
            merge_parts.insert(0, only.clone());
        }
        if has_activation && policy.min == 0 && current >= 1 {
            let activation: Vec<&CheckOutcome> = checks
                .iter()
                .zip(outcomes.iter())
                .filter(|(check, _)| check.activation.is_some())
                .map(|(_, outcome)| outcome)
                .collect();
            let idle = activation.iter().all(|o| o.active == Some(false));
            if idle {
                merged = Some(0);
                driver = None;
                merge_parts
                    .push("idle: every activation check is at or below its activation".to_string());
            } else if let Some(m) = merged {
                merged = Some(m.max(1));
            }
        }
    }
    if policy.on_missing == OnMissing::Ignore {
        for outcome in outcomes.iter().filter(|o| o.missing.is_some()) {
            merge_parts.push(format!("{} ignored: no data", outcome.name));
        }
    }
    let all_within = outcomes
        .iter()
        .filter(|o| o.counted)
        .all(|o| o.within_tolerance);
    let mut trace = Trace {
        value: current,
        stages: Vec::new(),
    };
    let signal = merged;
    match signal {
        Some(value) => {
            if all_within && value == current && driver.is_some() {
                merge_parts.push("within tolerance".to_string());
            }
            trace.record(Stage::Merge, value, merge_parts.join("; "));
        }
        None => {
            let why = if outcomes.iter().any(|o| o.desired.is_some()) {
                "no check voted"
            } else {
                "no check has data"
            };
            merge_parts.insert(0, format!("{why}; holding at {current}"));
            trace.record(Stage::Merge, current, merge_parts.join("; "));
        }
    }

    // ── headroom ─────────────────────────────────────────────────────────
    match signal {
        Some(value) if value > 0 && policy.headroom > 0 => {
            let after = value.saturating_add(policy.headroom);
            trace.record(
                Stage::Headroom,
                after,
                format!("+{} headroom", policy.headroom),
            );
        }
        _ => {
            let value = trace.value;
            trace.record(Stage::Headroom, value, "");
        }
    }

    // ── stabilize ────────────────────────────────────────────────────────
    let recommendation = trace.value;
    if signal.is_some() {
        state.recommendations.push_back(Sample {
            at_ms: now_ms,
            desired: recommendation,
        });
        let in_window = |window: Duration| {
            let since = now_ms - ms(window);
            state
                .recommendations
                .iter()
                .filter(move |s| s.at_ms > since || s.at_ms == now_ms)
                .map(|s| s.desired)
        };
        let up_rec = in_window(policy.up.stabilize)
            .min()
            .unwrap_or(recommendation);
        let down_rec = in_window(policy.down.stabilize)
            .max()
            .unwrap_or(recommendation);
        let mut value = current;
        if value < up_rec {
            value = up_rec;
        }
        if value > down_rec {
            value = down_rec;
        }
        let note = if recommendation > current {
            if policy.up.stabilize.is_zero() {
                "up: no window".to_string()
            } else {
                format!(
                    "up: {} min is {up_rec}",
                    format_duration(policy.up.stabilize)
                )
            }
        } else if recommendation < current {
            if policy.down.stabilize.is_zero() {
                "down: no window".to_string()
            } else {
                format!(
                    "down: {} max is {down_rec}",
                    format_duration(policy.down.stabilize)
                )
            }
        } else {
            "steady".to_string()
        };
        trace.record(Stage::Stabilize, value, note);
    } else {
        trace.record(Stage::Stabilize, current, "no recommendation to stabilize");
    }

    // ── hold (scale-in only) ─────────────────────────────────────────────
    {
        let mut reasons = Vec::new();
        let elapsed = now_ms - started_ms;
        if elapsed < ms(policy.down.stabilize) {
            reasons.push(format!(
                "collecting {} of history after restart",
                format_duration(policy.down.stabilize)
            ));
        }
        if let Some(why) = &missing_hold {
            reasons.push(why.clone());
        }
        if let Some(until) = state.flap_hold_until(now_ms) {
            reasons.push(format!(
                "flapping ({FLAP_REVERSALS} reversals in {}) until {}",
                format_duration(FLAP_WINDOW),
                clock(until)
            ));
        }
        let value = trace.value;
        if value < current && !reasons.is_empty() {
            trace.record(
                Stage::Hold,
                current,
                format!("held: {}", reasons.join("; ")),
            );
        } else {
            trace.record(Stage::Hold, value, reasons.join("; "));
        }
    }

    // ── step ─────────────────────────────────────────────────────────────
    {
        let value = trace.value;
        let (behavior, up) = if value >= current {
            (&policy.up, true)
        } else {
            (&policy.down, false)
        };
        let window: Vec<&AppliedChange> = state.step_window(behavior.every, now_ms).collect();
        let base = window.first().map(|c| c.from).unwrap_or(current);
        let used: u32 = window
            .iter()
            .filter(|c| (c.to > c.from) == up)
            .map(|c| c.to.abs_diff(c.from))
            .sum();
        let percent = f64::from(base) * behavior.step.percent / 100.0;
        let from_percent = if up {
            (percent - 1e-9).ceil().max(0.0) as u32
        } else {
            (percent + 1e-9).floor().max(0.0) as u32
        };
        let budget = from_percent.max(behavior.step.replicas);
        let remaining = budget.saturating_sub(used);
        let every = format_duration(behavior.every);
        let (sign, limit) = if up {
            ('+', current.saturating_add(remaining))
        } else {
            ('-', current.saturating_sub(remaining))
        };
        let limits = format!(
            "{} ≤ {sign}{budget} per {every}; {sign}{used} used",
            if up { "up" } else { "down" }
        );
        if (up && value > limit) || (!up && value < limit) {
            trace.record(
                Stage::Step,
                limit,
                format!("rate-limited {value} → {limit} ({limits})"),
            );
        } else if value == current {
            trace.record(Stage::Step, value, "");
        } else {
            trace.record(Stage::Step, value, limits);
        }
    }

    // ── bounds ───────────────────────────────────────────────────────────
    let mut limited: Option<(Stage, String)> = None;
    {
        let scheduled = scheduled_bounds(&policy.schedule, now_ms);
        let mut min = policy.min;
        let mut min_from = String::new();
        if let Some((floor, label)) = &scheduled.min {
            if *floor > min {
                min = *floor;
                min_from = format!(" ({label})");
            }
        }
        let mut max = policy.max;
        let mut max_from = String::new();
        if let Some((ceiling, label)) = &scheduled.max {
            if *ceiling < max {
                max = *ceiling;
                max_from = format!(" ({label})");
            }
        }
        let mut extra = String::new();
        if min > max {
            extra = format!("; scheduled floor {min} is above the ceiling {max}: the ceiling wins");
            min = max;
        }
        let value = trace.value;
        if value > max {
            if max >= current {
                limited = Some((Stage::Bounds, format!("at max {max} (raise autoscale.max)")));
            }
            trace.record(
                Stage::Bounds,
                max,
                format!("clamped to max {max}{max_from}{extra}"),
            );
        } else if value < min {
            trace.record(
                Stage::Bounds,
                min,
                format!("raised to min {min}{min_from}{extra}"),
            );
        } else {
            trace.record(
                Stage::Bounds,
                value,
                format!("{min}..{max}{min_from}{max_from}{extra}"),
            );
        }
    }

    // ── quota ────────────────────────────────────────────────────────────
    {
        let value = trace.value;
        match &obs.guards.quota {
            Some(quota) if value > current => {
                let allowed = current.saturating_add(quota.headroom);
                if value > allowed {
                    let reason = format!("limited by {}", quota.detail);
                    limited.get_or_insert_with(|| (Stage::Quota, reason.clone()));
                    trace.record(Stage::Quota, allowed, reason);
                } else {
                    trace.record(Stage::Quota, value, format!("{} more fit", quota.headroom));
                }
            }
            _ => trace.record(Stage::Quota, value, ""),
        }
    }

    // ── capacity ─────────────────────────────────────────────────────────
    {
        let value = trace.value;
        match &obs.guards.capacity {
            Some(capacity) if value > current && capacity.unplaced > 0 => {
                let last = capacity.last_scale_out.or(state.last_scale_out());
                let stuck = last.is_some_and(|last| capacity.unplaced * 2 > last);
                if stuck && !capacity.node_group_fits {
                    let reason = format!(
                        "held: {} replicas unplaced, no node group fits",
                        capacity.unplaced
                    );
                    limited.get_or_insert_with(|| (Stage::Capacity, reason.clone()));
                    trace.record(Stage::Capacity, current, reason);
                } else if stuck {
                    trace.record(
                        Stage::Capacity,
                        value,
                        format!("{} unplaced; a node group can host them", capacity.unplaced),
                    );
                } else {
                    trace.record(
                        Stage::Capacity,
                        value,
                        format!("{} unplaced", capacity.unplaced),
                    );
                }
            }
            _ => trace.record(Stage::Capacity, value, ""),
        }
    }

    // ── rollout ──────────────────────────────────────────────────────────
    {
        let value = trace.value;
        match &obs.guards.rollout {
            Some(rollout) if value < current => trace.record(
                Stage::Rollout,
                current,
                format!("deferred until rollout {rollout} completes"),
            ),
            Some(rollout) => {
                trace.record(Stage::Rollout, value, format!("rollout {rollout} active"))
            }
            None => trace.record(Stage::Rollout, value, ""),
        }
    }

    // ── pin ──────────────────────────────────────────────────────────────
    {
        let value = trace.value;
        match &obs.pin {
            Some(pin) if pin.until_ms > now_ms => {
                let reason = if pin.reason.is_empty() {
                    String::new()
                } else {
                    format!(": \"{}\"", pin.reason)
                };
                let who = format!("by {} until {}{reason}", pin.by, clock(pin.until_ms));
                let mut after = value;
                let mut note = Vec::new();
                if let Some(count) = pin.count {
                    after = count;
                    let above = if count > policy.max {
                        format!(" (above max {}: an operator override)", policy.max)
                    } else {
                        String::new()
                    };
                    note.push(format!("pinned at {count}{above}"));
                } else {
                    if let Some(min) = pin.min {
                        if after < min {
                            after = min;
                        }
                        note.push(format!("pinned floor {min}"));
                    }
                    if let Some(max) = pin.max {
                        if after > max {
                            after = max;
                        }
                        note.push(format!("pinned ceiling {max}"));
                    }
                }
                trace.record(Stage::Pin, after, format!("{} {who}", note.join(", ")));
            }
            _ => trace.record(Stage::Pin, value, ""),
        }
    }

    // ── pause ────────────────────────────────────────────────────────────
    {
        let value = trace.value;
        match &obs.pause {
            Some(pause) if pause.until_ms.is_none_or(|until| until > now_ms) => {
                let until = pause
                    .until_ms
                    .map(|until| format!(" until {}", clock(until)))
                    .unwrap_or_default();
                let reason = if pause.reason.is_empty() {
                    String::new()
                } else {
                    format!(": \"{}\"", pause.reason)
                };
                trace.record(
                    Stage::Pause,
                    current,
                    format!("paused by {}{until}{reason}", pause.by),
                );
            }
            _ => trace.record(Stage::Pause, value, ""),
        }
    }

    // ── mode ─────────────────────────────────────────────────────────────
    let to = trace.value;
    let kind = if to == current {
        DecisionKind::Held
    } else if policy.mode == Mode::DryRun {
        DecisionKind::Would
    } else {
        DecisionKind::Applied
    };
    let mode_note = match kind {
        DecisionKind::Would => format!("dry-run: would scale {current} → {to}"),
        _ => policy.mode.to_string(),
    };
    // Mode never changes the number: a "would" keeps `to` so history shows
    // what the policy would have done.
    trace.record(Stage::Mode, to, mode_note);

    // ── flags and summary ────────────────────────────────────────────────
    // One `scale.limited` per episode: it starts when a bound or guard
    // first holds the count back, and ends when the count moves without
    // one doing so. A tick held for another reason (warmup, stabilization)
    // neither ends it nor starts a new one.
    match limited {
        Some((stage, reason)) => {
            if state.limited != Some(stage) {
                flags.push(Flag::Limited { reason });
            }
            state.limited = Some(stage);
        }
        None if kind != DecisionKind::Held => state.limited = None,
        None => {}
    }
    let summary = summarize(&trace.stages, driver.map(|index| &outcomes[index]));
    Decision {
        at_ms: now_ms,
        from: current,
        to,
        kind,
        ready: obs.ready,
        warming: obs.warming,
        checks: outcomes,
        stages: trace.stages,
        summary,
        policy_digest: policy.digest(),
        flags,
    }
}

/// The one-line why: the check that won the merge, then every stage that
/// changed the number.
fn summarize(stages: &[StageNote], driver: Option<&CheckOutcome>) -> String {
    let mut parts = Vec::new();
    match driver {
        Some(outcome) => parts.push(format!("{} {}", outcome.name, outcome.detail)),
        None => {
            if let Some(merge) = stages.iter().find(|s| s.stage == Stage::Merge) {
                parts.push(merge.note.clone());
            }
        }
    }
    for note in stages.iter().filter(|s| s.changed()) {
        let brief = match note.stage {
            Stage::Merge => continue,
            Stage::Stabilize => format!("stabilized ({})", note.note),
            Stage::Step => format!("rate-limited from {}", note.before),
            _ => note.note.clone(),
        };
        parts.push(brief);
    }
    parts.retain(|part| !part.is_empty());
    parts.join("; ")
}

#[cfg(test)]
mod tests;
