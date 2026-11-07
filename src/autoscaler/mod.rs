//! The autoscaler engine core (autoscaler v2 plan, `internal_docs/plans/autoscaler-v2-plan.md`
//! §6.3–§6.6 and §6.12).
//!
//! A pure, deterministic library: no I/O, no RPC, no async, no clock. Every
//! function that needs the time takes it (`now_ms`, unix milliseconds), so
//! the live autoscaler, `legion scaling simulate` and the tests all run the
//! same code over different inputs.
//!
//! # Module map
//!
//! | Module | What it holds |
//! | --- | --- |
//! | [`spec`] | The jobspec `autoscale` field as written ([`AutoscaleSpec`], [`AutoscaleRecord`]): `true`/`false` or a sparse record, with serde, JSON schema and static validation. The same record is the cluster defaults' and a namespace policy's `autoscale`. |
//! | [`defaults`] | [`resolve`]: built-in → cluster → namespace policies → task, field by field, into an [`EffectivePolicy`] with the [`Origin`] of every field and a stable digest. Also the count-aware validation. |
//! | [`strategies`] | The built-in [`Strategy`] implementations: `target-value` (HPA, with tolerance and the warmup rule), `per-replica`, `threshold`, `pass-through`, `fixed`. |
//! | [`schedule`] | Cron windows in IANA time zones (DST-correct) that raise the floor or lower the ceiling. |
//! | [`pipeline`] | [`evaluate`]: the fixed safeguard pipeline, from an [`Observation`] and the per-policy [`PolicyState`] to a [`Decision`] whose stage notes explain it. |
//! | [`simulate`] | [`simulate()`](simulate::simulate) over rows or a CSV series, and [`simulate_with`] over a load model. |
//! | [`plugins`] | The plugins installed in the autoscaler's app config ([`PluginCatalog`](plugins::PluginCatalog)) and who may use them: a policy names a plugin, never a command or a URL. |
//! | [`policy`] | The autoscaler's object kinds, `autoscaler/ScalingPolicy` (an external target's record) and `autoscaler/NodeGroup`: parsing a stored spec, the kinds' schemas. |
//! | [`signals`] | How raw inputs become an observation: the warmup count and the averaging windows, shared by the live engine and the simulator. |
//! | [`shard`] | Which of the 64 shards a policy hashes to (FNV-1a of its key), which the policy watch filters by and autoscaler replicas split between them. |
//! | [`nodes`] | Node-group autoscaling (§6.11): the [`NodeGroupPolicy`] spec, the least-waste [`expand`]er, [`decide`] (scale-up from pending work, join timeouts, scale-in victims, never-victim rules, budget) and the drain state machine [`step_drain`]. |
//!
//! # How later phases use it
//!
//! ```text
//! submit / defaults change:  resolve(spec, count, layers, target)  -> ResolvedPolicy
//!                                 (errors are submit errors; digest -> scaling_policy_snapshot)
//! every tick, per policy:    evaluate(&policy, &mut state, &observation, now_ms) -> Decision
//!     Decision::kind == Applied -> ScaleTask(to, expected_version, decision)
//!                                  on success: state.record_applied(now_ms, from, to)
//!     Decision::kind == Would   -> scale_history row with applied = false
//!     Decision::flags           -> scale.limited / scale.flapping events
//! scaling simulate:          simulate(&policy, &rows, &options) -> Vec<Decision>
//! ```
//!
//! What the engine leaves to its caller: reading sources (an
//! [`Observation`] carries the readings), the cluster-aware guards (quota
//! headroom, unplaced replicas, active rollout, pins and pauses arrive as
//! inputs), the writes themselves, and the checks that need the rest of the
//! job or the cluster (see [`defaults`]' "What this does not check").

// This module is the reference later phases integrate against: keep every
// public item documented.
#![warn(missing_docs)]

pub mod defaults;
pub mod nodes;
pub mod pipeline;
pub mod plugins;
pub mod policy;
pub mod schedule;
pub mod shard;
pub mod signals;
pub mod simulate;
pub mod spec;
pub mod strategies;

pub use defaults::{
    builtin_defaults, effective_defaults, resolve, DefaultsLayers, EffectiveCheck,
    EffectiveDefaults, EffectivePolicy, EffectiveSchedule, NamespaceDefaults, Origin,
    ProvenanceRow, ResolvedPolicy,
};
pub use nodes::{
    decide, expand, step_drain, EffectiveNodeGroup, NodeDecision, NodeGroupObservation,
    NodeGroupPolicy, NodeGroupState, NodeIntent,
};
pub use pipeline::{
    evaluate, CapacityGuard, CheckOutcome, Decision, DecisionKind, Flag, Guards, Observation,
    Pause, Pin, PluginRecommendation, PolicyState, QuotaGuard, Reading, Stage, StageNote,
};
pub use simulate::{
    history_rows, read_csv, simulate_with, HistorySample, SimOptions, SimRow, Simulation,
};
pub use spec::{AutoscaleRecord, AutoscaleSpec, CountBound, Mode, OnMissing};
pub use strategies::{Strategy, StrategyConfig};
