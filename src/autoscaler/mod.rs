//! The autoscaler engine core (autoscaler v2 plan, `internal_docs/plans/autoscaler-v2-plan.md`
//! §6.3–§6.6 and §6.12).
//!
//! A pure, deterministic library: no I/O, no RPC, no async, no clock. Every
//! function that needs the time takes it (`now_ms`, unix milliseconds).
//!
//! | Module | What it holds |
//! | --- | --- |
//! | [`spec`] | The jobspec `autoscale` field as written ([`AutoscaleSpec`], [`AutoscaleRecord`]). |
//! | [`defaults`] | [`resolve`]: built-in → cluster → namespace policies → task, into an [`EffectivePolicy`] with provenance and a digest. |
//! | [`strategies`] | The built-in [`Strategy`] implementations. |
//! | [`schedule`] | Cron windows in IANA time zones. |
//! | [`pipeline`] | [`evaluate`]: the fixed safeguard pipeline, from an [`Observation`] and [`PolicyState`] to a [`Decision`]. |

pub mod defaults;
pub mod pipeline;
pub mod schedule;
pub mod spec;
pub mod strategies;

pub use defaults::{
    builtin_defaults, effective_defaults, resolve, DefaultsLayers, EffectiveCheck,
    EffectiveDefaults, EffectivePolicy, EffectiveSchedule, NamespaceDefaults, Origin,
    ProvenanceRow, ResolvedPolicy,
};
pub use pipeline::{
    evaluate, CapacityGuard, CheckOutcome, Decision, DecisionKind, Flag, Guards, Observation,
    Pause, Pin, PolicyState, QuotaGuard, Reading, Stage, StageNote,
};
pub use spec::{AutoscaleRecord, AutoscaleSpec, CountBound, Mode, OnMissing};
pub use strategies::{Strategy, StrategyConfig};
