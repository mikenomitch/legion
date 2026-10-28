//! The jobspec `autoscale` field, exactly as a user writes it.
//!
//! `autoscale` takes a boolean or a sparse record (plan §6.3, §6.4):
//!
//! ```nickel
//! autoscale = true,                       # the effective defaults
//! autoscale = { max = 20 },               # one field overridden
//! autoscale = {                           # the full surface
//!   min = 4, max = "4x",
//!   cpu = 0.65, memory = 0.8,
//!   checks = [{ name = "lag", source = "team-prom", query = "...",
//!               strategy = { name = "per-replica", value = 500 },
//!               activation = 1, scaleIn = true }],
//!   up   = { stabilize = "0s", step = { percent = 100, replicas = 4 }, every = "1m" },
//!   down = { stabilize = "5m", step = { percent = 10,  replicas = 1 }, every = "1m" },
//!   headroom = 1, warmup = "30s", tolerance = 0.1, window = "1m",
//!   schedule = [{ cron = "0 8 * * 1-5", for = "10h", min = 10, timezone = "America/New_York" }],
//!   onMissing = "hold", mode = "dry-run",
//! }
//! ```
//!
//! The same record shape is what a cluster's scaling defaults and a namespace
//! policy's `autoscale` field hold; [`AutoscaleRecord::validate_inherited`]
//! adds the rules that apply only at those levels.
//!
//! Every field is optional and stays unset (`None`) when the author left it
//! out. Nothing here fills a default: that is
//! [`resolve`](super::defaults::resolve)'s job, so the stored jobspec keeps
//! what the author wrote and the defaults stay live (plan §6.3, calls entry 3).
//!
//! Validation here is *static*: it needs only the record. Rules that need
//! the task's `count` (`min ≤ count ≤ max`, `"Nx"` resolution, the
//! scale-to-zero wake rule) run in [`resolve`](super::defaults::resolve).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use schemars::gen::SchemaGenerator;
use schemars::schema::{InstanceType, Metadata, Schema, SchemaObject, SubschemaValidation};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::schedule::CronWindow;
use super::strategies::StrategyConfig;

/// The longest schedule window. A floor that should hold for longer than a
/// week is a bound, not a schedule.
pub const MAX_SCHEDULE_WINDOW: Duration = Duration::from_secs(7 * 24 * 3600);

/// What the jobspec `autoscale` field holds: `true`, `false`, or a sparse
/// record.
///
/// Serializes back to exactly the form it was written in, so a stored
/// jobspec round-trips without expanding defaults.
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum AutoscaleSpec {
    /// `autoscale = true` turns autoscaling on with the effective defaults;
    /// `autoscale = false` is the same as leaving the field out.
    Enabled(bool),
    /// A sparse record. Fields left out take their effective default (the
    /// built-in value, or what the cluster or a namespace policy set).
    Policy(Box<AutoscaleRecord>),
}

impl AutoscaleSpec {
    /// Whether this spec turns autoscaling on (`true`, or any record).
    pub fn is_enabled(&self) -> bool {
        !matches!(self, AutoscaleSpec::Enabled(false))
    }

    /// The record the author wrote, or an empty record for `true`. `None`
    /// for `false`.
    pub fn record(&self) -> Option<AutoscaleRecord> {
        match self {
            AutoscaleSpec::Enabled(true) => Some(AutoscaleRecord::default()),
            AutoscaleSpec::Enabled(false) => None,
            AutoscaleSpec::Policy(record) => Some((**record).clone()),
        }
    }

    /// Static validation of the task-level spec (no `count` needed).
    pub fn validate(&self) -> Result<(), String> {
        match self {
            AutoscaleSpec::Enabled(_) => Ok(()),
            AutoscaleSpec::Policy(record) => record.validate(),
        }
    }

    /// The JSON schema of the field, for `job validate`, editors and agents.
    pub fn json_schema_value() -> Value {
        serde_json::to_value(schemars::schema_for!(AutoscaleSpec))
            .expect("autoscale schema should serialize")
    }
}

impl<'de> Deserialize<'de> for AutoscaleSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::Bool(enabled) => Ok(AutoscaleSpec::Enabled(enabled)),
            Value::Object(_) => serde_json::from_value::<AutoscaleRecord>(value)
                .map(|record| AutoscaleSpec::Policy(Box::new(record)))
                .map_err(|err| D::Error::custom(format!("autoscale: {err}"))),
            other => Err(D::Error::custom(format!(
                "autoscale must be true, false, or a record like {{ max = 20 }}; got {other}"
            ))),
        }
    }
}

/// The sparse `autoscale` record. Every field is optional; an unset field
/// takes its effective default (plan §6.3 "Overriding the defaults").
///
/// The same shape is used at all three override levels: the task's jobspec,
/// a namespace policy's `autoscale`, and the cluster's scaling defaults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AutoscaleRecord {
    /// The floor: a replica count, or a multiple of `count` (`"1x"`). The
    /// built-in default is `"1x"`, so the count you wrote is the floor. At
    /// the cluster and namespace levels it must be a multiple of at least
    /// `"1x"`; only the task itself may set a floor below its count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<CountBound>,
    /// The ceiling: a replica count, or a multiple of `count`. Built-in
    /// default `"4x"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<CountBound>,
    /// Hold average CPU over ready replicas at this fraction of the
    /// reservation (`0.7` = 70%). Shorthand for a `target-value` check on
    /// the `legion` source's `task.cpu.utilization`. Built-in default `0.7`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<f64>,
    /// Hold average memory over ready replicas at this fraction of the
    /// reservation. Off by default: memory rarely falls with load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<f64>,
    /// Explicit checks, each turning one signal into a desired count; the
    /// largest wins. Only the task may set `checks`: a check names a source
    /// and a query that only make sense for one task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checks: Option<Vec<CheckSpec>>,
    /// Scale-out behavior: stabilization, step limit, and its window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub up: Option<BehaviorSpec>,
    /// Scale-in behavior: stabilization, step limit, and its window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub down: Option<BehaviorSpec>,
    /// Replicas added on top of the merged recommendation. Default `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headroom: Option<u32>,
    /// How long a replica stays "warming" after it first passes its health
    /// checks. Warming replicas count as idle for scale-out and as exactly
    /// at target for scale-in (the HPA rule). Default `"30s"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup: Option<DurationSpec>,
    /// The band around a `target-value` target inside which nothing moves,
    /// as a fraction of the target (`0.1` = ±10%). Default `0.1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerance: Option<f64>,
    /// The averaging window for the `cpu`/`memory` shorthands, and for any
    /// check that names no `window` of its own. Default `"1m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<DurationSpec>,
    /// Time windows that raise the floor or lower the ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<Vec<ScheduleSpec>>,
    /// What a check with no data does: `hold` (default) blocks scale-in
    /// while any check is missing; `ignore` drops the check for that
    /// evaluation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_missing: Option<OnMissing>,
    /// `enforce` (default) acts; `dry-run` records what it would have done.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
}

/// One explicit check: a signal from a source, and the strategy that turns
/// it into a desired replica count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckSpec {
    /// Unique within the policy; names the check in notes and history.
    pub name: String,
    /// The source to read: `legion` (built in), or an operator-installed
    /// source such as a Prometheus endpoint.
    pub source: String,
    /// The source's query. For `legion`, one of its fixed vocabulary
    /// (`task.cpu.utilization`, ...). May be empty only for the `fixed`
    /// strategy, which reads nothing.
    #[serde(default)]
    pub query: String,
    /// The window the source averages over. Defaults to the policy's
    /// `window`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<DurationSpec>,
    /// The strategy and its settings, as one record:
    /// `{ name = "per-replica", value = 500 }`.
    pub strategy: StrategySpec,
    /// Scale-to-zero: the reading above which the target is "active". At
    /// zero replicas only a check with `activation` can wake the target, and
    /// a target with `min = 0` sleeps once every such check reads at or
    /// below its activation for `down.stabilize`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<f64>,
    /// `false` lets this check push the count up but never pull it down: it
    /// votes only when it wants more than the current count. Default `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_in: Option<bool>,
}

/// A strategy as written: its `name`, plus the strategy's own settings
/// flattened beside it (`{ name = "threshold", steps = [...] }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StrategySpec {
    /// `target-value`, `per-replica`, `threshold`, `pass-through`, or
    /// `fixed`.
    pub name: String,
    /// The strategy's settings (`target`, `value`, `steps`).
    #[serde(flatten)]
    pub config: BTreeMap<String, Value>,
}

impl StrategySpec {
    /// Parse into a typed built-in strategy, with an error naming what is
    /// wrong.
    pub fn to_config(&self) -> Result<StrategyConfig, String> {
        StrategyConfig::from_spec(self)
    }
}

/// Scale-out or scale-in behavior.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BehaviorSpec {
    /// How long a recommendation must hold before the count moves this way.
    /// Scale-out uses the lowest recommendation in the window, scale-in the
    /// highest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stabilize: Option<DurationSpec>,
    /// The most the count may move per `every`. Writing `step` replaces the
    /// whole record below it: a half left out is 0 (calls entry 19).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<StepSpec>,
    /// The rolling window the step limit is counted over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<DurationSpec>,
}

/// A step limit: the larger of `percent` of the count at the start of the
/// window and `replicas`, per `every`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepSpec {
    /// Percent of the count at the start of the window. Omitted is 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<f64>,
    /// Absolute replicas. Omitted is 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
}

/// A schedule: a cron expression that opens a window of length `for`, in
/// which the floor is raised to `min` and/or the ceiling lowered to `max`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScheduleSpec {
    /// A five-field crontab expression (`minute hour day-of-month month
    /// day-of-week`, Sunday = 0 or 7), or `@hourly`/`@daily`/`@weekly`/
    /// `@monthly`/`@yearly`. Evaluated in `timezone`.
    pub cron: String,
    /// How long each window lasts, from the time the expression fires.
    #[serde(rename = "for")]
    pub duration: DurationSpec,
    /// The floor during the window. It only raises: a value below the
    /// policy's `min` has no effect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<CountBound>,
    /// The ceiling during the window. It only lowers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<CountBound>,
    /// An IANA time zone (`America/New_York`). Default `UTC`. Windows follow
    /// the zone's wall clock across daylight-saving changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

/// What a check with no data does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OnMissing {
    /// Nothing scales in while any check has no data; checks with data may
    /// still scale out.
    #[default]
    Hold,
    /// A check with no data is dropped for that evaluation, and the rest
    /// decide in both directions.
    Ignore,
}

impl fmt::Display for OnMissing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OnMissing::Hold => "hold",
            OnMissing::Ignore => "ignore",
        })
    }
}

/// Whether the policy acts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// Decisions are written.
    #[default]
    Enforce,
    /// Decisions are recorded as "would" and never written.
    DryRun,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Mode::Enforce => "enforce",
            Mode::DryRun => "dry-run",
        })
    }
}

/// `min`/`max`: a replica count, or a multiple of the task's `count`
/// written as `"Nx"` (`"4x"`, `"1.5x"`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CountBound {
    /// An absolute replica count.
    Replicas(u32),
    /// A multiple of `count`. Resolves to `ceil(multiple × count)`.
    Multiple(f64),
}

impl CountBound {
    /// The replica count this bound means for a task with `count`.
    /// Multiples round up, so `"1.5x"` of 3 is 5.
    pub fn resolve(&self, count: u32) -> u32 {
        match *self {
            CountBound::Replicas(n) => n,
            CountBound::Multiple(m) => {
                let value = (m * f64::from(count) - 1e-9).ceil();
                if value <= 0.0 {
                    0
                } else if value >= f64::from(u32::MAX) {
                    u32::MAX
                } else {
                    value as u32
                }
            }
        }
    }

    fn parse_text(text: &str) -> Result<CountBound, String> {
        let trimmed = text.trim();
        let multiple = trimmed.strip_suffix('x').ok_or_else(|| {
            format!("'{text}' is not a count: write a number (20) or a multiple of count (\"4x\")")
        })?;
        let value: f64 = multiple.trim().parse().map_err(|_| {
            format!("'{text}' is not a multiple of count: write it like \"4x\" or \"1.5x\"")
        })?;
        if !value.is_finite() || value < 0.0 {
            return Err(format!("'{text}': the multiple must be 0 or more"));
        }
        Ok(CountBound::Multiple(value))
    }
}

impl fmt::Display for CountBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CountBound::Replicas(n) => write!(f, "{n}"),
            CountBound::Multiple(m) => write!(f, "{m}x"),
        }
    }
}

impl Serialize for CountBound {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            CountBound::Replicas(n) => serializer.serialize_u32(*n),
            CountBound::Multiple(_) => serializer.serialize_str(&self.to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for CountBound {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        match Value::deserialize(deserializer)? {
            Value::Number(number) => {
                if let Some(n) = number.as_u64() {
                    return u32::try_from(n)
                        .map(CountBound::Replicas)
                        .map_err(|_| D::Error::custom(format!("count {n} is too large")));
                }
                match number.as_f64() {
                    Some(f) if f >= 0.0 && f.fract() == 0.0 && f <= f64::from(u32::MAX) => {
                        Ok(CountBound::Replicas(f as u32))
                    }
                    _ => Err(D::Error::custom(format!(
                        "{number} is not a replica count: write a whole number, or a multiple \
                         of count like \"1.5x\""
                    ))),
                }
            }
            Value::String(text) => CountBound::parse_text(&text).map_err(D::Error::custom),
            other => Err(D::Error::custom(format!(
                "expected a replica count (20) or a multiple of count (\"4x\"), got {other}"
            ))),
        }
    }
}

impl JsonSchema for CountBound {
    fn schema_name() -> String {
        "CountBound".to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        let mut replicas = SchemaObject {
            instance_type: Some(InstanceType::Integer.into()),
            ..Default::default()
        };
        replicas.number().minimum = Some(0.0);
        let mut multiple = SchemaObject {
            instance_type: Some(InstanceType::String.into()),
            ..Default::default()
        };
        multiple.string().pattern = Some(r"^[0-9]+(\.[0-9]+)?x$".to_string());
        SchemaObject {
            metadata: Some(Box::new(Metadata {
                description: Some(
                    "A replica count, or a multiple of the task's count (\"4x\").".to_string(),
                ),
                ..Default::default()
            })),
            subschemas: Some(Box::new(SubschemaValidation {
                any_of: Some(vec![replicas.into(), multiple.into()]),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

/// A duration as written in a policy: a string such as `"30s"`, `"5m"`,
/// `"1h 30m"` (humantime syntax, like every other Legion duration), or a
/// whole number of seconds. Serializes back as a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct DurationSpec(pub Duration);

impl DurationSpec {
    /// From whole seconds.
    pub fn secs(secs: u64) -> Self {
        DurationSpec(Duration::from_secs(secs))
    }

    /// Parse `"30s"`-style text.
    pub fn parse(text: &str) -> Result<Self, String> {
        humantime::parse_duration(text.trim())
            .map(DurationSpec)
            .map_err(|err| {
                format!("invalid duration '{text}': {err} (write it like \"30s\" or \"5m\")")
            })
    }
}

impl fmt::Display for DurationSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format_duration(self.0))
    }
}

/// Format a duration the way policies write them: `"0s"`, `"30s"`, `"5m"`,
/// `"1h 30m"`.
pub fn format_duration(duration: Duration) -> String {
    if duration.is_zero() {
        return "0s".to_string();
    }
    humantime::format_duration(duration).to_string()
}

impl Serialize for DurationSpec {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DurationSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        match Value::deserialize(deserializer)? {
            Value::String(text) => DurationSpec::parse(&text).map_err(D::Error::custom),
            Value::Number(number) => number
                .as_u64()
                .map(DurationSpec::secs)
                .ok_or_else(|| D::Error::custom("a duration in seconds must be a whole number")),
            other => Err(D::Error::custom(format!(
                "expected a duration like \"30s\", got {other}"
            ))),
        }
    }
}

impl JsonSchema for DurationSpec {
    fn schema_name() -> String {
        "Duration".to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        SchemaObject {
            instance_type: Some(vec![InstanceType::String, InstanceType::Integer].into()),
            metadata: Some(Box::new(Metadata {
                description: Some(
                    "A duration like \"30s\", \"5m\" or \"1h 30m\", or whole seconds.".to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

/// Serde helpers for `std::time::Duration` fields of the *effective* policy,
/// which serialize as the same strings policies are written in.
pub(crate) mod duration_text {
    use super::{format_duration, DurationSpec};
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_duration(*duration))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        DurationSpec::deserialize(deserializer).map(|spec| spec.0)
    }
}

fn check_fraction(field: &str, value: Option<f64>) -> Result<(), String> {
    match value {
        Some(v) if !v.is_finite() || v <= 0.0 => Err(format!(
            "autoscale.{field} = {v} must be greater than 0: it is a fraction of the \
             reservation, like 0.7 for 70%"
        )),
        _ => Ok(()),
    }
}

impl AutoscaleRecord {
    /// Whether this record names any signal (`cpu`, `memory`, or `checks`).
    /// Signals are all or nothing: a level that names any replaces the
    /// signal set of every level below it.
    pub fn names_signal(&self) -> bool {
        self.cpu.is_some() || self.memory.is_some() || self.checks.is_some()
    }

    /// Static validation: every rule that needs only this record. Errors
    /// name the field (`autoscale.down.step`) and what to write instead.
    pub fn validate(&self) -> Result<(), String> {
        check_fraction("cpu", self.cpu)?;
        check_fraction("memory", self.memory)?;
        if let Some(tolerance) = self.tolerance {
            if !tolerance.is_finite() || !(0.0..1.0).contains(&tolerance) {
                return Err(format!(
                    "autoscale.tolerance = {tolerance} must be at least 0 and below 1 (a \
                     fraction of the target: 0.1 is ±10%)"
                ));
            }
        }
        if let Some(window) = self.window {
            if window.0.is_zero() {
                return Err("autoscale.window must be longer than 0s".to_string());
            }
        }
        if let (Some(CountBound::Replicas(min)), Some(CountBound::Replicas(max))) =
            (self.min, self.max)
        {
            if min > max {
                return Err(format!(
                    "autoscale.min = {min} is above autoscale.max = {max}. Hint: the floor \
                     must not exceed the ceiling"
                ));
            }
        }
        if let (Some(CountBound::Multiple(min)), Some(CountBound::Multiple(max))) =
            (self.min, self.max)
        {
            if min > max {
                return Err(format!(
                    "autoscale.min = \"{min}x\" is above autoscale.max = \"{max}x\""
                ));
            }
        }
        let zero_max = match self.max {
            Some(CountBound::Replicas(n)) => n == 0,
            Some(CountBound::Multiple(m)) => m <= 0.0,
            None => false,
        };
        if zero_max {
            return Err("autoscale.max must be at least 1".to_string());
        }
        for (label, behavior) in [("up", &self.up), ("down", &self.down)] {
            if let Some(behavior) = behavior {
                behavior.validate(label)?;
            }
        }
        if let Some(checks) = &self.checks {
            if checks.is_empty() {
                return Err(
                    "autoscale.checks is empty. Hint: leave checks out to use the \
                            default signal, or list at least one check"
                        .to_string(),
                );
            }
            let mut names = BTreeSet::new();
            for shorthand in ["cpu", "memory"] {
                let set = match shorthand {
                    "cpu" => self.cpu.is_some(),
                    _ => self.memory.is_some(),
                };
                if set {
                    names.insert(shorthand.to_string());
                }
            }
            for (index, check) in checks.iter().enumerate() {
                check.validate(index)?;
                if !names.insert(check.name.clone()) {
                    return Err(format!(
                        "autoscale.checks[{index}]: the name '{}' is used twice (the cpu and \
                         memory shorthands are checks named 'cpu' and 'memory'). Hint: give \
                         every check its own name",
                        check.name
                    ));
                }
            }
        }
        if let Some(schedules) = &self.schedule {
            for (index, schedule) in schedules.iter().enumerate() {
                schedule.validate(index)?;
            }
        }
        Ok(())
    }

    /// The extra rules for a record inherited by many tasks: the cluster's
    /// scaling defaults or a namespace policy's `autoscale` (`level` names
    /// it in errors, e.g. `"namespace policy payments"`).
    ///
    /// - `checks` is refused: a check's source and query only make sense
    ///   for one task.
    /// - `min` must be a multiple of `count` of at least `"1x"`, so a
    ///   default can raise every task's floor but never lower an HA floor
    ///   below the count its author wrote.
    pub fn validate_inherited(&self, level: &str) -> Result<(), String> {
        self.validate().map_err(|err| format!("{level}: {err}"))?;
        if self.checks.is_some() {
            return Err(format!(
                "{level}: autoscale.checks can only be set in a jobspec; a check names a \
                 source and query for one task. Hint: defaults may set cpu and memory"
            ));
        }
        match self.min {
            Some(CountBound::Replicas(n)) => Err(format!(
                "{level}: autoscale.min = {n} is a plain number; an inherited floor must be a \
                 multiple of each task's count of at least \"1x\", so it never lowers the \
                 count a job author wrote. Hint: write min = \"1x\" (or higher)"
            )),
            Some(CountBound::Multiple(m)) if m < 1.0 => Err(format!(
                "{level}: autoscale.min = \"{m}x\" is below \"1x\"; an inherited floor can \
                 raise every task's floor but never lower it below the count its author \
                 wrote. Hint: only a task's own autoscale record may set min below its count"
            )),
            _ => Ok(()),
        }
    }
}

impl BehaviorSpec {
    fn validate(&self, label: &str) -> Result<(), String> {
        if let Some(every) = self.every {
            if every.0.is_zero() {
                return Err(format!("autoscale.{label}.every must be longer than 0s"));
            }
        }
        if let Some(step) = &self.step {
            if step.percent.is_none() && step.replicas.is_none() {
                return Err(format!(
                    "autoscale.{label}.step names neither percent nor replicas. Hint: writing \
                     step replaces the whole default; write step = {{ replicas = 0 }} to stop \
                     scaling {label} on signals, or leave step out to keep the default"
                ));
            }
            if let Some(percent) = step.percent {
                if !percent.is_finite() || percent < 0.0 {
                    return Err(format!(
                        "autoscale.{label}.step.percent = {percent} must be 0 or more"
                    ));
                }
                if label == "down" && percent > 100.0 {
                    return Err(format!(
                        "autoscale.down.step.percent = {percent} is above 100; scale-in can \
                         remove at most every replica"
                    ));
                }
            }
        }
        Ok(())
    }
}

impl CheckSpec {
    fn validate(&self, index: usize) -> Result<(), String> {
        let at = format!("autoscale.checks[{index}]");
        if self.name.trim().is_empty() {
            return Err(format!("{at}.name must not be empty"));
        }
        if self.source.trim().is_empty() {
            return Err(format!(
                "{at} ({}): source must not be empty. Hint: `legion` reads Legion's own usage",
                self.name
            ));
        }
        let config = self
            .strategy
            .to_config()
            .map_err(|err| format!("{at} ({}): {err}", self.name))?;
        if config.needs_reading() && self.query.trim().is_empty() {
            return Err(format!(
                "{at} ({}): query must not be empty; strategy '{}' reads a value",
                self.name, self.strategy.name
            ));
        }
        if let Some(window) = self.window {
            if window.0.is_zero() {
                return Err(format!(
                    "{at} ({}): window must be longer than 0s",
                    self.name
                ));
            }
        }
        if let Some(activation) = self.activation {
            if !activation.is_finite() || activation < 0.0 {
                return Err(format!(
                    "{at} ({}): activation = {activation} must be 0 or more",
                    self.name
                ));
            }
        }
        Ok(())
    }
}

impl ScheduleSpec {
    fn validate(&self, index: usize) -> Result<(), String> {
        let at = format!("autoscale.schedule[{index}]");
        let timezone = self.timezone.as_deref().unwrap_or("UTC");
        CronWindow::parse(&self.cron, timezone).map_err(|err| format!("{at}: {err}"))?;
        if self.duration.0.is_zero() {
            return Err(format!("{at}: for must be longer than 0s"));
        }
        if self.duration.0 > MAX_SCHEDULE_WINDOW {
            return Err(format!(
                "{at}: for = {} is longer than 7 days. Hint: change min or max for a \
                 permanent bound",
                self.duration
            ));
        }
        if self.min.is_none() && self.max.is_none() {
            return Err(format!(
                "{at}: sets neither min nor max, so it changes nothing. Hint: a schedule \
                 raises the floor (min) or lowers the ceiling (max) while it is open"
            ));
        }
        if let (Some(CountBound::Replicas(min)), Some(CountBound::Replicas(max))) =
            (self.min, self.max)
        {
            if min > max {
                return Err(format!("{at}: min = {min} is above max = {max}"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(value: Value) -> Result<AutoscaleSpec, String> {
        serde_json::from_value::<AutoscaleSpec>(value).map_err(|err| err.to_string())
    }

    #[test]
    fn spec_accepts_bool_and_record_and_round_trips_as_written_test() {
        assert_eq!(parse(json!(true)).unwrap(), AutoscaleSpec::Enabled(true));
        assert_eq!(parse(json!(false)).unwrap(), AutoscaleSpec::Enabled(false));
        assert!(!AutoscaleSpec::Enabled(false).is_enabled());

        let written = json!({
            "min": 4,
            "max": "4x",
            "cpu": 0.65,
            "checks": [{
                "name": "lag",
                "source": "team-prom",
                "query": "sum(lag)",
                "window": "2m",
                "strategy": { "name": "per-replica", "value": 500 },
                "activation": 1.0,
                "scaleIn": false
            }],
            "up": { "stabilize": "30s", "step": { "replicas": 2 }, "every": "1m" },
            "down": { "stabilize": "10m" },
            "headroom": 1,
            "warmup": "45s",
            "schedule": [{ "cron": "0 8 * * 1-5", "for": "10h", "min": 10,
                           "timezone": "America/New_York" }],
            "onMissing": "ignore",
            "mode": "dry-run"
        });
        let spec = parse(written.clone()).unwrap();
        spec.validate().unwrap();
        let record = spec.record().unwrap();
        assert_eq!(record.min, Some(CountBound::Replicas(4)));
        assert_eq!(record.max, Some(CountBound::Multiple(4.0)));
        assert_eq!(record.on_missing, Some(OnMissing::Ignore));
        assert_eq!(record.mode, Some(Mode::DryRun));
        assert_eq!(
            record.up.as_ref().unwrap().step,
            Some(StepSpec {
                percent: None,
                replicas: Some(2)
            })
        );
        // Stored as written: nothing expanded, durations as the author's text.
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back, written);
    }

    #[test]
    fn spec_rejects_wrong_shapes_with_named_errors_test() {
        let cases: Vec<(Value, &str)> = vec![
            (json!(3), "autoscale must be true, false, or a record"),
            (json!({ "maxx": 3 }), "unknown field `maxx`"),
            (json!({ "max": "four" }), "is not a count"),
            (json!({ "max": "fourx" }), "is not a multiple of count"),
            (json!({ "max": 2.5 }), "is not a replica count"),
            (json!({ "warmup": "soon" }), "invalid duration"),
            (json!({ "mode": "yolo" }), "unknown variant"),
        ];
        for (value, expected) in cases {
            let err = parse(value.clone()).unwrap_err();
            assert!(err.contains(expected), "{value}: {err}");
        }
    }

    #[test]
    fn record_validation_table_test() {
        let cases: Vec<(Value, Option<&str>)> = vec![
            (json!({}), None),
            (
                json!({ "cpu": 0 }),
                Some("autoscale.cpu = 0 must be greater than 0"),
            ),
            (json!({ "memory": -1 }), Some("autoscale.memory = -1")),
            (json!({ "tolerance": 1.0 }), Some("autoscale.tolerance")),
            (
                json!({ "min": 5, "max": 3 }),
                Some("autoscale.min = 5 is above"),
            ),
            (
                json!({ "min": "2x", "max": "1x" }),
                Some("is above autoscale.max"),
            ),
            (
                json!({ "max": 0 }),
                Some("autoscale.max must be at least 1"),
            ),
            (
                json!({ "down": { "step": {} } }),
                Some("names neither percent nor replicas"),
            ),
            (json!({ "down": { "step": { "replicas": 0 } } }), None),
            (
                json!({ "down": { "step": { "percent": 150 } } }),
                Some("above 100"),
            ),
            (json!({ "up": { "step": { "percent": 300 } } }), None),
            (
                json!({ "up": { "every": "0s" } }),
                Some("autoscale.up.every"),
            ),
            (json!({ "checks": [] }), Some("autoscale.checks is empty")),
            (
                json!({ "checks": [{ "name": "a", "source": "legion", "query": "q",
                                     "strategy": { "name": "nope" } }] }),
                Some("unknown strategy 'nope'"),
            ),
            (
                json!({ "checks": [{ "name": "a", "source": "legion", "query": "",
                                     "strategy": { "name": "per-replica", "value": 5 } }] }),
                Some("query must not be empty"),
            ),
            (
                json!({ "checks": [{ "name": "a", "source": "legion",
                                     "strategy": { "name": "fixed", "value": 3 } }] }),
                None,
            ),
            (
                json!({ "cpu": 0.5, "checks": [{ "name": "cpu", "source": "prom", "query": "q",
                                     "strategy": { "name": "pass-through" } }] }),
                Some("the name 'cpu' is used twice"),
            ),
            (
                json!({ "checks": [{ "name": "a", "source": "", "query": "q",
                                     "strategy": { "name": "pass-through" } }] }),
                Some("source must not be empty"),
            ),
            (
                json!({ "schedule": [{ "cron": "0 8 * * 1-5", "for": "1h" }] }),
                Some("sets neither min nor max"),
            ),
            (
                json!({ "schedule": [{ "cron": "0 8 * * 1-5", "for": "8d", "min": 2 }] }),
                Some("longer than 7 days"),
            ),
            (
                json!({ "schedule": [{ "cron": "0 8 * *", "for": "1h", "min": 2 }] }),
                Some("five fields"),
            ),
            (
                json!({ "schedule": [{ "cron": "0 8 * * *", "for": "1h", "min": 2,
                                       "timezone": "Mars/Olympus" }] }),
                Some("unknown time zone"),
            ),
        ];
        for (value, expected) in cases {
            let spec = parse(value.clone()).unwrap();
            let result = spec.validate();
            match expected {
                None => assert!(result.is_ok(), "{value}: {result:?}"),
                Some(text) => {
                    let err = result.unwrap_err();
                    assert!(err.contains(text), "{value}: {err}");
                }
            }
        }
    }

    #[test]
    fn inherited_record_rules_test() {
        let cases: Vec<(Value, Option<&str>)> = vec![
            (json!({ "cpu": 0.6, "max": "6x" }), None),
            (json!({ "min": "1x" }), None),
            (json!({ "min": "2x" }), None),
            (json!({ "min": 3 }), Some("is a plain number")),
            (json!({ "min": "0.5x" }), Some("is below \"1x\"")),
            (
                json!({ "checks": [{ "name": "a", "source": "legion", "query": "q",
                                     "strategy": { "name": "pass-through" } }] }),
                Some("can only be set in a jobspec"),
            ),
        ];
        for (value, expected) in cases {
            let record: AutoscaleRecord = serde_json::from_value(value.clone()).unwrap();
            let result = record.validate_inherited("cluster defaults");
            match expected {
                None => assert!(result.is_ok(), "{value}: {result:?}"),
                Some(text) => {
                    let err = result.unwrap_err();
                    assert!(err.starts_with("cluster defaults: "), "{err}");
                    assert!(err.contains(text), "{value}: {err}");
                }
            }
        }
    }

    #[test]
    fn count_bound_resolution_test() {
        let cases = [
            (CountBound::Replicas(7), 3, 7),
            (CountBound::Multiple(1.0), 3, 3),
            (CountBound::Multiple(4.0), 2, 8),
            (CountBound::Multiple(1.5), 3, 5),
            (CountBound::Multiple(0.0), 3, 0),
            (CountBound::Multiple(2.0), 0, 0),
        ];
        for (bound, count, expected) in cases {
            assert_eq!(bound.resolve(count), expected, "{bound} of {count}");
        }
        assert_eq!(CountBound::Multiple(1.5).to_string(), "1.5x");
        assert_eq!(CountBound::Multiple(4.0).to_string(), "4x");
    }

    #[test]
    fn schema_describes_bool_or_record_test() {
        let schema = AutoscaleSpec::json_schema_value();
        let text = schema.to_string();
        assert!(text.contains("anyOf"), "{text}");
        assert!(text.contains("onMissing"), "{text}");
        assert!(text.contains("scaleIn"), "{text}");
        assert!(text.contains(r"^[0-9]+(\\.[0-9]+)?x$"), "{text}");
    }
}
