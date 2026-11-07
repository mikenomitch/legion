//! Defaults resolution: from what an author wrote to the policy the engine
//! runs (plan §6.3 "Overriding the defaults", calls entries 2, 3 and 19).
//!
//! Four levels, lowest first. A field set at one level wins over every
//! level below it; a field left unset falls through:
//!
//! | Level | Where | [`Origin`] |
//! | --- | --- | --- |
//! | built-in | [`builtin_defaults`] | `built-in` |
//! | cluster | `legion scaling defaults apply` | `cluster defaults` |
//! | namespace | `autoscale` in every namespace policy governing the namespace | `namespace policy <name>` |
//! | task | `autoscale` in the jobspec | `jobspec` |
//!
//! The rules, each enforced here:
//!
//! - **Field by field, including nested records.** `down = { stabilize =
//!   "10m" }` keeps the lower levels' `down.step` and `down.every`.
//! - **A `step` record replaces the whole `step` below it**, and a half it
//!   leaves out is 0 (calls entry 19).
//! - **`"Nx"` resolves against `count`**, rounding up.
//! - **An inherited `min` can't lower the floor**: at the cluster and
//!   namespace levels it must be a multiple of at least `"1x"`
//!   ([`AutoscaleRecord::validate_inherited`]).
//! - **Signals are all or nothing.** `cpu`, `memory` and `checks` move as
//!   one unit: the highest level that names any of them supplies all of
//!   them. Defaults may set `cpu` and `memory`, never `checks`.
//! - **Namespace policies have no precedence among themselves.** Every
//!   matching policy applies; two that set one field to different values
//!   are an error naming both, unless the task sets that field itself.
//!
//! The result is an [`EffectivePolicy`] (fully numeric: every default
//! filled, every `"Nx"` resolved), the [`Origin`] of each field, and a
//! stable digest of the effective policy.
//!
//! # What this does not check
//!
//! Resolution needs only the four records and `count`. Checks that need
//! the rest of the job or the cluster stay with the caller (the manager's
//! `validate_job_config` in later phases): that the task's runtime reports
//! usage when a `legion` utilization check is in use, the multi-task group
//! rules (calls entry 15), gang quorum, keyed instances and `count.per`, and
//! that a check's `source` names an installed source.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use super::spec::{
    format_duration, AutoscaleRecord, AutoscaleSpec, BehaviorSpec, CountBound, DurationSpec, Mode,
    OnMissing, StepSpec,
};
use super::strategies::{fmt_num, StrategyConfig, TargetValue};

/// The built-in source every task has.
pub const LEGION_SOURCE: &str = "legion";
/// The `legion` source's query behind the `cpu` shorthand.
pub const CPU_UTILIZATION_QUERY: &str = "task.cpu.utilization";
/// The `legion` source's query behind the `memory` shorthand.
pub const MEMORY_UTILIZATION_QUERY: &str = "task.memory.utilization";

/// The built-in defaults (plan §6.3; calls entry 2 defends each):
///
/// ```nickel
/// autoscale = {
///   min = "1x", max = "4x", cpu = 0.7,
///   up   = { stabilize = "0s", step = { percent = 100, replicas = 4 }, every = "1m" },
///   down = { stabilize = "5m", step = { percent = 10,  replicas = 1 }, every = "1m" },
///   headroom = 0, warmup = "30s", tolerance = 0.1, window = "1m",
///   schedule = [], onMissing = "hold", mode = "enforce",
/// }
/// ```
pub fn builtin_defaults() -> AutoscaleRecord {
    AutoscaleRecord {
        min: Some(CountBound::Multiple(1.0)),
        max: Some(CountBound::Multiple(4.0)),
        cpu: Some(0.7),
        memory: None,
        checks: None,
        up: Some(BehaviorSpec {
            stabilize: Some(DurationSpec::secs(0)),
            step: Some(StepSpec {
                percent: Some(100.0),
                replicas: Some(4),
            }),
            every: Some(DurationSpec::secs(60)),
        }),
        down: Some(BehaviorSpec {
            stabilize: Some(DurationSpec::secs(300)),
            step: Some(StepSpec {
                percent: Some(10.0),
                replicas: Some(1),
            }),
            every: Some(DurationSpec::secs(60)),
        }),
        headroom: Some(0),
        warmup: Some(DurationSpec::secs(30)),
        tolerance: Some(0.1),
        window: Some(DurationSpec::secs(60)),
        schedule: Some(Vec::new()),
        on_missing: Some(OnMissing::Hold),
        mode: Some(Mode::Enforce),
    }
}

/// Where a field's effective value came from.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "level", rename_all = "kebab-case")]
pub enum Origin {
    /// Legion's built-in default.
    BuiltIn,
    /// The cluster's scaling defaults.
    Cluster,
    /// One or more namespace policies (several when they agree on the
    /// value), sorted by name.
    NamespacePolicy {
        /// The policies that set the value.
        policies: Vec<String>,
    },
    /// The task's own `autoscale` record.
    Jobspec,
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Origin::BuiltIn => f.write_str("built-in"),
            Origin::Cluster => f.write_str("cluster defaults"),
            Origin::NamespacePolicy { policies } if policies.len() == 1 => {
                write!(f, "namespace policy {}", policies[0])
            }
            Origin::NamespacePolicy { policies } => {
                write!(f, "namespace policies {}", policies.join(", "))
            }
            Origin::Jobspec => f.write_str("jobspec"),
        }
    }
}

/// One namespace policy's `autoscale` record, named for errors and
/// provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceDefaults {
    /// The namespace policy's name.
    pub policy: String,
    /// Its `autoscale` record.
    pub record: AutoscaleRecord,
}

/// The inherited levels for one namespace.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultsLayers<'a> {
    /// The cluster's scaling defaults, if any are set.
    pub cluster: Option<&'a AutoscaleRecord>,
    /// The namespace the task lives in (for error messages).
    pub namespace: &'a str,
    /// The `autoscale` records of every namespace policy governing that
    /// namespace (policies without one may be included or left out).
    pub policies: &'a [NamespaceDefaults],
}

/// A scale-out or scale-in behavior, fully resolved.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Behavior {
    /// The stabilization window.
    #[serde(with = "super::spec::duration_text")]
    pub stabilize: Duration,
    /// The step limit per `every`.
    pub step: Step,
    /// The step limit's rolling window.
    #[serde(with = "super::spec::duration_text")]
    pub every: Duration,
}

/// A resolved step limit: the larger of the two halves per window.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Step {
    /// Percent of the count at the start of the window.
    pub percent: f64,
    /// Absolute replicas.
    pub replicas: u32,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let replicas = if self.replicas == 1 {
            "1 replica".to_string()
        } else {
            format!("{} replicas", self.replicas)
        };
        match (self.percent > 0.0, self.replicas > 0) {
            (true, true) => write!(f, "{}% or {replicas}", fmt_num(self.percent)),
            (true, false) => write!(f, "{}%", fmt_num(self.percent)),
            (false, _) => f.write_str(&replicas),
        }
    }
}

/// A check as the engine runs it: the `cpu`/`memory` shorthands expanded,
/// the window filled, the strategy typed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveCheck {
    /// The check's name; also the key of its reading in an
    /// [`Observation`](super::pipeline::Observation).
    pub name: String,
    /// The source to read.
    pub source: String,
    /// The source's query.
    pub query: String,
    /// The averaging window.
    #[serde(with = "super::spec::duration_text")]
    pub window: Duration,
    /// The strategy and its settings.
    pub strategy: StrategyConfig,
    /// The scale-to-zero activation threshold, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<f64>,
    /// Whether the check may pull the count down.
    pub scale_in: bool,
}

impl EffectiveCheck {
    /// Whether the reading is an average over ready replicas (a `legion`
    /// utilization), which is what the warmup rule applies to.
    pub fn per_ready_replica(&self) -> bool {
        self.source == LEGION_SOURCE && self.query.ends_with(".utilization")
    }

    /// Whether the check reads the task's own usage, which is empty at zero
    /// replicas and so can never wake a task from zero.
    pub fn reads_own_usage(&self) -> bool {
        self.source == LEGION_SOURCE && self.query.starts_with("task.")
    }
}

/// A schedule with its bounds resolved to replica counts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveSchedule {
    /// The crontab expression.
    pub cron: String,
    /// The window length (`for`).
    #[serde(rename = "for", with = "super::spec::duration_text")]
    pub duration: Duration,
    /// The floor while open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<u32>,
    /// The ceiling while open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
    /// The IANA zone the expression is evaluated in.
    pub timezone: String,
}

/// The policy the engine runs: every default filled, every `"Nx"` resolved,
/// every field numeric. This is what `WatchScalingPolicies` delivers, what
/// [`EffectivePolicy::digest`] hashes, and what a `scaling_policy_snapshot`
/// row stores.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectivePolicy {
    /// The floor.
    pub min: u32,
    /// The ceiling.
    pub max: u32,
    /// The CPU utilization target, if CPU is a signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<f64>,
    /// The memory utilization target, if memory is a signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<f64>,
    /// Explicit checks (the shorthands are not repeated here; see
    /// [`EffectivePolicy::all_checks`]).
    pub checks: Vec<EffectiveCheck>,
    /// Scale-out behavior.
    pub up: Behavior,
    /// Scale-in behavior.
    pub down: Behavior,
    /// Replicas added after the merge.
    pub headroom: u32,
    /// How long a newly healthy replica is warming.
    #[serde(with = "super::spec::duration_text")]
    pub warmup: Duration,
    /// The tolerance band, as a fraction of a target.
    pub tolerance: f64,
    /// The default averaging window.
    #[serde(with = "super::spec::duration_text")]
    pub window: Duration,
    /// Bound overrides in time windows.
    pub schedule: Vec<EffectiveSchedule>,
    /// What a check with no data does.
    pub on_missing: OnMissing,
    /// Whether decisions are written.
    pub mode: Mode,
}

impl EffectivePolicy {
    /// Every check the engine evaluates: `cpu`, then `memory` (when set,
    /// as `target-value` checks on the `legion` source), then the explicit
    /// checks.
    pub fn all_checks(&self) -> Vec<EffectiveCheck> {
        let shorthand = |name: &str, query: &str, target: f64| EffectiveCheck {
            name: name.to_string(),
            source: LEGION_SOURCE.to_string(),
            query: query.to_string(),
            window: self.window,
            strategy: StrategyConfig::TargetValue(TargetValue { target }),
            activation: None,
            scale_in: true,
        };
        let mut checks = Vec::with_capacity(self.checks.len() + 2);
        if let Some(cpu) = self.cpu {
            checks.push(shorthand("cpu", CPU_UTILIZATION_QUERY, cpu));
        }
        if let Some(memory) = self.memory {
            checks.push(shorthand("memory", MEMORY_UTILIZATION_QUERY, memory));
        }
        checks.extend(self.checks.iter().cloned());
        checks
    }

    /// The policy's canonical JSON: struct fields in declaration order, no
    /// maps with unordered keys, durations as text.
    pub fn canonical_json(&self) -> String {
        serde_json::to_string(self).expect("effective policy should serialize")
    }

    /// A stable digest of the effective policy: the hex SHA-256 of
    /// [`canonical_json`](Self::canonical_json). Two policies that behave
    /// the same have the same digest, whichever levels their fields came
    /// from. Each `scale_history` row carries it, so a decision always
    /// resolves to the exact policy that made it.
    pub fn digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.canonical_json().as_bytes());
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

/// The result of [`resolve`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedPolicy {
    /// The policy the engine runs.
    pub policy: EffectivePolicy,
    /// The task's `count` the policy was resolved against.
    pub count: u32,
    /// The merged record before `"Nx"` resolution (every field set), which
    /// keeps the written form of `min`, `max` and schedule bounds.
    pub record: AutoscaleRecord,
    /// The origin of every field, keyed by its path (`min`, `cpu`,
    /// `down.step`, `onMissing`, ...). Signal keys (`cpu`, `memory`,
    /// `checks`) appear only for the signals in effect.
    pub provenance: BTreeMap<String, Origin>,
    /// [`EffectivePolicy::digest`], computed once.
    pub digest: String,
}

/// One row of `legion scaling explain --policy`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvenanceRow {
    /// The field path.
    pub field: String,
    /// The effective value, rendered for people (`4 ("1x" of count 4)`).
    pub value: String,
    /// Where it came from.
    pub origin: Origin,
}

impl ResolvedPolicy {
    /// The provenance table, one row per field, in a fixed order.
    pub fn explain_rows(&self) -> Vec<ProvenanceRow> {
        provenance_rows(&self.record, &self.provenance, Some(self.count))
    }
}

/// The effective defaults for a namespace, with no task: what a task with
/// `autoscale = true` would get, before `"Nx"` resolution
/// (`legion scaling defaults show -n <namespace>`).
#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveDefaults {
    /// Every field set.
    pub record: AutoscaleRecord,
    /// The origin of every field.
    pub provenance: BTreeMap<String, Origin>,
}

impl EffectiveDefaults {
    /// The provenance table, with bounds shown as written (`"1x"`).
    pub fn explain_rows(&self) -> Vec<ProvenanceRow> {
        provenance_rows(&self.record, &self.provenance, None)
    }
}

const SIGNALS: &str = "signals";
const LEAF_ORDER: [&str; 16] = [
    "min",
    "max",
    SIGNALS,
    "up.stabilize",
    "up.step",
    "up.every",
    "down.stabilize",
    "down.step",
    "down.every",
    "headroom",
    "warmup",
    "tolerance",
    "window",
    "schedule",
    "onMissing",
    "mode",
];

type Leaves = BTreeMap<&'static str, Value>;
type Merged = BTreeMap<&'static str, (Value, Origin)>;

fn put<T: Serialize>(out: &mut Leaves, key: &'static str, value: &Option<T>) {
    if let Some(value) = value {
        out.insert(
            key,
            serde_json::to_value(value).expect("autoscale field should serialize"),
        );
    }
}

/// Flatten a sparse record into the units that override one another: every
/// scalar, each half of `up`/`down` (with `step` as one unit), and the
/// signal set as one unit.
fn leaves(record: &AutoscaleRecord) -> Leaves {
    let mut out = Leaves::new();
    put(&mut out, "min", &record.min);
    put(&mut out, "max", &record.max);
    if record.names_signal() {
        out.insert(
            SIGNALS,
            json!({ "cpu": record.cpu, "memory": record.memory, "checks": record.checks }),
        );
    }
    for (keys, behavior) in [
        (["up.stabilize", "up.step", "up.every"], &record.up),
        (["down.stabilize", "down.step", "down.every"], &record.down),
    ] {
        if let Some(behavior) = behavior {
            put(&mut out, keys[0], &behavior.stabilize);
            put(&mut out, keys[1], &behavior.step);
            put(&mut out, keys[2], &behavior.every);
        }
    }
    put(&mut out, "headroom", &record.headroom);
    put(&mut out, "warmup", &record.warmup);
    put(&mut out, "tolerance", &record.tolerance);
    put(&mut out, "window", &record.window);
    put(&mut out, "schedule", &record.schedule);
    put(&mut out, "onMissing", &record.on_missing);
    put(&mut out, "mode", &record.mode);
    out
}

fn overlay(merged: &mut Merged, leaves: Leaves, origin: &Origin) {
    for (key, value) in leaves {
        merged.insert(key, (value, origin.clone()));
    }
}

fn field_label(key: &str) -> String {
    if key == SIGNALS {
        "the autoscale signals (cpu, memory)".to_string()
    } else {
        format!("autoscale.{key}")
    }
}

fn render(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| format!("{k} = {}", render(v)))
                .collect();
            format!("{{ {} }}", parts.join(", "))
        }
        other => other.to_string(),
    }
}

/// Merge the namespace policies' records, refusing two that set one field
/// to different values unless the task sets that field itself.
fn merge_namespace_policies(
    layers: &DefaultsLayers<'_>,
    task: &Leaves,
) -> Result<BTreeMap<&'static str, (Value, Vec<String>)>, String> {
    let mut merged: BTreeMap<&'static str, (Value, Vec<String>)> = BTreeMap::new();
    let mut policies: Vec<&NamespaceDefaults> = layers.policies.iter().collect();
    policies.sort_by(|a, b| a.policy.cmp(&b.policy));
    for policy in policies {
        for (key, value) in leaves(&policy.record) {
            match merged.get_mut(key) {
                None => {
                    merged.insert(key, (value, vec![policy.policy.clone()]));
                }
                Some((existing, names)) if *existing == value => {
                    names.push(policy.policy.clone());
                }
                Some(_) if task.contains_key(key) => {}
                Some((existing, names)) => {
                    let field = field_label(key);
                    return Err(format!(
                        "namespace policies '{}' and '{}' both govern namespace {} and set \
                         {field} differently ({}, {}); a task that does not set it itself has \
                         no single value. Hint: set {field} in the task's autoscale record, or \
                         make the policies agree",
                        names[0],
                        policy.policy,
                        layers.namespace,
                        render(existing),
                        render(&value),
                    ));
                }
            }
        }
    }
    Ok(merged)
}

fn merge_layers(
    layers: &DefaultsLayers<'_>,
    task: Option<&AutoscaleRecord>,
) -> Result<Merged, String> {
    if let Some(cluster) = layers.cluster {
        cluster.validate_inherited("cluster defaults")?;
    }
    for policy in layers.policies {
        policy
            .record
            .validate_inherited(&format!("namespace policy {}", policy.policy))?;
    }
    let task_leaves = task.map(leaves).unwrap_or_default();
    let mut merged = Merged::new();
    overlay(&mut merged, leaves(&builtin_defaults()), &Origin::BuiltIn);
    if let Some(cluster) = layers.cluster {
        overlay(&mut merged, leaves(cluster), &Origin::Cluster);
    }
    for (key, (value, policies)) in merge_namespace_policies(layers, &task_leaves)? {
        merged.insert(key, (value, Origin::NamespacePolicy { policies }));
    }
    overlay(&mut merged, task_leaves, &Origin::Jobspec);
    Ok(merged)
}

/// Rebuild a full record from merged leaves.
fn rebuild(merged: &Merged) -> Result<AutoscaleRecord, String> {
    let mut object = Map::new();
    for (key, (value, _)) in merged {
        if *key == SIGNALS {
            if let Value::Object(signals) = value {
                for (name, signal) in signals {
                    if !signal.is_null() {
                        object.insert(name.clone(), signal.clone());
                    }
                }
            }
            continue;
        }
        match key.split_once('.') {
            Some((parent, child)) => {
                let entry = object
                    .entry(parent.to_string())
                    .or_insert_with(|| Value::Object(Map::new()));
                if let Value::Object(nested) = entry {
                    nested.insert(child.to_string(), value.clone());
                }
            }
            None => {
                object.insert(key.to_string(), value.clone());
            }
        }
    }
    serde_json::from_value(Value::Object(object))
        .map_err(|err| format!("internal error rebuilding the autoscale record: {err}"))
}

fn provenance_map(merged: &Merged, record: &AutoscaleRecord) -> BTreeMap<String, Origin> {
    let mut provenance = BTreeMap::new();
    for (key, (_, origin)) in merged {
        if *key == SIGNALS {
            for (name, set) in [
                ("cpu", record.cpu.is_some()),
                ("memory", record.memory.is_some()),
                ("checks", record.checks.is_some()),
            ] {
                if set {
                    provenance.insert(name.to_string(), origin.clone());
                }
            }
        } else {
            provenance.insert(key.to_string(), origin.clone());
        }
    }
    provenance
}

fn behavior_of(spec: &Option<BehaviorSpec>, fallback: &BehaviorSpec) -> Behavior {
    let spec = spec.as_ref().unwrap_or(fallback);
    let step = spec.step.as_ref().or(fallback.step.as_ref());
    Behavior {
        stabilize: spec.stabilize.or(fallback.stabilize).unwrap_or_default().0,
        step: Step {
            percent: step.and_then(|s| s.percent).unwrap_or(0.0),
            replicas: step.and_then(|s| s.replicas).unwrap_or(0),
        },
        every: spec
            .every
            .or(fallback.every)
            .unwrap_or(DurationSpec::secs(60))
            .0,
    }
}

impl EffectivePolicy {
    /// Build the effective policy from a fully merged record and `count`.
    /// Fields still unset (only possible for a hand-built record) take the
    /// built-in value.
    pub fn from_record(record: &AutoscaleRecord, count: u32) -> Result<EffectivePolicy, String> {
        let builtin = builtin_defaults();
        let fallback_up = builtin.up.clone().unwrap_or_default();
        let fallback_down = builtin.down.clone().unwrap_or_default();
        let window = record.window.or(builtin.window).unwrap_or_default().0;
        let mut checks = Vec::new();
        for (index, check) in record.checks.iter().flatten().enumerate() {
            let strategy = check
                .strategy
                .to_config()
                .map_err(|err| format!("autoscale.checks[{index}] ({}): {err}", check.name))?;
            checks.push(EffectiveCheck {
                name: check.name.clone(),
                source: check.source.clone(),
                query: check.query.clone(),
                window: check.window.map(|w| w.0).unwrap_or(window),
                strategy,
                activation: check.activation,
                scale_in: check.scale_in.unwrap_or(true),
            });
        }
        let schedule = record
            .schedule
            .iter()
            .flatten()
            .map(|entry| EffectiveSchedule {
                cron: entry.cron.trim().to_string(),
                duration: entry.duration.0,
                min: entry.min.map(|bound| bound.resolve(count)),
                max: entry.max.map(|bound| bound.resolve(count)),
                timezone: entry.timezone.clone().unwrap_or_else(|| "UTC".to_string()),
            })
            .collect();
        Ok(EffectivePolicy {
            min: record
                .min
                .or(builtin.min)
                .map(|b| b.resolve(count))
                .unwrap_or(count),
            max: record
                .max
                .or(builtin.max)
                .map(|b| b.resolve(count))
                .unwrap_or(count),
            cpu: record.cpu,
            memory: record.memory,
            checks,
            up: behavior_of(&record.up, &fallback_up),
            down: behavior_of(&record.down, &fallback_down),
            headroom: record.headroom.unwrap_or(0),
            warmup: record.warmup.or(builtin.warmup).unwrap_or_default().0,
            tolerance: record.tolerance.or(builtin.tolerance).unwrap_or(0.1),
            window,
            schedule,
            on_missing: record.on_missing.unwrap_or_default(),
            mode: record.mode.unwrap_or_default(),
        })
    }
}

/// The effective defaults for a namespace, with no task (see
/// [`EffectiveDefaults`]). Fails on the same inherited-level errors as
/// [`resolve`], including two namespace policies that disagree.
pub fn effective_defaults(layers: &DefaultsLayers<'_>) -> Result<EffectiveDefaults, String> {
    let merged = merge_layers(layers, None)?;
    let record = rebuild(&merged)?;
    let provenance = provenance_map(&merged, &record);
    Ok(EffectiveDefaults { record, provenance })
}

/// Resolve a task's `autoscale` against the inherited levels.
///
/// Returns `Ok(None)` for `autoscale = false`. `target` labels errors
/// (`web/default/web`), in the form submit errors use:
///
/// ```text
/// web/default/web: autoscale.min = 0 needs a check that can wake it from zero. ...
/// ```
///
/// Resolution is pure and cheap, so the manager can re-run it for every
/// inheriting policy whenever any level changes (defaults resolve live,
/// calls entry 3), and use the same function to refuse a defaults change
/// that would make some inheriting policy invalid.
pub fn resolve(
    spec: &AutoscaleSpec,
    count: u32,
    layers: &DefaultsLayers<'_>,
    target: &str,
) -> Result<Option<ResolvedPolicy>, String> {
    let Some(task) = spec.record() else {
        return Ok(None);
    };
    let with_target = |err: String| format!("{target}: {err}");
    task.validate().map_err(with_target)?;
    let merged = merge_layers(layers, Some(&task)).map_err(with_target)?;
    let record = rebuild(&merged).map_err(with_target)?;
    let provenance = provenance_map(&merged, &record);
    let policy = EffectivePolicy::from_record(&record, count).map_err(with_target)?;
    validate_effective(&policy, &record, &provenance, count).map_err(with_target)?;
    let digest = policy.digest();
    Ok(Some(ResolvedPolicy {
        policy,
        count,
        record,
        provenance,
        digest,
    }))
}

fn origin_of(provenance: &BTreeMap<String, Origin>, key: &str) -> String {
    provenance
        .get(key)
        .map(|origin| origin.to_string())
        .unwrap_or_else(|| "built-in".to_string())
}

/// The rules that need `count`: bounds against count, schedules against
/// bounds, and the scale-to-zero wake rule (plan §6.12).
fn validate_effective(
    policy: &EffectivePolicy,
    record: &AutoscaleRecord,
    provenance: &BTreeMap<String, Origin>,
    count: u32,
) -> Result<(), String> {
    let written = |bound: Option<CountBound>, value: u32| match bound {
        Some(multiple @ CountBound::Multiple(_)) => {
            format!("{value} (\"{multiple}\" of count {count})")
        }
        _ => value.to_string(),
    };
    let min_text = written(record.min, policy.min);
    let max_text = written(record.max, policy.max);
    let min_from = origin_of(provenance, "min");
    let max_from = origin_of(provenance, "max");
    if policy.max == 0 {
        return Err(format!(
            "autoscale.max is {max_text} (from {max_from}); the ceiling must be at least 1. \
             Hint: set max to a replica count"
        ));
    }
    if count > policy.max {
        return Err(format!(
            "count {count} is above autoscale.max = {max_text} (from {max_from}). Hint: raise \
             max in the task's autoscale record, or lower count"
        ));
    }
    if policy.min > policy.max {
        return Err(format!(
            "autoscale.min = {min_text} (from {min_from}) is above autoscale.max = {max_text} \
             (from {max_from}). Hint: raise max in the task's autoscale record"
        ));
    }
    if provenance.get("min") == Some(&Origin::Jobspec) && policy.min > count {
        return Err(format!(
            "autoscale.min = {min_text} is above count {count}; count is the starting size \
             and must sit within the bounds. Hint: raise count to at least {}, or lower min",
            policy.min
        ));
    }
    for (index, schedule) in policy.schedule.iter().enumerate() {
        if let Some(min) = schedule.min {
            if min > policy.max {
                return Err(format!(
                    "autoscale.schedule[{index}] ('{}'): min = {min} is above autoscale.max = \
                     {max_text}; a schedule only raises the floor within the bounds. Hint: \
                     raise max",
                    schedule.cron
                ));
            }
        }
        if let Some(max) = schedule.max {
            if max < policy.min {
                return Err(format!(
                    "autoscale.schedule[{index}] ('{}'): max = {max} is below autoscale.min = \
                     {min_text}; a schedule only lowers the ceiling within the bounds. Hint: \
                     lower min",
                    schedule.cron
                ));
            }
            if max == 0 {
                return Err(format!(
                    "autoscale.schedule[{index}] ('{}'): max must be at least 1",
                    schedule.cron
                ));
            }
        }
    }
    if policy.min == 0 {
        let checks = policy.all_checks();
        let can_wake = checks
            .iter()
            .any(|check| check.activation.is_some() && !check.reads_own_usage())
            || policy.schedule.iter().any(|s| s.min.unwrap_or(0) >= 1);
        if !can_wake {
            let own: Vec<String> = checks
                .iter()
                .filter(|check| check.reads_own_usage())
                .map(|check| format!("'{}'", check.name))
                .collect();
            let why = if own.is_empty() {
                "no check has an activation threshold".to_string()
            } else {
                format!(
                    "{} read{} the task's own usage, which is empty at 0 replicas",
                    own.join(", "),
                    if own.len() == 1 { "s" } else { "" }
                )
            };
            return Err(format!(
                "autoscale.min = 0 needs a check that can wake it from zero; {why}. Hint: add \
                 a check with `activation` on an external source or a schedule, or set min >= 1"
            ));
        }
    }
    Ok(())
}

fn provenance_rows(
    record: &AutoscaleRecord,
    provenance: &BTreeMap<String, Origin>,
    count: Option<u32>,
) -> Vec<ProvenanceRow> {
    let bound = |value: Option<CountBound>| -> String {
        match (value, count) {
            (Some(CountBound::Multiple(m)), Some(count)) => format!(
                "{} (\"{}\" of count {count})",
                CountBound::Multiple(m).resolve(count),
                CountBound::Multiple(m)
            ),
            (Some(bound), _) => bound.to_string(),
            (None, _) => "-".to_string(),
        }
    };
    let duration = |value: Option<DurationSpec>| {
        value
            .map(|d| format_duration(d.0))
            .unwrap_or_else(|| "-".to_string())
    };
    let step = |behavior: &Option<BehaviorSpec>| -> String {
        let step = behavior.as_ref().and_then(|b| b.step.as_ref());
        match step {
            Some(step) => Step {
                percent: step.percent.unwrap_or(0.0),
                replicas: step.replicas.unwrap_or(0),
            }
            .to_string(),
            None => "-".to_string(),
        }
    };
    let up = &record.up;
    let down = &record.down;
    let mut rows: Vec<(String, String)> = vec![
        ("min".into(), bound(record.min)),
        ("max".into(), bound(record.max)),
    ];
    if let Some(cpu) = record.cpu {
        rows.push(("cpu".into(), fmt_num(cpu)));
    }
    if let Some(memory) = record.memory {
        rows.push(("memory".into(), fmt_num(memory)));
    }
    if let Some(checks) = &record.checks {
        let names: Vec<&str> = checks.iter().map(|check| check.name.as_str()).collect();
        rows.push(("checks".into(), names.join(", ")));
    }
    rows.extend([
        (
            "up.stabilize".into(),
            duration(up.as_ref().and_then(|b| b.stabilize)),
        ),
        ("up.step".into(), step(up)),
        (
            "up.every".into(),
            duration(up.as_ref().and_then(|b| b.every)),
        ),
        (
            "down.stabilize".into(),
            duration(down.as_ref().and_then(|b| b.stabilize)),
        ),
        ("down.step".into(), step(down)),
        (
            "down.every".into(),
            duration(down.as_ref().and_then(|b| b.every)),
        ),
        ("headroom".into(), record.headroom.unwrap_or(0).to_string()),
        ("warmup".into(), duration(record.warmup)),
        (
            "tolerance".into(),
            record.tolerance.map(fmt_num).unwrap_or_else(|| "-".into()),
        ),
        ("window".into(), duration(record.window)),
        (
            "schedule".into(),
            match record.schedule.as_ref().map(Vec::len).unwrap_or(0) {
                0 => "none".to_string(),
                1 => "1 window".to_string(),
                n => format!("{n} windows"),
            },
        ),
        (
            "onMissing".into(),
            record.on_missing.unwrap_or_default().to_string(),
        ),
        ("mode".into(), record.mode.unwrap_or_default().to_string()),
    ]);
    rows.into_iter()
        .map(|(field, value)| {
            let origin = provenance.get(&field).cloned().unwrap_or(Origin::BuiltIn);
            ProvenanceRow {
                field,
                value,
                origin,
            }
        })
        .collect()
}

/// The field paths provenance is keyed by, in display order (signals
/// expand to `cpu`, `memory`, `checks`).
pub fn field_paths() -> Vec<&'static str> {
    LEAF_ORDER
        .iter()
        .flat_map(|key| {
            if *key == SIGNALS {
                vec!["cpu", "memory", "checks"]
            } else {
                vec![*key]
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(value: Value) -> AutoscaleRecord {
        serde_json::from_value(value).unwrap()
    }

    fn spec(value: Value) -> AutoscaleSpec {
        serde_json::from_value(value).unwrap()
    }

    fn ns(policy: &str, value: Value) -> NamespaceDefaults {
        NamespaceDefaults {
            policy: policy.to_string(),
            record: record(value),
        }
    }

    fn resolve_with(
        task: Value,
        count: u32,
        cluster: Option<Value>,
        policies: &[NamespaceDefaults],
    ) -> Result<ResolvedPolicy, String> {
        let cluster = cluster.map(record);
        let layers = DefaultsLayers {
            cluster: cluster.as_ref(),
            namespace: "payments-prod",
            policies,
        };
        resolve(&spec(task), count, &layers, "payments-prod/api/default/api")
            .map(|resolved| resolved.expect("autoscale is on"))
    }

    fn origin_of(resolved: &ResolvedPolicy, key: &str) -> String {
        resolved
            .provenance
            .get(key)
            .map(|o| o.to_string())
            .unwrap_or_else(|| format!("<no {key}>"))
    }

    #[test]
    fn autoscale_true_is_the_builtin_record_test() {
        let resolved = resolve_with(json!(true), 2, None, &[]).unwrap();
        let p = &resolved.policy;
        assert_eq!((p.min, p.max), (2, 8));
        assert_eq!(p.cpu, Some(0.7));
        assert_eq!(p.memory, None);
        assert!(p.checks.is_empty());
        assert_eq!(p.up.stabilize, Duration::ZERO);
        assert_eq!(
            p.up.step,
            Step {
                percent: 100.0,
                replicas: 4
            }
        );
        assert_eq!(p.up.every, Duration::from_secs(60));
        assert_eq!(p.down.stabilize, Duration::from_secs(300));
        assert_eq!(
            p.down.step,
            Step {
                percent: 10.0,
                replicas: 1
            }
        );
        assert_eq!(p.headroom, 0);
        assert_eq!(p.warmup, Duration::from_secs(30));
        assert_eq!(p.tolerance, 0.1);
        assert_eq!(p.window, Duration::from_secs(60));
        assert_eq!(p.on_missing, OnMissing::Hold);
        assert_eq!(p.mode, Mode::Enforce);
        for key in field_paths() {
            if key == "memory" || key == "checks" {
                assert!(!resolved.provenance.contains_key(key), "{key}");
            } else {
                assert_eq!(origin_of(&resolved, key), "built-in", "{key}");
            }
        }
        let all = p.all_checks();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "cpu");
        assert_eq!(all[0].query, CPU_UTILIZATION_QUERY);
        assert!(all[0].per_ready_replica());

        let layers = DefaultsLayers::default();
        assert!(resolve(&AutoscaleSpec::Enabled(false), 2, &layers, "t")
            .unwrap()
            .is_none());
    }

    /// The plan's §6.3 worked example: cluster, namespace and task levels,
    /// each overriding different fields, with the provenance table it shows.
    #[test]
    fn precedence_matrix_matches_the_plan_example_test() {
        let resolved = resolve_with(
            json!({ "max": 20 }),
            4,
            Some(json!({ "cpu": 0.6, "max": "6x", "down": { "stabilize": "10m" } })),
            &[ns(
                "payments",
                json!({ "down": { "step": { "replicas": 1 }, "every": "2m" }, "mode": "dry-run" }),
            )],
        )
        .unwrap();
        let p = &resolved.policy;
        assert_eq!((p.min, p.max), (4, 20));
        assert_eq!(p.cpu, Some(0.6));
        assert_eq!(p.down.stabilize, Duration::from_secs(600));
        assert_eq!(
            p.down.step,
            Step {
                percent: 0.0,
                replicas: 1
            }
        );
        assert_eq!(p.down.every, Duration::from_secs(120));
        assert_eq!(p.mode, Mode::DryRun);

        let table: Vec<(String, String, String)> = resolved
            .explain_rows()
            .into_iter()
            .map(|row| (row.field, row.value, row.origin.to_string()))
            .collect();
        let expect = |field: &str, value: &str, origin: &str| {
            assert!(
                table.contains(&(field.to_string(), value.to_string(), origin.to_string())),
                "{field} {value} {origin} not in {table:#?}"
            );
        };
        expect("min", "4 (\"1x\" of count 4)", "built-in");
        expect("max", "20", "jobspec");
        expect("cpu", "0.6", "cluster defaults");
        expect("down.stabilize", "10m", "cluster defaults");
        expect("down.step", "1 replica", "namespace policy payments");
        expect("down.every", "2m", "namespace policy payments");
        expect("up.stabilize", "0s", "built-in");
        expect("up.step", "100% or 4 replicas", "built-in");
        expect("up.every", "1m", "built-in");
        expect("mode", "dry-run", "namespace policy payments");
        expect("onMissing", "hold", "built-in");
    }

    #[test]
    fn nested_merge_and_step_replacement_test() {
        // (cluster, task, expected up, expected down)
        let cases = [
            // A nested field keeps its siblings from below.
            (
                json!({ "down": { "stabilize": "10m" } }),
                json!(true),
                (
                    0,
                    Step {
                        percent: 100.0,
                        replicas: 4,
                    },
                    60,
                ),
                (
                    600,
                    Step {
                        percent: 10.0,
                        replicas: 1,
                    },
                    60,
                ),
            ),
            // A partial step replaces the whole step: the other half is 0.
            (
                json!({}),
                json!({ "up": { "step": { "replicas": 2 } } }),
                (
                    0,
                    Step {
                        percent: 0.0,
                        replicas: 2,
                    },
                    60,
                ),
                (
                    300,
                    Step {
                        percent: 10.0,
                        replicas: 1,
                    },
                    60,
                ),
            ),
            (
                json!({ "down": { "step": { "percent": 50 } } }),
                json!({ "down": { "every": "2m" } }),
                (
                    0,
                    Step {
                        percent: 100.0,
                        replicas: 4,
                    },
                    60,
                ),
                (
                    300,
                    Step {
                        percent: 50.0,
                        replicas: 0,
                    },
                    120,
                ),
            ),
            // The task's step replaces the cluster's, not merges with it.
            (
                json!({ "up": { "step": { "percent": 300, "replicas": 10 } } }),
                json!({ "up": { "step": { "percent": 50 }, "stabilize": "30s" } }),
                (
                    30,
                    Step {
                        percent: 50.0,
                        replicas: 0,
                    },
                    60,
                ),
                (
                    300,
                    Step {
                        percent: 10.0,
                        replicas: 1,
                    },
                    60,
                ),
            ),
        ];
        for (cluster, task, up, down) in cases {
            let resolved = resolve_with(task.clone(), 2, Some(cluster.clone()), &[]).unwrap();
            let p = &resolved.policy;
            let got_up = (p.up.stabilize.as_secs(), p.up.step, p.up.every.as_secs());
            let got_down = (
                p.down.stabilize.as_secs(),
                p.down.step,
                p.down.every.as_secs(),
            );
            assert_eq!(got_up, up, "{cluster} / {task}");
            assert_eq!(got_down, down, "{cluster} / {task}");
        }
    }

    #[test]
    fn multiples_resolve_against_count_test() {
        // (count, cluster, task, (min, max))
        let cases = [
            (3, json!({}), json!(true), (3, 12)),
            (3, json!({ "max": "6x" }), json!(true), (3, 18)),
            (
                3,
                json!({ "min": "1.5x", "max": "6x" }),
                json!(true),
                (5, 18),
            ),
            (3, json!({ "max": "6x" }), json!({ "max": "2x" }), (3, 6)),
            (3, json!({}), json!({ "min": 1, "max": 10 }), (1, 10)),
            (1, json!({}), json!({ "max": "2.5x" }), (1, 3)),
        ];
        for (count, cluster, task, bounds) in cases {
            let resolved = resolve_with(task.clone(), count, Some(cluster.clone()), &[]).unwrap();
            assert_eq!(
                (resolved.policy.min, resolved.policy.max),
                bounds,
                "{count} {cluster} {task}"
            );
        }
    }

    #[test]
    fn schedule_bounds_resolve_against_count_test() {
        let resolved = resolve_with(
            json!({ "schedule": [{ "cron": "0 8 * * 1-5", "for": "10h", "min": "2x",
                                   "timezone": "America/New_York" }] }),
            3,
            None,
            &[],
        )
        .unwrap();
        assert_eq!(
            resolved.policy.schedule,
            vec![EffectiveSchedule {
                cron: "0 8 * * 1-5".to_string(),
                duration: Duration::from_secs(36_000),
                min: Some(6),
                max: None,
                timezone: "America/New_York".to_string(),
            }]
        );
        assert_eq!(origin_of(&resolved, "schedule"), "jobspec");
    }

    #[test]
    fn signals_are_all_or_nothing_test() {
        let lag = json!({ "name": "lag", "source": "team-prom", "query": "sum(lag)",
                          "strategy": { "name": "per-replica", "value": 500 } });
        // (cluster, namespace, task, expected cpu, memory, checks, origin of the signal)
        let cases = [
            // Task names a check: exactly that, no CPU.
            (
                json!({}),
                json!({}),
                json!({ "checks": [lag.clone()] }),
                None,
                None,
                1,
                "jobspec",
            ),
            // Task names memory: memory only, not memory plus the default CPU.
            (
                json!({}),
                json!({}),
                json!({ "memory": 0.8 }),
                None,
                Some(0.8),
                0,
                "jobspec",
            ),
            // Task names nothing: the default signal, from the highest level
            // that names one.
            (
                json!({ "cpu": 0.6 }),
                json!({}),
                json!({ "max": 9 }),
                Some(0.6),
                None,
                0,
                "cluster defaults",
            ),
            (
                json!({ "cpu": 0.6 }),
                json!({ "memory": 0.9 }),
                json!(true),
                None,
                Some(0.9),
                0,
                "namespace policy prod",
            ),
            (
                json!({ "cpu": 0.6, "memory": 0.8 }),
                json!({}),
                json!(true),
                Some(0.6),
                Some(0.8),
                0,
                "cluster defaults",
            ),
            // A cluster that sets memory alone replaces the built-in CPU.
            (
                json!({ "memory": 0.8 }),
                json!({}),
                json!(true),
                None,
                Some(0.8),
                0,
                "cluster defaults",
            ),
        ];
        for (cluster, namespace, task, cpu, memory, checks, origin) in cases {
            let policies = [ns("prod", namespace.clone())];
            let resolved = resolve_with(task.clone(), 2, Some(cluster.clone()), &policies).unwrap();
            let p = &resolved.policy;
            assert_eq!(p.cpu, cpu, "{cluster} {namespace} {task}");
            assert_eq!(p.memory, memory, "{cluster} {namespace} {task}");
            assert_eq!(p.checks.len(), checks, "{cluster} {namespace} {task}");
            let key = if checks > 0 {
                "checks"
            } else if cpu.is_some() {
                "cpu"
            } else {
                "memory"
            };
            assert_eq!(
                origin_of(&resolved, key),
                origin,
                "{cluster} {namespace} {task}"
            );
        }
    }

    #[test]
    fn namespace_policies_all_apply_and_conflicts_name_both_test() {
        // Different fields: both apply.
        let resolved = resolve_with(
            json!(true),
            2,
            None,
            &[
                ns("b-slow", json!({ "down": { "stabilize": "15m" } })),
                ns("a-dry", json!({ "mode": "dry-run" })),
            ],
        )
        .unwrap();
        assert_eq!(resolved.policy.down.stabilize, Duration::from_secs(900));
        assert_eq!(resolved.policy.mode, Mode::DryRun);
        assert_eq!(origin_of(&resolved, "mode"), "namespace policy a-dry");

        // The same field, the same value: fine, and both are named.
        let resolved = resolve_with(
            json!(true),
            2,
            None,
            &[
                ns("one", json!({ "warmup": "1m" })),
                ns("two", json!({ "warmup": "60s" })),
            ],
        )
        .unwrap();
        assert_eq!(
            origin_of(&resolved, "warmup"),
            "namespace policies one, two"
        );

        // The same field, different values: refused, naming both.
        let conflicting = [
            ns("payments", json!({ "down": { "every": "2m" } })),
            ns("prod", json!({ "down": { "every": "5m" } })),
        ];
        let err = resolve_with(json!(true), 2, None, &conflicting).unwrap_err();
        assert!(err.starts_with("payments-prod/api/default/api: "), "{err}");
        assert!(
            err.contains(
                "namespace policies 'payments' and 'prod' both govern namespace \
                          payments-prod and set autoscale.down.every differently (2m, 5m)"
            ),
            "{err}"
        );
        assert!(
            err.contains("Hint: set autoscale.down.every in the task"),
            "{err}"
        );

        // ... unless the task sets the field itself.
        let resolved =
            resolve_with(json!({ "down": { "every": "3m" } }), 2, None, &conflicting).unwrap();
        assert_eq!(resolved.policy.down.every, Duration::from_secs(180));

        // Signal sets conflict as one unit.
        let err = resolve_with(
            json!(true),
            2,
            None,
            &[
                ns("x", json!({ "cpu": 0.5 })),
                ns("y", json!({ "memory": 0.5 })),
            ],
        )
        .unwrap_err();
        assert!(
            err.contains("set the autoscale signals (cpu, memory) differently"),
            "{err}"
        );
        // A whole step record conflicts as one unit too.
        let err = resolve_with(
            json!(true),
            2,
            None,
            &[
                ns("x", json!({ "up": { "step": { "replicas": 2 } } })),
                ns(
                    "y",
                    json!({ "up": { "step": { "replicas": 2, "percent": 10 } } }),
                ),
            ],
        )
        .unwrap_err();
        assert!(err.contains("autoscale.up.step differently"), "{err}");
        // The no-task view refuses the same conflict.
        let layers = DefaultsLayers {
            cluster: None,
            namespace: "payments-prod",
            policies: &conflicting,
        };
        assert!(effective_defaults(&layers).is_err());
    }

    #[test]
    fn inherited_min_below_one_x_is_refused_test() {
        let cases = [
            (
                Some(json!({ "min": 2 })),
                vec![],
                "cluster defaults: autoscale.min = 2 is a plain number",
            ),
            (
                Some(json!({ "min": "0.5x" })),
                vec![],
                "cluster defaults: autoscale.min = \"0.5x\"",
            ),
            (
                None,
                vec![ns("prod", json!({ "min": "0x" }))],
                "namespace policy prod: autoscale.min",
            ),
            (
                None,
                vec![ns(
                    "prod",
                    json!({ "checks": [{ "name": "a", "source": "legion",
                     "query": "q", "strategy": { "name": "pass-through" } }] }),
                )],
                "namespace policy prod: autoscale.checks can only be set in a jobspec",
            ),
        ];
        for (cluster, policies, expected) in cases {
            let err = resolve_with(json!(true), 3, cluster, &policies).unwrap_err();
            assert!(err.contains(expected), "{err}");
        }
        // A task may set its own floor below count; an inherited "2x" raises it.
        let resolved = resolve_with(json!({ "min": 1 }), 3, None, &[]).unwrap();
        assert_eq!(resolved.policy.min, 1);
        let resolved = resolve_with(json!(true), 3, Some(json!({ "min": "2x" })), &[]).unwrap();
        assert_eq!((resolved.policy.min, resolved.policy.max), (6, 12));
    }

    #[test]
    fn count_aware_validation_table_test() {
        let wake = json!({ "name": "lag", "source": "team-prom", "query": "sum(lag)",
                           "strategy": { "name": "per-replica", "value": 500 }, "activation": 1 });
        let own = json!({ "name": "busy", "source": "legion", "query": "task.cpu.utilization",
                          "strategy": { "name": "target-value", "target": 0.5 }, "activation": 0.1 });
        let cases: Vec<(Value, u32, Option<Value>, Option<&str>)> = vec![
            (json!({ "min": 5 }), 3, None, Some("autoscale.min = 5 is above count 3")),
            (json!({ "max": 2 }), 3, None, Some("count 3 is above autoscale.max = 2 (from jobspec)")),
            (json!(true), 30, Some(json!({ "max": 20 })),
             Some("count 30 is above autoscale.max = 20 (from cluster defaults)")),
            (json!({ "max": 4 }), 3, Some(json!({ "min": "2x" })),
             Some("autoscale.min = 6 (\"2x\" of count 3) (from cluster defaults) is above autoscale.max = 4")),
            (json!(true), 0, None, Some("the ceiling must be at least 1")),
            (json!({ "min": 0 }), 2, None,
             Some("autoscale.min = 0 needs a check that can wake it from zero; 'cpu' reads the task's own usage")),
            (json!({ "min": 0, "checks": [own] }), 2, None, Some("'busy' reads the task's own usage")),
            (json!({ "min": 0, "checks": [wake] }), 2, None, None),
            (json!({ "min": 0, "schedule": [{ "cron": "0 8 * * *", "for": "1h", "min": 1 }] }),
             2, None, None),
            (json!({ "schedule": [{ "cron": "0 8 * * *", "for": "1h", "min": 9 }] }), 2, None,
             Some("min = 9 is above autoscale.max = 8 (\"4x\" of count 2)")),
            (json!({ "schedule": [{ "cron": "0 8 * * *", "for": "1h", "max": 1 }] }), 2, None,
             Some("max = 1 is below autoscale.min = 2 (\"1x\" of count 2)")),
            (json!({ "cpu": -1 }), 2, None, Some("autoscale.cpu = -1")),
        ];
        for (task, count, cluster, expected) in cases {
            let result = resolve_with(task.clone(), count, cluster, &[]);
            match expected {
                None => assert!(result.is_ok(), "{task}: {result:?}"),
                Some(text) => {
                    let err = result.unwrap_err();
                    assert!(err.starts_with("payments-prod/api/default/api: "), "{err}");
                    assert!(err.contains(text), "{task}: {err}");
                }
            }
        }
    }

    #[test]
    fn digest_is_stable_and_level_independent_test() {
        let a = resolve_with(json!({ "cpu": 0.6 }), 2, None, &[]).unwrap();
        let b = resolve_with(json!(true), 2, Some(json!({ "cpu": 0.6 })), &[]).unwrap();
        let c = resolve_with(json!({ "cpu": 0.65 }), 2, None, &[]).unwrap();
        assert_eq!(a.digest, b.digest, "same behavior, same digest");
        assert_ne!(a.digest, c.digest);
        assert_eq!(a.digest.len(), 64);
        assert_eq!(a.digest, a.policy.digest());
        // The canonical JSON round-trips to the same policy and digest.
        let back: EffectivePolicy = serde_json::from_str(&a.policy.canonical_json()).unwrap();
        assert_eq!(back, a.policy);
        assert_eq!(back.digest(), a.digest);
        // A count change that moves a "Nx" bound changes the digest.
        let d = resolve_with(json!({ "cpu": 0.6 }), 3, None, &[]).unwrap();
        assert_ne!(a.digest, d.digest);
    }

    #[test]
    fn effective_defaults_show_without_a_task_test() {
        let cluster = record(json!({ "cpu": 0.6, "max": "6x" }));
        let policies = [ns("payments", json!({ "mode": "dry-run" }))];
        let layers = DefaultsLayers {
            cluster: Some(&cluster),
            namespace: "payments-prod",
            policies: &policies,
        };
        let defaults = effective_defaults(&layers).unwrap();
        let rows = defaults.explain_rows();
        let find = |field: &str| rows.iter().find(|row| row.field == field).unwrap().clone();
        assert_eq!(find("max").value, "6x");
        assert_eq!(find("max").origin, Origin::Cluster);
        assert_eq!(find("min").value, "1x");
        assert_eq!(find("mode").origin.to_string(), "namespace policy payments");
    }
}
