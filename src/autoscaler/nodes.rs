//! Node-group autoscaling: the policy spec and the pure decisions behind it
//! (autoscaler v2 plan §6.11, calls entries 7 and 16).
//!
//! A node group is a pool plus a template. Its policy is the spec of an
//! `autoscaler/NodeGroup` object (`legion object apply -f nodes.ncl`; the
//! object's name is the group's, [`crate::autoscaler::policy::parse_node_group`]):
//!
//! ```nickel
//! {
//!   kind = "autoscaler/NodeGroup",
//!   name = "workers-general",
//!   spec = {
//!     target = { nodes = {
//!       pool = "default", workerType = "default",
//!       shape = { cpu = 8000, memory = 16384, attributes = { arch = "amd64" } },
//!       plugin = "aws-asg", config = { asg = "legion-workers" },
//!     } },
//!     min = 1, max = 20,
//!     up   = { step = { replicas = 5 }, every = "2m", joinTimeout = "10m" },
//!     down = { when = "empty", for = "10m", step = { percent = 10, replicas = 1 },
//!              every = "5m", drainDeadline = "10m" },
//!     expander = "least-waste",
//!   },
//! }
//! ```
//!
//! Like the rest of the engine this module is pure: no I/O, no clock (time
//! is `now_ms`, unix milliseconds), every cluster fact arrives in an
//! observation. The app (Phase 5b) reads the inputs, calls [`decide`], and
//! carries out the [`NodeIntent`]s it returns.
//!
//! ```text
//!   GetCapacityDemand(templates, upcoming)  ──▶ GroupDemand per group
//!   expand(options)                         ──▶ which group grows this tick
//!   decide(policy, state, obs, now)         ──▶ NodeDecision { to, intents, stages, summary }
//!       SetDesired(n)       → target/scale(n, token)        then state.record_scale(..)
//!       Cordon / Uncordon   → SetNodeAvailability
//!       Terminate           → target/terminate([provider id])
//!       Forget              → ForgetNode
//! ```
//!
//! # The stages
//!
//! | Stage | What it does |
//! | --- | --- |
//! | [`NodeStage::Provider`] | the count starts from what the provider holds (`target/status`), else what the engine last asked for, else the joined members |
//! | [`NodeStage::JoinTimeout`] | a scale-up whose nodes have not joined within `up.joinTimeout` is reported (`nodegroup.join_timeout`) and the group backs off for one `joinTimeout` |
//! | [`NodeStage::Demand`] | adds the nodes the pending work needs, when the expander chose this group |
//! | [`NodeStage::Step`] | the rolling `up.step` per `up.every` |
//! | [`NodeStage::Bounds`] | clamps into `min..max`; a floor applies at once, with or without demand |
//! | [`NodeStage::ScaleIn`] | picks victims: empty (or underutilized) for `down.for`, never a protected node, at most `down.step` per `down.every`, never below `min` |
//! | [`NodeStage::Pause`] | a pause holds the count, starts no drain, and aborts the drains not yet terminating |
//! | [`NodeStage::Mode`] | `dry-run` turns every action into a "would" |
//!
//! Draining is Legion's job, not the plugin's (calls entry 16): a victim is
//! cordoned and drained, terminated only once empty, uncordoned if the
//! drain outlives `down.drainDeadline`, and forgotten once the provider says
//! the instance is gone. [`step_drain`] is that state machine.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::defaults::Step;
use super::pipeline::{DecisionKind, Pause};
use super::spec::{format_duration, DurationSpec, Mode, StepSpec};
use crate::structs::namespace::SYSTEM_NAMESPACE;
use crate::structs::{is_structural_node_attr, NODEGROUP_ATTR, SCALE_IN_ATTR};

/// Default `up.step`: double the group, or add 5 nodes, whichever is more,
/// per `up.every`.
pub const DEFAULT_UP_STEP: Step = Step {
    percent: 100.0,
    replicas: 5,
};
/// Default `up.every`.
pub const DEFAULT_UP_EVERY: Duration = Duration::from_secs(120);
/// Default `up.joinTimeout`.
pub const DEFAULT_JOIN_TIMEOUT: Duration = Duration::from_secs(600);
/// Default `down.for`.
pub const DEFAULT_DOWN_FOR: Duration = Duration::from_secs(600);
/// Default `down.step`: 10% of the group or one node, whichever is more,
/// per `down.every`.
pub const DEFAULT_DOWN_STEP: Step = Step {
    percent: 10.0,
    replicas: 1,
};
/// Default `down.every`.
pub const DEFAULT_DOWN_EVERY: Duration = Duration::from_secs(300);
/// Default `down.drainDeadline`.
pub const DEFAULT_DRAIN_DEADLINE: Duration = Duration::from_secs(600);
/// `down.when = "underutilized"`: a node whose reserved share (the larger
/// of CPU and memory) is below this is underutilized.
pub const UNDERUTILIZED_BELOW: f64 = 0.5;
/// How often a terminate that has not been confirmed is asked again.
pub const TERMINATE_RETRY: Duration = Duration::from_secs(120);
/// How long after a drain starts a cordon that did not land is asked
/// again. Later, a node found uncordoned was uncordoned by someone else,
/// and the drain gives up.
pub const CORDON_GRACE: Duration = Duration::from_secs(60);
/// The longest node-group name: it is a node attribute value and a token
/// description.
pub const MAX_NAME_LEN: usize = 63;

fn ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX / 4)
}

fn clock(at_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(at_ms)
        .map(|at| at.format("%H:%M UTC").to_string())
        .unwrap_or_else(|| at_ms.to_string())
}

/// A step limit in node words: `100% or 5 nodes`.
fn step_text(step: Step) -> String {
    let nodes = plural(step.replicas, "node", "nodes");
    match (step.percent > 0.0, step.replicas > 0) {
        (true, true) => format!("{}% or {nodes}", step.percent),
        (true, false) => format!("{}%", step.percent),
        (false, _) => nodes,
    }
}

fn plural(n: u32, one: &str, many: &str) -> String {
    if n == 1 {
        format!("1 {one}")
    } else {
        format!("{n} {many}")
    }
}

// ── The spec, as written ─────────────────────────────────────────────────

/// A node-group policy as written. Every field but `name`, `target` and
/// `max` is optional and takes the default documented on it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeGroupPolicy {
    /// The group's name: lowercase letters, digits and `-`. Every member
    /// carries it as its `legion.nodegroup` attribute, stamped by the join
    /// token the group mints.
    pub name: String,
    /// What the group scales: `{ nodes = { ... } }`.
    pub target: NodeGroupTarget,
    /// The fewest nodes the group keeps. Default 0: an idle group scales to
    /// zero, and pending work brings it back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<u32>,
    /// The most nodes the group may have. Required: an unbounded node group
    /// is an unbounded bill.
    pub max: u32,
    /// Scale-up behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub up: Option<NodeUpSpec>,
    /// Scale-in behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub down: Option<NodeDownSpec>,
    /// How this group competes with other groups that could host the same
    /// pending work. Default `least-waste`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expander: Option<Expander>,
    /// `enforce` (default) acts; `dry-run` records what it would do.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
}

/// `target` of a node-group policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeGroupTarget {
    /// The node template and the provider that starts its machines.
    pub nodes: NodeTemplateSpec,
}

/// `target.nodes`: what a member looks like, and who makes one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTemplateSpec {
    /// The pool members join. Default: the worker type's pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// The worker type members join as (the join token's type). Default
    /// `default`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_type: Option<String>,
    /// A member's size and attributes. Needed to scale from zero, or before
    /// any member has joined; once one has, its observed capacity and
    /// attributes are used instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<NodeShape>,
    /// The target plugin that starts and stops the machines (`aws-asg`,
    /// `exec`, ...).
    pub plugin: String,
    /// The plugin's own settings, passed through unread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
}

/// A member's size and attributes, as the demand simulation builds it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeShape {
    /// CPU in MHz (all cores together), the unit task `cpu` reservations
    /// use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<u64>,
    /// Memory in MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<u64>,
    /// Whole cores, for tasks that reserve `cores`. Unset: not limited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cores: Option<u32>,
    /// Node attributes every member has (`arch = "amd64"`), which
    /// constraints and `distinct` see.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<BTreeMap<String, String>>,
    /// Custom resources a member offers, as a worker type's `resources`
    /// writes them (`{ gpu = 2 }` or `{ gpu = ["g0", "g1"] }`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<BTreeMap<String, Value>>,
    /// The most allocations one member takes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_allocations: Option<u32>,
}

/// `up`: scale-up behavior.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeUpSpec {
    /// The most nodes added per `every`: the larger of `percent` of the
    /// group at the start of the window and `replicas`. Default
    /// `{ percent = 100, replicas = 5 }`. Writing `step` replaces both
    /// halves: one left out is 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<StepSpec>,
    /// The step limit's rolling window. Default `"2m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<DurationSpec>,
    /// How long new nodes have to join before the scale-up is reported as
    /// timed out and the group backs off. Default `"10m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_timeout: Option<DurationSpec>,
}

/// `down`: scale-in behavior.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeDownSpec {
    /// Which nodes may go: `empty` (default) or `underutilized`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<ScaleInWhen>,
    /// How long a node must stay empty (or underutilized) before it is a
    /// victim. Default `"10m"`.
    #[serde(default, rename = "for", skip_serializing_if = "Option::is_none")]
    pub for_: Option<DurationSpec>,
    /// The most victims per `every`: the larger of `percent` of the group
    /// and `replicas`. Default `{ percent = 10, replicas = 1 }`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<StepSpec>,
    /// The step limit's rolling window. Default `"5m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<DurationSpec>,
    /// How long a drain may take. Past it, the node is uncordoned and the
    /// scale-in is reported as aborted. Default `"10m"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_deadline: Option<DurationSpec>,
}

/// `down.when`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ScaleInWhen {
    /// A node with no allocations for `for` (session envelopes with no
    /// sessions and per-node `count.per = "id"` allocations do not count).
    #[default]
    Empty,
    /// Also a node whose reserved share is under 50% for `for`, when every
    /// allocation on it fits on the group's other nodes.
    Underutilized,
}

impl fmt::Display for ScaleInWhen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ScaleInWhen::Empty => "empty",
            ScaleInWhen::Underutilized => "underutilized",
        })
    }
}

/// How groups that could host the same work are ranked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Expander {
    /// The group whose new nodes would sit least idle: the lowest mean of
    /// unreserved CPU and memory share across the nodes the pending work
    /// needs (Cluster Autoscaler's `least-waste`).
    #[default]
    LeastWaste,
}

impl fmt::Display for Expander {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Expander::LeastWaste => "least-waste",
        })
    }
}

// ── The effective policy ─────────────────────────────────────────────────

/// Scale-up behavior with every default filled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeUp {
    /// The step limit per `every`.
    pub step: Step,
    /// The step limit's window.
    #[serde(with = "super::spec::duration_text")]
    pub every: Duration,
    /// How long new nodes have to join.
    #[serde(with = "super::spec::duration_text")]
    pub join_timeout: Duration,
}

/// Scale-in behavior with every default filled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDown {
    /// Which nodes may go.
    pub when: ScaleInWhen,
    /// How long a node must qualify before it is a victim.
    #[serde(rename = "for", with = "super::spec::duration_text")]
    pub for_: Duration,
    /// The most victims per `every`.
    pub step: Step,
    /// The step limit's window.
    #[serde(with = "super::spec::duration_text")]
    pub every: Duration,
    /// How long a drain may take.
    #[serde(with = "super::spec::duration_text")]
    pub drain_deadline: Duration,
}

/// The node-group policy the engine runs: every default filled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveNodeGroup {
    /// The group's name.
    pub name: String,
    /// The pool, when the policy names one (else the worker type's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// The worker type members join as.
    pub worker_type: String,
    /// The member shape.
    pub shape: NodeShape,
    /// The target plugin.
    pub plugin: String,
    /// The plugin's settings.
    pub config: Value,
    /// The floor.
    pub min: u32,
    /// The ceiling.
    pub max: u32,
    /// Scale-up behavior.
    pub up: NodeUp,
    /// Scale-in behavior.
    pub down: NodeDown,
    /// The expander.
    pub expander: Expander,
    /// `enforce` or `dry-run`.
    pub mode: Mode,
}

impl EffectiveNodeGroup {
    /// The attributes every member carries: the shape's, plus
    /// `legion.nodegroup = <name>` (the join token stamps it).
    pub fn member_attributes(&self) -> BTreeMap<String, String> {
        let mut attributes = self.shape.attributes.clone().unwrap_or_default();
        attributes.insert(NODEGROUP_ATTR.to_string(), self.name.clone());
        attributes
    }

    /// The effective record as canonical JSON (field order fixed, maps
    /// sorted, durations as text).
    pub fn canonical_json(&self) -> String {
        serde_json::to_string(self).expect("an effective node group should serialize")
    }

    /// The hex SHA-256 of [`Self::canonical_json`]: the digest each history
    /// row of the group carries.
    pub fn digest(&self) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(self.canonical_json().as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// The group in one line: `nodes 0..20 via aws-asg, up 100% or 5 nodes
    /// per 2m, down when empty for 10m, enforce`.
    pub fn summary(&self) -> String {
        format!(
            "nodes {}..{} via {}, up {} per {}, down when {} for {}, {}",
            self.min,
            self.max,
            self.plugin,
            step_text(self.up.step),
            format_duration(self.up.every),
            self.down.when,
            format_duration(self.down.for_),
            self.mode
        )
    }
}

/// `field: old -> new` for every field of the effective record that
/// differs.
pub fn changed_fields(before: &EffectiveNodeGroup, after: &EffectiveNodeGroup) -> Vec<String> {
    fn flatten(prefix: &str, value: &Value, out: &mut BTreeMap<String, String>) {
        match value {
            Value::Object(map) => {
                for (key, value) in map {
                    let path = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    flatten(&path, value, out);
                }
            }
            other => {
                out.insert(prefix.to_string(), other.to_string());
            }
        }
    }
    let (mut old, mut new) = (BTreeMap::new(), BTreeMap::new());
    flatten(
        "",
        &serde_json::to_value(before).unwrap_or(Value::Null),
        &mut old,
    );
    flatten(
        "",
        &serde_json::to_value(after).unwrap_or(Value::Null),
        &mut new,
    );
    let fields: std::collections::BTreeSet<&String> = old.keys().chain(new.keys()).collect();
    fields
        .into_iter()
        .filter(|field| old.get(*field) != new.get(*field))
        .map(|field| {
            format!(
                "{field}: {} -> {}",
                old.get(field).map(String::as_str).unwrap_or("-"),
                new.get(field).map(String::as_str).unwrap_or("-")
            )
        })
        .collect()
}

fn step_of(spec: &Option<StepSpec>, default: Step) -> Step {
    match spec {
        None => default,
        // Writing `step` replaces both halves (calls entry 19).
        Some(step) => Step {
            percent: step.percent.unwrap_or(0.0),
            replicas: step.replicas.unwrap_or(0),
        },
    }
}

fn validate_step(field: &str, step: &Option<StepSpec>) -> Result<(), String> {
    let Some(step) = step else {
        return Ok(());
    };
    if step.percent.is_none() && step.replicas.is_none() {
        return Err(format!(
            "{field} names neither percent nor replicas. Hint: write step = {{ replicas = 1 }}, \
             or leave step out to keep the default"
        ));
    }
    if let Some(percent) = step.percent {
        if !percent.is_finite() || percent < 0.0 {
            return Err(format!("{field}.percent = {percent} must be 0 or more"));
        }
    }
    Ok(())
}

fn validate_duration(field: &str, value: &Option<DurationSpec>) -> Result<(), String> {
    match value {
        Some(duration) if duration.0.is_zero() => Err(format!("{field} must be longer than 0s")),
        _ => Ok(()),
    }
}

/// Whether `name` is a valid node-group name: 1 to 63 lowercase letters,
/// digits and `-`, starting and ending with a letter or digit.
pub fn valid_group_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_NAME_LEN
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes[0] != b'-'
        && bytes[bytes.len() - 1] != b'-'
}

impl NodeShape {
    /// Static validation of a shape (`field` names it in errors).
    pub fn validate(&self, field: &str) -> Result<(), String> {
        for (name, value) in [("cpu", self.cpu), ("memory", self.memory)] {
            if value == Some(0) {
                return Err(format!(
                    "{field}.{name} must be more than 0 (leave it out when unknown)"
                ));
            }
        }
        if self.cores == Some(0) {
            return Err(format!(
                "{field}.cores must be more than 0 (leave it out when unknown)"
            ));
        }
        if let Some(attributes) = &self.attributes {
            for key in attributes.keys() {
                if key.trim().is_empty() {
                    return Err(format!("{field}.attributes has an empty key"));
                }
                if key == NODEGROUP_ATTR {
                    return Err(format!(
                        "{field}.attributes sets {NODEGROUP_ATTR}, which every member gets from \
                         the group's name. Hint: leave it out"
                    ));
                }
                if is_structural_node_attr(key) {
                    return Err(format!(
                        "{field}.attributes sets {key:?}, which Legion reads as node \
                         structure, not as a label. Hint: set it on the worker type"
                    ));
                }
            }
        }
        if let Some(resources) = &self.resources {
            for (name, value) in resources {
                let ok = match value {
                    Value::Number(number) => number.as_f64().is_some_and(|n| n >= 0.0),
                    Value::String(_) => true,
                    Value::Array(items) => items.iter().all(Value::is_string),
                    _ => false,
                };
                if !ok {
                    return Err(format!(
                        "{field}.resources.{name} must be a number of 0 or more, or a list of \
                         item names"
                    ));
                }
            }
        }
        Ok(())
    }
}

impl NodeGroupPolicy {
    /// Static validation: every rule that needs only the policy. Errors
    /// name the field and say what to write instead.
    pub fn validate(&self) -> Result<(), String> {
        if !valid_group_name(&self.name) {
            return Err(format!(
                "node group name {:?} must be 1 to {MAX_NAME_LEN} lowercase letters, digits \
                 and '-', starting and ending with a letter or digit",
                self.name
            ));
        }
        let nodes = &self.target.nodes;
        if nodes.plugin.trim().is_empty() {
            return Err(format!(
                "{}: target.nodes.plugin is empty. Hint: name the target plugin that starts \
                 the machines, such as \"aws-asg\" or \"exec\"",
                self.name
            ));
        }
        for (field, value) in [("pool", &nodes.pool), ("workerType", &nodes.worker_type)] {
            if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
                return Err(format!(
                    "{}: target.nodes.{field} is empty; leave it out for the default",
                    self.name
                ));
            }
        }
        if let Some(shape) = &nodes.shape {
            shape
                .validate("target.nodes.shape")
                .map_err(|err| format!("{}: {err}", self.name))?;
        }
        if self.max == 0 {
            return Err(format!(
                "{}: max must be at least 1. Hint: a group that should never run a node \
                 does not need a policy",
                self.name
            ));
        }
        let min = self.min.unwrap_or(0);
        if min > self.max {
            return Err(format!(
                "{}: min = {min} is above max = {}",
                self.name, self.max
            ));
        }
        if let Some(up) = &self.up {
            validate_step("up.step", &up.step).map_err(|err| format!("{}: {err}", self.name))?;
            validate_duration("up.every", &up.every)
                .and_then(|_| validate_duration("up.joinTimeout", &up.join_timeout))
                .map_err(|err| format!("{}: {err}", self.name))?;
        }
        if let Some(down) = &self.down {
            validate_step("down.step", &down.step)
                .map_err(|err| format!("{}: {err}", self.name))?;
            validate_duration("down.for", &down.for_)
                .and_then(|_| validate_duration("down.every", &down.every))
                .and_then(|_| validate_duration("down.drainDeadline", &down.drain_deadline))
                .map_err(|err| format!("{}: {err}", self.name))?;
        }
        Ok(())
    }

    /// Validate and fill every default.
    pub fn resolve(&self) -> Result<EffectiveNodeGroup, String> {
        self.validate()?;
        let nodes = &self.target.nodes;
        let up = self.up.clone().unwrap_or_default();
        let down = self.down.clone().unwrap_or_default();
        Ok(EffectiveNodeGroup {
            name: self.name.clone(),
            pool: nodes.pool.clone(),
            worker_type: nodes
                .worker_type
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            shape: nodes.shape.clone().unwrap_or_default(),
            plugin: nodes.plugin.clone(),
            config: nodes.config.clone().unwrap_or(Value::Null),
            min: self.min.unwrap_or(0),
            max: self.max,
            up: NodeUp {
                step: step_of(&up.step, DEFAULT_UP_STEP),
                every: up.every.map(|d| d.0).unwrap_or(DEFAULT_UP_EVERY),
                join_timeout: up.join_timeout.map(|d| d.0).unwrap_or(DEFAULT_JOIN_TIMEOUT),
            },
            down: NodeDown {
                when: down.when.unwrap_or_default(),
                for_: down.for_.map(|d| d.0).unwrap_or(DEFAULT_DOWN_FOR),
                step: step_of(&down.step, DEFAULT_DOWN_STEP),
                every: down.every.map(|d| d.0).unwrap_or(DEFAULT_DOWN_EVERY),
                drain_deadline: down
                    .drain_deadline
                    .map(|d| d.0)
                    .unwrap_or(DEFAULT_DRAIN_DEADLINE),
            },
            expander: self.expander.unwrap_or_default(),
            mode: self.mode.unwrap_or_default(),
        })
    }

    /// The JSON schema of a node-group policy, as written in an
    /// `autoscaler/NodeGroup` object's spec (with `name` taken from the
    /// object; [`crate::autoscaler::policy::node_group_schema`]).
    pub fn json_schema_value() -> Value {
        serde_json::to_value(schemars::schema_for!(NodeGroupPolicy))
            .expect("node group schema should serialize")
    }
}

// ── Demand and the expander ──────────────────────────────────────────────

/// What `GetCapacityDemand` said about one group's template, already net
/// of the nodes on their way (`upcoming`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDemand {
    /// Nodes of this template the pending work needs (`nodegroup.pending`).
    pub nodes: u32,
    /// Pending replicas this template can host.
    pub replicas: u32,
    /// CPU those replicas reserve, MHz.
    pub cpu_requested_mhz: f64,
    /// Memory those replicas reserve, MiB.
    pub memory_requested_mib: f64,
    /// CPU the `nodes` new nodes offer, MHz.
    pub cpu_capacity_mhz: f64,
    /// Memory the `nodes` new nodes offer, MiB.
    pub memory_capacity_mib: f64,
}

impl GroupDemand {
    /// The share of the new nodes' capacity the pending work would leave
    /// idle: the mean of the unreserved CPU and memory shares, over the
    /// dimensions the template knows. `None` when the template knows
    /// neither (an unbounded shape wastes nothing it can measure).
    pub fn waste(&self) -> Option<f64> {
        let share = |requested: f64, capacity: f64| {
            (capacity.is_finite() && capacity > 0.0)
                .then(|| ((capacity - requested) / capacity).clamp(0.0, 1.0))
        };
        let parts: Vec<f64> = [
            share(self.cpu_requested_mhz, self.cpu_capacity_mhz),
            share(self.memory_requested_mib, self.memory_capacity_mib),
        ]
        .into_iter()
        .flatten()
        .collect();
        (!parts.is_empty()).then(|| parts.iter().sum::<f64>() / parts.len() as f64)
    }
}

/// One group the expander may choose.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpanderOption {
    /// The group.
    pub group: String,
    /// Its demand.
    pub demand: GroupDemand,
    /// Why it may not grow this tick (at `max`, backing off after a join
    /// timeout, paused), if it may not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<String>,
}

/// One option as the expander ranked it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RankedOption {
    /// The group.
    pub group: String,
    /// Its waste, when measurable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waste: Option<f64>,
    /// Why it was passed over, or chosen.
    pub note: String,
}

/// The expander's choice for one tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpanderChoice {
    /// The group that grows this tick, if any can.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chosen: Option<String>,
    /// Every option, best first.
    pub ranking: Vec<RankedOption>,
    /// One line: `least-waste chose gpu-a100 (waste 0.12 vs 0.40 for general)`.
    pub summary: String,
}

/// Choose the one group that grows this tick.
///
/// One group per tick, as Cluster Autoscaler does: `GetCapacityDemand`
/// subtracts the nodes already on their way (`upcoming`), so the next tick
/// sees only the work the chosen group's nodes will not host, and the next
/// best group picks that up. Groups that cannot grow (`blocked`) and
/// groups with no demand are passed over. `least-waste` ranks by
/// [`GroupDemand::waste`] (unmeasurable last), then more replicas hosted,
/// then fewer nodes, then name.
pub fn expand(expander: Expander, options: &[ExpanderOption]) -> ExpanderChoice {
    let mut eligible: Vec<&ExpanderOption> = options
        .iter()
        .filter(|option| option.blocked.is_none() && option.demand.nodes > 0)
        .collect();
    match expander {
        Expander::LeastWaste => eligible.sort_by(|a, b| {
            let waste = |o: &ExpanderOption| o.demand.waste().unwrap_or(f64::INFINITY);
            waste(a)
                .total_cmp(&waste(b))
                .then(b.demand.replicas.cmp(&a.demand.replicas))
                .then(a.demand.nodes.cmp(&b.demand.nodes))
                .then(a.group.cmp(&b.group))
        }),
    }
    let fmt_waste = |waste: Option<f64>| {
        waste
            .map(|w| format!("waste {w:.2}"))
            .unwrap_or_else(|| "waste unknown".to_string())
    };
    let chosen = eligible.first().map(|option| option.group.clone());
    let mut ranking: Vec<RankedOption> = eligible
        .iter()
        .enumerate()
        .map(|(index, option)| RankedOption {
            group: option.group.clone(),
            waste: option.demand.waste(),
            note: if index == 0 {
                format!(
                    "chosen: {} for {} ({})",
                    plural(option.demand.nodes, "node", "nodes"),
                    plural(option.demand.replicas, "replica", "replicas"),
                    fmt_waste(option.demand.waste())
                )
            } else {
                format!(
                    "passed over: {} ({})",
                    plural(option.demand.nodes, "node", "nodes"),
                    fmt_waste(option.demand.waste())
                )
            },
        })
        .collect();
    for option in options {
        if ranking.iter().any(|ranked| ranked.group == option.group) {
            continue;
        }
        ranking.push(RankedOption {
            group: option.group.clone(),
            waste: option.demand.waste(),
            note: match &option.blocked {
                Some(why) => format!("cannot grow: {why}"),
                None => "no pending work it can host".to_string(),
            },
        });
    }
    let summary = match (&chosen, eligible.get(1)) {
        (Some(group), Some(runner_up)) => format!(
            "{expander} chose {group} ({} vs {} for {})",
            fmt_waste(eligible[0].demand.waste()),
            fmt_waste(runner_up.demand.waste()),
            runner_up.group
        ),
        (Some(group), None) => {
            format!("{expander} chose {group}, the only group that can host the pending work")
        }
        (None, _) if options.iter().any(|o| o.demand.nodes > 0) => {
            "no group that could host the pending work can grow".to_string()
        }
        (None, _) => "no pending work".to_string(),
    };
    ExpanderChoice {
        chosen,
        ranking,
        summary,
    }
}

// ── Observations ─────────────────────────────────────────────────────────

/// What the provider reports for the group (`target/status`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderStatus {
    /// The count the provider is holding.
    pub desired: u32,
    /// Instances running.
    pub running: u32,
    /// The provider ids of the running instances, when it lists them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub instances: Vec<String>,
}

/// What kind of allocation, as scale-in sees it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum AllocationKind {
    /// Ordinary work.
    #[default]
    Work,
    /// A session envelope: one per node, never "empty" on its own. With no
    /// live sessions it does not keep a node from being empty.
    SessionEnvelope {
        /// Live sessions in it.
        sessions: u32,
    },
    /// A `count.per = "id"` allocation: one per node by design, so it does
    /// not keep a node from being empty, and it does not move.
    PerNode,
}

/// One allocation on a member, with the facts scale-in needs about it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocationView {
    /// The allocation id.
    pub id: i64,
    /// Its job's namespace.
    pub namespace: String,
    /// Its job.
    pub job: String,
    /// Its task.
    pub task: String,
    /// Work, envelope, or per-node.
    #[serde(flatten)]
    pub kind: AllocationKind,
    /// Holds stateful node-local storage: its data lives on this machine.
    #[serde(default)]
    pub node_local_storage: bool,
    /// Draining it now would take its task below `disruption.minAvailable`
    /// (the caller computes this from the whole job).
    #[serde(default)]
    pub breaks_min_available: bool,
}

impl AllocationView {
    /// Whether this allocation keeps its node from being empty.
    pub fn occupies(&self) -> bool {
        match self.kind {
            AllocationKind::Work => true,
            AllocationKind::SessionEnvelope { sessions } => sessions > 0,
            AllocationKind::PerNode => false,
        }
    }
}

/// One member of the group.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeView {
    /// The node id.
    pub id: String,
    /// `legion.provider-id`, what `target/terminate` takes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    /// Heartbeating (status `active`). Scale-in only picks live nodes; a
    /// lost one is the provider's to replace.
    pub ready: bool,
    /// Cordoned (`availability = drained`).
    #[serde(default)]
    pub cordoned: bool,
    /// The node's `legion.scale-in` attribute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_in: Option<String>,
    /// Its allocations.
    #[serde(default)]
    pub allocations: Vec<AllocationView>,
    /// Reserved CPU over allocatable CPU, 0 to 1.
    #[serde(default)]
    pub reserved_cpu: f64,
    /// Reserved memory over allocatable memory, 0 to 1.
    #[serde(default)]
    pub reserved_memory: f64,
    /// Why its `legion.provider-id` cannot be trusted to name its machine
    /// (none, shared with another member, or not an instance the provider
    /// lists), when it cannot: such a node is never a victim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_problem: Option<String>,
}

impl NodeView {
    /// No allocation keeps it occupied.
    pub fn is_empty(&self) -> bool {
        !self.allocations.iter().any(AllocationView::occupies)
    }

    /// The larger of its reserved CPU and memory shares.
    pub fn reserved_share(&self) -> f64 {
        self.reserved_cpu.max(self.reserved_memory)
    }

    /// The rule that makes this node never a victim, if one does.
    pub fn protection(&self) -> Option<Protection> {
        if self
            .scale_in
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("never"))
        {
            return Some(Protection::Tagged);
        }
        if let Some(allocation) = self
            .allocations
            .iter()
            .find(|a| a.namespace == SYSTEM_NAMESPACE && a.kind != AllocationKind::PerNode)
        {
            return Some(Protection::SystemAllocation {
                job: allocation.job.clone(),
            });
        }
        if let Some(allocation) = self.allocations.iter().find(|a| a.node_local_storage) {
            return Some(Protection::NodeLocalStorage {
                job: allocation.job.clone(),
                task: allocation.task.clone(),
            });
        }
        if let Some(allocation) = self.allocations.iter().find(|a| a.breaks_min_available) {
            return Some(Protection::MinAvailable {
                job: allocation.job.clone(),
                task: allocation.task.clone(),
            });
        }
        if let Some(why) = &self.provider_problem {
            return Some(Protection::ProviderId { why: why.clone() });
        }
        None
    }
}

/// Why a node is never a scale-in victim (plan §6.11, "Never victims").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "rule")]
pub enum Protection {
    /// It runs a `legion-system` allocation (the autoscaler itself, when
    /// placed on workers). Per-node `count.per = "id"` system work does not
    /// count: every node has it.
    SystemAllocation {
        /// The system job.
        job: String,
    },
    /// It holds stateful node-local storage.
    NodeLocalStorage {
        /// The job.
        job: String,
        /// The task.
        task: String,
    },
    /// It carries `legion.scale-in = "never"`.
    Tagged,
    /// Draining it would break a `disruption.minAvailable`.
    MinAvailable {
        /// The job.
        job: String,
        /// The task.
        task: String,
    },
    /// Its `legion.provider-id` cannot be trusted to name its machine, so
    /// terminating it could stop the wrong one, or none (the provider-id
    /// cross-check against `target/status`).
    ProviderId {
        /// Why.
        why: String,
    },
}

impl fmt::Display for Protection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Protection::SystemAllocation { job } => {
                write!(f, "runs legion-system job {job}")
            }
            Protection::NodeLocalStorage { job, task } => {
                write!(f, "holds node-local storage of {job}/{task}")
            }
            Protection::Tagged => write!(f, "tagged {SCALE_IN_ATTR} = \"never\""),
            Protection::MinAvailable { job, task } => write!(
                f,
                "draining it would take {job}/{task} below disruption.minAvailable"
            ),
            Protection::ProviderId { why } => f.write_str(why),
        }
    }
}

/// What a drain in flight sees of its node this tick.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainObservation {
    /// The node record still exists.
    pub exists: bool,
    /// It is cordoned.
    pub cordoned: bool,
    /// Allocations that still occupy it ([`AllocationView::occupies`]).
    pub occupied_by: u32,
    /// The provider no longer lists its instance (or says it terminated).
    pub terminated: bool,
}

/// Everything one evaluation of a group sees.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGroupObservation {
    /// Members that have joined (`legion.nodegroup = <name>`), live or not,
    /// including ones being drained.
    pub joined: u32,
    /// The provider's view, when `target/status` answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderStatus>,
    /// This group's demand, net of upcoming nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub demand: Option<GroupDemand>,
    /// Whether the expander chose this group this tick.
    #[serde(default)]
    pub chosen: bool,
    /// The expander's one-line summary, for the decision's note.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub expander_note: String,
    /// The members, for scale-in.
    #[serde(default)]
    pub nodes: Vec<NodeView>,
    /// What each drain in flight sees, by node id.
    #[serde(default)]
    pub drains: BTreeMap<String, DrainObservation>,
    /// A pause on this group (or on every policy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<Pause>,
    /// The group is being deleted (its `autoscaler/NodeGroup` object is in
    /// its cleanup window): nothing is added, and every member is drained,
    /// terminated, and forgotten through the scale-in path, whatever
    /// `down` says (`when`, `for`, the step, `min`). Protected members
    /// stay. Once no member is left and the provider holds none, the
    /// decision is [`NodeDecision::retired`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retiring: bool,
}

// ── The decision ─────────────────────────────────────────────────────────

/// The stages of a node-group decision, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStage {
    /// Where the count starts.
    Provider,
    /// Scale-ups whose nodes did not join.
    JoinTimeout,
    /// Pending work.
    Demand,
    /// The scale-up step limit.
    Step,
    /// `min`/`max`.
    Bounds,
    /// Victim selection and the scale-in budget.
    ScaleIn,
    /// A pause.
    Pause,
    /// `enforce` or `dry-run`.
    Mode,
}

impl NodeStage {
    /// Every stage in order.
    pub const ORDER: [NodeStage; 8] = [
        NodeStage::Provider,
        NodeStage::JoinTimeout,
        NodeStage::Demand,
        NodeStage::Step,
        NodeStage::Bounds,
        NodeStage::ScaleIn,
        NodeStage::Pause,
        NodeStage::Mode,
    ];
}

impl fmt::Display for NodeStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            NodeStage::Provider => "provider",
            NodeStage::JoinTimeout => "join-timeout",
            NodeStage::Demand => "demand",
            NodeStage::Step => "step",
            NodeStage::Bounds => "bounds",
            NodeStage::ScaleIn => "scale-in",
            NodeStage::Pause => "pause",
            NodeStage::Mode => "mode",
        })
    }
}

/// What one stage did to the group's desired count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStageNote {
    /// The stage.
    pub stage: NodeStage,
    /// The count it received.
    pub before: u32,
    /// The count it passed on.
    pub after: u32,
    /// Why.
    pub note: String,
}

impl NodeStageNote {
    /// Whether the stage changed the count.
    pub fn changed(&self) -> bool {
        self.before != self.after
    }
}

/// A node chosen for scale-in this tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Victim {
    /// The node.
    pub node: String,
    /// Why (`empty for 12m`).
    pub reason: String,
}

/// A member scale-in would have taken, but may not.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtectedNode {
    /// The node.
    pub node: String,
    /// The rule.
    pub protection: Protection,
}

/// Something the caller does for the group. Each one is idempotent, so a
/// repeated intent (a retried terminate, a cordon that did not land) is
/// safe.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "intent")]
pub enum NodeIntent {
    /// `target/scale(count)`, with a fresh join token for the group. Call
    /// [`NodeGroupState::record_scale`] once it succeeds.
    SetDesired {
        /// The count to hold.
        count: u32,
    },
    /// `SetNodeAvailability(drained, evict = true)`: cordon and drain.
    Cordon {
        /// The node.
        node: String,
    },
    /// `SetNodeAvailability(available)`: give up on a drain.
    Uncordon {
        /// The node.
        node: String,
    },
    /// `target/terminate([provider id])` (the provider lowers its count).
    Terminate {
        /// The node.
        node: String,
        /// Its `legion.provider-id`, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_id: Option<String>,
    },
    /// `ForgetNode`: drop the record and revoke its token.
    Forget {
        /// The node.
        node: String,
    },
}

/// An event worth publishing (plan §6.6). Each is emitted once.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "event")]
pub enum NodeFlag {
    /// `nodegroup.join_timeout`: nodes asked for did not join in time.
    JoinTimeout {
        /// Nodes still missing.
        missing: u32,
        /// When the scale-up was asked for (unix ms).
        asked_at_ms: i64,
        /// When the group stops backing off (unix ms).
        backoff_until_ms: i64,
    },
    /// `nodegroup.scale_in`: a drain finished, one way or the other.
    ScaleIn {
        /// The node.
        node: String,
        /// `completed` or `aborted`.
        outcome: String,
        /// Why.
        detail: String,
    },
    /// `scale.limited`: `max` started holding the group back.
    Limited {
        /// What held it.
        reason: String,
    },
}

/// The outcome of one evaluation of a node group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDecision {
    /// When (unix ms).
    pub at_ms: i64,
    /// The count before.
    pub from: u32,
    /// The count to hold (equal to `from` unless the group grows; scale-in
    /// lowers the count through terminations, one node at a time).
    pub to: u32,
    /// Applied, held, or would.
    pub kind: DecisionKind,
    /// Every stage, in order.
    pub stages: Vec<NodeStageNote>,
    /// Nodes chosen for scale-in this tick.
    pub victims: Vec<Victim>,
    /// Members passed over by a never-victim rule.
    pub protected: Vec<ProtectedNode>,
    /// What to do, in order.
    pub intents: Vec<NodeIntent>,
    /// Events to emit.
    pub flags: Vec<NodeFlag>,
    /// The one-line why.
    pub summary: String,
    /// A retiring group (see [`NodeGroupObservation::retiring`]) has nothing
    /// left: no member, no drain in flight, and the provider holds no
    /// instance. Its object can be released.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retired: bool,
}

impl NodeDecision {
    /// The note of one stage.
    pub fn stage(&self, stage: NodeStage) -> Option<&NodeStageNote> {
        self.stages.iter().find(|note| note.stage == stage)
    }
}

// ── State ────────────────────────────────────────────────────────────────

/// One change of the group's desired count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountChange {
    /// When it was written (unix ms).
    pub at_ms: i64,
    /// The count before.
    pub from: u32,
    /// The count after.
    pub to: u32,
}

/// A scale-up whose nodes have not all joined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingScaleUp {
    /// When it was asked for (unix ms).
    pub asked_at_ms: i64,
    /// The members that should exist once it lands.
    pub expect: u32,
}

/// Where one drain is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "phase")]
pub enum DrainPhase {
    /// Cordoned; waiting for its allocations to leave.
    Draining,
    /// Empty; the provider was asked to stop the instance.
    Terminating {
        /// When terminate was last asked for (unix ms).
        asked_at_ms: i64,
    },
    /// The instance is gone; the record is being forgotten.
    Forgetting,
    /// Finished: the node is gone.
    Done,
    /// Given up: past the deadline, uncordoned, or paused.
    Aborted,
}

/// One scale-in in flight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Drain {
    /// The node.
    pub node: String,
    /// Its `legion.provider-id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    /// When the drain began (unix ms).
    pub started_ms: i64,
    /// Where it is.
    pub phase: DrainPhase,
}

impl Drain {
    /// Whether the drain is finished, one way or the other.
    pub fn finished(&self) -> bool {
        matches!(self.phase, DrainPhase::Done | DrainPhase::Aborted)
    }
}

/// The per-group memory the engine keeps between evaluations. It lives in
/// the app; after a restart the scale-in clocks start over (a node must be
/// seen empty for a whole `down.for` again), which errs toward keeping
/// nodes. Drains in flight are recorded as action intents
/// (`RecordScalingAction`) so a new owner can rebuild them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGroupState {
    last_desired: Option<u32>,
    changes: VecDeque<CountChange>,
    pending_up: Option<PendingScaleUp>,
    backoff_until_ms: Option<i64>,
    empty_since: BTreeMap<String, i64>,
    underutilized_since: BTreeMap<String, i64>,
    scale_ins: VecDeque<i64>,
    drains: BTreeMap<String, Drain>,
    limited: bool,
}

impl NodeGroupState {
    /// Record a desired count the provider accepted (`target/scale`
    /// succeeded). A scale-up starts the join clock: the group expects
    /// `to` members by `up.joinTimeout` from now.
    pub fn record_scale(&mut self, at_ms: i64, from: u32, to: u32) {
        self.last_desired = Some(to);
        if from == to {
            return;
        }
        self.changes.push_back(CountChange { at_ms, from, to });
        if to > from {
            self.pending_up = Some(PendingScaleUp {
                asked_at_ms: self.pending_up.map_or(at_ms, |p| p.asked_at_ms),
                expect: to,
            });
        }
    }

    /// Adopt a drain an earlier owner started (from its action intent).
    pub fn adopt_drain(&mut self, drain: Drain) {
        self.drains.insert(drain.node.clone(), drain);
    }

    /// Replace the drains in flight with `drains`: what the group's action
    /// row recorded, when another replica acted on the group (a lost
    /// compare-and-set, or a takeover). Finished ones are dropped.
    pub fn replace_drains(&mut self, drains: Vec<Drain>) {
        self.drains = drains
            .into_iter()
            .filter(|drain| !drain.finished())
            .map(|drain| (drain.node.clone(), drain))
            .collect();
    }

    /// The drains in flight (and finished ones not yet pruned).
    pub fn drains(&self) -> impl Iterator<Item = &Drain> {
        self.drains.values()
    }

    /// The scale-up still waiting for its nodes, if any.
    pub fn pending_up(&self) -> Option<PendingScaleUp> {
        self.pending_up
    }

    /// When the join-timeout backoff ends, if it is in force.
    pub fn backoff_until(&self, now_ms: i64) -> Option<i64> {
        self.backoff_until_ms.filter(|until| *until > now_ms)
    }

    /// When a node was first seen empty (unix ms).
    pub fn empty_since(&self, node: &str) -> Option<i64> {
        self.empty_since.get(node).copied()
    }

    fn prune(&mut self, policy: &EffectiveNodeGroup, now_ms: i64) {
        let keep = ms(policy.up.every.max(policy.down.every));
        while self
            .changes
            .front()
            .is_some_and(|change| change.at_ms <= now_ms - keep)
        {
            self.changes.pop_front();
        }
        while self
            .scale_ins
            .front()
            .is_some_and(|at| *at <= now_ms - ms(policy.down.every))
        {
            self.scale_ins.pop_front();
        }
        self.drains.retain(|_, drain| !drain.finished());
    }
}

/// Builds the stage list as the count moves.
struct Trace {
    value: u32,
    stages: Vec<NodeStageNote>,
}

impl Trace {
    fn record(&mut self, stage: NodeStage, after: u32, note: impl Into<String>) {
        self.stages.push(NodeStageNote {
            stage,
            before: self.value,
            after,
            note: note.into(),
        });
        self.value = after;
    }
}

/// The larger of `percent` of `base` (rounded the cautious way for the
/// direction) and `replicas`.
fn step_budget(step: Step, base: u32, up: bool) -> u32 {
    let percent = f64::from(base) * step.percent / 100.0;
    let from_percent = if up {
        (percent - 1e-9).ceil().max(0.0) as u32
    } else {
        (percent + 1e-9).floor().max(0.0) as u32
    };
    from_percent.max(step.replicas)
}

/// Advance one drain by what its node looks like now. Returns the
/// intents to carry out and the event, if the drain finished.
///
/// ```text
///  Draining ──empty──▶ Terminating ──instance gone──▶ Forgetting ──record gone──▶ Done
///     │  └─past drainDeadline, or uncordoned by someone else──▶ Aborted (uncordon)
///     └─record gone──▶ Done
/// ```
///
/// A draining node found uncordoned within [`CORDON_GRACE`] of the start
/// is cordoned again (a cordon that did not land); after that an uncordon
/// is someone's decision, and the drain gives up.
pub fn step_drain(
    drain: &Drain,
    obs: &DrainObservation,
    deadline: Duration,
    now_ms: i64,
) -> (Drain, Vec<NodeIntent>, Option<NodeFlag>) {
    let mut next = drain.clone();
    let mut intents = Vec::new();
    let finished = |outcome: &str, detail: String| NodeFlag::ScaleIn {
        node: drain.node.clone(),
        outcome: outcome.to_string(),
        detail,
    };
    match drain.phase {
        DrainPhase::Draining => {
            if !obs.exists {
                next.phase = DrainPhase::Done;
                return (
                    next,
                    intents,
                    Some(finished("completed", "the node record is gone".to_string())),
                );
            }
            if obs.occupied_by == 0 && obs.cordoned {
                next.phase = DrainPhase::Terminating {
                    asked_at_ms: now_ms,
                };
                intents.push(NodeIntent::Terminate {
                    node: drain.node.clone(),
                    provider_id: drain.provider_id.clone(),
                });
                return (next, intents, None);
            }
            if now_ms - drain.started_ms >= ms(deadline) {
                next.phase = DrainPhase::Aborted;
                intents.push(NodeIntent::Uncordon {
                    node: drain.node.clone(),
                });
                return (
                    next,
                    intents,
                    Some(finished(
                        "aborted",
                        format!(
                            "{} still on it after drainDeadline {}; uncordoned",
                            plural(obs.occupied_by, "allocation", "allocations"),
                            format_duration(deadline)
                        ),
                    )),
                );
            }
            if !obs.cordoned {
                if now_ms - drain.started_ms < ms(CORDON_GRACE) {
                    intents.push(NodeIntent::Cordon {
                        node: drain.node.clone(),
                    });
                } else {
                    next.phase = DrainPhase::Aborted;
                    return (
                        next,
                        intents,
                        Some(finished(
                            "aborted",
                            "uncordoned by someone else while draining".to_string(),
                        )),
                    );
                }
            }
            (next, intents, None)
        }
        DrainPhase::Terminating { asked_at_ms } => {
            if !obs.exists {
                next.phase = DrainPhase::Done;
                return (
                    next,
                    intents,
                    Some(finished("completed", "terminated".to_string())),
                );
            }
            if obs.terminated {
                next.phase = DrainPhase::Forgetting;
                intents.push(NodeIntent::Forget {
                    node: drain.node.clone(),
                });
                return (next, intents, None);
            }
            if now_ms - asked_at_ms >= ms(TERMINATE_RETRY) {
                next.phase = DrainPhase::Terminating {
                    asked_at_ms: now_ms,
                };
                intents.push(NodeIntent::Terminate {
                    node: drain.node.clone(),
                    provider_id: drain.provider_id.clone(),
                });
            }
            (next, intents, None)
        }
        DrainPhase::Forgetting => {
            if !obs.exists {
                next.phase = DrainPhase::Done;
                return (
                    next,
                    intents,
                    Some(finished(
                        "completed",
                        "terminated and forgotten".to_string(),
                    )),
                );
            }
            intents.push(NodeIntent::Forget {
                node: drain.node.clone(),
            });
            (next, intents, None)
        }
        DrainPhase::Done | DrainPhase::Aborted => (next, intents, None),
    }
}

/// Run one evaluation of a node group.
///
/// Mutates `state`: the join clock and backoff, the per-node scale-in
/// clocks, the scale-in budget, and the drains (a victim's drain is
/// recorded as started when its `Cordon` intent is returned; the next tick
/// re-cordons once if it did not land). It does **not** record a new
/// desired count: call [`NodeGroupState::record_scale`] once
/// `target/scale` succeeds.
///
/// `fits_elsewhere` answers, for `down.when = "underutilized"`, whether
/// every allocation on a node fits on the group's other nodes (a
/// simulation the caller runs, such as `GetCapacityDemand` with the node's
/// allocations as the pending work). It is only asked about nodes that
/// would otherwise be victims.
pub fn decide(
    policy: &EffectiveNodeGroup,
    state: &mut NodeGroupState,
    obs: &NodeGroupObservation,
    now_ms: i64,
    fits_elsewhere: &dyn Fn(&NodeView) -> bool,
) -> NodeDecision {
    state.prune(policy, now_ms);
    let mut flags = Vec::new();
    let mut intents = Vec::new();
    let paused = obs
        .pause
        .as_ref()
        .filter(|pause| pause.until_ms.is_none_or(|until| until > now_ms));
    let enforcing = policy.mode == Mode::Enforce && paused.is_none();

    // ── provider ─────────────────────────────────────────────────────────
    let (current, provider_note) = match (&obs.provider, state.last_desired) {
        (Some(provider), Some(last)) if provider.desired != last => (
            provider.desired,
            format!(
                "provider holds {} (this group last asked for {last}): adopting the provider's \
                 count; {} running",
                provider.desired, provider.running
            ),
        ),
        (Some(provider), _) => (
            provider.desired,
            format!(
                "provider holds {}; {} running, {} joined",
                provider.desired, provider.running, obs.joined
            ),
        ),
        (None, Some(last)) => (
            last,
            format!("provider did not answer; last asked for {last}"),
        ),
        (None, None) => (
            obs.joined,
            format!(
                "provider did not answer; counting the {} joined",
                plural(obs.joined, "member", "members")
            ),
        ),
    };
    if obs.provider.is_some() {
        state.last_desired = Some(current);
    }
    let mut trace = Trace {
        value: current,
        stages: Vec::new(),
    };
    trace.record(NodeStage::Provider, current, provider_note);

    // ── join timeout ─────────────────────────────────────────────────────
    {
        let mut note = String::new();
        if let Some(pending) = state.pending_up {
            if obs.joined >= pending.expect || current < pending.expect {
                // Every node joined, or the count was lowered under the
                // scale-up (by the provider or an operator): nothing to
                // wait for.
                state.pending_up = None;
                note = format!("the scale-up to {} has joined", pending.expect);
            } else if now_ms - pending.asked_at_ms >= ms(policy.up.join_timeout) {
                let missing = pending.expect - obs.joined;
                let until = now_ms + ms(policy.up.join_timeout);
                state.pending_up = None;
                state.backoff_until_ms = Some(until);
                flags.push(NodeFlag::JoinTimeout {
                    missing,
                    asked_at_ms: pending.asked_at_ms,
                    backoff_until_ms: until,
                });
                note = format!(
                    "{} of the scale-up to {} did not join within {}; backing off until {}",
                    plural(missing, "node", "nodes"),
                    pending.expect,
                    format_duration(policy.up.join_timeout),
                    clock(until)
                );
            } else {
                note = format!(
                    "waiting for {} to join (since {})",
                    plural(pending.expect - obs.joined, "node", "nodes"),
                    clock(pending.asked_at_ms)
                );
            }
        } else if let Some(until) = state.backoff_until(now_ms) {
            note = format!("backing off after a join timeout until {}", clock(until));
        }
        trace.record(NodeStage::JoinTimeout, current, note);
    }
    let backing_off = state.backoff_until(now_ms);

    // ── demand ───────────────────────────────────────────────────────────
    let demand_nodes = obs.demand.as_ref().map_or(0, |demand| demand.nodes);
    {
        let note_tail = if obs.expander_note.is_empty() {
            String::new()
        } else {
            format!(" ({})", obs.expander_note)
        };
        match &obs.demand {
            _ if obs.retiring => trace.record(
                NodeStage::Demand,
                current,
                "the group is being deleted: nothing is added",
            ),
            Some(demand) if demand.nodes > 0 && obs.chosen && backing_off.is_none() => {
                let wanted = current.saturating_add(demand.nodes);
                trace.record(
                    NodeStage::Demand,
                    wanted,
                    format!(
                        "pending work needs {} ({}){note_tail}",
                        plural(demand.nodes, "node", "nodes"),
                        plural(demand.replicas, "replica", "replicas")
                    ),
                );
            }
            Some(demand) if demand.nodes > 0 && obs.chosen => trace.record(
                NodeStage::Demand,
                current,
                format!(
                    "pending work needs {}, held while backing off",
                    plural(demand.nodes, "node", "nodes")
                ),
            ),
            Some(demand) if demand.nodes > 0 => trace.record(
                NodeStage::Demand,
                current,
                format!(
                    "pending work this group could host needs {}, but another group grows \
                     this tick{note_tail}",
                    plural(demand.nodes, "node", "nodes")
                ),
            ),
            _ => trace.record(NodeStage::Demand, current, "no pending work for this group"),
        }
    }

    // ── step ─────────────────────────────────────────────────────────────
    {
        let value = trace.value;
        if value > current {
            let since = now_ms - ms(policy.up.every);
            let window: Vec<&CountChange> =
                state.changes.iter().filter(|c| c.at_ms > since).collect();
            let base = window.first().map_or(current, |change| change.from);
            let used: u32 = window
                .iter()
                .filter(|change| change.to > change.from)
                .map(|change| change.to - change.from)
                .sum();
            let budget = step_budget(policy.up.step, base, true);
            let limit = current.saturating_add(budget.saturating_sub(used));
            let limits = format!(
                "up ≤ +{budget} per {} ({}); +{used} used",
                format_duration(policy.up.every),
                step_text(policy.up.step)
            );
            if value > limit {
                trace.record(
                    NodeStage::Step,
                    limit,
                    format!("rate-limited {value} → {limit} ({limits})"),
                );
            } else {
                trace.record(NodeStage::Step, value, limits);
            }
        } else {
            trace.record(NodeStage::Step, value, "");
        }
    }

    // ── bounds ───────────────────────────────────────────────────────────
    let mut limited: Option<String> = None;
    // A retiring group with no member and no drain left holds nothing: the
    // provider's count goes to 0 (no Legion node can be stranded, since none
    // is left; an instance that never joined has no work).
    let emptied = obs.retiring
        && obs.joined == 0
        && obs.nodes.is_empty()
        && state.drains.values().all(Drain::finished);
    {
        let value = trace.value;
        if obs.retiring {
            let to = if emptied { 0 } else { current };
            trace.record(
                NodeStage::Bounds,
                to,
                if emptied && current > 0 {
                    "being deleted, and no member is left: the provider goes to 0".to_string()
                } else {
                    "being deleted: every member drains".to_string()
                },
            );
        } else if value > policy.max && current > policy.max {
            // Lowering the provider's count would let it stop any instance,
            // undrained: a group above max comes down through scale-in.
            trace.record(
                NodeStage::Bounds,
                current,
                format!(
                    "above max {}: held at {current}; scale-in brings it down",
                    policy.max
                ),
            );
        } else if value > policy.max {
            if value > current {
                limited = Some(format!(
                    "node group {} at max {} (raise max)",
                    policy.name, policy.max
                ));
            }
            trace.record(
                NodeStage::Bounds,
                policy.max,
                format!("clamped to max {}", policy.max),
            );
        } else if value < policy.min {
            trace.record(
                NodeStage::Bounds,
                policy.min,
                format!("raised to min {}", policy.min),
            );
        } else {
            trace.record(
                NodeStage::Bounds,
                value,
                format!("{}..{}", policy.min, policy.max),
            );
        }
    }

    // ── scale-in ─────────────────────────────────────────────────────────
    let mut victims = Vec::new();
    let mut protected = Vec::new();
    {
        // The per-node clocks run whether or not this tick may scale in,
        // so a hold does not reset how long a node has been empty.
        let present: std::collections::BTreeSet<&str> =
            obs.nodes.iter().map(|node| node.id.as_str()).collect();
        state
            .empty_since
            .retain(|node, _| present.contains(node.as_str()));
        state
            .underutilized_since
            .retain(|node, _| present.contains(node.as_str()));
        for node in &obs.nodes {
            if node.ready && node.is_empty() {
                state.empty_since.entry(node.id.clone()).or_insert(now_ms);
            } else {
                state.empty_since.remove(&node.id);
            }
            if node.ready && node.reserved_share() < UNDERUTILIZED_BELOW {
                state
                    .underutilized_since
                    .entry(node.id.clone())
                    .or_insert(now_ms);
            } else {
                state.underutilized_since.remove(&node.id);
            }
        }

        let value = trace.value;
        let draining = state.drains.len() as u32;
        let hold = if obs.retiring {
            None
        } else if value > current {
            Some("growing this tick".to_string())
        } else if demand_nodes > 0 {
            Some(format!(
                "pending work this group could host needs {}",
                plural(demand_nodes, "node", "nodes")
            ))
        } else {
            state
                .pending_up
                .map(|pending| format!("waiting for the scale-up to {} to join", pending.expect))
        };
        let window_used = state.scale_ins.len() as u32;
        let budget = step_budget(policy.down.step, current, false);
        let floor_room = current.saturating_sub(draining).saturating_sub(policy.min);
        // A retiring group drains every member at once: no budget, no
        // floor.
        let room = if obs.retiring {
            u32::MAX
        } else {
            budget.saturating_sub(window_used).min(floor_room)
        };
        let for_ms = ms(policy.down.for_);

        // Candidates: live, not already draining, past `for`.
        let mut candidates: Vec<(&NodeView, i64, &'static str)> = Vec::new();
        for node in &obs.nodes {
            if !node.ready || state.drains.contains_key(&node.id) {
                continue;
            }
            let empty = state
                .empty_since
                .get(&node.id)
                .copied()
                .filter(|since| now_ms - since >= for_ms);
            let underused = (policy.down.when == ScaleInWhen::Underutilized)
                .then(|| state.underutilized_since.get(&node.id).copied())
                .flatten()
                .filter(|since| now_ms - since >= for_ms);
            let (since, kind) = match (empty, underused) {
                _ if obs.retiring => (now_ms, "retiring"),
                (Some(since), _) => (since, "empty"),
                (None, Some(since)) => (since, "underutilized"),
                (None, None) => continue,
            };
            if let Some(protection) = node.protection() {
                protected.push(ProtectedNode {
                    node: node.id.clone(),
                    protection,
                });
                continue;
            }
            candidates.push((node, since, kind));
        }
        // Empty first, longest-idle first, then least reserved, then id.
        candidates.sort_by(|a, b| {
            (a.2 != "empty")
                .cmp(&(b.2 != "empty"))
                .then(a.1.cmp(&b.1))
                .then(a.0.reserved_share().total_cmp(&b.0.reserved_share()))
                .then(a.0.id.cmp(&b.0.id))
        });

        let mut note = Vec::new();
        if let Some(why) = &hold {
            if !candidates.is_empty() {
                note.push(format!(
                    "held: {why} ({} would qualify)",
                    plural(candidates.len() as u32, "node", "nodes")
                ));
            }
        } else if paused.is_some() {
            if !candidates.is_empty() {
                note.push("held: paused".to_string());
            }
        } else {
            let mut skipped_fit = 0u32;
            for (node, since, kind) in &candidates {
                if victims.len() as u32 >= room {
                    break;
                }
                if *kind == "underutilized" && !fits_elsewhere(node) {
                    skipped_fit += 1;
                    continue;
                }
                let reason = if *kind == "retiring" {
                    "the group is being deleted".to_string()
                } else if *kind == "empty" {
                    format!("empty for {}", format_duration(idle(now_ms - since)))
                } else {
                    format!(
                        "{:.0}% reserved for {}, and its allocations fit elsewhere",
                        node.reserved_share() * 100.0,
                        format_duration(idle(now_ms - since))
                    )
                };
                victims.push(Victim {
                    node: node.id.clone(),
                    reason,
                });
            }
            let left = candidates.len() as u32 - victims.len() as u32 - skipped_fit;
            if !victims.is_empty() {
                note.push(format!(
                    "draining {}",
                    victims
                        .iter()
                        .map(|v| format!("{} ({})", v.node, v.reason))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if left > 0 {
                if floor_room <= victims.len() as u32 {
                    note.push(format!(
                        "{} more held at min {}",
                        plural(left, "node", "nodes"),
                        policy.min
                    ));
                } else {
                    note.push(format!(
                        "{} more held by the budget: down ≤ {budget} per {} ({}); {} used",
                        plural(left, "node", "nodes"),
                        format_duration(policy.down.every),
                        step_text(policy.down.step),
                        window_used
                    ));
                }
            }
            if skipped_fit > 0 {
                note.push(format!(
                    "{} underutilized but not movable",
                    plural(skipped_fit, "node", "nodes")
                ));
            }
        }
        if !protected.is_empty() {
            note.push(format!(
                "protected: {}",
                protected
                    .iter()
                    .map(|p| format!("{} ({})", p.node, p.protection))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if draining > 0 {
            note.push(format!("{} in flight", plural(draining, "drain", "drains")));
        }
        trace.record(NodeStage::ScaleIn, value, note.join("; "));
    }

    // ── pause ────────────────────────────────────────────────────────────
    match paused {
        Some(pause) => {
            let until = pause
                .until_ms
                .map(|until| format!(" until {}", clock(until)))
                .unwrap_or_default();
            trace.record(
                NodeStage::Pause,
                current,
                format!("paused by {}{until}", pause.by),
            );
        }
        None => {
            let value = trace.value;
            trace.record(NodeStage::Pause, value, "");
        }
    }

    // ── mode ─────────────────────────────────────────────────────────────
    let to = trace.value;
    let acts = to != current || !victims.is_empty();
    let kind = if !acts {
        DecisionKind::Held
    } else if policy.mode == Mode::DryRun {
        DecisionKind::Would
    } else {
        DecisionKind::Applied
    };
    let mode_note = match kind {
        DecisionKind::Would => {
            let mut parts = Vec::new();
            if to != current {
                parts.push(format!("scale {current} → {to}"));
            }
            if !victims.is_empty() {
                parts.push(format!(
                    "drain {}",
                    victims
                        .iter()
                        .map(|v| v.node.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            format!("dry-run: would {}", parts.join(" and "))
        }
        _ => policy.mode.to_string(),
    };
    trace.record(NodeStage::Mode, to, mode_note);

    // ── intents ──────────────────────────────────────────────────────────
    if enforcing && to != current {
        intents.push(NodeIntent::SetDesired { count: to });
    }
    // Drains in flight move first, so a drain that finishes frees budget
    // for nothing this tick (the window counts starts, not finishes).
    let nodes_by_id: BTreeMap<&str, &NodeView> = obs
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let in_flight: Vec<Drain> = state.drains.values().cloned().collect();
    for drain in in_flight {
        if paused.is_some() && drain.phase == DrainPhase::Draining {
            let mut aborted = drain.clone();
            aborted.phase = DrainPhase::Aborted;
            intents.push(NodeIntent::Uncordon {
                node: drain.node.clone(),
            });
            flags.push(NodeFlag::ScaleIn {
                node: drain.node.clone(),
                outcome: "aborted".to_string(),
                detail: "the group was paused".to_string(),
            });
            state.drains.insert(drain.node.clone(), aborted);
            continue;
        }
        let seen = obs.drains.get(&drain.node).cloned().unwrap_or_else(|| {
            // No explicit observation: derive it from the member list.
            match nodes_by_id.get(drain.node.as_str()) {
                Some(node) => DrainObservation {
                    exists: true,
                    cordoned: node.cordoned,
                    occupied_by: node.allocations.iter().filter(|a| a.occupies()).count() as u32,
                    terminated: false,
                },
                None => DrainObservation::default(),
            }
        });
        let (next, more, flag) = step_drain(&drain, &seen, policy.down.drain_deadline, now_ms);
        if enforcing || matches!(next.phase, DrainPhase::Done | DrainPhase::Aborted) {
            intents.extend(more);
        }
        flags.extend(flag);
        state.drains.insert(next.node.clone(), next);
    }
    if enforcing {
        for victim in &victims {
            let provider_id = nodes_by_id
                .get(victim.node.as_str())
                .and_then(|node| node.provider_id.clone());
            state.drains.insert(
                victim.node.clone(),
                Drain {
                    node: victim.node.clone(),
                    provider_id,
                    started_ms: now_ms,
                    phase: DrainPhase::Draining,
                },
            );
            state.scale_ins.push_back(now_ms);
            intents.push(NodeIntent::Cordon {
                node: victim.node.clone(),
            });
        }
    }

    // ── flags and summary ────────────────────────────────────────────────
    match limited {
        Some(reason) => {
            if !state.limited {
                flags.push(NodeFlag::Limited { reason });
            }
            state.limited = true;
        }
        None if kind != DecisionKind::Held => state.limited = false,
        None => {}
    }
    let summary = summarize(&trace.stages, current, to, &victims);
    let retired = emptied
        && obs
            .provider
            .as_ref()
            .is_some_and(|provider| provider.desired == 0);
    NodeDecision {
        at_ms: now_ms,
        from: current,
        to,
        kind,
        stages: trace.stages,
        victims,
        protected,
        intents,
        flags,
        summary,
        retired,
    }
}

/// How long a node has idled, to the minute (to the second under one).
fn idle(ms: i64) -> Duration {
    let secs = u64::try_from(ms / 1000).unwrap_or(0);
    Duration::from_secs(if secs < 60 { secs } else { secs / 60 * 60 })
}

/// The one-line why: the demand (or the hold), every stage that changed the
/// count, and the victims.
fn summarize(stages: &[NodeStageNote], from: u32, to: u32, victims: &[Victim]) -> String {
    let mut parts = Vec::new();
    for note in stages {
        let keep = match note.stage {
            NodeStage::Demand => note.changed(),
            NodeStage::JoinTimeout => note.note.contains("did not join"),
            NodeStage::Step | NodeStage::Bounds | NodeStage::Pause => note.changed(),
            NodeStage::Mode => note.note.starts_with("dry-run"),
            _ => false,
        };
        if keep && !note.note.is_empty() {
            parts.push(note.note.clone());
        }
    }
    if from != to {
        parts.push(format!("{from} → {to}"));
    }
    if !victims.is_empty() {
        parts.push(format!(
            "scale-in: {}",
            victims
                .iter()
                .map(|v| format!("{} {}", v.node, v.reason))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if parts.is_empty() {
        format!("steady at {from}")
    } else {
        parts.join("; ")
    }
}

#[cfg(test)]
mod tests;
