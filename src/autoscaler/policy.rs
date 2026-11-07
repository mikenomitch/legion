//! Standalone scaling policies and node groups: the autoscaler's object
//! kinds (docs/autoscaling.md, "Standalone policies" and "Node groups";
//! app-object-kinds plan §8 O2).
//!
//! The bundled autoscaler app declares two kinds in its manifest, and people
//! write them with `legion object apply`:
//!
//! ```nickel
//! # An external target, through a target plugin (namespaced; scale:write)
//! {
//!   kind = "autoscaler/ScalingPolicy",
//!   name = "render-farm",
//!   namespace = "media",
//!   spec = {
//!     target = { plugin = "render-exec", config = { pool = "gpu" } },
//!     min = 0,
//!     max = 50,
//!     checks = [{
//!       name = "queue", source = "team-prom", query = "sum(render_queue_depth)",
//!       strategy = { name = "per-replica", value = 10 }, activation = 1,
//!     }],
//!     down = { stabilize = "10m" },
//!     mode = "dry-run",
//!   },
//! }
//!
//! # A node group (cluster-scoped; node:write and operator:write)
//! { kind = "autoscaler/NodeGroup", name = "workers-general",
//!   spec = { target = { nodes = { … } }, min = 1, max = 20, … } }
//! ```
//!
//! A `ScalingPolicy`'s spec is the jobspec `autoscale` record (the same
//! fields, the same layered defaults) plus `description` and `target`, with
//! three differences: `min` and `max` are required replica counts (there is
//! no `count` for `"Nx"` to multiply), it names `checks` (the `cpu` and
//! `memory` shorthands read a task's own usage), and a check on the
//! `legion` source may only use a strategy that reads nothing. A
//! `NodeGroup`'s spec is a [`NodeGroupPolicy`] without its `name`, which is
//! the object's.
//!
//! The kinds' JSON Schemas are generated here from the same serde types the
//! parsers read ([`scaling_policy_schema`], [`node_group_schema`]), so the
//! manager's schema check and the app's parser cannot drift apart. What a
//! schema cannot say (`min <= max`, a check's strategy settings, a plugin
//! the namespace may not use) is the app's to report, as `Accepted=false`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::nodes::NodeGroupPolicy;
use super::plugins::{Kind, PluginCatalog, LEGION_SOURCE};
use super::spec::{AutoscaleRecord, CountBound};
use super::strategies::StrategyConfig;

/// The app that owns the kinds.
pub const APP: &str = "autoscaler";
/// A standalone policy's kind, as declared.
pub const SCALING_POLICY_KIND: &str = "ScalingPolicy";
/// A node group's kind, as declared.
pub const NODE_GROUP_KIND: &str = "NodeGroup";
/// `autoscaler/ScalingPolicy`.
pub const SCALING_POLICY: &str = "autoscaler/ScalingPolicy";
/// `autoscaler/NodeGroup`.
pub const NODE_GROUP: &str = "autoscaler/NodeGroup";

/// A standalone policy's kind, from its object kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyKind {
    /// An `autoscaler/ScalingPolicy`: a target plugin scales something
    /// outside Legion.
    External,
    /// An `autoscaler/NodeGroup`.
    Nodes,
}

impl PolicyKind {
    /// The kind a declared kind name (`ScalingPolicy`, or qualified) is.
    pub fn of_kind(kind: &str) -> Option<PolicyKind> {
        let bare = kind.split_once('/').map_or(kind, |(_, bare)| bare);
        match bare {
            SCALING_POLICY_KIND => Some(PolicyKind::External),
            NODE_GROUP_KIND => Some(PolicyKind::Nodes),
            _ => None,
        }
    }

    /// The qualified kind: `autoscaler/ScalingPolicy` or
    /// `autoscaler/NodeGroup`.
    pub fn qualified(self) -> &'static str {
        match self {
            PolicyKind::External => SCALING_POLICY,
            PolicyKind::Nodes => NODE_GROUP,
        }
    }

    /// The kind as declared: `ScalingPolicy` or `NodeGroup`.
    pub fn declared(self) -> &'static str {
        match self {
            PolicyKind::External => SCALING_POLICY_KIND,
            PolicyKind::Nodes => NODE_GROUP_KIND,
        }
    }

    /// `external` or `nodes`: what the target is, as the status board and
    /// the CLI name it.
    pub fn as_str(self) -> &'static str {
        match self {
            PolicyKind::External => "external",
            PolicyKind::Nodes => "nodes",
        }
    }
}

/// `target` of a scaling policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalTarget {
    /// The target plugin: `noop`, or one installed in the autoscaler's app
    /// config (`exec`, `aws-asg`, …).
    pub plugin: String,
    /// The plugin's per-policy settings, checked against its
    /// `policyConfigSchema` by the autoscaler.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
}

impl ExternalTarget {
    /// The config sent to the plugin (`{}` when unset).
    pub fn config_value(&self) -> Value {
        self.config
            .clone()
            .unwrap_or_else(|| Value::Object(Map::new()))
    }
}

/// A scaling policy as written.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalPolicy {
    /// The object's name, unique in its namespace.
    pub name: String,
    /// Its namespace.
    pub namespace: String,
    /// One line for people.
    pub description: String,
    /// What it scales.
    pub target: ExternalTarget,
    /// The scaling record: bounds, checks, behavior.
    pub record: AutoscaleRecord,
}

/// A standalone policy, parsed from its object.
#[derive(Debug, Clone, PartialEq)]
pub enum StandaloneSpec {
    /// An `autoscaler/ScalingPolicy`.
    External(ExternalPolicy),
    /// An `autoscaler/NodeGroup`.
    Nodes(NodeGroupPolicy),
}

impl StandaloneSpec {
    /// The policy's kind.
    pub fn kind(&self) -> PolicyKind {
        match self {
            StandaloneSpec::External(_) => PolicyKind::External,
            StandaloneSpec::Nodes(_) => PolicyKind::Nodes,
        }
    }

    /// Its name.
    pub fn name(&self) -> &str {
        match self {
            StandaloneSpec::External(policy) => &policy.name,
            StandaloneSpec::Nodes(policy) => &policy.name,
        }
    }

    /// Its namespace; empty for a node group, which the cluster owns.
    pub fn namespace(&self) -> &str {
        match self {
            StandaloneSpec::External(policy) => &policy.namespace,
            StandaloneSpec::Nodes(_) => "",
        }
    }

    /// The object's id: `namespace/name`, or `name` for a node group.
    pub fn id(&self) -> String {
        object_label(self.namespace(), self.name())
    }
}

/// An object's id: `namespace/name`, or `name` for a cluster-scoped one.
pub fn object_label(namespace: &str, name: &str) -> String {
    if namespace.is_empty() {
        name.to_string()
    } else {
        format!("{namespace}/{name}")
    }
}

/// Parse an `autoscaler/ScalingPolicy`'s spec, as stored (the object's
/// name and namespace are its own). Static checks only: what needs the
/// cluster (plugins, the layered defaults) is [`check_external`] and the
/// resolution.
pub fn parse_scaling_policy(
    namespace: &str,
    name: &str,
    spec: &Value,
) -> Result<ExternalPolicy, String> {
    let label = object_label(namespace, name);
    let object = spec.as_object().ok_or_else(|| {
        format!("{label}: the spec is a record with target, min, max, and checks")
    })?;
    let description = match object.get("description") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(_) => return Err(format!("{label}: description must be a string")),
    };
    let target = object
        .get("target")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            format!(
                "{label}: target is required: {{ plugin = \"…\", config = {{ … }} }} (a node group \
                 is its own kind, autoscaler/NodeGroup)"
            )
        })?;
    let target: ExternalTarget = serde_json::from_value(Value::Object(target.clone()))
        .map_err(|err| format!("{label}: target: {err}"))?;
    if target.plugin.trim().is_empty() {
        return Err(format!(
            "{label}: target.plugin must name a target plugin (`noop`, or an installed one)"
        ));
    }
    if let Some(config) = &target.config {
        if !config.is_object() {
            return Err(format!("{label}: target.config must be a record"));
        }
    }
    let rest: Map<String, Value> = object
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "description" | "target"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let record: AutoscaleRecord = serde_json::from_value(Value::Object(rest)).map_err(|err| {
        format!(
            "{label}: {err} (a policy's spec takes description, target, and the autoscale \
             record's fields)"
        )
    })?;
    record.validate().map_err(|err| format!("{label}: {err}"))?;
    for (field, bound) in [("min", record.min), ("max", record.max)] {
        match bound {
            Some(CountBound::Replicas(_)) => {}
            Some(CountBound::Multiple(_)) => {
                return Err(format!(
                    "{label}: {field} must be a replica count: \"Nx\" multiplies a task's count, \
                     and an external target has none"
                ))
            }
            None => {
                return Err(format!(
                    "{label}: {field} is required: a standalone policy sets its own bounds as \
                     replica counts"
                ))
            }
        }
    }
    if record.cpu.is_some() || record.memory.is_some() {
        return Err(format!(
            "{label}: cpu and memory read a Legion task's own usage; a policy for an external \
             target names checks on the sources it scales by"
        ));
    }
    let checks = record.checks.as_ref().ok_or_else(|| {
        format!("{label}: checks are required: they are the signals the policy scales by")
    })?;
    for (index, check) in checks.iter().enumerate() {
        let reads = !check.query.trim().is_empty();
        if check.source == LEGION_SOURCE && reads {
            return Err(format!(
                "{label}: checks[{index}] ({}): the `legion` source measures a Legion task, and \
                 this policy has none. Hint: use an installed source (prometheus, http, a plugin)",
                check.name
            ));
        }
    }
    Ok(ExternalPolicy {
        name: name.to_string(),
        namespace: namespace.to_string(),
        description,
        target,
        record,
    })
}

/// Parse an `autoscaler/NodeGroup`'s spec, as stored; its name is the
/// object's. Static checks only.
pub fn parse_node_group(name: &str, spec: &Value) -> Result<NodeGroupPolicy, String> {
    let mut object = spec
        .as_object()
        .cloned()
        .ok_or_else(|| format!("{name}: the spec is a record with target and max"))?;
    if object.contains_key("name") {
        return Err(format!(
            "{name}: the group's name is the object's; leave name out of the spec"
        ));
    }
    object.insert("name".to_string(), Value::String(name.to_string()));
    let policy: NodeGroupPolicy =
        serde_json::from_value(Value::Object(object)).map_err(|err| format!("{name}: {err}"))?;
    policy.validate().map_err(|err| format!("{name}: {err}"))?;
    Ok(policy)
}

/// Parse an object of either kind (`ScalingPolicy` or `NodeGroup`, as
/// declared or qualified).
pub fn parse_object(
    kind: &str,
    namespace: &str,
    name: &str,
    spec: &Value,
) -> Result<StandaloneSpec, String> {
    match PolicyKind::of_kind(kind) {
        Some(PolicyKind::External) => {
            parse_scaling_policy(namespace, name, spec).map(StandaloneSpec::External)
        }
        Some(PolicyKind::Nodes) => parse_node_group(name, spec).map(StandaloneSpec::Nodes),
        None => Err(format!("{kind} is not an autoscaler kind")),
    }
}

/// The plugins an external policy uses: its target, every check's source
/// (except `legion`), and every plugin strategy. `(name, kind, what)`.
pub fn plugin_uses(policy: &ExternalPolicy) -> Vec<(String, Kind, String)> {
    let mut uses = vec![(
        policy.target.plugin.clone(),
        Kind::Target,
        format!("target uses plugin '{}'", policy.target.plugin),
    )];
    for check in policy.record.checks.iter().flatten() {
        if check.source != LEGION_SOURCE {
            uses.push((
                check.source.clone(),
                Kind::Source,
                format!("check '{}' uses source '{}'", check.name, check.source),
            ));
        }
        if let Ok(StrategyConfig::Plugin(plugin)) = check.strategy.to_config() {
            uses.push((
                plugin.name.clone(),
                Kind::Strategy,
                format!("check '{}' uses strategy '{}'", check.name, plugin.name),
            ));
        }
    }
    uses
}

/// Check every plugin an external policy uses is installed and open to its
/// namespace: the plugin allowlist, a security control (a plugin runs with
/// the autoscaler's identity), enforced fail-closed wherever a policy is
/// run or resolved.
pub fn check_external(policy: &ExternalPolicy, catalog: &PluginCatalog) -> Result<(), String> {
    let label = object_label(&policy.namespace, &policy.name);
    for (plugin, kind, what) in plugin_uses(policy) {
        catalog
            .check_use(&plugin, kind, &policy.namespace, &what)
            .map_err(|err| format!("{label}: {err}"))?;
    }
    Ok(())
}

// ── The kinds' schemas ──────────────────────────────────────────────────

/// Make a schemars schema self-contained and plain: every `$ref` inlined
/// (the types are not recursive), an `Option`'s `anyOf [T, null]` (or
/// `type: [T, "null"]`) reduced to `T` (a field is left out, never `null`),
/// a lone `allOf` unwrapped, and the generator's `$schema`, `title`,
/// `definitions`, and `default: null` dropped. The result reads well in
/// `legion object explain`, which walks `properties`.
fn plain_schema(root: Value) -> Value {
    let definitions = root
        .get("definitions")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    // The referring schema's own keys (a field's description) win over the
    // referred type's.
    fn merge(base: Value, extra: &Map<String, Value>) -> Value {
        match base {
            Value::Object(mut map) => {
                for (key, value) in extra {
                    map.insert(key.clone(), value.clone());
                }
                Value::Object(map)
            }
            other if extra.is_empty() => other,
            _ => Value::Object(extra.clone()),
        }
    }
    fn walk(value: Value, definitions: &Map<String, Value>, depth: usize) -> Value {
        if depth > 32 {
            return value;
        }
        match value {
            Value::Object(mut map) => {
                map.remove("$schema");
                map.remove("title");
                map.remove("definitions");
                if map.get("default").is_some_and(Value::is_null) {
                    map.remove("default");
                }
                if let Some(reference) = map.remove("$ref") {
                    let target = reference
                        .as_str()
                        .and_then(|r| r.strip_prefix("#/definitions/"))
                        .and_then(|name| definitions.get(name))
                        .cloned()
                        .unwrap_or(Value::Bool(true));
                    let target = walk(target, definitions, depth + 1);
                    return walk(merge(target, &map), definitions, depth + 1);
                }
                if let Some(Value::Array(branches)) = map.get("anyOf").cloned() {
                    let real: Vec<Value> = branches
                        .into_iter()
                        .filter(|branch| branch.get("type").and_then(Value::as_str) != Some("null"))
                        .collect();
                    if real.len() == 1 {
                        map.remove("anyOf");
                        let inner = walk(real[0].clone(), definitions, depth + 1);
                        return walk(merge(inner, &map), definitions, depth + 1);
                    }
                    map.insert("anyOf".to_string(), Value::Array(real));
                }
                if let Some(Value::Array(branches)) = map.get("allOf").cloned() {
                    if branches.len() == 1 {
                        map.remove("allOf");
                        let inner = walk(branches[0].clone(), definitions, depth + 1);
                        return walk(merge(inner, &map), definitions, depth + 1);
                    }
                }
                if let Some(Value::Array(types)) = map.get("type").cloned() {
                    let real: Vec<Value> = types
                        .into_iter()
                        .filter(|t| t.as_str() != Some("null"))
                        .collect();
                    if real.len() == 1 {
                        map.insert("type".to_string(), real[0].clone());
                    } else {
                        map.insert("type".to_string(), Value::Array(real));
                    }
                }
                Value::Object(
                    map.into_iter()
                        .map(|(key, value)| (key, walk(value, definitions, depth + 1)))
                        .collect(),
                )
            }
            Value::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(|item| walk(item, definitions, depth + 1))
                    .collect(),
            ),
            other => other,
        }
    }
    walk(root, &definitions, 0)
}

/// A replica count: a whole number, 0 or more.
fn replica_count(description: &str) -> Value {
    serde_json::json!({ "type": "integer", "minimum": 0, "description": description })
}

/// The JSON Schema (draft 7) of an `autoscaler/ScalingPolicy`'s spec:
/// the `autoscale` record's, generated from [`AutoscaleRecord`], without
/// `cpu` and `memory`, with `min` and `max` as required replica counts,
/// plus `description` and `target`. Closed: an unknown field is an error.
pub fn scaling_policy_schema() -> Value {
    let mut schema = plain_schema(
        serde_json::to_value(schemars::schema_for!(AutoscaleRecord))
            .expect("the autoscale schema serializes"),
    );
    let properties = schema
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .expect("the autoscale record has properties");
    properties.remove("cpu");
    properties.remove("memory");
    properties.insert(
        "min".to_string(),
        replica_count("The fewest replicas the target keeps: a replica count."),
    );
    properties.insert(
        "max".to_string(),
        replica_count("The most replicas the target may have: a replica count."),
    );
    if let Some(checks) = properties.get_mut("checks") {
        checks["description"] = Value::String(
            "The signals the policy scales by, each turning one source's reading into a desired \
             count; the largest wins. A check on the `legion` source may only use a strategy \
             that reads nothing (`fixed`)."
                .to_string(),
        );
        checks["minItems"] = Value::from(1);
    }
    properties.insert(
        "description".to_string(),
        serde_json::json!({ "type": "string", "description": "One line for people." }),
    );
    properties.insert(
        "target".to_string(),
        serde_json::json!({
            "type": "object",
            "description": "What the policy scales: a target plugin installed in the autoscaler's app config (`noop` is built in), and its settings.",
            "required": ["plugin"],
            "additionalProperties": false,
            "properties": {
                "plugin": { "type": "string", "minLength": 1, "description": "The target plugin: `noop`, or one installed in the autoscaler's app config. Immutable: a policy that moves to another target is a new policy." },
                "config": { "type": "object", "description": "The plugin's per-policy settings, passed to it as data and checked against its policyConfigSchema." },
            },
        }),
    );
    schema["required"] = serde_json::json!(["target", "min", "max", "checks"]);
    schema["additionalProperties"] = Value::Bool(false);
    schema["description"] = Value::String(
        "A scaling policy for a target outside Legion: the jobspec autoscale record plus \
         description and target."
            .to_string(),
    );
    schema
}

/// The JSON Schema (draft 7) of an `autoscaler/NodeGroup`'s spec,
/// generated from [`NodeGroupPolicy`] without `name` (the object's).
pub fn node_group_schema() -> Value {
    let mut schema = plain_schema(
        serde_json::to_value(schemars::schema_for!(NodeGroupPolicy))
            .expect("the node group schema serializes"),
    );
    if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
        properties.remove("name");
    }
    schema["required"] = serde_json::json!(["target", "max"]);
    schema["additionalProperties"] = Value::Bool(false);
    schema["description"] = Value::String(
        "A node group: a pool plus a template, whose machines a target plugin starts and stops."
            .to_string(),
    );
    schema
}

/// The JSON Schema of the status `fields` the autoscaler reports on its
/// objects: durable verdicts, written only when they change (per-tick
/// numbers stay on the scaling status board).
pub fn status_fields_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "target": { "type": "string", "description": "What the autoscaler scales for it, resolved: `plugin render-exec`, or `nodes via aws-asg in pool default`." },
            "policy": { "type": "string", "description": "The effective policy in one line, with every default filled." },
            "digest": { "type": "string", "description": "The digest of the effective policy, as each history row of it carries." },
            "error": { "type": "string", "description": "Why it is not accepted or not ready, when it is not." },
            "lastAction": {
                "type": "object",
                "description": "The last change the autoscaler made to the target.",
                "additionalProperties": false,
                "properties": {
                    "from": { "type": "integer", "minimum": 0 },
                    "to": { "type": "integer", "minimum": 0 },
                    "at": { "type": "string", "description": "When (RFC 3339)." },
                    "decision": { "type": "string", "description": "The decision's id, as `legion scaling history` shows it." },
                    "outcome": { "type": "string", "description": "`succeeded` or `failed`." },
                },
            },
            "cleanup": { "type": "string", "description": "A node group being deleted: what its cleanup is waiting for." },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(extra: Value) -> Value {
        let mut spec = json!({
            "target": { "plugin": "render", "config": { "pool": "gpu" } },
            "min": 0,
            "max": 50,
            "checks": [{
                "name": "queue", "source": "team-prom", "query": "sum(depth)",
                "strategy": { "name": "per-replica", "value": 10 }, "activation": 1,
            }],
        });
        if let (Value::Object(spec), Value::Object(extra)) = (&mut spec, extra) {
            for (key, value) in extra {
                if value.is_null() {
                    spec.remove(&key);
                } else {
                    spec.insert(key, value);
                }
            }
        }
        spec
    }

    #[test]
    fn scaling_policies_parse_and_refuse_task_only_fields_test() {
        let policy = parse_scaling_policy("media", "render-farm", &spec(json!({}))).unwrap();
        assert_eq!(
            object_label(&policy.namespace, &policy.name),
            "media/render-farm"
        );
        assert_eq!(policy.target.config_value(), json!({ "pool": "gpu" }));
        assert_eq!(policy.record.max, Some(CountBound::Replicas(50)));

        let cases: Vec<(Value, &str)> = vec![
            (json!({ "target": null }), "target is required"),
            (json!({ "target": { "plugin": "" } }), "target.plugin"),
            (
                json!({ "target": { "plugin": "x", "region": "eu" } }),
                "unknown field `region`",
            ),
            (json!({ "max": null }), "max is required"),
            (json!({ "max": "4x" }), "must be a replica count"),
            (json!({ "cpu": 0.7 }), "cpu and memory read a Legion task"),
            (json!({ "checks": null }), "checks are required"),
            (json!({ "maxx": 3 }), "unknown field `maxx`"),
            (
                json!({ "checks": [{ "name": "cpu", "source": "legion", "query": "task.cpu.utilization",
                                     "strategy": { "name": "target-value", "target": 0.5 } }] }),
                "the `legion` source measures a Legion task",
            ),
            (json!({ "min": 9, "max": 3 }), "is above"),
        ];
        for (extra, expected) in cases {
            let err =
                parse_scaling_policy("media", "render-farm", &spec(extra.clone())).unwrap_err();
            assert!(err.contains(expected), "{extra}: {err}");
        }
        // A strategy that reads nothing may sit on `legion`: a floor by schedule.
        let fixed = spec(json!({ "checks": [{ "name": "floor", "source": "legion",
            "strategy": { "name": "fixed", "value": 2 } }] }));
        assert!(parse_scaling_policy("media", "render-farm", &fixed).is_ok());
    }

    #[test]
    fn node_groups_take_their_name_from_the_object_test() {
        let spec = json!({
            "target": { "nodes": { "plugin": "aws-asg", "shape": { "cpu": 8000, "memory": 16384 } } },
            "max": 20,
        });
        let parsed = parse_object(NODE_GROUP, "", "workers-general", &spec).unwrap();
        assert_eq!(parsed.kind(), PolicyKind::Nodes);
        assert_eq!(parsed.id(), "workers-general");
        let mut named = spec.clone();
        named["name"] = json!("other");
        assert!(parse_node_group("workers-general", &named)
            .unwrap_err()
            .contains("the object's"));
        let mut inverted = spec.clone();
        inverted["min"] = json!(30);
        assert!(parse_node_group("workers-general", &inverted)
            .unwrap_err()
            .contains("is above max"));
    }

    #[test]
    fn plugin_use_follows_the_catalog_test() {
        let catalog = PluginCatalog::from_app_config(&json!({ "plugins": {
            "team-prom": { "builtin": "prometheus", "config": { "address": "http://p:9090" },
                           "namespaces": ["media"] },
            "render": { "builtin": "exec", "config": { "command": "/opt/render.sh" } },
        } }))
        .unwrap();
        let policy = parse_scaling_policy("media", "render-farm", &spec(json!({}))).unwrap();
        check_external(&policy, &catalog).unwrap();
        let other = parse_scaling_policy("payments", "render-farm", &spec(json!({}))).unwrap();
        let err = check_external(&other, &catalog).unwrap_err();
        assert!(
            err.contains("check 'queue' uses source 'team-prom', which is not available to namespace payments (allowed: media)"),
            "{err}"
        );
        let missing = parse_scaling_policy(
            "media",
            "render-farm",
            &spec(json!({ "target": { "plugin": "asg" } })),
        )
        .unwrap();
        let err = check_external(&missing, &catalog).unwrap_err();
        assert!(
            err.contains("target uses plugin 'asg', which is not installed"),
            "{err}"
        );
    }

    /// The generated schemas are plain (no `$ref`, no `null` branches),
    /// closed, compile as a kind's schema, accept what the parsers accept,
    /// and refuse the shapes the parsers refuse that a schema can express.
    #[test]
    fn the_generated_schemas_match_the_parsers_test() {
        for schema in [
            scaling_policy_schema(),
            node_group_schema(),
            status_fields_schema(),
        ] {
            let text = schema.to_string();
            assert!(
                !text.contains("$ref") && !text.contains("\"null\""),
                "{text}"
            );
            assert!(
                text.len() < crate::structs::app_object::MAX_KIND_SCHEMA_BYTES,
                "{} bytes",
                text.len()
            );
        }
        let policy = crate::structs::app_object::KindSchema::compile(&scaling_policy_schema())
            .expect("compiles");
        policy
            .check("spec", &spec(json!({})))
            .expect("the example passes");
        for (extra, field) in [
            (json!({ "cpu": 0.7 }), "cpu"),
            (json!({ "max": "4x" }), "max"),
            (
                json!({ "target": { "plugin": "x", "region": "eu" } }),
                "region",
            ),
            (json!({ "checks": null }), "checks"),
            (json!({ "down": { "stabilise": "1m" } }), "stabilise"),
        ] {
            let errors = policy.check("spec", &spec(extra.clone())).unwrap_err();
            assert!(
                errors.iter().any(|error| error.contains(field)),
                "{extra}: {errors:?}"
            );
        }
        let group = crate::structs::app_object::KindSchema::compile(&node_group_schema())
            .expect("compiles");
        let nodes = json!({
            "target": { "nodes": { "plugin": "aws-asg", "shape": { "cpu": 8000, "memory": 16384,
                                   "attributes": { "arch": "amd64" } }, "config": { "asg": "x" } } },
            "min": 0, "max": 20,
            "up": { "step": { "percent": 100, "replicas": 5 }, "every": "2m", "joinTimeout": "10m" },
            "down": { "when": "empty", "for": "10m", "drainDeadline": "10m" },
            "expander": "least-waste", "mode": "enforce",
        });
        group
            .check("spec", &nodes)
            .expect("a full node group passes");
        assert!(parse_node_group("workers", &nodes).is_ok());
        for (extra, field) in [
            (json!({ "name": "x" }), "name"),
            (json!({ "max": null }), "max"),
            (json!({ "down": { "when": "never" } }), "when"),
        ] {
            let mut bad = nodes.clone();
            for (key, value) in extra.as_object().unwrap() {
                if value.is_null() {
                    bad.as_object_mut().unwrap().remove(key);
                } else {
                    bad[key] = value.clone();
                }
            }
            let errors = group.check("spec", &bad).unwrap_err();
            assert!(
                errors.iter().any(|error| error.contains(field)),
                "{extra}: {errors:?}"
            );
        }
    }

    /// The examples in `configs/scaling/` evaluate and validate: the app
    /// config against the bundled autoscaler's schema and as a catalog, the
    /// render farm as a scaling policy whose plugins that catalog opens to
    /// `media`, and the node groups as node groups whose node targets that
    /// catalog installs. Each is an object file of the right kind, whose
    /// spec passes its kind's schema.
    #[test]
    fn the_example_configs_validate_test() {
        let file = |name: &str| -> Value {
            let path = format!("{}/configs/scaling/{name}", env!("CARGO_MANIFEST_DIR"));
            let rendered = crate::config_lang::eval_file(&path).expect("the example evaluates");
            serde_json::from_str(&rendered).expect("JSON")
        };
        let config = file("autoscaler-app-config.ncl");
        let manifest = crate::apps::bundled_manifest("autoscaler").expect("bundled");
        crate::structs::app::validate_app_config(manifest, &config).expect("valid app config");
        let catalog = PluginCatalog::from_app_config(&config).expect("a catalog");

        let object = |name: &str, kind: &str| -> (String, String, Value) {
            let document = file(name);
            let input = crate::structs::app_object::parse_object_input(&document)
                .unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_eq!(input.kind, kind, "{name}");
            let schema = if kind == SCALING_POLICY {
                scaling_policy_schema()
            } else {
                node_group_schema()
            };
            crate::structs::app_object::KindSchema::compile(&schema)
                .expect("compiles")
                .check("spec", &input.spec)
                .unwrap_or_else(|errors| panic!("{name}: {errors:?}"));
            (input.namespace.unwrap_or_default(), input.name, input.spec)
        };
        let (namespace, name, spec) = object("render-farm.ncl", SCALING_POLICY);
        assert_eq!(namespace, "media");
        let render = parse_scaling_policy(&namespace, &name, &spec).expect("render-farm parses");
        check_external(&render, &catalog).expect("its plugins are open to media");

        for file_name in ["nodes-exec.ncl", "nodes-aws.ncl"] {
            let (namespace, name, spec) = object(file_name, NODE_GROUP);
            assert!(
                namespace.is_empty(),
                "{file_name}: a node group has no namespace"
            );
            let group = parse_node_group(&name, &spec)
                .expect("parses")
                .resolve()
                .expect("resolves");
            catalog
                .check_node_target(&group.plugin)
                .expect("the example installs its node target");
        }
    }
}
