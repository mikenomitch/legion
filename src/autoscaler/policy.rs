//! Standalone scaling policies: the second home of a scaling policy (plan
//! §6.2, calls entry 8), for targets that are not Legion tasks.
//!
//! `legion scaling apply` stores one as its own object on the Control lane.
//! Two shapes, told apart by `target`:
//!
//! ```nickel
//! # An external target, through a target plugin (namespaced; scale:write)
//! {
//!   name = "render-farm",
//!   namespace = "media",                       # default: the CLI's namespace
//!   target = { plugin = "render-exec", config = { pool = "gpu" } },
//!   min = 0,
//!   max = 50,
//!   checks = [{
//!     name = "queue", source = "team-prom", query = "sum(render_queue_depth)",
//!     strategy = { name = "per-replica", value = 10 }, activation = 1,
//!   }],
//!   down = { stabilize = "10m" },
//!   mode = "dry-run",
//! }
//!
//! # A node group (cluster-scoped; operator:write and node:write)
//! { name = "workers-general", target = { nodes = { … } }, min = 1, max = 20, … }
//! ```
//!
//! An external policy is the jobspec `autoscale` record (the same fields,
//! the same layered defaults) plus `name`, `namespace`, `description` and
//! `target`, with three differences: `min` and `max` are required replica
//! counts (there is no `count` for `"Nx"` to multiply), it names `checks`
//! (the `cpu` and `memory` shorthands read a task's own usage), and a check
//! on the `legion` source may only use a strategy that reads nothing. A
//! node-group policy is [`NodeGroupPolicy`]; node groups run from Phase 5b.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::nodes::NodeGroupPolicy;
use super::plugins::{valid_name, Kind, PluginCatalog, LEGION_SOURCE};
use super::spec::{AutoscaleRecord, CountBound};
use super::strategies::StrategyConfig;

/// A standalone policy's kind, from its `target`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyKind {
    /// A target plugin scales something outside Legion.
    External,
    /// A node group.
    Nodes,
}

impl PolicyKind {
    /// `external` or `nodes`.
    pub fn as_str(self) -> &'static str {
        match self {
            PolicyKind::External => "external",
            PolicyKind::Nodes => "nodes",
        }
    }
}

/// `target` of an external policy.
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

/// An external-target policy as written.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalPolicy {
    /// The policy's name, unique in its namespace.
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

/// A standalone policy, parsed.
#[derive(Debug, Clone, PartialEq)]
pub enum StandaloneSpec {
    /// An external target.
    External(ExternalPolicy),
    /// A node group.
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

    /// The canonical id: `name` in `default`, `namespace/name` elsewhere,
    /// and `name` for a node group.
    pub fn id(&self) -> String {
        policy_id(self.namespace(), self.name())
    }
}

/// The canonical id of a standalone policy.
pub fn policy_id(namespace: &str, name: &str) -> String {
    crate::structs::namespace::job_id(namespace, name)
}

const META_FIELDS: [&str; 4] = ["name", "namespace", "description", "target"];

/// Parse a standalone policy from its JSON (the CLI evaluates the Nickel
/// file to it). `default_namespace` applies when the file names none.
/// Static checks only: what needs the cluster (plugins, the layered
/// defaults) is [`check_external`].
pub fn parse(spec: &Value, default_namespace: &str) -> Result<StandaloneSpec, String> {
    let object = spec
        .as_object()
        .ok_or_else(|| "a scaling policy is a record with name, target, min, max".to_string())?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "a scaling policy needs a name".to_string())?
        .to_string();
    if !valid_name(&name) {
        return Err(format!(
            "policy name {name:?}: use lowercase letters, digits and `-`, starting with a letter, \
             at most 63 characters"
        ));
    }
    let target = object
        .get("target")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            format!(
                "{name}: target is required: {{ plugin = \"…\", config = {{ … }} }} for an \
                 external target, or {{ nodes = {{ … }} }} for a node group"
            )
        })?;
    if target.contains_key("nodes") {
        if object.contains_key("namespace") {
            return Err(format!(
                "{name}: a node group belongs to the cluster, not a namespace. Hint: remove \
                 namespace"
            ));
        }
        let policy: NodeGroupPolicy =
            serde_json::from_value(spec.clone()).map_err(|err| format!("{name}: {err}"))?;
        policy.validate().map_err(|err| format!("{name}: {err}"))?;
        return Ok(StandaloneSpec::Nodes(policy));
    }
    let namespace = match object.get("namespace") {
        None | Some(Value::Null) => default_namespace.to_string(),
        Some(Value::String(namespace)) if !namespace.trim().is_empty() => {
            namespace.trim().to_string()
        }
        Some(_) => return Err(format!("{name}: namespace must be a namespace name")),
    };
    crate::structs::namespace::validate_namespace_name(&namespace)
        .map_err(|err| format!("{name}: namespace {namespace:?}: {err}"))?;
    let description = match object.get("description") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(_) => return Err(format!("{name}: description must be a string")),
    };
    let target: ExternalTarget = serde_json::from_value(Value::Object(target.clone()))
        .map_err(|err| format!("{name}: target: {err}"))?;
    if target.plugin.trim().is_empty() {
        return Err(format!(
            "{name}: target.plugin must name a target plugin (`noop`, or an installed one)"
        ));
    }
    if let Some(config) = &target.config {
        if !config.is_object() {
            return Err(format!("{name}: target.config must be a record"));
        }
    }
    let rest: Map<String, Value> = object
        .iter()
        .filter(|(key, _)| !META_FIELDS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let record: AutoscaleRecord = serde_json::from_value(Value::Object(rest)).map_err(|err| {
        format!(
            "{name}: {err} (a policy takes name, namespace, description, target, and the \
             autoscale record's fields)"
        )
    })?;
    record.validate().map_err(|err| format!("{name}: {err}"))?;
    let label = policy_id(&namespace, &name);
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
    Ok(StandaloneSpec::External(ExternalPolicy {
        name,
        namespace,
        description,
        target,
        record,
    }))
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
/// namespace.
pub fn check_external(policy: &ExternalPolicy, catalog: &PluginCatalog) -> Result<(), String> {
    let label = policy_id(&policy.namespace, &policy.name);
    for (plugin, kind, what) in plugin_uses(policy) {
        catalog
            .check_use(&plugin, kind, &policy.namespace, &what)
            .map_err(|err| format!("{label}: {err}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn external(extra: Value) -> Value {
        let mut spec = json!({
            "name": "render-farm",
            "namespace": "media",
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
    fn external_policies_parse_and_refuse_task_only_fields_test() {
        let parsed = parse(&external(json!({})), "default").unwrap();
        assert_eq!(parsed.kind(), PolicyKind::External);
        assert_eq!(parsed.id(), "media/render-farm");
        let StandaloneSpec::External(policy) = &parsed else {
            panic!("external");
        };
        assert_eq!(policy.target.config_value(), json!({ "pool": "gpu" }));
        assert_eq!(policy.record.max, Some(CountBound::Replicas(50)));
        // The CLI's namespace applies when the file names none.
        let defaulted = parse(&external(json!({ "namespace": null })), "team-a").unwrap();
        assert_eq!(defaulted.id(), "team-a/render-farm");

        let cases: Vec<(Value, &str)> = vec![
            (json!({ "name": "Render" }), "lowercase letters"),
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
            (json!({ "namespace": "Bad Space" }), "namespace"),
            (
                json!({ "checks": [{ "name": "cpu", "source": "legion", "query": "task.cpu.utilization",
                                     "strategy": { "name": "target-value", "target": 0.5 } }] }),
                "the `legion` source measures a Legion task",
            ),
            (json!({ "min": 9, "max": 3 }), "is above"),
        ];
        for (extra, expected) in cases {
            let err = parse(&external(extra.clone()), "default").unwrap_err();
            assert!(err.contains(expected), "{extra}: {err}");
        }
        // A strategy that reads nothing may sit on `legion`: a floor by schedule.
        let fixed = external(json!({ "checks": [{ "name": "floor", "source": "legion",
            "strategy": { "name": "fixed", "value": 2 } }] }));
        assert!(parse(&fixed, "default").is_ok());
    }

    #[test]
    fn node_groups_parse_as_node_group_policies_test() {
        let spec = json!({
            "name": "workers-general",
            "target": { "nodes": { "plugin": "aws-asg", "shape": { "cpu": 8000, "memory": 16384 } } },
            "max": 20,
        });
        let parsed = parse(&spec, "default").unwrap();
        assert_eq!(parsed.kind(), PolicyKind::Nodes);
        assert_eq!(parsed.id(), "workers-general");
        let mut namespaced = spec.clone();
        namespaced["namespace"] = json!("ops");
        assert!(parse(&namespaced, "default")
            .unwrap_err()
            .contains("belongs to the cluster"));
    }

    #[test]
    fn plugin_use_follows_the_catalog_test() {
        let catalog = PluginCatalog::from_app_config(&json!({ "plugins": {
            "team-prom": { "builtin": "prometheus", "config": { "address": "http://p:9090" },
                           "namespaces": ["media"] },
            "render": { "builtin": "exec", "config": { "command": "/opt/render.sh" } },
        } }))
        .unwrap();
        let StandaloneSpec::External(policy) = parse(&external(json!({})), "default").unwrap()
        else {
            panic!("external");
        };
        check_external(&policy, &catalog).unwrap();
        let StandaloneSpec::External(other) =
            parse(&external(json!({ "namespace": "payments" })), "default").unwrap()
        else {
            panic!("external");
        };
        let err = check_external(&other, &catalog).unwrap_err();
        assert!(
            err.contains("check 'queue' uses source 'team-prom', which is not available to namespace payments (allowed: media)"),
            "{err}"
        );
        let StandaloneSpec::External(missing) = parse(
            &external(json!({ "target": { "plugin": "asg" } })),
            "default",
        )
        .unwrap() else {
            panic!("external");
        };
        let err = check_external(&missing, &catalog).unwrap_err();
        assert!(
            err.contains("target uses plugin 'asg', which is not installed"),
            "{err}"
        );
    }
}
