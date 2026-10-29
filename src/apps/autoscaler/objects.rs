//! The autoscaler's own objects (app-object-kinds plan §8 O2;
//! docs/autoscaling.md, "Standalone policies" and "Node groups"):
//! `autoscaler/ScalingPolicy` and `autoscaler/NodeGroup`.
//!
//! - **The kinds' schemas** are filled into the bundled manifest from the
//!   parser's own types when it loads ([`fill_manifest`]).
//! - **Reading.** The engine keeps two [`ObjectCache`]s current with
//!   [`crate::apps::objects::run_object_watch`]: ScalingPolicies of the
//!   shards this replica holds, and every NodeGroup on the replica that
//!   holds the `nodegroups` shard. Each tick turns the cached objects into
//!   [`ObjectTarget`]s: parsed, resolved against the layered defaults
//!   `WatchScalingPolicies` sends (as a task's `autoscale` resolves), with
//!   the plugin allowlist enforced (fail-closed: a plugin closed to the
//!   policy's namespace makes it `invalid`, never run), and joined with
//!   the object's pause and action row from the same stream.
//! - **Status.** Durable verdicts only, written when they change
//!   ([`verdict`]): `Accepted` (the spec parses, resolves, and its plugins
//!   are open to it), `Ready` (its last evaluation worked), and fields:
//!   the resolved target, the effective policy and its digest, the error,
//!   the last action (from its action row), and a deleting node group's
//!   cleanup. Per-tick numbers stay on the `ReportScalingStatus` board.
//! - **The validate call** ([`register_validate`]) mirrors the same checks
//!   at apply, as advice; the engine's are the control.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::apps::objects::ObjectOutcome;
use crate::apps::ui::UiRegistry;
use crate::autoscaler::nodes::EffectiveNodeGroup;
use crate::autoscaler::plugins::PluginCatalog;
use crate::autoscaler::policy::{
    self as standalone, PolicyKind, StandaloneSpec, NODE_GROUP_KIND, SCALING_POLICY_KIND,
};
use crate::autoscaler::EffectivePolicy;
use crate::manager::autoscale::{object_target_key, summary_line, AutoscaleLayers};
use crate::prelude::*;
use crate::structs::app_object::ObjectValidateAnswer;
use crate::structs::{
    AppObject, ScalingAction, ScalingAppContext, ScalingObjectRef, ScalingObjectState,
    ScalingPauseInfo,
};

/// Fill the kinds' `schema` and `status.schema` into the bundled
/// autoscaler's evaluated manifest (`BundledApp::generated`), from the
/// types the parser reads.
pub fn fill_manifest(manifest: &mut Value) {
    let Some(kinds) = manifest.get_mut("kinds").and_then(Value::as_array_mut) else {
        return;
    };
    for kind in kinds {
        let schema = match kind.get("name").and_then(Value::as_str) {
            Some(SCALING_POLICY_KIND) => standalone::scaling_policy_schema(),
            Some(NODE_GROUP_KIND) => standalone::node_group_schema(),
            _ => continue,
        };
        kind["schema"] = schema;
        kind["status"] = json!({ "schema": standalone::status_fields_schema() });
    }
}

/// What one of the autoscaler's objects runs as, once resolved.
#[derive(Debug, Clone)]
pub(crate) enum Plan {
    /// A ScalingPolicy's effective policy (dry-run forced when the app's
    /// config says so).
    External(Box<EffectivePolicy>),
    /// A NodeGroup's, every default filled.
    Nodes(Box<EffectiveNodeGroup>),
}

/// One of the autoscaler's objects, as a tick runs it.
#[derive(Debug, Clone)]
pub(crate) struct ObjectTarget {
    /// The object as the watch delivered it.
    pub object: AppObject,
    pub kind: PolicyKind,
    /// Its wire reference, uid included (reports and intents name it).
    pub reference: ScalingObjectRef,
    /// `object:<uid>`: its history, pause, action row, board entry, and the
    /// engine's state live under it.
    pub key: String,
    /// `namespace/name`, or `name` for a node group: the label plugins see.
    pub id: String,
    /// `ScalingPolicy media/render-farm`: the label people see.
    pub label: String,
    /// Its cleanup window is running (a deleted node group).
    pub deleting: bool,
    /// What it runs as, or why it cannot run (the reason is the
    /// `Accepted=false` message).
    pub plan: Result<Plan, String>,
    /// The target plugin and its config.
    pub target_plugin: String,
    pub target_config: Value,
    /// Its own pause, or the cluster-wide one.
    pub pause: Option<ScalingPauseInfo>,
    /// Its action row, when it has one.
    pub action: Option<ScalingAction>,
    /// The effective policy's digest, canonical JSON, and one line.
    pub digest: String,
    pub policy_json: String,
    pub summary: String,
}

impl ObjectTarget {
    /// The engine's key for it: `(object:<uid>, "", "")`.
    pub fn target_key(&self) -> (String, String, String) {
        (self.key.clone(), String::new(), String::new())
    }

    /// `plugin render-exec`, or `nodes via aws-asg in pool default`.
    pub fn target_text(&self) -> String {
        match &self.plan {
            Ok(Plan::Nodes(group)) => format!(
                "nodes via {} in pool {} (worker type {})",
                group.plugin,
                group.pool.as_deref().unwrap_or("of the worker type"),
                group.worker_type
            ),
            _ if self.kind == PolicyKind::Nodes => format!("nodes via {}", self.target_plugin),
            _ => format!("plugin {}", self.target_plugin),
        }
    }
}

/// The layered defaults the context carries, with the app's plugins (so
/// the allowlist is checked on every resolution), or `None` before the
/// first context arrives.
pub(crate) fn layers_of(
    context: Option<&ScalingAppContext>,
    plugins: &PluginCatalog,
) -> Option<Result<AutoscaleLayers, String>> {
    let context = context?;
    Some(
        AutoscaleLayers::from_proto(context)
            .map(|layers| layers.with_plugins(plugins.clone()))
            .map_err(|err| format!("the scaling defaults do not parse in this build: {err:#}")),
    )
}

/// The wire reference of a watched object.
fn reference_of(object: &AppObject) -> ScalingObjectRef {
    ScalingObjectRef {
        kind: object.qualified_kind.clone(),
        namespace: object.namespace.clone(),
        name: object.name.clone(),
        uid: object.uid,
    }
}

/// Parse and resolve one watched object, and join it with its scaling
/// state. `layers` is `Err` when the defaults do not parse.
pub(crate) fn resolve_target(
    object: &AppObject,
    layers: &Result<AutoscaleLayers, String>,
    state: Option<&ScalingObjectState>,
    paused_all: Option<&ScalingPauseInfo>,
    force_dry_run: bool,
) -> Option<ObjectTarget> {
    let kind = PolicyKind::of_kind(&object.kind)?;
    let spec: Value = serde_json::from_str(&object.effective_json)
        .or_else(|_| serde_json::from_str(&object.spec_json))
        .unwrap_or(Value::Null);
    let parsed = standalone::parse_object(&object.kind, &object.namespace, &object.name, &spec);
    let (target_plugin, target_config) = match &parsed {
        Ok(StandaloneSpec::External(policy)) => {
            (policy.target.plugin.clone(), policy.target.config_value())
        }
        Ok(StandaloneSpec::Nodes(group)) => (
            group.target.nodes.plugin.clone(),
            group
                .target
                .nodes
                .config
                .clone()
                .unwrap_or_else(|| json!({})),
        ),
        Err(_) => (String::new(), json!({})),
    };
    let plan = match (&parsed, layers) {
        (Err(err), _) => Err(err.clone()),
        (_, Err(err)) => Err(err.clone()),
        (Ok(StandaloneSpec::External(policy)), Ok(layers)) => layers
            .resolve_standalone(policy)
            .map(|resolved| Plan::External(Box::new(resolved.policy))),
        (Ok(StandaloneSpec::Nodes(group)), Ok(layers)) => layers
            .resolve_node_group(group)
            .map(|group| Plan::Nodes(Box::new(group))),
    };
    // As the manager resolves it (`legion scaling status` shows the same
    // digest), before the app's config forces dry-run.
    let (digest, policy_json, summary) = match &plan {
        Ok(Plan::External(policy)) => (
            policy.digest(),
            policy.canonical_json(),
            summary_line(policy),
        ),
        Ok(Plan::Nodes(group)) => (group.digest(), group.canonical_json(), group.summary()),
        Err(err) => (String::new(), String::new(), format!("invalid: {err}")),
    };
    let plan = plan.map(|plan| match plan {
        Plan::External(mut policy) if force_dry_run => {
            policy.mode = crate::autoscaler::Mode::DryRun;
            Plan::External(policy)
        }
        Plan::Nodes(mut group) if force_dry_run => {
            group.mode = crate::autoscaler::Mode::DryRun;
            Plan::Nodes(group)
        }
        other => other,
    });
    let id = standalone::object_label(&object.namespace, &object.name);
    Some(ObjectTarget {
        reference: reference_of(object),
        key: object_target_key(object.uid),
        label: format!("{} {id}", kind.declared()),
        id,
        kind,
        deleting: object.deleting,
        plan,
        target_plugin,
        target_config,
        pause: state
            .and_then(|state| state.pause.clone())
            .or_else(|| paused_all.cloned()),
        action: state.and_then(|state| state.action.clone()),
        digest,
        policy_json,
        summary,
        object: object.clone(),
    })
}

/// When `unix_ms` was, RFC 3339.
fn rfc3339(unix_ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(unix_ms)
        .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

/// The last closed action intent, as the status's `lastAction`.
fn last_action(action: Option<&ScalingAction>) -> Option<Value> {
    let action = action.filter(|action| action.state != "open")?;
    let intent = action.intent.as_ref()?;
    Some(json!({
        "from": intent.from,
        "to": intent.desired.unwrap_or(intent.from),
        "at": rfc3339(if action.closed_at_unix_ms > 0 {
            action.closed_at_unix_ms
        } else {
            action.opened_at_unix_ms
        }),
        "decision": intent.decision_id,
        "outcome": action.state,
    }))
}

/// The `lastAction` the object's stored status already says: kept while
/// a newer intent is open, so the field never drops out mid-action.
fn stored_last_action(object: &AppObject) -> Option<Value> {
    let status = object.status.as_ref()?;
    let fields: Value = serde_json::from_str(&status.fields_json).ok()?;
    fields.get("lastAction").filter(|action| action.is_object()).cloned()
}

/// One object's durable verdict from its tick: what its status says. Only
/// facts that change when the world changes, so an unchanged verdict writes
/// nothing: whether the spec is accepted, whether the last evaluation
/// worked (`state` is the board's word for it), and the fields. `cleanup`
/// is a deleting node group's progress.
pub(crate) fn verdict(
    target: &ObjectTarget,
    state: &str,
    why: &str,
    cleanup: Option<String>,
) -> ObjectOutcome {
    let mut fields = serde_json::Map::new();
    fields.insert("target".to_string(), json!(target.target_text()));
    if !target.digest.is_empty() {
        fields.insert("policy".to_string(), json!(target.summary));
        fields.insert("digest".to_string(), json!(target.digest));
    }
    if let Some(action) = last_action(target.action.as_ref())
        .or_else(|| stored_last_action(&target.object))
    {
        fields.insert("lastAction".to_string(), action);
    }
    let outcome = match (&target.plan, state) {
        (Err(err), _) => {
            fields.insert("error".to_string(), json!(err));
            ObjectOutcome::rejected("Invalid", err)
        }
        (Ok(_), "invalid") => {
            fields.insert("error".to_string(), json!(why));
            ObjectOutcome::rejected("Invalid", why)
        }
        (Ok(_), "error") => {
            fields.insert("error".to_string(), json!(why));
            ObjectOutcome::not_ready("TargetError", why)
        }
        (Ok(_), "missing-data") => ObjectOutcome::not_ready("MissingData", why),
        (Ok(_), _) if target.deleting => {
            ObjectOutcome::not_ready("Deleting", cleanup.clone().unwrap_or_default())
        }
        (Ok(_), _) => match &target.pause {
            Some(pause) => ObjectOutcome::ready().because("Paused", format!("by {}", pause.by)),
            None => ObjectOutcome::ready(),
        },
    };
    if let Some(cleanup) = cleanup {
        fields.insert("cleanup".to_string(), json!(cleanup));
    }
    outcome.with_fields(Value::Object(fields))
}

/// What the validate call reads: the layers the policy watch last sent,
/// and the app's plugins.
#[derive(Clone)]
pub struct Validator {
    pub(crate) context: Arc<RwLock<Option<ScalingAppContext>>>,
    pub(crate) plugins: PluginCatalog,
}

impl Validator {
    /// The problems with an object's spec: the parser's (what a schema
    /// cannot say: `min <= max`, a check's strategy settings), the layered
    /// resolution's, and the plugin allowlist's. Advice: the engine checks
    /// the same at evaluation and holds a failing object `Accepted=false`.
    pub fn check(&self, kind: &str, namespace: &str, name: &str, spec: &Value) -> Vec<String> {
        let parsed = match standalone::parse_object(kind, namespace, name, spec) {
            Ok(parsed) => parsed,
            Err(err) => return vec![err],
        };
        let context = self.context.read().ok().and_then(|context| context.clone());
        match parsed {
            StandaloneSpec::External(policy) => {
                match layers_of(context.as_ref(), &self.plugins) {
                    Some(Ok(layers)) => layers.resolve_standalone(&policy).err(),
                    // No defaults yet: the plugins at least.
                    _ => standalone::check_external(&policy, &self.plugins).err(),
                }
            }
            StandaloneSpec::Nodes(group) => group
                .resolve()
                .and_then(|group| {
                    self.plugins
                        .check_node_target(&group.plugin)
                        .map_err(|err| format!("{}: {err}", group.name))
                })
                .err(),
        }
        .into_iter()
        .collect()
    }
}

/// Answer the validate call for both kinds (`validate = true` in the
/// manifest).
pub fn register_validate(ui: &UiRegistry, validator: Validator) {
    for kind in [SCALING_POLICY_KIND, NODE_GROUP_KIND] {
        let validator = validator.clone();
        ui.object_validate(kind, move |request| {
            let validator = validator.clone();
            async move {
                let call = request.call;
                Ok(ObjectValidateAnswer {
                    errors: validator.check(
                        &call.kind,
                        &call.namespace,
                        &call.name,
                        &call.effective,
                    ),
                    warnings: Vec::new(),
                })
            }
        });
    }
}

/// Which state the engine keeps per object, by key, for a quick lookup.
pub(crate) type ObjectStates = BTreeMap<String, ScalingObjectState>;

#[cfg(test)]
mod tests {
    use super::*;

    fn object(kind: &str, namespace: &str, name: &str, uid: u64, spec: Value) -> AppObject {
        AppObject {
            app: "autoscaler".into(),
            kind: kind.into(),
            qualified_kind: format!("autoscaler/{kind}"),
            namespace: namespace.into(),
            name: name.into(),
            uid,
            generation: 1,
            effective_json: spec.to_string(),
            state: "valid".into(),
            ..Default::default()
        }
    }

    fn catalog() -> PluginCatalog {
        PluginCatalog::from_app_config(&json!({ "plugins": {
            "prom": { "builtin": "prometheus", "config": { "address": "http://p:9090" },
                      "namespaces": ["media"] },
        } }))
        .unwrap()
    }

    fn policy_spec() -> Value {
        json!({
            "target": { "plugin": "noop" },
            "min": 0, "max": 10,
            "checks": [{ "name": "q", "source": "prom", "query": "sum(q)",
                         "strategy": { "name": "per-replica", "value": 10 },
                         "activation": 0 }],
        })
    }

    /// The bundled manifest declares both kinds with their generated
    /// schemas: the right scope, access, requires, immutable fields, and
    /// cleanup window.
    #[test]
    fn the_manifest_declares_both_kinds_test() {
        let manifest = crate::apps::bundled_manifest("autoscaler").expect("bundled");
        let kinds = crate::structs::app_object::manifest_kinds(manifest).expect("kinds");
        crate::structs::app_object::validate_kind_decls(&kinds).expect("valid kinds");
        let policy = kinds
            .iter()
            .find(|k| k.name == SCALING_POLICY_KIND)
            .expect("policy");
        assert_eq!(policy.access_name(), "scale");
        assert!(policy.scope.is_namespaced() && policy.validate);
        assert_eq!(policy.immutable, vec!["/spec/target/plugin".to_string()]);
        assert_eq!(policy.schema, standalone::scaling_policy_schema());
        let group = kinds
            .iter()
            .find(|k| k.name == NODE_GROUP_KIND)
            .expect("group");
        assert_eq!(group.access_name(), "node");
        assert!(!group.scope.is_namespaced());
        assert_eq!(group.requires[0].resource, "operator");
        assert_eq!(group.cleanup.as_deref(), Some("15m"));
        assert_eq!(group.schema, standalone::node_group_schema());
        assert!(group.status.is_some() && policy.status.is_some());
    }

    /// A policy resolves against the context's layers; a plugin closed to
    /// its namespace makes it unacceptable (the allowlist fails closed), and
    /// the verdict says so; a working one is Ready, with its target and
    /// digest; a pause is a Ready reason.
    #[test]
    fn objects_resolve_and_their_verdicts_follow_test() {
        let context = AutoscaleLayers::default().to_proto();
        let layers = layers_of(Some(&context), &catalog()).expect("context");
        let open = object(SCALING_POLICY_KIND, "media", "render", 7, policy_spec());
        let target = resolve_target(&open, &layers, None, None, false).expect("a policy");
        assert_eq!(target.key, "object:7");
        assert_eq!(target.label, "ScalingPolicy media/render");
        assert!(target.plan.is_ok(), "{:?}", target.plan);
        let ready = verdict(&target, "ok", "", None);
        let types: Vec<(String, String)> = ready
            .conditions()
            .into_iter()
            .map(|c| (c.r#type, c.status))
            .collect();
        assert_eq!(
            types,
            vec![
                ("Accepted".into(), "true".into()),
                ("Ready".into(), "true".into())
            ]
        );
        assert_eq!(ready.fields()["target"], "plugin noop");
        assert!(!ready.fields()["digest"]
            .as_str()
            .unwrap_or_default()
            .is_empty());

        let closed = object(SCALING_POLICY_KIND, "payments", "render", 8, policy_spec());
        let target = resolve_target(&closed, &layers, None, None, false).expect("a policy");
        let err = target.plan.clone().expect_err("closed to payments");
        assert!(err.contains("not available to namespace payments"), "{err}");
        let rejected = verdict(&target, "invalid", "", None);
        assert_eq!(rejected.conditions()[0].status, "false");
        assert_eq!(rejected.conditions()[0].reason, "Invalid");

        let inverted = object(
            SCALING_POLICY_KIND,
            "media",
            "inverted",
            9,
            json!({ "target": { "plugin": "noop" }, "min": 9, "max": 3,
                    "checks": [{ "name": "f", "source": "legion", "strategy": { "name": "fixed", "value": 1 } }] }),
        );
        let target = resolve_target(&inverted, &layers, None, None, false).expect("a policy");
        assert!(target.plan.expect_err("min > max").contains("is above"));

        let paused = ScalingPauseInfo {
            scope: "all".into(),
            by: "ops".into(),
            ..Default::default()
        };
        let target = resolve_target(&open, &layers, None, Some(&paused), true).expect("a policy");
        match &target.plan {
            Ok(Plan::External(policy)) => {
                assert_eq!(policy.mode, crate::autoscaler::Mode::DryRun)
            }
            other => panic!("{other:?}"),
        }
        let outcome = verdict(&target, "paused", "", None);
        assert_eq!(outcome.conditions()[1].reason, "Paused");
    }

    /// The validate call mirrors the engine: cross-field checks and the
    /// plugin allowlist come back as errors; a node group needs a node
    /// target.
    #[test]
    fn the_validate_call_mirrors_the_engine_test() {
        let validator = Validator {
            context: Arc::new(RwLock::new(Some(AutoscaleLayers::default().to_proto()))),
            plugins: catalog(),
        };
        assert!(validator
            .check(
                standalone::SCALING_POLICY,
                "media",
                "render",
                &policy_spec()
            )
            .is_empty());
        let errors = validator.check(
            standalone::SCALING_POLICY,
            "payments",
            "render",
            &policy_spec(),
        );
        assert!(
            errors[0].contains("not available to namespace payments"),
            "{errors:?}"
        );
        let mut inverted = policy_spec();
        inverted["min"] = json!(20);
        let errors = validator.check(standalone::SCALING_POLICY, "media", "render", &inverted);
        assert!(errors[0].contains("is above"), "{errors:?}");
        let group = json!({ "target": { "nodes": { "plugin": "noop" } }, "max": 3 });
        let errors = validator.check(standalone::NODE_GROUP, "", "workers", &group);
        assert_eq!(errors.len(), 1, "{errors:?}");
    }
}
