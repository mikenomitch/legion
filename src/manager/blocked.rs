//! Blocked evals: work that could not place waits under what blocked it,
//! and only capacity that could help wakes it (sandbox-scaling plan §6.15,
//! Phase 3; docs/scheduler-and-eval-loop.md, "Blocked evals").
//!
//! A plan that finds no node for a task fails with [`PlacementBlocked`].
//! The eval worker writes the eval `blocked` instead of `failed`, keyed by
//! what blocked it, in the same entry that used to fail it. The leader's
//! wake pass ([`crate::manager::daemon`]'s blocked-eval loop) re-pends a
//! blocked eval only when capacity appears on a node the eval's task could
//! run on: a node registering or being uncordoned, allocations leaving a
//! node, or a node's attributes changing.
//!
//! **Keys.** A blocked eval's key says what it waits for, for operators and
//! metrics:
//!
//! - `class:pool=<pool>[,<attr>=<value>...]`: the task's pool and the
//!   attributes its constraints pin with `=` (`pool=*` when its permit's
//!   pool rule is not a plain `=`). Any node of that class can help.
//! - `node:<id>`: a resume pinned to the node holding its checkpoint. Only
//!   that node can help.
//! - `escaped`: no task to reason about (the cluster had no schedulable
//!   node at all). Any capacity anywhere wakes it.
//!
//! The wake match is exact rather than by key string: the pass re-runs the
//! blocked task's own constraint and permit checks, and the node's
//! namespace restriction against the task's job, on the node whose capacity
//! changed, so operators the key cannot spell (`>`, `matches`,
//! `version_matches`, `contains`) still wake only on nodes that satisfy
//! them, and a node that accepts only other namespaces wakes nothing.
//! Capacity is not checked — the plan that runs next decides whether the
//! room is enough.

use crate::constraints::{ConstraintSet, PermitSet};
use crate::prelude::*;
use crate::structs::job::TaskConfig;
use crate::structs::Node;

/// Key for an eval blocked with no task in hand.
pub const ESCAPED_KEY: &str = "escaped";

/// A plan could not place a task on any node. Carried as the error of the
/// failed plan so the eval worker can block the eval instead of failing it;
/// its `Display` is the same message the failed eval used to record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementBlocked {
    pub key: String,
    /// `{task, taskConfigId, cpu, memory, message}`: what could not place and
    /// what it asks for, for `job status` and the wake pass.
    pub detail: Value,
    pub message: String,
    /// When a retry is known to help even with no capacity change: the
    /// placement could preempt, but only once its victims age past
    /// `victimExemptAge`. The eval worker requeues the eval after this delay
    /// instead of blocking it, because nothing would wake a blocked eval
    /// when an allocation merely gets older.
    pub retry_after: Option<std::time::Duration>,
}

impl std::fmt::Display for PlacementBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PlacementBlocked {}

impl PlacementBlocked {
    /// Blocked on placing `task` (config `task_config_id`). `pinned_node` is
    /// the node a restoring instance must land on, when it is pinned.
    pub fn for_task(
        task: &TaskConfig,
        task_config_id: i64,
        pinned_node: Option<&str>,
        message: String,
    ) -> Self {
        let resources = &task.resources;
        PlacementBlocked {
            key: blocking_key(task, pinned_node),
            detail: json!({
                "task": task.name,
                "taskConfigId": task_config_id,
                "cpu": resources.get("cpu").cloned().unwrap_or(Value::Null),
                "memory": resources.get("memory").cloned().unwrap_or(Value::Null),
                "pinnedNode": pinned_node,
                "message": message,
            }),
            message,
            retry_after: None,
        }
    }

    /// The same block, retried after `retry_after` (see the field).
    pub fn with_retry_after(mut self, retry_after: Option<std::time::Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }

    /// Blocked with no task to reason about.
    pub fn escaped(message: String) -> Self {
        PlacementBlocked {
            key: ESCAPED_KEY.to_string(),
            detail: json!({ "message": message }),
            message,
            retry_after: None,
        }
    }

    /// Find a `PlacementBlocked` anywhere in an error's chain.
    pub fn in_chain(error: &anyhow::Error) -> Option<&PlacementBlocked> {
        error
            .chain()
            .find_map(|cause| cause.downcast_ref::<PlacementBlocked>())
    }
}

/// The key a task blocks under (see the module docs).
pub fn blocking_key(task: &TaskConfig, pinned_node: Option<&str>) -> String {
    if let Some(node) = pinned_node {
        return format!("node:{node}");
    }
    // The permit's `=` pool (the implicit `default` included); a pool rule
    // with another operator spells no single pool, and the key says so.
    let pool = crate::constraints::permit_pool_binding(&task.permit)
        .map(|value| match value {
            Value::String(text) => text,
            other => other.to_string(),
        })
        .unwrap_or_else(|| "*".to_string());
    let mut parts = vec![format!("pool={pool}")];
    if let Ok(constraints) = ConstraintSet::new(&task.constraints) {
        let mut bindings: Vec<(String, String)> = constraints
            .equality_bindings()
            .into_iter()
            .filter(|(key, _)| key != "pool")
            .map(|(key, value)| {
                let value = match value {
                    Value::String(text) => text,
                    other => other.to_string(),
                };
                (key, value)
            })
            .collect();
        bindings.sort();
        parts.extend(
            bindings
                .into_iter()
                .map(|(key, value)| format!("{key}={value}")),
        );
    }
    format!("class:{}", parts.join(","))
}

/// The kind of a key, for the `legion_evals_blocked{key_kind}` gauge.
pub fn key_kind(key: &str) -> &'static str {
    if key.starts_with("node:") {
        "node"
    } else if key.starts_with("class:") {
        "class"
    } else {
        "escaped"
    }
}

/// Whether capacity appearing on `node` could help a blocked eval with this
/// key and detail: the node is the pinned one, or the task's own constraints
/// and permit accept it and the node accepts the namespace of its job
/// (`job_id`, canonical). A node restricted to other namespaces never wakes
/// the eval, whatever its pool, because the plan that ran next could not
/// place there either. `task` is `None` when the blocked task's config is
/// gone, which matches anything (the plan that runs next will find out).
pub fn node_could_help(key: &str, task: Option<&TaskConfig>, job_id: &str, node: &Node) -> bool {
    if key == ESCAPED_KEY {
        return true;
    }
    if let Some(pinned) = key.strip_prefix("node:") {
        return node.id == pinned;
    }
    let Some(task) = task else {
        return true;
    };
    let namespace = crate::structs::namespace::job_namespace(job_id);
    let permitted = PermitSet::new(&task.permit, namespace)
        .map(|permit| permit.is_permitted(node))
        .unwrap_or(true);
    let constrained = ConstraintSet::new(&task.constraints)
        .map(|constraints| constraints.is_satisfied(node, None))
        .unwrap_or(true);
    permitted && constrained
}

#[cfg(test)]
mod blocked_test {
    use super::*;

    fn node(id: &str, pool: &str, arch: &str) -> Node {
        Node {
            id: id.into(),
            pool: pool.into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs: json!({ "cpu.arch": arch }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    fn task(permit: Value, constraints: Value) -> TaskConfig {
        TaskConfig {
            permit,
            constraints,
            resources: json!({ "memory": 512, "cpu": 500 }),
            ..TaskConfig::new("workspace", "sandbox")
        }
    }

    #[test]
    fn keys_name_the_pool_and_pinned_attributes() {
        let agents = task(json!({ "pool": "agents" }), json!({ "cpu.arch": "x86_64" }));
        assert_eq!(
            blocking_key(&agents, None),
            "class:pool=agents,cpu.arch=x86_64"
        );
        assert_eq!(blocking_key(&agents, Some("w-17")), "node:w-17");
        let plain = task(json!({}), json!({}));
        assert_eq!(blocking_key(&plain, None), "class:pool=default");
        assert_eq!(key_kind("class:pool=default"), "class");
        assert_eq!(key_kind("node:w-1"), "node");
        assert_eq!(key_kind(ESCAPED_KEY), "escaped");
    }

    #[test]
    fn only_nodes_the_task_could_run_on_help() {
        let agents = task(json!({ "pool": "agents" }), json!({ "cpu.arch": "x86_64" }));
        let key = blocking_key(&agents, None);
        assert!(node_could_help(
            &key,
            Some(&agents),
            "agents",
            &node("a", "agents", "x86_64")
        ));
        assert!(!node_could_help(
            &key,
            Some(&agents),
            "agents",
            &node("b", "default", "x86_64")
        ));
        assert!(!node_could_help(
            &key,
            Some(&agents),
            "agents",
            &node("c", "agents", "aarch64")
        ));
        // Pinned resumes wait for their node only.
        assert!(node_could_help(
            "node:a",
            None,
            "agents",
            &node("a", "default", "x86_64")
        ));
        assert!(!node_could_help(
            "node:a",
            None,
            "agents",
            &node("b", "default", "x86_64")
        ));
        // Escaped wakes on anything, and so does a task whose config is gone.
        assert!(node_could_help(
            ESCAPED_KEY,
            None,
            "agents",
            &node("z", "x", "y")
        ));
        assert!(node_could_help(&key, None, "agents", &node("z", "x", "y")));
    }

    /// A node restricted to other namespaces never wakes the eval, even in
    /// the pool the task permits: the plan could not place there either.
    #[test]
    fn nodes_that_refuse_the_namespace_do_not_wake() {
        let task = task(json!({ "pool": "legion-system" }), json!({}));
        let key = blocking_key(&task, None);
        let mut system = node("m1", "legion-system", "x86_64");
        system.config_attrs = json!({ "namespaces": ["legion-system"] });
        assert!(!node_could_help(&key, Some(&task), "web", &system));
        assert!(!node_could_help(&key, Some(&task), "payments/web", &system));
        assert!(node_could_help(
            &key,
            Some(&task),
            "legion-system/agent",
            &system
        ));
        // `["*"]` does not reach legion-system.
        system.config_attrs = json!({ "namespaces": ["*"] });
        assert!(!node_could_help(
            &key,
            Some(&task),
            "legion-system/agent",
            &system
        ));
        assert!(node_could_help(&key, Some(&task), "payments/web", &system));
    }

    #[test]
    fn a_blocked_placement_survives_in_an_error_chain() {
        let blocked = PlacementBlocked::escaped("No nodes available to place allocations".into());
        let error = anyhow::Error::new(blocked).context("planning job agents");
        let found = PlacementBlocked::in_chain(&error).expect("in the chain");
        assert_eq!(found.key, ESCAPED_KEY);
        assert!(format!("{error:#}").contains("No nodes available"));
    }
}
