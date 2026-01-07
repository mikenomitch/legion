use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use libloading::Library;

use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{Allocation, Node, TaskConfig};
pub use policy::{load_policy, SchedulingContext, SchedulingPolicy};

mod policy;

#[derive(Debug, Clone)]
pub struct PlannedAllocation {
    pub node_id: String,
    pub task_config_id: i64,
}

#[derive(Debug, Clone)]
pub struct LedgerDelta {
    pub node_id: String,
    pub added: Vec<Allocation>,
    pub removed: Vec<Allocation>,
}

#[derive(Debug, Clone)]
pub enum PlanMode {
    Fresh,
    Reschedule,
}

#[derive(Debug, Clone)]
pub struct PlanOutcome {
    pub allocations: Vec<Allocation>,
    pub deltas: Vec<LedgerDelta>,
}

pub struct Scheduler<'a> {
    store: &'a Store,
    policy: Box<dyn SchedulingPolicy>,
    #[allow(dead_code)]
    policy_lib: Option<Library>,
}

impl<'a> Scheduler<'a> {
    pub fn new(store: &'a Store) -> R<Self> {
        let (policy_lib, policy) = load_policy(&store.scheduling_policy)?;

        Ok(Scheduler {
            store,
            policy,
            policy_lib,
        })
    }

    #[instrument(skip_all)]
    pub fn plan_and_apply(&self, job_config_id: &str, mode: PlanMode) -> R<PlanOutcome> {
        let nodes = self.available_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .map(|(id, config)| (id, config.clone(), config.count))
            .collect::<Vec<_>>();

        let allocations_by_node = self.store.cached_allocations_by_node()?;

        let required_counts = match mode {
            PlanMode::Fresh => tasks
                .iter()
                .map(|(id, _config, count)| (*id, *count))
                .collect::<HashMap<_, _>>(),
            PlanMode::Reschedule => {
                let mut current: HashMap<i64, u32> = HashMap::new();
                for allocations in allocations_by_node.values() {
                    for alloc in allocations {
                        if alloc.job_config_id == job_config_id {
                            *current.entry(alloc.task_config_id).or_default() += 1;
                        }
                    }
                }

                tasks
                    .iter()
                    .map(|(id, _config, desired)| {
                        let placed = current.get(id).copied().unwrap_or_default();
                        (*id, desired.saturating_sub(placed))
                    })
                    .collect::<HashMap<_, _>>()
            }
        };

        let placements = self.build_plan(nodes, tasks, required_counts, &allocations_by_node)?;
        if placements.is_empty() {
            return Ok(PlanOutcome {
                allocations: Vec::new(),
                deltas: Vec::new(),
            });
        }

        let placement_pairs = placements
            .iter()
            .map(|placement| (placement.node_id.clone(), placement.task_config_id))
            .collect::<Vec<_>>();
        let affected_nodes = placements
            .iter()
            .map(|placement| placement.node_id.clone())
            .collect::<HashSet<_>>();
        let allocations = self.store.apply_allocation_plan(&placement_pairs)?;
        let deltas = Self::allocations_delta(&affected_nodes, &allocations);

        Ok(PlanOutcome {
            allocations,
            deltas,
        })
    }

    fn available_nodes(&self) -> R<Vec<Node>> {
        let mut nodes: Vec<Node> = self
            .store
            .cached_nodes()?
            .into_iter()
            .filter(|node| node.availability == "available")
            .collect();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));

        Ok(nodes)
    }

    fn build_plan(
        &self,
        nodes: Vec<Node>,
        mut tasks: Vec<(i64, TaskConfig, u32)>,
        required_counts: HashMap<i64, u32>,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
    ) -> R<Vec<PlannedAllocation>> {
        tasks.sort_by(|a, b| a.0.cmp(&b.0));

        let mut placements = Vec::new();
        let mut planned: HashMap<String, usize> = HashMap::new();

        for (task_id, task_config, _desired_count) in tasks {
            let needed = required_counts.get(&task_id).copied().unwrap_or(0);
            if needed == 0 {
                continue;
            }

            for _ in 0..needed {
                let eligible = nodes
                    .iter()
                    .filter(|node| Self::node_satisfies_constraints(node, &task_config.constraints))
                    .collect::<Vec<_>>();

                if eligible.is_empty() {
                    bail!(
                        "Failed to place task {}: no available nodes matching constraints",
                        task_config.name
                    );
                }

                let eligible = self.apply_nodes_considered(eligible)?;
                let ctx =
                    SchedulingContext::new(eligible, &task_config, allocations_by_node, &planned);
                let node_id = self
                    .policy
                    .select_node(ctx)
                    .map(|node| node.id.clone())
                    .with_context(|| {
                        format!(
                            "Scheduling policy {} failed to place task {}",
                            self.policy.name(),
                            task_config.name
                        )
                    })?;

                *planned.entry(node_id.clone()).or_default() += 1;
                placements.push(PlannedAllocation {
                    node_id,
                    task_config_id: task_id,
                });
            }
        }

        Ok(placements)
    }

    fn apply_nodes_considered<'b>(&self, eligible: Vec<&'b Node>) -> R<Vec<&'b Node>> {
        if eligible.len() <= 1 {
            return Ok(eligible);
        }

        if !matches!(self.policy.name(), "bin_pack" | "spread") {
            return Ok(eligible);
        }

        let Some(settings) = self.store.scheduler_settings.as_ref() else {
            return Ok(eligible);
        };

        let settings = match settings {
            Value::Null => return Ok(eligible),
            Value::Object(map) => map,
            _ => {
                bail!(
                    "scheduler_settings for policy {} must be an object",
                    self.policy.name()
                )
            }
        };

        let Some(nodes_considered) = settings.get("nodes_considered") else {
            return Ok(eligible);
        };

        let limit = Self::parse_nodes_considered(nodes_considered, eligible.len())?;
        Ok(eligible.into_iter().take(limit).collect())
    }

    fn parse_nodes_considered(value: &Value, eligible_len: usize) -> R<usize> {
        if eligible_len == 0 {
            return Ok(0);
        }

        match value {
            Value::Number(count) => {
                let count = count
                    .as_u64()
                    .context("nodes_considered must be a positive integer or percentage")?;

                if count == 0 {
                    bail!("nodes_considered must be at least 1");
                }

                if eligible_len == 1 {
                    return Ok(1);
                }

                if count < 2 {
                    bail!("nodes_considered must be at least 2");
                }

                Ok(std::cmp::min(count as usize, eligible_len))
            }
            Value::String(raw) => {
                let trimmed = raw.trim();
                let percent = trimmed
                    .strip_suffix('%')
                    .context("nodes_considered percentage must end with '%'")?;
                let percent: f64 = percent
                    .trim()
                    .parse()
                    .context("nodes_considered percentage must be a number")?;

                if percent < 0.0 {
                    bail!("nodes_considered percentage cannot be negative");
                }

                if eligible_len == 1 {
                    return Ok(1);
                }

                let percent = percent / 100.0;
                let mut count = ((eligible_len as f64) * percent).ceil() as usize;
                if count < 2 {
                    count = 2;
                }

                Ok(std::cmp::min(count, eligible_len))
            }
            _ => bail!("nodes_considered must be a number or percentage string"),
        }
    }

    fn node_satisfies_constraints(node: &Node, constraints: &Value) -> bool {
        let constraint_map = match constraints.as_object() {
            Some(map) if !map.is_empty() => map,
            _ => return true,
        };

        let mut attrs = Map::new();

        if let Some(obj) = node.config_attrs.as_object() {
            attrs.extend(obj.clone());
        }
        if let Some(obj) = node.fingerprint_attrs.as_object() {
            attrs.extend(obj.clone());
        }
        if let Some(obj) = node.dynamic_attrs.as_object() {
            attrs.extend(obj.clone());
        }

        constraint_map
            .iter()
            .all(|(key, value)| attrs.get(key).map(|attr| attr == value).unwrap_or(false))
    }

    fn allocations_delta(
        affected_nodes: &HashSet<String>,
        allocations: &[Allocation],
    ) -> Vec<LedgerDelta> {
        let mut added_by_node: HashMap<String, Vec<Allocation>> = HashMap::new();
        for allocation in allocations {
            added_by_node
                .entry(allocation.node_id.clone())
                .or_default()
                .push(allocation.clone());
        }

        let mut ordered_nodes = affected_nodes.iter().collect::<Vec<_>>();
        ordered_nodes.sort();

        let mut deltas = Vec::new();
        for node_id in ordered_nodes {
            if let Some(added) = added_by_node.remove(node_id) {
                if !added.is_empty() {
                    deltas.push(LedgerDelta {
                        node_id: (*node_id).clone(),
                        added,
                        removed: Vec::new(),
                    });
                }
            }
        }

        for (node_id, added) in added_by_node {
            if !added.is_empty() {
                deltas.push(LedgerDelta {
                    node_id,
                    added,
                    removed: Vec::new(),
                });
            }
        }

        deltas
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use tempfile::Builder;

    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::{JobConfig, NodeMsg, Submission};

    fn store_path() -> R<String> {
        let store_dir = Builder::new()
            .prefix("khepri-scheduler-")
            .tempdir()
            .context("Error creating temp store directory")?
            .keep();

        Ok(store_dir
            .to_str()
            .context("Error parsing store path")?
            .to_string())
    }

    fn setup_store() -> R<Store> {
        setup_store_with_scheduler("bin_pack", None)
    }

    fn setup_store_with_scheduler(
        scheduling_policy: &str,
        scheduler_settings: Option<Value>,
    ) -> R<Store> {
        let store_config = ManagerConfigFull {
            store_path: store_path()?,
            scheduling_policy: scheduling_policy.into(),
            scheduler_settings,
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;

        Ok(store)
    }

    fn make_node_msg(id: &str, attrs: Value) -> NodeMsg {
        make_node_msg_with_sources(id, attrs, json!({}), json!({}))
    }

    fn make_node_msg_with_sources(
        id: &str,
        config_attrs: Value,
        fingerprint_attrs: Value,
        dynamic_attrs: Value,
    ) -> NodeMsg {
        NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: config_attrs.to_string(),
            fingerprint_attrs: fingerprint_attrs.to_string(),
            dynamic_attrs: dynamic_attrs.to_string(),
            health_checks: "{}".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        }
    }

    fn insert_test_nodes(store: &Store) -> R<()> {
        store.insert_node(make_node_msg("node-a", json!({ "tier": "edge" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "role": "db" })))?;

        Ok(())
    }

    fn dummy_submission() -> Submission {
        Submission {
            path: "job.jsonnet".into(),
            content: "{}".into(),
            is_primary: true,
        }
    }

    fn task_id_for(store: &Store, job_config_id: &str, task_name: &str) -> R<i64> {
        let task = store
            .get_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .find(|(_, cfg)| cfg.name == task_name)
            .context("missing task")?;

        Ok(task.0)
    }

    #[test]
    fn plans_fresh_allocations_and_deltas() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;

        let job = JobConfig {
            name: "sched-job".into(),
            tasks: vec![
                TaskConfig {
                    name: "web".into(),
                    count: 2,
                    kind: "noop".into(),
                    config: json!({}),
                    resources: json!({}),
                    constraints: json!({}),
                },
                TaskConfig {
                    name: "db".into(),
                    count: 1,
                    kind: "noop".into(),
                    config: json!({}),
                    resources: json!({}),
                    constraints: json!({ "role": "db" }),
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 3);

        let web_id = task_id_for(&store, &job_config_id, "web")?;
        let db_id = task_id_for(&store, &job_config_id, "db")?;

        let web_on_node_a = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.task_config_id == web_id && alloc.node_id == "node-a")
            .count();
        assert_eq!(web_on_node_a, 2);

        let db_on_node_b = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.task_config_id == db_id && alloc.node_id == "node-b")
            .count();
        assert_eq!(db_on_node_b, 1);

        assert_eq!(outcome.deltas.len(), 2);
        let node_a_delta = outcome
            .deltas
            .iter()
            .find(|delta| delta.node_id == "node-a")
            .expect("node-a delta missing");
        assert_eq!(node_a_delta.added.len(), 2);
        assert!(node_a_delta.removed.is_empty());

        let node_b_delta = outcome
            .deltas
            .iter()
            .find(|delta| delta.node_id == "node-b")
            .expect("node-b delta missing");
        assert_eq!(node_b_delta.added.len(), 1);
        assert!(node_b_delta.removed.is_empty());

        Ok(())
    }

    #[test]
    fn reschedule_only_places_missing_allocations() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;

        let job = JobConfig {
            name: "resched-job".into(),
            tasks: vec![TaskConfig {
                name: "batch".into(),
                count: 3,
                kind: "noop".into(),
                config: json!({}),
                resources: json!({}),
                constraints: json!({}),
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let batch_id = task_id_for(&store, &job_config_id, "batch")?;
        store.place_alloc("node-a", batch_id)?;
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;

        assert_eq!(outcome.allocations.len(), 2);
        assert_eq!(outcome.deltas.len(), 1);

        let delta = &outcome.deltas[0];
        assert_eq!(delta.node_id, "node-a");
        assert_eq!(delta.added.len(), 2);
        assert!(delta.removed.is_empty());

        Ok(())
    }

    #[test]
    fn errors_when_constraints_cannot_be_satisfied() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "tier": "edge" })))?;

        let job = JobConfig {
            name: "unschedulable".into(),
            tasks: vec![TaskConfig {
                name: "analytics".into(),
                count: 1,
                kind: "noop".into(),
                config: json!({}),
                resources: json!({}),
                constraints: json!({ "role": "db" }),
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;

        let err = scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .expect_err("expected scheduling error");

        assert!(err.to_string().contains("Failed to place task analytics"));

        Ok(())
    }

    #[test]
    fn constraint_matching_across_node_attributes() -> R<()> {
        let node = Node {
            id: "node-mixed".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
            config_attrs: json!({ "role": "db" }),
            fingerprint_attrs: json!({ "arch": "arm64" }),
            dynamic_attrs: json!({ "zone": "us-east" }),
            health_checks: json!({}),
        };

        let constraints = json!({
            "role": "db",
            "arch": "arm64",
            "zone": "us-east",
        });

        assert!(Scheduler::node_satisfies_constraints(&node, &constraints));

        let mismatched = json!({ "role": "web", "arch": "arm64", "zone": "us-east" });
        assert!(!Scheduler::node_satisfies_constraints(&node, &mismatched));

        Ok(())
    }

    #[test]
    fn scheduler_uses_nodes_matching_constraints_from_any_attribute_source() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;
        store.insert_node(make_node_msg_with_sources(
            "node-b",
            json!({}),
            json!({ "role": "db" }),
            json!({}),
        ))?;
        store.insert_node(make_node_msg_with_sources(
            "node-c",
            json!({}),
            json!({}),
            json!({ "role": "db" }),
        ))?;

        let job = JobConfig {
            name: "constraint-sources".into(),
            tasks: vec![TaskConfig {
                name: "db".into(),
                count: 2,
                kind: "noop".into(),
                config: json!({}),
                resources: json!({}),
                constraints: json!({ "role": "db" }),
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        for alloc in &outcome.allocations {
            let node = store.get_node(&alloc.node_id)?;
            assert!(Scheduler::node_satisfies_constraints(
                &node,
                &json!({ "role": "db" })
            ));
        }

        Ok(())
    }

    #[test]
    fn spread_limits_nodes_considered_by_count() -> R<()> {
        let store = setup_store_with_scheduler("spread", Some(json!({ "nodes_considered": 2 })))?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;
        store.insert_node(make_node_msg("node-c", json!({})))?;

        let job = JobConfig {
            name: "spread-considered".into(),
            tasks: vec![TaskConfig {
                name: "api".into(),
                count: 1,
                kind: "noop".into(),
                config: json!({}),
                resources: json!({}),
                constraints: json!({}),
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;

        for _ in 0..3 {
            store.place_alloc("node-a", task_id)?;
        }
        for _ in 0..2 {
            store.place_alloc("node-b", task_id)?;
        }

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-b");

        Ok(())
    }

    #[test]
    fn bin_pack_limits_nodes_considered_by_percentage() -> R<()> {
        let store =
            setup_store_with_scheduler("bin_pack", Some(json!({ "nodes_considered": "50%" })))?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;
        store.insert_node(make_node_msg("node-c", json!({})))?;

        let job = JobConfig {
            name: "binpack-considered".into(),
            tasks: vec![TaskConfig {
                name: "worker".into(),
                count: 1,
                kind: "noop".into(),
                config: json!({}),
                resources: json!({}),
                constraints: json!({}),
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "worker")?;

        for _ in 0..2 {
            store.place_alloc("node-b", task_id)?;
        }
        for _ in 0..5 {
            store.place_alloc("node-c", task_id)?;
        }

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-b");

        Ok(())
    }
}
