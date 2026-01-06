use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::prelude::*;
use crate::storage::store::Store;
use crate::structs::{Allocation, LedgerEntry, Node, TaskConfig};

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

#[derive(Debug)]
pub struct Scheduler<'a> {
    store: &'a Store,
}

impl<'a> Scheduler<'a> {
    pub fn new(store: &'a Store) -> Self {
        Scheduler { store }
    }

    #[instrument(skip_all)]
    pub fn plan_and_apply(&self, job_config_id: &str, mode: PlanMode) -> R<PlanOutcome> {
        let nodes = self.available_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let tasks = self
            .store
            .get_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .map(|(id, config)| (id, config.clone(), config.count))
            .collect::<Vec<_>>();

        let required_counts = match mode {
            PlanMode::Fresh => tasks
                .iter()
                .map(|(id, _config, count)| (*id, *count))
                .collect::<HashMap<_, _>>(),
            PlanMode::Reschedule => {
                let existing = self.store.allocs_for_job_config(job_config_id)?;
                let mut current: HashMap<i64, u32> = HashMap::new();
                for alloc in existing {
                    *current.entry(alloc.task_config_id).or_default() += 1;
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

        let placements = self.build_plan(nodes, tasks, required_counts)?;
        if placements.is_empty() {
            return Ok(PlanOutcome {
                allocations: Vec::new(),
                deltas: Vec::new(),
            });
        }

        let ledger_before = self.store.get_ledger()?;
        let placement_pairs = placements
            .iter()
            .map(|placement| (placement.node_id.clone(), placement.task_config_id))
            .collect::<Vec<_>>();
        let allocations = self.store.apply_allocation_plan(&placement_pairs)?;
        let ledger_after = self.store.get_ledger()?;
        let deltas = Self::ledger_delta(&ledger_before, &ledger_after);

        Ok(PlanOutcome {
            allocations,
            deltas,
        })
    }

    fn available_nodes(&self) -> R<Vec<Node>> {
        let mut nodes: Vec<Node> = self
            .store
            .get_nodes()?
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
    ) -> R<Vec<PlannedAllocation>> {
        tasks.sort_by(|a, b| a.0.cmp(&b.0));

        let mut placements = Vec::new();
        let mut node_cursor = 0usize;

        for (task_id, task_config, _desired_count) in tasks {
            let needed = required_counts.get(&task_id).copied().unwrap_or(0);
            if needed == 0 {
                continue;
            }

            for _ in 0..needed {
                let node = self
                    .next_node_for_task(&nodes, &task_config, &mut node_cursor)
                    .with_context(|| format!("Failed to place task {}", task_config.name))?;

                placements.push(PlannedAllocation {
                    node_id: node.id.clone(),
                    task_config_id: task_id,
                });
            }

            node_cursor = node_cursor.saturating_add(1) % nodes.len();
        }

        Ok(placements)
    }

    fn next_node_for_task<'b>(
        &self,
        nodes: &'b [Node],
        task: &TaskConfig,
        node_cursor: &mut usize,
    ) -> R<&'b Node> {
        let total_nodes = nodes.len();
        if total_nodes == 0 {
            bail!("No nodes available for placement");
        }

        for offset in 0..total_nodes {
            let idx = (*node_cursor + offset) % total_nodes;
            let candidate = &nodes[idx];

            if Self::node_satisfies_constraints(candidate, &task.constraints) {
                *node_cursor = idx;
                return Ok(candidate);
            }
        }

        bail!(
            "No available nodes matching constraints for task {}",
            task.name
        )
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

    fn ledger_delta(before: &[LedgerEntry], after: &[LedgerEntry]) -> Vec<LedgerDelta> {
        let mut before_map: HashMap<String, Vec<Allocation>> = before
            .iter()
            .map(|entry| (entry.node.id.clone(), entry.allocations.clone()))
            .collect();

        let mut deltas = Vec::new();

        for entry in after {
            let prev = before_map.remove(&entry.node.id).unwrap_or_default();
            let added = entry
                .allocations
                .iter()
                .filter(|alloc| !prev.iter().any(|p| p.id == alloc.id))
                .cloned()
                .collect::<Vec<_>>();
            let removed = prev
                .iter()
                .filter(|alloc| !entry.allocations.iter().any(|p| p.id == alloc.id))
                .cloned()
                .collect::<Vec<_>>();

            if !added.is_empty() || !removed.is_empty() {
                deltas.push(LedgerDelta {
                    node_id: entry.node.id.clone(),
                    added,
                    removed,
                });
            }
        }

        for (node_id, allocations) in before_map {
            if !allocations.is_empty() {
                deltas.push(LedgerDelta {
                    node_id,
                    added: Vec::new(),
                    removed: allocations,
                });
            }
        }

        deltas
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::{json, Value};

    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::{JobConfig, NodeMsg, Submission};

    fn store_path() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock error")
            .as_nanos();

        format!("tests/store_scheduler_{}", nanos)
    }

    fn setup_store() -> R<Store> {
        let store_config = ManagerConfigFull {
            store_path: store_path(),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        store.reset_all_tables()?;

        Ok(store)
    }

    fn make_node_msg(id: &str, attrs: Value) -> NodeMsg {
        NodeMsg {
            id: id.to_string(),
            pool: "default".into(),
            status: "active".into(),
            availability: "available".into(),
            config_attrs: attrs.to_string(),
            fingerprint_attrs: "{}".into(),
            dynamic_attrs: "{}".into(),
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

        let scheduler = Scheduler::new(&store);
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
        let scheduler = Scheduler::new(&store);
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
        let scheduler = Scheduler::new(&store);

        let err = scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .expect_err("expected scheduling error");

        assert!(err.to_string().contains("Failed to place task analytics"));

        Ok(())
    }
}
