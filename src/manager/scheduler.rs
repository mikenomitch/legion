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
