use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::Value;

use libloading::Library;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};

use crate::constraints::{AffinitySet, ConstraintSet, DistinctTracker, PermitSet};
use crate::prelude::*;
use crate::resources::ResourcePolicy;
use crate::storage::errors::StorageError;
use crate::storage::store::{PlanApplyOutcome, Store};
use crate::structs::{
    Allocation, JobConfig, Node, TaskConfig, TaskDependencyCondition, TaskLifecycle,
    TaskStatusState,
};
pub use policy::{load_policy, SchedulerWeights, SchedulingContext, SchedulingPolicy};

mod policy;

const RESCHEDULE_SCORE_JITTER_THRESHOLD: f64 = 0.01;

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

impl PlanMode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Reschedule => "reschedule",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlanOutcome {
    pub allocations: Vec<Allocation>,
    pub deltas: Vec<LedgerDelta>,
}

#[derive(Debug, Clone, Copy)]
enum NodesConsidered {
    Count(u64),
    Percent(f64),
}

#[derive(Debug, Clone)]
struct ResourceRequest {
    values: HashMap<String, ResourceValue>,
}

#[derive(Debug, Clone)]
struct ResourcePool {
    values: HashMap<String, ResourceValue>,
    unknown: HashSet<String>,
}

#[derive(Debug, Clone)]
enum ResourceValue {
    Amount(f64),
    Items(HashSet<String>),
}

#[derive(Debug, Clone)]
struct ResourceLedger {
    pools: HashMap<String, ResourcePool>,
}

struct TaskPlan<'a> {
    constraint_set: ConstraintSet,
    permit_set: PermitSet,
    affinity_set: AffinitySet,
    task_request: ResourceRequest,
    base_eligible: Vec<&'a Node>,
    distinct_tracker: Option<DistinctTracker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    name: String,
    token: String,
}

pub struct Scheduler<'a> {
    store: &'a Store,
    policy: Box<dyn SchedulingPolicy>,
    #[allow(dead_code)]
    policy_lib: Option<Library>,
}

#[derive(Debug, Clone)]
pub struct PlanConfigOutcome {
    pub allocations: Vec<PlannedAllocation>,
    pub task_lookup: HashMap<i64, String>,
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
    pub fn plan_only(&self, job_config_id: &str, mode: PlanMode) -> R<Vec<PlannedAllocation>> {
        let nodes = self.available_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let all_tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?;
        let tasks = self.tasks_for_plan_from_store(job_config_id)?;

        let mut allocations_by_node = self.store.cached_allocations_by_node()?;
        if matches!(mode, PlanMode::Fresh) {
            for allocations in allocations_by_node.values_mut() {
                allocations.retain(|alloc| alloc.job_config_id != job_config_id);
            }
        }

        let required_counts = Self::required_counts_for_mode(
            &tasks,
            mode.clone(),
            job_config_id,
            &allocations_by_node,
        );

        self.build_plan(
            job_config_id,
            nodes,
            tasks,
            all_tasks,
            required_counts,
            &allocations_by_node,
            mode,
        )
    }

    #[instrument(skip_all)]
    pub fn plan_for_config(
        &self,
        job_config: &JobConfig,
        mode: PlanMode,
        clear_job_id: Option<&str>,
    ) -> R<PlanConfigOutcome> {
        let nodes = self.available_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let status_map = HashMap::new();
        let tasks = Self::tasks_for_plan(job_config, &status_map)?;
        let all_tasks = job_config
            .tasks
            .iter()
            .enumerate()
            .map(|(index, config)| ((index + 1) as i64, config.clone()))
            .collect::<Vec<_>>();
        let task_lookup = tasks
            .iter()
            .map(|(id, task, _count)| (*id, task.name.clone()))
            .collect::<HashMap<_, _>>();

        let mut allocations_by_node = self.store.cached_allocations_by_node()?;
        if let Some(job_id) = clear_job_id {
            for allocations in allocations_by_node.values_mut() {
                allocations.retain(|alloc| alloc.job_id != job_id);
            }
        }

        let required_counts = Self::required_counts_for_mode(
            &tasks,
            mode.clone(),
            &job_config.id(),
            &allocations_by_node,
        );

        let allocations = self.build_plan(
            &job_config.id(),
            nodes,
            tasks,
            all_tasks,
            required_counts,
            &allocations_by_node,
            mode,
        )?;

        Ok(PlanConfigOutcome {
            allocations,
            task_lookup,
        })
    }

    #[instrument(skip_all)]
    pub fn plan_and_apply(&self, job_config_id: &str, mode: PlanMode) -> R<PlanOutcome> {
        self.plan_and_apply_with_exclusions(job_config_id, mode, None)
    }

    pub fn plan_and_apply_with_exclusions(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        excluded_nodes: Option<&HashSet<String>>,
    ) -> R<PlanOutcome> {
        let mut nodes = self.available_nodes()?;
        if let Some(excluded) = excluded_nodes {
            nodes.retain(|node| !excluded.contains(&node.id));
        }
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let all_tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?;
        let tasks = self.tasks_for_plan_from_store(job_config_id)?;

        let allocations_by_node = self.store.cached_allocations_by_node()?;

        let required_counts = Self::required_counts_for_mode(
            &tasks,
            mode.clone(),
            job_config_id,
            &allocations_by_node,
        );

        let placements = self.build_plan(
            job_config_id,
            nodes,
            tasks,
            all_tasks,
            required_counts,
            &allocations_by_node,
            mode.clone(),
        )?;
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
        let submission =
            self.store
                .insert_plan_submission(job_config_id, mode.as_str(), &placement_pairs)?;
        let allocations = match self.store.apply_plan_submission(&submission)? {
            PlanApplyOutcome::Applied(allocations) => allocations,
            PlanApplyOutcome::Rejected {
                reason,
                latest_versions,
            } => {
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: format!(
                        "plan submission rejected: {reason} (expected versions {:?}, latest versions {:?})",
                        submission.expected_versions, latest_versions
                    ),
                }));
            }
        };
        let deltas = Self::allocations_delta(&affected_nodes, &allocations);

        Ok(PlanOutcome {
            allocations,
            deltas,
        })
    }

    fn required_counts_for_mode(
        tasks: &[(i64, TaskConfig, u32)],
        mode: PlanMode,
        job_config_id: &str,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
    ) -> HashMap<i64, u32> {
        if matches!(mode, PlanMode::Fresh) {
            return tasks
                .iter()
                .map(|(id, _config, desired)| (*id, *desired))
                .collect();
        }

        let mut placed_by_task: HashMap<i64, u32> = HashMap::new();
        for allocations in allocations_by_node.values() {
            for alloc in allocations {
                if alloc.job_config_id == job_config_id {
                    *placed_by_task.entry(alloc.task_config_id).or_default() += 1;
                }
            }
        }

        tasks
            .iter()
            .map(|(id, _config, desired)| {
                let placed = placed_by_task.get(id).copied().unwrap_or_default();
                (*id, desired.saturating_sub(placed))
            })
            .collect()
    }

    fn existing_alloc_counts_by_task_node(
        job_config_id: &str,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
    ) -> HashMap<(i64, String), u32> {
        let mut counts = HashMap::new();
        for (node_id, allocations) in allocations_by_node {
            for alloc in allocations {
                if alloc.job_config_id == job_config_id {
                    *counts
                        .entry((alloc.task_config_id, node_id.clone()))
                        .or_default() += 1;
                }
            }
        }

        counts
    }

    fn count_existing_allocations_in_group(
        existing_alloc_counts_by_task_node: &HashMap<(i64, String), u32>,
        task_id: i64,
        nodes: &[&Node],
    ) -> u32 {
        nodes
            .iter()
            .map(|node| {
                existing_alloc_counts_by_task_node
                    .get(&(task_id, node.id.clone()))
                    .copied()
                    .unwrap_or_default()
            })
            .sum()
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
        job_config_id: &str,
        nodes: Vec<Node>,
        mut tasks: Vec<(i64, TaskConfig, u32)>,
        all_tasks: Vec<(i64, TaskConfig)>,
        required_counts: HashMap<i64, u32>,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
        mode: PlanMode,
    ) -> R<Vec<PlannedAllocation>> {
        tasks.sort_by(|a, b| a.0.cmp(&b.0));

        let mut placements = Vec::new();
        let mut planned: HashMap<String, usize> = HashMap::new();
        let weights = self.scheduler_weights()?;
        let nodes_considered_setting = self.nodes_considered_setting()?;
        let mut reschedule_rng = if matches!(mode, PlanMode::Reschedule) {
            Some(reschedule_rng())
        } else {
            None
        };
        let mut nodes_considered_rng =
            StdRng::from_rng(rand::thread_rng()).context("Failed to seed nodes_considered RNG")?;
        let existing_alloc_counts_by_task_node =
            Self::existing_alloc_counts_by_task_node(job_config_id, allocations_by_node);
        let node_lookup: HashMap<&str, &Node> =
            nodes.iter().map(|node| (node.id.as_str(), node)).collect();
        let resource_policy = &self.store.resource_policy;
        let mut resource_ledger =
            ResourceLedger::new(&nodes, allocations_by_node, resource_policy)?;
        let mut group_per_key: HashMap<String, Option<String>> = HashMap::new();
        let mut group_sizes: HashMap<String, usize> = HashMap::new();
        let mut group_constraints: HashMap<String, Vec<ConstraintSet>> = HashMap::new();
        let mut task_group_map: HashMap<i64, (String, Option<String>)> = HashMap::new();
        let task_name_to_id: HashMap<String, i64> = all_tasks
            .iter()
            .map(|(task_id, task_config)| (task_config.name.clone(), *task_id))
            .collect();
        let mut planned_task_allocations: HashMap<String, Vec<i64>> = HashMap::new();

        for (task_id, task_config) in &all_tasks {
            let per_key = task_config.count.per.clone();
            *group_sizes.entry(task_config.group.clone()).or_default() += 1;
            match group_per_key.get(&task_config.group) {
                Some(existing) if existing.as_deref() != per_key.as_deref() => {
                    bail!(
                        "Group {} mixes count.per values ({:?} vs {:?})",
                        task_config.group,
                        existing,
                        per_key
                    );
                }
                None => {
                    group_per_key.insert(task_config.group.clone(), per_key.clone());
                }
                _ => {}
            }
            if group_sizes.get(&task_config.group).copied().unwrap_or(0) > 1 {
                let constraint_set =
                    ConstraintSet::new(&task_config.constraints).with_context(|| {
                        format!("Failed to parse constraints for task {}", task_config.name)
                    })?;
                group_constraints
                    .entry(task_config.group.clone())
                    .or_default()
                    .push(constraint_set);
            }
            task_group_map.insert(*task_id, (task_config.group.clone(), per_key));
        }

        let mut group_assignments: HashMap<GroupKey, String> = HashMap::new();
        for (node_id, allocations) in allocations_by_node {
            for allocation in allocations {
                if allocation.job_config_id != job_config_id {
                    continue;
                }
                let Some((group_name, per_key)) = task_group_map.get(&allocation.task_config_id)
                else {
                    continue;
                };
                if group_sizes.get(group_name).copied().unwrap_or(0) <= 1 {
                    continue;
                }
                let token = if let Some(per_key) = per_key.as_deref() {
                    let node = node_lookup
                        .get(node_id.as_str())
                        .context("missing node for allocation")?;
                    let value = node_attribute(node, per_key).with_context(|| {
                        format!("Node {} missing attribute {}", node_id, per_key)
                    })?;
                    value_token(value)
                } else {
                    "default".to_string()
                };
                let key = GroupKey {
                    name: group_name.clone(),
                    token,
                };
                if let Some(existing) = group_assignments.get(&key) {
                    if existing != node_id {
                        bail!(
                            "Group {} already assigned to node {} but allocation exists on {}",
                            key.name,
                            existing,
                            node_id
                        );
                    }
                } else {
                    group_assignments.insert(key, node_id.clone());
                }
            }
        }

        let mut group_candidates: HashMap<GroupKey, Vec<&Node>> = HashMap::new();
        for (group_name, constraint_sets) in &group_constraints {
            let per_key = group_per_key
                .get(group_name)
                .and_then(|value| value.clone());
            let mut grouped: HashMap<String, Vec<&Node>> = HashMap::new();
            for node in &nodes {
                if !constraint_sets
                    .iter()
                    .all(|constraint| constraint.is_satisfied(node, None))
                {
                    continue;
                }
                let token = if let Some(per_key) = per_key.as_deref() {
                    let Some(value) = node_attribute(node, per_key) else {
                        continue;
                    };
                    value_token(value)
                } else {
                    "default".to_string()
                };
                grouped.entry(token).or_default().push(node);
            }

            for (token, group_nodes) in grouped {
                group_candidates.insert(
                    GroupKey {
                        name: group_name.clone(),
                        token,
                    },
                    group_nodes,
                );
            }
        }

        for (task_id, task_config, _desired_count) in tasks {
            let needed = required_counts.get(&task_id).copied().unwrap_or(0);
            let per_key = task_config.count.per.as_deref();
            if needed == 0 && per_key.is_none() {
                continue;
            }

            let mut plan = Self::build_task_plan(
                task_id,
                &task_config,
                &nodes,
                resource_policy,
                allocations_by_node,
                &node_lookup,
            )?;

            let base_eligible = plan.base_eligible.clone();
            if let Some(per_key) = per_key {
                let grouped = group_nodes_by_attr(&base_eligible, per_key);
                if grouped.is_empty() {
                    bail!(
                        "Failed to place task {}: no eligible nodes with attribute {}",
                        task_config.name,
                        per_key
                    );
                }

                for (group_name, group) in grouped {
                    let placed = if matches!(mode, PlanMode::Reschedule) {
                        Self::count_existing_allocations_in_group(
                            &existing_alloc_counts_by_task_node,
                            task_id,
                            &group,
                        )
                    } else {
                        0
                    };
                    let needed = task_config.count.num.saturating_sub(placed);
                    let group_key = GroupKey {
                        name: task_config.group.clone(),
                        token: group_name.to_string(),
                    };
                    let group_key = if group_sizes.get(&task_config.group).copied().unwrap_or(0) > 1
                    {
                        Some(group_key)
                    } else {
                        None
                    };
                    let candidates_for_group = if let Some(group_key) = group_key.as_ref() {
                        group_candidates
                            .get(group_key)
                            .with_context(|| {
                                format!(
                                    "No eligible nodes for group {} ({})",
                                    group_key.name, group_key.token
                                )
                            })?
                            .as_slice()
                    } else {
                        group.as_slice()
                    };
                    self.place_task_allocations(
                        needed,
                        task_id,
                        &task_config,
                        job_config_id,
                        candidates_for_group,
                        allocations_by_node,
                        &task_name_to_id,
                        &mut planned_task_allocations,
                        &mut planned,
                        &mut reschedule_rng,
                        nodes_considered_setting.as_ref(),
                        &mut nodes_considered_rng,
                        &node_lookup,
                        &mut resource_ledger,
                        &mut plan,
                        &mut placements,
                        weights,
                        group_key.as_ref(),
                        &mut group_assignments,
                    )?;
                }
            } else {
                let group_key = GroupKey {
                    name: task_config.group.clone(),
                    token: "default".to_string(),
                };
                let group_key = if group_sizes.get(&task_config.group).copied().unwrap_or(0) > 1 {
                    Some(group_key)
                } else {
                    None
                };
                let candidates_for_group = if let Some(group_key) = group_key.as_ref() {
                    group_candidates
                        .get(group_key)
                        .with_context(|| {
                            format!(
                                "No eligible nodes for group {} ({})",
                                group_key.name, group_key.token
                            )
                        })?
                        .as_slice()
                } else {
                    base_eligible.as_slice()
                };
                self.place_task_allocations(
                    needed,
                    task_id,
                    &task_config,
                    job_config_id,
                    candidates_for_group,
                    allocations_by_node,
                    &task_name_to_id,
                    &mut planned_task_allocations,
                    &mut planned,
                    &mut reschedule_rng,
                    nodes_considered_setting.as_ref(),
                    &mut nodes_considered_rng,
                    &node_lookup,
                    &mut resource_ledger,
                    &mut plan,
                    &mut placements,
                    weights,
                    group_key.as_ref(),
                    &mut group_assignments,
                )?;
            }
        }

        Ok(placements)
    }

    fn build_task_plan<'b>(
        task_id: i64,
        task_config: &'b TaskConfig,
        nodes: &'b [Node],
        resource_policy: &ResourcePolicy,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
        node_lookup: &HashMap<&'b str, &'b Node>,
    ) -> R<TaskPlan<'b>> {
        let task_resources =
            resource_policy.normalize_task_resources(&task_config.resources, true)?;
        let task_request = ResourceRequest::from_value(&task_resources)?;

        let constraint_set = ConstraintSet::new(&task_config.constraints).with_context(|| {
            format!("Failed to parse constraints for task {}", task_config.name)
        })?;
        let permit_set = PermitSet::new(&task_config.permit)
            .with_context(|| format!("Failed to parse permit for task {}", task_config.name))?;
        let affinity_set = AffinitySet::new(&task_config.affinities)
            .with_context(|| format!("Failed to parse affinities for task {}", task_config.name))?;
        let distinct_tracker = Self::build_distinct_tracker(
            &constraint_set,
            task_id,
            allocations_by_node,
            node_lookup,
        );
        let base_eligible = nodes
            .iter()
            .filter(|node| constraint_set.is_satisfied(node, None) && permit_set.is_permitted(node))
            .collect::<Vec<_>>();

        Ok(TaskPlan {
            constraint_set,
            permit_set,
            affinity_set,
            task_request,
            base_eligible,
            distinct_tracker,
        })
    }

    fn place_task_allocations<'b>(
        &self,
        count: u32,
        task_id: i64,
        task_config: &TaskConfig,
        job_config_id: &str,
        candidates: &[&'b Node],
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
        task_name_to_id: &HashMap<String, i64>,
        planned_task_allocations: &mut HashMap<String, Vec<i64>>,
        planned: &mut HashMap<String, usize>,
        reschedule_rng: &mut Option<StdRng>,
        nodes_considered_setting: Option<&NodesConsidered>,
        nodes_considered_rng: &mut StdRng,
        node_lookup: &HashMap<&'b str, &'b Node>,
        resource_ledger: &mut ResourceLedger,
        plan: &mut TaskPlan<'b>,
        placements: &mut Vec<PlannedAllocation>,
        weights: SchedulerWeights,
        group_key: Option<&GroupKey>,
        group_assignments: &mut HashMap<GroupKey, String>,
    ) -> R<()> {
        for _ in 0..count {
            let eligible = Self::eligible_nodes(
                candidates,
                &plan.constraint_set,
                &plan.permit_set,
                plan.distinct_tracker.as_ref(),
                resource_ledger,
                &plan.task_request,
            );

            if eligible.is_empty() {
                bail!(
                    "Failed to place task {}: no available nodes matching constraints, permit, or resources",
                    task_config.name
                );
            }

            let node_id = if let Some(group_key) = group_key {
                if let Some(existing) = group_assignments.get(group_key) {
                    let is_eligible = eligible
                        .iter()
                        .any(|node| node.id.as_str() == existing.as_str());
                    if !is_eligible {
                        bail!(
                            "Failed to place task {} in group {}: assigned node {} is not eligible",
                            task_config.name,
                            group_key.name,
                            existing
                        );
                    }
                    existing.clone()
                } else {
                    let selected = self.select_node_id(
                        eligible,
                        reschedule_rng,
                        nodes_considered_setting,
                        nodes_considered_rng,
                        task_config,
                        &plan.affinity_set,
                        job_config_id,
                        allocations_by_node,
                        task_name_to_id,
                        planned_task_allocations,
                        planned,
                        weights,
                    )?;
                    group_assignments.insert(group_key.clone(), selected.clone());
                    selected
                }
            } else {
                self.select_node_id(
                    eligible,
                    reschedule_rng,
                    nodes_considered_setting,
                    nodes_considered_rng,
                    task_config,
                    &plan.affinity_set,
                    job_config_id,
                    allocations_by_node,
                    task_name_to_id,
                    planned_task_allocations,
                    planned,
                    weights,
                )?
            };

            *planned.entry(node_id.clone()).or_default() += 1;
            planned_task_allocations
                .entry(node_id.clone())
                .or_default()
                .push(task_id);
            if let Some(tracker) = plan.distinct_tracker.as_mut() {
                if let Some(node) = node_lookup.get(node_id.as_str()) {
                    tracker.insert_from_node(node);
                }
            }
            resource_ledger.consume(&node_id, &plan.task_request)?;
            placements.push(PlannedAllocation {
                node_id,
                task_config_id: task_id,
            });
        }

        Ok(())
    }

    fn select_node_id<'b>(
        &self,
        eligible: Vec<&'b Node>,
        reschedule_rng: &mut Option<StdRng>,
        nodes_considered_setting: Option<&NodesConsidered>,
        nodes_considered_rng: &mut StdRng,
        task_config: &TaskConfig,
        affinity_set: &AffinitySet,
        job_config_id: &str,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
        task_name_to_id: &HashMap<String, i64>,
        planned_task_allocations: &HashMap<String, Vec<i64>>,
        planned: &HashMap<String, usize>,
        weights: SchedulerWeights,
    ) -> R<String> {
        let eligible = self.apply_nodes_considered_with_setting(
            eligible,
            nodes_considered_setting,
            nodes_considered_rng,
        )?;
        let ctx = SchedulingContext::new(
            eligible,
            task_config,
            affinity_set,
            job_config_id,
            allocations_by_node,
            task_name_to_id,
            planned_task_allocations,
            planned,
            weights,
        );
        let selected = reschedule_rng
            .as_mut()
            .map(|rng| self.select_policy_node_with_jitter(&ctx, rng))
            .unwrap_or_else(|| self.policy.select_node(ctx));
        selected.map(|node| node.id.clone()).with_context(|| {
            format!(
                "Scheduling policy {} failed to place task {}",
                self.policy.name(),
                task_config.name
            )
        })
    }

    fn eligible_nodes<'b>(
        candidates: &[&'b Node],
        constraint_set: &ConstraintSet,
        permit_set: &PermitSet,
        distinct_tracker: Option<&DistinctTracker>,
        resource_ledger: &ResourceLedger,
        task_request: &ResourceRequest,
    ) -> Vec<&'b Node> {
        candidates
            .iter()
            .copied()
            .filter(|node| {
                permit_set.is_permitted(node)
                    && distinct_tracker
                        .map(|tracker| constraint_set.distinct_satisfied(node, tracker))
                        .unwrap_or(true)
                    && resource_ledger.can_fit(&node.id, task_request)
            })
            .collect()
    }

    fn tasks_for_plan_from_store(&self, job_config_id: &str) -> R<Vec<(i64, TaskConfig, u32)>> {
        let tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .map(|(id, config)| (id, config.clone(), config.count.num))
            .collect::<Vec<_>>();
        let status_by_id = self.store.task_statuses_for_job_config(job_config_id)?;
        let status_map = tasks
            .iter()
            .map(|(id, task, _)| {
                (
                    task.name.clone(),
                    status_by_id
                        .get(id)
                        .copied()
                        .unwrap_or(TaskStatusState::Pending),
                )
            })
            .collect::<HashMap<_, _>>();

        Self::ready_tasks(tasks, &status_map)
    }

    fn tasks_for_plan(
        job_config: &JobConfig,
        status_map: &HashMap<String, TaskStatusState>,
    ) -> R<Vec<(i64, TaskConfig, u32)>> {
        let tasks = job_config
            .tasks
            .iter()
            .enumerate()
            .map(|(index, config)| ((index + 1) as i64, config.clone(), config.count.num))
            .collect::<Vec<_>>();

        Self::ready_tasks(tasks, status_map)
    }

    fn ready_tasks(
        tasks: Vec<(i64, TaskConfig, u32)>,
        status_map: &HashMap<String, TaskStatusState>,
    ) -> R<Vec<(i64, TaskConfig, u32)>> {
        let mut tasks_by_name: HashMap<String, &TaskConfig> = HashMap::new();
        for (_task_id, task, _count) in &tasks {
            tasks_by_name.insert(task.name.clone(), task);
        }

        let ordered_names = Self::task_names_topologically_sorted(&tasks, &tasks_by_name)?;

        let mut planned = HashSet::new();
        let mut progressed = true;
        while progressed {
            progressed = false;
            for task_name in &ordered_names {
                if planned.contains(task_name) {
                    continue;
                }

                let task = tasks_by_name
                    .get(task_name)
                    .copied()
                    .context("Task missing from task map")?;
                let ready = task.depends_on.iter().all(|dependency| {
                    Self::dependency_satisfied(dependency, status_map, &planned, &tasks_by_name)
                });
                if ready {
                    planned.insert(task_name.clone());
                    progressed = true;
                }
            }
        }

        Ok(tasks
            .into_iter()
            .filter(|(_, task, _)| planned.contains(&task.name))
            .collect())
    }

    fn task_names_topologically_sorted(
        tasks: &[(i64, TaskConfig, u32)],
        tasks_by_name: &HashMap<String, &TaskConfig>,
    ) -> R<Vec<String>> {
        let mut indegree: HashMap<String, usize> = tasks
            .iter()
            .map(|(_, task, _)| (task.name.clone(), 0))
            .collect();
        let mut graph: HashMap<String, Vec<String>> = HashMap::new();

        for (_task_id, task, _count) in tasks {
            for dependency in &task.depends_on {
                let dep_name = dependency.task.as_str();
                if dep_name == task.name {
                    bail!("Task {} cannot depend on itself", task.name);
                }
                if !tasks_by_name.contains_key(dep_name) {
                    bail!("Task {} depends on unknown task {}", task.name, dep_name);
                }

                graph
                    .entry(dep_name.to_string())
                    .or_default()
                    .push(task.name.clone());
                *indegree.entry(task.name.clone()).or_default() += 1;
            }
        }

        let mut zero_degree = indegree
            .iter()
            .filter_map(|(name, degree)| (*degree == 0).then_some(name.clone()))
            .collect::<Vec<_>>();
        zero_degree.sort();
        let mut queue: VecDeque<String> = zero_degree.into_iter().collect();

        let mut ordered = Vec::with_capacity(tasks.len());
        while let Some(name) = queue.pop_front() {
            ordered.push(name.clone());
            if let Some(children) = graph.get(&name) {
                for child in children {
                    if let Some(count) = indegree.get_mut(child) {
                        *count -= 1;
                        if *count == 0 {
                            queue.push_back(child.clone());
                        }
                    }
                }
            }
        }

        if ordered.len() != tasks.len() {
            bail!("Task dependencies contain a cycle");
        }

        Ok(ordered)
    }

    fn dependency_satisfied(
        dependency: &crate::structs::TaskDependency,
        status_map: &HashMap<String, TaskStatusState>,
        planned: &HashSet<String>,
        tasks_by_name: &HashMap<String, &TaskConfig>,
    ) -> bool {
        let dep_name = dependency.task.as_str();
        let dep_status = status_map
            .get(dep_name)
            .copied()
            .unwrap_or(TaskStatusState::Pending);
        let dep_lifecycle = tasks_by_name
            .get(dep_name)
            .map(|task| task.lifecycle.clone())
            .unwrap_or(TaskLifecycle::Service);
        let planned_dep = planned.contains(dep_name);

        match dependency.condition {
            TaskDependencyCondition::Start => dep_status.is_started() || planned_dep,
            TaskDependencyCondition::Complete => {
                dep_status != TaskStatusState::Failed
                    && (dep_status.is_complete()
                        || (planned_dep && dep_lifecycle == TaskLifecycle::Batch))
            }
            TaskDependencyCondition::Success => {
                dep_status != TaskStatusState::Failed
                    && (dep_status.is_success()
                        || (planned_dep && dep_lifecycle == TaskLifecycle::Batch))
            }
        }
    }

    fn apply_nodes_considered_with_setting<'b>(
        &self,
        eligible: Vec<&'b Node>,
        setting: Option<&NodesConsidered>,
        rng: &mut StdRng,
    ) -> R<Vec<&'b Node>> {
        if eligible.len() <= 1 {
            return Ok(eligible);
        }

        let Some(setting) = setting else {
            return Ok(eligible);
        };

        let limit = Self::nodes_considered_limit(setting, eligible.len())?;
        let mut eligible = eligible;
        if limit < eligible.len() {
            eligible.shuffle(rng);
        }

        Ok(eligible.into_iter().take(limit).collect())
    }

    fn select_policy_node_with_jitter<'b>(
        &self,
        ctx: &SchedulingContext<'b>,
        rng: &mut StdRng,
    ) -> Option<&'b Node> {
        if !matches!(self.policy.name(), "bin_pack" | "spread") {
            let fallback_ctx = SchedulingContext::new(
                ctx.eligible_nodes.clone(),
                ctx.task,
                ctx.affinity_set,
                ctx.job_config_id,
                ctx.allocations,
                ctx.task_name_to_id,
                ctx.planned_task_allocations,
                ctx.planned,
                ctx.weights,
            );
            return self.policy.select_node(fallback_ctx);
        }

        let scored = score_nodes_for_context(ctx);
        if scored.is_empty() {
            return None;
        }

        let max_score = scored
            .iter()
            .fold(f64::NEG_INFINITY, |best, (_, score)| best.max(*score));
        let mut top = scored
            .into_iter()
            .filter(|(_, score)| *score >= max_score - RESCHEDULE_SCORE_JITTER_THRESHOLD)
            .map(|(node, _)| node)
            .collect::<Vec<_>>();
        top.shuffle(rng);
        top.first().copied()
    }

    fn scheduler_weights(&self) -> R<SchedulerWeights> {
        let mut weights = SchedulerWeights::for_policy(self.policy.name());
        let Some(settings) = self.store.scheduler_settings.as_ref() else {
            return Ok(weights);
        };

        let settings = match settings {
            Value::Null => return Ok(weights),
            Value::Object(map) => map,
            _ => {
                bail!(
                    "scheduler_settings for policy {} must be an object",
                    self.policy.name()
                )
            }
        };

        let Some(weight_values) = settings.get("weights") else {
            return Ok(weights);
        };

        let weight_values = match weight_values {
            Value::Object(map) => map,
            _ => bail!("scheduler_settings weights must be an object"),
        };

        for (key, value) in weight_values {
            let weight = value
                .as_f64()
                .with_context(|| format!("scheduler_settings weights {key} must be a number"))?;

            if weight < 0.0 {
                bail!("scheduler_settings weights {key} cannot be negative");
            }

            match key.as_str() {
                "affinities" => weights.affinity = weight,
                "spread" => weights.spread = weight,
                "bin_pack" => weights.bin_pack = weight,
                _ => {}
            }
        }

        Ok(weights)
    }

    fn build_distinct_tracker<'b>(
        constraints: &ConstraintSet,
        task_id: i64,
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
        node_lookup: &HashMap<&'b str, &'b Node>,
    ) -> Option<DistinctTracker> {
        let distinct_keys = constraints.distinct_keys();
        if distinct_keys.is_empty() {
            return None;
        }

        let mut tracker = DistinctTracker::new(&distinct_keys);
        for (node_id, allocations) in allocations_by_node {
            if allocations
                .iter()
                .any(|alloc| alloc.task_config_id == task_id)
            {
                if let Some(node) = node_lookup.get(node_id.as_str()) {
                    tracker.insert_from_node(node);
                }
            }
        }

        Some(tracker)
    }

    fn nodes_considered_setting(&self) -> R<Option<NodesConsidered>> {
        if !matches!(self.policy.name(), "bin_pack" | "spread") {
            return Ok(None);
        }

        let Some(settings) = self.store.scheduler_settings.as_ref() else {
            return Ok(None);
        };

        let settings = match settings {
            Value::Null => return Ok(None),
            Value::Object(map) => map,
            _ => {
                bail!(
                    "scheduler_settings for policy {} must be an object",
                    self.policy.name()
                )
            }
        };

        let Some(nodes_considered) = settings.get("nodes_considered") else {
            return Ok(None);
        };

        match nodes_considered {
            Value::Number(count) => {
                let count = count
                    .as_u64()
                    .context("nodes_considered must be a positive integer or percentage")?;

                if count == 0 {
                    bail!("nodes_considered must be at least 1");
                }

                Ok(Some(NodesConsidered::Count(count)))
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

                Ok(Some(NodesConsidered::Percent(percent)))
            }
            _ => bail!("nodes_considered must be a number or percentage string"),
        }
    }

    fn nodes_considered_limit(setting: &NodesConsidered, eligible_len: usize) -> R<usize> {
        if eligible_len == 0 {
            return Ok(0);
        }

        match setting {
            NodesConsidered::Count(count) => {
                if eligible_len == 1 {
                    return Ok(1);
                }

                if *count < 2 {
                    bail!("nodes_considered must be at least 2");
                }

                Ok(std::cmp::min(*count as usize, eligible_len))
            }
            NodesConsidered::Percent(percent) => {
                if eligible_len == 1 {
                    return Ok(1);
                }

                let percent = *percent / 100.0;
                let mut count = ((eligible_len as f64) * percent).ceil() as usize;
                if count < 2 {
                    count = 2;
                }

                Ok(std::cmp::min(count, eligible_len))
            }
        }
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

fn group_nodes_by_attr<'a>(nodes: &'a [&'a Node], key: &str) -> HashMap<String, Vec<&'a Node>> {
    let mut grouped: HashMap<String, Vec<&'a Node>> = HashMap::new();
    for node in nodes {
        if key == "id" {
            grouped.entry(node.id.clone()).or_default().push(*node);
            continue;
        }
        let Some(value) = node_attribute(node, key) else {
            continue;
        };
        let token = value_token(value);
        grouped.entry(token).or_default().push(*node);
    }
    grouped
}

fn score_nodes_for_context<'a>(ctx: &SchedulingContext<'a>) -> Vec<(&'a Node, f64)> {
    if ctx.eligible_nodes.is_empty() {
        return Vec::new();
    }

    let mut min_load: Option<usize> = None;
    let mut max_load: Option<usize> = None;
    let mut loads: HashMap<&'a str, usize> = HashMap::new();

    for node in &ctx.eligible_nodes {
        let load = ctx.total_assigned(&node.id);
        loads.insert(node.id.as_str(), load);
        min_load = Some(min_load.map_or(load, |min| min.min(load)));
        max_load = Some(max_load.map_or(load, |max| max.max(load)));
    }

    let min_load = min_load.unwrap_or_default() as f64;
    let max_load = max_load.unwrap_or_default() as f64;
    let range = (max_load - min_load).max(0.0);

    ctx.eligible_nodes
        .iter()
        .copied()
        .map(|node| {
            let load = loads.get(node.id.as_str()).copied().unwrap_or_default() as f64;
            let (spread_score, bin_pack_score) = if range > 0.0 {
                ((max_load - load) / range, (load - min_load) / range)
            } else {
                (0.0, 0.0)
            };
            let affinity_score = ctx.affinity_set.score(node);
            let score = ctx.weights.affinity * affinity_score
                + ctx.weights.spread * spread_score
                + ctx.weights.bin_pack * bin_pack_score;
            (node, score)
        })
        .collect()
}

fn node_attribute<'a>(node: &'a Node, key: &str) -> Option<&'a Value> {
    node.config_attrs
        .as_object()
        .and_then(|attrs| attrs.get(key))
        .or_else(|| {
            node.fingerprint_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
        .or_else(|| {
            node.dynamic_attrs
                .as_object()
                .and_then(|attrs| attrs.get(key))
        })
}

impl ResourceRequest {
    fn from_value(value: &Value) -> R<Self> {
        let obj = value.as_object().context("resources must be an object")?;
        let mut values = HashMap::new();

        for (key, value) in obj {
            let demand = match value {
                Value::Number(num) => {
                    let amount = num.as_f64().context("resource values must be numbers")?;
                    if amount < 0.0 {
                        bail!("resource {key} cannot be negative");
                    }
                    ResourceValue::Amount(amount)
                }
                Value::String(item) => {
                    let mut items = HashSet::new();
                    items.insert(item.clone());
                    ResourceValue::Items(items)
                }
                Value::Array(items) => {
                    let set = items.iter().map(value_token).collect::<HashSet<_>>();
                    ResourceValue::Items(set)
                }
                _ => bail!("resource {key} must be a number or list"),
            };

            if matches!(demand, ResourceValue::Items(_)) && is_builtin_resource(key) {
                bail!("resource {key} must be a number");
            }

            values.insert(key.clone(), demand);
        }

        Ok(ResourceRequest { values })
    }
}

impl ResourceLedger {
    fn new(
        nodes: &[Node],
        allocations_by_node: &HashMap<String, Vec<Allocation>>,
        policy: &ResourcePolicy,
    ) -> R<Self> {
        let mut pools = HashMap::new();
        for node in nodes {
            let mut pool = ResourcePool::from_node(node)?;
            if !pool.unknown.is_empty() {
                warn!(
                    node_id = %node.id,
                    unknown_resources = ?pool.unknown,
                    "Node missing resource fingerprints; assuming infinite capacity"
                );
            }
            if let Some(allocs) = allocations_by_node.get(&node.id) {
                for alloc in allocs {
                    let normalized = policy.normalize_task_resources(&alloc.resources, false)?;
                    let request = ResourceRequest::from_value(&normalized)?;
                    pool.apply_usage(&request);
                }
            }
            pools.insert(node.id.clone(), pool);
        }

        Ok(ResourceLedger { pools })
    }

    fn can_fit(&self, node_id: &str, request: &ResourceRequest) -> bool {
        self.pools
            .get(node_id)
            .map(|pool| pool.can_fit(request))
            .unwrap_or(false)
    }

    fn consume(&mut self, node_id: &str, request: &ResourceRequest) -> R<()> {
        let pool = self
            .pools
            .get_mut(node_id)
            .context("missing resource pool")?;
        if !pool.can_fit(request) {
            bail!("node {node_id} does not have required resources");
        }
        pool.consume(request);
        Ok(())
    }
}

impl ResourcePool {
    fn from_node(node: &Node) -> R<Self> {
        let mut values = HashMap::new();
        let mut unknown = HashSet::new();

        let (memory_mib, memory_unknown) = fingerprint_memory_mib(node)?;
        if memory_unknown {
            unknown.insert("memory".to_string());
        }
        values.insert("memory".to_string(), ResourceValue::Amount(memory_mib));

        let (cpu_mhz, cores, cpu_unknown) = fingerprint_cpu(node)?;
        if cpu_unknown {
            unknown.insert("cpu".to_string());
            unknown.insert("cores".to_string());
        }
        values.insert("cpu".to_string(), ResourceValue::Amount(cpu_mhz));
        values.insert("cores".to_string(), ResourceValue::Amount(cores));

        let config_attrs = node.config_attrs.as_object().cloned().unwrap_or_default();

        let max_allocations = config_value(&config_attrs, &["maxAllocations", "max_allocations"])
            .and_then(|value| value.as_f64());
        let allocations = max_allocations.unwrap_or(f64::INFINITY);
        values.insert(
            "allocations".to_string(),
            ResourceValue::Amount(allocations),
        );

        if let Some(custom) = config_value(&config_attrs, &["resources"]) {
            let custom = custom
                .as_object()
                .context("node resources must be an object")?;
            for (name, value) in custom {
                if is_builtin_resource(name) {
                    continue;
                }
                let resource = parse_resource_value(name, value)?;
                values.insert(name.clone(), resource);
            }
        }

        let mut pool = ResourcePool { values, unknown };

        if let Some(reserved) =
            config_value(&config_attrs, &["reservedResources", "reserved_resources"])
        {
            let reserved = reserved
                .as_object()
                .context("reserved_resources must be an object")?;
            for (name, value) in reserved {
                let reservation = parse_resource_value(name, value)?;
                pool.reserve(name, &reservation);
            }
        }

        Ok(pool)
    }

    fn can_fit(&self, request: &ResourceRequest) -> bool {
        request.values.iter().all(|(name, demand)| match demand {
            ResourceValue::Amount(amount) => match self.values.get(name) {
                Some(ResourceValue::Amount(available)) => available >= amount,
                _ => false,
            },
            ResourceValue::Items(items) => match self.values.get(name) {
                Some(ResourceValue::Items(available)) => items.is_subset(available),
                _ => false,
            },
        })
    }

    fn consume(&mut self, request: &ResourceRequest) {
        self.apply_usage(request);
    }

    fn apply_usage(&mut self, request: &ResourceRequest) {
        for (name, demand) in &request.values {
            match demand {
                ResourceValue::Amount(amount) => {
                    let entry = self
                        .values
                        .entry(name.clone())
                        .or_insert(ResourceValue::Amount(0.0));
                    if let ResourceValue::Amount(available) = entry {
                        *available -= amount;
                    }
                }
                ResourceValue::Items(items) => {
                    if let Some(ResourceValue::Items(available)) = self.values.get_mut(name) {
                        for item in items {
                            available.remove(item);
                        }
                    }
                }
            }
        }
    }

    fn reserve(&mut self, name: &str, reservation: &ResourceValue) {
        match reservation {
            ResourceValue::Amount(amount) => {
                let entry = self
                    .values
                    .entry(name.to_string())
                    .or_insert(ResourceValue::Amount(0.0));
                if let ResourceValue::Amount(available) = entry {
                    *available -= amount;
                }
            }
            ResourceValue::Items(items) => {
                if let Some(ResourceValue::Items(available)) = self.values.get_mut(name) {
                    for item in items {
                        available.remove(item);
                    }
                }
            }
        }
    }
}

fn reschedule_rng() -> StdRng {
    if let Ok(seed) = std::env::var("LEGION_RESCHEDULE_SEED") {
        if let Ok(seed) = seed.parse::<u64>() {
            return StdRng::seed_from_u64(seed);
        }
    }
    if cfg!(test) {
        return StdRng::seed_from_u64(7);
    }
    StdRng::from_entropy()
}

fn parse_resource_value(name: &str, value: &Value) -> R<ResourceValue> {
    match value {
        Value::Number(num) => {
            let amount = num.as_f64().context("resource values must be numbers")?;
            if amount < 0.0 {
                bail!("resource {name} cannot be negative");
            }
            Ok(ResourceValue::Amount(amount))
        }
        Value::String(item) => {
            let mut items = HashSet::new();
            items.insert(item.clone());
            Ok(ResourceValue::Items(items))
        }
        Value::Array(items) => Ok(ResourceValue::Items(
            items.iter().map(value_token).collect::<HashSet<_>>(),
        )),
        _ => bail!("resource {name} must be a number or list"),
    }
}

fn fingerprint_memory_mib(node: &Node) -> R<(f64, bool)> {
    let total = node
        .fingerprint_attrs
        .get("memory_total")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<f64>().ok());

    match total {
        Some(total) => Ok((total / 1024.0, false)),
        None => Ok((f64::INFINITY, true)),
    }
}

fn fingerprint_cpu(node: &Node) -> R<(f64, f64, bool)> {
    let mut total_mhz = 0.0;
    let mut cores = 0.0;
    let mut known = false;
    if let Some(cpus) = node
        .fingerprint_attrs
        .get("cpus")
        .and_then(|val| val.as_object())
    {
        for cpu in cpus.values() {
            if let Some(freq) = cpu.get("frequency") {
                if let Some(freq) = freq.as_str().and_then(|val| val.parse::<f64>().ok()) {
                    total_mhz += freq;
                    known = true;
                } else if let Some(freq) = freq.as_f64() {
                    total_mhz += freq;
                    known = true;
                }
            }
            cores += 1.0;
        }
        if !cpus.is_empty() {
            known = true;
        }
    }

    if known {
        Ok((total_mhz, cores, false))
    } else {
        Ok((f64::INFINITY, f64::INFINITY, true))
    }
}

fn config_value<'a>(attrs: &'a serde_json::Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| attrs.get(*key))
}

fn is_builtin_resource(name: &str) -> bool {
    matches!(name, "memory" | "cpu" | "cores" | "allocations")
}

fn value_token(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use std::collections::HashSet;
    use tempfile::Builder;

    use super::*;
    use crate::structs::config::ManagerConfigFull;
    use crate::structs::{
        JobConfig, NodeMsg, Submission, TaskCount, TaskDependencyCondition, TaskLifecycle,
    };

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
        make_node_msg_with_sources(id, attrs, default_fingerprint(), json!({}))
    }

    fn make_node_msg_with_sources(
        id: &str,
        config_attrs: Value,
        fingerprint_attrs: Value,
        dynamic_attrs: Value,
    ) -> NodeMsg {
        let fingerprint_attrs = merge_fingerprint(fingerprint_attrs);
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

    fn default_fingerprint() -> Value {
        json!({
            "memory_total": "8388608",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        })
    }

    fn merge_fingerprint(extra: Value) -> Value {
        let mut base = default_fingerprint()
            .as_object()
            .cloned()
            .unwrap_or_default();
        if let Value::Object(additional) = extra {
            for (key, value) in additional {
                base.insert(key, value);
            }
        }
        Value::Object(base)
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
                    count: TaskCount::new(2),
                    ..TaskConfig::new("web", "noop")
                },
                TaskConfig {
                    constraints: json!({ "role": "db" }),
                    ..TaskConfig::new("db", "noop")
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
        let web_on_node_b = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.task_config_id == web_id && alloc.node_id == "node-b")
            .count();
        assert_eq!(web_on_node_a + web_on_node_b, 2);

        let db_on_node_b = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.task_config_id == db_id && alloc.node_id == "node-b")
            .count();
        assert_eq!(db_on_node_b, 1);

        assert!(outcome.deltas.len() >= 1);
        let total_added: usize = outcome.deltas.iter().map(|delta| delta.added.len()).sum();
        let total_removed: usize = outcome.deltas.iter().map(|delta| delta.removed.len()).sum();
        assert_eq!(total_added, 3);
        assert_eq!(total_removed, 0);

        Ok(())
    }

    #[test]
    fn reschedule_only_places_missing_allocations() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;

        let job = JobConfig {
            name: "resched-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(3),
                ..TaskConfig::new("batch", "noop")
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

        let node_a_added = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.node_id == "node-a")
            .count();
        let node_b_added = outcome
            .allocations
            .iter()
            .filter(|alloc| alloc.node_id == "node-b")
            .count();
        assert_eq!(node_a_added + node_b_added, 2);

        Ok(())
    }

    #[test]
    fn count_per_attribute_places_allocations_per_group() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;

        let job = JobConfig {
            name: "per-attr-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(2, "region"),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 4);

        let mut counts: HashMap<String, usize> = HashMap::new();
        for alloc in &outcome.allocations {
            let node = store.get_node(&alloc.node_id)?;
            let region = node
                .config_attrs
                .get("region")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            *counts.entry(region).or_default() += 1;
        }

        assert_eq!(counts.get("east"), Some(&2));
        assert_eq!(counts.get("west"), Some(&2));

        Ok(())
    }

    #[test]
    fn count_per_attribute_reschedules_within_group() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;

        let job = JobConfig {
            name: "per-attr-resched".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(2, "region"),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;

        for _ in 0..2 {
            store.place_alloc("node-a", task_id)?;
        }
        for _ in 0..2 {
            store.place_alloc("node-c", task_id)?;
        }

        store.mark_node_lost("node-a")?;
        store.remove_allocs_for_node("node-a")?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;

        assert_eq!(outcome.allocations.len(), 2);
        for alloc in &outcome.allocations {
            let node = store.get_node(&alloc.node_id)?;
            let region = node
                .config_attrs
                .get("region")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            assert_eq!(region, "east");
        }

        Ok(())
    }

    #[test]
    fn count_per_attribute_respects_constraints() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "region": "east", "role": "db" }),
        ))?;
        store.insert_node(make_node_msg(
            "node-b",
            json!({ "region": "east", "role": "web" }),
        ))?;
        store.insert_node(make_node_msg(
            "node-c",
            json!({ "region": "west", "role": "db" }),
        ))?;

        let job = JobConfig {
            name: "per-attr-constraints".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(1, "region"),
                constraints: json!({ "role": "db" }),
                ..TaskConfig::new("db", "noop")
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
            let role = node
                .config_attrs
                .get("role")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            assert_eq!(role, "db");
        }

        Ok(())
    }

    #[test]
    fn reschedule_excludes_failed_node() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;

        let job = JobConfig {
            name: "resched-exclude".into(),
            tasks: vec![TaskConfig::new("batch", "noop")],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let excluded = HashSet::from(["node-a".to_string()]);
        let outcome = scheduler.plan_and_apply_with_exclusions(
            &job_config_id,
            PlanMode::Reschedule,
            Some(&excluded),
        )?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-b");

        Ok(())
    }

    #[test]
    fn errors_when_constraints_cannot_be_satisfied() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "tier": "edge" })))?;

        let job = JobConfig {
            name: "unschedulable".into(),
            tasks: vec![TaskConfig {
                constraints: json!({ "role": "db" }),
                ..TaskConfig::new("analytics", "noop")
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

        let constraint_set = ConstraintSet::new(&constraints)?;
        assert!(constraint_set.is_satisfied(&node, None));

        let mismatched = json!({ "role": "web", "arch": "arm64", "zone": "us-east" });
        let mismatched_set = ConstraintSet::new(&mismatched)?;
        assert!(!mismatched_set.is_satisfied(&node, None));

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
                count: TaskCount::new(2),
                constraints: json!({ "role": "db" }),
                ..TaskConfig::new("db", "noop")
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
            let constraint_set = ConstraintSet::new(&json!({ "role": "db" }))?;
            assert!(constraint_set.is_satisfied(&node, None));
        }

        Ok(())
    }

    #[test]
    fn distinct_constraints_spread_allocations() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "zone": "us-east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "zone": "us-west" })))?;

        let job = JobConfig {
            name: "distinct-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                constraints: json!([{ "key": "zone", "operator": "distinct" }]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        let node_ids: HashSet<_> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.node_id.as_str())
            .collect();
        assert_eq!(node_ids.len(), 2);

        Ok(())
    }

    #[test]
    fn apply_time_constraints_are_rechecked() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;

        let job = JobConfig {
            name: "apply-constraints".into(),
            tasks: vec![TaskConfig {
                constraints: json!([{ "key": "role", "value": "db" }]),
                ..TaskConfig::new("db", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "db")?;

        let err = store
            .apply_allocation_plan(&[(String::from("node-a"), task_id)])
            .expect_err("expected constraint failure");
        assert!(err.to_string().contains("does not satisfy constraints"));

        Ok(())
    }

    #[test]
    fn permit_allows_single_restriction() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "restriction": { "foo": "bar" } }),
        ))?;

        let job = JobConfig {
            name: "permit-single".into(),
            tasks: vec![TaskConfig {
                permit: json!([{ "key": "foo", "operator": "equal", "value": "bar" }]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-a");
        Ok(())
    }

    #[test]
    fn permit_handles_multiple_restrictions_pass_and_fail() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "restriction": { "foo": "bar", "number": 4 } }),
        ))?;

        let passing_job = JobConfig {
            name: "permit-multi-pass".into(),
            tasks: vec![TaskConfig {
                permit: json!([
                    { "key": "foo", "operator": "equal", "value": "bar" },
                    { "key": "number", "operator": "equal", "value": 4 }
                ]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&passing_job, &[dummy_submission()])?;
        let pass_job_config_id = store.get_job_config(&passing_job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let pass_outcome = scheduler.plan_and_apply(&pass_job_config_id, PlanMode::Fresh)?;
        assert_eq!(pass_outcome.allocations.len(), 1);

        let failing_job = JobConfig {
            name: "permit-multi-fail".into(),
            tasks: vec![TaskConfig {
                permit: json!([
                    { "key": "foo", "operator": "equal", "value": "bar" },
                    { "key": "number", "operator": "equal", "value": 5 }
                ]),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&failing_job, &[dummy_submission()])?;
        let fail_job_config_id = store.get_job_config(&failing_job.name)?.id();
        let fail_err = scheduler
            .plan_and_apply(&fail_job_config_id, PlanMode::Fresh)
            .expect_err("expected permit failure");
        assert!(fail_err
            .to_string()
            .contains("no available nodes matching constraints, permit, or resources"));

        Ok(())
    }

    #[test]
    fn permit_defaults_to_default_pool_when_no_restrictions() -> R<()> {
        let store = setup_store()?;
        let mut node_default = make_node_msg("node-a", json!({}));
        node_default.pool = "default".into();
        store.insert_node(node_default)?;
        let mut node_other = make_node_msg("node-b", json!({}));
        node_other.pool = "gpu".into();
        store.insert_node(node_other)?;

        let job = JobConfig {
            name: "permit-default".into(),
            tasks: vec![TaskConfig::new("api", "noop")],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-a");

        Ok(())
    }

    #[test]
    fn resource_limits_prevent_overcommit() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg_with_sources(
            "node-a",
            json!({}),
            json!({
                "memory_total": "524288",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            json!({}),
        ))?;

        let job = JobConfig {
            name: "mem-limited".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(3),
                resources: json!({ "memory": 256 }),
                ..TaskConfig::new("worker", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;

        let err = scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .expect_err("expected resource scheduling failure");

        assert!(err
            .to_string()
            .contains("no available nodes matching constraints, permit, or resources"));

        Ok(())
    }

    #[test]
    fn max_allocations_limits_node_capacity() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg_with_sources(
            "node-a",
            json!({ "maxAllocations": 1 }),
            json!({
                "memory_total": "1048576",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            json!({}),
        ))?;

        let job = JobConfig {
            name: "alloc-limit".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 128 }),
                ..TaskConfig::new("worker", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;

        let err = scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .expect_err("expected max allocations failure");

        assert!(err
            .to_string()
            .contains("no available nodes matching constraints, permit, or resources"));

        Ok(())
    }

    #[test]
    fn spread_limits_nodes_considered_by_count() -> R<()> {
        let mut selected = HashSet::new();
        for _ in 0..20 {
            let store =
                setup_store_with_scheduler("spread", Some(json!({ "nodes_considered": 2 })))?;
            store.insert_node(make_node_msg("node-a", json!({})))?;
            store.insert_node(make_node_msg("node-b", json!({})))?;
            store.insert_node(make_node_msg("node-c", json!({})))?;

            let job = JobConfig {
                name: "spread-considered".into(),
                tasks: vec![TaskConfig::new("api", "noop")],
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
            selected.insert(outcome.allocations[0].node_id.clone());
        }

        assert!(selected.contains("node-b"));
        assert!(selected.contains("node-c"));

        Ok(())
    }

    #[test]
    fn bin_pack_limits_nodes_considered_by_percentage() -> R<()> {
        let mut selected = HashSet::new();
        for _ in 0..20 {
            let store =
                setup_store_with_scheduler("bin_pack", Some(json!({ "nodes_considered": "50%" })))?;
            store.insert_node(make_node_msg("node-a", json!({})))?;
            store.insert_node(make_node_msg("node-b", json!({})))?;
            store.insert_node(make_node_msg("node-c", json!({})))?;

            let job = JobConfig {
                name: "binpack-considered".into(),
                tasks: vec![TaskConfig::new("worker", "noop")],
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
            selected.insert(outcome.allocations[0].node_id.clone());
        }

        assert!(selected.contains("node-b"));
        assert!(selected.contains("node-c"));

        Ok(())
    }

    #[test]
    fn nodes_considered_randomizes_subset_across_runs() -> R<()> {
        let store = setup_store_with_scheduler("spread", Some(json!({ "nodes_considered": 2 })))?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;
        store.insert_node(make_node_msg("node-c", json!({})))?;
        store.insert_node(make_node_msg("node-d", json!({})))?;

        let scheduler = Scheduler::new(&store)?;
        let nodes = scheduler.available_nodes()?;
        let eligible = nodes.iter().collect::<Vec<_>>();
        let setting = NodesConsidered::Count(2);

        let mut subsets = HashSet::new();
        let mut rng = StdRng::seed_from_u64(13);
        for _ in 0..20 {
            let subset = scheduler.apply_nodes_considered_with_setting(
                eligible.clone(),
                Some(&setting),
                &mut rng,
            )?;
            let mut ids = subset
                .iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            ids.sort();
            subsets.insert(ids);
        }

        assert!(
            subsets.len() > 1,
            "expected nodes_considered to vary eligible subset"
        );

        Ok(())
    }

    #[test]
    fn group_tasks_co_locate_on_same_node() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;

        let job = JobConfig {
            name: "grouped".into(),
            tasks: vec![
                TaskConfig {
                    group: "default".into(),
                    ..TaskConfig::new("api", "noop")
                },
                TaskConfig {
                    group: "default".into(),
                    ..TaskConfig::new("cache", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        let node_ids: HashSet<_> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.node_id.as_str())
            .collect();
        assert_eq!(node_ids.len(), 1);

        Ok(())
    }

    #[test]
    fn groups_allow_distinct_constraints() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "role": "db" })))?;

        let job = JobConfig {
            name: "group-constraints".into(),
            tasks: vec![
                TaskConfig {
                    group: "web".into(),
                    constraints: json!({ "role": "web" }),
                    ..TaskConfig::new("frontend", "noop")
                },
                TaskConfig {
                    group: "db".into(),
                    constraints: json!({ "role": "db" }),
                    ..TaskConfig::new("database", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        let mut allocations_by_task = HashMap::new();
        for allocation in &outcome.allocations {
            allocations_by_task.insert(allocation.task_config_id, allocation.node_id.clone());
        }
        let node_frontend = allocations_by_task
            .get(&task_id_for(&store, &job_config_id, "frontend")?)
            .cloned()
            .unwrap();
        let node_database = allocations_by_task
            .get(&task_id_for(&store, &job_config_id, "database")?)
            .cloned()
            .unwrap();
        assert_ne!(node_frontend, node_database);

        Ok(())
    }

    #[test]
    fn dag_batch_success_allows_service_within_group() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let job = JobConfig {
            name: "dag-group".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    group: "app".into(),
                    ..TaskConfig::new("build", "exec")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    group: "app".into(),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "build".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    ..TaskConfig::new("api", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        let node_ids: HashSet<_> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.node_id.as_str())
            .collect();
        assert_eq!(node_ids.len(), 1);

        Ok(())
    }

    #[test]
    fn dag_batch_success_across_groups() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "prep" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "role": "app" })))?;

        let job = JobConfig {
            name: "dag-cross".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    group: "prep".into(),
                    constraints: json!({ "role": "prep" }),
                    ..TaskConfig::new("seed", "exec")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    group: "app".into(),
                    constraints: json!({ "role": "app" }),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "seed".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    ..TaskConfig::new("web", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        let mut roles = HashSet::new();
        for allocation in &outcome.allocations {
            let node = store.get_node(&allocation.node_id)?;
            let role = node
                .config_attrs
                .get("role")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            roles.insert(role);
        }
        assert!(roles.contains("prep"));
        assert!(roles.contains("app"));

        Ok(())
    }

    #[test]
    fn required_counts_for_mode_reschedule_only_plans_missing() {
        let tasks = vec![
            (1, TaskConfig::new("api", "noop"), 3),
            (2, TaskConfig::new("worker", "noop"), 2),
        ];
        let allocations_by_node = HashMap::from([
            (
                "node-a".to_string(),
                vec![
                    Allocation {
                        id: 1,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                    },
                    Allocation {
                        id: 2,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 2,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                    },
                ],
            ),
            (
                "node-b".to_string(),
                vec![Allocation {
                    id: 3,
                    job_id: "other".into(),
                    job_config_id: "cfg-2".into(),
                    task_config_id: 1,
                    node_id: "node-b".into(),
                    status: "running".into(),
                    resources: json!({}),
                }],
            ),
        ]);

        let required = Scheduler::required_counts_for_mode(
            &tasks,
            PlanMode::Reschedule,
            "cfg-1",
            &allocations_by_node,
        );

        assert_eq!(required.get(&1), Some(&2));
        assert_eq!(required.get(&2), Some(&1));
    }

    #[test]
    fn existing_alloc_counts_by_task_node_indexes_by_task_and_node() {
        let allocations_by_node = HashMap::from([
            (
                "node-a".to_string(),
                vec![
                    Allocation {
                        id: 1,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                    },
                    Allocation {
                        id: 2,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                    },
                    Allocation {
                        id: 3,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 2,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                    },
                ],
            ),
            (
                "node-b".to_string(),
                vec![
                    Allocation {
                        id: 4,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-b".into(),
                        status: "running".into(),
                        resources: json!({}),
                    },
                    Allocation {
                        id: 5,
                        job_id: "other".into(),
                        job_config_id: "cfg-other".into(),
                        task_config_id: 1,
                        node_id: "node-b".into(),
                        status: "running".into(),
                        resources: json!({}),
                    },
                ],
            ),
        ]);

        let indexed = Scheduler::existing_alloc_counts_by_task_node("cfg-1", &allocations_by_node);

        assert_eq!(indexed.get(&(1, "node-a".to_string())), Some(&2));
        assert_eq!(indexed.get(&(1, "node-b".to_string())), Some(&1));
        assert_eq!(indexed.get(&(2, "node-a".to_string())), Some(&1));
        assert!(indexed.get(&(2, "node-b".to_string())).is_none());
    }

    #[test]
    fn ready_tasks_allows_dependent_service_after_batch_chain() -> R<()> {
        let build = TaskConfig {
            lifecycle: TaskLifecycle::Batch,
            ..TaskConfig::new("build", "exec")
        };
        let seed = TaskConfig {
            lifecycle: TaskLifecycle::Batch,
            depends_on: vec![crate::structs::TaskDependency {
                task: "build".into(),
                condition: TaskDependencyCondition::Success,
            }],
            ..TaskConfig::new("seed", "exec")
        };
        let api = TaskConfig {
            depends_on: vec![crate::structs::TaskDependency {
                task: "seed".into(),
                condition: TaskDependencyCondition::Success,
            }],
            ..TaskConfig::new("api", "noop")
        };

        let ready = Scheduler::ready_tasks(
            vec![(1, build, 1), (2, seed, 1), (3, api, 1)],
            &HashMap::new(),
        )?;

        assert_eq!(ready.len(), 3);
        Ok(())
    }

    #[test]
    fn multiple_jobs_schedule_without_resource_oversubscription() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg_with_sources(
            "node-a",
            json!({}),
            json!({
                "memory_total": "2097152",
                "cpus": { "cpu-1": { "frequency": "2000" } }
            }),
            json!({}),
        ))?;
        store.insert_node(make_node_msg_with_sources(
            "node-b",
            json!({}),
            json!({
                "memory_total": "2097152",
                "cpus": { "cpu-1": { "frequency": "2000" } }
            }),
            json!({}),
        ))?;

        let job_one = JobConfig {
            name: "job-one".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 768 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        let job_two = JobConfig {
            name: "job-two".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    group: "prep".into(),
                    resources: json!({ "memory": 512 }),
                    ..TaskConfig::new("prepare", "exec")
                },
                TaskConfig {
                    group: "app".into(),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "prepare".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    resources: json!({ "memory": 768 }),
                    ..TaskConfig::new("worker", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job_one, &[dummy_submission()])?;
        store.handle_job_submit(&job_two, &[dummy_submission()])?;

        let scheduler = Scheduler::new(&store)?;
        let outcome_one =
            scheduler.plan_and_apply(&store.get_job_config("job-one")?.id(), PlanMode::Fresh)?;
        let outcome_two =
            scheduler.plan_and_apply(&store.get_job_config("job-two")?.id(), PlanMode::Fresh)?;

        assert_eq!(outcome_one.allocations.len(), 2);
        assert_eq!(outcome_two.allocations.len(), 2);

        let ledger = store.get_ledger()?;
        for entry in ledger {
            let total_memory: f64 = entry
                .allocations
                .iter()
                .map(|alloc| {
                    alloc
                        .resources
                        .get("memory")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0)
                })
                .sum();
            assert!(
                total_memory <= 2048.0,
                "node {} exceeded memory budget: {}",
                entry.node.id,
                total_memory
            );
        }

        Ok(())
    }

    #[test]
    fn spread_prefers_affinity_over_load_when_weighted() -> R<()> {
        let store = setup_store_with_scheduler(
            "spread",
            Some(json!({
                "weights": {
                    "affinities": 2.0,
                    "spread": 1.0
                }
            })),
        )?;

        store.insert_node(make_node_msg("node-a", json!({ "zone": "us-east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "zone": "us-west" })))?;

        let load_job = JobConfig {
            name: "load-job".into(),
            tasks: vec![TaskConfig::new("load", "noop")],
            version: None,
        };
        store.handle_job_submit(&load_job, &[dummy_submission()])?;
        let load_job_config_id = store.get_job_config(&load_job.name)?.id();
        let load_task_id = task_id_for(&store, &load_job_config_id, "load")?;

        for _ in 0..3 {
            store.place_alloc("node-a", load_task_id)?;
        }

        let job = JobConfig {
            name: "affinity-job".into(),
            tasks: vec![TaskConfig {
                affinities: json!({ "zone": "us-east" }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-a");

        Ok(())
    }
}
