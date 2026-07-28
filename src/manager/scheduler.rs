// Scheduler planning entry points pass many parameters (nodes, policy, caches,
// epochs, …) by design; bundling them into structs would not improve clarity.
#![allow(clippy::too_many_arguments)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde_json::Value;

use libloading::Library;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};

use crate::constraints::{AffinitySet, ConstraintSet, DistinctTracker, PermitSet};
use crate::prelude::*;
use crate::resources::{ResourcePolicy, ResourcePool, ResourceRequest, ResourceValue};
use crate::scheduling_index::{IndexMode, IndexQueryOutcome};
use crate::storage::errors::StorageError;
use crate::storage::store::{PlanApplyOutcome, SharedNodeAllocations, Store};
use crate::structs::{
    Allocation, JobConfig, Node, TaskConfig, TaskDependencyCondition, TaskLifecycle,
    TaskStatusState,
};
use policy::score_node_for_context;
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

/// How candidate construction went for one planned task — the scheduling
/// index's explain record, surfaced by `job plan` (docs/scheduling-indexes.md).
#[derive(Debug, Clone)]
pub struct TaskIndexUsage {
    pub task_name: String,
    /// The index that served candidates, `None` when the scan ran instead.
    pub index: Option<String>,
    /// `"on"` or `"shadow"` when an index participated.
    pub mode: Option<&'static str>,
    /// Candidate count handed to placement.
    pub candidates: usize,
    /// True when the candidate list was cut to the budget (fit-best-first).
    pub truncated: bool,
    /// Times placement exhausted truncated candidates and widened to a scan.
    pub refills: u32,
    /// True when the scan had to serve after the index came up short.
    pub fell_back: bool,
    /// Shadow mode only: did the index's candidate set match the scan's?
    pub shadow_agreement: Option<bool>,
    /// Why no index served, when one was expected to (`None` otherwise).
    pub reason: Option<String>,
}

impl TaskIndexUsage {
    fn scan(task_name: &str) -> Self {
        TaskIndexUsage {
            task_name: task_name.to_string(),
            index: None,
            mode: None,
            candidates: 0,
            truncated: false,
            refills: 0,
            fell_back: false,
            shadow_agreement: None,
            reason: None,
        }
    }
}

/// `build_plan`'s result: the placements plus the per-task index report.
struct BuiltPlan {
    placements: Vec<PlannedAllocation>,
    index_report: Vec<TaskIndexUsage>,
}

/// One plan's capacity state, taken from the read model in a single lock
/// acquisition: the allocations the plan places against, the cached free
/// pools, and the nodes whose allocation set the plan itself changed by
/// dropping the job's own allocations (a Fresh plan replaces them) — the
/// ledger recomputes exactly those nodes from scratch.
struct CapacitySnapshot {
    allocations_by_node: SharedNodeAllocations,
    pools: HashMap<String, ResourcePool>,
    freed_nodes: HashSet<String>,
}

#[derive(Debug, Clone, Copy)]
enum NodesConsidered {
    Count(u64),
    Percent(f64),
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
    index_usage: TaskIndexUsage,
}

#[derive(Debug, Clone, Default)]
struct CommonResourceDemand {
    memory: f64,
    cpu: f64,
    cores: f64,
    allocations: f64,
}

#[derive(Debug, Clone, Default)]
struct CommonResourceAvailability {
    memory: f64,
    cpu: f64,
    cores: f64,
    allocations: f64,
}

#[derive(Debug, Clone)]
struct NodeScoringState {
    assigned_count: usize,
    same_task_allocations: usize,
    same_group_allocations: usize,
    task_presence: HashSet<i64>,
    base_affinity: f64,
    can_fit_task_request: bool,
    common_resources: CommonResourceAvailability,
    common_feasibility_bitmap: u8,
}

#[derive(Debug, Clone)]
struct TaskAffinityCache {
    preferences: Vec<(i64, f64)>,
    total_weight: f64,
}

#[derive(Debug, Clone)]
struct PlanScoringCache {
    node_states: HashMap<String, NodeScoringState>,
    affinity: TaskAffinityCache,
    common_demand: CommonResourceDemand,
    group_name: Option<String>,
}

impl PlanScoringCache {
    const COMMON_MEMORY: u8 = 1 << 0;
    const COMMON_CPU: u8 = 1 << 1;
    const COMMON_CORES: u8 = 1 << 2;
    const COMMON_ALLOCATIONS: u8 = 1 << 3;

    fn from_plan(
        candidates: &[&Node],
        task_id: i64,
        task_config: &TaskConfig,
        affinity_set: &AffinitySet,
        job_config_id: &str,
        allocations_by_node: &SharedNodeAllocations,
        task_name_to_id: &HashMap<String, i64>,
        task_id_to_group: &HashMap<i64, String>,
        planned_task_allocations: &HashMap<String, Vec<i64>>,
        planned: &HashMap<String, usize>,
        resource_ledger: &ResourceLedger,
        task_request: &ResourceRequest,
    ) -> Self {
        let group_name = task_id_to_group.get(&task_id).cloned();
        let affinity = TaskAffinityCache::new(task_config, task_name_to_id);
        let common_demand = CommonResourceDemand::from_request(task_request);

        let node_states = candidates
            .iter()
            .map(|node| {
                let node_id = node.id.clone();
                let existing_allocs = allocations_by_node.get(&node_id);
                let planned_ids = planned_task_allocations
                    .get(&node_id)
                    .cloned()
                    .unwrap_or_default();

                let mut task_presence = HashSet::new();
                let mut same_task_allocations = 0usize;
                let mut same_group_allocations = 0usize;
                let assigned_count = existing_allocs.map_or(0, |allocs| allocs.len())
                    + planned.get(&node_id).copied().unwrap_or_default();

                if let Some(allocs) = existing_allocs {
                    for alloc in allocs.iter() {
                        if alloc.job_config_id != job_config_id {
                            continue;
                        }
                        task_presence.insert(alloc.task_config_id);
                        if alloc.task_config_id == task_id {
                            same_task_allocations += 1;
                        }
                        if group_name.as_ref().is_some_and(|group| {
                            task_id_to_group
                                .get(&alloc.task_config_id)
                                .is_some_and(|alloc_group| alloc_group == group)
                        }) {
                            same_group_allocations += 1;
                        }
                    }
                }

                for planned_task_id in &planned_ids {
                    task_presence.insert(*planned_task_id);
                    if *planned_task_id == task_id {
                        same_task_allocations += 1;
                    }
                    if group_name.as_ref().is_some_and(|group| {
                        task_id_to_group
                            .get(planned_task_id)
                            .is_some_and(|alloc_group| alloc_group == group)
                    }) {
                        same_group_allocations += 1;
                    }
                }

                let (common_resources, common_feasibility_bitmap, can_fit_task_request) =
                    resource_ledger
                        .pools
                        .get(&node_id)
                        .map(|pool| {
                            let common_resources = CommonResourceAvailability::from_pool(pool);
                            let bitmap = common_resources.feasibility_bitmap(&common_demand);
                            (common_resources, bitmap, pool.can_fit(task_request))
                        })
                        .unwrap_or_else(|| {
                            (
                                CommonResourceAvailability::default(),
                                0,
                                resource_ledger.can_fit(&node_id, task_request),
                            )
                        });

                (
                    node_id,
                    NodeScoringState {
                        assigned_count,
                        same_task_allocations,
                        same_group_allocations,
                        task_presence,
                        base_affinity: affinity_set.score(node),
                        can_fit_task_request,
                        common_resources,
                        common_feasibility_bitmap,
                    },
                )
            })
            .collect();

        Self {
            node_states,
            affinity,
            common_demand,
            group_name,
        }
    }

    fn can_fit_task_request(&self, node_id: &str) -> bool {
        self.node_states
            .get(node_id)
            .map(|state| {
                let required_bitmap = self.required_common_bitmap();
                let common_feasible =
                    (state.common_feasibility_bitmap & required_bitmap) == required_bitmap;
                common_feasible && state.can_fit_task_request
            })
            .unwrap_or(false)
    }

    fn update_after_placement(
        &mut self,
        node_id: &str,
        task_id: i64,
        task_id_to_group: &HashMap<i64, String>,
        resource_ledger: &ResourceLedger,
        task_request: &ResourceRequest,
    ) {
        if let Some(state) = self.node_states.get_mut(node_id) {
            state.assigned_count += 1;
            state.same_task_allocations += 1;
            if self.group_name.as_ref().is_some_and(|group| {
                task_id_to_group
                    .get(&task_id)
                    .is_some_and(|task_group| task_group == group)
            }) {
                state.same_group_allocations += 1;
            }
            state.task_presence.insert(task_id);
            state.common_resources.consume(&self.common_demand);
            state.common_feasibility_bitmap = state
                .common_resources
                .feasibility_bitmap(&self.common_demand);
            state.can_fit_task_request = resource_ledger.can_fit(node_id, task_request);
        }
    }

    fn required_common_bitmap(&self) -> u8 {
        let mut bitmap = 0u8;
        if self.common_demand.memory > 0.0 {
            bitmap |= Self::COMMON_MEMORY;
        }
        if self.common_demand.cpu > 0.0 {
            bitmap |= Self::COMMON_CPU;
        }
        if self.common_demand.cores > 0.0 {
            bitmap |= Self::COMMON_CORES;
        }
        if self.common_demand.allocations > 0.0 {
            bitmap |= Self::COMMON_ALLOCATIONS;
        }
        bitmap
    }
}

impl CommonResourceDemand {
    fn from_request(request: &ResourceRequest) -> Self {
        let mut demand = CommonResourceDemand::default();
        for (name, value) in &request.values {
            if let ResourceValue::Amount(amount) = value {
                match name.as_str() {
                    "memory" => demand.memory = *amount,
                    "cpu" => demand.cpu = *amount,
                    "cores" => demand.cores = *amount,
                    "allocations" => demand.allocations = *amount,
                    _ => {}
                }
            }
        }
        demand
    }
}

impl CommonResourceAvailability {
    fn from_pool(pool: &ResourcePool) -> Self {
        fn amount(values: &HashMap<String, ResourceValue>, key: &str) -> f64 {
            match values.get(key) {
                Some(ResourceValue::Amount(value)) => *value,
                _ => 0.0,
            }
        }

        CommonResourceAvailability {
            memory: amount(&pool.values, "memory"),
            cpu: amount(&pool.values, "cpu"),
            cores: amount(&pool.values, "cores"),
            allocations: amount(&pool.values, "allocations"),
        }
    }

    fn consume(&mut self, demand: &CommonResourceDemand) {
        self.memory -= demand.memory;
        self.cpu -= demand.cpu;
        self.cores -= demand.cores;
        self.allocations -= demand.allocations;
    }

    fn feasibility_bitmap(&self, demand: &CommonResourceDemand) -> u8 {
        let mut bitmap = 0u8;
        if self.memory >= demand.memory {
            bitmap |= PlanScoringCache::COMMON_MEMORY;
        }
        if self.cpu >= demand.cpu {
            bitmap |= PlanScoringCache::COMMON_CPU;
        }
        if self.cores >= demand.cores {
            bitmap |= PlanScoringCache::COMMON_CORES;
        }
        if self.allocations >= demand.allocations {
            bitmap |= PlanScoringCache::COMMON_ALLOCATIONS;
        }
        bitmap
    }
}

impl TaskAffinityCache {
    fn new(task_config: &TaskConfig, task_name_to_id: &HashMap<String, i64>) -> Self {
        let mut preferences = Vec::new();
        let mut total_weight = 0.0;
        for (task_name, multiplier) in task_colocation_preferences(&task_config.affinities) {
            if let Some(task_id) = task_name_to_id.get(task_name) {
                preferences.push((*task_id, multiplier));
                total_weight += multiplier.abs();
            }
        }

        TaskAffinityCache {
            preferences,
            total_weight,
        }
    }

    fn colocation_score(&self, state: &NodeScoringState) -> f64 {
        if self.preferences.is_empty() || self.total_weight == 0.0 {
            return 0.0;
        }

        let matched = self
            .preferences
            .iter()
            .filter_map(|(task_id, multiplier)| {
                state.task_presence.contains(task_id).then_some(*multiplier)
            })
            .sum::<f64>();
        matched / self.total_weight
    }
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
    /// Per-task scheduling-index usage (docs/scheduling-indexes.md).
    pub index_report: Vec<TaskIndexUsage>,
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
        Ok(self.plan_only_with_report(job_config_id, mode)?.0)
    }

    /// `plan_only`, additionally returning the per-task scheduling-index
    /// usage report for the `job plan` explain surface.
    #[instrument(skip_all)]
    pub fn plan_only_with_report(
        &self,
        job_config_id: &str,
        mode: PlanMode,
    ) -> R<(Vec<PlannedAllocation>, Vec<TaskIndexUsage>)> {
        let started = std::time::Instant::now();
        let mode_label = mode.as_str();
        info!(%job_config_id, mode = mode_label, "Starting scheduler plan_only");
        let nodes = self.available_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let all_tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?;
        let mut tasks = self.tasks_for_plan_from_store(job_config_id)?;
        if matches!(mode, PlanMode::Reschedule) {
            if let Some(job_id) = self.store.job_id_for_job_config(job_config_id)? {
                Self::apply_slot_staged_counts(self.store, &job_id, job_config_id, &mut tasks)?;
            }
        }

        let drop_own = |allocation: &Allocation| allocation.job_config_id == job_config_id;
        let snapshot = self.capacity_snapshot(if matches!(mode, PlanMode::Fresh) {
            Some(&drop_own)
        } else {
            None
        })?;

        let required_counts = Self::required_counts_for_mode(
            &tasks,
            mode.clone(),
            job_config_id,
            &snapshot.allocations_by_node,
        );

        let BuiltPlan {
            placements,
            index_report,
        } = self.build_plan(
            job_config_id,
            nodes,
            tasks,
            all_tasks,
            required_counts,
            snapshot,
            mode,
            None,
        )?;
        info!(
            %job_config_id,
            mode = mode_label,
            planned_allocations = placements.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Completed scheduler plan_only"
        );
        Ok((placements, index_report))
    }

    /// Takes the plan's capacity snapshot from the read model, optionally
    /// dropping (and recording the nodes of) the allocations this plan
    /// replaces.
    fn capacity_snapshot(
        &self,
        should_drop: Option<&dyn Fn(&Allocation) -> bool>,
    ) -> R<CapacitySnapshot> {
        let (mut allocations_by_node, pools) = self.store.plan_capacity_snapshot()?;
        let mut freed_nodes = HashSet::new();
        if let Some(should_drop) = should_drop {
            for (node_id, allocations) in allocations_by_node.iter_mut() {
                // `Arc::make_mut` copies the vector even when nothing would
                // be dropped, so check read-only first: only the planned
                // job's own nodes pay for a copy.
                if !allocations.iter().any(should_drop) {
                    continue;
                }
                Arc::make_mut(allocations).retain(|allocation| !should_drop(allocation));
                freed_nodes.insert(node_id.clone());
            }
        }
        Ok(CapacitySnapshot {
            allocations_by_node,
            pools,
            freed_nodes,
        })
    }

    #[instrument(skip_all)]
    pub fn plan_for_config(
        &self,
        job_config: &JobConfig,
        mode: PlanMode,
        clear_job_id: Option<&str>,
    ) -> R<PlanConfigOutcome> {
        let started = std::time::Instant::now();
        let mode_label = mode.as_str();
        let nodes = self.available_nodes()?;
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        let status_map = HashMap::new();
        let mut tasks = Self::tasks_for_plan(job_config, &status_map)?;
        // Keyed tasks plan against live instances, not count.num, even for
        // not-yet-submitted configs (a fresh job simply has zero instances).
        Self::apply_keyed_instance_counts(self.store, &job_config.name, &mut tasks)?;
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

        let drop_cleared =
            |allocation: &Allocation| Some(allocation.job_id.as_str()) == clear_job_id;
        let snapshot = self.capacity_snapshot(clear_job_id.is_some().then_some(&drop_cleared))?;

        let required_counts = Self::required_counts_for_mode(
            &tasks,
            mode.clone(),
            &job_config.id(),
            &snapshot.allocations_by_node,
        );

        let BuiltPlan {
            placements,
            index_report,
        } = self.build_plan(
            &job_config.id(),
            nodes,
            tasks,
            all_tasks,
            required_counts,
            snapshot,
            mode,
            None,
        )?;
        info!(
            job_id = %job_config.name,
            job_config_id = %job_config.id(),
            mode = mode_label,
            planned_allocations = placements.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Completed scheduler plan_for_config"
        );

        Ok(PlanConfigOutcome {
            allocations: placements,
            task_lookup,
            index_report,
        })
    }

    #[instrument(skip_all)]
    pub fn plan_and_apply(&self, job_config_id: &str, mode: PlanMode) -> R<PlanOutcome> {
        self.plan_and_apply_with_exclusions(job_config_id, mode, None)
    }

    #[instrument(skip_all)]
    pub fn plan_and_apply_with_targets(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        task_targets: &HashMap<i64, u32>,
    ) -> R<PlanOutcome> {
        self.plan_and_apply_with_targets_and_exclusions(job_config_id, mode, task_targets, None)
    }

    pub fn plan_and_apply_with_targets_and_exclusions(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        task_targets: &HashMap<i64, u32>,
        excluded_nodes: Option<&HashSet<String>>,
    ) -> R<PlanOutcome> {
        self.plan_and_apply_internal(job_config_id, mode, excluded_nodes, Some(task_targets))
    }

    pub fn plan_and_apply_with_exclusions(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        excluded_nodes: Option<&HashSet<String>>,
    ) -> R<PlanOutcome> {
        self.plan_and_apply_internal(job_config_id, mode, excluded_nodes, None)
    }

    fn plan_and_apply_internal(
        &self,
        job_config_id: &str,
        mode: PlanMode,
        excluded_nodes: Option<&HashSet<String>>,
        task_targets: Option<&HashMap<i64, u32>>,
    ) -> R<PlanOutcome> {
        let started = std::time::Instant::now();
        info!(%job_config_id, mode = mode.as_str(), target_override = task_targets.is_some(), "Starting scheduler plan_and_apply");
        let mut nodes = self.available_nodes()?;
        if let Some(excluded) = excluded_nodes {
            nodes.retain(|node| !excluded.contains(&node.id));
        }
        if nodes.is_empty() {
            bail!("No nodes available to place allocations");
        }

        // Optimistic-concurrency fence, sampled HERE — before the capacity
        // snapshot below (`capacity_snapshot`) that `build_plan` places
        // against.
        //
        // Every transaction that mutates a scope's allocations bumps that
        // scope's epoch (`bump_epochs_for_allocations_tx`), and
        // `apply_plan_submission` re-reads these epochs inside its write
        // transaction and refuses the plan if any of them moved. So what the
        // epochs fence is: "nothing changed the allocations I planned against
        // between the moment I read them and the moment I write". That only
        // holds if the sample is taken no later than the capacity read. If it
        // were taken after planning (as it once was), a competing plan that
        // applied in the window between the capacity read and the sample would
        // already have bumped the epoch, so this plan would sample the
        // *post-bump* value, pass the compare-and-set, and commit placements
        // computed against capacity that no longer exists — oversubscribing the
        // node. Sampling first makes that case fail the compare-and-set
        // instead, and the caller retries against a fresh snapshot.
        //
        // The affected node set is only known after planning, so sample every
        // candidate node now and select the affected subset at submit time —
        // never re-read. That is one batched query rather than one per node;
        // see `Store::allocation_version_vector_for_candidates`.
        let candidate_node_ids: Vec<&str> = nodes.iter().map(|node| node.id.as_str()).collect();
        let candidate_versions = self
            .store
            .allocation_version_vector_for_candidates(job_config_id, &candidate_node_ids)?;

        let all_tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?;
        let mut tasks = self.tasks_for_plan_from_store(job_config_id)?;
        if task_targets.is_none() && matches!(mode, PlanMode::Reschedule) {
            if let Some(job_id) = self.store.job_id_for_job_config(job_config_id)? {
                Self::apply_slot_staged_counts(self.store, &job_id, job_config_id, &mut tasks)?;
            }
        }

        // A `Fresh` plan replaces the job's allocations wholesale: whichever
        // handler asked for it either already dropped the previous ones
        // (`job_update`) or drops them the moment this plan applies
        // (`job_submit`). Counting them against node capacity here would
        // demand headroom for two copies of the job and fail a plain
        // resubmit of anything using more than half a node — and it would
        // contradict `plan_only` / `plan_for_config`, which drop them and so
        // report a plan the apply path then refuses.
        let replaced_job_id = if matches!(mode, PlanMode::Fresh) {
            self.store.job_id_for_job_config(job_config_id)?
        } else {
            None
        };
        let drop_replaced = |allocation: &Allocation| {
            Some(allocation.job_id.as_str()) == replaced_job_id.as_deref()
        };
        let snapshot =
            self.capacity_snapshot(replaced_job_id.is_some().then_some(&drop_replaced))?;
        let existing_counts =
            Self::existing_alloc_counts_by_task(job_config_id, &snapshot.allocations_by_node);
        if let Some(task_targets) = task_targets {
            for (task_id, _config, desired) in &mut tasks {
                if let Some(target) = task_targets.get(task_id) {
                    *desired = (*target).min(*desired);
                } else if matches!(mode, PlanMode::Reschedule) {
                    *desired = existing_counts.get(task_id).copied().unwrap_or_default();
                }
            }
        }

        let required_counts = Self::required_counts_for_mode(
            &tasks,
            mode.clone(),
            job_config_id,
            &snapshot.allocations_by_node,
        );

        let build_started = std::time::Instant::now();
        let BuiltPlan {
            placements,
            index_report,
        } = self.build_plan(
            job_config_id,
            nodes,
            tasks,
            all_tasks,
            required_counts,
            snapshot,
            mode.clone(),
            task_targets,
        )?;
        let indexed_tasks = index_report
            .iter()
            .filter(|usage| usage.index.is_some())
            .count();
        debug!(
            %job_config_id,
            mode = mode.as_str(),
            placement_count = placements.len(),
            indexed_tasks,
            planned_tasks = index_report.len(),
            build_plan_ms = build_started.elapsed().as_millis() as u64,
            "Scheduler build_plan completed"
        );
        if placements.is_empty() {
            info!(%job_config_id, "No placements generated during plan_and_apply");
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
        // Select from the pre-planning sample; re-reading here would reopen the
        // race the sample above exists to close.
        let expected_versions =
            Self::expected_versions_for(job_config_id, &candidate_versions, &affected_nodes);
        let submission = self.store.insert_plan_submission(
            job_config_id,
            mode.as_str(),
            &placement_pairs,
            &expected_versions,
        )?;
        let apply_started = std::time::Instant::now();
        let allocations = match self.store.apply_plan_submission(&submission)? {
            PlanApplyOutcome::Applied(allocations) => allocations,
            PlanApplyOutcome::Rejected {
                reason,
                latest_versions,
            } => {
                let scope_key = format!("job_config:{job_config_id}");
                let expected_epoch = submission
                    .expected_versions
                    .get(&scope_key)
                    .copied()
                    .unwrap_or_default();
                let latest_epoch = latest_versions.get(&scope_key).copied().unwrap_or_default();
                warn!(
                    %job_config_id,
                    submission_id = submission.id,
                    mode = mode.as_str(),
                    apply_plan_submission_ms = apply_started.elapsed().as_millis() as u64,
                    "Scheduler plan submission rejected: {reason}"
                );
                return Err(anyhow::Error::new(StorageError::Conflict {
                    message: format!(
                        "plan submission rejected: {reason} (expected epoch {}, latest epoch {})",
                        expected_epoch, latest_epoch
                    ),
                }));
            }
        };
        let deltas = Self::allocations_delta(&affected_nodes, &allocations);

        let outcome = PlanOutcome {
            allocations,
            deltas,
        };
        info!(
            %job_config_id,
            mode = mode.as_str(),
            applied_allocations = outcome.allocations.len(),
            affected_nodes = outcome.deltas.len(),
            apply_plan_submission_ms = apply_started.elapsed().as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Completed scheduler plan_and_apply"
        );
        Ok(outcome)
    }

    /// Narrow the pre-planning candidate sample down to the scopes this plan
    /// actually touches: the job config plus the nodes it placed on.
    ///
    /// A node missing from the sample (it was not a planning candidate, which
    /// `build_plan` never produces) falls back to `0` — the same default a
    /// never-allocated scope has, so the compare-and-set stays conservative and
    /// rejects rather than admits.
    fn expected_versions_for(
        job_config_id: &str,
        candidate_versions: &BTreeMap<String, i64>,
        affected_nodes: &HashSet<String>,
    ) -> BTreeMap<String, i64> {
        let mut expected = BTreeMap::new();
        let job_key = format!("job_config:{job_config_id}");
        let job_epoch = candidate_versions
            .get(&job_key)
            .copied()
            .unwrap_or_default();
        expected.insert(job_key, job_epoch);
        for node_id in affected_nodes {
            let node_key = format!("node:{node_id}");
            let epoch = candidate_versions
                .get(&node_key)
                .copied()
                .unwrap_or_default();
            expected.insert(node_key, epoch);
        }
        expected
    }

    fn existing_alloc_counts_by_task(
        job_config_id: &str,
        allocations_by_node: &SharedNodeAllocations,
    ) -> HashMap<i64, u32> {
        let mut counts = HashMap::new();
        for allocations in allocations_by_node.values() {
            for allocation in allocations.iter() {
                if allocation.job_config_id == job_config_id {
                    *counts.entry(allocation.task_config_id).or_default() += 1;
                }
            }
        }
        counts
    }

    fn required_counts_for_mode(
        tasks: &[(i64, TaskConfig, u32)],
        mode: PlanMode,
        job_config_id: &str,
        allocations_by_node: &SharedNodeAllocations,
    ) -> HashMap<i64, u32> {
        if matches!(mode, PlanMode::Fresh) {
            return tasks
                .iter()
                .map(|(id, _config, desired)| (*id, *desired))
                .collect();
        }

        let mut placed_by_task: HashMap<i64, u32> = HashMap::new();
        for allocations in allocations_by_node.values() {
            for alloc in allocations.iter() {
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
        allocations_by_node: &SharedNodeAllocations,
    ) -> HashMap<(i64, String), u32> {
        let mut counts = HashMap::new();
        for (node_id, allocations) in allocations_by_node {
            for alloc in allocations.iter() {
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

    /// The plan's node set: every available node, sorted by id, snapshotted
    /// as `Arc`s (refcount bumps — no per-plan deep clone of node JSON).
    fn available_nodes(&self) -> R<Vec<Arc<Node>>> {
        self.store.available_node_arcs()
    }

    fn build_plan(
        &self,
        job_config_id: &str,
        nodes: Vec<Arc<Node>>,
        mut tasks: Vec<(i64, TaskConfig, u32)>,
        all_tasks: Vec<(i64, TaskConfig)>,
        required_counts: HashMap<i64, u32>,
        capacity: CapacitySnapshot,
        mode: PlanMode,
        task_targets: Option<&HashMap<i64, u32>>,
    ) -> R<BuiltPlan> {
        tasks.sort_by_key(|a| a.0);

        let CapacitySnapshot {
            allocations_by_node,
            pools,
            freed_nodes,
        } = capacity;
        // The body reads allocations by reference throughout (the owned map
        // outlives the plan); the pools move into the ledger below.
        let allocations_by_node = &allocations_by_node;
        let mut placements = Vec::new();
        let mut index_report: Vec<TaskIndexUsage> = Vec::new();
        // Indexes arm only above the configured node threshold; below it the
        // scan is cheaper than any bookkeeping (docs/scheduling-indexes.md).
        let index_armed = self.store.index_settings.armed_for(nodes.len());
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
        let node_lookup: HashMap<&str, &Node> = nodes
            .iter()
            .map(|node| (node.id.as_str(), &**node))
            .collect();
        let resource_policy = &self.store.resource_policy;
        let mut resource_ledger = ResourceLedger::from_snapshot(
            pools,
            &freed_nodes,
            &node_lookup,
            allocations_by_node,
            resource_policy,
        );
        let mut group_per_key: HashMap<String, Option<String>> = HashMap::new();
        let mut group_sizes: HashMap<String, usize> = HashMap::new();
        // Hard filters (constraints + permit) of every member of a group. A
        // multi-task group is placed on a single node, so that node must
        // satisfy all of them.
        let mut group_filters: HashMap<String, Vec<(ConstraintSet, PermitSet)>> = HashMap::new();
        let mut task_group_map: HashMap<i64, (String, Option<String>)> = HashMap::new();
        let mut task_id_to_group: HashMap<i64, String> = HashMap::new();
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
            // Collected for every task, not just the second and later members:
            // group size is only known once the whole list has been walked, and
            // the first member's filters count just as much as the rest.
            let constraint_set =
                ConstraintSet::new(&task_config.constraints).with_context(|| {
                    format!("Failed to parse constraints for task {}", task_config.name)
                })?;
            let permit_set = PermitSet::new(&task_config.permit)
                .with_context(|| format!("Failed to parse permit for task {}", task_config.name))?;
            group_filters
                .entry(task_config.group.clone())
                .or_default()
                .push((constraint_set, permit_set));
            task_group_map.insert(*task_id, (task_config.group.clone(), per_key));
            task_id_to_group.insert(*task_id, task_config.group.clone());
        }

        let mut group_assignments: HashMap<GroupKey, String> = HashMap::new();
        for (node_id, allocations) in allocations_by_node {
            for allocation in allocations.iter() {
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
                // Only allocations on nodes that are still placement
                // candidates pin their group. An allocation left behind on a
                // drained or lost node (a fenced stateful task waits out its
                // `onLost.replaceAfter` there) would otherwise pin the group
                // to a node nothing can be placed on, blocking replacement of
                // its every group-mate. Later passes re-converge the group
                // onto whichever candidate node the survivors landed on.
                let Some(node) = node_lookup.get(node_id.as_str()) else {
                    continue;
                };
                let token = if let Some(per_key) = per_key.as_deref() {
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
        for (group_name, filters) in &group_filters {
            if group_sizes.get(group_name).copied().unwrap_or(0) <= 1 {
                continue;
            }
            let per_key = group_per_key
                .get(group_name)
                .and_then(|value| value.clone());
            let mut grouped: HashMap<String, Vec<&Node>> = HashMap::new();
            for node in nodes.iter().map(|node| &**node) {
                if !filters.iter().all(|(constraint, permit)| {
                    constraint.is_satisfied(node, None) && permit.is_permitted(node)
                }) {
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

            let in_multi_task_group = group_sizes.get(&task_config.group).copied().unwrap_or(0) > 1;
            let mut plan = self.build_task_plan(
                task_id,
                &task_config,
                &nodes,
                resource_policy,
                allocations_by_node,
                &node_lookup,
                needed,
                index_armed,
                in_multi_task_group,
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

                // Deterministic order so an explicit target budget below is
                // always spent on the same groups.
                let mut grouped = grouped.into_iter().collect::<Vec<_>>();
                grouped.sort_by(|left, right| left.0.cmp(&right.0));

                // `count.per` normally converges each group to `count.num`
                // independently. An explicit target is a total for the task
                // (rollouts stage placements that way), so it has to cap the
                // sum across groups instead — otherwise a canary step places
                // the whole fleet at once.
                let mut target_budget = task_targets
                    .map(|_| required_counts.get(&task_id).copied().unwrap_or_default());

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
                    let mut needed = task_config.count.num.saturating_sub(placed);
                    if let Some(budget) = target_budget.as_mut() {
                        needed = needed.min(*budget);
                        *budget -= needed;
                    }
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
                        &task_id_to_group,
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
                    &task_id_to_group,
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
            index_report.push(plan.index_usage.clone());
        }

        Ok(BuiltPlan {
            placements,
            index_report,
        })
    }

    /// Budget for a truncated (index-served) candidate list: a generous
    /// multiple of what the task still needs, floored so small counts still
    /// see real choice before scoring.
    fn candidate_budget(needed: u32) -> usize {
        (needed as usize).saturating_mul(8).max(64)
    }

    /// Today's authoritative candidate filter: constraints, permit, and
    /// volume-driver support over every available node. The permanent
    /// fallback the scheduling index is measured against.
    fn scan_base_eligible<'b>(
        nodes: impl IntoIterator<Item = &'b Node>,
        constraint_set: &ConstraintSet,
        permit_set: &PermitSet,
        required_drivers: &std::collections::BTreeSet<String>,
    ) -> Vec<&'b Node> {
        nodes
            .into_iter()
            .filter(|node| {
                constraint_set.is_satisfied(node, None)
                    && permit_set.is_permitted(node)
                    && node_provides_volume_drivers(node, required_drivers)
            })
            .collect()
    }

    /// Candidate construction through the scheduling index
    /// (docs/scheduling-indexes.md). Every index candidate is re-verified
    /// with exactly the scan's predicate, so a stale or wrong index costs
    /// coverage, never correctness — and empty coverage falls straight back
    /// to the scan. Untruncated results are re-sorted by node id so
    /// index-served planning is byte-identical to scan planning; truncation
    /// keeps the index's nearest-fit-first order.
    fn indexed_base_eligible<'b>(
        &self,
        nodes: &'b [Arc<Node>],
        node_lookup: &HashMap<&'b str, &'b Node>,
        constraint_set: &ConstraintSet,
        permit_set: &PermitSet,
        task_request: &ResourceRequest,
        required_drivers: &std::collections::BTreeSet<String>,
        needed: u32,
        allow_truncation: bool,
        usage: &mut TaskIndexUsage,
    ) -> R<Vec<&'b Node>> {
        let metrics = crate::manager::telemetry::metrics();

        // Partition bindings: the task's `=` constraints plus the pool its
        // permit pins. A `pool` partition key always means the node's pool
        // column, so only the permit may bind it.
        let mut bindings = constraint_set.equality_bindings();
        bindings.remove("pool");
        if let Some(pool) = permit_set.pool_binding() {
            bindings.insert("pool".to_string(), pool.clone());
        }
        let request_amounts: HashMap<String, f64> = task_request
            .values
            .iter()
            .filter_map(|(name, value)| match value {
                ResourceValue::Amount(amount) => Some((name.clone(), *amount)),
                ResourceValue::Items(_) => None,
            })
            .collect();

        let candidates =
            match self
                .store
                .scheduling_index_query(&bindings, &request_amounts, None)?
            {
                IndexQueryOutcome::Disabled => {
                    return Ok(Self::scan_base_eligible(
                        node_refs(nodes),
                        constraint_set,
                        permit_set,
                        required_drivers,
                    ));
                }
                IndexQueryOutcome::Unusable { reason } => {
                    metrics
                        .scheduler_index_queries_total
                        .inc(&["none", "unusable"]);
                    usage.reason = Some(reason);
                    return Ok(Self::scan_base_eligible(
                        node_refs(nodes),
                        constraint_set,
                        permit_set,
                        required_drivers,
                    ));
                }
                IndexQueryOutcome::Candidates(candidates) => candidates,
            };

        let mut verified: Vec<&'b Node> = candidates
            .node_ids
            .iter()
            .filter_map(|id| node_lookup.get(id.as_str()).copied())
            .filter(|node| {
                constraint_set.is_satisfied(node, None)
                    && permit_set.is_permitted(node)
                    && node_provides_volume_drivers(node, required_drivers)
            })
            .collect();

        if candidates.mode == IndexMode::Shadow {
            // The scan stays authoritative; the index runs alongside and any
            // node it missed is a disagreement worth shouting about.
            let scan = Self::scan_base_eligible(
                node_refs(nodes),
                constraint_set,
                permit_set,
                required_drivers,
            );
            let scan_ids: HashSet<&str> = scan.iter().map(|node| node.id.as_str()).collect();
            let index_ids: HashSet<&str> = verified.iter().map(|node| node.id.as_str()).collect();
            let agreement = scan_ids == index_ids;
            usage.index = Some(candidates.index.clone());
            usage.mode = Some(IndexMode::Shadow.as_str());
            usage.shadow_agreement = Some(agreement);
            metrics
                .scheduler_index_queries_total
                .inc(&[candidates.index.as_str(), "shadow"]);
            metrics.scheduler_index_shadow_total.inc(&[
                candidates.index.as_str(),
                if agreement { "agree" } else { "disagree" },
            ]);
            if !agreement {
                let missing: Vec<&&str> = scan_ids.difference(&index_ids).take(3).collect();
                warn!(
                    index = %candidates.index,
                    scan_count = scan_ids.len(),
                    index_count = index_ids.len(),
                    missing_sample = ?missing,
                    "Scheduling index shadow disagreement: index candidates diverge from the scan"
                );
            }
            return Ok(scan);
        }

        if verified.is_empty() {
            // Either nothing matches (the scan agrees and this plan fails
            // with the usual diagnostic) or the index under-covered; one
            // scan on the empty path tells them apart and keeps the scan's
            // answer authoritative either way.
            let scan = Self::scan_base_eligible(
                node_refs(nodes),
                constraint_set,
                permit_set,
                required_drivers,
            );
            usage.index = Some(candidates.index.clone());
            usage.mode = Some(IndexMode::On.as_str());
            if scan.is_empty() {
                metrics
                    .scheduler_index_queries_total
                    .inc(&[candidates.index.as_str(), "used"]);
            } else {
                usage.fell_back = true;
                metrics
                    .scheduler_index_queries_total
                    .inc(&[candidates.index.as_str(), "fell_back"]);
                warn!(
                    index = %candidates.index,
                    scan_count = scan.len(),
                    "Scheduling index found no eligible candidates but the scan did; using the scan"
                );
            }
            return Ok(scan);
        }

        usage.index = Some(candidates.index.clone());
        usage.mode = Some(IndexMode::On.as_str());
        metrics
            .scheduler_index_queries_total
            .inc(&[candidates.index.as_str(), "used"]);
        let budget = Self::candidate_budget(needed);
        if allow_truncation && verified.len() > budget {
            verified.truncate(budget);
            usage.truncated = true;
        } else {
            verified.sort_by(|a, b| a.id.cmp(&b.id));
        }
        metrics
            .scheduler_index_candidates
            .observe(&[candidates.index.as_str()], verified.len() as f64);
        Ok(verified)
    }

    fn build_task_plan<'b>(
        &self,
        task_id: i64,
        task_config: &'b TaskConfig,
        nodes: &'b [Arc<Node>],
        resource_policy: &ResourcePolicy,
        allocations_by_node: &SharedNodeAllocations,
        node_lookup: &HashMap<&'b str, &'b Node>,
        needed: u32,
        index_armed: bool,
        in_multi_task_group: bool,
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
        // Storage is a hard placement requirement: a node without the task's
        // volume driver (or with it installed-but-unusable) can never run the
        // task, so filter it out here rather than letting the task start and
        // fail at mount time.
        let required_drivers = task_config.required_volume_drivers();

        let mut index_usage = TaskIndexUsage::scan(&task_config.name);
        // Truncating candidates is only safe when nothing downstream needs
        // the full set: `count.per` targets every attribute group, `distinct`
        // constraints need diversity, volume-pinned tasks need their exact
        // node, and multi-task groups place from their own candidate sets.
        let allow_truncation = index_armed
            && task_config.count.per.is_none()
            && constraint_set.distinct_keys().is_empty()
            && task_config.volumes.is_empty()
            && !in_multi_task_group;

        let base_eligible = if index_armed {
            self.indexed_base_eligible(
                nodes,
                node_lookup,
                &constraint_set,
                &permit_set,
                &task_request,
                &required_drivers,
                needed,
                allow_truncation,
                &mut index_usage,
            )?
        } else {
            Self::scan_base_eligible(
                node_refs(nodes),
                &constraint_set,
                &permit_set,
                &required_drivers,
            )
        };
        index_usage.candidates = base_eligible.len();
        if base_eligible.is_empty() && !required_drivers.is_empty() {
            let missing = required_drivers
                .iter()
                .filter(|plugin| {
                    !nodes
                        .iter()
                        .any(|node| node_provides_volume_drivers(node, &[(*plugin).clone()].into()))
                })
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                bail!(
                    "Failed to place task {}: no node provides volume driver(s) {}. \
                     Install them under plugins/volumes/<name>/ on a worker, or check \
                     `legion volume drivers` for why an installed one is unusable",
                    task_config.name,
                    missing.join(", ")
                );
            }
        }

        Ok(TaskPlan {
            constraint_set,
            permit_set,
            affinity_set,
            task_request,
            base_eligible,
            distinct_tracker,
            index_usage,
        })
    }

    fn place_task_allocations<'b>(
        &self,
        count: u32,
        task_id: i64,
        task_config: &TaskConfig,
        job_config_id: &str,
        candidates: &[&'b Node],
        allocations_by_node: &SharedNodeAllocations,
        task_name_to_id: &HashMap<String, i64>,
        task_id_to_group: &HashMap<i64, String>,
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
        // Owned so the truncated-index refill below can widen it mid-task.
        let mut candidates: Vec<&'b Node> = candidates.to_vec();
        let mut scoring_cache = PlanScoringCache::from_plan(
            &candidates,
            task_id,
            task_config,
            &plan.affinity_set,
            job_config_id,
            allocations_by_node,
            task_name_to_id,
            task_id_to_group,
            planned_task_allocations,
            planned,
            resource_ledger,
            &plan.task_request,
        );

        // Instances holding node-local storage can only be replaced on the node
        // their data lives on. Pins are consumed one per placement, in slot
        // order; the allocation binder independently prefers the instance whose
        // volume is pinned to the node an allocation landed on, so the two
        // agree even if their orderings differ (see docs/volumes.md).
        let mut pins: std::collections::VecDeque<(String, String)> = self
            .store
            .pinned_placements_for_task(task_id)
            .unwrap_or_else(|err| {
                warn!(task_id, error = ?err, "Failed loading volume pins; placing unpinned");
                Vec::new()
            })
            .into();

        // Disk accounting for node-local volumes, within this plan. Placing 3
        // replicas that each provision a 100G local volume onto a node with
        // 150G free must fail the third placement, not fill the disk at
        // runtime. Deliberately per-plan and best-effort (fingerprinted free
        // space, no cross-plan reservation ledger): the durable guard is the
        // fingerprint itself, which shrinks as volumes fill. Remote volumes
        // (nfs) never count — their bytes live on the storage server, and
        // charging them to the node would block placements for no reason.
        let mut planned_local_bytes: HashMap<String, u64> = HashMap::new();

        for _ in 0..count {
            let mut eligible = Self::eligible_nodes(
                &candidates,
                &plan.constraint_set,
                &plan.permit_set,
                plan.distinct_tracker.as_ref(),
                &scoring_cache,
            );

            // A truncated (index-served) candidate list can run dry while the
            // fleet still has room. Widen once to the authoritative scan and
            // continue — only the single-task path truncates, so group
            // pinning is never disturbed.
            if eligible.is_empty() && plan.index_usage.truncated && group_key.is_none() {
                let required_drivers = task_config.required_volume_drivers();
                let mut widened = Self::scan_base_eligible(
                    node_lookup.values().copied(),
                    &plan.constraint_set,
                    &plan.permit_set,
                    &required_drivers,
                );
                widened.sort_by(|a, b| a.id.cmp(&b.id));
                debug!(
                    task = %task_config.name,
                    truncated_count = candidates.len(),
                    widened_count = widened.len(),
                    "Scheduling index candidates exhausted; refilling from full scan"
                );
                plan.index_usage.truncated = false;
                plan.index_usage.refills += 1;
                plan.index_usage.fell_back = true;
                crate::manager::telemetry::metrics()
                    .scheduler_index_queries_total
                    .inc(&[
                        plan.index_usage.index.as_deref().unwrap_or("none"),
                        "refilled",
                    ]);
                candidates = widened;
                scoring_cache = PlanScoringCache::from_plan(
                    &candidates,
                    task_id,
                    task_config,
                    &plan.affinity_set,
                    job_config_id,
                    allocations_by_node,
                    task_name_to_id,
                    task_id_to_group,
                    planned_task_allocations,
                    planned,
                    resource_ledger,
                    &plan.task_request,
                );
                eligible = Self::eligible_nodes(
                    &candidates,
                    &plan.constraint_set,
                    &plan.permit_set,
                    plan.distinct_tracker.as_ref(),
                    &scoring_cache,
                );
            }

            if eligible.is_empty() {
                bail!(
                    "Failed to place task {}: no available nodes matching constraints, permit, or resources",
                    task_config.name
                );
            }

            // Disk fit: drop nodes whose remaining free disk (minus what this
            // plan already put there) cannot hold the task's node-local
            // volumes. Nodes that don't report disk are not excluded — missing
            // data is not evidence of a full disk.
            let eligible: Vec<&Node> = eligible
                .into_iter()
                .filter(|node| {
                    let wanted = task_local_volume_bytes(node, task_config);
                    if wanted == 0 {
                        return true;
                    }
                    match fingerprint_disk_free_bytes(node) {
                        None => true,
                        Some(free) => {
                            let already = planned_local_bytes
                                .get(node.id.as_str())
                                .copied()
                                .unwrap_or(0);
                            free.saturating_sub(already) >= wanted
                        }
                    }
                })
                .collect();
            if eligible.is_empty() {
                bail!(
                    "Failed to place task {}: no node has enough free disk for its node-local \
                     volume(s). Shrink the volume `size`, add disk, or use a network driver \
                     (nfs), whose storage does not consume node disk",
                    task_config.name
                );
            }

            // A pinned instance is not a preference: placing it elsewhere would
            // silently start the workload on empty storage, so failing the eval
            // with a diagnostic is the correct outcome.
            let pin = pins.pop_front();
            let eligible = match pin.as_ref() {
                None => eligible,
                Some((instance_id, node_id)) => {
                    let pinned = eligible
                        .iter()
                        .copied()
                        .filter(|node| node.id.as_str() == node_id.as_str())
                        .collect::<Vec<_>>();
                    if pinned.is_empty() {
                        bail!(
                            "Failed to place task {}: instance {instance_id} holds node-local \
                             storage on node {node_id}, which is unavailable. Waiting for that \
                             node to return; use `legion volume delete --force <id>` to abandon \
                             the data if it never will",
                            task_config.name
                        );
                    }
                    pinned
                }
            };

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
                        task_id,
                        task_config,
                        &plan.affinity_set,
                        job_config_id,
                        allocations_by_node,
                        task_name_to_id,
                        task_id_to_group,
                        planned_task_allocations,
                        planned,
                        &scoring_cache,
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
                    task_id,
                    task_config,
                    &plan.affinity_set,
                    job_config_id,
                    allocations_by_node,
                    task_name_to_id,
                    task_id_to_group,
                    planned_task_allocations,
                    planned,
                    &scoring_cache,
                    weights,
                )?
            };

            if let Some(node) = node_lookup.get(node_id.as_str()) {
                let wanted = task_local_volume_bytes(node, task_config);
                if wanted > 0 {
                    *planned_local_bytes.entry(node_id.clone()).or_default() += wanted;
                }
            }
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
            scoring_cache.update_after_placement(
                &node_id,
                task_id,
                task_id_to_group,
                resource_ledger,
                &plan.task_request,
            );
            placements.push(PlannedAllocation {
                node_id,
                task_config_id: task_id,
            });
        }

        Ok(())
    }

    fn select_node_id(
        &self,
        eligible: Vec<&Node>,
        reschedule_rng: &mut Option<StdRng>,
        nodes_considered_setting: Option<&NodesConsidered>,
        nodes_considered_rng: &mut StdRng,
        task_id: i64,
        task_config: &TaskConfig,
        affinity_set: &AffinitySet,
        job_config_id: &str,
        allocations_by_node: &SharedNodeAllocations,
        task_name_to_id: &HashMap<String, i64>,
        task_id_to_group: &HashMap<i64, String>,
        planned_task_allocations: &HashMap<String, Vec<i64>>,
        planned: &HashMap<String, usize>,
        scoring_cache: &PlanScoringCache,
        weights: SchedulerWeights,
    ) -> R<String> {
        let eligible = self.apply_nodes_considered_with_setting(
            eligible,
            nodes_considered_setting,
            nodes_considered_rng,
        )?;

        if matches!(self.policy.name(), "bin_pack" | "spread") {
            let selected = if let Some(rng) = reschedule_rng.as_mut() {
                self.select_cached_node_with_jitter(
                    &eligible,
                    affinity_set,
                    scoring_cache,
                    weights,
                    rng,
                )
            } else {
                self.select_cached_node(&eligible, affinity_set, scoring_cache, weights)
            };

            return selected.map(|node| node.id.clone()).with_context(|| {
                format!(
                    "Scheduling policy {} failed to place task {}",
                    self.policy.name(),
                    task_config.name
                )
            });
        }

        let ctx = SchedulingContext::new(
            eligible,
            task_config,
            task_id,
            affinity_set,
            job_config_id,
            allocations_by_node,
            task_name_to_id,
            task_id_to_group,
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
        scoring_cache: &PlanScoringCache,
    ) -> Vec<&'b Node> {
        candidates
            .iter()
            .copied()
            .filter(|node| {
                permit_set.is_permitted(node)
                    && distinct_tracker
                        .map(|tracker| constraint_set.distinct_satisfied(node, tracker))
                        .unwrap_or(true)
                    && scoring_cache.can_fit_task_request(&node.id)
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

        let mut tasks = Self::ready_tasks(tasks, &status_map)?;
        if let Some(job_id) = self.store.job_id_for_job_config(job_config_id)? {
            Self::apply_keyed_instance_counts(self.store, &job_id, &mut tasks)?;
        }
        Ok(tasks)
    }

    /// Replace `count.num`-derived desired counts with live keyed-instance
    /// counts for keyed-mode stateful tasks. `count.num` does not drive keyed
    /// tasks: their desired allocation count is exactly the number of
    /// instances in Pending/Active/Rescheduling state, created and destroyed
    /// through the instance management API (zero instances = zero
    /// allocations). Slot-mode and stateless tasks are untouched.
    fn apply_keyed_instance_counts(
        store: &Store,
        job_id: &str,
        tasks: &mut [(i64, TaskConfig, u32)],
    ) -> R<()> {
        for (_task_id, task, desired) in tasks.iter_mut() {
            let keyed = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Keyed);
            if keyed {
                *desired = store.count_live_keyed_instances(job_id, &task.group, &task.name)?;
            }
        }
        Ok(())
    }

    /// Reduce a slot-mode stateful task's desired count by the number of
    /// slots still deliberately occupied by live allocations of OTHER
    /// configs of the same job.
    ///
    /// After (or during) a staged rollout (`rollout.slots.updateFrom`),
    /// slots below the floor keep running the previous config's allocations
    /// on purpose. A plain reschedule of the latest config would otherwise
    /// converge that config to the full `count.num` and double-place those
    /// slots (two live allocations on one instance — the at-most-one
    /// invariant). Only applied to plain (no explicit per-task targets)
    /// `Reschedule` plans: rollouts pass explicit targets and own this math
    /// themselves, and `Fresh` plans deliberately ignore existing
    /// allocations. Keyed and stateless tasks are untouched.
    fn apply_slot_staged_counts(
        store: &Store,
        job_id: &str,
        job_config_id: &str,
        tasks: &mut [(i64, TaskConfig, u32)],
    ) -> R<()> {
        for (_task_id, task, desired) in tasks.iter_mut() {
            let slot_mode = task
                .stateful
                .as_ref()
                .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Slot);
            if slot_mode {
                let other_config_slots = store.count_slot_allocations_excluding_config(
                    job_id,
                    &task.group,
                    job_config_id,
                )?;
                *desired = desired.saturating_sub(other_config_slots);
            }
        }
        Ok(())
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
                ctx.task_id,
                ctx.affinity_set,
                ctx.job_config_id,
                ctx.allocations,
                ctx.task_name_to_id,
                ctx.task_id_to_group,
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

    fn select_cached_node<'b>(
        &self,
        eligible: &[&'b Node],
        affinity_set: &AffinitySet,
        scoring_cache: &PlanScoringCache,
        weights: SchedulerWeights,
    ) -> Option<&'b Node> {
        let scored = score_nodes_with_cache(eligible, affinity_set, scoring_cache, weights);
        if scored.is_empty() {
            return None;
        }

        let best_score = scored
            .iter()
            .fold(f64::NEG_INFINITY, |best, (_, score)| best.max(*score));
        let mut top_nodes = scored
            .into_iter()
            .filter_map(|(node, score)| {
                if (score - best_score).abs() <= 1e-9 {
                    Some(node)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        top_nodes.shuffle(&mut rand::thread_rng());
        top_nodes.into_iter().next()
    }

    fn select_cached_node_with_jitter<'b>(
        &self,
        eligible: &[&'b Node],
        affinity_set: &AffinitySet,
        scoring_cache: &PlanScoringCache,
        weights: SchedulerWeights,
        rng: &mut StdRng,
    ) -> Option<&'b Node> {
        let scored = score_nodes_with_cache(eligible, affinity_set, scoring_cache, weights);
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
                "automatic_anti_affinity" => weights.automatic_anti_affinity = weight,
                _ => {}
            }
        }

        Ok(weights)
    }

    fn build_distinct_tracker<'b>(
        constraints: &ConstraintSet,
        task_id: i64,
        allocations_by_node: &SharedNodeAllocations,
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

                // Matches the floor the percentage form clamps to, and the
                // one `nodes_considered_limit` enforces: a subset of one
                // leaves the policy nothing to compare.
                if count < 2 {
                    bail!("nodes_considered must be at least 2");
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

/// Whether a node advertises every driver in `required`, and reports each as
/// usable. An installed-but-unavailable driver (no `mount` binary, no declared
/// host volumes) is treated as absent — it cannot serve a mount either way, and
/// skipping the node produces a better error than a failed task start.
fn node_provides_volume_drivers(
    node: &Node,
    required: &std::collections::BTreeSet<String>,
) -> bool {
    if required.is_empty() {
        return true;
    }
    let Some(drivers) = node.fingerprint_attrs.get("volumeDrivers") else {
        // A node that advertises nothing predates plugin fingerprinting (or is
        // a test fixture). Assume the built-ins rather than excluding it, so an
        // upgrade doesn't strand every stateful job on the old workers.
        return required
            .iter()
            .all(|name| crate::volumes::BUILTIN_VOLUME_DRIVERS.contains(&name.as_str()));
    };
    required.iter().all(|name| {
        drivers
            .get(name)
            .map(|caps| {
                // Missing `available` means an older worker that predates the
                // field; assume usable rather than silently excluding it.
                caps.get("available")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
            })
            .unwrap_or(false)
    })
}

/// Bytes of **node-local** volume storage one placement of this task would put
/// on `node`, judged by the node's own advertised driver capabilities.
///
/// The locality question is per node deliberately: `size` on an `nfs` volume
/// consumes the NFS server's disk, not this node's, so remote drivers must not
/// count — that is the difference between "volumes respect node disk" and
/// "volumes double-count storage that lives elsewhere". Drivers the node does
/// not advertise fall back to the built-in set's locality. `tmpfs` is
/// node-local but RAM-backed, so it is excluded from *disk* accounting too.
/// Declarations without a parseable `size` contribute nothing — this is
/// placement sanity, not quota enforcement.
fn task_local_volume_bytes(node: &Node, task_config: &TaskConfig) -> u64 {
    let advertised = node.fingerprint_attrs.get("volumeDrivers");
    task_config
        .volume_size_declarations()
        .into_iter()
        .filter_map(|(driver, size)| {
            let size = size?;
            if driver == "tmpfs" {
                return None;
            }
            let node_local = advertised
                .and_then(|drivers| drivers.get(&driver))
                .and_then(|caps| caps.get("nodeLocal"))
                .and_then(Value::as_bool)
                .unwrap_or_else(|| {
                    // Unadvertised: assume the built-ins' locality (nfs is the
                    // one remote built-in), and treat unknown drivers as local
                    // — over-counting delays a placement; under-counting fills
                    // a disk.
                    driver != "nfs"
                });
            node_local.then_some(size)
        })
        .sum()
}

/// Free disk on a node, from the kitchen-sink fingerprint
/// (`disk_available_space`, bytes as a string). `None` when the node does not
/// report it — the disk check is skipped for such nodes rather than excluding
/// them on missing data.
fn fingerprint_disk_free_bytes(node: &Node) -> Option<u64> {
    match node.fingerprint_attrs.get("disk_available_space")? {
        Value::String(raw) => raw.parse().ok(),
        Value::Number(number) => number.as_u64(),
        _ => None,
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
            let score = score_node_for_context(ctx, node, &loads, min_load, range);
            (node, score)
        })
        .collect()
}

fn score_nodes_with_cache<'a>(
    eligible: &[&'a Node],
    affinity_set: &AffinitySet,
    scoring_cache: &PlanScoringCache,
    weights: SchedulerWeights,
) -> Vec<(&'a Node, f64)> {
    if eligible.is_empty() {
        return Vec::new();
    }

    let mut min_load: Option<usize> = None;
    let mut max_load: Option<usize> = None;
    for node in eligible {
        let load = scoring_cache
            .node_states
            .get(node.id.as_str())
            .map(|state| state.assigned_count)
            .unwrap_or_default();
        min_load = Some(min_load.map_or(load, |min| min.min(load)));
        max_load = Some(max_load.map_or(load, |max| max.max(load)));
    }

    let min_load = min_load.unwrap_or_default() as f64;
    let max_load = max_load.unwrap_or_default() as f64;
    let range = (max_load - min_load).max(0.0);

    eligible
        .iter()
        .copied()
        .map(|node| {
            let state = scoring_cache.node_states.get(node.id.as_str());
            let load = state.map(|s| s.assigned_count).unwrap_or_default() as f64;
            let (spread_score, bin_pack_score) = if range > 0.0 {
                ((max_load - load) / range, (load - min_load) / range)
            } else {
                (0.0, 0.0)
            };
            let affinity_score = state
                .map(|s| s.base_affinity + scoring_cache.affinity.colocation_score(s))
                .unwrap_or_else(|| affinity_set.score(node));
            let anti_affinity_penalty = state
                .map(|s| s.same_task_allocations.max(s.same_group_allocations) as f64)
                .unwrap_or_else(|| {
                    if scoring_cache.group_name.is_some() {
                        1.0
                    } else {
                        0.0
                    }
                });

            (
                node,
                weights.affinity * affinity_score
                    + weights.spread * spread_score
                    + weights.bin_pack * bin_pack_score
                    - weights.automatic_anti_affinity * anti_affinity_penalty,
            )
        })
        .collect()
}

fn task_colocation_preferences(affinities: &Value) -> Vec<(&str, f64)> {
    match affinities {
        Value::Object(map) => map
            .get("same_node_as")
            .and_then(Value::as_str)
            .map(|task| vec![(task, 1.0)])
            .unwrap_or_default(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                let obj = item.as_object()?;
                let key = obj.get("key")?.as_str()?;
                if key != "same_node_as" {
                    return None;
                }

                let task = obj.get("value")?.as_str()?;
                let multiplier = obj.get("multiplier").and_then(Value::as_f64).unwrap_or(1.0);
                Some((task, multiplier))
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Borrows through a plan's `Arc`'d node snapshot as plain `&Node`s —
/// everything below candidate construction works on borrows, untouched by
/// how the snapshot is owned.
fn node_refs(nodes: &[Arc<Node>]) -> impl Iterator<Item = &Node> {
    nodes.iter().map(|node| &**node)
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

impl ResourceLedger {
    /// Builds the plan-time ledger from the read model's cached free pools —
    /// no per-node JSON re-parsing. Nodes whose allocation set THIS plan
    /// changed (`freed_nodes`: a Fresh plan drops the job's own allocations
    /// before planning) are recomputed from scratch against the filtered
    /// allocation map, which is exactly the value a full rebuild would
    /// produce; every other node's cached pool already is that value. A
    /// candidate node with no cached pool (removed or drained between the
    /// node and capacity snapshots) simply fits nothing.
    fn from_snapshot(
        mut pools: HashMap<String, ResourcePool>,
        freed_nodes: &HashSet<String>,
        node_lookup: &HashMap<&str, &Node>,
        allocations_by_node: &SharedNodeAllocations,
        policy: &ResourcePolicy,
    ) -> Self {
        for node_id in freed_nodes {
            let Some(node) = node_lookup.get(node_id.as_str()) else {
                continue;
            };
            let allocations = allocations_by_node
                .get(node_id)
                .map(|allocations| allocations.as_slice())
                .unwrap_or(&[]);
            pools.insert(
                node_id.clone(),
                ResourcePool::free_for_node(node, allocations, policy),
            );
        }
        ResourceLedger { pools }
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

fn value_token(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use std::collections::{HashMap, HashSet};
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
            "memory_total": "8589934592",
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

    /// The scheduling-index equivalence property on the `on` path, asserted
    /// directly: untruncated index-served candidates are the scan's set in
    /// the scan's (by-id) order, across constraint shapes. Shadow mode
    /// asserts the same live comparison in the integration suite; this pins
    /// the mode that actually serves.
    #[test]
    fn indexed_candidates_equal_scan_candidates_untruncated() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({
                "index_min_nodes": 1,
                "indexes": {
                    "fit": false,
                    "topo": ["pool", "region"],
                },
            })),
        )?;
        for idx in 0..40 {
            store.insert_node(make_node_msg(
                &format!("node-{idx:02}"),
                json!({
                    "region": format!("region-{}", idx % 4),
                    "tier": if idx % 2 == 0 { "web" } else { "batch" },
                }),
            ))?;
        }
        let scheduler = Scheduler::new(&store)?;
        let nodes = store.available_node_arcs()?;
        let node_lookup: HashMap<&str, &Node> = nodes
            .iter()
            .map(|node| (node.id.as_str(), &**node))
            .collect();
        let no_drivers = std::collections::BTreeSet::new();
        let request = ResourceRequest::from_value(&json!({ "memory": 64 }))?;

        for constraints in [
            json!({ "region": "region-1" }),
            json!({ "region": "region-2", "tier": "web" }),
            json!([
                { "key": "region", "value": "region-3" },
                { "key": "tier", "operator": "matches", "value": "^b" },
            ]),
        ] {
            let constraint_set = ConstraintSet::new(&constraints)?;
            let permit_set = PermitSet::new(&Value::Null)?;
            let scan = Scheduler::scan_base_eligible(
                node_refs(&nodes),
                &constraint_set,
                &permit_set,
                &no_drivers,
            );
            let mut usage = TaskIndexUsage::scan("prop");
            let indexed = scheduler.indexed_base_eligible(
                &nodes,
                &node_lookup,
                &constraint_set,
                &permit_set,
                &request,
                &no_drivers,
                2,
                false,
                &mut usage,
            )?;
            assert_eq!(usage.index.as_deref(), Some("topo"), "{constraints}");
            let scan_ids: Vec<&str> = scan.iter().map(|node| node.id.as_str()).collect();
            let indexed_ids: Vec<&str> = indexed.iter().map(|node| node.id.as_str()).collect();
            assert!(!scan_ids.is_empty(), "test data must match {constraints}");
            assert_eq!(
                indexed_ids, scan_ids,
                "index-served candidates must equal the scan for {constraints}"
            );
        }

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

    fn score_with_legacy_context<'a>(
        eligible: Vec<&'a Node>,
        task: &'a TaskConfig,
        task_id: i64,
        affinity_set: &'a AffinitySet,
        job_config_id: &'a str,
        allocations_by_node: &'a SharedNodeAllocations,
        task_name_to_id: &'a HashMap<String, i64>,
        task_id_to_group: &'a HashMap<i64, String>,
        planned_task_allocations: &'a HashMap<String, Vec<i64>>,
        planned: &'a HashMap<String, usize>,
        weights: SchedulerWeights,
    ) -> Vec<(&'a Node, f64)> {
        let ctx = SchedulingContext::new(
            eligible,
            task,
            task_id,
            affinity_set,
            job_config_id,
            allocations_by_node,
            task_name_to_id,
            task_id_to_group,
            planned_task_allocations,
            planned,
            weights,
        );
        score_nodes_for_context(&ctx)
    }

    #[test]
    fn plans_fresh_allocations_and_deltas() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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

    fn stateful_slot_task(name: &str, count: u32) -> TaskConfig {
        TaskConfig {
            count: TaskCount::new(count),
            stateful: Some(crate::structs::StatefulConfig {
                mode: crate::structs::InstanceMode::Slot,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/var/lib/data".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    fn submit_job(store: &Store, name: &str, tasks: Vec<TaskConfig>) -> R<String> {
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.into(),
            tasks,
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        Ok(store.get_job_config(name)?.id())
    }

    #[test]
    fn slot_mode_initial_plan_assigns_slots_zero_through_n_minus_one() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "stateful-initial";
        let job_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 3)])?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 3);

        let mut instance_ids: Vec<String> = outcome
            .allocations
            .iter()
            .map(|alloc| {
                alloc
                    .instance_id
                    .clone()
                    .expect("stateful alloc has instance")
            })
            .collect();
        instance_ids.sort();
        assert_eq!(
            instance_ids,
            vec![
                "stateful-initial-default-0",
                "stateful-initial-default-1",
                "stateful-initial-default-2",
            ]
        );

        let instances = store.list_instances(job_id)?;
        assert_eq!(instances.len(), 3);
        for (slot, instance) in instances.iter().enumerate() {
            assert_eq!(instance.slot_index, Some(slot as u32));
            assert_eq!(instance.state, InstanceState::Active);
            assert!(instance
                .storage_handle
                .as_deref()
                .is_some_and(|handle| handle.starts_with("local-dir/")));
            let bound_alloc = outcome
                .allocations
                .iter()
                .find(|alloc| alloc.instance_id.as_deref() == Some(instance.instance_id.as_str()))
                .expect("instance bound to an allocation");
            assert_eq!(instance.current_alloc_id, Some(bound_alloc.id));
        }
        Ok(())
    }

    #[test]
    fn killed_slot_allocation_replacement_reuses_instance_and_storage() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "stateful-resched";
        let job_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 3)])?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        let slot1_alloc = outcome
            .allocations
            .iter()
            .find(|alloc| alloc.instance_id.as_deref() == Some("stateful-resched-default-1"))
            .cloned()
            .expect("slot 1 placed");
        let slot1_instance = store
            .get_instance("stateful-resched-default-1")?
            .expect("slot 1 instance");
        let original_handle = slot1_instance.storage_handle.clone();

        // Kill slot 1's allocation: the instance survives in rescheduling.
        store.remove_allocation_by_id(slot1_alloc.id)?;
        let waiting = store
            .get_instance("stateful-resched-default-1")?
            .expect("instance retained");
        assert_eq!(waiting.state, InstanceState::Rescheduling);

        // The reschedule plan places exactly one replacement that reuses slot
        // 1's instance id and reattaches the same storage handle.
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 1);
        let replacement = &outcome.allocations[0];
        assert_eq!(
            replacement.instance_id.as_deref(),
            Some("stateful-resched-default-1")
        );
        assert_ne!(replacement.id, slot1_alloc.id);

        let reactivated = store
            .get_instance("stateful-resched-default-1")?
            .expect("instance reactivated");
        assert_eq!(reactivated.state, InstanceState::Active);
        assert_eq!(reactivated.current_alloc_id, Some(replacement.id));
        assert_eq!(reactivated.storage_handle, original_handle);
        Ok(())
    }

    #[test]
    fn slot_reuse_survives_lost_node_reschedule() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "stateful-lost-node";
        let job_config_id = submit_job(&store, job_id, vec![stateful_slot_task("db", 2)])?;

        let scheduler = Scheduler::new(&store)?;
        scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        let handles_before: HashMap<String, Option<String>> = store
            .list_instances(job_id)?
            .into_iter()
            .map(|instance| (instance.instance_id.clone(), instance.storage_handle))
            .collect();

        store.mark_node_lost("node-a")?;
        store.remove_allocs_for_node("node-a")?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;

        for alloc in &outcome.allocations {
            assert_eq!(alloc.node_id, "node-b");
            let instance_id = alloc.instance_id.as_deref().expect("instance id");
            let instance = store.get_instance(instance_id)?.expect("instance");
            assert_eq!(
                &instance.storage_handle,
                handles_before.get(instance_id).expect("known instance"),
                "storage handle reattached on the new node"
            );
        }
        Ok(())
    }

    fn stateful_keyed_task(name: &str) -> TaskConfig {
        TaskConfig {
            // count.num is deliberately non-zero to prove it is ignored for
            // keyed tasks: live instances drive the desired count.
            count: TaskCount::new(3),
            stateful: Some(crate::structs::StatefulConfig {
                mode: crate::structs::InstanceMode::Keyed,
                id_source: None,
                initial_instances: None,
                on_lost: None,
                storage: Some(crate::structs::StatefulStorageConfig {
                    driver: None,
                    mode: None,
                    params: None,
                    path: "/workspace".into(),
                    retain_on_scale_in: None,
                    retain_on_stop: None,
                }),
            }),
            ..TaskConfig::new(name, "noop")
        }
    }

    #[test]
    fn keyed_task_with_zero_instances_plans_zero_allocations() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "keyed-zero";
        let job_config_id = submit_job(&store, job_id, vec![stateful_keyed_task("sandbox")])?;

        // count.num = 3, but no instances exist: nothing to place.
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert!(outcome.allocations.is_empty(), "count.num must be ignored");
        assert!(store.allocs_for_job(job_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn keyed_instances_drive_desired_count_and_each_binds_a_distinct_instance() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "keyed-drive";
        let job_config_id = submit_job(&store, job_id, vec![stateful_keyed_task("sandbox")])?;
        for key in ["user-a", "user-b"] {
            store.create_keyed_instance(job_id, "default", "sandbox", Some(key))?;
        }

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 2, "one allocation per instance");

        let mut bound: Vec<String> = outcome
            .allocations
            .iter()
            .map(|alloc| alloc.instance_id.clone().expect("keyed alloc bound"))
            .collect();
        bound.sort();
        assert_eq!(bound, vec!["user-a", "user-b"]);

        for instance in store.list_instances(job_id)? {
            assert_eq!(instance.state, InstanceState::Active);
            assert_eq!(instance.slot_index, None);
            assert!(instance
                .storage_handle
                .as_deref()
                .is_some_and(|handle| handle.starts_with("local-dir/")));
        }

        // Replanning when everything is placed adds nothing.
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert!(outcome.allocations.is_empty());
        Ok(())
    }

    #[test]
    fn killed_keyed_allocation_replacement_reuses_id_and_storage() -> R<()> {
        use crate::structs::InstanceState;

        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "keyed-resched-plan";
        let job_config_id = submit_job(&store, job_id, vec![stateful_keyed_task("sandbox")])?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-b"))?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        let victim = outcome
            .allocations
            .iter()
            .find(|alloc| alloc.instance_id.as_deref() == Some("user-a"))
            .cloned()
            .expect("user-a placed");
        let original_handle = store
            .get_instance("user-a")?
            .expect("instance")
            .storage_handle;

        store.remove_allocation_by_id(victim.id)?;
        assert_eq!(
            store.get_instance("user-a")?.expect("retained").state,
            InstanceState::Rescheduling
        );

        // Exactly one replacement, re-bound to the same identity + storage.
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(
            outcome.allocations[0].instance_id.as_deref(),
            Some("user-a")
        );
        let reactivated = store.get_instance("user-a")?.expect("reactivated");
        assert_eq!(reactivated.state, InstanceState::Active);
        assert_eq!(reactivated.storage_handle, original_handle);
        Ok(())
    }

    #[test]
    fn destroyed_keyed_instance_is_not_replaced_by_reconciliation() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "keyed-destroyed";
        let job_config_id = submit_job(&store, job_id, vec![stateful_keyed_task("sandbox")])?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-a"))?;
        store.create_keyed_instance(job_id, "default", "sandbox", Some("user-b"))?;

        let scheduler = Scheduler::new(&store)?;
        scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        store.destroy_keyed_instance("user-b")?;

        // The tombstone is not desired: nothing is re-placed for it.
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert!(outcome.allocations.is_empty());
        let allocations = store.allocs_for_job(job_id)?;
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations[0].instance_id.as_deref(), Some("user-a"));
        Ok(())
    }

    #[test]
    fn stateless_job_plans_carry_no_instance_metadata() -> R<()> {
        let store = setup_store()?;
        insert_test_nodes(&store)?;
        let job_id = "plain-job";
        let job_config_id = submit_job(
            &store,
            job_id,
            vec![TaskConfig {
                count: TaskCount::new(2),
                ..TaskConfig::new("web", "noop")
            }],
        )?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 2);
        assert!(outcome
            .allocations
            .iter()
            .all(|alloc| alloc.instance_id.is_none()));
        assert!(store.list_instances(job_id)?.is_empty());

        // Reschedule after losing an allocation behaves exactly as today.
        store.remove_allocation_by_id(outcome.allocations[0].id)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 1);
        assert!(outcome.allocations[0].instance_id.is_none());
        assert!(store.list_instances(job_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn count_per_attribute_places_allocations_per_group() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
    fn distinct_constraints_cache_selection_matches_legacy_scoring() -> R<()> {
        let nodes = vec![
            Node {
                id: "node-a".into(),
                status: "active".into(),
                pool: "default".into(),
                availability: "available".into(),
                last_heartbeat: String::new(),
                config_attrs: json!({"zone": "us-east"}),
                fingerprint_attrs: default_fingerprint(),
                dynamic_attrs: json!({}),
                health_checks: json!({}),
            },
            Node {
                id: "node-b".into(),
                status: "active".into(),
                pool: "default".into(),
                availability: "available".into(),
                last_heartbeat: String::new(),
                config_attrs: json!({"zone": "us-west"}),
                fingerprint_attrs: default_fingerprint(),
                dynamic_attrs: json!({}),
                health_checks: json!({}),
            },
        ];
        let candidate_refs = nodes.iter().collect::<Vec<_>>();
        let task = TaskConfig {
            count: TaskCount::new(2),
            constraints: json!([{ "key": "zone", "operator": "distinct" }]),
            ..TaskConfig::new("api", "noop")
        };
        let task_id = 7_i64;
        let allocations_by_node = SharedNodeAllocations::new();
        let planned_task_allocations: HashMap<String, Vec<i64>> = HashMap::new();
        let planned: HashMap<String, usize> = HashMap::new();
        let task_name_to_id = HashMap::from([(task.name.clone(), task_id)]);
        let task_id_to_group = HashMap::new();
        let affinity_set = AffinitySet::new(&task.affinities)?;
        let constraint_set = ConstraintSet::new(&task.constraints)?;
        let permit_set = PermitSet::new(&task.permit)?;
        let mut distinct_tracker = DistinctTracker::new(&constraint_set.distinct_keys());
        let mut pools = HashMap::new();
        for node in &nodes {
            pools.insert(node.id.clone(), ResourcePool::from_node(node)?);
        }
        let mut resource_ledger = ResourceLedger { pools };
        let request =
            ResourceRequest::from_value(&json!({"memory": 256, "cpu": 100, "allocations": 1}))?;
        let scoring_cache = PlanScoringCache::from_plan(
            &candidate_refs,
            task_id,
            &task,
            &affinity_set,
            "job-1",
            &allocations_by_node,
            &task_name_to_id,
            &task_id_to_group,
            &planned_task_allocations,
            &planned,
            &resource_ledger,
            &request,
        );

        let eligible = Scheduler::eligible_nodes(
            &candidate_refs,
            &constraint_set,
            &permit_set,
            Some(&distinct_tracker),
            &scoring_cache,
        );
        let legacy_scored = score_with_legacy_context(
            eligible.clone(),
            &task,
            task_id,
            &affinity_set,
            "job-1",
            &allocations_by_node,
            &task_name_to_id,
            &task_id_to_group,
            &planned_task_allocations,
            &planned,
            SchedulerWeights::for_policy("bin_pack"),
        );
        let cached_scored = score_nodes_with_cache(
            &eligible,
            &affinity_set,
            &scoring_cache,
            SchedulerWeights::for_policy("bin_pack"),
        );

        assert_eq!(legacy_scored.len(), cached_scored.len());
        for ((legacy_node, legacy_score), (cached_node, cached_score)) in
            legacy_scored.iter().zip(cached_scored.iter())
        {
            assert_eq!(legacy_node.id, cached_node.id);
            assert!((legacy_score - cached_score).abs() <= 1e-9);
        }

        let chosen = eligible[0];
        distinct_tracker.insert_from_node(chosen);
        resource_ledger.consume(&chosen.id, &request)?;

        Ok(())
    }

    #[test]
    fn apply_time_constraints_are_rechecked() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
                "memory_total": "536870912",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            json!({}),
        ))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
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
    fn resource_limit_cache_feasibility_matches_ledger() -> R<()> {
        let node = Node {
            id: "node-a".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs: json!({
                "memory_total": "536870912",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };
        let nodes = vec![node];
        let candidate_refs = nodes.iter().collect::<Vec<_>>();
        let task = TaskConfig {
            count: TaskCount::new(3),
            resources: json!({ "memory": 256 }),
            ..TaskConfig::new("worker", "noop")
        };
        let task_id = 11_i64;
        let request =
            ResourceRequest::from_value(&json!({"memory": 256, "cpu": 100, "allocations": 1}))?;
        let mut pools = HashMap::new();
        pools.insert("node-a".to_string(), ResourcePool::from_node(&nodes[0])?);
        let mut ledger = ResourceLedger { pools };
        let allocations_by_node = SharedNodeAllocations::new();
        let mut planned: HashMap<String, usize> = HashMap::new();
        let mut planned_task_allocations: HashMap<String, Vec<i64>> = HashMap::new();
        let task_name_to_id = HashMap::from([(task.name.clone(), task_id)]);
        let task_id_to_group = HashMap::new();
        let affinity_set = AffinitySet::new(&task.affinities)?;
        let mut cache = PlanScoringCache::from_plan(
            &candidate_refs,
            task_id,
            &task,
            &affinity_set,
            "job-1",
            &allocations_by_node,
            &task_name_to_id,
            &task_id_to_group,
            &planned_task_allocations,
            &planned,
            &ledger,
            &request,
        );

        assert!(cache.can_fit_task_request("node-a"));
        ledger.consume("node-a", &request)?;
        *planned.entry("node-a".to_string()).or_default() += 1;
        planned_task_allocations
            .entry("node-a".to_string())
            .or_default()
            .push(task_id);
        cache.update_after_placement("node-a", task_id, &task_id_to_group, &ledger, &request);
        assert!(cache.can_fit_task_request("node-a"));

        ledger.consume("node-a", &request)?;
        *planned.entry("node-a".to_string()).or_default() += 1;
        planned_task_allocations
            .entry("node-a".to_string())
            .or_default()
            .push(task_id);
        cache.update_after_placement("node-a", task_id, &task_id_to_group, &ledger, &request);
        assert!(!cache.can_fit_task_request("node-a"));
        assert_eq!(
            ledger.can_fit("node-a", &request),
            cache.can_fit_task_request("node-a")
        );

        Ok(())
    }

    fn node_with_fingerprint(fingerprint_attrs: Value) -> Node {
        Node {
            id: "node-a".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs,
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    #[test]
    fn default_task_request_fits_an_apple_silicon_node() -> R<()> {
        // Regression: with the frequency summed as MHz this node advertised
        // 40 MHz against the 100 MHz per-task default, so no job could ever
        // be placed on an Apple Silicon worker.
        let cpus = (1..=10)
            .map(|i| (format!("cpu-{i}"), json!({ "frequency": "4" })))
            .collect::<serde_json::Map<_, _>>();
        let node = node_with_fingerprint(json!({
            "memory_total": "34359738368",
            "cpus": cpus,
        }));
        let pool = ResourcePool::from_node(&node)?;
        let request =
            ResourceRequest::from_value(&json!({ "memory": 128, "cpu": 100, "allocations": 1 }))?;

        assert!(pool.can_fit(&request));
        Ok(())
    }

    #[test]
    fn max_allocations_limits_node_capacity() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg_with_sources(
            "node-a",
            json!({ "maxAllocations": 1 }),
            json!({
                "memory_total": "1073741824",
                "cpus": { "cpu-1": { "frequency": "1000" } }
            }),
            json!({}),
        ))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
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
                rollout: None,
                rollout_by_group: Default::default(),
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
                rollout: None,
                rollout_by_group: Default::default(),
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
        let eligible = node_refs(&nodes).collect::<Vec<_>>();
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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
        let allocations_by_node = SharedNodeAllocations::from([
            (
                "node-a".to_string(),
                Arc::new(vec![
                    Allocation {
                        id: 1,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 2,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 2,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                ]),
            ),
            (
                "node-b".to_string(),
                Arc::new(vec![Allocation {
                    id: 3,
                    job_id: "other".into(),
                    job_config_id: "cfg-2".into(),
                    task_config_id: 1,
                    node_id: "node-b".into(),
                    status: "running".into(),
                    resources: json!({}),
                    instance_id: None,
                    network: None,
                }]),
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
        let allocations_by_node = SharedNodeAllocations::from([
            (
                "node-a".to_string(),
                Arc::new(vec![
                    Allocation {
                        id: 1,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 2,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 3,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 2,
                        node_id: "node-a".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                ]),
            ),
            (
                "node-b".to_string(),
                Arc::new(vec![
                    Allocation {
                        id: 4,
                        job_id: "job-1".into(),
                        job_config_id: "cfg-1".into(),
                        task_config_id: 1,
                        node_id: "node-b".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                    Allocation {
                        id: 5,
                        job_id: "other".into(),
                        job_config_id: "cfg-other".into(),
                        task_config_id: 1,
                        node_id: "node-b".into(),
                        status: "running".into(),
                        resources: json!({}),
                        instance_id: None,
                        network: None,
                    },
                ]),
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
                "memory_total": "2147483648",
                "cpus": { "cpu-1": { "frequency": "2000" } }
            }),
            json!({}),
        ))?;
        store.insert_node(make_node_msg_with_sources(
            "node-b",
            json!({}),
            json!({
                "memory_total": "2147483648",
                "cpus": { "cpu-1": { "frequency": "2000" } }
            }),
            json!({}),
        ))?;

        let job_one = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "job-one".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 768 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        let job_two = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
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
    fn automatic_anti_affinity_penalizes_existing_group_allocations() -> R<()> {
        let store = setup_store_with_scheduler(
            "bin_pack",
            Some(json!({
                "weights": {
                    "affinities": 0.0,
                    "spread": 0.0,
                    "bin_pack": 0.0,
                    "automatic_anti_affinity": 1.0
                }
            })),
        )?;

        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "anti-affinity-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                group: "service".into(),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;
        store.place_alloc("node-a", task_id)?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;

        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-b");

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
            rollout: None,
            rollout_by_group: Default::default(),
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
            rollout: None,
            rollout_by_group: Default::default(),
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

    #[test]
    fn multi_task_group_honors_constraints_of_every_member() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "role": "web" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "role": "db" })))?;

        // The constrained task is listed FIRST in the group: the group's node
        // must still satisfy it.
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "group-first-constraint".into(),
            tasks: vec![
                TaskConfig {
                    group: "app".into(),
                    constraints: json!({ "role": "db" }),
                    ..TaskConfig::new("database", "noop")
                },
                TaskConfig {
                    group: "app".into(),
                    ..TaskConfig::new("sidecar", "noop")
                },
            ],
            version: None,
        };

        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;

        assert_eq!(outcome.allocations.len(), 2);
        for allocation in &outcome.allocations {
            assert_eq!(allocation.node_id, "node-b");
        }

        Ok(())
    }

    #[test]
    fn distinct_constraint_limits_one_allocation_per_attribute_value() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "zone": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "zone": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "zone": "west" })))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "distinct-zone".into(),
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

        let zones = outcome
            .allocations
            .iter()
            .map(|alloc| {
                let node = store.get_node(&alloc.node_id)?;
                Ok(node
                    .config_attrs
                    .get("zone")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string())
            })
            .collect::<R<HashSet<_>>>()?;
        assert_eq!(zones.len(), 2, "one allocation per distinct zone");

        Ok(())
    }

    #[test]
    fn unavailable_nodes_are_not_placement_candidates() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        let mut draining = make_node_msg("node-b", json!({}));
        draining.availability = "draining".into();
        store.insert_node(draining)?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "avail".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert!(outcome
            .allocations
            .iter()
            .all(|alloc| alloc.node_id == "node-a"));

        Ok(())
    }

    #[test]
    fn custom_item_resources_are_consumed_once() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "resources": { "gpu": ["gpu0", "gpu1"] } }),
        ))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "gpu-job".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 64, "gpu": ["gpu0"] }),
                ..TaskConfig::new("trainer", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let result = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh);
        assert!(
            result.is_err(),
            "second allocation cannot claim an already-consumed gpu item"
        );

        Ok(())
    }

    #[test]
    fn reserved_resources_reduce_schedulable_capacity() -> R<()> {
        let store = setup_store()?;
        // 8 GiB total, 7 GiB reserved -> only 1 GiB schedulable.
        store.insert_node(make_node_msg(
            "node-a",
            json!({ "reservedResources": { "memory": 7168 } }),
        ))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "reserved".into(),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 2048 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        assert!(scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .is_err());

        Ok(())
    }

    #[test]
    fn reschedule_places_only_the_missing_allocations() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "scale-up".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(3),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;
        store.place_alloc("node-a", task_id)?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 2, "places only the missing two");

        Ok(())
    }

    #[test]
    fn group_mixing_count_per_values_is_rejected() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "mixed-per".into(),
            tasks: vec![
                TaskConfig {
                    group: "app".into(),
                    count: TaskCount::per(1, "region"),
                    ..TaskConfig::new("api", "noop")
                },
                TaskConfig {
                    group: "app".into(),
                    count: TaskCount::new(1),
                    ..TaskConfig::new("cache", "noop")
                },
            ],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        assert!(scheduler
            .plan_and_apply(&job_config_id, PlanMode::Fresh)
            .is_err());

        Ok(())
    }

    #[test]
    fn concurrent_plans_cannot_oversubscribe_a_node() -> R<()> {
        let store = setup_store()?;
        // One node with room for exactly one 6000 MiB allocation.
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let mut config_ids = Vec::new();
        for name in ["job-one", "job-two"] {
            let job = JobConfig {
                rollout: None,
                rollout_by_group: Default::default(),
                name: name.into(),
                tasks: vec![TaskConfig {
                    resources: json!({ "memory": 6000 }),
                    ..TaskConfig::new("api", "noop")
                }],
                version: None,
            };
            store.handle_job_submit(&job, &[dummy_submission()])?;
            config_ids.push(store.get_job_config(name)?.id());
        }

        let handles = config_ids
            .into_iter()
            .map(|job_config_id| {
                let store = store.clone();
                std::thread::spawn(move || {
                    let scheduler = Scheduler::new(&store)?;
                    scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)
                })
            })
            .collect::<Vec<_>>();

        let mut applied = 0;
        for handle in handles {
            if let Ok(Ok(outcome)) = handle.join() {
                applied += outcome.allocations.len();
            }
        }

        assert_eq!(applied, 1, "only one plan may claim the node");
        assert_eq!(store.allocs_for_node("node-a")?.len(), 1);

        Ok(())
    }

    #[test]
    fn failed_batch_dependency_blocks_dependent_task() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({})))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "dag-failure".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Batch,
                    group: "build".into(),
                    ..TaskConfig::new("migrate", "noop")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    group: "serve".into(),
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "migrate".into(),
                        condition: TaskDependencyCondition::Success,
                    }],
                    ..TaskConfig::new("api", "noop")
                },
            ],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let migrate_id = task_id_for(&store, &job_config_id, "migrate")?;
        store.set_task_status(migrate_id, TaskStatusState::Failed)?;

        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert!(
            outcome
                .allocations
                .iter()
                .all(|alloc| alloc.task_config_id == migrate_id),
            "api must not be placed while its dependency has failed"
        );

        Ok(())
    }

    #[test]
    fn fresh_replan_ignores_the_jobs_own_outgoing_allocations() -> R<()> {
        let store = setup_store()?;
        store.insert_node(make_node_msg("node-a", json!({ "zone": "east" })))?;

        let v1 = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "heavy".into(),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 6000 }),
                affinities: json!({ "zone": "east" }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&v1, &[dummy_submission()])?;
        let v1_id = store.get_job_config(&v1.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        assert_eq!(
            scheduler
                .plan_and_apply(&v1_id, PlanMode::Fresh)?
                .allocations
                .len(),
            1
        );

        // A schedule-relevant change (affinity) forces a real replan.
        let v2 = JobConfig {
            tasks: vec![TaskConfig {
                resources: json!({ "memory": 6000 }),
                affinities: json!({ "zone": "west" }),
                ..TaskConfig::new("api", "noop")
            }],
            ..v1.clone()
        };
        store.handle_job_submit(&v2, &[dummy_submission()])?;
        let v2_id = store.get_job_config(&v2.name)?.id();
        assert_ne!(v1_id, v2_id);

        let outcome = scheduler.plan_and_apply(&v2_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 1);

        Ok(())
    }

    #[test]
    fn lost_node_replacement_respects_node_capacity() -> R<()> {
        let store = setup_store()?;
        // Both nodes fingerprint 8192 MiB, so a 6000 MiB task fits once each.
        store.insert_node(make_node_msg("node-a", json!({})))?;
        store.insert_node(make_node_msg("node-b", json!({})))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "capacity".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(2),
                resources: json!({ "memory": 6000 }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(outcome.allocations.len(), 2);

        // node-a dies. node-b is the only candidate left and has no room, so
        // no replacement may be placed there.
        let _ = store.mark_nodes_as_lost(&["node-a".to_string()]);

        let on_node_b = store
            .allocs_for_job_config(&job_config_id)?
            .into_iter()
            .filter(|alloc| alloc.node_id == "node-b")
            .count();
        assert_eq!(on_node_b, 1, "node-b has room for exactly one allocation");

        Ok(())
    }

    #[test]
    fn count_per_honors_explicit_task_targets() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;
        store.insert_node(make_node_msg("node-d", json!({ "region": "west" })))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "per-targets".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(2, "region"),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let task_id = task_id_for(&store, &job_config_id, "api")?;

        // A rollout stages placements by passing explicit per-task targets.
        let targets = HashMap::from([(task_id, 1)]);
        let scheduler = Scheduler::new(&store)?;
        let outcome = scheduler.plan_and_apply_with_targets(
            &job_config_id,
            PlanMode::Reschedule,
            &targets,
        )?;
        assert_eq!(outcome.allocations.len(), 1);

        Ok(())
    }

    #[test]
    fn count_per_reschedule_only_refills_the_affected_group() -> R<()> {
        let store = setup_store_with_scheduler("spread", None)?;
        store.insert_node(make_node_msg("node-a", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-b", json!({ "region": "east" })))?;
        store.insert_node(make_node_msg("node-c", json!({ "region": "west" })))?;
        store.insert_node(make_node_msg("node-d", json!({ "region": "west" })))?;

        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: "per-refill".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::per(2, "region"),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(&job, &[dummy_submission()])?;
        let job_config_id = store.get_job_config(&job.name)?.id();
        let scheduler = Scheduler::new(&store)?;
        let fresh = scheduler.plan_and_apply(&job_config_id, PlanMode::Fresh)?;
        assert_eq!(fresh.allocations.len(), 4);

        store.mark_node_lost("node-c")?;
        store.remove_allocs_for_node("node-c")?;
        let outcome = scheduler.plan_and_apply(&job_config_id, PlanMode::Reschedule)?;
        assert_eq!(outcome.allocations.len(), 1);
        assert_eq!(outcome.allocations[0].node_id, "node-d");

        Ok(())
    }
}
