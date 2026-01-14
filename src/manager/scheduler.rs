use std::collections::{HashMap, HashSet};

use serde_json::Value;

use libloading::Library;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};

use crate::constraints::{AffinitySet, ConstraintSet, DistinctTracker};
use crate::prelude::*;
use crate::resources::ResourcePolicy;
use crate::storage::store::Store;
use crate::structs::{Allocation, JobConfig, Node, TaskConfig};
pub use policy::{load_policy, SchedulerWeights, SchedulingContext, SchedulingPolicy};

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

        let tasks = self
            .store
            .cached_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .map(|(id, config)| (id, config.clone(), config.count))
            .collect::<Vec<_>>();

        let mut allocations_by_node = self.store.cached_allocations_by_node()?;
        if matches!(mode, PlanMode::Fresh) {
            for allocations in allocations_by_node.values_mut() {
                allocations.retain(|alloc| alloc.job_config_id != job_config_id);
            }
        }

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

        self.build_plan(
            job_config_id,
            nodes,
            tasks,
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

        let tasks = Self::tasks_for_plan(job_config);
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

        let required_counts = match mode {
            PlanMode::Fresh => tasks
                .iter()
                .map(|(id, _config, count)| (*id, *count))
                .collect::<HashMap<_, _>>(),
            PlanMode::Reschedule => {
                let mut current: HashMap<i64, u32> = HashMap::new();
                for allocations in allocations_by_node.values() {
                    for alloc in allocations {
                        if alloc.job_config_id == job_config.id() {
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

        let allocations = self.build_plan(
            &job_config.id(),
            nodes,
            tasks,
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

        let placements = self.build_plan(
            job_config_id,
            nodes,
            tasks,
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
        job_config_id: &str,
        nodes: Vec<Node>,
        mut tasks: Vec<(i64, TaskConfig, u32)>,
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
        let node_lookup: HashMap<&str, &Node> =
            nodes.iter().map(|node| (node.id.as_str(), node)).collect();
        let resource_policy = &self.store.resource_policy;
        let mut resource_ledger =
            ResourceLedger::new(&nodes, allocations_by_node, resource_policy)?;

        for (task_id, task_config, _desired_count) in tasks {
            let needed = required_counts.get(&task_id).copied().unwrap_or(0);
            if needed == 0 {
                continue;
            }

            let task_resources =
                resource_policy.normalize_task_resources(&task_config.resources, true)?;
            let task_request = ResourceRequest::from_value(&task_resources)?;

            let constraint_set =
                ConstraintSet::new(&task_config.constraints).with_context(|| {
                    format!("Failed to parse constraints for task {}", task_config.name)
                })?;
            let affinity_set = AffinitySet::new(&task_config.affinities).with_context(|| {
                format!("Failed to parse affinities for task {}", task_config.name)
            })?;
            let mut distinct_tracker = Self::build_distinct_tracker(
                &constraint_set,
                task_id,
                allocations_by_node,
                &node_lookup,
            );
            let base_eligible = nodes
                .iter()
                .filter(|node| constraint_set.is_satisfied(node, None))
                .collect::<Vec<_>>();

            for _ in 0..needed {
                let eligible = if let Some(tracker) = distinct_tracker.as_ref() {
                    base_eligible
                        .iter()
                        .copied()
                        .filter(|node| constraint_set.distinct_satisfied(node, tracker))
                        .collect::<Vec<_>>()
                } else {
                    base_eligible.clone()
                };

                let eligible = eligible
                    .into_iter()
                    .filter(|node| resource_ledger.can_fit(&node.id, &task_request))
                    .collect::<Vec<_>>();

                if eligible.is_empty() {
                    bail!(
                        "Failed to place task {}: no available nodes matching constraints or resources",
                        task_config.name
                    );
                }

                let node_id = if let Some(rng) = reschedule_rng.as_mut() {
                    let choice = eligible
                        .choose(rng)
                        .context("No nodes available to place allocations")?;
                    choice.id.clone()
                } else {
                    let eligible = self.apply_nodes_considered_with_setting(
                        eligible,
                        nodes_considered_setting.as_ref(),
                    )?;
                    let ctx = SchedulingContext::new(
                        eligible,
                        &task_config,
                        &affinity_set,
                        job_config_id,
                        allocations_by_node,
                        &planned,
                        weights,
                    );
                    self.policy
                        .select_node(ctx)
                        .map(|node| node.id.clone())
                        .with_context(|| {
                            format!(
                                "Scheduling policy {} failed to place task {}",
                                self.policy.name(),
                                task_config.name
                            )
                        })?
                };

                *planned.entry(node_id.clone()).or_default() += 1;
                if let Some(tracker) = distinct_tracker.as_mut() {
                    if let Some(node) = node_lookup.get(node_id.as_str()) {
                        tracker.insert_from_node(node);
                    }
                }
                resource_ledger.consume(&node_id, &task_request)?;
                placements.push(PlannedAllocation {
                    node_id,
                    task_config_id: task_id,
                });
            }
        }

        Ok(placements)
    }

    fn tasks_for_plan(job_config: &JobConfig) -> Vec<(i64, TaskConfig, u32)> {
        job_config
            .tasks
            .iter()
            .enumerate()
            .map(|(index, config)| ((index + 1) as i64, config.clone(), config.count))
            .collect()
    }

    fn apply_nodes_considered_with_setting<'b>(
        &self,
        eligible: Vec<&'b Node>,
        setting: Option<&NodesConsidered>,
    ) -> R<Vec<&'b Node>> {
        if eligible.len() <= 1 {
            return Ok(eligible);
        }

        let Some(setting) = setting else {
            return Ok(eligible);
        };

        let limit = Self::nodes_considered_limit(setting, eligible.len())?;
        Ok(eligible.into_iter().take(limit).collect())
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
                    count: 2,
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
        assert_eq!(web_on_node_a, 2);
        assert_eq!(web_on_node_b, 0);

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
                count: 3,
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
                count: 2,
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
                count: 2,
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
                count: 3,
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
            .contains("no available nodes matching constraints or resources"));

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
                count: 2,
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
            .contains("no available nodes matching constraints or resources"));

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
