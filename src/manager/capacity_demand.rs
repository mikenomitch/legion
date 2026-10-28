//! `GetCapacityDemand`: how many nodes of each node-group template the
//! pending work needs (autoscaler v2 plan §6.11, calls entry 7).
//!
//! A read-only simulation, off the scheduler's path. It takes the pending
//! work ([`Store::pending_capacity_work`]), builds one synthetic node per
//! template, and packs the pending replicas onto copies of it, first fit
//! decreasing. Every feasibility rule is the scheduler's own function, so
//! the answer cannot disagree with the scheduler about what fits:
//!
//! | Rule | Function |
//! | --- | --- |
//! | `permit` | [`PermitSet::permits_restrictions`] (pool and `restriction`) |
//! | `namespace` | [`accepts_namespace`] (the node's `namespaces`) |
//! | `constraint` | [`ConstraintSet::is_satisfied`] |
//! | `volume-driver` | `scheduler::node_provides_volume_drivers` |
//! | `resources` | [`ResourcePool::from_node`] and `can_fit`, with `reservedResources`, `capacity` and oversubscription |
//! | `distinct` | [`ConstraintSet::distinct_satisfied`] against a [`DistinctTracker`] seeded from the task's placed nodes |
//! | `static-port` | `scheduler::task_static_ports`: one claim per port per node |
//! | `gang` | a gang's missing quorum places on one template together, or not at all |
//!
//! What it does not model, and so may over-ask by a node: scoring and
//! spread (first fit is not the scheduler's choice), preemption, disk for
//! node-local volumes, checkpoint headroom, and the free room on existing
//! nodes. That last one is deliberate: the work is pending because the
//! scheduler already found no room for it.
//!
//! Nodes already on their way (`upcoming`) absorb pending work first,
//! shared across templates, so a group that was just asked to grow is not
//! asked again for the same replicas.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde_json::{json, Map, Value};

use crate::autoscaler::nodes::NodeShape;
use crate::constraints::{accepts_namespace, ConstraintSet, DistinctTracker, PermitSet};
use crate::manager::scheduler::{node_provides_volume_drivers, task_static_ports};
use crate::resources::{ResourcePolicy, ResourcePool, ResourceRequest, ResourceValue};
use crate::storage::store::{PendingTaskRow, PendingWork};
use crate::structs::config::WorkerResourceEnforcementConfig;
use crate::structs::namespace::job_namespace;
use crate::structs::{Node, WorkerConfig, NODEGROUP_ATTR};

/// The most pending replicas one call simulates. Past it, the rest of the
/// work is reported as pending but not packed, and the reply says
/// `truncated`.
pub const MAX_SIMULATED_REPLICAS: u32 = 20_000;

/// The suffix of a synthetic node's id: `<template>~new`.
const SYNTHETIC_SUFFIX: &str = "~new";

/// Why a template cannot host some pending work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Rule {
    /// The task's permit does not tolerate the template's pool or
    /// restrictions.
    Permit,
    /// The template's nodes do not accept the job's namespace.
    Namespace,
    /// A constraint fails on the template's attributes.
    Constraint,
    /// The template's nodes lack a volume driver the task needs.
    VolumeDriver,
    /// One replica needs more than an empty node offers.
    Resources,
    /// `distinct` leaves no value for more replicas.
    Distinct,
    /// Members of one group claim the same static port.
    StaticPort,
    /// The gang's quorum cannot place together.
    Gang,
    /// The task's resources, constraints or permit do not parse.
    Invalid,
}

impl Rule {
    /// The rule's name in replies (`resources`, `distinct`, ...).
    pub fn as_str(&self) -> &'static str {
        match self {
            Rule::Permit => "permit",
            Rule::Namespace => "namespace",
            Rule::Constraint => "constraint",
            Rule::VolumeDriver => "volume-driver",
            Rule::Resources => "resources",
            Rule::Distinct => "distinct",
            Rule::StaticPort => "static-port",
            Rule::Gang => "gang",
            Rule::Invalid => "invalid",
        }
    }
}

/// One node-group template, resolved.
#[derive(Debug, Clone)]
pub struct Template {
    /// The node group's name.
    pub name: String,
    /// The synthetic node every new member is a copy of.
    pub node: Node,
    /// Members already on their way.
    pub upcoming: u32,
}

/// What a template is built from.
#[derive(Debug, Clone, Default)]
pub struct TemplateSpec {
    /// The node group's name.
    pub name: String,
    /// The pool, when the request names one (else the worker type's, else
    /// `default`).
    pub pool: Option<String>,
    /// The worker type members join as, when it exists.
    pub worker_type: Option<WorkerConfig>,
    /// The member shape.
    pub shape: NodeShape,
    /// An existing member to copy instead of building from the worker
    /// type.
    pub like: Option<Node>,
    /// Members already on their way.
    pub upcoming: u32,
}

/// Build the synthetic node a template stands for.
///
/// From a member (`like`), it is that node with a new id. Otherwise it is
/// what a node of the worker type registers as: the type's pool,
/// `namespaces`, attributes, custom `resources`, `reservedResources`,
/// `maxAllocations` and oversubscription. Then the shape applies: `cpu`,
/// `memory` and `cores` become the node's `capacity` (the same ceiling a
/// system worker's lend uses), its attributes and resources are added, and
/// every template node carries `legion.nodegroup = <name>`, as every member
/// will.
pub fn synthetic_node(spec: &TemplateSpec) -> Node {
    let mut node = match &spec.like {
        Some(member) => member.clone(),
        None => {
            let mut attrs = Map::new();
            let mut pool = "default".to_string();
            if let Some(worker_type) = &spec.worker_type {
                pool = worker_type.pool.clone();
                for (key, value) in &worker_type.attributes {
                    attrs.insert(key.clone(), Value::String(value.clone()));
                }
                if !worker_type.namespaces.is_empty() {
                    attrs.insert(
                        crate::constraints::NODE_NAMESPACES_ATTR.to_string(),
                        json!(worker_type.namespaces),
                    );
                }
                for (key, raw) in [
                    ("resources", &worker_type.resources),
                    ("reservedResources", &worker_type.reserved_resources),
                ] {
                    if let Ok(value @ Value::Object(_)) = serde_json::from_str::<Value>(raw) {
                        attrs.insert(key.to_string(), value);
                    }
                }
                if let Some(max) = worker_type.max_allocations {
                    attrs.insert("maxAllocations".to_string(), json!(max));
                }
                if let Ok(enforcement) = serde_json::from_str::<WorkerResourceEnforcementConfig>(
                    &worker_type.resource_enforcement,
                ) {
                    let (memory, cpu) = (enforcement.memory_ratio(), enforcement.cpu_ratio());
                    if enforcement.enabled && (memory > 1.0 || cpu > 1.0) {
                        attrs.insert(
                            "oversubscribe".to_string(),
                            json!({ "memory": memory, "cpu": cpu }),
                        );
                    }
                }
            }
            Node {
                id: String::new(),
                status: "active".to_string(),
                pool,
                availability: "available".to_string(),
                last_heartbeat: String::new(),
                heartbeat_source: String::new(),
                config_attrs: Value::Object(attrs),
                fingerprint_attrs: json!({}),
                dynamic_attrs: json!({}),
                health_checks: json!({}),
            }
        }
    };
    node.id = format!("{}{SYNTHETIC_SUFFIX}", spec.name);
    node.status = "active".to_string();
    node.availability = "available".to_string();
    if let Some(pool) = &spec.pool {
        node.pool = pool.clone();
    }
    if !node.config_attrs.is_object() {
        node.config_attrs = json!({});
    }
    let attrs = node
        .config_attrs
        .as_object_mut()
        .expect("made an object above");
    let shape = &spec.shape;
    let mut capacity = Map::new();
    if let Some(cpu) = shape.cpu {
        capacity.insert("cpu".to_string(), json!(cpu));
    }
    if let Some(memory) = shape.memory {
        capacity.insert("memory".to_string(), json!(memory));
    }
    if let Some(cores) = shape.cores {
        capacity.insert("cores".to_string(), json!(cores));
    }
    if !capacity.is_empty() {
        if spec.like.is_some() {
            // A member's fingerprint is the truth; a shape on top of it can
            // only lower it, like any `capacity` ceiling.
            let existing = attrs
                .get(crate::resources::CAPACITY_ATTR)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            for (key, value) in existing {
                capacity.entry(key).or_insert(value);
            }
        } else {
            // No fingerprint: the shape is the whole machine.
            node.fingerprint_attrs = json!({});
        }
        attrs.insert(
            crate::resources::CAPACITY_ATTR.to_string(),
            Value::Object(capacity),
        );
    }
    for (key, value) in shape.attributes.iter().flatten() {
        attrs.insert(key.clone(), Value::String(value.clone()));
    }
    if let Some(resources) = &shape.resources {
        let mut merged = attrs
            .get("resources")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        for (key, value) in resources {
            merged.insert(key.clone(), value.clone());
        }
        attrs.insert("resources".to_string(), Value::Object(merged));
    }
    if let Some(max) = shape.max_allocations {
        attrs.insert("maxAllocations".to_string(), json!(max));
    }
    attrs.insert(NODEGROUP_ATTR.to_string(), Value::String(spec.name.clone()));
    node
}

/// One member task of a pending unit, parsed.
struct Member {
    constraints: ConstraintSet,
    permit: PermitSet,
    drivers: BTreeSet<String>,
    distinct: Option<DistinctTracker>,
}

/// The pending work of one task group of one job: the replicas that must
/// land together on one node each.
pub struct PendingUnit {
    /// The job.
    pub job_id: String,
    /// Its namespace.
    pub namespace: String,
    /// The task group.
    pub group: String,
    /// The short tasks of the group (usually one).
    pub tasks: Vec<String>,
    /// The count the group should have.
    pub desired: u32,
    /// What is placed.
    pub placed: u32,
    /// Replicas waiting.
    pub pending: u32,
    /// For a gang: the members still needed to reach its quorum, which must
    /// place together.
    pub gang_needed: Option<u32>,
    /// The gang's quorum, for the reply.
    pub gang_quorum: Option<u32>,
    /// What one replica reserves (every short member together).
    pub request: Option<ResourceRequest>,
    /// The request as JSON, for the reply.
    pub request_json: Value,
    /// Why the unit cannot be simulated at all, when it cannot.
    pub invalid: Option<String>,
    /// Two of its tasks claim the same static port, so they can never
    /// share the node a group needs.
    pub port_conflict: Option<String>,
    /// The static ports one replica claims.
    pub ports: Vec<u16>,
    members: Vec<Member>,
}

impl PendingUnit {
    /// The `distinct` keys of its tasks.
    pub fn distinct_keys(&self) -> Vec<String> {
        self.members
            .iter()
            .flat_map(|member| member.constraints.distinct_keys())
            .collect()
    }

    /// CPU one replica reserves, MHz.
    pub fn cpu_mhz(&self) -> f64 {
        self.request
            .as_ref()
            .and_then(|r| r.amount("cpu"))
            .unwrap_or(0.0)
    }

    /// Memory one replica reserves, MiB.
    pub fn memory_mib(&self) -> f64 {
        self.request
            .as_ref()
            .and_then(|r| r.amount("memory"))
            .unwrap_or(0.0)
    }
}

fn add_request(into: &mut ResourceRequest, other: &ResourceRequest) {
    for (name, value) in &other.values {
        match (into.values.get_mut(name), value) {
            (Some(ResourceValue::Amount(have)), ResourceValue::Amount(more)) => *have += more,
            (Some(ResourceValue::Items(have)), ResourceValue::Items(more)) => {
                have.extend(more.iter().cloned())
            }
            _ => {
                into.values.insert(name.clone(), value.clone());
            }
        }
    }
}

fn request_json(request: &ResourceRequest) -> Value {
    let mut out = Map::new();
    let mut names: Vec<&String> = request.values.keys().collect();
    names.sort();
    for name in names {
        let value = match &request.values[name] {
            ResourceValue::Amount(amount) => json!(amount),
            ResourceValue::Items(items) => {
                let mut items: Vec<&String> = items.iter().collect();
                items.sort();
                json!(items)
            }
        };
        out.insert(name.clone(), value);
    }
    Value::Object(out)
}

/// Group the pending tasks into units (one per job and task group), and
/// parse what the simulation needs. `nodes` resolves the nodes a task is
/// placed on, for `distinct`.
pub fn pending_units(
    rows: &[PendingTaskRow],
    policy: &ResourcePolicy,
    nodes: &HashMap<String, Node>,
) -> Vec<PendingUnit> {
    let mut grouped: BTreeMap<(String, String), Vec<&PendingTaskRow>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry((row.job_id.clone(), row.task.group.clone()))
            .or_default()
            .push(row);
    }
    let mut units = Vec::new();
    for ((job_id, group), rows) in grouped {
        let namespace = job_namespace(&job_id).to_string();
        let mut unit = PendingUnit {
            job_id: job_id.clone(),
            namespace: namespace.clone(),
            group,
            tasks: rows.iter().map(|row| row.task.name.clone()).collect(),
            desired: rows.iter().map(|row| row.desired).max().unwrap_or(0),
            placed: rows.iter().map(|row| row.placed).min().unwrap_or(0),
            pending: rows.iter().map(|row| row.pending).max().unwrap_or(0),
            gang_needed: None,
            gang_quorum: None,
            request: None,
            request_json: json!({}),
            invalid: None,
            port_conflict: None,
            ports: Vec::new(),
            members: Vec::new(),
        };
        let mut request = ResourceRequest {
            values: HashMap::new(),
        };
        let mut ports: Vec<u16> = Vec::new();
        let mut errors = Vec::new();
        for row in &rows {
            let task = &row.task;
            if let Some(gang) = &task.gang {
                let quorum = gang.quorum(row.desired);
                unit.gang_quorum = Some(quorum);
                unit.gang_needed = Some(quorum.saturating_sub(row.placed));
            }
            match policy
                .normalize_task_resources(&task.resources, true)
                .and_then(|value| ResourceRequest::from_value(&value))
            {
                Ok(one) => add_request(&mut request, &one),
                Err(err) => errors.push(format!("task {} resources: {err:#}", task.name)),
            }
            let constraints = match ConstraintSet::new(&task.constraints) {
                Ok(set) => set,
                Err(err) => {
                    errors.push(format!("task {} constraints: {err:#}", task.name));
                    continue;
                }
            };
            let permit = match PermitSet::new(&task.permit, &namespace) {
                Ok(set) => set,
                Err(err) => {
                    errors.push(format!("task {} permit: {err:#}", task.name));
                    continue;
                }
            };
            let keys = constraints.distinct_keys();
            let distinct = (!keys.is_empty()).then(|| {
                let mut tracker = DistinctTracker::new(&keys);
                for node in row.placed_nodes.iter().filter_map(|id| nodes.get(id)) {
                    tracker.insert_from_node(node);
                }
                tracker
            });
            for port in task_static_ports(task) {
                if ports.contains(&port) {
                    unit.port_conflict.get_or_insert_with(|| {
                        format!(
                            "tasks of group {} both claim static port {port}, so they can \
                             never share a node",
                            unit.group
                        )
                    });
                }
                ports.push(port);
            }
            unit.members.push(Member {
                constraints,
                permit,
                drivers: task.required_volume_drivers(),
                distinct,
            });
        }
        if !errors.is_empty() {
            unit.invalid = Some(errors.join("; "));
        }
        unit.request_json = request_json(&request);
        unit.request = Some(request);
        ports.sort_unstable();
        ports.dedup();
        unit.ports = ports;
        units.push(unit);
    }
    units
}

/// Why a template cannot host some of a unit's replicas.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal {
    /// Index into [`DemandReport::units`].
    pub unit: usize,
    /// Replicas refused.
    pub replicas: u32,
    /// The failing rule.
    pub rule: Rule,
    /// What failed, in words.
    pub detail: String,
}

/// One template's answer.
#[derive(Debug, Clone, Default)]
pub struct GroupReport {
    /// The node group.
    pub name: String,
    /// New nodes the pending work needs (`nodegroup.pending`).
    pub nodes: u32,
    /// Pending replicas it can host on those nodes.
    pub replicas: u32,
    /// Pending replicas its upcoming nodes absorbed.
    pub absorbed: u32,
    /// CPU the hosted replicas reserve, MHz.
    pub cpu_requested_mhz: f64,
    /// Memory the hosted replicas reserve, MiB.
    pub memory_requested_mib: f64,
    /// CPU the new nodes offer, MHz (0 when the template does not say).
    pub cpu_capacity_mhz: f64,
    /// Memory the new nodes offer, MiB (0 when the template does not say).
    pub memory_capacity_mib: f64,
    /// Work it cannot host, and why.
    pub refusals: Vec<Refusal>,
}

/// Replicas no template can host.
#[derive(Debug, Clone)]
pub struct Unfittable {
    /// Index into [`DemandReport::units`].
    pub unit: usize,
    /// Replicas no single template can host.
    pub replicas: u32,
    /// Each template's failing rule: (template, rule, detail).
    pub reasons: Vec<(String, Rule, String)>,
}

/// The whole answer.
pub struct DemandReport {
    /// The pending work considered.
    pub units: Vec<PendingUnit>,
    /// Replicas of each unit the upcoming nodes absorbed.
    pub absorbed: Vec<u32>,
    /// One per template, in request order.
    pub groups: Vec<GroupReport>,
    /// Work no template can host.
    pub unfittable: Vec<Unfittable>,
    /// Short jobs whose eval has not run yet.
    pub awaiting_scheduler: Vec<String>,
    /// Pending work past [`MAX_SIMULATED_REPLICAS`] was not packed.
    pub truncated: bool,
}

/// Static feasibility of a unit on a template's node: everything but
/// capacity, ports and `distinct`, which depend on what else is on the
/// node.
fn static_check(unit: &PendingUnit, node: &Node) -> Result<(), (Rule, String)> {
    if let Some(why) = &unit.invalid {
        return Err((Rule::Invalid, why.clone()));
    }
    if let Some(why) = &unit.port_conflict {
        return Err((Rule::StaticPort, why.clone()));
    }
    for (member, task) in unit.members.iter().zip(&unit.tasks) {
        if !member.permit.permits_restrictions(node) {
            return Err((
                Rule::Permit,
                format!(
                    "task {task}'s permit does not tolerate pool {:?}{}",
                    node.pool,
                    if node
                        .config_attrs
                        .get("restriction")
                        .is_some_and(|r| r.as_object().is_some_and(|r| !r.is_empty()))
                    {
                        " and its restrictions"
                    } else {
                        ""
                    }
                ),
            ));
        }
        if !accepts_namespace(node, &unit.namespace) {
            let accepted = crate::constraints::node_namespaces(node)
                .map(|list| list.join(", "))
                .unwrap_or_default();
            return Err((
                Rule::Namespace,
                format!(
                    "the template's nodes accept namespaces [{accepted}], not {:?}",
                    unit.namespace
                ),
            ));
        }
        if !member.constraints.is_satisfied(node, None) {
            let failing = member
                .constraints
                .unsatisfied_rule(node)
                .unwrap_or_else(|| "a constraint".to_string());
            return Err((
                Rule::Constraint,
                format!("task {task}: {failing} does not hold on the template"),
            ));
        }
        if !node_provides_volume_drivers(node, &member.drivers) {
            return Err((
                Rule::VolumeDriver,
                format!(
                    "task {task} needs volume driver(s) {}",
                    member
                        .drivers
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
    }
    Ok(())
}

/// Why a request does not fit an empty pool, dimension by dimension.
fn resource_shortfall(pool: &ResourcePool, request: &ResourceRequest) -> String {
    let mut parts = Vec::new();
    let mut names: Vec<&String> = request.values.keys().collect();
    names.sort();
    for name in names {
        match (&request.values[name], pool.values.get(name)) {
            (ResourceValue::Amount(want), Some(ResourceValue::Amount(have))) if want > have => {
                parts.push(format!("{name} {want} > {have} per node"))
            }
            (ResourceValue::Amount(want), None) => {
                parts.push(format!("{name} {want}: the template offers none"))
            }
            (ResourceValue::Items(want), Some(ResourceValue::Items(have)))
                if !want.is_subset(have) =>
            {
                parts.push(format!("{name}: the template lacks some of the items"))
            }
            (ResourceValue::Items(_), None | Some(ResourceValue::Amount(_))) => {
                parts.push(format!("{name}: the template offers no such items"))
            }
            _ => {}
        }
    }
    if parts.is_empty() {
        "does not fit an empty node".to_string()
    } else {
        format!("one replica needs {}", parts.join(", "))
    }
}

/// A node in the simulation.
#[derive(Clone)]
struct SimNode {
    template: usize,
    free: ResourcePool,
    ports: HashSet<u16>,
}

/// Whether one replica of `unit` fits on `sim`, given its template node.
fn fits(
    unit: &PendingUnit,
    sim: &SimNode,
    template_node: &Node,
    trackers: &[Option<DistinctTracker>],
) -> bool {
    let Some(request) = &unit.request else {
        return false;
    };
    sim.free.can_fit(request)
        && unit.ports.iter().all(|port| !sim.ports.contains(port))
        && unit
            .members
            .iter()
            .zip(trackers)
            .all(|(member, tracker)| match tracker {
                Some(tracker) => member
                    .constraints
                    .distinct_satisfied(template_node, tracker),
                None => true,
            })
}

fn place(
    unit: &PendingUnit,
    sim: &mut SimNode,
    template_node: &Node,
    trackers: &mut [Option<DistinctTracker>],
) {
    if let Some(request) = &unit.request {
        sim.free.consume(request);
    }
    sim.ports.extend(unit.ports.iter().copied());
    for tracker in trackers.iter_mut().flatten() {
        tracker.insert_from_node(template_node);
    }
}

/// The empty pool of a template's node, or `None` when its resources do
/// not parse.
fn empty_pool(node: &Node) -> Option<ResourcePool> {
    ResourcePool::from_node(node).ok()
}

/// A unit's size on a template, for the decreasing order: its dominant
/// share of an empty node.
fn dominant_share(unit: &PendingUnit, pool: &ResourcePool) -> f64 {
    let Some(request) = &unit.request else {
        return 0.0;
    };
    request
        .values
        .iter()
        .filter_map(|(name, value)| match (value, pool.amount(name)) {
            (ResourceValue::Amount(want), Some(have)) if have.is_finite() && have > 0.0 => {
                Some(want / have)
            }
            _ => None,
        })
        .fold(0.0, f64::max)
}

/// Pack `count` replicas of `unit` onto `sims` (first fit), opening new
/// nodes of `template` when `open` allows. Returns the template of every
/// replica placed. A gang (`atomic`) places all `count` or none.
#[allow(clippy::too_many_arguments)]
fn pack(
    unit: &PendingUnit,
    count: u32,
    atomic: bool,
    sims: &mut Vec<SimNode>,
    trackers: &mut Vec<Option<DistinctTracker>>,
    templates: &[Template],
    allowed: &dyn Fn(usize) -> bool,
    open: Option<(usize, &ResourcePool)>,
) -> Vec<usize> {
    let snapshot = atomic.then(|| (sims.clone(), trackers.clone()));
    let mut placed: Vec<usize> = Vec::new();
    // Replicas of one unit are identical and nodes only fill up, so a node
    // the last replica skipped cannot take the next one: each replica
    // resumes where the last one landed, which keeps a unit's packing
    // linear in replicas plus nodes rather than their product.
    let mut start = 0;
    'replica: while (placed.len() as u32) < count {
        for (index, sim) in sims.iter_mut().enumerate().skip(start) {
            if !allowed(sim.template) {
                continue;
            }
            let node = &templates[sim.template].node;
            if fits(unit, sim, node, trackers) {
                place(unit, sim, node, trackers);
                placed.push(sim.template);
                start = index;
                continue 'replica;
            }
        }
        start = sims.len();
        let Some((template, pool)) = open else {
            break;
        };
        let mut sim = SimNode {
            template,
            free: pool.clone(),
            ports: HashSet::new(),
        };
        let node = &templates[template].node;
        if !fits(unit, &sim, node, trackers) {
            break;
        }
        place(unit, &mut sim, node, trackers);
        sims.push(sim);
        placed.push(template);
    }
    if atomic && (placed.len() as u32) < count {
        if let Some((saved_sims, saved_trackers)) = snapshot {
            *sims = saved_sims;
            *trackers = saved_trackers;
        }
        return Vec::new();
    }
    placed
}

fn finite(value: Option<f64>) -> f64 {
    value.filter(|v| v.is_finite()).unwrap_or(0.0)
}

/// Run the simulation.
pub fn simulate(
    work: PendingWork,
    templates: &[Template],
    policy: &ResourcePolicy,
    nodes: &HashMap<String, Node>,
) -> DemandReport {
    let mut units = pending_units(&work.tasks, policy, nodes);
    // Past the budget, units (in job order) keep their pending count in the
    // reply but are not packed: the node counts are then a floor.
    let mut budget = MAX_SIMULATED_REPLICAS;
    let mut truncated = false;
    let mut considered: Vec<u32> = units
        .iter()
        .map(|unit| {
            let take = unit.pending.min(budget);
            if take < unit.pending {
                truncated = true;
            }
            budget -= take;
            take
        })
        .collect();

    let pools: Vec<Option<ResourcePool>> = templates.iter().map(|t| empty_pool(&t.node)).collect();
    let statics: Vec<Vec<Result<(), (Rule, String)>>> = units
        .iter()
        .map(|unit| {
            templates
                .iter()
                .zip(&pools)
                .map(|(template, pool)| {
                    static_check(unit, &template.node)?;
                    match (pool, &unit.request) {
                        (None, _) => Err((
                            Rule::Invalid,
                            "the template's resources do not parse".to_string(),
                        )),
                        (Some(pool), Some(request)) if !pool.can_fit(request) => {
                            Err((Rule::Resources, resource_shortfall(pool, request)))
                        }
                        _ => Ok(()),
                    }
                })
                .collect()
        })
        .collect();

    // Decreasing order over a reference pool: the first template's.
    let mut order: Vec<usize> = (0..units.len()).collect();
    if let Some(Some(reference)) = pools.first() {
        order.sort_by(|a, b| {
            dominant_share(&units[*b], reference)
                .total_cmp(&dominant_share(&units[*a], reference))
                .then(a.cmp(b))
        });
    }

    // The distinct trackers each unit starts from.
    let mut trackers: Vec<Vec<Option<DistinctTracker>>> = units
        .iter_mut()
        .map(|unit| {
            unit.members
                .iter_mut()
                .map(|member| member.distinct.take())
                .collect()
        })
        .collect();

    // Upcoming nodes absorb what they can first, across templates.
    let mut absorbed = vec![0u32; units.len()];
    let mut upcoming: Vec<SimNode> = Vec::new();
    for (index, template) in templates.iter().enumerate() {
        if let Some(pool) = &pools[index] {
            for _ in 0..template.upcoming.min(MAX_SIMULATED_REPLICAS) {
                upcoming.push(SimNode {
                    template: index,
                    free: pool.clone(),
                    ports: HashSet::new(),
                });
            }
        }
    }
    let mut absorbed_by_template = vec![0u32; templates.len()];
    if !upcoming.is_empty() {
        for &u in &order {
            let unit = &units[u];
            if considered[u] == 0 {
                continue;
            }
            let ok: Vec<bool> = statics[u].iter().map(Result::is_ok).collect();
            let landed = pack(
                unit,
                considered[u],
                unit.gang_needed.is_some(),
                &mut upcoming,
                &mut trackers[u],
                templates,
                &|t| ok[t],
                None,
            );
            for t in &landed {
                absorbed_by_template[*t] += 1;
            }
            let placed = u32::try_from(landed.len()).unwrap_or(u32::MAX);
            absorbed[u] = placed;
            considered[u] -= placed;
        }
    }

    // Each template on its own.
    let mut groups = Vec::with_capacity(templates.len());
    let mut hosted = vec![vec![0u32; templates.len()]; units.len()];
    for (t, template) in templates.iter().enumerate() {
        let mut report = GroupReport {
            name: template.name.clone(),
            absorbed: absorbed_by_template[t],
            ..Default::default()
        };
        let Some(pool) = &pools[t] else {
            for (u, unit) in units.iter().enumerate() {
                if considered[u] > 0 {
                    report.refusals.push(Refusal {
                        unit: u,
                        replicas: considered[u],
                        rule: Rule::Invalid,
                        detail: format!("the template's resources do not parse ({})", unit.job_id),
                    });
                }
            }
            groups.push(report);
            continue;
        };
        let mut sims: Vec<SimNode> = Vec::new();
        let mut local_trackers = trackers.clone();
        let mut local_order = order.clone();
        local_order.sort_by(|a, b| {
            dominant_share(&units[*b], pool)
                .total_cmp(&dominant_share(&units[*a], pool))
                .then(a.cmp(b))
        });
        for &u in &local_order {
            let unit = &units[u];
            let want = considered[u];
            if want == 0 {
                continue;
            }
            if let Err((rule, detail)) = &statics[u][t] {
                report.refusals.push(Refusal {
                    unit: u,
                    replicas: want,
                    rule: *rule,
                    detail: detail.clone(),
                });
                continue;
            }
            let only_this = |candidate: usize| candidate == t;
            let mut placed = 0;
            let mut refusal: Option<(Rule, String)> = None;
            let gang_needed = unit
                .gang_needed
                .map(|needed| needed.saturating_sub(absorbed[u]).min(want));
            if let Some(needed) = gang_needed.filter(|needed| *needed > 0) {
                placed = pack(
                    unit,
                    needed,
                    true,
                    &mut sims,
                    &mut local_trackers[u],
                    templates,
                    &only_this,
                    Some((t, pool)),
                )
                .len() as u32;
                if placed == 0 {
                    refusal = Some((
                        Rule::Gang,
                        format!(
                            "the gang needs {needed} more members together to reach its \
                             quorum of {}, and new nodes of this template can host fewer \
                             (distinct leaves too few values)",
                            unit.gang_quorum.unwrap_or(needed),
                        ),
                    ));
                }
            }
            if refusal.is_none() && placed < want {
                placed += pack(
                    unit,
                    want - placed,
                    false,
                    &mut sims,
                    &mut local_trackers[u],
                    templates,
                    &only_this,
                    Some((t, pool)),
                )
                .len() as u32;
                if placed < want {
                    // New nodes are unlimited, so only `distinct` can stop
                    // a replica that fits an empty node.
                    refusal = Some((
                        Rule::Distinct,
                        format!(
                            "distinct {} leaves no value for more replicas: every new node \
                             of this template has the same value",
                            unit.distinct_keys().join(", ")
                        ),
                    ));
                }
            }
            hosted[u][t] = placed;
            report.replicas += placed;
            report.cpu_requested_mhz += unit.cpu_mhz() * f64::from(placed);
            report.memory_requested_mib += unit.memory_mib() * f64::from(placed);
            if let Some((rule, detail)) = refusal {
                report.refusals.push(Refusal {
                    unit: u,
                    replicas: want - placed,
                    rule,
                    detail,
                });
            }
        }
        report.nodes = u32::try_from(sims.len()).unwrap_or(u32::MAX);
        report.cpu_capacity_mhz = finite(pool.amount("cpu")) * f64::from(report.nodes);
        report.memory_capacity_mib = finite(pool.amount("memory")) * f64::from(report.nodes);
        groups.push(report);
    }

    let mut unfittable = Vec::new();
    for (u, _) in units.iter().enumerate() {
        let want = considered[u];
        if want == 0 {
            continue;
        }
        let best = hosted[u].iter().copied().max().unwrap_or(0);
        if best >= want {
            continue;
        }
        let reasons = groups
            .iter()
            .filter_map(|group| {
                group
                    .refusals
                    .iter()
                    .find(|refusal| refusal.unit == u)
                    .map(|refusal| (group.name.clone(), refusal.rule, refusal.detail.clone()))
            })
            .collect();
        unfittable.push(Unfittable {
            unit: u,
            replicas: want - best,
            reasons,
        });
    }

    DemandReport {
        units,
        absorbed,
        groups,
        unfittable,
        awaiting_scheduler: work.awaiting_scheduler,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::TaskConfig;

    fn policy() -> ResourcePolicy {
        ResourcePolicy::from_manager_config(&Default::default()).expect("default policy")
    }

    fn row(job: &str, task: Value, desired: u32, placed: u32) -> PendingTaskRow {
        let task: TaskConfig = serde_json::from_value(task).expect("task config");
        PendingTaskRow {
            job_id: job.to_string(),
            job_config_id: format!("{job}-v1"),
            task,
            desired,
            placed,
            pending: desired - placed,
            placed_nodes: Vec::new(),
        }
    }

    fn task(name: &str, cpu: u64, memory: u64) -> Value {
        json!({
            "name": name, "runtime": "noop", "config": {},
            "resources": { "cpu": cpu, "memory": memory },
        })
    }

    fn shape(cpu: u64, memory: u64) -> NodeShape {
        NodeShape {
            cpu: Some(cpu),
            memory: Some(memory),
            ..Default::default()
        }
    }

    fn template(name: &str, shape: NodeShape, upcoming: u32) -> Template {
        let spec = TemplateSpec {
            name: name.to_string(),
            shape,
            upcoming,
            ..Default::default()
        };
        Template {
            name: name.to_string(),
            node: synthetic_node(&spec),
            upcoming,
        }
    }

    fn run(rows: Vec<PendingTaskRow>, templates: &[Template]) -> DemandReport {
        simulate(
            PendingWork {
                tasks: rows,
                awaiting_scheduler: Vec::new(),
            },
            templates,
            &policy(),
            &HashMap::new(),
        )
    }

    #[test]
    fn the_template_node_is_what_a_member_registers_as_test() {
        let worker_type = WorkerConfig {
            name: "gpu".into(),
            pool: "gpu".into(),
            attributes: [("accel".to_string(), "a100".to_string())].into(),
            namespaces: vec!["ml-*".into()],
            resources: r#"{"gpu": 4}"#.into(),
            reserved_resources: r#"{"memory": 1024}"#.into(),
            max_allocations: Some(8),
            resource_enforcement: r#"{"enabled": true, "oversubscribe": {"cpu": 2.0}}"#.into(),
            ..Default::default()
        };
        let mut member_shape = shape(16000, 65536);
        member_shape.attributes = Some([("zone".to_string(), "b".to_string())].into());
        let node = synthetic_node(&TemplateSpec {
            name: "gpus".into(),
            worker_type: Some(worker_type),
            shape: member_shape,
            ..Default::default()
        });
        assert_eq!(node.id, "gpus~new");
        assert_eq!(node.pool, "gpu");
        let attrs = &node.config_attrs;
        assert_eq!(attrs["accel"], "a100");
        assert_eq!(attrs["zone"], "b");
        assert_eq!(attrs[NODEGROUP_ATTR], "gpus");
        assert_eq!(attrs["namespaces"], json!(["ml-*"]));
        assert_eq!(attrs["maxAllocations"], 8);
        assert_eq!(attrs["oversubscribe"]["cpu"], 2.0);
        // The scheduler's own pool math: capacity, reservation, ratio.
        let pool = ResourcePool::from_node(&node).expect("pool");
        assert_eq!(pool.amount("cpu"), Some(32000.0));
        assert_eq!(pool.amount("memory"), Some(65536.0 - 1024.0));
        assert_eq!(pool.amount("gpu"), Some(4.0));
        assert_eq!(pool.amount("allocations"), Some(8.0));
    }

    #[test]
    fn a_member_copy_keeps_its_fingerprint_and_a_shape_only_lowers_it_test() {
        let member = Node {
            id: "w-1".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "drained".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "rack": "r1" }),
            fingerprint_attrs: json!({
                "memory_total": (8192u64 * 1024 * 1024).to_string(),
                "cpus": { "0": { "frequency": "2000" }, "1": { "frequency": "2000" } },
            }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        };
        let copy = synthetic_node(&TemplateSpec {
            name: "g".into(),
            like: Some(member.clone()),
            ..Default::default()
        });
        assert_eq!(copy.availability, "available");
        assert_eq!(copy.config_attrs["rack"], "r1");
        let pool = ResourcePool::from_node(&copy).expect("pool");
        assert_eq!(pool.amount("cpu"), Some(4000.0));
        let lowered = synthetic_node(&TemplateSpec {
            name: "g".into(),
            like: Some(member),
            shape: shape(3000, 99999),
            ..Default::default()
        });
        let pool = ResourcePool::from_node(&lowered).expect("pool");
        assert_eq!(pool.amount("cpu"), Some(3000.0));
        assert_eq!(pool.amount("memory"), Some(8192.0), "a ceiling only lowers");
    }

    #[test]
    fn first_fit_decreasing_counts_nodes_test() {
        let report = run(
            vec![
                row("web", task("web", 1000, 1024), 6, 0),
                row("big", task("big", 3000, 2048), 2, 0),
            ],
            &[template("g", shape(4000, 8192), 0)],
        );
        // big (3000) first: two nodes with 1000 MHz left, which take two web
        // replicas; four more web replicas fill one more node.
        let group = &report.groups[0];
        assert_eq!(group.nodes, 3, "{group:?}");
        assert_eq!(group.replicas, 8);
        assert_eq!(group.cpu_requested_mhz, 12000.0);
        assert_eq!(group.cpu_capacity_mhz, 12000.0);
        assert!(report.unfittable.is_empty());
    }

    #[test]
    fn static_ports_take_one_node_per_replica_and_a_clash_is_unfittable_test() {
        let mut lb = task("lb", 100, 64);
        lb["network"] = json!({ "ports": [{ "name": "http", "static": 8080 }] });
        let report = run(
            vec![row("lb", lb.clone(), 3, 0)],
            &[template("g", shape(4000, 8192), 0)],
        );
        assert_eq!(report.groups[0].nodes, 3);

        // Two tasks of one group claim the same port: never co-placeable.
        let mut a = lb.clone();
        a["group"] = json!("pair");
        let mut b = lb;
        b["name"] = json!("lb2");
        b["group"] = json!("pair");
        let report = run(
            vec![row("pair", a, 1, 0), row("pair", b, 1, 0)],
            &[template("g", shape(4000, 8192), 0)],
        );
        assert_eq!(report.units.len(), 1, "a task group is one unit");
        assert_eq!(report.unfittable.len(), 1);
        assert_eq!(report.unfittable[0].reasons[0].1, Rule::StaticPort);
    }

    #[test]
    fn a_group_packs_as_one_unit_test() {
        let mut a = task("a", 3000, 1024);
        a["group"] = json!("g");
        let mut b = task("b", 3000, 1024);
        b["group"] = json!("g");
        // Each fits a 4000 MHz node alone; together they never do.
        let report = run(
            vec![row("app", a, 1, 0), row("app", b, 1, 0)],
            &[template("small", shape(4000, 8192), 0)],
        );
        assert_eq!(report.groups[0].nodes, 0);
        let unfittable = &report.unfittable[0];
        assert_eq!(unfittable.reasons[0].1, Rule::Resources);
        assert!(
            unfittable.reasons[0].2.contains("cpu 6000 > 4000 per node"),
            "{}",
            unfittable.reasons[0].2
        );
    }

    #[test]
    fn a_gang_places_its_quorum_together_or_not_at_all_test() {
        let mut gang = task("worker", 500, 256);
        gang["gang"] = json!(true);
        gang["constraints"] = json!([{ "key": "zone", "operator": "distinct" }]);
        let mut zoned = shape(4000, 8192);
        zoned.attributes = Some([("zone".to_string(), "a".to_string())].into());
        let report = run(vec![row("mpi", gang, 3, 0)], &[template("g", zoned, 0)]);
        // distinct leaves one value, the quorum needs three: nothing places.
        assert_eq!(report.groups[0].nodes, 0);
        assert_eq!(report.groups[0].refusals[0].rule, Rule::Gang);
        assert_eq!(report.unfittable[0].replicas, 3);
        assert_eq!(report.units[0].gang_quorum, Some(3));
    }

    #[test]
    fn upcoming_nodes_absorb_work_across_templates_test() {
        let report = run(
            vec![row("web", task("web", 1000, 1024), 6, 0)],
            &[
                template("a", shape(4000, 8192), 1),
                template("b", shape(2000, 8192), 0),
            ],
        );
        assert_eq!(report.absorbed[0], 4);
        assert_eq!(report.groups[0].absorbed, 4);
        // Two left: one more node of a, or one of b.
        assert_eq!((report.groups[0].nodes, report.groups[0].replicas), (1, 2));
        assert_eq!((report.groups[1].nodes, report.groups[1].replicas), (1, 2));
    }

    #[test]
    fn constraint_and_permit_refusals_name_the_rule_test() {
        let mut gpu = task("train", 100, 64);
        gpu["constraints"] = json!({ "accel": "a100" });
        let mut other_pool = task("etl", 100, 64);
        other_pool["permit"] = json!({ "pool": "batch" });
        let report = run(
            vec![row("train", gpu, 1, 0), row("etl", other_pool, 1, 0)],
            &[template("g", shape(4000, 8192), 0)],
        );
        let rules: BTreeMap<&str, (Rule, String)> = report
            .unfittable
            .iter()
            .map(|u| {
                let (_, rule, detail) = &u.reasons[0];
                (
                    report.units[u.unit].job_id.as_str(),
                    (*rule, detail.clone()),
                )
            })
            .collect();
        assert_eq!(rules["train"].0, Rule::Constraint);
        assert!(
            rules["train"].1.contains(r#"accel = "a100""#),
            "{}",
            rules["train"].1
        );
        assert_eq!(rules["etl"].0, Rule::Permit);
        assert!(
            rules["etl"].1.contains(r#"pool "default""#),
            "{}",
            rules["etl"].1
        );
    }

    #[test]
    fn a_huge_backlog_is_truncated_not_unbounded_test() {
        let report = run(
            vec![row("web", task("web", 1, 1), MAX_SIMULATED_REPLICAS + 5, 0)],
            &[template("g", shape(1_000_000, 1_000_000), 0)],
        );
        assert!(report.truncated);
        assert_eq!(report.groups[0].replicas, MAX_SIMULATED_REPLICAS);
    }
}
