//! A node group's members, as node scale-in sees them (`GetCapacityDemand`
//! with `members`; docs/autoscaling.md, "Node groups"; autoscaler-v2 plan
//! §6.11).
//!
//! For every node whose `legion.nodegroup` is the group's name: its
//! provider id and `legion.scale-in` tag, its reserved share, and each
//! allocation with the facts the never-victim rules read:
//!
//! - **kind**: a session envelope (with its live sessions), a per-node
//!   `count.per` allocation, or ordinary work;
//! - **node-local storage**: its instance holds a node-local volume;
//! - **`disruption.minAvailable`**: the task's live allocations, less the
//!   ones on this node, would fall under its floor (the stricter of
//!   `minAvailable` against its effective count and a gang's quorum), the
//!   same floor preemption honors.
//!
//! With `fit_check`, each live, schedulable member under half reserved is
//! also simulated: would every allocation on it fit on the group's other
//! live, schedulable members, with their free capacity, by the demand's
//! own rules ([`capacity_demand::fit_on`])? That is `down.when =
//! "underutilized"`'s "and its allocations fit elsewhere".

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;

use crate::autoscaler::nodes::UNDERUTILIZED_BELOW;
use crate::manager::capacity_demand;
use crate::prelude::*;
use crate::resources::ResourcePool;
use crate::storage::store::{NodeAllocationRow, PendingTaskRow, Store};
use crate::structs::{Node, TaskConfig, NODEGROUP_ATTR, PROVIDER_ID_ATTR, SCALE_IN_ATTR};

/// One allocation on a member.
#[derive(Debug, Clone, PartialEq)]
pub struct MemberAllocation {
    pub id: i64,
    pub job_id: String,
    pub namespace: String,
    pub group: String,
    pub task: String,
    /// `work`, `session-envelope`, or `per-node`.
    pub kind: &'static str,
    /// A session envelope's live sessions.
    pub sessions: u32,
    pub node_local_storage: bool,
    /// Why draining it now breaks `disruption.minAvailable`, when it does.
    pub min_available: Option<String>,
}

/// One member.
#[derive(Debug, Clone)]
pub struct Member {
    pub node: Node,
    pub provider_id: Option<String>,
    pub scale_in: Option<String>,
    pub reserved_cpu: f64,
    pub reserved_memory: f64,
    pub allocations: Vec<MemberAllocation>,
    /// The fits-elsewhere simulation, when run: the nodes the work would
    /// land on, or what does not fit.
    pub fits: Option<Result<Vec<String>, String>>,
}

fn text_attr(node: &Node, key: &str) -> Option<String> {
    node.config_attrs
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// The members of node group `group`, sorted by node id.
pub fn members(store: &Store, group: &str, fit_check: bool) -> R<Vec<Member>> {
    let every = store.get_nodes()?;
    let mut nodes: Vec<Node> = every
        .iter()
        .filter(|node| text_attr(node, NODEGROUP_ATTR).as_deref() == Some(group))
        .cloned()
        .collect();
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    if nodes.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<&str> = nodes.iter().map(|node| node.id.as_str()).collect();
    let rows = store.allocations_on_nodes(&ids)?;

    // The task of every allocation, by its config.
    let mut tasks: HashMap<i64, TaskConfig> = HashMap::new();
    let configs: BTreeSet<&str> = rows.iter().map(|row| row.job_config_id.as_str()).collect();
    for config in configs {
        for (id, task) in store.get_task_configs_for_job_config(config)? {
            tasks.insert(id, task);
        }
    }
    // Where each task's allocations are, fleet-wide, across versions.
    let jobs: Vec<String> = rows
        .iter()
        .map(|row| row.job_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let placed = store.task_allocation_nodes(&jobs)?;
    // Each task's floor: minAvailable against its effective count, or a
    // gang's quorum, whichever is higher.
    let mut floors: HashMap<(String, String, String), u32> = HashMap::new();
    let policy = &store.resource_policy;

    let mut out = Vec::with_capacity(nodes.len());
    for node in &nodes {
        let here: Vec<&NodeAllocationRow> = rows.iter().filter(|row| row.node_id == node.id).collect();
        let mut allocations = Vec::with_capacity(here.len());
        let (mut used_cpu, mut used_memory) = (0.0, 0.0);
        for row in &here {
            if let Ok(request) = policy.usage_request(&row.resources) {
                used_cpu += request.amount("cpu").unwrap_or(0.0);
                used_memory += request.amount("memory").unwrap_or(0.0);
            }
            let namespace = crate::structs::namespace::job_namespace(&row.job_id).to_string();
            let Some(task) = tasks.get(&row.task_config_id) else {
                allocations.push(MemberAllocation {
                    id: row.id,
                    job_id: row.job_id.clone(),
                    namespace,
                    group: String::new(),
                    task: String::new(),
                    kind: "work",
                    sessions: 0,
                    node_local_storage: row.node_local,
                    min_available: None,
                });
                continue;
            };
            let (kind, sessions) = match crate::structs::session::envelope_serves(task) {
                Some(served) => (
                    "session-envelope",
                    here.iter()
                        .filter(|other| {
                            other.job_id == row.job_id
                                && tasks
                                    .get(&other.task_config_id)
                                    .is_some_and(|t| t.name == served)
                        })
                        .count() as u32,
                ),
                None if task.count.per.is_some() => ("per-node", 0),
                None => ("work", 0),
            };
            let key = (row.job_id.clone(), task.group.clone(), task.name.clone());
            let floor = match floors.get(&key) {
                Some(floor) => *floor,
                None => {
                    let count = store
                        .effective_task_count(&row.job_id, &task.group, &task.name, task.count.num)
                        .unwrap_or(task.count.num);
                    let floor = task
                        .disruption
                        .as_ref()
                        .map(|disruption| disruption.min_available_for(count))
                        .unwrap_or(0)
                        .max(task.gang.as_ref().map(|gang| gang.preemption_floor()).unwrap_or(0));
                    floors.insert(key.clone(), floor);
                    floor
                }
            };
            let min_available = if kind == "work" && floor > 0 {
                let live = placed.get(&key).map_or(0, Vec::len) as u32;
                let on_here = here
                    .iter()
                    .filter(|other| {
                        other.job_id == row.job_id
                            && tasks.get(&other.task_config_id).is_some_and(|t| {
                                t.group == task.group && t.name == task.name
                            })
                    })
                    .count() as u32;
                (live.saturating_sub(on_here) < floor).then(|| {
                    format!(
                        "{}/{} has {live} live, {on_here} on this node, and a floor of {floor}",
                        row.job_id, task.name
                    )
                })
            } else {
                None
            };
            allocations.push(MemberAllocation {
                id: row.id,
                job_id: row.job_id.clone(),
                namespace,
                group: task.group.clone(),
                task: task.name.clone(),
                kind,
                sessions,
                node_local_storage: row.node_local,
                min_available,
            });
        }
        let pool = ResourcePool::from_node(node).ok();
        let share = |used: f64, name: &str| {
            pool.as_ref()
                .and_then(|pool| pool.amount(name))
                .filter(|total| total.is_finite() && *total > 0.0)
                .map_or(0.0, |total| (used / total).clamp(0.0, 1.0))
        };
        out.push(Member {
            node: node.clone(),
            provider_id: text_attr(node, PROVIDER_ID_ATTR),
            scale_in: text_attr(node, SCALE_IN_ATTR),
            reserved_cpu: share(used_cpu, "cpu"),
            reserved_memory: share(used_memory, "memory"),
            allocations,
            fits: None,
        });
    }

    if fit_check {
        let live = |member: &Member| {
            member.node.status == "active" && member.node.availability == "available"
        };
        // Every live member's free capacity.
        let free: BTreeMap<String, ResourcePool> = out
            .iter()
            .filter(|member| live(member))
            .filter_map(|member| {
                let mut pool = ResourcePool::from_node(&member.node).ok()?;
                for row in rows.iter().filter(|row| row.node_id == member.node.id) {
                    if let Ok(request) = policy.usage_request(&row.resources) {
                        pool.consume(&request);
                    }
                }
                Some((member.node.id.clone(), pool))
            })
            .collect();
        let all_nodes: HashMap<String, Node> = every
            .iter()
            .map(|node| (node.id.clone(), node.clone()))
            .collect();
        for index in 0..out.len() {
            let member = &out[index];
            if !live(member)
                || member.reserved_cpu.max(member.reserved_memory) >= UNDERUTILIZED_BELOW
            {
                continue;
            }
            // The work to move: ordinary allocations, by task.
            let mut moving: BTreeMap<i64, u32> = BTreeMap::new();
            for allocation in member.allocations.iter().filter(|a| a.kind == "work") {
                if let Some(row) = rows.iter().find(|row| row.id == allocation.id) {
                    *moving.entry(row.task_config_id).or_default() += 1;
                }
            }
            let mut pending = Vec::new();
            for (task_config_id, count) in moving {
                let Some(task) = tasks.get(&task_config_id) else {
                    continue;
                };
                let Some(row) = rows.iter().find(|row| row.task_config_id == task_config_id) else {
                    continue;
                };
                let key = (row.job_id.clone(), task.group.clone(), task.name.clone());
                let elsewhere: Vec<String> = placed
                    .get(&key)
                    .map(|nodes| {
                        nodes
                            .iter()
                            .filter(|node| **node != member.node.id)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                pending.push(PendingTaskRow {
                    job_id: row.job_id.clone(),
                    job_config_id: row.job_config_id.clone(),
                    task: task.clone(),
                    desired: elsewhere.len() as u32 + count,
                    placed: elsewhere.len() as u32,
                    pending: count,
                    placed_nodes: elsewhere,
                });
            }
            let targets: Vec<(Node, ResourcePool)> = out
                .iter()
                .filter(|other| other.node.id != member.node.id)
                .filter_map(|other| {
                    free.get(&other.node.id)
                        .map(|pool| (other.node.clone(), pool.clone()))
                })
                .collect();
            let fits = capacity_demand::fit_on(&pending, &targets, policy, &all_nodes);
            out[index].fits = Some(fits);
        }
    }
    Ok(out)
}
