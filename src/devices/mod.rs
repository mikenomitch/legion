//! The device model: GPUs, FPGAs, NICs and other things a task needs whole
//! (docs/devices.md; plan: internal_docs/plans/device-model-and-plugin-seams-
//! plan.md).
//!
//! A node advertises **device groups**: identical devices from one source,
//! named `<vendor>/<type>/<model>` (`nvidia/gpu/A100-SXM4-80GB`), each with
//! **instances** that have an opaque id, a health bit, and attributes (NUMA
//! node, PCI bus id, NVLink peer group). Groups come from device plugins
//! (exec + JSON, protocol [`PROTOCOL`], see [`plugin`]) and from a worker
//! type's `devices.static` list.
//!
//! A task asks for devices in its resources:
//!
//! ```nickel
//! resources = { devices = [
//!   { name = "nvidia/gpu", count = 2,
//!     constraints = [{ key = "memoryMiB", operator = ">=", value = 40000 }] },
//! ] }
//! ```
//!
//! Where the pieces live:
//!
//! - **The advertisement** rides the worker's periodic node-check report,
//!   under the reserved key [`REPORT_KEY`] of the node's health checks
//!   ([`node_report`]). That path is status-lane, forwarded to the lane
//!   leader, and compared before it is written, so a steady report costs
//!   nothing.
//! - **Capacity**: [`crate::resources::ResourcePool::from_node`] turns each
//!   group's healthy instances into an item set under [`pool_key`], so the
//!   existing item arithmetic (usage, the plan-apply re-check, the cached
//!   usage aggregates) counts devices with no new code path.
//! - **Assignment** happens once, in the store's single allocation-insert
//!   choke point, inside the apply transaction: the chosen ids are written
//!   into the allocation's own `resources` as `devices[i].assigned`
//!   ([`write_assignments`]). Every later reader (the worker, quota, usage)
//!   reads the row.
//! - **Allocate** happens on the worker at task start ([`plugin`]): the
//!   plugin returns CDI container edits ([`cdi`]) that the container runtime
//!   applies to the OCI bundle and the `exec` runtime applies as env.

pub mod cdi;
pub mod plugin;
pub mod quota;

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::constraints::{AffinitySet, ConstraintSet};
use crate::prelude::*;

/// The protocol device plugins speak, sent as `protocol` on every request.
pub const PROTOCOL: &str = "legion-device/1";

/// The health-check key the worker reports its devices under.
pub const REPORT_KEY: &str = "devices";

/// The `resources` key a task's device requests live under.
pub const RESOURCES_KEY: &str = "devices";

/// Prefix of the resource-pool keys that hold one group's free instances.
pub const POOL_KEY_PREFIX: &str = "device:";

/// The `plugin` a statically configured group reports.
pub const STATIC_PLUGIN: &str = "static";

/// Most devices one request may ask for.
pub const MAX_REQUEST_COUNT: u32 = 1024;

/// Most device requests one task may make.
pub const MAX_REQUESTS: usize = 16;

fn default_true() -> bool {
    true
}

/// One group of identical devices on a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceGroup {
    /// `nvidia`, `amd`, `xilinx`, ... Lower case by convention.
    pub vendor: String,
    /// `gpu`, `fpga`, `nic`, ...
    #[serde(rename = "type")]
    pub kind: String,
    /// The model, without `/` (`A100-SXM4-80GB`).
    pub model: String,
    /// The plugin that reported the group, or `static`. Filled by the worker.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plugin: String,
    /// Attributes every instance shares (`memoryMiB`, `driver`).
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub attributes: Map<String, Value>,
    #[serde(default)]
    pub instances: Vec<DeviceInstance>,
}

/// One device.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceInstance {
    /// Opaque, unique within the group (a GPU UUID, a PCI address).
    pub id: String,
    /// Unhealthy instances are advertised but never assigned.
    #[serde(default = "default_true")]
    pub healthy: bool,
    /// Why it is unhealthy.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub health_detail: String,
    /// Per-instance attributes (`index`, `numaNode`, `pciBusId`,
    /// `peerGroup`); they win over the group's.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub attributes: Map<String, Value>,
    /// The fully qualified CDI device name (`nvidia.com/gpu=GPU-…`) whose
    /// spec, under the worker's CDI spec dirs, carries this device's
    /// container edits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cdi: Option<String>,
}

impl DeviceGroup {
    /// `<vendor>/<type>/<model>`.
    pub fn id(&self) -> String {
        format!("{}/{}/{}", self.vendor, self.kind, self.model)
    }

    /// The attributes a request's constraints see for one instance: the
    /// group's, then the instance's (which win), then `vendor`, `type`,
    /// `model`, and `id`.
    pub fn instance_attrs(&self, instance: &DeviceInstance) -> Map<String, Value> {
        let mut attrs = self.attributes.clone();
        for (key, value) in &instance.attributes {
            attrs.insert(key.clone(), value.clone());
        }
        attrs.insert("vendor".into(), Value::String(self.vendor.clone()));
        attrs.insert("type".into(), Value::String(self.kind.clone()));
        attrs.insert("model".into(), Value::String(self.model.clone()));
        attrs.insert("id".into(), Value::String(instance.id.clone()));
        attrs
    }

    /// A group as a plugin or a config may write it: every name part
    /// present and free of `/`, at least one instance, ids non-empty and
    /// unique.
    pub fn validate(&self) -> R<()> {
        for (what, part) in [
            ("vendor", &self.vendor),
            ("type", &self.kind),
            ("model", &self.model),
        ] {
            validate_name_part(what, part)?;
        }
        let mut seen = HashSet::new();
        for instance in &self.instances {
            if instance.id.trim().is_empty() || instance.id.trim() != instance.id {
                bail!(
                    "device group {} has an instance with an empty or padded id",
                    self.id()
                );
            }
            if !seen.insert(instance.id.as_str()) {
                bail!(
                    "device group {} lists instance {} twice",
                    self.id(),
                    instance.id
                );
            }
        }
        Ok(())
    }
}

fn validate_name_part(what: &str, part: &str) -> R<()> {
    if part.trim().is_empty() {
        bail!("device {what} must not be empty");
    }
    if part.trim() != part || part.contains('/') || part.contains(',') || part.contains('=') {
        bail!("device {what} {part:?} must not contain '/', ',', '=' or surrounding spaces");
    }
    Ok(())
}

/// The device pool key for a group id: `device:<vendor>/<type>/<model>`.
pub fn pool_key(group_id: &str) -> String {
    format!("{POOL_KEY_PREFIX}{group_id}")
}

/// What the worker reports under [`REPORT_KEY`]: every group it sees, and
/// how each plugin's last fingerprint went.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceReport {
    #[serde(default)]
    pub groups: Vec<DeviceGroup>,
    /// `plugin -> status`. Absent for a worker with no plugins.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub plugins: BTreeMap<String, PluginStatus>,
}

/// One plugin's last fingerprint.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginStatus {
    /// `ok` or `error`.
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    /// How many groups it reported.
    #[serde(default)]
    pub groups: usize,
}

impl DeviceReport {
    /// The report as a health-check value. Deterministic, so an unchanged
    /// report is byte-identical and the store skips the write.
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty() && self.plugins.is_empty()
    }
}

/// The device report a node last sent, empty when it sent none or the value
/// does not parse (a node whose report is garbage has no devices, rather
/// than failing every pool build).
pub fn node_report(node: &crate::structs::Node) -> DeviceReport {
    node.health_checks
        .get(REPORT_KEY)
        .and_then(|value| serde_json::from_value::<DeviceReport>(value.clone()).ok())
        .unwrap_or_default()
}

/// The node's device groups.
pub fn node_devices(node: &crate::structs::Node) -> Vec<DeviceGroup> {
    node_report(node).groups
}

/// Whether a health-check payload about to be stored carries a device report
/// different from the one `node` holds now (a node not yet known counts as
/// changed when the payload reports devices).
pub fn report_changed(node: Option<&crate::structs::Node>, health_checks: &str) -> bool {
    let incoming = serde_json::from_str::<Value>(health_checks)
        .ok()
        .and_then(|value| value.get(REPORT_KEY).cloned());
    let current = node.and_then(|node| node.health_checks.get(REPORT_KEY).cloned());
    incoming != current
}

// ── Names ────────────────────────────────────────────────────────────────

/// A device name as a request or a quota writes it: `type`, `vendor/type`,
/// or `vendor/type/model`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceName {
    pub vendor: Option<String>,
    pub kind: String,
    pub model: Option<String>,
}

impl DeviceName {
    pub fn parse(raw: &str) -> R<Self> {
        let parts: Vec<&str> = raw.split('/').collect();
        let name = match parts.as_slice() {
            [kind] => DeviceName {
                vendor: None,
                kind: kind.to_string(),
                model: None,
            },
            [vendor, kind] => DeviceName {
                vendor: Some(vendor.to_string()),
                kind: kind.to_string(),
                model: None,
            },
            [vendor, kind, model] => DeviceName {
                vendor: Some(vendor.to_string()),
                kind: kind.to_string(),
                model: Some(model.to_string()),
            },
            _ => bail!(
                "device name {raw:?} must be `type`, `vendor/type`, or `vendor/type/model` \
                 (for example `gpu` or `nvidia/gpu`)"
            ),
        };
        validate_name_part("type", &name.kind).with_context(|| format!("device name {raw:?}"))?;
        if let Some(vendor) = &name.vendor {
            validate_name_part("vendor", vendor).with_context(|| format!("device name {raw:?}"))?;
        }
        if let Some(model) = &name.model {
            validate_name_part("model", model).with_context(|| format!("device name {raw:?}"))?;
        }
        Ok(name)
    }

    /// Whether `group` is a device this name asks for.
    pub fn matches_group(&self, group: &DeviceGroup) -> bool {
        self.kind == group.kind
            && self.vendor.as_deref().is_none_or(|v| v == group.vendor)
            && self.model.as_deref().is_none_or(|m| m == group.model)
    }

    /// Whether the two names could describe the same device: every part
    /// both of them name is equal. Quota charges a request against a key
    /// this way, so `gpu` counts against an `nvidia/gpu` cap (it could be
    /// served by one) and `amd/gpu` does not.
    pub fn compatible(&self, other: &DeviceName) -> bool {
        fn agree(a: &Option<String>, b: &Option<String>) -> bool {
            match (a, b) {
                (Some(a), Some(b)) => a == b,
                _ => true,
            }
        }
        self.kind == other.kind
            && agree(&self.vendor, &other.vendor)
            && agree(&self.model, &other.model)
    }
}

impl std::fmt::Display for DeviceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(vendor) = &self.vendor {
            write!(f, "{vendor}/")?;
        }
        write!(f, "{}", self.kind)?;
        if let Some(model) = &self.model {
            write!(f, "/{model}")?;
        }
        Ok(())
    }
}

/// `LEGION_DEVICE_<NAME>_IDS` for a request name: upper-cased, every
/// character that is not a letter or digit as `_`.
pub fn ids_env_var(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("LEGION_DEVICE_{sanitized}_IDS")
}

// ── Requests ─────────────────────────────────────────────────────────────

/// One entry of `resources.devices`, as written (plus the assignment the
/// store adds to the allocation's copy).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceRequestSpec {
    pub name: String,
    #[serde(default = "default_count")]
    pub count: u32,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub constraints: Value,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub affinities: Value,
    /// Set by the scheduler on the allocation's copy; refused in a jobspec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assigned: Option<DeviceAssignment>,
}

fn default_count() -> u32 {
    1
}

/// The devices one request got.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceAssignment {
    /// The group, `<vendor>/<type>/<model>`.
    pub device: String,
    /// The plugin that owns the group (`static` for a configured group).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plugin: String,
    pub ids: Vec<String>,
}

/// A request not yet assigned, parsed for the scheduler.
#[derive(Debug, Clone)]
pub struct DeviceDemand {
    /// Position in `resources.devices`.
    pub index: usize,
    pub name: DeviceName,
    pub count: u32,
    pub constraints: ConstraintSet,
    pub affinities: AffinitySet,
}

/// The request list under `resources.devices`, as written. Absent or null
/// is no requests.
pub fn request_specs(resources: &Value) -> R<Vec<DeviceRequestSpec>> {
    match resources.get(RESOURCES_KEY) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                serde_json::from_value::<DeviceRequestSpec>(item.clone())
                    .with_context(|| format!("resources.devices[{index}]"))
            })
            .collect(),
        Some(_) => bail!("resources.devices must be a list of device requests"),
    }
}

/// Whether `resources` carries a device request the scheduler has not
/// assigned yet.
pub fn has_unassigned(resources: &Value) -> bool {
    resources
        .get(RESOURCES_KEY)
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("assigned").is_none_or(Value::is_null))
        })
}

/// What a request's parsed `resources.devices` contributes to the
/// scheduler's resource math: assigned ids as items under their group's
/// [`pool_key`], and unassigned requests as demands.
pub fn parse_for_scheduling(
    resources: &Value,
) -> R<(Vec<DeviceDemand>, Vec<(String, Vec<String>)>)> {
    let mut demands = Vec::new();
    let mut held = Vec::new();
    for (index, spec) in request_specs(resources)?.into_iter().enumerate() {
        match spec.assigned {
            Some(assigned) => held.push((pool_key(&assigned.device), assigned.ids)),
            None => demands.push(DeviceDemand {
                index,
                name: DeviceName::parse(&spec.name)
                    .with_context(|| format!("resources.devices[{index}]"))?,
                count: spec.count,
                constraints: ConstraintSet::new(&spec.constraints)
                    .with_context(|| format!("resources.devices[{index}].constraints"))?,
                affinities: AffinitySet::new(&spec.affinities)
                    .with_context(|| format!("resources.devices[{index}].affinities"))?,
            }),
        }
    }
    Ok((demands, held))
}

/// Submit-time validation of a task's device requests: names parse, counts
/// are 1..=[`MAX_REQUEST_COUNT`], constraints and affinities parse, no two
/// requests share a name or an env var, and none carries an assignment.
pub fn validate_requests(resources: &Value) -> R<()> {
    let specs = request_specs(resources)?;
    if specs.len() > MAX_REQUESTS {
        bail!(
            "resources.devices has {} requests; at most {MAX_REQUESTS} are allowed",
            specs.len()
        );
    }
    let mut env_vars: HashMap<String, String> = HashMap::new();
    for (index, spec) in specs.iter().enumerate() {
        let at = format!("resources.devices[{index}]");
        if spec.assigned.is_some() {
            bail!("{at}.assigned is set by the scheduler; remove it from the jobspec");
        }
        DeviceName::parse(&spec.name).with_context(|| at.clone())?;
        if spec.count == 0 || spec.count > MAX_REQUEST_COUNT {
            bail!(
                "{at}.count must be between 1 and {MAX_REQUEST_COUNT}, got {}",
                spec.count
            );
        }
        ConstraintSet::new(&spec.constraints).with_context(|| format!("{at}.constraints"))?;
        AffinitySet::new(&spec.affinities).with_context(|| format!("{at}.affinities"))?;
        let var = ids_env_var(&spec.name);
        if let Some(other) = env_vars.insert(var.clone(), spec.name.clone()) {
            bail!(
                "{at}: device requests {other:?} and {:?} would both set {var}; give each \
                 request a distinct name",
                spec.name
            );
        }
    }
    Ok(())
}

/// `resources` with each listed request's assignment written in, by the
/// request's position. The value the store keeps on the allocation row.
pub fn write_assignments(resources: &Value, assignments: &[(usize, DeviceAssignment)]) -> Value {
    let mut out = resources.clone();
    if let Some(items) = out.get_mut(RESOURCES_KEY).and_then(Value::as_array_mut) {
        for (index, assignment) in assignments {
            if let Some(Value::Object(item)) = items.get_mut(*index) {
                item.insert(
                    "assigned".into(),
                    serde_json::to_value(assignment).unwrap_or(Value::Null),
                );
            }
        }
    }
    out
}

/// The assignments recorded on an allocation's resources, with their
/// request names: `(name, assignment)`.
pub fn assignments(resources: &Value) -> Vec<(String, DeviceAssignment)> {
    request_specs(resources)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|spec| spec.assigned.map(|assigned| (spec.name, assigned)))
        .collect()
}

// ── Assignment ───────────────────────────────────────────────────────────

/// A placement's device requests cannot be met on its node: every matching
/// instance is taken, unhealthy, or fails the request's constraints. Typed
/// so the plan-apply walk can move to the next candidate node, the way it
/// does for a static port collision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevicesUnavailable {
    pub node_id: String,
    pub message: String,
}

impl DevicesUnavailable {
    pub fn new(node_id: &str, demands: &[DeviceDemand]) -> Self {
        let wanted: Vec<String> = demands
            .iter()
            .map(|demand| format!("{} x {}", demand.count, demand.name))
            .collect();
        DevicesUnavailable {
            node_id: node_id.to_string(),
            message: format!(
                "node {node_id} has no free healthy devices for {} (every matching instance \
                 is taken, unhealthy, or fails the request's constraints)",
                wanted.join(", ")
            ),
        }
    }
}

impl std::fmt::Display for DevicesUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DevicesUnavailable {}

/// The topology attributes assignment tries to keep a request inside, most
/// valuable first: an NVLink (or other peer) domain, then a NUMA node.
pub const TOPOLOGY_KEYS: [&str; 2] = ["peerGroup", "numaNode"];

/// A node's device inventory, as the resource pool keeps it.
#[derive(Debug, Clone, Default)]
pub struct NodeDeviceInventory {
    pub groups: Vec<DeviceGroup>,
}

impl NodeDeviceInventory {
    /// Choose devices for every demand, in order, from `free` (pool key ->
    /// free instance ids). Each demand is served from one group; demands see
    /// what earlier ones took. `None` when any demand cannot be met.
    pub fn assign(
        &self,
        free: &HashMap<String, HashSet<String>>,
        demands: &[DeviceDemand],
    ) -> Option<Vec<(usize, DeviceAssignment)>> {
        let mut taken: HashSet<(String, String)> = HashSet::new();
        let mut out = Vec::with_capacity(demands.len());
        for demand in demands {
            let mut best: Option<(Choice, &DeviceGroup)> = None;
            for group in &self.groups {
                if !demand.name.matches_group(group) {
                    continue;
                }
                let key = pool_key(&group.id());
                let Some(free_ids) = free.get(&key) else {
                    continue;
                };
                let candidates: Vec<Candidate> = group
                    .instances
                    .iter()
                    .enumerate()
                    .filter(|(_, instance)| {
                        instance.healthy
                            && free_ids.contains(&instance.id)
                            && !taken.contains(&(key.clone(), instance.id.clone()))
                    })
                    .filter_map(|(order, instance)| {
                        let attrs = group.instance_attrs(instance);
                        demand
                            .constraints
                            .is_satisfied_by_attrs(&attrs)
                            .then(|| Candidate {
                                id: instance.id.clone(),
                                order,
                                affinity: demand.affinities.score_attrs(&attrs),
                                attrs,
                            })
                    })
                    .collect();
                let Some(choice) = choose(candidates, demand.count as usize) else {
                    continue;
                };
                let better = match &best {
                    None => true,
                    Some((current, _)) => choice.rank() < current.rank(),
                };
                if better {
                    best = Some((choice, group));
                }
            }
            let (choice, group) = best?;
            let key = pool_key(&group.id());
            for id in &choice.ids {
                taken.insert((key.clone(), id.clone()));
            }
            out.push((
                demand.index,
                DeviceAssignment {
                    device: group.id(),
                    plugin: group.plugin.clone(),
                    ids: choice.ids,
                },
            ));
        }
        Some(out)
    }
}

struct Candidate {
    id: String,
    order: usize,
    affinity: f64,
    attrs: Map<String, Value>,
}

/// A choice within one group, with what ranks it against another group's.
struct Choice {
    ids: Vec<String>,
    /// How many topology keys the choice stays inside (higher is better).
    locality: usize,
    /// Summed affinity of the chosen instances (higher is better).
    affinity: f64,
    /// Size of the smallest domain that held it (smaller is better: best
    /// fit leaves bigger domains whole for bigger asks).
    domain: usize,
}

impl Choice {
    /// Lower sorts first: the job's affinities, then locality, then best
    /// fit.
    fn rank(&self) -> (i64, std::cmp::Reverse<usize>, usize) {
        (
            -(self.affinity * 1_000_000.0) as i64,
            std::cmp::Reverse(self.locality),
            self.domain,
        )
    }
}

/// Pick `count` of `candidates`: inside one topology domain when one is big
/// enough (the smallest such domain, then the best affinity), else the best
/// affinity across the group, in instance order on ties.
fn choose(mut candidates: Vec<Candidate>, count: usize) -> Option<Choice> {
    if candidates.len() < count {
        return None;
    }
    candidates.sort_by(|a, b| {
        b.affinity
            .partial_cmp(&a.affinity)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.order.cmp(&b.order))
    });
    for (level, key) in TOPOLOGY_KEYS.iter().enumerate() {
        let mut domains: BTreeMap<String, Vec<&Candidate>> = BTreeMap::new();
        for candidate in &candidates {
            if let Some(value) = candidate.attrs.get(*key).filter(|v| !v.is_null()) {
                domains
                    .entry(crate::constraints::value_token(value))
                    .or_default()
                    .push(candidate);
            }
        }
        let best = domains
            .values()
            .filter(|members| members.len() >= count)
            .map(|members| {
                let picked = &members[..count];
                (
                    members.len(),
                    picked.iter().map(|c| c.affinity).sum::<f64>(),
                    picked.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
                )
            })
            // The job's own preference first, then best fit.
            .min_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.0.cmp(&b.0))
            });
        if let Some((domain, affinity, ids)) = best {
            return Some(Choice {
                ids,
                locality: TOPOLOGY_KEYS.len() - level,
                affinity,
                domain,
            });
        }
    }
    let picked = &candidates[..count];
    Some(Choice {
        ids: picked.iter().map(|c| c.id.clone()).collect(),
        locality: 0,
        affinity: picked.iter().map(|c| c.affinity).sum(),
        domain: candidates.len(),
    })
}

/// Lines for `legion node info`: one per group, then one per instance.
pub fn render_node_devices(report: &DeviceReport) -> Vec<String> {
    let mut lines = Vec::new();
    if report.groups.is_empty() && report.plugins.is_empty() {
        return lines;
    }
    lines.push("Devices:".to_string());
    for group in &report.groups {
        let healthy = group.instances.iter().filter(|i| i.healthy).count();
        let attrs = if group.attributes.is_empty() {
            String::new()
        } else {
            format!("  {}", Value::Object(group.attributes.clone()))
        };
        lines.push(format!(
            "  {}  ({} healthy of {}, plugin {}){attrs}",
            group.id(),
            healthy,
            group.instances.len(),
            if group.plugin.is_empty() {
                "?"
            } else {
                group.plugin.as_str()
            }
        ));
        for instance in &group.instances {
            let health = if instance.healthy {
                "healthy".to_string()
            } else if instance.health_detail.is_empty() {
                "UNHEALTHY".to_string()
            } else {
                format!("UNHEALTHY: {}", instance.health_detail)
            };
            let attrs = if instance.attributes.is_empty() {
                String::new()
            } else {
                format!("  {}", Value::Object(instance.attributes.clone()))
            };
            lines.push(format!("    {}  {health}{attrs}", instance.id));
        }
    }
    for (plugin, status) in &report.plugins {
        if status.status != "ok" {
            lines.push(format!(
                "  plugin {plugin}: {} {}",
                status.status, status.error
            ));
        }
    }
    lines
}

#[cfg(test)]
mod tests;
