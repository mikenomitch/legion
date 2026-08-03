use std::collections::HashSet;

use serde_json::{json, Map, Value};

use crate::constraints::value_token;
use crate::prelude::*;
use crate::structs::config::ManagerConfigFull;
use crate::structs::{Allocation, Node};

const DEFAULT_TASK_MEMORY_MIB: f64 = 256.0;
const DEFAULT_TASK_CPU_MHZ: f64 = 100.0;
const DEFAULT_TASK_ALLOCATIONS: f64 = 1.0;

#[derive(Debug, Clone)]
pub struct ResourcePolicy {
    pub task_defaults: Map<String, Value>,
    pub required_custom_resources: HashSet<String>,
}

impl ResourcePolicy {
    pub fn from_manager_config(config: &ManagerConfigFull) -> R<Self> {
        let mut defaults = match config.task_resource_defaults.as_ref() {
            Some(Value::Object(map)) => map.clone(),
            Some(_) => bail!("task_resource_defaults must be an object"),
            None => default_task_resource_defaults(),
        };

        normalize_cpu_aliases(&mut defaults)?;

        if defaults.contains_key("cpu") && defaults.contains_key("cores") {
            bail!("task_resource_defaults cannot set both cpu and cores");
        }

        let required_custom_resources =
            parse_required_custom_resources(config.custom_resources.as_ref())?;

        Ok(ResourcePolicy {
            task_defaults: defaults,
            required_custom_resources,
        })
    }

    pub fn normalize_task_resources(&self, resources: &Value, enforce_required: bool) -> R<Value> {
        let map = match resources {
            Value::Null => Map::new(),
            Value::Object(map) => map.clone(),
            _ => bail!("task resources must be an object"),
        };

        let has_cpu = map.contains_key("cpu") || map.contains_key("cpus");
        let has_cores = map.contains_key("cores");

        if map.contains_key("cpu") && map.contains_key("cpus") {
            bail!("task resources cannot define both cpu and cpus");
        }

        if has_cpu && has_cores {
            bail!("task resources cannot define both cpu and cores");
        }

        let mut normalized = self.task_defaults.clone();
        normalize_cpu_aliases(&mut normalized)?;

        if map.contains_key("cpus") {
            normalized.insert("cpu".to_string(), map["cpus"].clone());
        }

        for (key, value) in map {
            if key == "cpus" {
                continue;
            }
            normalized.insert(key, value);
        }

        if has_cores {
            normalized.remove("cpu");
        } else if has_cpu {
            normalized.remove("cores");
        }

        if enforce_required {
            for required in &self.required_custom_resources {
                if !normalized.contains_key(required) {
                    bail!("task resources missing required custom resource {required}");
                }
            }
        }

        Ok(Value::Object(normalized))
    }
}

fn default_task_resource_defaults() -> Map<String, Value> {
    let mut defaults = Map::new();
    defaults.insert("memory".to_string(), json!(DEFAULT_TASK_MEMORY_MIB));
    defaults.insert("cpu".to_string(), json!(DEFAULT_TASK_CPU_MHZ));
    defaults.insert("allocations".to_string(), json!(DEFAULT_TASK_ALLOCATIONS));
    defaults
}

fn normalize_cpu_aliases(resources: &mut Map<String, Value>) -> R<()> {
    if resources.contains_key("cpu") && resources.contains_key("cpus") {
        bail!("resources cannot include both cpu and cpus");
    }

    if let Some(cpus) = resources.remove("cpus") {
        resources.insert("cpu".to_string(), cpus);
    }

    Ok(())
}

fn parse_required_custom_resources(resources: Option<&Value>) -> R<HashSet<String>> {
    let mut required = HashSet::new();
    let Some(resources) = resources else {
        return Ok(required);
    };

    let map = match resources {
        Value::Null => return Ok(required),
        Value::Object(map) => map,
        _ => bail!("custom_resources must be an object"),
    };

    for (name, value) in map {
        let is_required = match value {
            Value::Bool(required) => *required,
            Value::String(raw) => match raw.as_str() {
                "required" => true,
                "optional" => false,
                _ => bail!("custom_resources {name} must be 'required' or 'optional'"),
            },
            Value::Object(obj) => obj
                .get("required")
                .and_then(|val| val.as_bool())
                .context("custom_resources entries must include boolean required field")?,
            _ => bail!("custom_resources {name} must be a boolean or object"),
        };

        if is_required {
            required.insert(name.clone());
        }
    }

    Ok(required)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_defaults_apply_with_aliases() -> R<()> {
        let config = ManagerConfigFull {
            task_resource_defaults: Some(json!({
                "memory": 512,
                "cpus": 200,
            })),
            ..Default::default()
        };
        let policy = ResourcePolicy::from_manager_config(&config)?;
        let normalized = policy.normalize_task_resources(&json!({}), true)?;
        let normalized = normalized.as_object().context("missing object")?;

        assert_eq!(normalized["memory"], json!(512));
        assert_eq!(normalized["cpu"], json!(200));
        assert!(!normalized.contains_key("cpus"));

        Ok(())
    }

    #[test]
    fn required_custom_resources_are_enforced() -> R<()> {
        let config = ManagerConfigFull {
            custom_resources: Some(json!({
                "gpu": "required",
                "disk": "optional",
            })),
            ..Default::default()
        };
        let policy = ResourcePolicy::from_manager_config(&config)?;
        let err = policy
            .normalize_task_resources(&json!({}), true)
            .expect_err("expected required resource failure");

        assert!(err
            .to_string()
            .contains("missing required custom resource gpu"));

        Ok(())
    }

    fn node_with_fingerprint(fingerprint_attrs: Value) -> Node {
        Node {
            id: "node-a".into(),
            status: "active".into(),
            pool: "default".into(),
            availability: "available".into(),
            last_heartbeat: String::new(),
            heartbeat_source: String::new(),
            config_attrs: json!({}),
            fingerprint_attrs,
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }
    }

    #[test]
    fn memory_fingerprint_is_read_as_bytes() -> R<()> {
        // 32 GiB, the unit sysinfo::System::total_memory reports.
        let node = node_with_fingerprint(json!({ "memory_total": "34359738368" }));

        let memory_mib = fingerprint_memory_mib(&node)?;

        assert_eq!(memory_mib, 32.0 * 1024.0);
        Ok(())
    }

    #[test]
    fn implausible_cpu_frequency_falls_back_to_unknown() -> R<()> {
        // Apple Silicon reports "4" per core rather than a figure in MHz.
        // Summing it would advertise 40 MHz on a 10-core machine and reject
        // every task, so the frequency is discarded while cores are kept.
        let cpus = (1..=10)
            .map(|i| (format!("cpu-{i}"), json!({ "frequency": "4" })))
            .collect::<serde_json::Map<_, _>>();
        let node = node_with_fingerprint(json!({ "cpus": cpus }));

        let cpu = fingerprint_cpu(&node)?;

        assert_eq!(cpu.mhz, f64::INFINITY);
        // Core count survives, so it still bin-packs on its own.
        assert_eq!(cpu.cores, 10.0);
        Ok(())
    }

    #[test]
    fn plausible_cpu_frequency_sums_across_cores() -> R<()> {
        let node = node_with_fingerprint(json!({
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        }));

        let cpu = fingerprint_cpu(&node)?;

        assert_eq!(cpu.mhz, 4800.0);
        assert_eq!(cpu.cores, 2.0);
        Ok(())
    }

    #[test]
    fn absent_cpu_fingerprint_is_unknown() -> R<()> {
        let cpu = fingerprint_cpu(&node_with_fingerprint(json!({ "cpus": {} })))?;

        assert_eq!(cpu.mhz, f64::INFINITY);
        assert_eq!(cpu.cores, f64::INFINITY);
        Ok(())
    }

    #[test]
    fn free_for_node_is_total_and_empties_on_bad_data() {
        let policy =
            ResourcePolicy::from_manager_config(&ManagerConfigFull::default()).expect("policy");
        // Malformed node resources: an empty pool, not an error.
        let mut node = node_with_fingerprint(json!({}));
        node.config_attrs = json!({ "resources": "not-an-object" });
        let pool = ResourcePool::free_for_node(&node, &[], &policy);
        assert!(pool.values.is_empty());

        // Healthy node: usage subtracts from fingerprinted capacity.
        let node = node_with_fingerprint(json!({
            "memory_total": "1073741824",
            "cpus": { "cpu-1": { "frequency": "1000" } },
        }));
        let allocation = Allocation {
            id: 1,
            status: "running".into(),
            resources: json!({ "memory": 256, "cpu": 100 }),
            node_id: "node-a".into(),
            job_config_id: "jc".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
        };
        let pool = ResourcePool::free_for_node(&node, &[allocation], &policy);
        assert_eq!(pool.amount("memory"), Some(1024.0 - 256.0));
        assert_eq!(pool.amount("cpu"), Some(1000.0 - 100.0));
    }
}

// ---------------------------------------------------------------------------
// Resource values, requests, and per-node pools
// ---------------------------------------------------------------------------
//
// Crate-internal resource math shared by the scheduler (the plan-time
// `ResourceLedger`), the storage read model (cached free pools per node),
// and the scheduling index (free-capacity dims). Moved here from the
// scheduler once it grew those three consumers.

/// A resource quantity: a numeric amount (cpu MHz, memory MiB, disk MB,
/// custom counters) or a set of distinct items (device IDs).
#[derive(Debug, Clone)]
pub(crate) enum ResourceValue {
    Amount(f64),
    Items(HashSet<String>),
}

/// A task's normalized resource demands.
#[derive(Debug, Clone)]
pub(crate) struct ResourceRequest {
    pub(crate) values: HashMap<String, ResourceValue>,
}

/// One node's resource capacity (or free capacity, once usage is applied).
/// Resources the node did not fingerprint carry infinite capacity, so an
/// unfingerprinted node does not reject every task.
#[derive(Debug, Clone)]
pub(crate) struct ResourcePool {
    pub(crate) values: HashMap<String, ResourceValue>,
}

impl ResourceRequest {
    pub(crate) fn from_value(value: &Value) -> R<Self> {
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

impl ResourcePool {
    pub(crate) fn from_node(node: &Node) -> R<Self> {
        let mut values = HashMap::new();

        let memory_mib = fingerprint_memory_mib(node)?;
        values.insert("memory".to_string(), ResourceValue::Amount(memory_mib));

        let cpu = fingerprint_cpu(node)?;
        values.insert("cpu".to_string(), ResourceValue::Amount(cpu.mhz));
        values.insert("cores".to_string(), ResourceValue::Amount(cpu.cores));

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

        let mut pool = ResourcePool { values };

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

    /// The node's FREE pool: fingerprinted capacity minus every given
    /// allocation's normalized usage. This is the cached-per-node form the
    /// read model maintains and the plan-time ledger clones.
    ///
    /// Deliberately total (never errors): a node or allocation whose
    /// resources fail to parse yields an **empty** pool — the node fits
    /// nothing and shows up as zero candidates rather than failing every
    /// plan in the cluster (or a heartbeat) on one bad row. The warning
    /// names the node.
    pub(crate) fn free_for_node(
        node: &Node,
        allocations: &[Allocation],
        policy: &ResourcePolicy,
    ) -> ResourcePool {
        Self::free_from_rows(
            node,
            allocations
                .iter()
                .map(|allocation| (allocation.id, &allocation.resources)),
            policy,
        )
    }

    /// Same subtraction as [`ResourcePool::free_for_node`], from bare
    /// `(allocation id, resources)` rows. The plan-apply transaction uses
    /// this to rebuild a placement node's free pool from the rows the
    /// transaction itself can see, without materializing full
    /// [`Allocation`] values.
    pub(crate) fn free_from_rows<'a>(
        node: &Node,
        allocation_resources: impl IntoIterator<Item = (i64, &'a Value)>,
        policy: &ResourcePolicy,
    ) -> ResourcePool {
        let empty = || ResourcePool {
            values: HashMap::new(),
        };
        let mut pool = match ResourcePool::from_node(node) {
            Ok(pool) => pool,
            Err(error) => {
                warn!(
                    node_id = %node.id,
                    %error,
                    "Node resources failed to parse; node will fit no tasks until corrected"
                );
                return empty();
            }
        };
        for (allocation_id, resources) in allocation_resources {
            let usage = policy
                .normalize_task_resources(resources, false)
                .and_then(|normalized| ResourceRequest::from_value(&normalized));
            match usage {
                Ok(request) => pool.apply_usage(&request),
                Err(error) => {
                    warn!(
                        node_id = %node.id,
                        allocation_id,
                        %error,
                        "Allocation resources failed to parse; node will fit no tasks until corrected"
                    );
                    return empty();
                }
            }
        }
        pool
    }

    /// Free amount of one resource, if it is amount-typed. The scheduling
    /// index's view of `resources.*` dims.
    pub(crate) fn amount(&self, name: &str) -> Option<f64> {
        match self.values.get(name) {
            Some(ResourceValue::Amount(amount)) => Some(*amount),
            _ => None,
        }
    }

    pub(crate) fn can_fit(&self, request: &ResourceRequest) -> bool {
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

    pub(crate) fn consume(&mut self, request: &ResourceRequest) {
        self.apply_usage(request);
    }

    /// Give a request's resources back — the inverse of [`Self::consume`].
    ///
    /// Used by preemption to model "what would this node's free capacity be
    /// if these victims were gone", without mutating the plan's ledger until
    /// the victim set is actually chosen.
    ///
    /// A resource the pool does not track is *not* invented: an untracked
    /// amount stays untracked (the node reported no capacity for it, and
    /// returning a victim's usage cannot conjure any), and items are only
    /// returned to a set that exists.
    pub(crate) fn release(&mut self, request: &ResourceRequest) {
        for (name, usage) in &request.values {
            match usage {
                ResourceValue::Amount(amount) => {
                    if let Some(ResourceValue::Amount(available)) = self.values.get_mut(name) {
                        *available += amount;
                    }
                }
                ResourceValue::Items(items) => {
                    if let Some(ResourceValue::Items(available)) = self.values.get_mut(name) {
                        for item in items {
                            available.insert(item.clone());
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn apply_usage(&mut self, request: &ResourceRequest) {
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

/// Workers fingerprint `memory_total` straight from
/// `sysinfo::System::total_memory`, which reports bytes.
const BYTES_PER_MIB: f64 = 1024.0 * 1024.0;

/// Per-core frequencies below this are not plausible for a real CPU and mean
/// the platform did not report a usable value. `sysinfo` yields `0` where the
/// frequency is unavailable, and on Apple Silicon it reports a small
/// single-digit number (`4`) rather than MHz. Treating those as real capacity
/// leaves a 10-core Mac advertising 40 MHz in total, which cannot satisfy even
/// the built-in 100 MHz per-task default, so nothing schedules at all.
const MIN_PLAUSIBLE_CPU_MHZ: f64 = 100.0;

/// Node CPU capacity derived from a worker's fingerprint. `INFINITY` means
/// the platform reported nothing usable for that figure — treated as
/// unlimited so an unfingerprinted node does not silently reject every task.
/// Frequency and core count are tracked separately because a platform can
/// report one without the other: core count is reliable everywhere, while
/// the per-core frequency is not.
struct CpuFingerprint {
    /// Summed clock across all cores in MHz, or `INFINITY` when unknown.
    mhz: f64,
    /// Number of cores, or `INFINITY` when unknown.
    cores: f64,
}

impl CpuFingerprint {
    fn unknown() -> Self {
        CpuFingerprint {
            mhz: f64::INFINITY,
            cores: f64::INFINITY,
        }
    }
}

/// Fingerprinted memory in MiB, or `INFINITY` when the node reported none.
fn fingerprint_memory_mib(node: &Node) -> R<f64> {
    let total = node
        .fingerprint_attrs
        .get("memory_total")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<f64>().ok());

    match total {
        Some(total) => Ok(total / BYTES_PER_MIB),
        None => Ok(f64::INFINITY),
    }
}

fn fingerprint_cpu(node: &Node) -> R<CpuFingerprint> {
    let cpus = node
        .fingerprint_attrs
        .get("cpus")
        .and_then(|val| val.as_object())
        .filter(|cpus| !cpus.is_empty());

    let Some(cpus) = cpus else {
        return Ok(CpuFingerprint::unknown());
    };

    let mut total_mhz = 0.0;
    let mut mhz_unknown = false;
    for cpu in cpus.values() {
        let freq = cpu.get("frequency").and_then(|freq| {
            freq.as_str()
                .and_then(|freq| freq.parse::<f64>().ok())
                .or_else(|| freq.as_f64())
        });

        match freq {
            Some(freq) if freq >= MIN_PLAUSIBLE_CPU_MHZ => total_mhz += freq,
            // One bogus core makes the total meaningless, so drop the whole
            // figure rather than advertise a partial sum.
            _ => {
                mhz_unknown = true;
                break;
            }
        }
    }

    Ok(CpuFingerprint {
        mhz: if mhz_unknown {
            f64::INFINITY
        } else {
            total_mhz
        },
        cores: cpus.len() as f64,
    })
}

fn config_value<'a>(attrs: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| attrs.get(*key))
}

pub(crate) fn is_builtin_resource(name: &str) -> bool {
    matches!(name, "memory" | "cpu" | "cores" | "allocations")
}
