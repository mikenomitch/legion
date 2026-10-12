use std::collections::{BTreeMap, HashSet};

use serde_json::{json, Map, Value};

use crate::constraints::value_token;
use crate::prelude::*;
use crate::structs::config::ManagerConfigFull;
use crate::structs::{Allocation, Node};

/// The `(id, resources)` rows of the allocations that hold node capacity.
///
/// A terminal allocation (`complete` or `failed`: its process exited) keeps
/// its row so its task's slot stays spent, but it no longer uses the node's
/// CPU, memory, disk, or allocation count, so it is left out of every usage
/// sum: the read model's free pools, the plan-time ledger, and the
/// plan-apply transaction's capacity check. A DAG's finished steps would
/// otherwise keep their resources until the job stopped, and the next step
/// could not fit (docs/jobs-tasks-and-scheduling.md).
pub(crate) fn capacity_rows(allocations: &[Allocation]) -> impl Iterator<Item = (i64, &Value)> {
    allocations
        .iter()
        .filter(|allocation| !crate::constants::allocation_status_is_terminal(&allocation.status))
        .map(|allocation| (allocation.id, &allocation.resources))
}

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
            // Checked against the NORMALIZED memory, so a task that leaves
            // `memory` to `taskResourceDefaults` cannot set a ceiling below
            // the default it will be reserved at. Submit-path only: a stored
            // row is never re-judged, or raising the default would make every
            // node holding an old row fit nothing.
            validate_memory_max(&normalized)?;
        }

        Ok(Value::Object(normalized))
    }

    /// One allocation's stored resources as the normalized
    /// [`ResourceRequest`] the usage aggregates and free-pool subtractions
    /// consume — the exact normalization [`NodeUsage::from_rows`] and
    /// [`ResourcePool::free_from_rows`] apply per row.
    pub(crate) fn usage_request(&self, resources: &Value) -> R<ResourceRequest> {
        let normalized = self.normalize_task_resources(resources, false)?;
        ResourceRequest::from_value(&normalized)
    }

    /// A canonical, deterministic rendering of everything in this policy
    /// that changes what [`Self::normalize_task_resources`] produces.
    ///
    /// This is the era tag on every persisted `node_alloc_usage` row: the
    /// cached aggregate is rows run through *some* policy's normalization,
    /// and the row-set fingerprint (epoch, count, id sum) cannot see a
    /// POLICY change — an operator raising `taskResourceDefaults.memory`
    /// and restarting would otherwise keep serving aggregates computed
    /// under the old default, understating usage on every quiet node
    /// indefinitely, which is silent overcommit. A cached row whose
    /// fingerprint does not match the running policy is simply never
    /// served, so a policy change costs one rebuild per node.
    ///
    /// Deliberately the readable canonical string rather than a hash of
    /// it: it is written once per cache publish, compared byte-wise, and a
    /// human debugging a rebuild storm can read WHICH policy wrote a row.
    /// Keys are sorted so construction order can never produce two
    /// spellings of one policy.
    pub(crate) fn fingerprint(&self) -> String {
        let defaults: std::collections::BTreeMap<&String, &Value> =
            self.task_defaults.iter().collect();
        let mut required: Vec<&String> = self.required_custom_resources.iter().collect();
        required.sort_unstable();
        let rendered = serde_json::json!({
            "defaults": defaults,
            "required": required,
            // Bumped when what an aggregate contains changes shape, not just
            // its inputs. v2: `memoryMax` is a ceiling, not a dimension, so it
            // is no longer summed (an aggregate cached before that would carry
            // a stray `memoryMax` dim that fits nothing). Oversubscription
            // ratios are deliberately NOT here: they scale capacity, which is
            // read from the fresh node row on every pool build, never from
            // this cache, so a ratio change invalidates nothing it needs to.
            "aggregate": 2,
        });
        rendered.to_string()
    }
}

/// The task resource key for the burstable ceiling (MiB). A ceiling, not a
/// scheduled dimension: the scheduler packs on `memory`, and `memoryMax` only
/// reaches the worker, which enforces it as `memory.max`.
pub(crate) const MEMORY_MAX_KEY: &str = "memoryMax";

/// `memoryMax` must be a positive number no smaller than `memory`. The error
/// spells out the fix, because the two fields are easy to swap.
pub(crate) fn validate_memory_max(resources: &Map<String, Value>) -> R<()> {
    let Some(raw) = resources.get(MEMORY_MAX_KEY) else {
        return Ok(());
    };
    let Some(max) = raw.as_f64().filter(|max| max.is_finite() && *max > 0.0) else {
        bail!("resources.memoryMax must be a positive number of MiB, got {raw}");
    };
    let memory = resources
        .get("memory")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    if max < memory {
        bail!(
            "resources.memoryMax ({max} MiB) is below resources.memory ({memory} MiB). \
             `memory` is what the scheduler reserves and `memoryMax` is the ceiling the task \
             may burst to, so the ceiling must be at least the reservation. Raise memoryMax, \
             or lower memory"
        );
    }
    Ok(())
}

/// What a suspended instance keeps charged to its node (docs/
/// suspend-and-resume.md, "Capacity for resumes"). The charge rides the
/// allocation row the suspend would otherwise delete, so the scheduler's fit
/// check, the read model, and the plan-apply recheck all count it with no
/// new code path.
#[derive(Debug, Clone, PartialEq)]
pub enum SuspendedHold {
    /// Nothing is held: the allocation row is deleted.
    Nothing,
    /// The whole reservation, exclusive cores and devices included: the row
    /// stays exactly as it is.
    Full,
    /// A reduced charge: the row stays with these resources. Only `memory`
    /// and `cpu` (MHz) are non-zero. Every other dimension (cores, devices,
    /// custom resources, the `allocations` slot) is written as zero or empty,
    /// explicitly, so `taskResourceDefaults` cannot fill it back in when the
    /// row is normalized.
    Partial(Value),
}

impl ResourcePolicy {
    /// The charge a suspended instance leaves on its node, from its job's
    /// `suspend.reserve`. A ratio scales `memory` and `cpu` MHz. A fixed
    /// amount is capped at what the task reserves. Exclusive `cores` and
    /// devices are held only by a full hold.
    pub fn suspended_hold(
        &self,
        resources: &Value,
        reserve: crate::structs::suspend::SuspendReserve,
    ) -> R<SuspendedHold> {
        use crate::structs::suspend::SuspendReserve;
        if reserve.is_full() {
            return Ok(SuspendedHold::Full);
        }
        let normalized = self.normalize_task_resources(resources, false)?;
        let map = normalized
            .as_object()
            .context("normalized resources must be an object")?;
        let amount = |name: &str| map.get(name).and_then(Value::as_f64).unwrap_or(0.0);
        let (memory, cpu) = (amount("memory"), amount("cpu"));
        let (held_memory, held_cpu) = match reserve {
            SuspendReserve::Ratio(ratio) => (memory * ratio, cpu * ratio),
            SuspendReserve::Amount {
                memory_mib,
                cpu_mhz,
            } => (memory_mib.min(memory), cpu_mhz.min(cpu)),
        };
        if held_memory <= 0.0 && held_cpu <= 0.0 {
            return Ok(SuspendedHold::Nothing);
        }
        let mut held = Map::new();
        for (key, value) in map {
            let value = match key.as_str() {
                "memory" => json!(held_memory),
                "cpu" => json!(held_cpu),
                MEMORY_MAX_KEY => continue,
                _ if value.is_number() => json!(0),
                // Device lists and other item resources: release them all.
                _ => json!([]),
            };
            held.insert(key.clone(), value);
        }
        Ok(SuspendedHold::Partial(Value::Object(held)))
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

    mod suspended_hold_test {
        use super::*;
        use crate::structs::suspend::SuspendReserve;

        fn policy() -> ResourcePolicy {
            ResourcePolicy::from_manager_config(&ManagerConfigFull::default()).expect("policy")
        }

        fn partial(hold: SuspendedHold) -> Map<String, Value> {
            match hold {
                SuspendedHold::Partial(Value::Object(map)) => map,
                other => panic!("expected a partial hold, got {other:?}"),
            }
        }

        /// The held row through the scheduler's own normalization: what the
        /// fit check will actually subtract.
        fn charged(policy: &ResourcePolicy, map: Map<String, Value>) -> ResourceRequest {
            policy
                .usage_request(&Value::Object(map))
                .expect("held row parses")
        }

        #[test]
        fn a_ratio_scales_memory_and_cpu_and_releases_the_rest() {
            let policy = policy();
            let resources =
                json!({ "memory": 2048, "memoryMax": 4096, "cpu": 1000, "gpu": ["g0"] });
            let held = partial(
                policy
                    .suspended_hold(&resources, SuspendReserve::Ratio(0.25))
                    .unwrap(),
            );
            assert_eq!(held["memory"], json!(512.0));
            assert_eq!(held["cpu"], json!(250.0));
            assert_eq!(held["gpu"], json!([]), "devices are released below 1.0");
            assert_eq!(held["allocations"], json!(0), "the slot is released");
            assert!(!held.contains_key(MEMORY_MAX_KEY));
            let request = charged(&policy, held);
            assert_eq!(request.amount("memory"), Some(512.0));
            assert_eq!(request.amount("cpu"), Some(250.0));
            assert_eq!(request.amount("allocations"), Some(0.0));
        }

        #[test]
        fn cores_are_held_only_at_one() {
            let policy = policy();
            let resources = json!({ "memory": 1024, "cores": 2 });
            let held = partial(
                policy
                    .suspended_hold(&resources, SuspendReserve::Ratio(0.5))
                    .unwrap(),
            );
            assert_eq!(held["cores"], json!(0));
            assert!(
                !held.contains_key("cpu"),
                "a cores task holds no cpu MHz, and the default must not return"
            );
            let request = charged(&policy, held);
            assert_eq!(request.amount("cpu"), None);
            assert_eq!(request.amount("cores"), Some(0.0));
            assert_eq!(
                policy
                    .suspended_hold(&resources, SuspendReserve::FULL)
                    .unwrap(),
                SuspendedHold::Full
            );
        }

        #[test]
        fn an_amount_is_capped_at_the_reservation() {
            let policy = policy();
            let resources = json!({ "memory": 2048, "cpu": 1000 });
            let held = partial(
                policy
                    .suspended_hold(
                        &resources,
                        SuspendReserve::Amount {
                            memory_mib: 1024.0,
                            cpu_mhz: 5000.0,
                        },
                    )
                    .unwrap(),
            );
            assert_eq!(held["memory"], json!(1024.0));
            assert_eq!(held["cpu"], json!(1000.0));
        }

        /// The fit check, end to end on a pool: a node full of running work
        /// with one instance suspended at 0.25 has three quarters of that
        /// instance's reservation free again.
        #[test]
        fn a_partial_hold_frees_the_rest_in_the_pool() {
            let policy = policy();
            let mut pool = ResourcePool {
                values: HashMap::from([
                    ("memory".to_string(), ResourceValue::Amount(4096.0)),
                    ("cpu".to_string(), ResourceValue::Amount(4000.0)),
                    ("allocations".to_string(), ResourceValue::Amount(2.0)),
                ]),
            };
            let running = policy
                .usage_request(&json!({ "memory": 2048, "cpu": 2000 }))
                .unwrap();
            let held = partial(
                policy
                    .suspended_hold(
                        &json!({ "memory": 2048, "cpu": 2000 }),
                        SuspendReserve::Ratio(0.25),
                    )
                    .unwrap(),
            );
            pool.consume(&running);
            pool.consume(&charged(&policy, held));
            assert_eq!(pool.amount("memory"), Some(4096.0 - 2048.0 - 512.0));
            assert_eq!(pool.amount("allocations"), Some(1.0));
            let resume = policy
                .usage_request(&json!({ "memory": 2048, "cpu": 2000 }))
                .unwrap();
            assert!(
                !pool.can_fit(&resume),
                "1536 MiB free: the resume needs 2048"
            );
            let small = policy
                .usage_request(&json!({ "memory": 1536, "cpu": 1500 }))
                .unwrap();
            assert!(pool.can_fit(&small));
        }
    }

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

    fn node_with_attrs(config_attrs: Value) -> Node {
        Node {
            config_attrs,
            ..node_with_fingerprint(json!({
                "memory_total": (8192u64 * 1024 * 1024).to_string(),
                "cpus": { "0": { "frequency": "2000" }, "1": { "frequency": "2000" } },
            }))
        }
    }

    /// `(physical - reservedResources) x ratio`, for memory and CPU MHz,
    /// never for cores. Junk ratios read as 1.0.
    #[test]
    fn oversubscription_scales_capacity_after_reserved_resources() -> R<()> {
        let pool = ResourcePool::from_node(&node_with_attrs(json!({
            "reservedResources": { "memory": 1024 },
            "oversubscribe": { "memory": 1.5, "cpu": 2.0 },
        })))?;
        assert_eq!(pool.amount("memory"), Some(7168.0 * 1.5));
        assert_eq!(pool.amount("cpu"), Some(8000.0));
        assert_eq!(pool.amount("cores"), Some(2.0));

        for junk in [
            json!({ "memory": 0.5 }),
            json!({ "memory": "lots" }),
            json!(3),
        ] {
            let pool = ResourcePool::from_node(&node_with_attrs(json!({ "oversubscribe": junk })))?;
            assert_eq!(pool.amount("memory"), Some(8192.0));
        }
        let capacity = NodeCapacity::of(&node_with_attrs(json!({
            "oversubscribe": { "memory": 2.0 },
        })))?;
        assert_eq!(capacity.memory_mib, Some(16384.0));
        assert_eq!(capacity.ratios.memory, 2.0);
        assert_eq!(capacity.ratios.cpu, 1.0);
        Ok(())
    }

    /// The cached per-node aggregate holds allocation demand only, and the
    /// free pool is rebuilt from the node row each time, so a re-registration
    /// that changes the ratio changes the pool with the very same cached
    /// aggregate: nothing stale can overcommit further.
    #[test]
    fn a_ratio_change_reaches_a_pool_built_from_a_cached_aggregate() -> R<()> {
        let policy = ResourcePolicy::from_manager_config(&ManagerConfigFull::default())?;
        let rows = [(1, json!({ "memory": 4096, "memoryMax": 8192 }))];
        let usage = NodeUsage::from_rows(rows.iter().map(|(id, value)| (*id, value)), &policy)?;
        assert!(
            !usage.dims.contains_key(MEMORY_MAX_KEY),
            "a ceiling is never summed"
        );
        let plain = ResourcePool::free_from_usage(&node_with_attrs(json!({})), &usage);
        let packed = ResourcePool::free_from_usage(
            &node_with_attrs(json!({ "oversubscribe": { "memory": 1.5 } })),
            &usage,
        );
        assert_eq!(plain.amount("memory"), Some(4096.0));
        assert_eq!(packed.amount("memory"), Some(12288.0 - 4096.0));
        Ok(())
    }

    #[test]
    fn memory_max_is_a_ceiling_not_a_dimension() -> R<()> {
        let request = ResourceRequest::from_value(&json!({ "memory": 256, "memoryMax": 4096 }))?;
        assert_eq!(request.amount("memory"), Some(256.0));
        assert!(request.amount(MEMORY_MAX_KEY).is_none());

        // Checked against the normalized memory on the submit path only.
        let policy = ResourcePolicy::from_manager_config(&ManagerConfigFull::default())?;
        let err = policy
            .normalize_task_resources(&json!({ "memoryMax": 128 }), true)
            .expect_err("below the 256 MiB default");
        assert!(err.to_string().contains("memoryMax (128 MiB)"), "{err}");
        policy.normalize_task_resources(&json!({ "memoryMax": 128 }), false)?;
        assert!(policy
            .normalize_task_resources(&json!({ "memory": 64, "memoryMax": -1 }), true)
            .is_err());
        policy.normalize_task_resources(&json!({ "memory": 64, "memoryMax": 64 }), true)?;
        Ok(())
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

    fn usage_test_policy() -> ResourcePolicy {
        ResourcePolicy::from_manager_config(&ManagerConfigFull::default()).expect("policy")
    }

    fn request(resources: Value) -> ResourceRequest {
        let policy = usage_test_policy();
        let normalized = policy
            .normalize_task_resources(&resources, false)
            .expect("normalize");
        ResourceRequest::from_value(&normalized).expect("request")
    }

    /// The pools the two paths build must agree dim for dim. Amounts carry a
    /// tolerance because the aggregate re-associates the sum; see the
    /// `NodeUsage` docs.
    fn assert_same_pool(aggregate: &ResourcePool, walk: &ResourcePool, what: &str) {
        let aggregate_keys: std::collections::BTreeSet<&String> = aggregate.values.keys().collect();
        let walk_keys: std::collections::BTreeSet<&String> = walk.values.keys().collect();
        assert_eq!(aggregate_keys, walk_keys, "{what}: dims differ");
        for (name, expected) in &walk.values {
            match (&aggregate.values[name], expected) {
                (ResourceValue::Amount(got), ResourceValue::Amount(want)) => assert!(
                    got == want || (got - want).abs() <= 1e-9 * want.abs().max(1.0),
                    "{what}: {name} is {got}, expected {want}"
                ),
                (ResourceValue::Items(got), ResourceValue::Items(want)) => {
                    assert_eq!(got, want, "{what}: {name} items differ")
                }
                (got, want) => panic!("{what}: {name} kind changed: {got:?} vs {want:?}"),
            }
        }
    }

    /// The property the whole cache rests on: subtracting an aggregate is
    /// the same as subtracting the rows one at a time — including for dims
    /// that arrive as devices from one allocation and as a counter from
    /// another, and for a zero-amount demand, which still has to CREATE the
    /// pool entry.
    #[test]
    fn free_from_usage_matches_the_row_walk() {
        let policy = usage_test_policy();
        let mut node = node_with_fingerprint(json!({
            "memory_total": "1073741824",
            "cpus": { "cpu-1": { "frequency": "1000" } },
        }));
        node.config_attrs = json!({
            "maxAllocations": 4,
            "resources": { "gpu": ["g0", "g1", "g2"], "widgets": 6 },
            "reservedResources": { "memory": 128 },
        });

        let rows = [
            (1i64, json!({ "cpu": 100, "gpu": ["g0"], "widgets": 2 })),
            (2, json!({ "memory": 64, "gpu": 1 })),
            (3, json!({ "scratch": 0, "fractional": 0.1 })),
            (4, json!({ "cpu": 50, "gpu": ["g1"] })),
        ];

        let walk = ResourcePool::free_from_rows(
            &node,
            rows.iter().map(|(id, value)| (*id, value)),
            &policy,
        );
        let usage = NodeUsage::from_rows(rows.iter().map(|(id, value)| (*id, value)), &policy)
            .expect("agg");
        assert_same_pool(
            &ResourcePool::free_from_usage(&node, &usage),
            &walk,
            "all rows",
        );

        // And the `drop_own` shape: total minus one job's rows must equal
        // the walk over everyone else's, which is how a Fresh plan's pool is
        // built without re-reading the node.
        let mut others = usage.clone();
        others.remove(&request(rows[0].1.clone()));
        others.remove(&request(rows[3].1.clone()));
        let walk_without = ResourcePool::free_from_rows(
            &node,
            rows[1..3].iter().map(|(id, value)| (*id, value)),
            &policy,
        );
        assert_same_pool(
            &ResourcePool::free_from_usage(&node, &others),
            &walk_without,
            "own rows dropped",
        );
    }

    /// `remove` is the exact inverse of `add`, contributor counts included:
    /// a dim must DISAPPEAR when its last contributor leaves rather than
    /// linger as a zero, because a lingering zero would create a pool entry
    /// the row walk never created.
    #[test]
    fn node_usage_add_and_remove_are_inverses() {
        let mut usage = NodeUsage::default();
        let a = request(json!({ "cpu": 100, "gpu": ["g0"], "scratch": 0 }));
        let b = request(json!({ "cpu": 250, "gpu": ["g0", "g1"] }));

        usage.add(&a);
        usage.add(&b);
        usage.remove(&b);
        let mut only_a = NodeUsage::default();
        only_a.add(&a);
        assert_eq!(usage, only_a, "removing b must leave exactly a");

        usage.remove(&a);
        assert_eq!(usage, NodeUsage::default(), "the aggregate must empty out");
    }

    /// Two allocations claiming the same device is a bug elsewhere, but the
    /// aggregate must not turn it into free capacity: releasing one holder
    /// leaves the device taken while the other still holds it.
    #[test]
    fn items_are_counted_not_set_unioned() {
        let mut usage = NodeUsage::default();
        let a = request(json!({ "gpu": ["g0"] }));
        let b = request(json!({ "gpu": ["g0"] }));
        usage.add(&a);
        usage.add(&b);
        usage.remove(&a);
        // Device lists are tokenized (`value_token` JSON-encodes each
        // element), so read the count back rather than guessing the spelling.
        assert_eq!(
            usage.dims["gpu"]
                .items
                .values()
                .copied()
                .collect::<Vec<_>>(),
            vec![1],
            "the surviving holder must keep the device"
        );
    }

    /// An allocation whose resources cannot be parsed must not produce an
    /// aggregate at all: `free_from_rows` answers that case with "the node
    /// fits nothing", and a cached value must never stand in for a row
    /// nobody could read.
    #[test]
    fn unparseable_rows_refuse_to_aggregate() {
        let policy = usage_test_policy();
        let rows = [(7i64, json!({ "cpu": "a lot" }))];
        let error = NodeUsage::from_rows(rows.iter().map(|(id, value)| (*id, value)), &policy)
            .expect_err("expected the bad row to be refused");
        assert!(
            error.to_string().contains("allocation 7"),
            "the error should name the row: {error}"
        );
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
    /// One amount-typed demand, if present.
    pub(crate) fn amount(&self, name: &str) -> Option<f64> {
        match self.values.get(name) {
            Some(ResourceValue::Amount(amount)) => Some(*amount),
            _ => None,
        }
    }

    pub(crate) fn from_value(value: &Value) -> R<Self> {
        let obj = value.as_object().context("resources must be an object")?;
        let mut values = HashMap::new();

        for (key, value) in obj {
            // A ceiling the worker enforces, never something a node has to
            // have free. Counting it would make it a custom resource no node
            // advertises, which fits nothing.
            if key == MEMORY_MAX_KEY {
                continue;
            }
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

        // Borrowed, not cloned: this runs for every node a plan apply
        // re-checks and every reprojected node, and cloning the whole
        // attribute map each time showed up in the 20k-node leader profile.
        let no_attrs = Map::new();
        let config_attrs = node.config_attrs.as_object().unwrap_or(&no_attrs);

        let max_allocations = config_value(config_attrs, &["maxAllocations", "max_allocations"])
            .and_then(|value| value.as_f64());
        let allocations = max_allocations.unwrap_or(f64::INFINITY);
        values.insert(
            "allocations".to_string(),
            ResourceValue::Amount(allocations),
        );

        if let Some(custom) = config_value(config_attrs, &["resources"]) {
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
            config_value(config_attrs, &["reservedResources", "reserved_resources"])
        {
            let reserved = reserved
                .as_object()
                .context("reserved_resources must be an object")?;
            for (name, value) in reserved {
                let reservation = parse_resource_value(name, value)?;
                pool.reserve(name, &reservation);
            }
        }

        // Oversubscription scales what is left after `reservedResources`:
        // `(physical - reserved) x ratio`. Applied here, at the one place
        // capacity is born, so `can_fit`, the read model, and the apply
        // transaction's recheck all see the same multiplied capacity with
        // no change in shape. `cores` is never scaled: a pinned core is
        // exclusive by definition.
        let ratios = OversubscribeRatios::of(config_attrs);
        for (name, ratio) in [("memory", ratios.memory), ("cpu", ratios.cpu)] {
            if ratio > 1.0 {
                if let Some(ResourceValue::Amount(available)) = pool.values.get_mut(name) {
                    *available *= ratio;
                }
            }
        }

        Ok(pool)
    }

    /// The node's FREE pool: fingerprinted capacity minus the normalized
    /// usage of every given allocation that still holds capacity (see
    /// [`capacity_rows`]). This is the cached-per-node form the read model
    /// maintains and the plan-time ledger clones.
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
        Self::free_from_rows(node, capacity_rows(allocations), policy)
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

/// One dimension of a [`NodeUsage`] aggregate.
///
/// Both halves are kept because a dim can legally arrive as a number from
/// one allocation (`gpu: 2`) and as a device list from another
/// (`gpu: ["g0"]`). [`ResourcePool::apply_usage`] silently ignores whichever
/// kind does not match the pool's own — an `Items` demand against an
/// `Amount` pool entry is a no-op and vice versa — so the aggregate carries
/// both and lets the pool make the same choice it would have made row by
/// row.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct UsageDim {
    /// How many allocations contributed a *numeric* value here. Tracked
    /// separately from the sum because `apply_usage` CREATES a missing pool
    /// entry at zero before subtracting: "one allocation asked for 0 cpu"
    /// and "no allocation mentioned cpu" leave different pools, and only a
    /// contributor count can tell them apart once the amounts are summed.
    #[serde(default)]
    pub(crate) amount_contributors: u32,
    #[serde(default)]
    pub(crate) amount: f64,
    /// item -> how many allocations hold it. A multiset, not a set: if two
    /// allocations somehow claim the same device, removing one of them must
    /// not hand the device back to the pool.
    #[serde(default)]
    pub(crate) items: BTreeMap<String, u32>,
}

impl UsageDim {
    /// Nothing contributes to this dim any more, so it must disappear
    /// rather than linger as a zero — see `amount_contributors`.
    fn is_empty(&self) -> bool {
        self.amount_contributors == 0 && self.items.is_empty()
    }
}

/// The aggregated resource usage of a *set* of allocations on one node:
/// exactly what [`ResourcePool::free_from_rows`] subtracts from the node's
/// fingerprinted capacity, held as a single value so the plan-apply
/// transaction can cache it per node instead of re-reading and re-parsing
/// every allocation row on that node (see `Store::free_pool_for_node_tx`).
///
/// Two properties make it a faithful stand-in for the row-by-row
/// subtraction:
///
/// - **Kind-faithful**, per [`UsageDim`]: numeric and item demands on the
///   same dim are kept apart, so the pool resolves them exactly as it would
///   have per row.
/// - **Reversible**: [`Self::remove`] is the exact inverse of [`Self::add`],
///   down to the contributor counts. That is what lets a Fresh plan's
///   "everything except this job's own allocations" be computed as
///   *total − own* rather than by re-reading everyone else's rows.
///
/// The one place the two forms are not bit-identical is float association:
/// the aggregate computes `capacity − (a₁ + a₂ + …)` where the row walk
/// computes `((capacity − a₁) − a₂) − …`. Both are correct sums of the same
/// numbers and differ by at most a rounding step — never by a whole
/// allocation's worth of capacity, which is the property that matters for
/// overcommit.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct NodeUsage {
    /// `BTreeMap`, not `HashMap`: this value is serialized into a replicated
    /// row, and replicas compare table content byte for byte.
    pub(crate) dims: BTreeMap<String, UsageDim>,
}

impl NodeUsage {
    /// Fold one allocation's normalized demands into the aggregate.
    pub(crate) fn add(&mut self, request: &ResourceRequest) {
        for (name, demand) in &request.values {
            let dim = self.dims.entry(name.clone()).or_default();
            match demand {
                ResourceValue::Amount(amount) => {
                    dim.amount_contributors += 1;
                    dim.amount += amount;
                }
                ResourceValue::Items(items) => {
                    for item in items {
                        *dim.items.entry(item.clone()).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    /// Take one allocation's demands back out — the exact inverse of
    /// [`Self::add`]. Used to turn "every allocation on the node" into
    /// "every allocation except the planned job's own", which is what a
    /// Fresh plan's pool is.
    ///
    /// Saturating on the counters rather than panicking: a caller removing
    /// something it never added is a bug, but one that must not take the
    /// apply transaction down. The counters can only under-report, which
    /// makes the aggregate look *more* used, not less.
    pub(crate) fn remove(&mut self, request: &ResourceRequest) {
        for (name, demand) in &request.values {
            let Some(dim) = self.dims.get_mut(name) else {
                continue;
            };
            match demand {
                ResourceValue::Amount(amount) => {
                    dim.amount_contributors = dim.amount_contributors.saturating_sub(1);
                    dim.amount -= amount;
                    if dim.amount_contributors == 0 {
                        // The last numeric contributor left, so the residue
                        // of summing and un-summing floats goes with it.
                        dim.amount = 0.0;
                    }
                }
                ResourceValue::Items(items) => {
                    for item in items {
                        if let Some(count) = dim.items.get_mut(item) {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                dim.items.remove(item);
                            }
                        }
                    }
                }
            }
            if dim.is_empty() {
                self.dims.remove(name);
            }
        }
    }

    /// Aggregate `(allocation id, stored resources)` rows the way
    /// [`ResourcePool::free_from_rows`] consumes them.
    ///
    /// Errors — naming the offending allocation — when a row's resources do
    /// not parse. That is deliberately NOT the "node fits nothing" posture
    /// `free_from_rows` takes: an aggregate is cached and reused, so a value
    /// derived from a row nobody could read must never be stored. The caller
    /// falls back to the row walk, which applies (and logs) the fits-nothing
    /// rule itself.
    pub(crate) fn from_rows<'a>(
        rows: impl IntoIterator<Item = (i64, &'a Value)>,
        policy: &ResourcePolicy,
    ) -> R<Self> {
        let mut usage = NodeUsage::default();
        for (allocation_id, resources) in rows {
            let request = policy
                .usage_request(resources)
                .with_context(|| format!("allocation {allocation_id} resources failed to parse"))?;
            usage.add(&request);
        }
        Ok(usage)
    }
}

impl ResourcePool {
    /// Subtract a pre-aggregated [`NodeUsage`] from the node's fingerprinted
    /// capacity. The aggregate counterpart of
    /// [`ResourcePool::free_from_rows`], and deliberately total in the same
    /// way: a node whose own resources do not parse fits nothing.
    ///
    /// The per-dim arithmetic mirrors [`ResourcePool::apply_usage`]
    /// statement for statement, because the whole point of the aggregate is
    /// that it produces the same pool the row walk would have.
    pub(crate) fn free_from_usage(node: &Node, usage: &NodeUsage) -> ResourcePool {
        let mut pool = match ResourcePool::from_node(node) {
            Ok(pool) => pool,
            Err(error) => {
                warn!(
                    node_id = %node.id,
                    %error,
                    "Node resources failed to parse; node will fit no tasks until corrected"
                );
                return ResourcePool {
                    values: HashMap::new(),
                };
            }
        };
        for (name, dim) in &usage.dims {
            if dim.amount_contributors > 0 {
                // `or_insert` before subtracting, exactly as `apply_usage`
                // does: a dim the node never fingerprinted becomes a real
                // (negative) entry the moment a task asks for it, which is
                // what makes `can_fit` reject the ask.
                let entry = pool
                    .values
                    .entry(name.clone())
                    .or_insert(ResourceValue::Amount(0.0));
                if let ResourceValue::Amount(available) = entry {
                    *available -= dim.amount;
                }
            }
            if !dim.items.is_empty() {
                if let Some(ResourceValue::Items(available)) = pool.values.get_mut(name) {
                    for item in dim.items.keys() {
                        available.remove(item);
                    }
                }
            }
        }
        pool
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
/// A node's schedulable memory and CPU, for usage displays
/// (`GetNodeUsage`). Unknown amounts are `None` rather than the infinity
/// the scheduler uses, because a display cannot print infinity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NodeCapacity {
    /// Schedulable memory in MiB: fingerprinted memory minus
    /// `reservedResources.memory`, times the memory oversubscription ratio.
    pub memory_mib: Option<f64>,
    /// Schedulable CPU in MHz, the same way.
    pub cpu_mhz: Option<f64>,
    /// Average clock per core, for turning measured cores into MHz.
    pub mhz_per_core: Option<f64>,
    /// Fingerprinted core count, which every platform reports even when it
    /// reports no usable clock speed (Apple Silicon).
    pub cpu_cores: Option<f64>,
    /// The ratios already folded into the two amounts above.
    pub ratios: OversubscribeRatios,
}

impl NodeCapacity {
    pub(crate) fn of(node: &Node) -> R<Self> {
        let pool = ResourcePool::from_node(node)?;
        let finite = |value: Option<f64>| value.filter(|value| value.is_finite());
        let cpu = fingerprint_cpu(node)?;
        let config_attrs = node.config_attrs.as_object().cloned().unwrap_or_default();
        Ok(Self {
            memory_mib: finite(pool.amount("memory")),
            cpu_mhz: finite(pool.amount("cpu")),
            mhz_per_core: finite(Some(cpu.mhz / cpu.cores)).filter(|mhz| *mhz > 0.0),
            cpu_cores: finite(Some(cpu.cores)).filter(|cores| *cores > 0.0),
            ratios: OversubscribeRatios::of(&config_attrs),
        })
    }
}

/// A node's oversubscription ratios, as its worker reported them at
/// registration (`config_attrs.oversubscribe`, from the worker type's
/// `resourceEnforcement.oversubscribe`). 1.0 means off. A missing, malformed,
/// or below-1.0 value reads as 1.0: the safe direction to guess in is "no
/// more than physical".
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct OversubscribeRatios {
    pub memory: f64,
    pub cpu: f64,
}

impl Default for OversubscribeRatios {
    fn default() -> Self {
        Self {
            memory: 1.0,
            cpu: 1.0,
        }
    }
}

impl OversubscribeRatios {
    pub(crate) fn of(config_attrs: &Map<String, Value>) -> Self {
        let Some(block) = config_attrs.get("oversubscribe").and_then(Value::as_object) else {
            return Self::default();
        };
        let ratio = |name: &str| {
            block
                .get(name)
                .and_then(Value::as_f64)
                .filter(|ratio| ratio.is_finite() && *ratio >= 1.0)
                .unwrap_or(1.0)
        };
        Self {
            memory: ratio("memory"),
            cpu: ratio("cpu"),
        }
    }
}

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
