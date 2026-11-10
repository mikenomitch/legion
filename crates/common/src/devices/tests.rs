use super::*;
use serde_json::json;

fn group(value: Value) -> DeviceGroup {
    serde_json::from_value(value).expect("group")
}

fn demand(name: &str, count: u32, constraints: Value, affinities: Value) -> DeviceDemand {
    DeviceDemand {
        index: 0,
        name: DeviceName::parse(name).unwrap(),
        count,
        constraints: ConstraintSet::new(&constraints).unwrap(),
        affinities: AffinitySet::new(&affinities).unwrap(),
    }
}

fn free_all(inventory: &NodeDeviceInventory) -> HashMap<String, HashSet<String>> {
    inventory
        .groups
        .iter()
        .map(|g| {
            (
                pool_key(&g.id()),
                g.instances.iter().map(|i| i.id.clone()).collect(),
            )
        })
        .collect()
}

/// Eight GPUs: two NVLink domains of four, split across two NUMA nodes.
fn eight_gpus() -> NodeDeviceInventory {
    let instances: Vec<Value> = (0..8)
        .map(|i| {
            json!({
                "id": format!("GPU-{i}"),
                "attributes": {
                    "index": i,
                    "numaNode": i / 4,
                    "peerGroup": format!("nvlink-{}", i / 4),
                },
            })
        })
        .collect();
    NodeDeviceInventory {
        groups: vec![group(json!({
            "vendor": "nvidia", "type": "gpu", "model": "A100",
            "plugin": "nvidia",
            "attributes": { "memoryMiB": 81920 },
            "instances": instances,
        }))],
    }
}

#[test]
fn names_parse_and_match_test() {
    let a100 = &eight_gpus().groups[0];
    for (raw, matches) in [
        ("gpu", true),
        ("nvidia/gpu", true),
        ("nvidia/gpu/A100", true),
        ("amd/gpu", false),
        ("nvidia/gpu/H100", false),
        ("fpga", false),
    ] {
        assert_eq!(
            DeviceName::parse(raw).unwrap().matches_group(a100),
            matches,
            "{raw}"
        );
        assert_eq!(DeviceName::parse(raw).unwrap().to_string(), raw);
    }
    for bad in ["", "a/b/c/d", "nvidia//gpu", " gpu", "gpu=1"] {
        assert!(DeviceName::parse(bad).is_err(), "{bad:?}");
    }
    let name = |raw| DeviceName::parse(raw).unwrap();
    assert!(name("gpu").compatible(&name("nvidia/gpu")));
    assert!(name("nvidia/gpu/A100").compatible(&name("nvidia/gpu")));
    assert!(!name("amd/gpu").compatible(&name("nvidia/gpu")));
    assert!(!name("fpga").compatible(&name("gpu")));
}

#[test]
fn env_var_names_test() {
    assert_eq!(ids_env_var("nvidia/gpu"), "LEGION_DEVICE_NVIDIA_GPU_IDS");
    assert_eq!(ids_env_var("gpu"), "LEGION_DEVICE_GPU_IDS");
    assert_eq!(
        ids_env_var("nvidia/gpu/A100-SXM4"),
        "LEGION_DEVICE_NVIDIA_GPU_A100_SXM4_IDS"
    );
}

#[test]
fn request_validation_test() {
    let ok = json!({ "devices": [
        { "name": "nvidia/gpu", "count": 2,
          "constraints": [{ "key": "memoryMiB", "operator": ">=", "value": 40000 }] },
        { "name": "fpga" },
    ]});
    validate_requests(&ok).unwrap();
    validate_requests(&json!({ "memory": 1 })).unwrap();
    for (bad, needle) in [
        (
            json!({ "devices": [{ "name": "gpu", "count": 0 }] }),
            "count",
        ),
        (json!({ "devices": [{ "name": "a/b/c/d" }] }), "device name"),
        (
            json!({ "devices": [{ "name": "gpu", "colour": "red" }] }),
            "devices[0]",
        ),
        (
            json!({ "devices": [{ "name": "gpu", "assigned": { "device": "x/y/z", "ids": [] } }] }),
            "set by the scheduler",
        ),
        (
            json!({ "devices": [{ "name": "nvidia/gpu" }, { "name": "nvidia-gpu" }] }),
            "LEGION_DEVICE_NVIDIA_GPU_IDS",
        ),
        (json!({ "devices": { "name": "gpu" } }), "list"),
        (
            json!({ "devices": [{ "name": "gpu", "constraints": [{ "key": "x", "operator": "~" }] }] }),
            "constraints",
        ),
    ] {
        let err = format!("{:#}", validate_requests(&bad).unwrap_err());
        assert!(err.contains(needle), "{needle}: {err}");
    }
}

#[test]
fn assignments_round_trip_through_resources_test() {
    let resources = json!({ "memory": 64, "devices": [
        { "name": "gpu", "count": 2 },
        { "name": "fpga" },
    ]});
    assert!(has_unassigned(&resources));
    let assigned = write_assignments(
        &resources,
        &[(
            0,
            DeviceAssignment {
                device: "nvidia/gpu/A100".into(),
                plugin: "nvidia".into(),
                ids: vec!["GPU-0".into(), "GPU-1".into()],
            },
        )],
    );
    assert!(has_unassigned(&assigned), "the fpga is still unassigned");
    let (demands, held) = parse_for_scheduling(&assigned).unwrap();
    assert_eq!(demands.len(), 1);
    assert_eq!(demands[0].index, 1);
    assert_eq!(
        held,
        vec![(
            "device:nvidia/gpu/A100".to_string(),
            vec!["GPU-0".to_string(), "GPU-1".to_string()]
        )]
    );
    let recorded = assignments(&assigned);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].0, "gpu");
    assert!(!has_unassigned(&json!({ "memory": 1 })));
    assert!(!has_unassigned(&json!({ "devices": [] })));
}

#[test]
fn assignment_keeps_a_request_inside_one_nvlink_domain_test() {
    let inventory = eight_gpus();
    let mut free = free_all(&inventory);
    // GPU-0 and GPU-1 are taken: domain 0 has 2 free, domain 1 has 4.
    let key = pool_key("nvidia/gpu/A100");
    free.get_mut(&key).unwrap().remove("GPU-0");
    free.get_mut(&key).unwrap().remove("GPU-1");

    // Two fit in domain 0 exactly: best fit leaves domain 1 whole.
    let two = inventory
        .assign(&free, &[demand("gpu", 2, Value::Null, Value::Null)])
        .unwrap();
    assert_eq!(two[0].1.ids, vec!["GPU-2", "GPU-3"]);
    assert_eq!(two[0].1.device, "nvidia/gpu/A100");
    assert_eq!(two[0].1.plugin, "nvidia");

    // Four only fit in domain 1.
    let four = inventory
        .assign(&free, &[demand("gpu", 4, Value::Null, Value::Null)])
        .unwrap();
    assert_eq!(four[0].1.ids, vec!["GPU-4", "GPU-5", "GPU-6", "GPU-7"]);

    // Five fit in no domain: spread across both, in order.
    let five = inventory
        .assign(&free, &[demand("gpu", 5, Value::Null, Value::Null)])
        .unwrap();
    assert_eq!(five[0].1.ids.len(), 5);

    // Seven do not fit at all.
    assert!(inventory
        .assign(&free, &[demand("gpu", 7, Value::Null, Value::Null)])
        .is_none());
}

#[test]
fn assignment_honors_health_constraints_and_affinities_test() {
    let mut inventory = eight_gpus();
    inventory.groups[0].instances[4].healthy = false;
    let free = free_all(&inventory);

    // Constraints filter instances; an unhealthy one is never chosen.
    let numa1 = inventory
        .assign(
            &free,
            &[demand(
                "nvidia/gpu",
                3,
                json!([{ "key": "numaNode", "value": 1 }]),
                Value::Null,
            )],
        )
        .unwrap();
    assert_eq!(numa1[0].1.ids, vec!["GPU-5", "GPU-6", "GPU-7"]);
    assert!(inventory
        .assign(
            &free,
            &[demand(
                "gpu",
                4,
                json!([{ "key": "numaNode", "value": 1 }]),
                Value::Null
            )]
        )
        .is_none());
    // Group attributes are visible to constraints too.
    assert!(inventory
        .assign(
            &free,
            &[demand(
                "gpu",
                1,
                json!([{ "key": "memoryMiB", "operator": ">=", "value": 100000 }]),
                Value::Null
            )]
        )
        .is_none());

    // Affinity picks instances on the node.
    let preferred = inventory
        .assign(
            &free,
            &[demand(
                "gpu",
                1,
                Value::Null,
                json!([{ "key": "index", "value": 6, "multiplier": 1 }]),
            )],
        )
        .unwrap();
    assert_eq!(preferred[0].1.ids, vec!["GPU-6"]);
}

#[test]
fn several_demands_do_not_share_devices_test() {
    let inventory = eight_gpus();
    let free = free_all(&inventory);
    let mut second = demand("nvidia/gpu", 4, Value::Null, Value::Null);
    second.index = 1;
    let both = inventory
        .assign(&free, &[demand("gpu", 4, Value::Null, Value::Null), second])
        .unwrap();
    let mut all: Vec<&String> = both.iter().flat_map(|(_, a)| a.ids.iter()).collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 8, "{both:?}");
    assert_eq!(both[1].0, 1);
}

#[test]
fn a_request_prefers_the_group_with_better_locality_test() {
    let mut inventory = eight_gpus();
    // A second model with no topology attributes at all.
    inventory.groups.push(group(json!({
        "vendor": "nvidia", "type": "gpu", "model": "T4",
        "instances": [{ "id": "T4-0" }, { "id": "T4-1" }],
    })));
    let free = free_all(&inventory);
    let chosen = inventory
        .assign(&free, &[demand("gpu", 2, Value::Null, Value::Null)])
        .unwrap();
    assert_eq!(chosen[0].1.device, "nvidia/gpu/A100");
    // Naming the model wins over locality.
    let t4 = inventory
        .assign(
            &free,
            &[demand("nvidia/gpu/T4", 2, Value::Null, Value::Null)],
        )
        .unwrap();
    assert_eq!(t4[0].1.ids, vec!["T4-0", "T4-1"]);
}

#[test]
fn reports_read_from_health_checks_test() {
    let report = DeviceReport {
        groups: eight_gpus().groups,
        plugins: BTreeMap::from([(
            "nvidia".to_string(),
            PluginStatus {
                status: "ok".into(),
                error: String::new(),
                groups: 1,
            },
        )]),
    };
    let node = crate::structs::Node {
        id: "n1".into(),
        status: "active".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
        config_attrs: json!({}),
        fingerprint_attrs: json!({}),
        dynamic_attrs: json!({}),
        health_checks: json!({ "disk": "pass", "devices": report.to_value() }),
    };
    assert_eq!(node_report(&node), report);
    assert_eq!(node_devices(&node).len(), 1);
    let lines = render_node_devices(&report);
    assert!(lines[1].contains("nvidia/gpu/A100  (8 healthy of 8, plugin nvidia)"));
    let mut garbage = node.clone();
    garbage.health_checks = json!({ "devices": "nope" });
    assert!(node_devices(&garbage).is_empty());
}
