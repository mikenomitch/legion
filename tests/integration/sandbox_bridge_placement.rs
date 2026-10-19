//! Gated plan Phase 4 at placement: a TAP sandbox
//! (`config.network = { mode = "tap", bridge = "br0" }`) carries an implicit
//! `sandbox.bridges contains "br0"` constraint, so it places only on nodes
//! whose worker config gives that bridge an address pool
//! (docs/task-runtime-sandbox.md, "Addressing").

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::{config::ManagerConfigFull, job::JobConfig, Node, NodeMsg, Submission};
use serde_json::Value;

const JOB: &str = "agents";

fn store() -> R<Store> {
    let dir = tempfile::Builder::new()
        .prefix("legion-bridge-placement-")
        .tempdir_in(crate::support::scratch_root())?
        .keep();
    Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    })
}

/// A node that declares `bridges` in its worker config's `sandbox.bridges`.
fn add_node(store: &Store, id: &str, bridges: &[&str]) -> R<()> {
    let mut config_attrs = json!({
        "node_id": id,
        "maxAllocations": 20,
        "resources": { "memory": 65536, "cpu": 40000 },
    });
    if !bridges.is_empty() {
        config_attrs["sandbox.bridges"] = json!(bridges);
    }
    store.insert_node(NodeMsg::from(Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs,
        fingerprint_attrs: json!({
            "arch": "x86_64",
            "memory_total": "68719476736",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
            "sandbox.vmm": "firecracker",
        }),
        dynamic_attrs: json!({ "buildVersion": "0.9.0" }),
        health_checks: json!({}),
    }))?;
    Ok(())
}

fn sandbox_job(count: u32, network: Value) -> R<JobConfig> {
    Ok(serde_json::from_value(json!({
        "name": JOB,
        "tasks": [{
            "name": "workspace",
            "runtime": "sandbox",
            "count": count,
            "resources": { "cpu": 100, "memory": 256 },
            "config": { "image": "alpine", "command": ["sleep", "3600"], "network": network },
        }],
    }))?)
}

/// Submit the job, drain the evals, and return the nodes its allocations
/// landed on.
fn placed_on(store: &Store, job: &JobConfig) -> R<Vec<String>> {
    store.handle_job_submit(
        job,
        &[Submission {
            path: format!("{JOB}.json"),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    let config_id = store.get_job_config(JOB)?.id();
    store.enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
    while let Some(eval) = store.get_next_eval()? {
        let outcome = eval.handle(store);
        store.set_eval_status(
            &eval.id,
            if outcome.is_ok() {
                "complete"
            } else {
                "failed"
            },
        )?;
        outcome?;
    }
    Ok(store
        .allocs_for_job(JOB)?
        .into_iter()
        .map(|allocation| allocation.node_id)
        .collect())
}

#[test]
fn a_tap_sandbox_places_only_where_its_bridge_has_a_pool() -> R<()> {
    let store = store()?;
    add_node(&store, "w-01", &[])?;
    add_node(&store, "w-02", &["br1"])?;
    add_node(&store, "w-03", &["br0", "br1"])?;
    let nodes = placed_on(
        &store,
        &sandbox_job(4, json!({ "mode": "tap", "bridge": "br0" }))?,
    )?;
    assert_eq!(nodes.len(), 4, "{nodes:?}");
    assert!(nodes.iter().all(|node| node == "w-03"), "{nodes:?}");
    // The constraint is on the stored task, where `job get` shows it.
    let stored = store.get_job_config(JOB)?;
    let task = stored.tasks.first().context("the task")?;
    assert_eq!(
        task.constraints,
        json!([{ "key": "sandbox.bridges", "operator": "contains", "value": "br0" }])
    );
    Ok(())
}

/// Placement fails the way any unsatisfiable constraint does, rather than
/// the allocation failing at start on a node with no pool for its bridge.
#[test]
fn a_tap_sandbox_with_no_node_for_its_bridge_is_not_placed() -> R<()> {
    let store = store()?;
    add_node(&store, "w-01", &["br1"])?;
    let err = placed_on(
        &store,
        &sandbox_job(1, json!({ "mode": "tap", "bridge": "br0" }))?,
    )
    .expect_err("no node declares br0");
    assert!(
        format!("{err:#}").contains("no available nodes matching constraints"),
        "{err:#}"
    );
    assert!(store.allocs_for_job(JOB)?.is_empty());
    Ok(())
}

#[test]
fn a_sandbox_without_a_nic_places_anywhere() -> R<()> {
    let store = store()?;
    add_node(&store, "w-01", &[])?;
    let nodes = placed_on(&store, &sandbox_job(1, json!({ "mode": "none" }))?)?;
    assert_eq!(nodes, vec!["w-01".to_string()]);
    Ok(())
}
