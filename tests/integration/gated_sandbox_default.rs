//! Gated plan Phase 2 at placement: a `sandbox` that declares no network
//! resolves `network.mode = "auto"` to `gated` on a microVM node and to
//! `host` on a dev backend, once the gated default is open. The default is
//! held closed until Phase 0 has run on a real VMM
//! (`legion::structs::network::SANDBOX_GATED_BY_DEFAULT`), so these tests
//! open it on their own store.

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::{config::ManagerConfigFull, job::JobConfig, Node, NodeMsg, Submission};
use serde_json::Value;

const JOB: &str = "agents";

fn store() -> R<Store> {
    let dir = tempfile::Builder::new()
        .prefix("legion-gated-default-")
        .tempdir_in(crate::support::scratch_root())?
        .keep();
    Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    })
}

fn add_node(store: &Store, id: &str, vmm: &str) -> R<()> {
    store.insert_node(NodeMsg::from(Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "node_id": id,
            "maxAllocations": 20,
            "resources": { "memory": 65536, "cpu": 40000 },
        }),
        fingerprint_attrs: json!({
            "arch": "x86_64",
            "memory_total": "68719476736",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
            "sandbox.vmm": vmm,
        }),
        dynamic_attrs: json!({ "buildVersion": "0.9.0" }),
        health_checks: json!({}),
    }))?;
    Ok(())
}

fn sandbox_job(config: Value) -> R<JobConfig> {
    Ok(serde_json::from_value(json!({
        "name": JOB,
        "tasks": [{
            "name": "workspace",
            "runtime": "sandbox",
            "resources": { "cpu": 100, "memory": 256 },
            "config": config,
        }],
    }))?)
}

/// Submit the job, drain the evals, and return the mode its allocation was
/// placed with (`None`: no network on the row, which reads as `host`).
fn placed_mode(store: &Store, config: Value) -> R<Option<String>> {
    store.handle_job_submit(
        &sandbox_job(config)?,
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
    let allocations = store.allocs_for_job(JOB)?;
    let allocation = allocations.first().context("the sandbox was placed")?;
    Ok(allocation
        .network
        .as_ref()
        .and_then(|network| network.mode.clone()))
}

fn plain() -> Value {
    json!({ "image": "alpine", "command": ["sleep", "3600"] })
}

#[test]
fn a_default_sandbox_is_placed_gated_on_a_microvm_node() -> R<()> {
    let store = store()?;
    store.set_sandbox_gated_default(true);
    add_node(&store, "w-01", "firecracker")?;
    assert_eq!(placed_mode(&store, plain())?.as_deref(), Some("gated"));
    Ok(())
}

#[test]
fn a_default_sandbox_on_a_dev_backend_is_placed_host() -> R<()> {
    let store = store()?;
    store.set_sandbox_gated_default(true);
    add_node(&store, "mac-01", "apple-container")?;
    assert_eq!(placed_mode(&store, plain())?, None);
    Ok(())
}

#[test]
fn a_sandbox_that_declares_its_own_network_is_never_gated_by_default() -> R<()> {
    let store = store()?;
    store.set_sandbox_gated_default(true);
    add_node(&store, "w-01", "firecracker")?;
    let config = json!({
        "image": "alpine",
        "command": ["sleep", "3600"],
        "network": { "mode": "none" },
    });
    assert_eq!(placed_mode(&store, config)?, None);
    Ok(())
}

/// Until Phase 0 opens it, a default sandbox is placed exactly as before.
#[test]
fn the_gated_default_is_closed_until_phase_0() -> R<()> {
    let store = store()?;
    assert_eq!(
        store.sandbox_gated_default(),
        legion::structs::network::SANDBOX_GATED_BY_DEFAULT
            || std::env::var(legion::structs::network::SANDBOX_GATED_DEFAULT_ENV).as_deref()
                == Ok("1")
    );
    store.set_sandbox_gated_default(false);
    add_node(&store, "w-01", "firecracker")?;
    assert_eq!(placed_mode(&store, plain())?, None);
    Ok(())
}
