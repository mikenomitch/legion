//! Worker-plane-auth plan Phase 3 at placement: a `container` that leaves
//! `network.mode` unset runs gated on a Linux node and on host networking on
//! a macOS node. A gated container gets a namespace resolver at
//! `127.0.0.53` and an ingress twin for every declared port, inside its
//! namespace: the port's `to`, or the advertised number itself.

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, AllocNetwork, Node, NodeMsg, Submission,
};
use serde_json::Value;

const JOB: &str = "web";

fn store() -> R<Store> {
    let dir = tempfile::Builder::new()
        .prefix("legion-gated-container-")
        .tempdir()?
        .keep();
    Store::start_storage(&ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    })
}

fn add_node(store: &Store, id: &str, os: &str) -> R<()> {
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
            "workerAddress": "https://10.0.0.7:4848",
        }),
        fingerprint_attrs: json!({
            "os": os,
            "arch": "x86_64",
            "memory_total": "68719476736",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
        }),
        dynamic_attrs: json!({ "buildVersion": "0.9.0" }),
        health_checks: json!({}),
    }))?;
    Ok(())
}

fn container_job(network: Option<Value>) -> R<JobConfig> {
    let mut task = json!({
        "name": "web",
        "runtime": "container",
        "resources": { "cpu": 100, "memory": 256 },
        "config": { "image": "nginx:1.27" },
    });
    if let Some(network) = network {
        task["network"] = network;
    }
    Ok(serde_json::from_value(
        json!({ "name": JOB, "tasks": [task] }),
    )?)
}

/// Submit the job, drain the evals, and return its allocation's network.
fn placed(store: &Store, network: Option<Value>) -> R<Option<AllocNetwork>> {
    store.handle_job_submit(
        &container_job(network)?,
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
    let allocation = allocations.first().context("the container was placed")?;
    Ok(allocation.network.clone())
}

#[test]
fn a_default_container_on_linux_is_gated_with_a_resolver_test() -> R<()> {
    let store = store()?;
    add_node(&store, "w-01", "linux")?;
    let network = placed(&store, None)?.context("a gated container carries a network")?;
    assert_eq!(network.mode.as_deref(), Some("gated"));
    assert_eq!(network.resolver_address().as_deref(), Some("127.0.0.53"));
    let env: Vec<(String, String)> = network.env_pairs();
    assert!(env.contains(&("LEGION_DNS".to_string(), "127.0.0.53".to_string())));
    assert!(env.contains(&(
        "https_proxy".to_string(),
        "http://127.0.0.1:1080".to_string()
    )));
    Ok(())
}

#[test]
fn a_gated_containers_ports_get_twins_inside_its_namespace_test() -> R<()> {
    let store = store()?;
    add_node(&store, "w-01", "linux")?;
    let network = placed(
        &store,
        Some(json!({ "ports": [
            { "name": "http", "to": 80 },
            { "name": "admin" },
        ] })),
    )?
    .context("placed with ports")?;
    assert_eq!(network.mode.as_deref(), Some("gated"));
    let http = network.port("http").context("http")?;
    assert_eq!(http.private_port, Some(80), "the twin is `to`");
    assert_eq!(http.bind_port(), 80);
    let admin = network.port("admin").context("admin")?;
    assert_eq!(
        admin.private_port,
        Some(admin.host_port),
        "without `to`, the twin is the advertised number"
    );
    Ok(())
}

#[test]
fn a_default_container_on_macos_stays_on_host_networking_test() -> R<()> {
    let store = store()?;
    add_node(&store, "mac-01", "macos")?;
    assert_eq!(placed(&store, None)?, None);
    Ok(())
}

#[test]
fn an_explicit_host_container_is_never_gated_test() -> R<()> {
    let store = store()?;
    add_node(&store, "w-01", "linux")?;
    let network = placed(
        &store,
        Some(json!({ "mode": "host", "ports": [{ "name": "http" }] })),
    )?
    .context("placed with ports")?;
    assert_eq!(network.mode.as_deref(), Some("host"));
    assert_eq!(network.resolver_address(), None);
    assert_eq!(network.port("http").context("http")?.private_port, None);
    Ok(())
}
