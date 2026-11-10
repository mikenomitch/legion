//! `bin_pack` and `spread` score what an allocation *requests* against what
//! a node *has*, not how many allocations the node holds (architecture
//! review finding SCH4; docs/scheduler-and-eval-loop.md, "Scoring").
//!
//! The shape that count-based scoring gets wrong: one node holds a single
//! large allocation, another holds a few small ones. Counted, the second node
//! looks fuller, so `bin_pack` stacks the next placement there and strands
//! the capacity a later large allocation needed; `spread` piles onto the
//! node that is already nearly full. Scored by requested resources, both
//! policies see which node is actually full.

use legion::manager::eval::EvalExt;
use legion::prelude::R;
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, TaskConfig, TaskCount};
use legion::structs::{Node, NodeMsg, Submission};
use serde_json::{json, Value};

const GIB: u64 = 1024 * 1024 * 1024;

fn node(id: &str, slot: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "maxAllocations": 32, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": (16 * GIB).to_string(),
            "cpus": {
                "cpu-1": { "frequency": 2400.0 }, "cpu-2": { "frequency": 2400.0 },
                "cpu-3": { "frequency": 2400.0 }, "cpu-4": { "frequency": 2400.0 },
                "cpu-5": { "frequency": 2400.0 }, "cpu-6": { "frequency": 2400.0 },
                "cpu-7": { "frequency": 2400.0 }, "cpu-8": { "frequency": 2400.0 }
            },
            "slot": slot
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn store(policy: &str, dir: &tempfile::TempDir) -> R<Store> {
    let store = Store::start_storage(&ManagerConfigFull {
        store_path: dir.path().join("store").to_str().unwrap().to_string(),
        scheduling_policy: policy.to_string(),
        ..Default::default()
    })?;
    store.insert_node(NodeMsg::from(node("node-x", "x")))?;
    store.insert_node(NodeMsg::from(node("node-y", "y")))?;
    Ok(store)
}

/// Submit a one-task job and run every eval it raises. Returns whether the
/// job placed every replica.
fn run_job(store: &Store, name: &str, count: u32, memory_mib: u64, constraints: Value) -> R<bool> {
    let job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(count),
            resources: json!({ "cpu": 200, "memory": memory_mib }),
            constraints,
            ..TaskConfig::new("main", "noop")
        }],
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    };
    store.handle_job_submit(
        &job,
        &[Submission {
            path: format!("{name}.ncl"),
            content: "job".into(),
            is_primary: true,
        }],
    )?;
    let job_config_id = store.get_job_config(name)?.id();
    store.enqueue_eval("job_submit", Some(name), Some(&job_config_id), None, None)?;
    while let Some(eval) = store.claim_eval()? {
        let result = eval.handle(store);
        store.complete_eval(&eval.id, result.is_ok())?;
    }
    let placed = store
        .get_allocs()?
        .into_iter()
        .filter(|allocation| allocation.job_id == name)
        .count();
    Ok(placed == count as usize)
}

fn nodes_of(store: &Store, job: &str) -> R<Vec<String>> {
    let mut nodes: Vec<String> = store
        .get_allocs()?
        .into_iter()
        .filter(|allocation| allocation.job_id == job)
        .map(|allocation| allocation.node_id)
        .collect();
    nodes.sort();
    Ok(nodes)
}

/// node-x: one 12 GiB allocation. node-y: three 256 MiB allocations.
fn seed(store: &Store) -> R<()> {
    assert!(run_job(
        store,
        "large",
        1,
        12 * 1024,
        json!({ "slot": "x" })
    )?);
    assert!(run_job(store, "smalls", 3, 256, json!({ "slot": "y" }))?);
    assert_eq!(nodes_of(store, "large")?, vec!["node-x"]);
    assert_eq!(nodes_of(store, "smalls")?, vec!["node-y"; 3]);
    Ok(())
}

/// `bin_pack` fills the node that is fullest by *resources*: the 2 GiB
/// placement tops off node-x, which keeps node-y whole for the 14 GiB job
/// that comes next. Counted by allocations, node-y (three small) looked
/// fuller than node-x (one large), the 2 GiB placement went there, and the
/// 14 GiB job then fit nowhere.
#[test]
fn bin_pack_small_allocations_do_not_outweigh_one_large_one() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = store("bin_pack", &dir)?;
    seed(&store)?;

    assert!(run_job(&store, "medium", 1, 2 * 1024, json!({}))?);
    let medium = nodes_of(&store, "medium")?;
    let big_placed = run_job(&store, "big", 1, 14 * 1024, json!({}))?;
    println!("resource_scoring bin_pack: medium on {medium:?}, 14 GiB job placed: {big_placed}");

    assert_eq!(medium, vec!["node-x"], "bin_pack tops off the fuller node");
    assert!(big_placed, "the large job must not be stranded");
    assert_eq!(nodes_of(&store, "big")?, vec!["node-y"]);
    Ok(())
}

/// `spread` puts the next placement on the node with the most free
/// resources, not the one with the fewest allocations.
#[test]
fn spread_prefers_the_emptier_node_not_the_one_with_fewer_allocations() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = store("spread", &dir)?;
    seed(&store)?;

    assert!(run_job(&store, "medium", 1, 1024, json!({}))?);
    let medium = nodes_of(&store, "medium")?;
    println!("resource_scoring spread: medium on {medium:?}");
    assert_eq!(medium, vec!["node-y"], "spread avoids the 75%-full node");
    Ok(())
}
