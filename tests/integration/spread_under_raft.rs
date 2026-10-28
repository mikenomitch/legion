//! Repro harness for the 2026-08-06 storm-drill finding: on a real raft
//! cluster, 25,002 `spread`-policy allocations concentrated on the
//! first-registered 1,250-node slice of a 5,000-node fleet, and their
//! replacements packed the NEXT slice. Local raft-OFF repros do not
//! trigger (spread behaves), so the suspect is the capture -> apply ->
//! projection path, which only runs with the raft engine on. A single-node
//! bootstrap cluster runs that exact path.

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig, TaskCount};
use legion::structs::{Node, NodeMsg, Submission};
use serde_json::json;

fn raft_config(root: &std::path::Path) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        scheduling_policy: "spread".to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.topology = "lanes".to_string();
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-a".to_string();
    config.persistence.raft.data_dir = root
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn sim_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

/// 100 nodes registered through raft, one 50-replica spread job planned
/// through the real eval path. If the concentration bug lives in the
/// replicated projection, the low half of the id order (300 nodes, index armed) will hold nearly
/// everything, as it did on the rig.
#[test]
fn spread_under_raft_does_not_concentrate_on_low_node_ids() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&raft_config(dir.path())?)?;

    for index in 0..300 {
        store.insert_node(NodeMsg::from(sim_node(&format!("n-{index:03}"))))?;
    }

    let job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: "spread-raft-repro".into(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(50),
            ..TaskConfig::new("api", "noop")
        }],
        version: None,
        pending_extensions: Default::default(),
    };
    store.handle_job_submit(
        &job,
        &[Submission {
            path: "spread-raft-repro.ncl".into(),
            content: "job".into(),
            is_primary: true,
        }],
    )?;

    let job_config_id = store.get_job_config(&job.name)?.id();
    store.enqueue_eval(
        "job_submit",
        Some(&job.name),
        Some(&job_config_id),
        None,
        None,
    )?;

    // Drive the enqueued eval the way the daemon's worker would.
    let mut handled = 0;
    while let Some(eval) = store.claim_eval()? {
        handled += 1;
        let result = eval.handle(&store);
        println!("eval {} ({}): {:?}", eval.id, eval.kind, result);
        store.complete_eval(&eval.id, result.is_ok())?;
        result?;
    }
    println!("evals handled: {handled}");

    let allocations = store.get_allocs()?;
    assert_eq!(allocations.len(), 50, "all replicas placed");

    let low = allocations
        .iter()
        .filter(|alloc| alloc.node_id.as_str() < "n-150")
        .count();
    let distinct: std::collections::HashSet<&str> = allocations
        .iter()
        .map(|alloc| alloc.node_id.as_str())
        .collect();
    println!(
        "raft spread: low-half {low}/50, distinct nodes {}",
        distinct.len()
    );
    assert!(
        (15..=35).contains(&low),
        "spread under raft concentrated: {low}/50 on the low half"
    );
    assert!(
        distinct.len() >= 40,
        "spread under raft stacked replicas: {} distinct nodes",
        distinct.len()
    );
    Ok(())
}
