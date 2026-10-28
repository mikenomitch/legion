//! Repro for the 2026-08-06 storm-drill finding (aws-phase5-findings.md):
//! on a 5,000-node fleet, 25,002 `spread` allocations concentrated on the
//! first-registered 1,250-node slice, replacements packed the next slice,
//! and every candidate in sampled `plan_submission` rows came from one
//! slice, before scoring or binding.
//!
//! The mechanism is the scheduling index's candidate budget, not the raft
//! projection: a task needing 4 allocations gets a 64-candidate budget, the
//! default `fit` index arms at 128 nodes, and every idle sim node of a
//! profile projects to the SAME cell. The scheduler asks the index for the
//! whole shard in best-first order, re-verifies it, and cut the verified list
//! at the budget — a sorted-id prefix of that one cell — so the planner's
//! universe was the lexicographically-lowest ids, `sim-0-*`, for every plan.
//! The cut now samples the boundary cell
//! (`scheduling_index::truncate_within_boundary_cell`). The earlier
//! repro (`spread_under_raft.rs`) used `count: 50`, which lifts the budget
//! to 400 above its 300-node fleet, so it never truncated.
//!
//! This test uses the rig's shape at 1/8 scale: four identically-shaped
//! slices of 150 nodes, jobs needing 4 allocations each, spread policy,
//! local-disk persistence (the mechanism does not need raft).

use legion::prelude::R;
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, TaskConfig, TaskCount};
use legion::structs::{Node, NodeMsg, Submission};
use serde_json::json;
use std::collections::HashMap;

const SLICES: usize = 4;
const NODES_PER_SLICE: usize = 150;
const JOBS: usize = 40;
const COUNT_PER_JOB: u32 = 4;

fn sim_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "maxAllocations": 32, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "34359738368",
            "cpus": {
                "cpu-1": { "frequency": 2400.0 }, "cpu-2": { "frequency": 2400.0 },
                "cpu-3": { "frequency": 2400.0 }, "cpu-4": { "frequency": 2400.0 },
                "cpu-5": { "frequency": 2400.0 }, "cpu-6": { "frequency": 2400.0 },
                "cpu-7": { "frequency": 2400.0 }, "cpu-8": { "frequency": 2400.0 }
            },
            "class": "web"
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn slice_of(node_id: &str) -> usize {
    node_id
        .strip_prefix("sim-")
        .and_then(|rest| rest.split('-').next())
        .and_then(|slice| slice.parse().ok())
        .expect("sim node id")
}

#[test]
fn truncated_index_candidates_span_the_fleet_under_spread() -> R<()> {
    let dir = tempfile::tempdir()?;
    let config = ManagerConfigFull {
        store_path: dir.path().join("store").to_str().unwrap().to_string(),
        scheduling_policy: "spread".to_string(),
        ..Default::default()
    };
    let store = Store::start_storage(&config)?;

    // Register slice by slice, as the rig's four sim instances did.
    for slice in 0..SLICES {
        for n in 0..NODES_PER_SLICE {
            store.insert_node(NodeMsg::from(sim_node(&format!("sim-{slice}-web-{n:03}"))))?;
        }
    }

    for job_index in 0..JOBS {
        let job = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: format!("api-{job_index:03}"),
            tasks: vec![TaskConfig {
                count: TaskCount::new(COUNT_PER_JOB),
                resources: json!({ "cpu": 500, "memory": 1024 }),
                constraints: json!({ "class": "web" }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
            pending_extensions: Default::default(),
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: format!("{}.ncl", job.name),
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
    }

    while let Some(eval) = store.claim_eval()? {
        let result = eval.handle(&store);
        store.complete_eval(&eval.id, result.is_ok())?;
        result?;
    }

    let allocations = store.get_allocs()?;
    let expected = JOBS * COUNT_PER_JOB as usize;
    assert_eq!(allocations.len(), expected, "every replica placed");

    let mut per_node: HashMap<&str, usize> = HashMap::new();
    for allocation in &allocations {
        *per_node.entry(allocation.node_id.as_str()).or_default() += 1;
    }
    let mut busiest: Vec<(&str, usize)> = per_node.iter().map(|(k, v)| (*k, *v)).collect();
    busiest.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    println!(
        "distinct nodes: {}, busiest: {:?}",
        per_node.len(),
        &busiest[..busiest.len().min(12)]
    );

    // What the planner reports for one more job of the same shape.
    {
        let probe = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "probe".into(),
            tasks: vec![TaskConfig {
                count: TaskCount::new(COUNT_PER_JOB),
                resources: json!({ "cpu": 500, "memory": 1024 }),
                constraints: json!({ "class": "web" }),
                ..TaskConfig::new("api", "noop")
            }],
            version: None,
            pending_extensions: Default::default(),
        };
        store.handle_job_submit(
            &probe,
            &[Submission {
                path: "probe.ncl".into(),
                content: "job".into(),
                is_primary: true,
            }],
        )?;
        let id = store.get_job_config(&probe.name)?.id();
        let scheduler = legion::manager::scheduler::Scheduler::new(&store)?;
        let (plan, report, _) =
            scheduler.plan_only_with_report(&id, legion::manager::scheduler::PlanMode::Fresh)?;
        println!(
            "probe plan nodes: {:?}",
            plan.iter().map(|p| p.node_id.clone()).collect::<Vec<_>>()
        );
        println!("probe index report: {report:?}");
        // The test only means something while the index truncates: a budget
        // at or above the fleet would spread trivially through the scan.
        let usage = &report[0];
        assert_eq!(usage.index.as_deref(), Some("fit"));
        assert!(
            usage.truncated,
            "budget must be below the fleet for this test to bite: {usage:?}"
        );
        assert!(usage.candidates < SLICES * NODES_PER_SLICE);
    }

    let mut per_slice: HashMap<usize, usize> = HashMap::new();
    for allocation in &allocations {
        *per_slice.entry(slice_of(&allocation.node_id)).or_default() += 1;
    }
    let mut counts: Vec<(usize, usize)> = per_slice.into_iter().collect();
    counts.sort();
    println!("allocations per slice: {counts:?}");

    // A fair candidate sample gives each slice roughly a quarter of the
    // work; the id-prefix truncation gave the first slice everything it
    // could hold (150 of 160) and the second slice the spill.
    let floor = expected / SLICES / 2; // half of a fair share
    for slice in 0..SLICES {
        let got = counts
            .iter()
            .find(|(s, _)| *s == slice)
            .map(|(_, n)| *n)
            .unwrap_or(0);
        assert!(
            got >= floor,
            "slice {slice} received {got} of {expected} allocations (< {floor}); \
             the planner's candidate universe collapsed to an id prefix: {counts:?}"
        );
    }
    Ok(())
}
