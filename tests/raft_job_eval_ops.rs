//! Raft entries per job eval on an idle cluster (multi-eval-plans plan,
//! Phase 2; `internal_docs/plans/multi-eval-plans-plan.md`).
//!
//! A placing eval whose plan is its last write settles inside that plan's
//! entry (`arm_eval_settle`): its row, the duplicates its claim subsumed,
//! and its `eval.completed` event ride the plan. `job_reschedule` did this
//! first; `job_submit` (a first submit), `job_update`, and `job_scale` (a
//! scale-out) do it too. A plan that leaves work unplaced does not settle,
//! because the handler may write again after it.
//!
//! The counts are asserted exactly, on both topologies, the way
//! `tests/raft_lifecycle_ops.rs` asserts the keyed lifecycle. One `#[test]`,
//! because the op counter is process-wide.

use std::collections::BTreeMap;

use legion::manager::daemon::resolve_claimed_eval;
use legion::prelude::{json, Context, R};
use legion::storage::store::{ClaimScope, EvalBatchLimits, Store};
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig, TaskCount};
use legion::structs::{NodeMsg, Submission};

const NODE: &str = "ops-node";
const JOB: &str = "web";

/// Expected entries per stage. The eval stages count only what the eval
/// worker wrote: the plan (with the settle inside it) and the status lane's
/// `task_status` projection, which this harness runs inline.
const EXPECTED: &[(&str, u64)] = &[
    // The plan, which settles the eval, and the task's status projection.
    ("submit eval (3 placements)", 2),
    // The restart's own writes before the plan (marker clear, allocation
    // removal), then the plan, which settles the eval. The status
    // projection writes nothing: `reset_task_statuses` already left the
    // rows where the new placements put them.
    ("update eval (restart)", 3),
    // Nothing to place: the eval settles in its own write.
    ("reschedule eval (no-op)", 1),
];

fn raft_config(root: &std::path::Path, topology: &str) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.topology = topology.to_string();
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-a".to_string();
    config.persistence.raft.data_dir = root
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn node() -> NodeMsg {
    NodeMsg {
        id: NODE.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        config_attrs: json!({ "maxAllocations": 64 }).to_string(),
        fingerprint_attrs: json!({
            "memory_total": "68719476736",
            "cpus": { "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" } },
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        heartbeat_source: String::new(),
    }
}

fn job() -> JobConfig {
    let mut task = TaskConfig::new("app", "noop");
    task.count = TaskCount::new(3);
    task.resources = json!({ "memory": 64, "cpu": 50 });
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![task],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
        pending_extensions: Default::default(),
    }
}

/// Raft entries applied so far, summed over the lanes, from the state
/// machine's applied marker: exact as soon as a write returns, where the
/// engine's metrics (`raft_status`) are published a moment later and made
/// a count taken right after a write flaky under load.
fn entries(store: &Store) -> R<u64> {
    store.raft_applied_entries()
}

/// `legion_raft_ops_total{op, outcome="applied"}`, summed over lanes.
fn applied_ops() -> BTreeMap<String, u64> {
    let mut ops = BTreeMap::new();
    for line in legion::manager::telemetry::render().lines() {
        let Some(rest) = line.strip_prefix("legion_raft_ops_total{") else {
            continue;
        };
        let Some((labels, value)) = rest.split_once("} ") else {
            continue;
        };
        if !labels.contains("outcome=\"applied\"") {
            continue;
        }
        let Some(op) = labels
            .split(',')
            .find_map(|label| label.strip_prefix("op=\""))
            .and_then(|op| op.strip_suffix('"'))
        else {
            continue;
        };
        *ops.entry(op.to_string()).or_insert(0) +=
            value.trim().parse::<f64>().unwrap_or(0.0) as u64;
    }
    ops
}

/// The real eval worker's write sequence until the queue is empty.
fn run_eval_worker(store: &Store) -> R<()> {
    let limits = EvalBatchLimits {
        max: 16,
        workers: 1,
    };
    let mut batch = store.claim_evals(limits, ClaimScope::Any)?;
    while !batch.is_empty() {
        let completions = batch
            .iter()
            .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
            .collect();
        batch = store.complete_evals_and_claim_next(completions, limits, ClaimScope::Any)?;
    }
    Ok(())
}

/// Run `body` and count the entries it committed, with the ops behind them.
fn stage(store: &Store, body: impl FnOnce() -> R<()>) -> R<(u64, String)> {
    let before = entries(store)?;
    let ops_before = applied_ops();
    body()?;
    let ops = applied_ops()
        .into_iter()
        .filter_map(|(op, after)| {
            let delta = after - ops_before.get(&op).copied().unwrap_or(0);
            (delta > 0).then(|| format!("{op}×{delta}"))
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok((entries(store)? - before, ops))
}

fn measure(topology: &str) -> R<Vec<(&'static str, u64, String)>> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&raft_config(dir.path(), topology)?)?;
    store.insert_node(node())?;
    store.handle_job_submit(
        &job(),
        &[Submission {
            path: format!("{JOB}.json"),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    let config_id = store.get_job_config(JOB)?.id();
    // `RunJob` enqueues the submit's eval after the config commits
    // (`submit_rendered_job`); the store call alone does not.
    store.enqueue_eval("job_submit", Some(JOB), Some(&config_id), None, None)?;
    let mut stages = Vec::new();

    let (count, ops) = stage(&store, || run_eval_worker(&store))?;
    stages.push(("submit eval (3 placements)", count, ops));

    store.request_job_restart(JOB)?;
    store.enqueue_eval("job_update", Some(JOB), Some(&config_id), None, None)?;
    let (count, ops) = stage(&store, || run_eval_worker(&store))?;
    stages.push(("update eval (restart)", count, ops));

    store.enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
    let (count, ops) = stage(&store, || run_eval_worker(&store))?;
    stages.push(("reschedule eval (no-op)", count, ops));
    Ok(stages)
}

#[test]
fn raft_entries_per_job_eval() -> R<()> {
    for topology in ["single", "lanes"] {
        let stages = measure(topology)?;
        let mut report = format!("\n== raft entries per job eval: topology={topology} ==\n");
        for (stage, count, ops) in &stages {
            report.push_str(&format!("{stage:<28} {count:>3}  {ops}\n"));
        }
        println!("{report}");
        let measured: Vec<(&str, u64)> = stages
            .iter()
            .map(|(stage, count, _)| (*stage, *count))
            .collect();
        assert_eq!(
            measured, EXPECTED,
            "raft entries per job eval changed (topology={topology}); if intended, update \
             EXPECTED:{report}"
        );
    }
    Ok(())
}
