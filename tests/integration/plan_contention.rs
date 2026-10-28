//! Plan-contention harness (docs/late-binding-plan-apply.md, plan Phase 0).
//!
//! N-way concurrent single-allocation job submits race against one pool,
//! pumped by real eval workers (`handle_claimed_eval`, requeue backstop
//! included). The harness records conflict rejections, requeues, fallback
//! binds, failed evals, and time-to-all-placed.
//!
//! The non-ignored tests are the CI-friendly regression matrix; the
//! `#[ignore]`d `contention_matrix` prints the full grid used for the
//! numbers in `benchmarks/scheduler/README.md`:
//!
//! ```bash
//! cargo test --test integration plan_contention:: -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use legion::manager::daemon::handle_claimed_eval;
use legion::prelude::{json, Context, R};
use legion::storage::events::{SchedulerEventFilter, SchedulerEventRecord};
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{NodeMsg, Submission};
use serde_json::Value;
use tempfile::Builder;

fn setup_store(scheduler_settings: Value) -> R<Store> {
    let store_dir = Builder::new()
        .prefix("legion-contention-")
        .tempdir_in(crate::support::scratch_root())
        .context("error creating temp store directory")?
        .keep();
    let store_config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("error parsing store path")?
            .to_string(),
        scheduling_policy: "bin_pack".into(),
        scheduler_settings: Some(scheduler_settings),
        ..Default::default()
    };
    let store = Store::start_storage(&store_config)?;
    store.reset_all_tables()?;
    Ok(store)
}

/// `max_allocations` caps the node's allocation slots (`maxAllocations`),
/// which is how a test asks for genuine capacity contention: with memory
/// alone the pool is far too roomy for two planners to ever fight over a
/// node's last slot.
fn insert_node(store: &Store, id: &str, max_allocations: Option<u64>) -> R<()> {
    let config_attrs = match max_allocations {
        Some(max) => json!({ "maxAllocations": max }).to_string(),
        None => "{}".to_string(),
    };
    store.insert_node(NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs,
        fingerprint_attrs: json!({
            "memory_total": "8589934592",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        heartbeat_source: String::new(),
    })?;
    Ok(())
}

/// Submit one single-task job and enqueue its `job_submit` eval, the way
/// the RunJob RPC does.
fn submit_job(store: &Store, name: &str) -> R<()> {
    let job = JobConfig {
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.into(),
        tasks: vec![TaskConfig {
            resources: json!({ "memory": 200 }),
            ..TaskConfig::new("api", "noop")
        }],
        priority: Default::default(),
        version: None,
        pending_extensions: Default::default(),
    };
    store.handle_job_submit(
        &job,
        &[Submission {
            path: "job.ncl".into(),
            content: "job".into(),
            is_primary: true,
        }],
    )?;
    let job_config_id = store.get_job_config(name)?.id();
    store.enqueue_eval("job_submit", Some(name), Some(&job_config_id), None, None)?;
    Ok(())
}

/// Every scheduler event of `kind`, paginated past the per-call limit.
fn events_of_kind(store: &Store, kind: &str) -> R<Vec<SchedulerEventRecord>> {
    let mut events = Vec::new();
    let mut after_id = None;
    loop {
        let page = store.list_scheduler_events(&SchedulerEventFilter {
            after_id,
            limit: Some(1_000),
            kind: Some(kind.to_string()),
            ..Default::default()
        })?;
        let Some(last) = page.last() else {
            break;
        };
        after_id = Some(last.id);
        let full_page = page.len() == 1_000;
        events.extend(page);
        if !full_page {
            break;
        }
    }
    Ok(events)
}

fn placed_allocation_count(store: &Store) -> R<usize> {
    Ok(store
        .get_ledger()?
        .iter()
        .map(|entry| entry.allocations.len())
        .sum())
}

#[derive(Debug)]
struct BurstMetrics {
    label: String,
    jobs: usize,
    nodes: usize,
    elapsed: Duration,
    conflict_rejections: usize,
    requeues: usize,
    fallbacks: i64,
    failed_evals: usize,
    placed: usize,
}

impl BurstMetrics {
    fn print(&self) {
        println!(
            "{:<28} jobs={:<4} nodes={:<4} placed={:<4} conflicts={:<4} requeues={:<4} \
             fallbacks={:<4} failed={:<2} time_to_all_placed={:?}",
            self.label,
            self.jobs,
            self.nodes,
            self.placed,
            self.conflict_rejections,
            self.requeues,
            self.fallbacks,
            self.failed_evals,
            self.elapsed,
        );
    }
}

/// Fire `jobs` single-allocation submits at a `nodes`-node pool and pump
/// the eval queue with `workers` threads running the real worker logic.
/// Returns once every job placed (or panics on timeout with diagnostics).
fn run_burst(
    label: &str,
    jobs: usize,
    nodes: usize,
    workers: usize,
    max_allocations_per_node: Option<u64>,
    scheduler_settings: Value,
) -> R<BurstMetrics> {
    let store = setup_store(scheduler_settings)?;
    for index in 0..nodes {
        insert_node(
            &store,
            &format!("node-{index:03}"),
            max_allocations_per_node,
        )?;
    }

    // Seed one allocation so bin_pack has a deterministic argmax from the
    // first wave — the exact correlated-pick pathology the plan targets.
    submit_job(&store, "seed-job")?;
    let seed_eval = store.claim_eval()?.context("expected the seed eval")?;
    handle_claimed_eval(&store, 0, &seed_eval);

    for index in 0..jobs {
        submit_job(&store, &format!("burst-{index:03}"))?;
    }

    let expected = jobs + 1;
    let started = Instant::now();
    let done = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..workers)
        .map(|worker_id| {
            let store = store.clone();
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    match store.claim_eval() {
                        Ok(Some(eval)) => handle_claimed_eval(&store, worker_id, &eval),
                        Ok(None) => std::thread::sleep(Duration::from_millis(5)),
                        Err(_) => std::thread::sleep(Duration::from_millis(5)),
                    }
                }
            })
        })
        .collect();

    let deadline = Instant::now() + Duration::from_secs(120);
    let elapsed = loop {
        let placed = placed_allocation_count(&store)?;
        if placed >= expected {
            break started.elapsed();
        }
        if Instant::now() > deadline {
            done.store(true, Ordering::Relaxed);
            for handle in handles {
                let _ = handle.join();
            }
            let evals = store.get_evals()?;
            let failed = evals.iter().filter(|eval| eval.status == "failed").count();
            panic!(
                "burst `{label}` timed out: placed {placed}/{expected}, \
                 failed evals {failed}, live evals {}",
                evals.len() - failed
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    done.store(true, Ordering::Relaxed);
    for handle in handles {
        let _ = handle.join();
    }

    let conflict_rejections = events_of_kind(&store, "allocation.plan_rejected")?.len();
    let requeues = events_of_kind(&store, "eval.requeued")?.len();
    let failed_evals = store
        .get_evals()?
        .iter()
        .filter(|eval| eval.status == "failed")
        .count();
    let mut fallbacks = 0i64;
    for event in events_of_kind(&store, "allocation.placed")? {
        let metadata: Value = serde_json::from_str(&event.metadata_json)?;
        fallbacks += metadata
            .get("fallbacks")
            .and_then(Value::as_i64)
            .unwrap_or(0);
    }

    // Correctness: no node may be oversubscribed, fallbacks or not.
    let ledger = store.get_ledger()?;
    let mut memory_by_node: HashMap<&str, i64> = HashMap::new();
    for entry in &ledger {
        for allocation in &entry.allocations {
            let memory = allocation
                .resources
                .get("memory")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            *memory_by_node.entry(entry.node.id.as_str()).or_default() += memory;
        }
    }
    for (node_id, memory) in &memory_by_node {
        assert!(
            *memory <= 8192,
            "node {node_id} is oversubscribed: {memory} MiB placed"
        );
    }
    let placed = placed_allocation_count(&store)?;

    Ok(BurstMetrics {
        label: label.to_string(),
        jobs,
        nodes,
        elapsed,
        conflict_rejections,
        requeues,
        fallbacks,
        failed_evals,
        placed,
    })
}

/// The headline regression: a burst at headroom never drops work, and
/// candidate lists cut conflict rejections relative to single-node plans.
#[test]
fn burst_at_headroom_never_drops_work() -> R<()> {
    let with_candidates = run_burst(
        "candidates-on",
        32,
        12,
        8,
        None,
        json!({ "plan_conflict_requeue_delay_ms": 100 }),
    )?;
    with_candidates.print();
    assert_eq!(
        with_candidates.failed_evals, 0,
        "no eval may terminally fail"
    );
    assert_eq!(with_candidates.placed, 33, "every job must place");

    let without_candidates = run_burst(
        "candidates-off",
        32,
        12,
        8,
        None,
        json!({ "plan_candidates": 0, "plan_conflict_requeue_delay_ms": 100 }),
    )?;
    without_candidates.print();
    assert_eq!(without_candidates.failed_evals, 0);
    assert_eq!(without_candidates.placed, 33);

    // The point of candidate lists: races cost ranks, not retries. A small
    // slack absorbs scheduling noise on loaded CI machines.
    assert!(
        with_candidates.conflict_rejections <= without_candidates.conflict_rejections + 2,
        "candidates on must not conflict more than off (on: {}, off: {})",
        with_candidates.conflict_rejections,
        without_candidates.conflict_rejections,
    );
    Ok(())
}

/// The requeue backstop end to end: with a one-attempt retry budget, a
/// single lost race exhausts an eval's budget — and every exhausted eval
/// must requeue (visibly) and place, never fail.
///
/// The contention has to be *capacity* contention. Node-epoch collisions
/// used to supply it for free, but a plan whose candidate merely had its
/// epoch bumped is now revalidated and bound
/// (internal_docs/plans/plan-apply-conflict-reduction-plan.md), so this
/// same burst runs clean on a roomy pool. Three allocation slots per node
/// over 12 nodes is 36 slots for 33 allocations: planners genuinely fight
/// over the last slots of the `bin_pack` argmax, and the losers reject on
/// the in-transaction capacity recheck.
#[test]
fn conflict_exhaustion_requeues_and_places() -> R<()> {
    let metrics = run_burst(
        "exhaustion-requeue",
        32,
        12,
        8,
        Some(3),
        json!({
            "plan_candidates": 0,
            "plan_conflict_max_retries": 1,
            "plan_conflict_requeue_delay_ms": 100,
        }),
    )?;
    metrics.print();
    assert_eq!(
        metrics.failed_evals, 0,
        "conflict exhaustion must never fail an eval"
    );
    assert_eq!(
        metrics.placed, 33,
        "every requeued eval must eventually place"
    );
    assert!(
        metrics.conflict_rejections > 0,
        "the harness must actually produce contention for this test to mean anything"
    );
    assert!(
        metrics.requeues > 0,
        "with a one-attempt budget, at least one eval must hit the requeue backstop"
    );
    Ok(())
}

/// The full matrix behind `benchmarks/scheduler/README.md`. Run with
/// `cargo test --test integration plan_contention:: -- --ignored --nocapture`.
#[test]
#[ignore = "benchmark matrix for scheduler-contention changes"]
fn contention_matrix() -> R<()> {
    for (jobs, nodes) in [(8, 6), (8, 64), (32, 6), (32, 64), (128, 6), (128, 64)] {
        for candidates in [0, 8] {
            let label = format!("N={jobs} pool={nodes} k={candidates}");
            let metrics = run_burst(
                &label,
                jobs,
                nodes,
                8,
                None,
                json!({
                    "plan_candidates": candidates,
                    "plan_conflict_requeue_delay_ms": 100,
                }),
            )?;
            metrics.print();
            assert_eq!(metrics.failed_evals, 0, "{label}: no dropped work");
        }
    }
    Ok(())
}
