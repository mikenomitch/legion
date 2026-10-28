//! Integration coverage for adaptive scheduler concurrency
//! (docs/adaptive-scheduler-concurrency.md, plan Phase 3).
//!
//! The unit suite in `src/manager/concurrency.rs` proves the decision RULE.
//! This suite proves the two things a pure function cannot:
//!
//! 1. The gate is invisible to correctness. A burst pinned to one planner
//!    places exactly what a burst at the ceiling places, and no node is
//!    oversubscribed either way.
//! 2. Throttling planning never becomes a stuck cluster. A `job stop` issued
//!    while planning is pinned at the floor completes promptly, ahead of the
//!    higher-priority planning work it is queued behind — the one deliberate
//!    cross-class priority inversion the design introduces (plan §8.2).
//!
//! It also checks the wiring the controller depends on: that real plan
//! outcomes actually land in the process-global `ConflictWindow`.
//!
//! The gate, the mode and the window are process-global (one manager, one
//! controller), so every test here takes `SERIAL` — they share the same
//! statics and would otherwise read each other's state.

// Only `scratch_root` is used here; the cluster gate is for the raft tests.
#[allow(dead_code)]
#[path = "integration/support.rs"]
mod support;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use legion::manager::concurrency::{self, ConcurrencyMode};
use legion::manager::daemon::{claim_eval_batch, handle_claimed_eval};
use legion::prelude::{json, Context, R};
use legion::storage::store::{EvalBatchLimits, Store};
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{NodeMsg, Submission};
use serde_json::Value;
use tempfile::Builder;

static SERIAL: Mutex<()> = Mutex::new(());

/// Take the process-wide lock and leave the controller's globals where a
/// fresh manager would leave them.
fn serial() -> MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|poison| poison.into_inner());
    concurrency::gate().set_mode(ConcurrencyMode::Off);
    concurrency::gate().set_limit(usize::MAX);
    concurrency::conflict_window().drain();
    guard
}

fn setup_store() -> R<Store> {
    let store_dir = Builder::new()
        .prefix("legion-adaptive-concurrency-")
        .tempdir_in(support::scratch_root())
        .context("error creating temp store directory")?
        .keep();
    let store_config = ManagerConfigFull {
        store_path: store_dir
            .to_str()
            .context("error parsing store path")?
            .to_string(),
        scheduling_policy: "bin_pack".into(),
        ..Default::default()
    };
    let store = Store::start_storage(&store_config)?;
    store.reset_all_tables()?;
    Ok(store)
}

/// `max_allocations` caps the node's slots, which is how a test asks for
/// genuine contention: with memory alone the pool is far too roomy for two
/// planners to fight over a node's last slot.
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

fn submit_job(store: &Store, name: &str) -> R<String> {
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
    Ok(store.get_job_config(name)?.id())
}

fn placed_allocation_count(store: &Store) -> R<usize> {
    Ok(store
        .get_ledger()?
        .iter()
        .map(|entry| entry.allocations.len())
        .sum())
}

/// The eval worker's admission logic, as `eval_worker_loop` runs it: ask the
/// gate, claim a batch narrowly when throttled, hold the permit across the
/// batch when any of it plans, release it otherwise. Handling and settling
/// are the single-eval forms; the batch bookkeeping the real loop fuses is
/// not what these tests are about.
fn drain_queue(
    store: &Store,
    workers: usize,
    until: impl Fn(&Store) -> bool + Send + Sync + 'static,
) -> R<Vec<String>> {
    let done = Arc::new(AtomicBool::new(false));
    let order: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let until = Arc::new(until);
    let limits = EvalBatchLimits { max: 4, workers };
    let handles: Vec<_> = (0..workers)
        .map(|worker_id| {
            let store = store.clone();
            let done = Arc::clone(&done);
            let order = Arc::clone(&order);
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    let admission = concurrency::gate().admit();
                    let throttled = admission.is_throttled();
                    let batch = match claim_eval_batch(&store, throttled, limits) {
                        Ok(batch) if !batch.is_empty() => batch,
                        _ => {
                            drop(admission);
                            std::thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                    };
                    let _permit = batch
                        .iter()
                        .any(|eval| concurrency::is_planning_kind(&eval.kind))
                        .then_some(admission);
                    for eval in &batch {
                        handle_claimed_eval(&store, worker_id, eval);
                        if let Ok(mut order) = order.lock() {
                            order.push(eval.kind.clone());
                        }
                    }
                }
            })
        })
        .collect();

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if until(store) {
            break;
        }
        if Instant::now() > deadline {
            done.store(true, Ordering::Relaxed);
            for handle in handles {
                let _ = handle.join();
            }
            panic!("queue drain timed out");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    done.store(true, Ordering::Relaxed);
    for handle in handles {
        let _ = handle.join();
    }
    let order = order.lock().unwrap_or_else(|p| p.into_inner()).clone();
    Ok(order)
}

fn assert_no_node_is_oversubscribed(store: &Store) -> R<()> {
    let mut memory_by_node: HashMap<String, i64> = HashMap::new();
    for entry in store.get_ledger()? {
        for allocation in &entry.allocations {
            let memory = allocation
                .resources
                .get("memory")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            *memory_by_node.entry(entry.node.id.clone()).or_default() += memory;
        }
    }
    for (node_id, memory) in &memory_by_node {
        assert!(
            *memory <= 8192,
            "node {node_id} is oversubscribed: {memory} MiB placed"
        );
    }
    Ok(())
}

/// The controller must be invisible to correctness: the same burst at the
/// floor and at the ceiling places the same allocations, and neither
/// oversubscribes a node. Only wall time is allowed to differ.
#[test]
fn a_burst_places_the_same_work_at_the_floor_as_at_the_ceiling() -> R<()> {
    const JOBS: usize = 24;

    let run = |limit: usize| -> R<usize> {
        let store = setup_store()?;
        for index in 0..6 {
            insert_node(&store, &format!("node-{index:03}"), Some(6))?;
        }
        for index in 0..JOBS {
            let name = format!("burst-{index:03}");
            let job_config_id = submit_job(&store, &name)?;
            store.enqueue_eval("job_submit", Some(&name), Some(&job_config_id), None, None)?;
        }
        concurrency::gate().set_mode(ConcurrencyMode::Enforce);
        concurrency::gate().set_limit(limit);
        drain_queue(&store, 8, |store| {
            placed_allocation_count(store).unwrap_or(0) >= JOBS
        })?;
        assert_no_node_is_oversubscribed(&store)?;
        let failed = store
            .get_evals()?
            .iter()
            .filter(|eval| eval.status == "failed")
            .count();
        assert_eq!(failed, 0, "no eval may fail at limit {limit}");
        placed_allocation_count(&store)
    };

    let _guard = serial();
    let at_floor = run(1)?;
    let at_ceiling = run(8)?;
    assert_eq!(at_floor, JOBS, "everything placed at the floor");
    assert_eq!(
        at_floor, at_ceiling,
        "throttling changes wall time, never the outcome"
    );
    Ok(())
}

/// The §8.2 starvation guarantee, and the one ordering deviation the design
/// makes. Pinned at the floor, a `job_stop` at the LOWEST priority must still
/// run early: stops cannot conflict, they frequently free the capacity the
/// congestion is about, and an operator's `job stop` during an incident must
/// not queue behind the storm it is trying to end.
#[test]
fn a_job_stop_runs_promptly_while_planning_is_pinned_at_the_floor() -> R<()> {
    const PLANNING_EVALS: usize = 24;

    let store = setup_store()?;
    for index in 0..4 {
        insert_node(&store, &format!("node-{index:03}"), Some(8))?;
    }

    // One job that already placed, so there is something to stop.
    let stoppable = submit_job(&store, "stoppable")?;
    for index in 0..PLANNING_EVALS {
        let name = format!("planner-{index:03}");
        let job_config_id = submit_job(&store, &name)?;
        store.enqueue_eval(
            "job_submit",
            Some(&name),
            Some(&job_config_id),
            None,
            Some(90),
        )?;
    }
    // Lowest priority in the queue, and behind every one of them in FIFO
    // order: without the narrow claim path it would run last.
    store.enqueue_eval(
        "job_stop",
        Some("stoppable"),
        Some(&stoppable),
        None,
        Some(10),
    )?;

    let _guard = serial();
    concurrency::gate().set_mode(ConcurrencyMode::Enforce);
    concurrency::gate().set_limit(1);
    let order = drain_queue(&store, 6, |store| {
        store
            .get_evals()
            .map(|evals| {
                evals
                    .iter()
                    .all(|eval| eval.status != "pending" && eval.status != "in_flight")
            })
            .unwrap_or(false)
    })?;

    let stop_position = order
        .iter()
        .position(|kind| kind == "job_stop")
        .context("the stop must have run")?;
    assert!(
        stop_position < PLANNING_EVALS / 2,
        "a throttled cluster must not be a stuck one: the stop ran at position \
         {stop_position} of {}",
        order.len()
    );
    assert!(
        concurrency::admission_skips() > 0,
        "the cross-class inversion must be counted, not silent"
    );
    Ok(())
}

/// `mode = "off"` restores today's behavior: the gate is never consulted, so
/// nothing narrows the claim and strict priority order holds — the lowest
/// priority stop runs LAST. This is the control for the test above.
#[test]
fn off_mode_leaves_queue_order_exactly_as_it_was() -> R<()> {
    const PLANNING_EVALS: usize = 8;

    let store = setup_store()?;
    insert_node(&store, "node-000", Some(64))?;
    let stoppable = submit_job(&store, "stoppable")?;
    for index in 0..PLANNING_EVALS {
        let name = format!("planner-{index:03}");
        let job_config_id = submit_job(&store, &name)?;
        store.enqueue_eval(
            "job_submit",
            Some(&name),
            Some(&job_config_id),
            None,
            Some(90),
        )?;
    }
    store.enqueue_eval(
        "job_stop",
        Some("stoppable"),
        Some(&stoppable),
        None,
        Some(10),
    )?;

    let _guard = serial();
    concurrency::gate().set_mode(ConcurrencyMode::Off);
    // A single worker, so the recorded order IS the claim order.
    let order = drain_queue(&store, 1, |store| {
        store
            .get_evals()
            .map(|evals| {
                evals
                    .iter()
                    .all(|eval| eval.status != "pending" && eval.status != "in_flight")
            })
            .unwrap_or(false)
    })?;
    // The stop frees capacity, so it can queue a capacity pass
    // (`node_join`) behind itself; that eval is not part of the ordering
    // under test.
    let enqueued: Vec<&str> = order
        .iter()
        .map(String::as_str)
        .filter(|kind| *kind != "node_join")
        .collect();
    assert_eq!(
        enqueued.last().copied(),
        Some("job_stop"),
        "with the controller off, priority order is untouched: {order:?}"
    );
    assert!(matches!(
        concurrency::gate().admit(),
        legion::manager::concurrency::Admission::Unrestricted
    ));
    Ok(())
}

/// The controller's whole input signal is the two `plan_apply_total` call
/// sites mirroring into the process-global window. If that wiring breaks, the
/// controller silently decides on an empty window forever — so it is checked
/// against real placements rather than assumed.
#[test]
fn real_plan_outcomes_land_in_the_conflict_window() -> R<()> {
    const JOBS: usize = 12;

    let store = setup_store()?;
    for index in 0..3 {
        insert_node(&store, &format!("node-{index:03}"), Some(8))?;
    }
    for index in 0..JOBS {
        let name = format!("burst-{index:03}");
        let job_config_id = submit_job(&store, &name)?;
        store.enqueue_eval("job_submit", Some(&name), Some(&job_config_id), None, None)?;
    }

    let _guard = serial();
    drain_queue(&store, 4, |store| {
        placed_allocation_count(store).unwrap_or(0) >= JOBS
    })?;

    let counts = concurrency::conflict_window().drain();
    assert_eq!(
        counts.applied + counts.fallback,
        JOBS as u64,
        "every placed plan must be counted as progress: {counts:?}"
    );
    assert_eq!(
        concurrency::conflict_window().drain(),
        legion::manager::concurrency::WindowCounts::default(),
        "and the window is drained, not accumulated"
    );
    Ok(())
}
