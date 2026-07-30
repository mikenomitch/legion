//! Plan-apply contention benchmark (late-binding plan, Phase 0).
//!
//! N-way concurrent single-allocation job submits race for one pool,
//! driven through the REAL eval pipeline (claim → `Eval::handle` →
//! dispatch semantics via `handle_claimed_eval`), so conflict retries,
//! terminal failures, and — once built — the requeue backstop and
//! candidate walk are all exercised exactly as production exercises them.
//!
//! Per cell it records what the late-binding plan's acceptance criteria
//! read from: conflict rejections (rejected `plan_submission` rows),
//! dropped evals (terminally failed), placed allocations, and
//! time-to-all-placed.
//!
//! ```bash
//! LEGION_CONTENTION_N=8,32,128 LEGION_CONTENTION_POOLS=tight,headroom \
//!   cargo test --release --test contention_benchmark -- --ignored --nocapture
//! ```
//!
//! Pools: `headroom` = one node per job (4x capacity); `tight` = capacity
//! exactly equal to demand (every job fits, maximum contention).

#[path = "benchmark_support/mod.rs"]
mod benchmark_support;

use benchmark_support::{insert_profile_node, setup_backend, Backend, NodeProfile};
use legion::prelude::{json, R};
use legion::storage::store::Store;
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::Submission;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// One 8 GiB node fits four 2 GiB allocations.
const ALLOCS_PER_NODE: usize = 4;
const TASK_MEMORY_MIB: u64 = 2048;

struct Cell {
    backend: &'static str,
    pool: String,
    n: usize,
    nodes: usize,
    placed: usize,
    dropped: usize,
    conflict_rejections: i64,
    submissions: i64,
    time_to_all_placed_ms: Option<u128>,
    wall_ms: u128,
}

fn count(store: &Store, sql: &str) -> R<i64> {
    store.bench_scalar(sql)
}

fn run_cell(backend: Backend, pool: &str, n: usize) -> R<Cell> {
    let scenario = serde_json::from_value(json!({
        "name": format!("contention-{pool}-{n}"),
        "scheduling_policy": "bin_pack",
        "iterations": 1,
        "node_count": 1, "job_count": 0, "tasks_per_job": 0, "task_count_per_task": 0,
    }))
    .expect("static scenario json");
    let handle = setup_backend(backend, &scenario)?;
    let store = handle.store();

    let nodes = match pool {
        // Capacity == demand: every job fits, contention is maximal.
        "tight" => n.div_ceil(ALLOCS_PER_NODE),
        // 4x capacity: the burst-with-headroom scenario.
        "headroom" => n,
        other => panic!("unknown pool shape {other}"),
    };
    let profile = NodeProfile {
        name: "cont".into(),
        count: nodes,
        memory_gb: 8,
        cpus: 2,
        attrs: BTreeMap::new(),
    };
    for index in 0..nodes {
        insert_profile_node(store, &profile, &format!("node-cont-{index}"))?;
    }

    // Submit N single-allocation jobs and enqueue their submit evals.
    for index in 0..n {
        let name = format!("cont-job-{index}");
        let job = JobConfig {
            rollout: None,
            rollout_by_group: Default::default(),
            name: name.clone(),
            tasks: vec![TaskConfig {
                resources: json!({ "memory": TASK_MEMORY_MIB }),
                ..TaskConfig::new("sandbox", "noop")
            }],
            version: None,
        };
        store.handle_job_submit(
            &job,
            &[Submission {
                path: format!("{name}.jsonnet"),
                content: name.clone(),
                is_primary: true,
            }],
        )?;
        let job_config_id = store.get_job_config(&name)?.id();
        store.enqueue_eval("job_submit", Some(&name), Some(&job_config_id), None, None)?;
    }

    // Worker pool: the daemon's claim → handle → complete loop.
    let workers: usize = env_or("LEGION_CONTENTION_WORKERS", "8").parse()?;
    let deadline = Duration::from_secs(env_or("LEGION_CONTENTION_DEADLINE_SECS", "60").parse()?);
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let live = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..workers)
        .map(|worker_id| {
            let store = store.clone();
            let stop = Arc::clone(&stop);
            let live = Arc::clone(&live);
            std::thread::spawn(move || -> R<()> {
                while !stop.load(Ordering::Relaxed) {
                    match store.claim_eval()? {
                        Some(eval) => {
                            live.fetch_add(1, Ordering::Relaxed);
                            legion::manager::daemon::handle_claimed_eval(&store, worker_id, &eval);
                            live.fetch_sub(1, Ordering::Relaxed);
                        }
                        None => std::thread::sleep(Duration::from_millis(5)),
                    }
                }
                Ok(())
            })
        })
        .collect();

    // Watch until everything placed, or the queue is fully drained (no
    // pending/in-flight work left — pre-backstop, failed evals drain), or
    // the deadline passes (post-backstop saturation keeps retrying).
    let mut time_to_all_placed_ms: Option<u128> = None;
    loop {
        let placed = count(store, "SELECT COUNT(*) FROM allocation;")?;
        if placed as usize >= n {
            time_to_all_placed_ms = Some(started.elapsed().as_millis());
            break;
        }
        let queued = count(
            store,
            "SELECT COUNT(*) FROM eval WHERE status IN ('pending', 'in_flight');",
        )?;
        if queued == 0 && live.load(Ordering::Relaxed) == 0 {
            break; // drained without placing everything: drops happened
        }
        if started.elapsed() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    let wall_ms = started.elapsed().as_millis();
    for thread in threads {
        thread.join().expect("worker panicked")?;
    }

    let placed = count(store, "SELECT COUNT(*) FROM allocation;")? as usize;
    let dropped = count(store, "SELECT COUNT(*) FROM eval WHERE status = 'failed';")? as usize;
    let conflict_rejections = count(
        store,
        "SELECT COUNT(*) FROM plan_submission WHERE status = 'rejected';",
    )?;
    let submissions = count(store, "SELECT COUNT(*) FROM plan_submission;")?;

    // Correctness regardless of contention: never oversubscribe a node.
    let oversubscribed = count(
        store,
        &format!(
            "SELECT COUNT(*) FROM (SELECT node_id, COUNT(*) c FROM allocation \
             GROUP BY node_id HAVING c > {ALLOCS_PER_NODE});"
        ),
    )?;
    assert_eq!(oversubscribed, 0, "a node exceeded its capacity");

    Ok(Cell {
        backend: backend.label(),
        pool: pool.to_string(),
        n,
        nodes,
        placed,
        dropped,
        conflict_rejections,
        submissions,
        time_to_all_placed_ms,
        wall_ms,
    })
}

#[test]
#[ignore = "benchmark matrix; run with --ignored --nocapture"]
fn contention_benchmark_matrix() -> R<()> {
    let backends = Backend::parse_list(&env_or("LEGION_BENCH_BACKENDS", "local_disk"));
    let ns: Vec<usize> = env_or("LEGION_CONTENTION_N", "8,32,128")
        .split(',')
        .map(|value| value.trim().parse().expect("burst size"))
        .collect();
    let pools: Vec<String> = env_or("LEGION_CONTENTION_POOLS", "tight,headroom")
        .split(',')
        .map(|value| value.trim().to_string())
        .collect();

    let mut cells = Vec::new();
    for backend in &backends {
        for pool in &pools {
            for &n in &ns {
                let cell = run_cell(*backend, pool, n)?;
                eprintln!(
                    "[{} {} N={}] placed {}/{} dropped {} conflicts {} submissions {} all_placed {:?}ms wall {}ms",
                    cell.backend, cell.pool, cell.n, cell.placed, cell.n, cell.dropped,
                    cell.conflict_rejections, cell.submissions, cell.time_to_all_placed_ms, cell.wall_ms
                );
                cells.push(cell);
            }
        }
    }

    println!(
        "{:<10} {:<9} {:>5} {:>6} {:>7} {:>8} {:>10} {:>12} {:>14}",
        "backend",
        "pool",
        "N",
        "nodes",
        "placed",
        "dropped",
        "conflicts",
        "submissions",
        "all_placed ms"
    );
    for cell in &cells {
        println!(
            "{:<10} {:<9} {:>5} {:>6} {:>7} {:>8} {:>10} {:>12} {:>14}",
            cell.backend,
            cell.pool,
            cell.n,
            cell.nodes,
            cell.placed,
            cell.dropped,
            cell.conflict_rejections,
            cell.submissions,
            cell.time_to_all_placed_ms
                .map(|ms| ms.to_string())
                .unwrap_or_else(|| format!("DNF@{}", cell.wall_ms)),
        );
    }

    let out_dir = std::path::Path::new("target/benchmark-results");
    std::fs::create_dir_all(out_dir)?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let rows: Vec<serde_json::Value> = cells
        .iter()
        .map(|cell| {
            json!({
                "backend": cell.backend, "pool": cell.pool, "n": cell.n,
                "nodes": cell.nodes, "placed": cell.placed, "dropped": cell.dropped,
                "conflict_rejections": cell.conflict_rejections,
                "submissions": cell.submissions,
                "time_to_all_placed_ms": cell.time_to_all_placed_ms.map(|v| v as u64),
                "wall_ms": cell.wall_ms as u64,
            })
        })
        .collect();
    let out = out_dir.join(format!("contention-{stamp}.json"));
    std::fs::write(
        &out,
        serde_json::to_string_pretty(&json!({
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "cells": rows,
        }))?,
    )?;
    println!("results written to {}", out.display());
    Ok(())
}
