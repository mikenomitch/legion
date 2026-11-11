//! Heartbeat scale benchmark: fleet size × persistence backend
//! (`internal_docs/plans/heartbeat-scale-plan.md`).
//!
//! Two measurements per cell:
//!
//! - **Heartbeat throughput**: N driver threads heartbeat a partition of
//!   the fleet as fast as the store allows for a fixed window —
//!   heartbeats/sec plus p50/p99 latency of `Store::update_node_heartbeat`.
//! - **Liveness tick**: wall-clock of one manager liveness pass
//!   (`liveness_snapshot` + the mark-lost decision) at that
//!   fleet size.
//!
//! ```bash
//! LEGION_BENCH_BACKENDS=local_disk,raft1,raft3 \
//! LEGION_HEARTBEAT_NODES=1000,10000 \
//!   cargo test --release --test integration heartbeat_scale_benchmark:: -- --ignored --nocapture
//! ```
//!
//! Results land as JSON in `target/benchmark-results/heartbeat-*.json`;
//! recorded numbers live in `benchmarks/scheduler/README.md`.

use crate::benchmark_support;

use benchmark_support::{insert_profile_node, pctl, setup_backend, Backend, NodeProfile};
use legion::prelude::R;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn dummy_scenario(nodes: usize) -> benchmark_support::SchedulerScenario {
    serde_json::from_value(serde_json::json!({
        "name": format!("heartbeat-{nodes}"),
        "scheduling_policy": "bin_pack",
        "iterations": 1,
        "node_count": nodes,
        "job_count": 0,
        "tasks_per_job": 0,
        "task_count_per_task": 0,
    }))
    .expect("static scenario json")
}

struct Cell {
    backend: &'static str,
    nodes: usize,
    threads: usize,
    heartbeats_per_sec: f64,
    p50_ms: f64,
    p99_ms: f64,
    liveness_tick_ms: f64,
    /// The interval this cell's fleet was told to heartbeat at, and the
    /// fraction of it that checked in on time.
    ///
    /// Non-negotiable in the result JSON (heartbeat-cadence plan §6.4): a
    /// throughput number without its cadence is not publishable, because a
    /// system that quietly slows its own heartbeats scores better for free.
    served_interval_secs: u64,
    freshness_ratio: f64,
    /// Whether liveness was on the raft path for this cell. A heartbeat number
    /// from a cluster in the mixed-version forwarding fallback measures the
    /// commit path; one from a modern cluster measures the gRPC handler. The
    /// two are not comparable (docs/testing-and-benchmarks.md).
    liveness_on_raft_path: bool,
}

#[test]
#[ignore = "benchmark matrix; run with --ignored --nocapture"]
fn heartbeat_scale_benchmark_matrix() -> R<()> {
    let backends = Backend::parse_list(&env_or("LEGION_BENCH_BACKENDS", "local_disk"));
    let fleet_sizes: Vec<usize> = env_or("LEGION_HEARTBEAT_NODES", "1000,5000")
        .split(',')
        .map(|value| value.trim().parse().expect("fleet size"))
        .collect();
    let threads: usize = env_or("LEGION_HEARTBEAT_THREADS", "8").parse()?;
    let measure_secs: u64 = env_or("LEGION_HEARTBEAT_SECONDS", "3").parse()?;

    let mut cells: Vec<Cell> = Vec::new();
    for backend in &backends {
        for &nodes in &fleet_sizes {
            let scenario = dummy_scenario(nodes);
            let handle = setup_backend(*backend, &scenario)?;
            let store = handle.store();

            let profile = NodeProfile {
                name: "hb".into(),
                count: nodes,
                memory_gb: 8,
                cpus: 2,
                attrs: BTreeMap::new(),
            };
            let insert_started = Instant::now();
            for index in 0..nodes {
                insert_profile_node(store, &profile, &format!("node-hb-{index}"))?;
            }
            eprintln!(
                "[{} nodes={nodes}] fleet inserted in {:.1}s",
                backend.label(),
                insert_started.elapsed().as_secs_f64()
            );

            // Warm the path once per node partition boundary.
            store.update_node_heartbeat("node-hb-0".to_string())?;

            // Throughput phase: each thread walks its own partition.
            let stop = Arc::new(AtomicBool::new(false));
            let workers: Vec<_> = (0..threads)
                .map(|thread_index| {
                    let store = store.clone();
                    let stop = Arc::clone(&stop);
                    std::thread::spawn(move || -> R<(u64, Vec<f64>)> {
                        let mut count = 0u64;
                        let mut latencies_ms = Vec::new();
                        let mut cursor = thread_index;
                        while !stop.load(Ordering::Relaxed) {
                            let id = format!("node-hb-{}", cursor % nodes);
                            cursor += threads;
                            let started = Instant::now();
                            store.update_node_heartbeat(id)?;
                            latencies_ms.push(started.elapsed().as_secs_f64() * 1000.0);
                            count += 1;
                        }
                        Ok((count, latencies_ms))
                    })
                })
                .collect();
            let window_started = Instant::now();
            std::thread::sleep(Duration::from_secs(measure_secs));
            stop.store(true, Ordering::Relaxed);
            let mut total = 0u64;
            let mut latencies_ms: Vec<f64> = Vec::new();
            for worker in workers {
                let (count, worker_latencies) = worker.join().expect("worker panicked")?;
                total += count;
                latencies_ms.extend(worker_latencies);
            }
            let elapsed = window_started.elapsed().as_secs_f64();

            // Liveness phase: one manager tick's scan + decision, averaged.
            // The fake value is a fake RESPONSIBILITY age, not a fake process
            // uptime — the grace clock now runs from status-lane leadership
            // acquisition (liveness plan §6.4), and the ages the decision
            // compares come from the in-memory table the heartbeats above
            // populated rather than from the database.
            let tick_rounds = 10;
            let tick_started = Instant::now();
            for _ in 0..tick_rounds {
                let snapshot = legion::manager::daemon::liveness_snapshot(store, Instant::now())?;
                let lost = legion::manager::cadence::decide_lost_nodes(
                    &snapshot,
                    &legion::manager::cadence::LossContext {
                        responsible_for: Duration::from_secs(3600),
                        ..Default::default()
                    },
                )
                .lost;
                // Every node just heartbeated: nothing should be lost.
                assert!(
                    lost.len() < nodes,
                    "liveness decision marked the whole fleet lost"
                );
            }
            let liveness_tick_ms =
                tick_started.elapsed().as_secs_f64() * 1000.0 / tick_rounds as f64;

            // Cadence, recorded alongside throughput rather than assumed.
            let served_interval =
                Duration::from_secs(legion::constants::WORKER_HEARTBEAT_DURATION_SEC);
            let snapshot = legion::manager::daemon::liveness_snapshot(store, Instant::now())?;
            let freshness_ratio =
                legion::manager::cadence::freshness_ratio(&snapshot, served_interval, 0)
                    .unwrap_or(0.0);

            cells.push(Cell {
                backend: backend.label(),
                nodes,
                threads,
                heartbeats_per_sec: total as f64 / elapsed,
                p50_ms: pctl(&latencies_ms, 0.50),
                p99_ms: pctl(&latencies_ms, 0.99),
                liveness_tick_ms,
                served_interval_secs: served_interval.as_secs(),
                freshness_ratio,
                liveness_on_raft_path: store.liveness().digest_fallback(),
            });
            let cell = cells.last().expect("just pushed");
            eprintln!(
                "[{} nodes={nodes}] {:.0} hb/s p50 {:.2}ms p99 {:.2}ms liveness_tick {:.2}ms \
                 cadence {}s freshness {:.2}",
                cell.backend,
                cell.heartbeats_per_sec,
                cell.p50_ms,
                cell.p99_ms,
                cell.liveness_tick_ms,
                cell.served_interval_secs,
                cell.freshness_ratio
            );
        }
    }

    println!(
        "{:<12} {:>8} {:>8} {:>10} {:>9} {:>9} {:>12}",
        "backend", "nodes", "threads", "hb/s", "p50 ms", "p99 ms", "liveness ms"
    );
    for cell in &cells {
        println!(
            "{:<12} {:>8} {:>8} {:>10.0} {:>9.2} {:>9.2} {:>12.2}",
            cell.backend,
            cell.nodes,
            cell.threads,
            cell.heartbeats_per_sec,
            cell.p50_ms,
            cell.p99_ms,
            cell.liveness_tick_ms
        );
    }

    let results: Vec<serde_json::Value> = cells
        .iter()
        .map(|cell| {
            serde_json::json!({
                "backend": cell.backend,
                "nodes": cell.nodes,
                "threads": cell.threads,
                "heartbeats_per_sec": cell.heartbeats_per_sec,
                "p50_ms": cell.p50_ms,
                "p99_ms": cell.p99_ms,
                "liveness_tick_ms": cell.liveness_tick_ms,
                "served_interval_secs": cell.served_interval_secs,
                "freshness_ratio": cell.freshness_ratio,
                "liveness_on_raft_path": cell.liveness_on_raft_path,
            })
        })
        .collect();
    let out_dir = std::path::Path::new("target/benchmark-results");
    std::fs::create_dir_all(out_dir)?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let out = out_dir.join(format!("heartbeat-{stamp}.json"));
    std::fs::write(
        &out,
        serde_json::to_string_pretty(&serde_json::json!({
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "cells": results,
        }))?,
    )?;
    println!("results written to {}", out.display());
    Ok(())
}
