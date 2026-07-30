//! Scheduler benchmark matrix: every scenario in
//! `benchmarks/scheduler/scenarios/` × every requested persistence backend.
//!
//! This is the "get benchmarks for all of this" runner from
//! `internal_docs/plans/scheduler-performance-plan.md`: local_disk, raft
//! single-node, raft 3-node (in-process cluster, real tonic transport), and
//! streaming persistence (rustyriver, `file://` replica).
//!
//! ```bash
//! # Full matrix (slow — release recommended):
//! LEGION_BENCH_BACKENDS=local_disk,raft1,raft3,streaming \
//!   cargo test --release --test scheduler_scale_benchmark -- --ignored --nocapture
//!
//! # One scenario, one backend:
//! LEGION_BENCH_BACKENDS=raft3 LEGION_BENCH_SCENARIOS=web-fleet \
//!   cargo test --release --test scheduler_scale_benchmark -- --ignored --nocapture
//! ```
//!
//! Results also land as JSON in `target/benchmark-results/` so runs can be
//! diffed mechanically. Recorded numbers live in
//! `benchmarks/scheduler/README.md` (host and build profile noted).

#[path = "benchmark_support/mod.rs"]
mod benchmark_support;

use benchmark_support::{
    load_scenarios, run_scenario_on, setup_backend, Backend, SchedulerScenario,
};
use legion::prelude::R;

fn requested_backends() -> Vec<Backend> {
    match std::env::var("LEGION_BENCH_BACKENDS") {
        Ok(raw) => Backend::parse_list(&raw),
        Err(_) => vec![Backend::LocalDisk],
    }
}

fn requested_scenarios() -> R<Vec<SchedulerScenario>> {
    let scenarios = load_scenarios()?;
    let filter = std::env::var("LEGION_BENCH_SCENARIOS").ok();
    Ok(match filter {
        None => scenarios,
        Some(raw) => {
            let wanted: Vec<&str> = raw.split(',').map(str::trim).collect();
            scenarios
                .into_iter()
                .filter(|scenario| wanted.contains(&scenario.name.as_str()))
                .collect()
        }
    })
}

#[test]
#[ignore = "benchmark matrix; run with --ignored --nocapture (see module docs)"]
fn scheduler_scale_benchmark_matrix() -> R<()> {
    let scenarios = requested_scenarios()?;
    let backends = requested_backends();
    assert!(!scenarios.is_empty(), "no scenarios matched");
    // LEGION_BENCH_CONCURRENCY=N drives each cell with N concurrent
    // submitters (bounded conflict retries, like the production eval
    // path) — the shape that exercises the plan-apply group commit.
    let concurrency: usize = std::env::var("LEGION_BENCH_CONCURRENCY")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(1);

    let mut results = Vec::new();
    println!(
        "{:<22} {:<10} {:>9} {:>9} {:>9} {:>9} {:>8} {:>7}",
        "scenario", "backend", "evals/s", "p50 ms", "p95 ms", "p99 ms", "confl%", "correct"
    );

    for scenario in &scenarios {
        for backend in &backends {
            let handle = setup_backend(*backend, scenario)?;
            let metrics = if concurrency > 1 {
                benchmark_support::run_scenario_concurrent(handle.store(), scenario, concurrency)?
            } else {
                run_scenario_on(handle.store(), scenario)?
            };
            println!(
                "{:<22} {:<10} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>8.2} {:>7.3}",
                scenario.name,
                backend.label(),
                metrics.evals_per_sec,
                metrics.p50_latency_ms,
                metrics.p95_latency_ms,
                metrics.p99_latency_ms,
                metrics.conflict_rejection_rate * 100.0,
                metrics.correctness_ratio,
            );
            println!("    phases: {}", metrics.phase_means_us.render());

            // A cell only counts if it scheduled correctly — a fast wrong
            // scheduler is not a benchmark result.
            assert!(
                (metrics.correctness_ratio - 1.0).abs() < f64::EPSILON,
                "{} on {}: correctness {} != 1.0",
                scenario.name,
                backend.label(),
                metrics.correctness_ratio
            );

            results.push(serde_json::json!({
                "scenario": scenario.name,
                "backend": backend.label(),
                "concurrency": concurrency,
                "evals_per_sec": metrics.evals_per_sec,
                "p50_latency_ms": metrics.p50_latency_ms,
                "p95_latency_ms": metrics.p95_latency_ms,
                "p99_latency_ms": metrics.p99_latency_ms,
                "conflict_rejection_rate": metrics.conflict_rejection_rate,
                "correctness_ratio": metrics.correctness_ratio,
                "phase_means_us": {
                    "available_nodes": metrics.phase_means_us.available_nodes_us,
                    "version_sample": metrics.phase_means_us.version_sample_us,
                    "task_fetch": metrics.phase_means_us.task_fetch_us,
                    "capacity_snapshot": metrics.phase_means_us.capacity_snapshot_us,
                    "build_plan": metrics.phase_means_us.build_plan_us,
                    "apply": metrics.phase_means_us.apply_us,
                    "total": metrics.phase_means_us.total_us,
                },
            }));
        }
    }

    let out_dir = std::path::Path::new("target/benchmark-results");
    std::fs::create_dir_all(out_dir)?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let out_path = out_dir.join(format!("scale-{stamp}.json"));
    std::fs::write(
        &out_path,
        serde_json::to_string_pretty(&serde_json::json!({
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "results": results,
        }))?,
    )?;
    println!("results written to {}", out_path.display());

    Ok(())
}

/// Instrumentation smoke (not ignored — runs in normal CI): a tiny scenario
/// on local_disk must produce populated, coherent `PlanTimings`.
#[test]
fn plan_timings_are_populated_and_coherent() -> R<()> {
    let scenario: SchedulerScenario = serde_json::from_value(serde_json::json!({
        "name": "timings-smoke",
        "scheduling_policy": "bin_pack",
        "iterations": 1,
        "node_profiles": [
            { "name": "a", "count": 4, "attrs": { "class": "a" } },
            { "name": "b", "count": 2, "attrs": { "class": "b" } }
        ],
        "job_types": [
            { "name": "svc", "count": 2, "tasks_per_job": 2, "task_count_per_task": 1,
              "constraints": { "class": "a" } }
        ]
    }))?;

    let handle = setup_backend(Backend::LocalDisk, &scenario)?;
    let metrics = run_scenario_on(handle.store(), &scenario)?;

    assert!((metrics.correctness_ratio - 1.0).abs() < f64::EPSILON);
    let phases = &metrics.phase_means_us;
    assert!(phases.total_us > 0, "total_us must be measured");
    assert!(phases.build_plan_us > 0, "build_plan_us must be measured");
    assert!(phases.apply_us > 0, "apply_us must be measured");
    // Phases are disjoint slices of total: their sum can't exceed it.
    let sum = phases.available_nodes_us
        + phases.version_sample_us
        + phases.task_fetch_us
        + phases.capacity_snapshot_us
        + phases.build_plan_us
        + phases.apply_us;
    assert!(
        sum <= phases.total_us,
        "phase sum {sum} exceeds total {}",
        phases.total_us
    );
    Ok(())
}
