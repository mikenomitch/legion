//! Scheduler SLO gate: every scenario in `benchmarks/scheduler/scenarios/`
//! must meet its throughput / latency / conflict / correctness thresholds
//! on the default (`local_disk`) backend.
//!
//! Thresholds come from `benchmarks/scheduler/slo.json`, overridable per
//! scenario via the scenario's `slo` block (bigger scenarios get looser
//! bounds than the global default). CI runs this on scheduler-impacting
//! changes; the full backend matrix lives in
//! `tests/integration/scheduler_scale_benchmark.rs`.
//!
//! Run locally:
//! `cargo test --test integration scheduler_benchmark_slo:: -- --ignored --nocapture`

use crate::benchmark_support;

use benchmark_support::{load_scenarios, load_slo, run_scenario_on, setup_backend, Backend};

#[test]
#[ignore = "benchmark gate for scheduler-impacting changes"]
fn scheduler_benchmark_slo_gate() -> anyhow::Result<()> {
    let scenarios = load_scenarios()?;
    let global_slo = load_slo()?;
    assert!(!scenarios.is_empty(), "at least one scenario is required");

    for scenario in scenarios {
        let slo = global_slo.for_scenario(&scenario);
        let handle = setup_backend(Backend::LocalDisk, &scenario)?;
        let metrics = run_scenario_on(handle.store(), &scenario)?;
        println!(
            "scenario={} evals_per_sec={:.2} p50_latency_ms={:.2} p99_latency_ms={:.2} conflict_rejection_rate={:.4} correctness_ratio={:.3}",
            scenario.name,
            metrics.evals_per_sec,
            metrics.p50_latency_ms,
            metrics.p99_latency_ms,
            metrics.conflict_rejection_rate,
            metrics.correctness_ratio,
        );
        println!("    phases: {}", metrics.phase_means_us.render());

        assert!(
            metrics.evals_per_sec >= slo.min_evals_per_sec,
            "scenario={} evals/sec {:.2} below target {:.2}",
            scenario.name,
            metrics.evals_per_sec,
            slo.min_evals_per_sec
        );
        assert!(
            metrics.p99_latency_ms <= slo.max_p99_latency_ms,
            "scenario={} p99 latency {:.2}ms exceeds target {:.2}ms",
            scenario.name,
            metrics.p99_latency_ms,
            slo.max_p99_latency_ms
        );
        assert!(
            metrics.conflict_rejection_rate <= slo.max_conflict_rejection_rate,
            "scenario={} conflict rejection rate {:.4} exceeds target {:.4}",
            scenario.name,
            metrics.conflict_rejection_rate,
            slo.max_conflict_rejection_rate
        );
        assert!(
            metrics.correctness_ratio >= slo.min_correctness_ratio,
            "scenario={} correctness ratio {:.3} below target {:.3}",
            scenario.name,
            metrics.correctness_ratio,
            slo.min_correctness_ratio
        );
    }

    Ok(())
}
