# Testing and Benchmarks

This document summarizes current test and benchmark coverage.

## Test Layout

Legion uses a mixed strategy:

- Unit tests colocated in module files (`#[cfg(test)]` in `src/`).
- Integration tests under `tests/`.

Integration tests currently include:

- `tests/config_jobs.rs`
  - validates all Jsonnet job configs deserialize to `JobConfig`.
  - validates maximal config shape.
- `tests/scheduler_integration.rs`
  - end-to-end scheduling scenarios (constraints, permit, count.per, reschedule behavior).
- `tests/eval_loop.rs`
  - eval ordering and eval-driven ledger reconciliation behavior.
- `tests/rollout_integration.rs`
  - rollout progression behavior (auto/manual/create-first/destroy-first, etc).

## Running Tests

Primary command:

- `cargo test --all --locked`

Repo convention also expects running tests for Rust changes (docs/frontend-only changes are the usual exception).

## Benchmark Coverage

Benchmark target:

- `benches/scheduler_bench.rs` (Criterion)
- `tests/scheduler_benchmark_slo.rs` (repeatable scenario + SLO gate)
- Scenario definitions in `benchmarks/scheduler/scenarios/*.json`
- SLO thresholds in `benchmarks/scheduler/slo.json`

What it measures:

- Scheduler planning/apply throughput across scale matrix.
- Policies tested: `bin_pack`, `spread`.
- Scale dimensions:
  - node counts: 10 -> 100,000
  - allocation counts: 10 -> 100,000

Run command:

- `cargo bench --bench scheduler_bench`
- `cargo test --test scheduler_benchmark_slo -- --ignored --nocapture`

### Scheduler Benchmark SLOs

The scheduler SLO gate validates the following across every scenario file:

- minimum throughput (`evals_per_sec`)
- maximum p99 scheduling latency (`max_p99_latency_ms`)
- maximum conflict rejection rate (`max_conflict_rejection_rate`)
- minimum scheduling correctness ratio (`min_correctness_ratio`)

Scenario files encode:

- number of jobs
- tasks per job
- task count per task
- node count
- churn cadence (`churn_every`) and churn magnitude (`churn_nodes`)

CI behavior:

- `.github/workflows/ci.yml` runs the SLO benchmark gate only when scheduler-impacting files are changed (`src/manager/scheduler.rs`, `src/storage/store.rs`, `src/manager/agent.rs`, benchmark scenarios, or the gate test itself).

## Plugin-Related Test Notes

Dynamic plugin tests may depend on plugin build presence.

- `LEGION_BUILD_PLUGINS=1` enables plugin builds via `build.rs`.
- `LEGION_SKIP_PLUGIN_BUILD` skips plugin builds.

Some tests are written to tolerate plugin binaries being absent.

## Test Data and Isolation

Most store-backed tests:

- create temp SQLite directories per test
- reset migrations/tables for clean state
- avoid shared mutable fixtures
