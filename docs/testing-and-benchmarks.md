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

Benchmark targets:

- `benches/scheduler_bench.rs` (Criterion)
- `benches/eval_critical_path_bench.rs` (Criterion)
- `tests/scheduler_benchmark_slo.rs` (scenario runner + SLO gate, CI)
- `tests/scheduler_scale_benchmark.rs` (scenario × persistence-backend
  matrix, manual)
- `tests/plan_contention.rs` (concurrent-submit contention harness)
- Scenario definitions in `benchmarks/scheduler/scenarios/*.json`
- SLO thresholds in `benchmarks/scheduler/slo.json` (global), overridable
  per scenario via the scenario's `slo` block
- Shared runner code in `tests/benchmark_support/mod.rs`

What they measure:

- `scheduler_bench`: scheduler planning/apply throughput across a scale matrix.
  - Policies tested: `bin_pack`, `spread`.
  - Scale dimensions: node counts 10 -> 100,000; allocation counts 10 -> 100,000.
  - `candidate_construction` group: isolates per-task candidate-set cost
    (constraints over a 16-region heterogeneous fleet) with scheduling
    indexes off vs on ([scheduling-indexes.md](scheduling-indexes.md)), at
    1k/5k/10k nodes. Scheduler changes ship with before/after numbers from
    this group (see AGENTS.md).
- `eval_critical_path_bench`: end-to-end enqueue -> claim -> apply -> complete
  latency under 1/4/8 concurrent eval workers (write-lock contention).
- `plan_contention`: N-way concurrent single-allocation submits against one
  pool, pumped by real eval workers — measures conflict rejections,
  requeues, fallback binds, dropped evals, and time-to-all-placed, with
  candidate lists on vs off
  ([late-binding-plan-apply.md](late-binding-plan-apply.md)). The
  non-ignored tests are the CI regression; the `#[ignore]`d
  `contention_matrix` prints the full grid recorded in
  `benchmarks/scheduler/README.md`.
- `scheduler_benchmark_slo` / `scheduler_scale_benchmark`: scenario-driven
  whole-plan runs. Scenarios describe realistic fleets and workloads:
  `node_profiles` (heterogeneous node classes: count, memory, cpus,
  attrs like region/class/gpu) and `job_types` (mixed workloads with
  `resources` and `constraints`), plus optional `scheduler_settings`
  (e.g. arming scheduling indexes) and per-scenario `slo` bounds. Legacy
  uniform fields (`node_count`, `job_count`, ...) still parse.
  - Both runners report throughput, p50/p95/p99 latency, conflict rate,
    correctness (full placement + constraint conformance), and the mean
    per-phase breakdown from `PlanTimings` (the same phases as the
    `legion_scheduler_plan_phase_seconds` metric — [metrics.md](metrics.md)).
  - The scale matrix additionally runs every scenario against each
    requested persistence backend: `local_disk`, `raft1` (single-node
    raft), `raft3` (3-node in-process cluster over real tonic transport),
    and `streaming` (rustyriver, `file://` replica). Results are printed
    and written as JSON to `target/benchmark-results/`.

Run commands:

- `cargo bench --bench scheduler_bench`
- `cargo bench --bench eval_critical_path_bench`
- `cargo test --test scheduler_benchmark_slo -- --ignored --nocapture`
- `cargo test --test plan_contention -- --ignored --nocapture`
- Full backend matrix (use a release build for recorded numbers):

```bash
LEGION_BENCH_BACKENDS=local_disk,raft1,raft3,streaming cargo test --release --test scheduler_scale_benchmark -- --ignored --nocapture
```

- Subsets via `LEGION_BENCH_BACKENDS` and `LEGION_BENCH_SCENARIOS`
  (comma-separated scenario names).

- Heartbeat scale matrix (`tests/heartbeat_scale_benchmark.rs`): fleet
  size × backend; measures heartbeat throughput (N driver threads
  heartbeating a fleet partition flat-out), p50/p99 heartbeat latency,
  and the wall-clock of one liveness tick at that fleet size:

```bash
LEGION_BENCH_BACKENDS=local_disk,raft1 LEGION_HEARTBEAT_NODES=1000,5000 \
  cargo test --release --test heartbeat_scale_benchmark -- --ignored --nocapture
```

  Knobs: `LEGION_HEARTBEAT_THREADS` (default 8) and
  `LEGION_HEARTBEAT_SECONDS` (measurement window per cell, default 3).

Recorded results live in `benchmarks/scheduler/README.md`, with the host
and build profile noted alongside every table.

### Measuring scheduler event-stream overhead

`LEGION_EVENTS_OFF=1` disables scheduler event emission, which allows an
A/B comparison of emission cost in the same binary:

```bash
LEGION_EVENTS_OFF=1 cargo bench --bench eval_critical_path_bench -- --save-baseline events-off
cargo bench --bench eval_critical_path_bench -- --baseline events-off
```

See `docs/event-stream.md` for the current numbers and the design constraints
that keep this overhead at the noise floor.

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

- `.github/workflows/ci.yml` runs the SLO benchmark gate only when scheduler-impacting files are changed (`src/manager/scheduler.rs`, `src/storage/store.rs`, `src/manager/daemon.rs`, benchmark scenarios, the shared runner in `tests/benchmark_support/`, or the gate test itself).
- The backend matrix (`scheduler_scale_benchmark`) is manual — it stands up
  raft clusters and is too heavy for per-PR CI. Its `plan_timings_are_populated_and_coherent`
  smoke test does run in normal CI and pins the phase instrumentation.

### AWS comparative benchmark rig

`benchmarks/aws/` holds the full-system comparison rig: Legion (raft
lanes and streaming persistence), Nomad, and Kubernetes measured on
identical AWS hardware with simulated worker fleets (Legion's
[`--simulate`](worker-simulation.md), nomad-nodesim, KWOK). The
directory's `README.md` is the runbook; the local, no-AWS validation
loop is:

```bash
# Driver counting/latency math against known-good fixtures:
python3 -m unittest discover benchmarks/aws/driver/tests

# Full local loop: manager + simulated fleet + translated workload +
# the real driver (also builds plugins):
benchmarks/aws/scripts/local-smoke.sh
```

CI runs the fixture tests, `terraform validate`, and shell syntax checks
whenever `benchmarks/aws/**` changes; nothing in CI touches AWS. The
end-to-end sim-fleet wire test is `tests/worker_simulation.rs` (runs in
the normal `cargo test` suite).

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
