# Testing and Benchmarks

This document summarizes current test and benchmark coverage.

## Test Layout

Legion uses a mixed strategy:

- Unit tests colocated in module files (`#[cfg(test)]` in `src/`).
- Integration tests under `tests/`.

Integration tests currently include:

- `tests/config_jobs.rs`
  - evaluates every shipped `configs/jobs/*.ncl` and validates it the way the
    manager does on submit, not just that it deserializes to `JobConfig`.
  - validates maximal config shape.
  - evaluates the shipped manager configs (`configs/*.ncl`) and worker configs
    (`configs/workers/*.ncl`) into their config structs.
  - parses the shipped job template, resolves its defaults, renders it through
    the manager's sandboxed path, and validates the result.
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

Everything benchmark-related lives under `benchmarks/`: the Criterion
micro-benchmarks in `benchmarks/micro/`, the scenario-driven scheduler
harness in `benchmarks/scheduler/`, and the AWS comparison rig in
`benchmarks/aws/`. The Criterion targets are declared in `Cargo.toml` with
an explicit `path`, so `cargo bench --bench <name>` works as usual even
though they are not in Cargo's default `benches/` directory. Only the
runners themselves live elsewhere, next to the tests they are written as
(`tests/`).

Benchmark targets:

- `benchmarks/micro/scheduler_bench.rs` (Criterion)
- `benchmarks/micro/eval_critical_path_bench.rs` (Criterion)
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

    **Measure the noise floor before believing a delta.** On a shared or
    virtualized host this group's 5k/10k cases routinely swing ±10% run to
    run, and Criterion will report that swing as "Performance has regressed"
    with `p = 0.00` — the p-value tests whether two *samples* differ, not
    whether the *code* does. Run the base commit against its own saved
    baseline first:

    ```console
    $ git checkout <base> && cargo bench --bench scheduler_bench -- \
        candidate_construction --save-baseline base
    $ cargo bench --bench scheduler_bench -- candidate_construction --baseline base
    # base vs base: whatever this reports is noise, not signal
    $ git checkout <branch> && cargo bench --bench scheduler_bench -- \
        candidate_construction --baseline base
    ```

    A change is only real if it exceeds that floor. Report both, and say which
    it is.
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

**A heartbeat number states which storage it was taken on**, and whether
liveness was on the raft path. Since a steady-state heartbeat costs no
commit ([node-liveness.md](node-liveness.md)), a modern number measures the
gRPC handler; a number from a cluster in the mixed-version fallback measures
the commit path, and the two are not comparable. The gp3-to-NVMe ratio is the
single figure that tells them apart: near 1.0 means liveness is off the raft
path, ~25x means it is not.

**A throughput number carries the cadence it was achieved at.** A system that
quietly slows its own heartbeats to score better is cheating, so every
benchmark run records the served interval and
`legion_heartbeat_freshness_ratio` alongside throughput, and comparative runs
either pin all systems to the same interval or report all three systems'
*achieved* cadence. Adaptive cadence is off by default for exactly this
reason ([heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md)).

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

The rig submits Legion jobs through `legion-loadgen`
(`benchmarks/loadgen/`), a benchmark-only binary that holds a small pool
of gRPC connections open and issues many concurrent `RunJob` calls. It
is feature-gated so it is not part of an ordinary build:

```bash
cargo build --release --features loadgen --bin legion-loadgen
```

It exists because submitting with one `legion job run` process per job
ceilings at roughly 40-100 jobs/sec, which was low enough that campaign
cells reported the driver's rate rather than the scheduler's — the
driver flags that case as `submission.driver_bound: true`.

**It is Legion-only, and that makes it a fairness hazard.** Nomad and
Kubernetes still submit one CLI process per job, so a cross-system
comparison must put all three on the shared loop
(`driver.py run --submitter cli`) or add native submitters for the
others first. Every cell records which path it took as `submitter` in
its metrics. See `benchmarks/aws/README.md` § Methodology.

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
