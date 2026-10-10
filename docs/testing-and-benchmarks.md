# Testing and Benchmarks

This page covers how Legion is tested and benchmarked, and how to run each suite. It is written for contributors.

## Test Layout

Legion uses a mixed strategy:

- Unit tests colocated in module files (`#[cfg(test)]` in `src/`).
- Integration tests under `tests/`.

Integration tests currently include:

- `tests/config_jobs.rs`
  - evaluates every shipped `configs/jobs/*.ncl` and validates it the same
    way the manager does on submit. Deserializing to `JobConfig` alone is not
    enough to pass.
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

Run the tests for any Rust change. Docs-only and frontend-only changes are the usual exception.

## Benchmark Coverage

Benchmark code lives under `benchmarks/`:

- Criterion micro-benchmarks in `benchmarks/micro/`.
- The scenario-driven scheduler harness in `benchmarks/scheduler/`.
- The AWS comparison rig in `benchmarks/aws/`.

The Criterion targets are declared in `Cargo.toml` with an explicit `path`,
so `cargo bench --bench <name>` works even though they are not in Cargo's
default `benches/` directory. The scenario runners are written as tests, so
they live in `tests/`.

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
  - Scale dimensions: node counts 10 -> 100,000, and allocation counts 10 -> 100,000.
  - `candidate_construction` group: isolates per-task candidate-set cost
    (constraints over a 16-region heterogeneous fleet) with scheduling
    indexes off vs on ([scheduling-indexes.md](scheduling-indexes.md)), at
    1k/5k/10k nodes. Scheduler changes ship with before/after numbers from
    this group (see AGENTS.md).

    Measure the noise floor before trusting a delta. On a shared or
    virtualized host, this group's 5k/10k cases often swing ±10% from run to
    run. Criterion reports that swing as "Performance has regressed" with
    `p = 0.00`, because the p-value tests whether two samples differ, and
    says nothing about whether the code changed. Run the base commit against
    its own saved baseline first:

    ```console
    $ git checkout <base> && cargo bench --bench scheduler_bench -- \
        candidate_construction --save-baseline base
    $ cargo bench --bench scheduler_bench -- candidate_construction --baseline base
    # base vs base: whatever this reports is noise, not signal
    $ git checkout <branch> && cargo bench --bench scheduler_bench -- \
        candidate_construction --baseline base
    ```

    Treat a change as meaningful only if it exceeds that floor. Report both
    numbers, and say which one the change falls under.
  - `plan_under_fill` group: measures how planning cost changes as the
    cluster fills up. A 5,000-node fleet is pre-filled with 0, 10, or 40
    allocations per node from other jobs. The fill goes through the store's
    allocation writer, so it is not itself a scheduling measurement. A
    six-placement job is then planned against it two ways:
    `plan_only` (the planning phases alone, with no state drift between
    iterations) and `plan_and_apply` (a fresh job per iteration).

    A cost that stays flat across fill levels is a per-plan constant. A cost
    that grows with fill depends on how full the cluster is, independent of
    the job being placed. That is the slope the AWS `dense_fleet` ladder
    measured. The groups above cannot reproduce it because both start from
    an empty cluster.

    The `plan_and_apply` cells also print the mean per-phase `PlanTimings`
    breakdown, so each run shows where the time went at each fill level.
    These cells include two local commits per plan: the apply, and the
    inline status projection (because the daemon's drain task is not
    running). They vary more from run to run on a VM than the read-only
    cells. Use `plan_only` for planning cost and the phase means for apply
    time.
- `eval_critical_path_bench`: end-to-end enqueue -> claim -> apply -> complete
  latency under 1/4/8 concurrent eval workers (write-lock contention).
- `plan_contention`: N concurrent single-allocation submits against one
  pool, processed by real eval workers. It measures conflict rejections,
  requeues, fallback binds, dropped evals, and time until everything is
  placed, with candidate lists on and off
  ([late-binding-plan-apply.md](late-binding-plan-apply.md)). The
  non-ignored tests are the CI regression check. The `#[ignore]`d
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
    `legion_scheduler_plan_phase_seconds` metric in [metrics.md](metrics.md)).
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
  size × backend. It measures heartbeat throughput (N driver threads
  heartbeating a fleet partition as fast as they can), p50/p99 heartbeat
  latency, and the wall-clock time of one liveness tick at that fleet size:

```bash
LEGION_BENCH_BACKENDS=local_disk,raft1 LEGION_HEARTBEAT_NODES=1000,5000 \
  cargo test --release --test heartbeat_scale_benchmark -- --ignored --nocapture
```

  Knobs: `LEGION_HEARTBEAT_THREADS` (default 8) and
  `LEGION_HEARTBEAT_SECONDS` (measurement window per cell, default 3).

Recorded results live in `benchmarks/scheduler/README.md`, with the host
and build profile noted alongside every table.

When you report a heartbeat number, state which storage it was taken on and
whether liveness was on the raft path. A steady-state heartbeat costs no
commit ([node-liveness.md](node-liveness.md)), so a current number measures
the gRPC handler. A number from a cluster in the mixed-version fallback
measures the commit path instead. The two are not comparable. The gp3-to-NVMe
ratio tells them apart: near 1.0 means liveness is off the raft path, and
around 25x means it is on it.

Report every throughput number with the cadence it was achieved at. A
system could slow its own heartbeats to post a better score. To rule that
out, every benchmark run records the served interval and
`legion_heartbeat_freshness_ratio` alongside throughput. Comparative runs
either pin all systems to the same interval or report each system's achieved
cadence. This is also why adaptive cadence is off by default
([heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md)).

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

- minimum throughput (`min_evals_per_sec`)
- maximum p99 scheduling latency (`max_p99_latency_ms`)
- maximum conflict rejection rate (`max_conflict_rejection_rate`)
- minimum scheduling correctness ratio (`min_correctness_ratio`)

Scenario files encode:

- the fleet: either `node_profiles` (heterogeneous node classes) or a uniform
  `node_count`
- the workload: either `job_types` (mixed workloads) or the uniform
  `job_count`, `tasks_per_job`, and `task_count_per_task`
- the scheduling policy (`scheduling_policy`) and number of `iterations`
- churn cadence (`churn_every`) and churn magnitude (`churn_nodes`)
- optional `scheduler_settings` and per-scenario `slo` overrides

CI behavior:

- `.github/workflows/ci.yml` runs the SLO benchmark gate only when scheduler-impacting files are changed (`src/manager/scheduler.rs`, `src/storage/store.rs`, `src/manager/daemon.rs`, benchmark scenarios, the shared runner in `tests/benchmark_support/`, or the gate test itself).
- The backend matrix (`scheduler_scale_benchmark`) runs manually. It stands
  up raft clusters and is too heavy for per-PR CI. Its
  `plan_timings_are_populated_and_coherent` smoke test does run in normal CI
  and checks the phase instrumentation.

### AWS comparative benchmark rig

`benchmarks/aws/` holds the full-system comparison rig. It measures Legion
(raft lanes and streaming persistence), Nomad, and Kubernetes on identical
AWS hardware with simulated worker fleets (Legion's
[`--simulate`](worker-simulation.md), nomad-nodesim, and KWOK). The
directory's `README.md` is the runbook. To validate locally without AWS,
run:

```bash
# Driver counting/latency math against known-good fixtures:
python3 -m unittest discover benchmarks/aws/driver/tests

# Full local loop: manager + simulated fleet + translated workload +
# the real driver (also builds plugins):
benchmarks/aws/scripts/local-smoke.sh
```

CI runs the fixture tests, `terraform validate`, and shell syntax checks
whenever `benchmarks/aws/**` or `benchmarks/loadgen/**` changes. Nothing in CI touches AWS. The
end-to-end sim-fleet wire test is `tests/worker_simulation.rs`, which runs
in the normal `cargo test` suite.

The rig submits Legion jobs through `legion-loadgen`
(`benchmarks/loadgen/`), a benchmark-only binary that holds a small pool
of gRPC connections open and issues many concurrent `RunJob` calls. It
is feature-gated so it is not part of an ordinary build:

```bash
cargo build --release --features loadgen --bin legion-loadgen
```

Submitting with one `legion job run` process per job tops out at roughly
40-100 jobs/sec. At that rate, campaign cells measured the driver instead of
the scheduler. The driver flags that case as `submission.driver_bound: true`.

`legion-loadgen` is Legion-only, which can make comparisons unfair. Nomad and
Kubernetes still submit one CLI process per job. For a cross-system
comparison, put all three on the shared loop
(`driver.py run --submitter cli`) or add native submitters for the others
first. Every cell records which path it took as `submitter` in its metrics.
See `benchmarks/aws/README.md` § Methodology.

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
