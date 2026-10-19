# Testing and Benchmarks

This page covers how Legion is tested and benchmarked, and how to run each suite. It is written for contributors.

## Test Layout

Legion uses a mixed strategy:

- Unit tests colocated in module files (`#[cfg(test)]` in `src/`).
- Integration tests under `tests/`, almost all of them modules of one test
  binary, `tests/integration/main.rs`.

### One integration binary

Cargo builds every top-level `tests/*.rs` file as its own test binary, and
each of those links all of Legion and its ~520 dependencies: about 200 MB of
unoptimized code and a full link per file, repeated whenever the library
changes. With some sixty files that was most of a `cargo test` run's build
time and most of the target directory. So the integration tests are modules
of `tests/integration/main.rs` and link once.

Run one module, or one test, with a filter:

```sh
cargo test --test integration acl_endpoints::
cargo test --test integration raft_lanes::killing_a_single_lane_leader_disturbs_only_that_lane -- --nocapture
cargo test --test integration scheduler_benchmark_slo:: -- --ignored --nocapture
```

The filter is a substring of the test's full path, so the trailing `::`
keeps `raft_lanes::` from also matching `raft_lanes_durability::`. libtest
treats several filters as "any of", so name a single test by its full path
rather than adding a second filter after `--`.

The modules share one process and libtest's thread pool, which decides what
may live there:

- **Process-wide state.** A test that asserts on exact deltas of the global
  Prometheus metrics, sets the scheduler's planner-concurrency gate, or
  installs a `#[global_allocator]` would see or disturb its neighbours. Those
  stay as separate `tests/*.rs` binaries: `tests/raft_lifecycle_ops.rs`,
  `tests/raft_job_eval_ops.rs`, `tests/adaptive_scheduler_concurrency.rs`,
  and `tests/session_index_scale_measure.rs`. Assert monotonic growth, as
  `metrics_endpoint` does, when a shared-binary test reads a global.
- **Raft clusters.** Tests that bring up a multi-node in-process cluster with
  tight election timeouts take `support::cluster_gate()`, so only one runs
  at a time across the whole binary.
- **Measurements.** Benchmark and measurement tests are `#[ignore]`d and
  live in the binary too. Run them with a module filter, as above; a bare
  `--ignored` runs every one of them at once.

### Build footprint

`[profile.dev]` in `Cargo.toml` (which the test profile inherits) keeps line
tables for Legion's own crates and drops debuginfo for dependencies. Panics
and backtraces keep `file:line`; stepping through code with variables in a
debugger needs `CARGO_PROFILE_DEV_DEBUG=2` for that build. The bundled
SQLite is compiled with optimizations even in dev builds, because every
store-backed test runs the full migration chain through it. The library's
doc-test pass and the `legion` binary's empty unit-test target are switched
off, since both cost a full rustdoc pass or link and ran nothing.

### Temporary directories

A test helper that returns a `Store` has to keep the store's directory alive
past its `TempDir` guard, so it calls `keep()`. Such directories go under a
per-process scratch root, `crate::test_scratch::root()` in `src/` and
`crate::support::scratch_root()` in `tests/integration/`:

```rust
let dir = tempfile::Builder::new()
    .prefix("legion-store-")
    .tempdir_in(crate::test_scratch::root())?
    .keep();
```

The root is removed when the test process exits. A directory kept directly
in the system temp dir is never removed; at ~1.7 MB per store, a full run
used to leave over a gigabyte behind. A run killed before it exits (Ctrl-C)
still leaves its one `legion-tests-*` root in `$TMPDIR`.

### Notable integration tests

- `config_jobs`
  - evaluates every shipped `configs/jobs/*.ncl` and validates it the same
    way the manager does on submit. Deserializing to `JobConfig` alone is not
    enough to pass.
  - validates maximal config shape.
  - evaluates the shipped manager configs (`configs/*.ncl`) and worker configs
    (`configs/workers/*.ncl`) into their config structs.
  - parses the shipped job template, resolves its defaults, renders it through
    the manager's sandboxed path, and validates the result.
- `scheduler_integration`
  - end-to-end scheduling scenarios (constraints, permit, count.per, reschedule behavior).
- `eval_loop`
  - eval ordering and eval-driven ledger reconciliation behavior.
- `rollout_integration`
  - rollout progression behavior (auto/manual/create-first/destroy-first, etc).

## Running Tests

Primary command:

- `cargo test --all --locked`

While iterating, build and run only what the change touches:

- `cargo test --lib <filter>` for unit tests in `src/`.
- `cargo test --test integration <module>::` for one integration module.

Run the tests for any Rust change. Docs-only and frontend-only changes are the usual exception.

The suite opens many SQLite files at once: every test store holds a write
connection and a read pool, and the test harness runs one test per core. On
a many-core machine the common default of 1,024 open files per process is
not enough, and about ten `storage::` tests fail with `unable to open
database file`. Raise the limit for the shell that runs the tests:

```bash
ulimit -n 65536
cargo test --all --locked
```

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
- `benchmarks/micro/net_bench.rs` (Criterion)
- `tests/integration/scheduler_benchmark_slo.rs` (scenario runner + SLO gate, CI)
- `tests/integration/scheduler_scale_benchmark.rs` (scenario × persistence-backend
  matrix, manual)
- `tests/integration/plan_contention.rs` (concurrent-submit contention harness)
- Scenario definitions in `benchmarks/scheduler/scenarios/*.json`
- SLO thresholds in `benchmarks/scheduler/slo.json` (global), overridable
  per scenario via the scenario's `slo` block
- Shared runner code in `tests/integration/benchmark_support/mod.rs`

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
  - `plan_fleet_scaling` group: measures how planning cost changes with
    node count, with fill held fixed. It uses a `web_fleet`-shaped fleet
    (5 of 6 nodes `class = web`, one pool) at 1k, 2k, 5k, 10k, 20k, and 50k
    nodes with 0 or 5 allocations per node. It plans the `web_fleet` api job
    (two groups of 4, `class = "web"`). The `plan_and_apply` cells print
    per-phase means.

    At fill 5, an `eval_cycle` cell runs 150 whole evals the way an eval
    worker does: claim, the `job_submit` handler (`resolve_claimed_eval`),
    and the settle round. It prints the mean of each step and the
    `legion_eval_phase_seconds` and plan-phase breakdowns, so the time an
    eval spends outside `plan_and_apply` is measured rather than inferred.

    The 20k and 50k cells take minutes to set up. Pick node counts with
    `LEGION_BENCH_SCALING_NODES=20000,50000`, and skip the concurrent cells
    with `LEGION_BENCH_SKIP_CONCURRENT=1`.

    At 5k and 10k nodes, a concurrent cell runs four planners for four
    seconds against a background writer that keeps publishing to the read
    model. It prints plans per second, per-phase means, and the lazy-sample
    fallback count. This is the only local benchmark that reproduces
    contention between planners. Watch for per-plan cost that grows from 5k
    to 10k nodes at fixed fill. That is a regression.
- `eval_critical_path_bench`: end-to-end enqueue -> claim -> apply -> complete
  latency under 1/4/8 concurrent eval workers (write-lock contention).
- `net_bench`: what the node's connect datapath costs compared to dialing a
  backend directly. Each group runs the real `UpstreamForwarder` against
  loopback backends and has a `direct` row next to the forwarder row, so a
  run is self-comparing on any machine. Compare the gap, not absolute
  numbers. There is no NIC, no network round trip, and no TLS in any of it.
  - `l4_rr`: 100-byte request and reply on one kept-open connection.
  - `l4_conn_rr`: a new connection per request. The backend closes after
    replying, so client ports do not pile up in `TIME_WAIT`.
  - `l4_throughput`: one 64 MiB one-way transfer per iteration. On Linux the
    forwarder splices bulk transfers; on macOS it copies, so this group is
    only meaningful on the platform you care about. A `plain_proxy` row (a
    bare tokio accept-dial-copy proxy) sits between `direct` and
    `forwarder` as the floor for any userspace hop in the same harness.
    After the group, `l4_cpu` lines print CPU-seconds per GiB moved for
    each row. On a busy or virtualized host, wall-clock throughput through
    extra threads mostly measures the scheduler; CPU per byte holds up
    better, and the forwarder's own cost is its gap to `direct`.
  - `http_rr`: HTTP/1.1 requests on one kept-open client connection through
    HTTP mode, which shows whether upstream connections are reused.
  - `slow_endpoint`: three backends, one 2 ms slower, once per balancing
    strategy, in HTTP mode and over L4 with a connection per request. A
    strategy that avoids the slow backend lands near the fast backends'
    latency. One that spreads evenly pays a third of the delay on average.
  - `balancer_pick`: one pick across 16 endpoints, per strategy.

  ```console
  $ cargo bench --bench net_bench
  $ cargo bench --bench net_bench -- slow_endpoint
  ```

  Reference numbers from this target, and what they mean for a deployment,
  are in [What the datapath costs](network-handlers.md#what-the-datapath-costs).

  Datapath changes in `src/worker/forwarder.rs`, `src/worker/netd/`, or
  `src/worker/balance.rs` should report before/after numbers from this
  target. Run it on Linux for anything that touches the copy path.
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
- `cargo test --test integration scheduler_benchmark_slo:: -- --ignored --nocapture`
- `cargo test --test integration plan_contention:: -- --ignored --nocapture`
- Full backend matrix (use a release build for recorded numbers):

```bash
LEGION_BENCH_BACKENDS=local_disk,raft1,raft3,streaming cargo test --release --test integration scheduler_scale_benchmark:: -- --ignored --nocapture
```

- Subsets via `LEGION_BENCH_BACKENDS` and `LEGION_BENCH_SCENARIOS`
  (comma-separated scenario names).

- Heartbeat scale matrix (`tests/integration/heartbeat_scale_benchmark.rs`): fleet
  size × backend. It measures heartbeat throughput (N driver threads
  heartbeating a fleet partition as fast as they can), p50/p99 heartbeat
  latency, and the wall-clock time of one liveness tick at that fleet size:

```bash
LEGION_BENCH_BACKENDS=local_disk,raft1 LEGION_HEARTBEAT_NODES=1000,5000 \
  cargo test --release --test integration heartbeat_scale_benchmark:: -- --ignored --nocapture
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

Run scale benchmarks with the default heartbeat cadence, which is adaptive:
the manager serves an interval sized from fleet size (2s up to 2,500 nodes,
21s at 20,000 at the default `targetRatePerSecond`). A fixed 2s cadence at
20k nodes is 10,000 heartbeat RPCs a second into the leader, and that load
shows up as slower scheduling rather than as a heartbeat problem.

Report every throughput number with the cadence it was achieved at. A system
could slow its own heartbeats to post a better score, so every benchmark run
records `legion_heartbeat_served_interval_seconds`,
`legion_heartbeat_lost_window_seconds`, and
`legion_heartbeat_freshness_ratio` alongside throughput. The lost window is
what the slower cadence costs, and it belongs next to the number it bought.
Comparative runs compare defaults and say so: Nomad also adapts its
heartbeat TTL by default (about 50 heartbeats a second), and Kubernetes
renews node leases every 10 seconds. Either report each system's achieved
cadence and detection window, or pin all of them to the same interval
(`heartbeat.adaptive = false` on Legion) and state that you did. See
[heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md).

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

- `.github/workflows/ci.yml` runs the SLO benchmark gate only when scheduler-impacting files are changed (`src/manager/scheduler.rs`, `src/storage/store.rs`, `src/manager/daemon.rs`, benchmark scenarios, the shared runner in `tests/integration/benchmark_support/`, or the gate test itself).
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
end-to-end sim-fleet wire test is `tests/integration/worker_simulation.rs`, which runs
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

By default a cell submits its whole workload as one burst. That measures
throughput, and its p99 latency is mostly the time to drain the burst,
because the last job waits behind every job ahead of it. To measure how
long a job actually waits, pace the cell below capacity with an open-loop
arrival rate:

```bash
# 50 jobs/sec, sent on a fixed schedule whatever the manager's latency
benchmarks/aws/scripts/run-campaign.sh --system legion-a --scenarios web_fleet \
    --steps "5000 10000 20000" --storm-step 1 --submit-rate 50
```

`--submit-rate` maps to `legion-loadgen --rate` and is Legion-only. A paced
cell archives under `<run>-r<rate>` and records `submit_rate_target`.

### Keyed-instance lifecycles (`--mode instances`)

`legion-loadgen --mode instances` drives sandbox-style sessions instead of
jobs: each session is a keyed instance that is created, runs for
`--hold-ms`, is suspended to memory, resumed, and destroyed, at an
open-loop `--rate`. It embeds `--simulate N` simulated nodes with the
checkpoint channel faked (the `--sim-checkpoints` fleet), so it needs only a
manager and a join token for the fleet (`legion join-token create`; the
token's CA pin is how the fleet verifies the manager). The token's worker
type must grant the fleet, whose name is the `--sim-name-prefix` (default
`lg-<pid>`): give it `attributes = { workerFleet = "lg-*" }`
([worker-simulation.md](worker-simulation.md)). Here that type is `sim`:

```bash
legion-loadgen --mode instances --manager http://127.0.0.1:15747 \
    --join-token "$(legion join-token create --type sim -j | jq -r .data.token)" \
    --simulate 50 --sessions 2000 --rate 100 --hold-ms 2000
```

It prints one JSON summary: sessions offered, completed, and failed, the
achieved rate, and p50/p99 latency per stage (`create_rpc`, `placement`,
`dispatch`, `create_to_running`, `suspend`, `resume_dispatch`, `resume`,
`destroy_rpc`). `placement` and `dispatch` come from the manager's
`allocation.placed` events and the simulated nodes' delivery hooks, so they
split the time a running sandbox took into the control plane's share and
the node's. The sandbox-scaling plan's Phase 0 baseline and every later
phase's numbers in `benchmarks/scheduler/README.md` were measured with it.

On AWS, `benchmarks/aws/scripts/run-sandbox-campaign.sh` runs it against the
rig's 3-manager raft control plane with no worker fleet (four on-demand
boxes, about $1/hour), climbs a rate ladder to the ceiling, holds a density
cell, and `benchmarks/aws/analysis/sandbox_report.py` writes the report:
ceiling, latencies, raft entries and manager CPU per lifecycle, manager
memory per resident sandbox, and labelled projections. See
`benchmarks/aws/README.md`, "Sandbox-scaling campaign". Rig flags the
campaign uses: `--join-token` (the fleet's worker type and secret),
`--advertise-addr`, `--sim-name-prefix` (a fixed prefix so successive runs
re-register the same nodes), and `--sim-profiles` (the node shape).
Use isolated ports for a local manager: never the defaults another cluster
on the machine may be using.

### Session starts on real nodes (`--mode sessions`)

`legion-loadgen --mode sessions` starts unnamed [sessions](session-mode.md)
of an existing session job on real nodes, at an open-loop `--rate` or as fast
as `--concurrency` allows, through a manager's router (`--via manager`, the
default, spread over every comma-separated `--manager`) or straight at the
nodes with a session ticket (`--via edge --ticket "$T"`, nodes from
`ListSessionNodes`):

```bash
legion-loadgen --mode sessions --manager http://10.0.1.5:4747 \
    --job agents --task workspace --wait running \
    --count 1200 --rate 40 --concurrency 512 --hold-ms 2000
```

`--wait running` times each start to `running`, which for a sandbox is its
guest taking exec. The default, `accepted`, times it to the node's admit.
`--hold-ms` stops each session that long after it started, so a run at a
steady rate holds a steady number of sessions. `--stop-at-end` stops what is
left. The JSON summary has the counts by outcome, the achieved rate, p50 to
max latency, completions per second, and refusals grouped by reason.

`benchmarks/aws/sandbox-boot/` runs it against a small spot cluster of KVM
workers, and adds each guest's own timeline, stamped on the host when its
agent first takes exec ([results](../benchmarks/aws/sandbox-boot/RESULTS.md),
[write-up](sandboxes-at-scale.md#measured-on-aws)). Two in-process
measurements cover the control plane without nodes:
`tests/session_index_scale_measure.rs` (a manager's memory per session and
heartbeat cost at up to a million sessions) and the router's
`pick_cost_measure` (one pick per `stateful.placement` and fleet size):

```bash
cargo test --release --test session_index_scale_measure -- --ignored --nocapture
cargo test --release --lib -- pick_cost_measure --ignored --nocapture
```

### Control-plane write and delivery tests

These run in the ordinary `cargo test` suite and guard the numbers the
sandbox-scaling work bought:

- `tests/raft_lifecycle_ops.rs` counts the raft entries each stage of a
  keyed instance's lifecycle costs, on `single` and `lanes`, by summing every
  lane's applied index. A change that adds an entry to a stage fails it; a
  change that removes one updates the expected table.
- `tests/integration/work_stream.rs` covers `Manager.StreamWork`: delivery within a
  round trip with no heartbeat, replay after a dropped stream, resync on
  another epoch or a gap, a sub-KiB steady heartbeat, a leader kill on a
  3-manager raft cluster, and checkpoint work (a suspend's capture, the
  reclaim after a resume) pushed with no heartbeat and never replayed. `LEGION_MEASURE_STREAM_ALLOCS=10000 cargo test
  --release --test integration work_stream::a_streaming_heartbeat -- --nocapture` is the
  10,000-allocation heartbeat measurement.
- `tests/integration/blocked_evals.rs` covers blocked evals: wake by pool, by freed
  capacity, escaped, an unblock during the running eval, one blocked eval
  per job, and a scheduler-lane failover that replays a missed wake.
- `tests/integration/instance_scale_measure.rs` (ignored; run with `--ignored
  --nocapture`) measures bytes per instance and snapshot row, the store
  image at 10^5 (or `LEGION_MEASURE_INSTANCES`) instances, and full-set
  heartbeat bytes per dense node.
- `tests/integration/session_integration.rs` has two ignored session measurements,
  against 16 fake nodes on loopback mTLS with no raft entry per start.
  `session_start_latency_measure` times routed starts one at a time and 64
  in flight. `session_start_cost_breakdown_measure` times each step of a
  routed start on its own (the job lookup, the worker client, the start
  payload, the `PostWork` round trip, the router, and the whole
  `CreateInstance`) and prints the median and mean of each. Run them in a
  release build:
  `cargo test --release --test integration session_integration::session_start -- --ignored --nocapture`.

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
