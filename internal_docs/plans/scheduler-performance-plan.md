# Scheduler Performance Plan (benchmarks, instrumentation, optimization)

**Status: IN EXECUTION** — phases 1-3 shipped 2026-07-29 (scenario schema
v2 + realistic scenarios, backend matrix runner, `PlanTimings` +
`legion_scheduler_plan_phase_seconds`); phase 4's first optimization
(fused `submit_and_apply_plan`, one quorum commit per plan instead of
two) shipped the same day with before/after in
`benchmarks/scheduler/README.md`. Phase 4 remains open by design — the
next measured targets (O(fleet) version sample, per-plan snapshot
rebuild) are architecture-review items D4/D6. The 2026-09-02 pass added
the cluster-fill axis to the micro benchmarks (`plan_under_fill`) and
removed every per-plan term that scaled with it (a per-job allocation
index in the read model, delta publishing for local plan commits), along
with the per-placement permit re-check and id-string hashing in the
scoring cache and the uncached statements on the apply and eval paths —
calls #1-#5, numbers in `benchmarks/scheduler/README.md`.

> **Judgement calls:** [`../calls/scheduler-performance-plan_calls.md`](../calls/scheduler-performance-plan_calls.md)

## Problem

Legion's stated goal is to be **the fastest scheduler at scale, provably**.
Today we cannot prove it, and we only partially know it:

- The benchmark scenarios are toy-shaped: uniform nodes, no resources, no
  constraints, 12-32 node fleets, noop tasks with identical shapes. Real
  clusters are heterogeneous (node classes, regions, GPU subsets) and run
  mixed workloads (long-lived services + bursty batch + constraint-heavy
  jobs).
- Every benchmark runs against the default `local_disk` store. Nobody has
  numbers for the deployment shapes we actually recommend: raft
  (single-node and 3-node quorum) and streaming persistence (rustyriver).
  The write path is completely different in each; "scheduler speed" that
  ignores commit cost is fiction at cluster scale.
- The scheduler's internal cost structure is opaque. `plan_and_apply` logs
  one ad-hoc `build_plan_ms`; nothing separates snapshot cost from
  candidate construction from scoring from plan apply, so optimization is
  guesswork (the plan-snapshot caching work found its target by manual
  profiling, not by looking at a dashboard).

## Prior art

- **Nomad**: [C1M](https://www.hashicorp.com/en/c1m)/C2M drove its perf
  story; internally the scheduler samples candidates
  (`LimitIterator`, min(2, log2 n) feasible nodes for service jobs) so
  per-placement cost is ~O(log n). nomad-bench provides load-test infra.
  Nomad instruments scheduler phases with `metrics.MeasureSince` keys
  (`nomad.worker.dequeue_eval`, `.invoke_scheduler`, `.submit_plan`, plan
  apply time) — a per-phase breakdown of exactly the loop we have.
- **Kubernetes**: `scheduler_perf` integration benchmarks with declarative
  workload YAML (node profiles + pod templates + measured ops) — the same
  scenario-file shape this plan extends ours toward. kube-scheduler
  exposes `scheduler_framework_extension_point_duration_seconds`
  histograms per phase; `percentageOfNodesToScore` caps filtering cost.
- **PR #84** (open): storage-side instrumentation sketch — `lock_wait_ms`
  on connection acquisition, `storage_tx` spans with `tx_duration_ms` on
  `apply_plan_submission` / `claim_eval` / `complete_eval`, batched task
  status updates. This plan absorbs its instrumentation ideas (fresh
  implementation) and defers its batching ideas to the measured
  optimization phase.

## Jobs to be done

- **Legion maintainer**: "When I make the scheduler faster or slower, I
  want one command that tells me by how much, phase by phase, on workloads
  that look like production, on every persistence backend we ship."
- **Operator / evaluator**: "I want published numbers for the deployment I
  would actually run (3-manager raft, streaming persistence), not a
  best-case in-memory figure."
- **CI (autonomous agent)**: "I want an SLO gate tight enough to catch a
  real regression, not just a 10x collapse."
- **AWS comparative-benchmark author** (the follow-on plan): "I want the
  phase timings and scenario runner reusable in a real cluster so the AWS
  test measures the same things with the same names."

## Goals

1. Benchmark scenarios that resemble real workloads: heterogeneous node
   profiles, mixed job types, resources, constraints, indexes on/off.
2. A backend axis: `local_disk`, `raft` (1-node), `raft` (3-node,
   in-process cluster with real transport), `streaming` (rustyriver,
   file replica locally). Recorded numbers for all.
3. Per-phase scheduler instrumentation (`PlanTimings`) surfaced in logs,
   spans, the metrics endpoint, and benchmark output.
4. Storage-side observability: lock wait + transaction durations on the
   hot paths (PR #84's ideas, implemented properly).
5. Measured optimizations with before/after numbers recorded in
   `benchmarks/scheduler/README.md`.

## Non-goals

- The AWS comparative test itself (separate plan:
  [`aws-comparative-benchmark-plan.md`](aws-comparative-benchmark-plan.md)).
- Architectural changes (candidate sampling, sharded schedulers, etc.) —
  those are proposals in
  [`scheduler-architecture-review-plan.md`](scheduler-architecture-review-plan.md);
  this plan only measures and burns down costs within the current
  architecture.
- A public "benchmarks" marketing page. Numbers land in
  `benchmarks/scheduler/README.md` (internal record) and
  `docs/testing-and-benchmarks.md` (methodology); external claims wait for
  the AWS test.
- Multi-manager *scheduling* (concurrent schedulers on different
  managers). Raft backends here measure replication cost on today's
  single-active-scheduler path.

## What exists today

- `benchmarks/micro/scheduler_bench.rs`: `scheduler_speed` (uniform scale matrix)
  and `candidate_construction` (indexes on/off) Criterion groups.
- `benchmarks/micro/eval_critical_path_bench.rs`: enqueue→claim→apply→complete
  under 1/4/8 threads, local_disk only.
- `tests/scheduler_benchmark_slo.rs` + `benchmarks/scheduler/scenarios/*.json`
  + `slo.json`: 3 small uniform scenarios, generous global thresholds, CI
  gate on scheduler-file changes.
- `tests/raft_perf_smoke.rs`: single-node raft `put_kv` latency smoke +
  lanes-vs-single A/B; `tests/raft_lanes.rs`: reusable 3-node in-process
  cluster harness (real tonic transport).
- `src/storage/persistence.rs`: `PersistenceType::{LocalDisk, Raft,
  Streaming}`; streaming = rustyriver with `file://` or `s3://` replica.
- Scheduler timing today: one `build_plan_ms` debug log; `#[instrument]`
  spans without phase fields.

## Proposed design

### 1. Scenario schema v2 (additive)

`benchmarks/scheduler/scenarios/*.json` gains optional fields; legacy
fields keep working (a v1 file is a v2 file with one uniform profile and
one job type):

```json
{
  "name": "mixed-heterogeneous",
  "scheduling_policy": "bin_pack",
  "iterations": 6,
  "node_profiles": [
    { "name": "web",   "count": 300, "memory_gb": 32, "cpus": 8,
      "attrs": { "class": "web", "region": "us-east" } },
    { "name": "batch", "count": 150, "memory_gb": 64, "cpus": 16,
      "attrs": { "class": "batch", "region": "us-east" } },
    { "name": "gpu",   "count": 50,  "memory_gb": 128, "cpus": 32,
      "attrs": { "class": "gpu", "gpu": "a100", "region": "us-west" } }
  ],
  "job_types": [
    { "name": "service", "count": 30, "tasks_per_job": 3,
      "task_count_per_task": 4,
      "resources": { "cpu": 500, "memory": 1073741824 },
      "constraints": { "class": "web" } },
    { "name": "batch",   "count": 10, "tasks_per_job": 1,
      "task_count_per_task": 20,
      "resources": { "cpu": 1000, "memory": 2147483648 },
      "constraints": { "class": "batch" } },
    { "name": "gpu-train", "count": 4, "tasks_per_job": 1,
      "task_count_per_task": 2,
      "constraints": { "gpu": "a100" } }
  ],
  "scheduler_settings": { "indexes": { "topo": ["pool", "region"] } },
  "churn_every": 3, "churn_nodes": 10,
  "slo": { "min_evals_per_sec": 10.0, "max_p99_latency_ms": 500.0 }
}
```

- `node_profiles` replaces `node_count` (either allowed, not both).
  Profiles map to fingerprints (memory/cpus) and `config_attrs`.
- `job_types` replaces `job_count`/`tasks_per_job`/`task_count_per_task`.
  `resources` and `constraints` flow into `TaskConfig`. All tasks stay on
  the noop runtime — we are benchmarking the scheduler, not runtimes.
- `scheduler_settings` passes through to `ManagerConfigFull` (this is how
  a scenario arms indexes).
- `slo` overrides the global `slo.json` per scenario; absent fields fall
  back to global. Churn replacement nodes reuse the profile of the node
  they replace.

### 2. Shared runner + backend axis

Scenario parsing/build/run/validate code moves to
`tests/benchmark_support/mod.rs`, included via `#[path]` from both test
binaries (same pattern as the existing `tests/store/`, `tests/eval/`
dirs):

- `tests/scheduler_benchmark_slo.rs` — unchanged role: local_disk CI gate,
  now over v2 scenarios with per-scenario SLOs.
- `tests/scheduler_scale_benchmark.rs` (new, `#[ignore]`) — the full
  matrix: every scenario × every requested backend.
  `LEGION_BENCH_BACKENDS=local_disk,raft1,raft3,streaming` (default
  `local_disk`) and `LEGION_BENCH_SCENARIOS=<name,...>` (default all)
  select the matrix. Emits a per-cell metrics table (throughput, p50/p95/
  p99, conflict rate, correctness, per-phase timing means) and writes
  JSON results to `target/benchmark-results/<timestamp>.json` so runs
  diff mechanically.

Backends:

- `local_disk` — today's store.
- `raft1` — `PersistenceType::Raft`, bootstrap single node
  (`tests/raft_perf_smoke.rs` config shape). Isolates
  propose→commit→apply cost without network.
- `raft3` — 3-node in-process cluster, real tonic `RaftTransport`
  (`tests/raft_lanes.rs` harness, extracted into the shared support
  module). Scheduling drives the leader's store. Measures quorum-commit
  cost on the scheduler write path.
- `streaming` — `PersistenceType::Streaming`, `file://` replica, 1s sync
  interval. Measures the rustyriver-managed-Db write-path overhead (WAL
  capture is async; expected near-noise — that expectation is exactly
  what the measurement checks).

### 3. `PlanTimings` (scheduler phase instrumentation)

`PlanOutcome` (and a by-ref out-param for `plan_only`) gains:

```rust
pub struct PlanTimings {
    pub available_nodes_us: u64,     // node snapshot fetch
    pub version_sample_us: u64,      // epoch fence sample
    pub task_fetch_us: u64,          // task config load
    pub capacity_snapshot_us: u64,   // allocations + free pools
    pub build_plan_us: u64,          // candidates + scoring + placement
    pub insert_submission_us: u64,   // plan_submission row write
    pub apply_us: u64,               // apply_plan_submission (epoch CAS + writes)
    pub total_us: u64,
}
```

- One `debug!` line per plan with all fields; fields also recorded on the
  existing `#[instrument]` span.
- Prometheus: `legion_scheduler_plan_phase_duration_seconds{phase=...}`
  histogram via the existing metrics endpoint, so the AWS test scrapes
  the same breakdown.
- The benchmark runner aggregates timings per cell (mean + p95 per phase)
  — this is the "where does the time go" table optimization reads from.

### 4. Storage hot-path observability (PR #84, done right)

- `write_connection`/`read_connection`: measure mutex wait; record
  `lock_wait_us` on the current span and `debug!` when it exceeds 1ms.
- `storage_tx` spans with `tx_duration_ms` on `apply_plan_submission`,
  `claim_eval`, `complete_eval`.
- No behavior changes in this phase — batching/index ideas from PR #84
  become Phase 4 candidates only if the numbers indict them.

### 5. Measured optimization

Run the matrix, read the phase table, burn down the top costs. Candidate
list from code reading (to be confirmed or killed by data): per-plan
fingerprint JSON re-parse in snapshot building; apply-path per-allocation
statement overhead (batch into one tx / IN-lists); eval claim queue scan;
redundant `available_nodes` clones on the multi-plan eval path. Every
change ships with before/after matrix cells in
`benchmarks/scheduler/README.md`, per the AGENTS.md rule.

## Phases

1. **Scenario schema v2 + shared runner + realistic scenarios** (ships
   alone: SLO gate on richer workloads, local_disk).
2. **Backend axis** (`scheduler_scale_benchmark.rs`, raft1/raft3/
   streaming; recorded matrix in `benchmarks/scheduler/README.md`).
3. **Instrumentation** (`PlanTimings`, storage spans, metrics histogram,
   runner phase table).
4. **Optimization** (data-driven, before/after recorded). Repeatable; the
   first pass targets the top 2-3 phase costs.

Each phase leaves the tree green and ships value on its own.

## Testing plan

- Existing suites stay green (`cargo test --all --locked`), including
  scheduler integration, eval loop, raft suites, and the scheduling-index
  equivalence property.
- The SLO gate keeps gating in CI; scenario v2 parsing gets unit coverage
  in the support module (v1 files parse as v2, profile/job-type
  expansion, SLO override fallback).
- `scheduler_scale_benchmark` self-validates like the SLO gate
  (correctness ratio = 1.0 required for a cell to count).
- Instrumentation: unit test that `PlanTimings` phases sum ≈ total and
  that all phases are nonzero on a real plan; metrics endpoint test
  extends `tests/metrics_endpoint.rs`.

## Rollout

Benchmarks and tests are dev-facing; instrumentation is additive (new
debug logs, span fields, one new metric family). No config surface, no
wire change, no migration. The one visible surface — the new metric — is
documented in `docs/metrics.md` and `docs/observability.md`.

Public docs updated: `docs/testing-and-benchmarks.md` (methodology,
backend matrix, how to run), `docs/metrics.md` (new histogram). Numbers
recorded in `benchmarks/scheduler/README.md` with host + build profile
noted (the existing record omits hosts, which already bit us when
comparing container numbers to laptop numbers).
