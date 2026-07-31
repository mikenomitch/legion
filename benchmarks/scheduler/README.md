# Scheduler Benchmark Scenarios

This directory contains repeatable, file-backed scheduler load scenarios and SLO targets.

- `scenarios/*.json`: load shapes (jobs, tasks/job, task counts, node counts, churn pattern).
- `slo.json`: global SLO thresholds validated by `tests/scheduler_benchmark_slo.rs`.

Run locally:

```bash
cargo test --test scheduler_benchmark_slo -- --ignored --nocapture
```

The benchmark test emits per-scenario metrics:

- throughput (`evals_per_sec`)
- scheduling latency (`p50`, `p95`, `p99` in ms)
- retry/conflict rejection rate
- correctness checks (expected vs actual allocations)

## Candidate construction — scheduling-index baseline (2026-07-28)

`cargo bench --bench scheduler_bench -- candidate_construction` (Criterion
medians, dev container, `bin_pack`, 16-region fleet; "on" arms a
`["pool", "region"]` partition index, "off" pins the scan with
`index_min_nodes: 0`):

| Shape | nodes | indexes off | indexes on |
| --- | --- | --- | --- |
| 1 task × 50 allocs | 1,000 | 10.8 ms | 11.6 ms (noise) |
| 1 task × 50 allocs | 5,000 | 77.8 ms | 76.3 ms (noise) |
| 12 tasks × 2 allocs | 5,000 | 93.6 ms | 83.3 ms (−11%) |

Reading at the time: the index removes the per-task constraint scan
(visible as the multi-task delta — scan cost scales with tasks × nodes,
index cost with tasks × shard), and whole-plan time was otherwise
dominated by the per-plan snapshot — `available_nodes()` cloning every
node and `ResourceLedger::new` parsing every node's capacity — which
indexing deliberately does not touch. No measured shape regressed outside
noise.

## Plan-snapshot caching — same cells, re-measured (2026-07-28)

After `internal_docs/plans/scheduler-plan-snapshot-plan.md` (Arc'd node
snapshots, cached per-node free pools, Arc'd per-node allocation vectors),
the same benchmark cells:

| Bench | Shape | Before | After |
| --- | --- | --- | --- |
| `candidate_construction` (plan-only) | 12 tasks × 2 allocs, 5k nodes, indexes off | 93.6 ms | 42.4 ms (−55%) |
| `candidate_construction` (plan-only) | 12 tasks × 2 allocs, 5k nodes, indexes on | 83.3 ms | 33.6 ms (−60%) |
| `scheduler_speed` (plan-and-apply) | 10 jobs × 10 allocs, 1k nodes, defaults | 48.2 ms | 30.1 ms (−38%) |
| `scheduler_speed` (plan-and-apply) | 10 jobs × 10 allocs, 10k nodes, defaults | 687 ms | 379 ms (−45%) |

With the snapshot floor gone, the scheduling index's candidate win is no
longer masked: indexes-on now beats the scan by ~21% on the 5k multi-task
cell (33.6 vs 42.4 ms) instead of ~11%. The 12-task 1k cell measures
6.7 ms (off) / 7.1 ms (on) — small fleets remain index-neutral, which is
why `index_min_nodes` exists. No remaining O(cluster) deep clone sits on
the plan path; further wins want profiling, not this list.

## Scenario × backend matrix — baseline (2026-07-29)

**Host: Apple Silicon MacBook (macOS), release build, in-process
backends.** Run: `tests/scheduler_scale_benchmark.rs` (see
`docs/testing-and-benchmarks.md`). Single-shot runs on a laptop carry
real thermal variance between runs — compare *phases within a run* and
before/after pairs measured close together; treat absolute evals/sec as
indicative. The AWS campaign
(`internal_docs/plans/aws-comparative-benchmark-plan.md`) is where
steady-state absolutes become authoritative.

Baseline, before the fused plan apply — selected cells (full JSON in
`target/benchmark-results/scale-1785387758.json` at the time; phases are
mean µs per plan):

| Scenario | Backend | evals/s | p50 ms | p99 ms | build µs | insert µs | apply µs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| steady-small | local_disk | 1058 | 0.92 | 1.39 | 185 | 48 | 646 |
| steady-small | raft1 | 355 | 2.75 | 3.63 | 202 | 496 | 2046 |
| steady-small | raft3 | 179 | 4.96 | 13.71 | 241 | 847 | 4402 |
| steady-small | streaming | 1005 | 0.93 | 1.52 | 191 | 51 | 687 |
| web-fleet | raft1 | 360 | 2.79 | 4.88 | 329 | 337 | 1822 |
| web-fleet | raft3 | 133 | 6.16 | 37.91 | 527 | 1119 | 5272 |
| mixed-heterogeneous | raft3 | 67 | 6.87 | 48.55 | 583 | 2122 | 7814 |
| churn-medium | raft1 | 215 | 3.23 | 16.59 | 298 | 807 | 2901 |
| churn-medium | raft3 | 119 | 6.59 | 9.85 | 367 | 1299 | 5068 |
| batch-burst | raft3 | 92 | 10.19 | 18.66 | 1374 | 1171 | 7928 |

Readings:

- **The write path dominates every backend.** Even on local_disk, apply
  is the largest phase; on raft3, insert+apply is 70-85% of plan time.
- **`insert_submission` on raft is a full quorum round-trip** (0.3-2.1 ms)
  for a row consumed microseconds later by the apply — two proposes per
  plan. This is what the fused `submit_and_apply_plan` removes (below).
- **Streaming ≈ local_disk** across every scenario — rustyriver's async
  WAL capture stays at the noise floor on the plan path, as
  `docs/persistence-streaming.md` predicts.
- **`version_sample` is O(fleet)**: ~1 µs at 12 nodes, ~200-350 µs at
  1,000 (churn-large). Architecture-review item D4.
- Correctness (full placement + constraint conformance) was 1.000 in all
  28 cells, conflict rate 0.

## Fused plan apply (`submit_and_apply_plan`) — 2026-07-29

The change: the scheduler's plan-submission insert and apply now run as
ONE replicated op instead of two (`Store::submit_and_apply_plan`). On
raft backends this structurally removes one propose→quorum-commit→apply
round-trip per plan — the baseline's `insert` column (0.3-2.1 ms/plan of
separate quorum cost) no longer exists as a phase.

**Measurement honesty first.** Same-day re-runs of the scenario matrix
varied up to ~2x on this host (sustained-load performance degradation;
`pmset` recorded no thermal event, but unrelated phases drifted 30-40%
between runs). Single-shot scenario runs therefore could not quantify the
raft win today. What is defensible:

- **Controlled same-state Criterion A/B** (stash/unstash, back-to-back,
  local_disk, `scheduler_speed/bin_pack`, 100 samples):

  | Cell | unfused | fused | delta |
  | --- | --- | --- | --- |
  | 1k nodes, 100 allocs | 22.94 ms | 22.51 ms | −1.9% (p = 0.04) |
  | 10k nodes, 100 allocs | 210.8 ms | 198.0 ms | −6% (p = 0.49, noisy) |

  Local-disk verdict: neutral-to-slightly-positive — the fused op never
  costs anything, and one write-transaction boundary replaces two.
- **Raft verdict**: structural. Two blocking quorum round-trips became
  one; the removed round-trip measured 0.3-2.1 ms/plan in the baseline
  table. Same-state scenario cells (post-compile runs of the matrix) put
  the fused raft1 apply at roughly the baseline's apply alone
  (e.g. steady-small raft1 2.59 ms fused vs 2.05+0.50 ms split;
  churn-medium 2.95 ms vs 2.90+0.81 ms) — consistent with the win, but
  within this host's run-to-run noise. The AWS rig
  (`internal_docs/plans/aws-comparative-benchmark-plan.md`) is where the
  raft delta gets an authoritative number.
- Full after-table: `target/benchmark-results/scale-1785389951.json`
  (28 cells, correctness 1.000 throughout, conflicts 0).

Also of note in the after-run: `apply` remains the dominant phase on
every backend (group commit is architecture-review item D5), and
laptop-hosted absolute numbers should be compared only within a run —
record the host, build profile, and machine state with every table.

## Plan-path scale levers (D4 lazy sample, D6 cheap snapshots, scoring fusion) — 2026-07-30

**Host: 4-vCPU Linux container (Claude Code remote env), release build,
in-process backends.** Container quorum-commit (fsync) latency swings
run to run — compare phases within a run and before/after pairs, not
absolutes; the same caveat the 2026-07-29 tables carry.

Before (`target/benchmark-results/scale-1785396333.json`) vs after
(`scale-1785406846.json`), sequential driver, correctness 1.000 and
conflicts 0 in every cell of both runs. Selected cells (phase means, µs
per plan):

| Scenario (nodes) | Backend | nodes µs | versions µs | capacity µs | build µs |
| --- | --- | --- | --- | --- | --- |
| churn-large (1k) | local | 186 → 39 | **229 → 5** | **504 → 115** | 1427 → 1397 |
| churn-large (1k) | raft1 | 235 → 54 | **254 → 4** | 793 → 177 | 1792 → 1741 |
| churn-large (1k) | raft3 | 352 → 55 | **309 → 5** | **1262 → 223** | 2704 → 1986 |
| mixed-heterogeneous (500) | local | 91 → 15 | 112 → 2 | 355 → 100 | 895 → 787 |
| web-fleet (240) | raft3 | 99 → 20 | 107 → 5 | 315 → 133 | 817 → 789 |
| steady-small (12) | local | 1 → 0 | 2 → 1 | 40 → 35 | 303 → 286 |

Readings:

- **The O(fleet) `version_sample` term is gone** (D4 lazy sample):
  ~50x at 1k nodes, and — the point — no longer grows with fleet size.
  The eager fallback never fired in these runs
  (`legion_scheduler_lazy_sample_fallback_total` = 0; the sequential
  driver never mutates mid-plan).
- **Snapshot phases dropped 4.5–5.8x** at 1k nodes (D6: cached sorted
  node list + Arc'd pools). churn-large is the *worst* case for the
  node-list cache (25 nodes churn every iteration, invalidating it);
  quiet fleets see `nodes` at 0–1 µs.
- `build_plan` improved modestly (scoring-loop fusion; biggest on the
  1k-node cells). `apply` means moved with container fsync noise in
  both directions — the D5 group-commit win is measured separately
  below, under concurrency, where it actually engages.

## Plan-apply group commit (D5) under concurrency — 2026-07-30

Same host/build. `LEGION_BENCH_CONCURRENCY=4` drives each cell with 4
concurrent submitters (bounded conflict retries, like the production
eval path); `LEGION_BENCH_SCHEDULER_SETTINGS` toggles the lever for the
A/B. Back-to-back runs:

4 concurrent submitters, raft backends, `steady-small` (12 nodes / 4
jobs, the contended cell) and `web-fleet` (240 nodes / 30 jobs):

| Scenario | Backend | evals/s off → on | p50 ms off → on | p99 ms off → on | conflicts/attempt (both) |
| --- | --- | --- | --- | --- | --- |
| steady-small | raft1 | 33.2 → **51.4** | 45.9 → **15.1** | 80.4 → 32.1 | 1.29 |
| steady-small | raft3 | 15.6 → 19.5 | 99.1 → **41.2** | 158.5 → 93.0 | 1.25 |
| web-fleet | raft1 | 63.9 → 63.3 | 45.9 → 30.1 | 72.3 → 79.4 | 0.32 / 0.47 |
| web-fleet | raft3 | 30.0 → 34.9 | 92.1 → **64.9** | 155.5 → 124.5 | 0.36 |

Readings — with measurement honesty first:

- **p50 plan latency fell 2.4–3x** on the contended cells: concurrent
  applies share one quorum commit instead of queueing solo fsyncs. That
  is the group commit doing exactly what it was built for.
- **Throughput gained 16–55%, not the plan's ≥2x target.** The ceiling
  is not commit rate — it is optimistic-concurrency contention:
  conflict rates are high (1.25+ retries per plan on steady-small,
  where 4 bin_pack planners all pick the same argmax node) and
  *identical with the lever on and off*, so batching neither causes nor
  can fix them. Re-planning burns the latency the batch saved. This is
  the parallel-planner pathology the late-binding plan
  (`internal_docs/plans/late-binding-plan-apply-plan.md`) exists for;
  group commit and late binding compose.
- The purest measure of the coalescer mechanism is the heartbeat table
  above (same primitive, no CAS in the loop): 5.3–6.7x at 8 threads.
- Correctness 1.000 in every cell, both arms (bounded conflict retries,
  as production's eval path does).


## Heartbeat path (heartbeat-scale plan) — 2026-07-30

Same host/build. `tests/heartbeat_scale_benchmark.rs`, 8 driver
threads, 3s measurement window per cell. Before
(`target/benchmark-results/heartbeat-1785401390.json`, commit before
the heartbeat pass) vs after (`heartbeat-1785406179.json`):

| Backend | Nodes | hb/s | p50 ms | p99 ms | liveness tick ms |
| --- | --- | --- | --- | --- | --- |
| local_disk | 1000 | 9,428 → **22,182** | 0.53 → 0.19 | 4.17 → 1.52 | 0.80 → 0.76 |
| local_disk | 5000 | 9,586 → **21,409** | 0.52 → 0.20 | 4.78 → 1.47 | **7.71 → 3.60** |
| raft1 | 1000 | 158 → **839** | 50.3 → 10.0 | 90.4 → 34.4 | 0.76 → 0.66 |
| raft1 | 5000 | 143 → **953** | 51.5 → 9.3 | 123.1 → 15.0 | 4.11 → 3.92 |

Readings:

- **Raft heartbeat throughput is up 5.3–6.7x** — the group commit turns
  one consensus round per heartbeat into one per batch. With 8 driver
  threads the natural batch is ≤8; real fleets present hundreds of
  concurrent RPCs and batch up to 128, so this measurement is the
  *floor* of the win, not the ceiling. p50 fell from ~50 ms (a queued
  solo fsync each) to ~10 ms (a shared commit).
- Local-disk throughput roughly doubled (skip-reprojection: no
  read-model write lock or node re-read on a bare refresh) and p99
  dropped ~3x (no more lock convoys against scheduler reads).
- The 5k-node liveness tick halved (integer heartbeat clock instead of
  10k RFC3339 parses every 2s). The old parse path remains only as the
  NULL-column fallback for pre-migration rows.
- Baseline context: 10k workers at the default 2s cadence need 5,000
  heartbeats/s. On this host, raft went from 33x short (158/s) to ~5x
  short at driver-limited batch sizes — with real arrival concurrency
  (batch 128) the same commit rate (~100 commits/s observed) covers
  ~12,800 hb/s. `heartbeatSeconds` remains the fleet-wide knob on top.

## Plan-path cleanups: shared filter parsing, cached settings, optional shard sort — 2026-07-30

Three measured leftovers from the TODO's "slowest bits" list, in one pass
(dev container, Criterion, back-to-back before/after runs on the same
host state):

- **Per-task `ConstraintSet`/`PermitSet` parsing deduplicated** —
  `build_plan`'s group-filter pass and `build_task_plan` each parsed
  every task's constraints and permit; they now share one `Arc`'d parse
  per task per plan.
- **`scheduler_settings` walks hoisted to `Scheduler::new`** — weights,
  `nodes_considered`, and `score_sample` were re-parsed from the settings
  JSON on every `build_plan`; they are parsed once per scheduler now
  (and malformed settings fail at construction instead of the first
  plan).
- **Index shard-sort skipped when the order is discarded** — the
  index query's nearest-fit cell sort only matters when candidates may
  be truncated to a best-first budget; the untruncated path re-sorts
  verified candidates by node id, so it now passes `ordered: false` and
  skips the sort (and the ordered path ranks each cell once instead of
  recomputing ranks inside the comparator).

| Cell | before | after | delta |
| --- | --- | --- | --- |
| `scheduler_speed/bin_pack/nodes_1000_allocs_100` (plan-and-apply) | 25.81 ms | 25.19 ms | −2.4% (p = 0.02) |
| `candidate_construction/indexes_off/nodes_5000` (plan-only) | 32.11 ms | 32.88 ms | noise (CIs overlap) |
| `candidate_construction/indexes_on/nodes_5000` (plan-only) | 26.27 ms | 26.02 ms | noise |

Honest reading: the wins are real but small at these shapes — constraint
JSON in the bench cells is tiny (two keys), the topo index's shards hold
few cells, and the settings walk was microseconds each. The changes are
kept because they scale with what the benchmark shapes don't stress
(constraint complexity, task count, cells per shard) and they remove
per-plan work with no behavioral cost; the `-2.4%` on the whole
plan-and-apply path is the aggregate. No cell regressed outside noise.
