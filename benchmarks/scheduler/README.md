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

## Plan contention — late-binding candidate lists (2026-07-29)

`cargo test --test plan_contention -- --ignored --nocapture`
(dev container; N single-allocation `job_submit` evals pumped by 8 real
eval workers against one bin_pack pool, one seeded allocation so the
argmax is deterministic; `k` is `schedulerSettings.plan_candidates`, so
`k=0` is the pre-candidate behavior in the same binary). Baseline and
after are the same run — the kill switch *is* the baseline. See
[docs/late-binding-plan-apply.md](../../docs/late-binding-plan-apply.md).

| N | pool | k=0 conflicts | k=8 conflicts | k=8 fallback binds | k=0 → k=8 time-to-all-placed |
| --- | --- | --- | --- | --- | --- |
| 8 | 6 (small) | 9 | 2 | 6 | 191 ms → 76 ms |
| 8 | 64 (headroom) | 9 | **0** | 7 | 249 ms → 47 ms |
| 32 | 6 (small) | 16 | 2 | 19 | 320 ms → 75 ms |
| 32 | 64 (headroom) | 17 | **0** | 22 | 248 ms → 88 ms |
| 128 | 6 (small) | 41 | 2 | 70 | 617 ms → 325 ms |
| 128 | 64 (headroom) | 58 | **0** | 82 | 827 ms → 349 ms |

Reading: at headroom, candidate lists remove conflict rejections
entirely — every race is absorbed as a fallback bind (rank > 0) inside
the near-tie band — and time-to-all-placed drops 2–5×. On the small
pool, the residual conflicts are walks whose whole candidate set went
stale within the snapshot-to-apply window (the pool only has 6 nodes),
and the requeue backstop absorbs what the retry budget does not: zero
failed evals in every cell, both modes. The `k=0` rows also show the
Phase 1 backstop working alone (requeues > 0 at N=128, still zero
drops).

## Criterion guard — late-binding candidate lists (2026-07-29)

Same-machine interleaved runs (base = the merge-base of the late-binding
branch), `cargo bench --bench scheduler_bench`. Two findings shaped the
final shape of the change:

- An early cut of the apply path eagerly prefetched node rows for the
  whole candidate UNION and measured ~1.9× slower on the 10k-node
  plan-and-apply cell; rows now load lazily per candidate actually
  attempted (the walk usually binds rank 0), which returned the cell to
  parity. The below-band fallback also uses partial selection instead of
  a full per-placement sort.
- What remains is the honest price of building ranked lists at plan time:

| Bench | Shape | Base | Late-binding |
| --- | --- | --- | --- |
| `candidate_construction` | 12 tasks × 2, 1k nodes, indexes off / on | 1.37 / 1.35 ms | 1.51 ms (+10%) / 1.50 ms (+11%) |
| `candidate_construction` | 12 tasks × 2, 5k nodes, indexes off / on | 8.39 / 6.54 ms | 8.71 ms (+4%) / 6.85 ms (+5%) |
| `candidate_construction` | 12 tasks × 2, 10k nodes, indexes off / on | 19.7 / 14.6 ms | 20.2 ms (+3%) / 14.8 ms (+2%) |
| `scheduler_speed` (plan-and-apply) | 10 jobs × 10 allocs, 1k nodes | 6.18 ms | 6.28 ms (+1.6%) |
| `scheduler_speed` (plan-and-apply) | 10 jobs × 10 allocs, 10k nodes | 71.4 ms | 80.0 / 67.3 ms (two rounds; parity within run noise) |

Reading: single-planner, uncontended plan/apply cost is unchanged within
noise on the end-to-end cells; plan-only construction pays a few percent
(highest on small fleets, where the absolute cost is ~1.5 ms) for the
band partition, tail sampling, and per-placement candidate vectors.
Against the contention table above — conflicts eliminated at headroom,
2–5× faster time-to-all-placed under bursts — the trade is decisively
worth it. Revisit only if profiling shows the tail sampling on the plan
path of a latency-sensitive fleet.

Note: these cells were measured against the pre-fused two-op apply path
(insert + apply as separate replicated ops). The fused
`submit_and_apply_plan` below landed in parallel; the late-binding walk
now runs inside the fused op, so the two changes compose — the walk adds
its per-candidate epoch checks to the single round-trip the fusion left.

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

## Plan batch as one transaction — 2026-08-03

Phase 4 of `internal_docs/plans/plan-path-scale-levers-plan.md`: a group
commit is now ONE write transaction on every backend (previously the
amortization landed only under raft — judgement call #6), and the
batch's post-apply `task_status` projection is one status-lane write for
the batch instead of one per plan.

**Commit count, measured directly** (`cargo test --lib
local_plan_batch_commits_once_and_projects_statuses_once`, a SQLite
commit hook counting transactions that reach the disk):

| Three plans | commits before | commits after |
| --- | --- | --- |
| applied singly (`plan_apply_coalescing: false`) | 6 | 6 |
| applied as one batch | 6 | **2** |

Six is three plan commits plus three status projections; two is one of
each. This is exact, not a timing.

**Wall clock, conflict-free** (`cargo test --release --lib
bench_local_plan_batch -- --ignored --nocapture`; batch of 16
independent single-placement plans on `local_disk`, medians of 20
repetitions, dev container). The "before" arm applies the same 16 plans
one at a time, which is precisely what the pre-phase-4 batch did on a
local backend:

| Batch of 16, local_disk | before | after |
| --- | --- | --- |
| plan applies + status projections | 31.99 ms | **4.33 ms** (7.4x) |

The plans are built with disjoint scopes on purpose: this measures the
write path, not optimistic-concurrency contention.

**Macro matrix** (`LEGION_BENCH_CONCURRENCY=4`, release, same host,
`steady-small` + `web-fleet`, one run per arm):

| Scenario | Backend | evals/s before → after | p99 ms before → after |
| --- | --- | --- | --- |
| steady-small | local_disk | 277.8 → 294.2 | 7.84 → 7.41 |
| steady-small | raft1 | 30.5 → 29.0 | 68.4 → 71.7 |
| web-fleet | local_disk | 325.9 → 360.7 | 29.2 → 28.3 |
| web-fleet | raft1 | 69.9 → 69.2 | 51.1 → 46.6 |

Honest reading: **the macro matrix does not resolve this change on this
host.** Three back-to-back runs of the *same* (after) binary measured
raft1 `steady-small` at 29.0, 40.2 and 52.3 evals/s — a spread far wider
than any delta in the table above. These cells are also conflict-bound
(112-129% conflict rate on `steady-small`), which is the ceiling the
phase-3 entry above already documented and which batching cannot move.
The commit-count table is the result; the wall-clock A/B is what that
costs in time; the macro numbers are recorded only to show nothing
regressed beyond noise, with correctness 1.000 in every cell.

Under raft the plan write was already one propose per batch, so the new
win there is the status projection: a batch of N plans drops from N
status-lane rounds to one (N more raft rounds under
`topology: "lanes"`).

## Free-pool rebuild inside the apply commit — 2026-08-05

Phase 4 of `internal_docs/plans/eval-path-raft-cost-plan.md`:
`Store::free_pool_for_node_tx` no longer reads every allocation row on a
node on the common path. The node's total usage is cached in
`node_alloc_usage` and validated on each read against a fingerprint of the
live rows (node allocation epoch + row count + allocation-id sum), which
comes out of `idx_allocation_node_id` without touching a row body or
parsing any JSON.

The thing being fixed is a *slope*, not a constant: the AWS `dense_fleet`
ladder measured throughput at 736.1 → 456.58 → 272.91 alloc/s as live
allocations went 50k → 100k → 200k at 5,000 nodes (≈1.6x per doubling),
while plan total grew only 22%.

**Wall clock per free pool** (`cargo test --release --lib
bench_local_free_pool -- --ignored --nocapture`; 50 nodes, 20 rounds,
both arms against the same store in the same process, `drop_own` on —
the Fresh-plan shape):

| allocations/node | row walk | cached | speedup |
| --- | --- | --- | --- |
| 10 | 15.573 µs | **8.635 µs** | 1.8x |
| 40 | 37.764 µs | **9.358 µs** | 4.0x |

Read the *columns*, not the rows: over the same 4x change in cluster
fill, the row walk grows 2.4x while the cached path grows 1.08x. That
flattening is the point — the residual growth is the fingerprint scan,
which is still proportional to allocations-per-node but at roughly a
hundredth of the constant.

For an eval touching ~44 nodes at 40 allocations/node, that is ~1.66 ms
of in-commit time against ~0.41 ms.

**Not measured here:** end-to-end throughput. This is a serialized-commit
cost on a multi-node cluster under fill; a single-node local run cannot
reproduce either the serialization or the fill. The AWS ladder is the
real number and has not been re-run.

## Read-model reprojection under fill — 2026-08-06

Phase 5 of `internal_docs/plans/eval-path-raft-cost-plan.md`. After the
delta refresh and the per-node usage cache both provably flattened their
own paths, the AWS `dense_fleet` ladder's ~1.65x-per-doubling degradation
did not move — pointing at `reproject_model_nodes`, which re-aggregated
each touched node's **whole allocation list** (one JSON normalization per
allocation) after every projected changeset, on every replica, inside the
read-model write lock every planner reads through.

The read model now keeps a per-node `NodeUsage` aggregate beside each
cached allocation list; reprojection subtracts it (O(resource dims)), the
delta path adjusts it with exactly the rows it spliced, and every
wholesale list reload drops the entry so the next reprojection rebuilds
it from the list.

**Wall clock per reprojection of 44 touched nodes** (`cargo test
--release --lib bench_local_reprojection -- --ignored --nocapture`; both
arms against the same store in the same process — the walk arm clears the
memo each round, which is cost-for-cost the pre-memo reprojection):

| allocations/node | walk | memo | speedup |
| --- | --- | --- | --- |
| 10 | 413.99 µs | **49.93 µs** | 8.3x |
| 40 | 1.921 ms | **73.64 µs** | 26x |

Read the columns: over a 4x change in fill the walk grows 4.6x while the
memo arm grows 1.5x (the residual is `ResourcePool::from_node` and the
index reprojection, which do not scale with fill). This time was
invisible to the plan-phase timers — it runs after the commit — and it
was paid under the write lock, so it also stalled every concurrent
planner's snapshot.

The same change batches a refresh's per-scope epoch reads (one read
connection + one cached statement for the whole scope set, was ~44
acquisitions and statement parses per eval, per replica).

**Free-pool re-measurement on this host** (the policy-fingerprint gate
added one predicate to the cache read): row walk 36.49 µs / 96.57 µs at
10 / 40 allocations per node against cached 25.32 µs / 28.29 µs — the
cached path still flat across fill (1.12x over the 4x change), so the
extra predicate is noise. Absolute numbers are not comparable to the
2026-08-05 table above (different machine); the within-run arms are the
comparison.

**Not measured here:** end-to-end throughput, as ever — the ladder is
the real number. With both the SQL and in-memory fill terms gone, the
remaining suspects if the slope still does not flatten are outside this
path entirely.

**Eval critical path** (`cargo bench --bench eval_critical_path_bench`,
base `c0a175f` vs this change, same host, runs serialized): workers_1
159.50 → 161.12 ms, workers_4 423.74 → 441.16 ms (quiet re-run; a first
run that overlapped a background build read 469.93), workers_8 736.48 →
733.49 ms. No regression beyond this bench's run-to-run spread — it
drives the Store API directly with empty-placement plans, so the changed
store paths (refresh aggregation, epoch batching, policy-fingerprint
predicate) are all near-no-ops on it and the daemon changes are not in
its path at all; it is recorded as the no-regression check, per the
convention above.

## JSONB → TEXT column affinity — 2026-08-07

`internal_docs/plans/jsonb-text-affinity-plan.md`. Every JSON column had
NUMERIC affinity (declared `JSONB` matches no SQLite affinity keyword),
so every JSON insert first ran `sqlite3AtoF` over the bound blob trying
to store it as a number — ~4-5% of a sampled manager's CPU, inside the
plan-apply commit and the intake path. One migration rebuilds all 17
affected tables with TEXT-affinity columns, preserving column order,
count, and rowids exactly (raft changesets bind by position, so the
layout is effectively replication wire format).

**Direct A/B of the mechanism** (standalone rusqlite probe, this host:
200k-row inserts of a realistic `resources` blob into 3 JSON columns +
1 timestamp, NUMERIC-affinity twin vs TEXT twin, interleaved rounds):
the NUMERIC table is **5-10% slower on the raw insert path**, entirely
`applyNumericAffinity`. That is the cost the migration deletes.

**End-to-end Criterion cells** (base `3a62fcd` via a worktree vs the
migration, same host — a shared cloud VM, `bench` profile, runs
serialized on an otherwise idle box, disk usage flat throughout):

| `scheduler_speed/bin_pack/nodes_1000` | before | after | change |
| --- | --- | --- | --- |
| allocs_10 | 14.44 ms | 14.45 ms | +0.07% (p=0.94) |
| allocs_100 | 25.25 ms | 25.47 ms | +0.84% (p=0.33) |
| allocs_1000 | 308.01 ms | 310.67 ms | +0.86% (p=0.25) |
| allocs_10000 | 3.621 s | 3.686 s | +1.81% (p=0.02) |
| allocs_100000 | 34.19 s | 34.17 s | **−0.05% (p=0.90)** |

No change detected — including the 100k-allocation cell, the heaviest
JSON-write volume in the suite. The eval critical path
(`eval_claim_apply_complete_latency`) read +8.4%/+4.5%/+4.8% at 1/4/8
workers, but a control run of the **unchanged baseline against its own
saved baseline** read "+27%" at workers_1 in the same session — that
bench's fsync-heavy iterations put these deltas inside its run-to-run
spread on this host, and its plans carry empty placements so the
affinity change is a near-no-op on it by construction. Recorded as the
no-regression check; the mechanism-level probe above is the
measurement.

**Why the first attempt's numbers were discarded:** both micro
harnesses leaked their per-iteration store directories
(`tempdir().keep()`), so a long run filled the disk — the first
"after" run measured filesystem pressure, then died of ENOSPC
mid-suite. The eval bench also created its store (running the full
migration suite twice) **inside** the timed iteration, charging every
appended migration to a benchmark named claim/apply/complete latency.
Both fixed in this change: stores ride their `TempDir` guard and drop
outside the timed section, and the eval bench builds its store in
`iter_batched` setup. Absolute eval numbers are therefore not
comparable to earlier records (they no longer include store setup);
within-run comparisons are unaffected.

## Plan-path constants and the cluster-fill slope — 2026-09-02

**Host: 4-vCPU Linux VM (Claude Code remote env), `bench` profile
(release), local_disk, runs serialized on an otherwise idle box.** Base is
`c3291c9` (the commit that adds the `plan_under_fill` group, on top of the
#196 merge); the "after" column is this change set. Every cell is
Criterion's median against the saved base baseline; `scheduler_speed`
cells ran with `--sample-size 20` on both sides (their 10k-node setups
insert 10,000 nodes per iteration). The `plan_and_apply` cells are
fsync-bound on this VM and swing ±20% run to run — read `plan_only` for
planning cost.

The pass had two targets, both found by adding the fill axis to the
micro benchmarks: costs that scaled with **cluster fill** and costs that
scaled with **fleet size per placement**, neither of which had anything to
do with the job being placed.

- **Fill.** `plan_under_fill/plan_only` went 2.55 → 7.07 ms from an empty
  5,000-node fleet to 40 allocations per node (200k allocations) on the
  base, planning the same six placements. Five to six per-plan walks
  over every allocation in the cluster (the Fresh drop, two count
  passes, group pinning, the `distinct` seed, and the scoring cache's
  per-candidate walk) each filtered for the planned job. Fusing them into
  one walk took the slope to ~2.1 ms per 200k (round 1, below); a per-job
  allocation index in the read model — maintained by the same writes as
  the per-node lists — takes it out entirely, and the post-commit refresh
  now publishes a plan's rows as the same delta the raft apply path
  projects instead of reloading every touched node's list.
- **Fleet per placement.** `eligible_nodes` re-ran the task's permit over
  every candidate on every placement, allocating a restriction map per
  candidate each time, for a question the candidate list had already
  answered; the scoring cache hashed each node's id string three times
  per candidate per placement. The permit re-check is gone (the fast
  `is_permitted` path that replaced the map allocation also speeds the
  scan that builds candidate lists), and the cache indexes by candidate
  address.
- **Statement parsing.** The per-node statements inside the plan-apply
  commit (epoch bump, usage fingerprint and cache read, usage upsert),
  the per-plan submission insert/select/update and task-state load, the
  eval claim/complete/count statements, and the scheduler's small
  per-plan reads all re-parsed their SQL on every call. All go through
  `prepare_cached` now, and a scope-epoch read no longer seeds a row
  first.

| Cell | base (median) | after (median) | change |
| --- | --- | --- | --- |
| `candidate_construction/indexes_off/nodes_1000` | 2.8519 ms | 2.3642 ms | -17.101% |
| `candidate_construction/indexes_on/nodes_1000` | 2.9096 ms | 2.3249 ms | -20.095% |
| `candidate_construction/indexes_off/nodes_5000` | 15.386 ms | 11.971 ms | -22.200% |
| `candidate_construction/indexes_on/nodes_5000` | 9.3330 ms | 8.1300 ms | -12.890% |
| `candidate_construction/indexes_off/nodes_10000` | 31.575 ms | 23.416 ms | -25.839% |
| `candidate_construction/indexes_on/nodes_10000` | 15.965 ms | 14.104 ms | -11.657% |
| `plan_under_fill/plan_only/fill_0` | 2.5532 ms | 1.9633 ms | -26.114% |
| `plan_under_fill/plan_and_apply/fill_0` | 15.050 ms | 3.9539 ms | -66.976% |
| `plan_under_fill/plan_only/fill_10` | 4.1905 ms | 2.9454 ms | -29.700% |
| `plan_under_fill/plan_and_apply/fill_10` | 14.572 ms | 4.5034 ms | -63.303% |
| `plan_under_fill/plan_only/fill_40` | 7.0677 ms | 2.8559 ms | -59.683% |
| `plan_under_fill/plan_and_apply/fill_40` | 16.139 ms | 4.7164 ms | -67.799% |
| `scheduler_speed/bin_pack/nodes_1000_allocs_100` | 21.492 ms | 16.611 ms | -22.710% |
| `scheduler_speed/bin_pack/nodes_1000_allocs_1000` | 254.19 ms | 106.05 ms | -58.279% |
| `scheduler_speed/bin_pack/nodes_10000_allocs_100` | 100.70 ms | 86.142 ms | -14.458% |
| `scheduler_speed/bin_pack/nodes_10000_allocs_1000` | 365.04 ms | 185.54 ms | -49.173% |
| `eval_claim_apply_complete_latency/workers_1` | 27.703 ms | 20.335 ms | -26.597% |
| `eval_claim_apply_complete_latency/workers_4` | 248.36 ms | 174.50 ms | -29.738% |
| `eval_claim_apply_complete_latency/workers_8` | 493.22 ms | 357.81 ms | -27.453% |

Readings:

- **The fill slope is gone.** `plan_only` reads 1.96 / 2.95 / 2.86 ms
  at 0 / 10 / 40 allocations per node: flat between 10 and 40, where the
  base grew 4.19 → 7.07 ms. The step between 0 and 10 is not fill but
  fleet — at fill 0 no node has an allocation list at all, so the
  snapshot's per-node map clone and the scoring cache's per-candidate
  lookups against it cost nothing; from the first allocation per node
  onward they cost O(fleet), and then stay there (the phase means below
  show `capacity` 371 → 715 → 763 µs). Round 1 (walk fusion only, no
  index) still read 1.94 / 2.97 / 4.02 ms — the one remaining walk —
  which is what motivated the index.
- **Per-placement cost dropped by the map allocation.** The
  1,000-allocation `scheduler_speed` cells halved (10 jobs of 100
  placements each: ~100k redundant permit checks per plan on the base);
  `candidate_construction` — one to two placements per task — gained
  12-26%, mostly from the allocation-free `is_permitted` on the scan
  (5k-10k nodes × 12 tasks of permit checks per plan).
- **The eval critical path dropped 27-30%** at 1/4/8 workers from the
  statement cache alone: that bench drives the store API with
  empty-placement plans, so nothing else in this change set is on its
  path.
- **`plan_and_apply` dropped ~3.8x at every fill level**, and the
  attribution is by elimination: round 1 of this change set (everything
  except the delta publish and the job index) left the cell where the
  base was (15.5 ms at fill 0, within its 12-17 ms spread); round 2
  took it to 3.95 ms (tight, 3.6-4.0 ms), and of the round-2 changes
  only the delta publish is on the apply path. So the post-commit
  reload — one full per-node allocation reload plus reprojection per
  touched node, even on an empty fleet — was the bulk of a local
  plan's apply cost, ahead of the commit itself; the after-phase means
  below put `apply` at ~1.4 ms including the commit. The base's
  variance (12-17 ms) went with it. The printed phase means are the
  reading for where the remaining apply time goes:

  Mean microseconds per phase over every measured plan (after):

| fill | plans | nodes | versions | tasks | capacity | build | apply | total |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 1725 | 89 | 10 | 50 | 371 | 1747 | 1372 | 3710 |
| 10 | 1387 | 91 | 13 | 52 | 715 | 1993 | 1346 | 4320 |
| 40 | 1387 | 85 | 13 | 54 | 763 | 2025 | 1441 | 4519 |

**Not measured here:** the AWS ladder. The 5,000-node `dense_fleet`
cells that first showed the fill degradation are the real number and
have not been re-run; `plan_under_fill` is the local stand-in and the
regression test for this change set.
