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
