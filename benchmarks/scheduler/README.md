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
