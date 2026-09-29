# AWS Phase 3 — conflicts are fixed, and that was not the ceiling

**Date:** 2026-08-04 (third session, ~1 hour of rig time)
**Binary under test:** `8e90745` — the merge of #189 (two-pass plan bind)
**Rig:** Legion A, 3-voter raft, `lanes` topology, 3x `m6id.xlarge`
(4 vCPU) with the raft store on instance NVMe, `bin_pack`, `web_fleet`,
`evalThreads = 4`.
**Campaign:** `20260804-1506-legion-a`, archived to
`s3://legion-bench-archive-594906412132/campaigns/`.

## The result in one table

Two cells, each on a freshly reset control plane and an exact node census
(1000/1000 and 5000/5000 active, zero lost, zero foreign), with the eval
backlog drained to 0 before the 5,000-node cell.

| | phase 2 (pre-#189) | phase 3 (post-#189) | change |
| --- | --- | --- | --- |
| **plan-apply rejected** | 31.9% | **0%** | **eliminated** |
| plan-apply applied / fallback | — | 2,897 / 2,104 | — |
| bind pass `clean` | — | 30,006 (= every placement) | — |
| bind pass `revalidated` | — | **0** | — |
| eval conflict requeues | — | **0** | — |
| throughput @5,000 nodes | 86.6/s | **80.35/s** | −7% |
| p50 @5,000 nodes | 57.6 s | **49.4 s** | −14% |
| p99 @5,000 nodes | 177.1 s | **103.4 s** | **−42%** |
| mean plan time/eval @5,000 | 65.18 ms | **41.42 ms** | **−36%** |

**#189 works, completely.** Across 30,006 placements at two fleet sizes,
`legion_plan_apply_total{outcome="rejected"}` was never incremented — the
series does not exist in the export, on a control plane installed fresh
at 15:07:11. Zero conflict requeues. Every placement bound on the first
pass.

**And it did not raise throughput.** Per-eval planning cost fell by more
than a third and throughput moved by −7%. Latency is where the win
landed: p99 down 42%, which is the signature of removing retries (a
rejected plan produced a very long tail, and killing the retries cut the
tail without moving the ceiling).

So the phase-2 diagnosis identified a real defect and **misattributed the
throughput collapse to it**. The discarded planning work was happening in
parallel with the bottleneck, not in front of it.

## Where the ceiling actually is

At 41.42 ms of plan time per eval and 4 eval threads, the planner alone
could clear ~97 evals/sec, and at ~6 allocations per eval that is ~580
alloc/sec. Measured: **80.35**. The scheduler runs roughly **7x below
what its own planning cost permits**, so the limit is not planning and is
not conflicts. What is left is the replicated write path:

| phase | ms/eval | share |
| --- | --- | --- |
| **apply** | **27.90** | **67%** |
| build_plan | 7.90 | 19% |
| capacity_snapshot | 0.96 | 2% |
| task_fetch | 0.65 | 2% |
| version_sample | 0.39 | 1% |
| available_nodes | 0.12 | <1% |
| total | 41.42 | |

`apply` — which contains the raft commit — is two thirds of plan time,
and the residual gap between 580 and 80 alloc/sec points at the
scheduler lane's serialized commit pipeline, consistent with phase 2's
measurement of 3.07 ms of `pipeline_wait` per op across 67,165 scheduler
lane ops on a single `pipeline_loop` thread.

**The next lever is raft round-trips per eval**, mapped below.

## A hypothesis this session raised and then refuted

Re-reading the phase-2 archives per cell showed `task_fetch` growing
**107x** (0.14 -> 14.96 ms/eval, 1k -> 5k). The manager has (had) two
SQLite read connections:

```rust
const READ_ONLY_CONN_POOL_SIZE: usize = 2;   // store.rs
```

acquired by a round-robin that picks a slot and then *blocks* on it,
never taking a free neighbour — worse than a true 2-slot pool. The
heartbeat path (`build_desired_allocations`, `server.rs:8072`) demands
~10-15 acquisitions per node per 2s, so ~15-37k/sec at 5,000 nodes. That
looked like the ceiling, and it explained CPU sitting at 29% with no
thread above 40% of a core.

**Tonight's 5,000-node cell measured `task_fetch` at 0.65 ms — on the
unmodified binary.** Same fleet size, same heartbeat load, same two
connections. So read-pool queueing is *not* a fleet-size-driven constant;
it was **coupled to the conflict rate**. Every rejected plan triggers a
retry that re-runs the plan phases, multiplying concurrent read demand.
Remove the conflicts and the queueing mostly disappears.

The pool is still a genuine defect and the fix is in this branch
(unmeasured at scale, now lower priority):

| Change | Where | Why |
| --- | --- | --- |
| Pool sized `available_parallelism().max(4)` instead of a fixed 2 | `store.rs` | cores is the bound; these are CPU-bound page-cache hits, so more connections than cores cannot add read throughput |
| Steal-on-contention before queueing | `store.rs` | stops a pool of N behaving worse than N |
| Heartbeat reads task configs from the read model | `server.rs:8094` | removes DB reads for rows the read model already holds |
| `legion_store_read_lock_wait_seconds` | `telemetry.rs` | read-connection wait had **no metric at all**; its cost landed inside whichever plan phase held the stopwatch |

The missing metric is the part worth keeping regardless of the verdict:
it is why this took two campaigns to even look at.

## Second finding: raft round-trips per eval

`Store::replicated` short-circuits when nested, so each top-level call is
exactly one quorum round. Mapped from the code:

| eval kind | scheduler lane | control | status | total |
| --- | --- | --- | --- | --- |
| `job_reschedule` | claim, plan-apply, [follow-up], complete | 0 | 1 | 4-5 |
| `job_submit` | + `remove_allocs_for_job_config` | 0 | 1 | 5-6 |
| `job_update` | + `remove_allocs_for_job` | 1 | 1 | 6-7 |
| `node_lost_reschedule` | claim, `remove_allocations`, C x plan-apply, complete | 0 | C | 3+2C |
| `rollout_tick` | 7 + R | 0 | 1 | 8+R |

Targets, in impact order:

1. **`claim_eval` and `complete_eval_with_event` are two separate
   scheduler-lane rounds bracketing every eval** (`daemon.rs:1653`,
   `daemon.rs:2280`), in a loop where the same worker immediately claims
   again. A `complete_and_claim_next` halves the queue's raft cost. The
   precedent exists — completion and its event were already fused
   (`store.rs:12763`).
2. **No enqueue batching exists anywhere.** No multi-row enqueue API, so
   every enqueue is its own quorum round, including the fan-out loops in
   `regenerate_scheduling_evals` (`store.rs:12531`, `:12564`). This is
   what makes the node-loss storm expensive rather than merely numerous.
3. **`rollout_tick` retired allocations one raft round each**
   (`rollout.rs:1434`) — a step retiring 25% of a 10,000-allocation job
   cost 2,500 sequential rounds. **Fixed in this branch** by calling the
   batched `remove_allocations` that already existed (`store.rs:11170`).

## Rig defects found and fixed

1. **The settle timeout was a flat 600s regardless of cell size**, so any
   large saturation point would have been reported incomplete for
   arithmetic reasons and read as "the scheduler stopped converging".
   Replaced with a size-independent stall timeout (no placement for 120s)
   plus a cap derived from the expected allocation count; `metrics.json`
   records which fired. Confirmed live this session: the 5,000-node cell
   logged `stall timeout 120.0s, absolute cap 1250s` where the old code
   would have imposed 600s.
2. **`run-campaign.sh` defaults `WARMUP=30`, which exceeds the duration
   of a 1,000-node cell.** Tonight's 1k cell placed 5,004/5,004 and
   reported `throughput_per_sec: 0.0` with `steady_window_placed: 0` —
   every placement landed inside the warmup and was excluded. The cell is
   valid for counters (its rejection data is in the table above) and
   useless for throughput. **Open:** scale warmup to cell size, or floor
   the steady window. This is why phase 3 has no 1,000-node throughput
   number.
3. **The density axis scales job count, not replicas**, so every density
   point is also a submission-rate point. Measured tonight: the driver
   submitted 4,167 jobs in ~2 minutes (~35 jobs/sec) on the 2-vCPU
   loadgen box. At 5,000 nodes that offered ~210 alloc/sec against a
   measured 80 — so the scheduler was still the constraint here, but the
   margin is only 2.6x and it closes as density rises.
4. **`web_fleet`'s 500 mCPU requests cap the fleet at ~16 allocations per
   node**, so a density sweep past that measures capacity exhaustion.

(3) and (4) are addressed by the new `dense_fleet` scenario: same node
profiles, ~44 allocations per job against 6, and 50 mCPU requests leaving
room for ~800k allocations on a 5,000-node fleet. It passes the scheduler
SLO gate (31.3 evals/sec, p99 81 ms, correctness 1.000). **Not yet run on
AWS** — the saturation sweep is still unmeasured.

## Tooling added

- **`analysis/cell_summary.py`** — cell archives to one table row each,
  with `--phases` for the per-phase breakdown that produced the tables
  above. It reproduces the phase-2 numbers exactly from their archives.
- **`collect.sh`** now archives `legion_scheduler_plan_bind_pass`, raw
  `plan_apply_total` counters (exact per-cell percentages instead of an
  integrated rate) and `eval_conflict_requeues_total`.
- **`run-campaign.sh --eval-threads N`**, refused rather than ignored
  where it does not apply.
- **`phase1.sh --skip-density`**.

## What the next session must do

1. **Cut raft round-trips per eval** — the fusion and batching above.
   This is now the top item; the evidence says the ceiling is the
   replicated write path.
2. **Fix the warmup defect** and re-run the 1,000-node cell, so the
   regression table has a small-fleet throughput number at all.
3. **Run the saturation sweep** on `dense_fleet` — max throughput is
   still unmeasured above 5,000 nodes / 5 allocations per node.
4. **The 16-eval-thread rows.** Phase 2's headline was that more threads
   *cost* throughput because the extra planners collided. Conflicts are
   now zero, so this should invert — and it is one command
   (`--eval-threads 16`). It did not get run.

Budget ~55 minutes from nothing to a first cell on a cold bootstrap; the
LTO `dist` build alone was 24.5 minutes.
