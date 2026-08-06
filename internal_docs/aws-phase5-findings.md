# AWS phase 5 — the night the rig lost

**Date:** 2026-08-06. **Branch:** `perf-night-2` (off `main` @ `ea27ee7`, i.e.
after PR #193). **Status:** measurements current as of this branch; the CLI
load generator can no longer saturate the scheduler on any scenario.

## Where the ceiling moved

Every cell at 5,000 nodes, three m6id.xlarge managers, 32 eval threads.
"Floor" means `submission.driver_bound: true` — the driver, not Legion, set
the number.

| dense_fleet fill | #191 | #193 (16t) | this branch (32t) |
| --- | --- | --- | --- |
| 50,000 | 1,721.5 | 1,693.3 | 1,625.5 (drain 2.4 s — near-floor) |
| 100,000 | 1,170.9 | 1,105.4 | **≥1,323.8, floor** (drain 1.5 s) |
| 200,000 | 636.6 | 643.2 | **≥741.6, floor** (drain 1.8 s) |

The drains are the story. #193 ran 39–177 seconds behind the driver at the
deep-fill points; this branch runs **1.5–1.8 seconds** behind a driver whose
own rate sagged from 89 to 17 jobs/sec across the day. The scheduler
consumed everything the rig could produce at every fill level, dense
scenario included. Since the dense scenario was built specifically to outrun
the submitter, the rig is now the floor everywhere, and the fast native
submitter (in progress) is the only way to see the ceiling again.

### What the truncation bug means for every number in this file

Every binary measured in phases 2–5 carried the same read-model truncation,
so the A/B comparisons in this document stand — both sides packed the same
artificially small universe. But the absolute framing "at 5,000 nodes"
overstates the placement universe: the planner effectively worked ~1,250
nodes, so per-node fill ran ~4x the nominal figure (the 200k-allocation
cells were ~160 allocations/node, not 40), and anything O(rows-per-node) —
the free-pool fingerprint, reprojection before its memo — was amplified
accordingly. Fixing the truncation may therefore flatten the measured fill
slope on its own. Re-measure after the fix before attributing the slope to
anything else.

## What this branch changed, with the evidence for each

1. **Worker dispatch left the eval's critical path** (`3be4c68`). The eval
   worker awaited every placed worker's `PostWork` RPC despite dispatch
   being best-effort by design (the heartbeat carries the desired set).
   Measured before: `eval_handle` 272 ms against a 94 ms plan path at 100k.
   Sends are now spawned, bounded by a 512-permit semaphore, observable via
   `legion_dispatch_send_seconds`.
2. **Job intake group-commits** (merge `331bd97`). Submission cost three
   quorum rounds per job on three lanes, none batched, while the driver
   submitted 32-wide. The scheduler-lane enqueue and the status-lane seed
   now coalesce (per-request savepoints, batch-of-one unchanged); the
   control-lane `handle_job_submit` was deliberately NOT batched — its
   version derivation reads committed state, and batching would promote a
   commented race into a routine one. Also fixed en route: two concurrent
   same-key `RunJob`s both reported `deduplicated: false`.
3. **Statement caching + WAL off the commit path** (`8f10ad8`). A sample
   profile of the serialized batch body showed ~15% SQL *parsing* (rusqlite
   `execute`/`query_row` never hit the statement cache) and pwrite
   dominance (inline WAL checkpoints). Hot statements now `prepare_cached`
   (cache raised 16 → 128), checkpoints run PASSIVE from a background
   thread with the inline threshold raised to 4000 pages as backstop.
   **Measured: batch body 160.8 ms → 87.7 ms** (200-batch mean, table at
   40k→128k rows). On-rig, the group-commit execution dropped from 285 ms
   at batch 9.6 to ~20 ms in the post-fix snapshot.
4. **Benches for the commit** (`3164703`). `bench_plan_batch_commit_split`
   proved the commit ≈ body execution (session diff ~0–10 ms, changeset
   apply 9–12 ms — the leader's "double execution" costs ~17%, not 2×), and
   `bench_plan_body_hotspot_loop` is the profiling harness that found the
   parser and pwrite. Post-fix, the body is b-tree seeks, WAL append, and
   row-JSON parsing — approaching inherent cost.
5. **`--keep-jobs`** (`645695e`). The storm drill ran after the cell's
   cleanup and measured a 1,250-node loss with no allocations on it. The
   flag leaves the workload resident so the drill measures replacement.

## Storm drill (on #193's binary, allocation-less by accident)

A 1,250-node loss produced **max eval-queue depth 5** — #193's
no-allocation enqueue filter works; the pre-#193 shape was one eval per
lost node. 1,194 nodes recovered via `reregistration` and the new
`legion_nodes_recovered_total{path}` attributes it. Replacement throughput
was unmeasurable (no resident workload); re-run with `--keep-jobs` covers
it, and node-loss replacement is generated internally, so the slow CLI
driver cannot bound that number.

## Storm with a resident workload (this branch)

`--keep-jobs` landed and the drill re-ran with 25,002 `web_fleet`
allocations resident when the slice died:

| metric | value |
| --- | --- |
| placement gap | **0.006 s** |
| replaced | 23,935 allocations |
| recovery time | 86.4 s |
| **replacement throughput** | **~277 alloc/s sustained** |

The placement gap is the headline: the scheduler never stopped placing
while it absorbed the loss storm. Replacement work is internally generated,
so this is a true Legion-bound number — ~46 evals/s of failure-recovery
scheduling concurrent with the loss processing itself. (For scale:
Kubernetes' default eviction rate limit is 0.1 nodes/sec, so a loss this
size takes hours to evict at defaults before rescheduling begins.)

*And a placement-availability bug, found by the drill and then localized:* `web_fleet` declares
`spread`, yet ALL 25,002 allocations sat on the killed one-of-four slice,
and their replacements packed the NEXT slice (24,012 on `sim-1-` after the
drill, with slices 2 and 3 available and empty). The investigation
eliminated, in order: slice naming (clean, 4x1,250), availability rows
(correct), class distribution (uniform 1,042 web + 208 util per slice),
node pools (all `default`), the head-jitter tie-break (uniform in code and
in tests), available-nodes cache staleness on registration (invalidation
verified on both the direct and the projection path), and the
capture->apply->projection path itself (a single-voter raft repro at 100
and at 300 nodes spreads perfectly; `tests/spread_under_raft.rs` pins it).

What localized it: `plan_submission` rows from the replacement window carry
their candidate lists, and every candidate in every sampled plan is
`sim-1-*` — the PLANNER'S ELIGIBLE SET was one slice, before scoring or
binding. Confirming detail: nodes filled to exactly their `maxAllocations`
cap (32/node) before spilling — cap-fill is what placement into an
artificially tiny universe looks like — and even `util`-class jobs landed
only on slice 1, so the truncation is class-independent. The read model's
node universe under rig-scale registration (worker-registration RPC at
250/s x 4 instances, 5,000 nodes) holds one slice at a time; no local
harness up to 300 nodes reproduces it. This is the top open item: it
defeats failure-domain spreading (one instance loss took 96% of the
workload) and it caps effective fleet utilization at one slice per epoch
of whatever refresh is failing. Silver lining: the storm drill therefore
measured a WORST-CASE loss, and the 86 s / 6 ms numbers held.

## Corrections from tonight's own analysis

- "~300 ms of eval life unaccounted post-dispatch-detach" — an artifact of
  reading `eval_handle` means during a cell's drain phase; the plan path is
  the eval cost now.
- The 50k single-shot arms of the commit-split bench are noisy (one run
  showed the 40k body *above* the previous run's); the 200-round loop mean
  is the number to trust.
- `acked_write_survives_any_single_node_loss` failed under the fully
  parallel suite (161 s, CPU-starved elections) and passes 4/4 in isolation
  in 8 s — same load-sensitive family as the two known flakes, not the WAL
  change.

## Deliberately not done, and why

- **Batching control-lane `handle_job_submit`**: version derivation reads
  committed state; two same-job submits in one uncommitted batch would
  both compute N+1. Restructuring the RPC handler is the prerequisite.
- **Leader direct-commit at capture** (skip rollback + changeset re-apply):
  structurally safe under per-lane serialization and the ownership guard,
  but breaks "store state == applied log prefix" if the leader crashes or
  loses leadership between body-commit and quorum. Needs changeset
  inversion; and the split bench showed the double-execution costs only
  ~17%, so the prize is small.
- **Batched plan/apply across evals**: handlers write across lanes as they
  go; making them pure functions of a snapshot is a scheduler rewrite.

## Open, in priority order

1. **The planner's node universe truncates to one slice at rig scale** —
   see the storm section for the full elimination record. Next probe: a
   fleet-scale repro with worker-registration RPCs (not `insert_node`) at
   the sim's register rate, and read-model introspection (expose the
   read model's node count as a gauge next to `legion_nodes`).
2. **The fast native submitter** — the gate on every further measurement.
2. **Re-run the fill ladder behind it** — the slope (1.6×/doubling through
   #193) is untested on this branch because every deep point is a floor.
4. **The eval-thread knee above 32** — same gate.
