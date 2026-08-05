# Next Up: Current Focus

**Plan-apply conflicts are fixed, and they were not the ceiling.** Third
AWS session, 2026-08-04 ([findings](aws-phase3-findings.md)), campaign
`20260804-1506-legion-a`.

Two clean cells against `8e90745` (the #189 merge), each on a freshly
reset control plane with an exact node census:

| | phase 2 | phase 3 | change |
| --- | --- | --- | --- |
| plan-apply rejected | 31.9% | **0%** | eliminated |
| throughput @5,000 nodes | 86.6/s | 80.35/s | -7% |
| p99 @5,000 nodes | 177.1 s | 103.4 s | **-42%** |
| plan time/eval @5,000 | 65.18 ms | 41.42 ms | **-36%** |

Across 30,006 placements the `rejected` counter was never incremented
once, conflict requeues were zero, and every placement bound on the first
pass. **Per-eval planning cost fell by a third and throughput did not
move.** The win landed entirely in latency, which is what removing
retries looks like.

So the ceiling is elsewhere. At 41.42 ms/eval and 4 eval threads the
planner could clear ~580 alloc/sec; we measured 80. `apply` — which
contains the raft commit — is **67% of plan time**, and the residual gap
points at the scheduler lane's serialized commit pipeline.

## Do this first, next session

1. **Cut the raft round-trips per eval.** This is now the top item —
   the measurement says the ceiling is the replicated write path. In
   order: (a) fuse `claim_eval` + `complete_eval_with_event` into
   `complete_and_claim_next`; (b) add a batched enqueue API. Details and
   the per-eval op map are in
   [aws-phase3-findings.md](aws-phase3-findings.md). Target <= 2.5
   ops/eval.
2. **Fix the warmup defect, then re-run the 1,000-node cell.**
   `run-campaign.sh` defaults `WARMUP=30`, which is longer than a 1k cell
   takes: tonight's placed 5,004/5,004 and reported `throughput 0.0`
   because every placement fell inside the warmup. Phase 3 therefore has
   **no 1,000-node throughput number**.
3. **Run the 16-eval-thread rows** — one command now
   (`--eval-threads 16`). Phase 2's headline was that more threads *cost*
   throughput because planners collided; conflicts are now zero, so this
   should invert. Untested.
4. **Then the saturation sweep** — max throughput above 5,000 nodes /
   5 allocations per node is still unmeasured. Use `dense_fleet`:
   `run-campaign.sh --system legion-a --scenarios dense_fleet
   --steps "5000" --allocs-per-node-sweep "5 10 20 40 80"`.

The rig is ready and the bootstrap is the expensive part (~55 min from
nothing to a running cell, most of it an LTO `dist` build). **Budget for
it, or leave the operator box stopped rather than destroyed** — it is
~$0.016/hr stopped and holds the terraform state.

## Also shipped this session, unmeasured

- **Rollout retirement is batched.** `rollout.rs` looped
  `remove_allocation_by_id_because`, one raft quorum round per
  allocation; a step retiring 25% of a 10,000-allocation job cost 2,500
  sequential rounds. Now one `remove_allocations` call.
- **The rig can now measure saturation honestly.** The driver had a flat
  600s settle timeout regardless of cell size, which would have reported
  every large sweep point as "stopped converging" for arithmetic reasons.
  It now separates a size-independent stall timeout from a derived cap
  and records which fired (`outcome` in `metrics.json`).


## The rig is the bottleneck, not the scheduler (2026-08-05)

A full session produced **zero cells**. Not one of the failures was in
Legion; all of them were first-boot cost or rot in the rig:

| Cost | Time |
| --- | --- |
| Operator box cloud-init (`cloud-init analyze blame`: `config-scripts_user`) | **54 min** |
| `docker-compose-v2` no longer resolving on Noble, killing bringup twice | ~20 min |
| Rig control-plane/loadgen cloud-init | 10 min |
| `cargo build --profile distfast` | 28 min |
| Spot workers' cloud-init on scale-up | 8+ min, never finished |

**Fix the rig before the next campaign, or the next session goes the same
way.** In order:

1. **Bake the AMIs.** `scripts/bake-operator-ami.sh` exists and is
   stub-tested; run it for real (`--region us-east-1`). Then do the same
   for the **worker** AMI — the sim workers install almost nothing, yet
   pay a full cloud-init on every ASG scale-up, which is what stalled
   this session's last attempt.
2. **Cache the binary, not the compiler flags.** `distfast` (thin LTO, 16
   codegen units) took 28 minutes against 24.5 for full fat LTO — the
   cost is compiling ~900 crates, not linking. `build-and-upload.sh`
   already fingerprints plugins and skips them when unchanged; do the
   same for the main binary, keyed on git rev, in S3.
3. **Keep the operator box stopped rather than destroyed** between
   sessions (~$0.016/hr plus its EBS). It holds the terraform state, the
   toolchain, and `~/legion/target` — i.e. items 1 and 2 for free until
   they are built properly.

Measured end to end: **~80 minutes from `launch` to a first cell**, on a
good run. That is the number to beat.

## Known open questions

- **Why is `apply` 27.9 ms/eval?** It is 67% of plan time and it contains
  the raft commit, but the arithmetic still does not close: 41.42 ms/eval
  across 4 eval threads permits ~580 alloc/sec and we measured 80. Either
  evals are far more serialized than 4-way (the scheduler lane's single
  `pipeline_loop` thread is the suspect — phase 2 measured 3.07 ms of
  `pipeline_wait` per op over 67,165 ops) or `apply` itself blocks. Get
  `legion_store_write_lock_wait_seconds` and the lane pipeline metrics
  into the next campaign's collection.
- **A read-pool hypothesis was raised and refuted this session.** The
  manager had two SQLite read connections with no steal-on-contention,
  and `task_fetch` grew 107x between 1k and 5k in the phase-2 archives —
  which looked like the ceiling. But tonight's 5k cell measured
  `task_fetch` at **0.65 ms on the unmodified binary**. Read-pool
  queueing was *coupled to the conflict rate* (every rejected plan
  retries and re-runs the phases), not driven by fleet size. The pool fix
  is still in the branch and still correct, but it is no longer a
  priority. Its `legion_store_read_lock_wait_seconds` is worth keeping
  regardless: read-connection wait had no metric at all, which is why
  this took two campaigns to notice.
- **Does `build_plan` scale with fleet size?** 7.90 ms/eval at 5k this
  session against 9.82 ms in phase 2, so it did not improve much when
  conflicts went to zero. Still unexplained against a bounded 33-64
  candidate list.
- **Is there a node-deregistration story?** Still no. `Store::remove_node`
  is implemented, correct, tested, and has no caller. Every campaign
  works around it with a control-plane reset.

## Before the next campaign

Nothing to prepare — the rig changes are in the tree and `dense_fleet` is
SLO-gated. Bring up, then go straight to (1).

Higher-level backlog: [TODO.md](TODO.md).
