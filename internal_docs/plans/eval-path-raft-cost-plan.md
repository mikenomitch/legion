# Plan: Eval-Path Raft Cost

> **Judgement calls:** [`../calls/eval-path-raft-cost-plan_calls.md`](../calls/eval-path-raft-cost-plan_calls.md)

**Status: IN EXECUTION** — phases 1 and 2 landed together (claim/complete
fusion, batched enqueue). Phase 3 (re-measurement on AWS) is not run, and
until it is, every throughput claim here is a prediction. Deliberately
deferred: fusing the status-lane `task_status` projection (different
lane, so it cannot share a round), and any change to plan-apply itself.

## 1. Problem statement

The AWS phase-3 campaign
([../aws-phase3-findings.md](../aws-phase3-findings.md), 2026-08-04)
eliminated plan-apply conflicts entirely — `plan_apply_total{rejected}`
went 31.9% → **0%** across 30,006 placements — and throughput did not
move: 86.6 → 80.35 alloc/sec at 5,000 nodes. Per-eval plan time fell 36%
(65.18 → 41.42 ms) and p99 fell 42%. Latency improved; the ceiling did
not.

The arithmetic says where the ceiling is. At 5,000 nodes the cluster
placed 16,546 allocations in a 205.9 s window, and `web_fleet` produces
~6 allocations per job/eval — so **~13.4 evals/sec**. With 4 eval
threads, each eval occupies a worker for **~298 ms** of wall clock while
its measured plan time is **41.42 ms**.

**~86% of every eval's wall time is spent outside the planner.**

`legion_scheduler_plan_phase_seconds` only covers `plan_and_apply`. But
one eval costs 4-5 raft quorum rounds, and only one of them is inside
that 41 ms:

| round | inside plan timing? |
| --- | --- |
| `claim_eval` (scheduler) | no |
| plan-apply (scheduler) | yes — part of the 27.9 ms `apply` phase |
| `update_task_statuses_for_allocations` (status) | **yes — also inside `apply`** |
| `complete_eval_with_event` (scheduler) | no |

**Correction (2026-08-04, from a code audit after this plan was first
written):** the status-lane projection *is* inside the timed region.
`submit_and_apply_plan_coalesced` calls `plan_apply_bookkeeping` before
returning, and `run_apply_bookkeeping` (`store.rs:3301`) issues a
status-lane `replicated()` whenever anything was placed — which is every
applied plan. So `apply_us` contains **two full quorum rounds on two
different raft groups**, each with its own single-threaded pipeline. That
makes `apply` the densest thing on the eval path, and it means the
unmeasured remainder outside the plan phases is two rounds
(`claim`/`complete`), not three or four. A raft commit on instance NVMe inside one AZ
should be 1-3 ms, so those rounds are not committing slowly, they are
**queueing**: each lane drains through a single `pipeline_loop` thread
(`src/storage/raft/driver.rs`), and phase 2 measured 3.07 ms of
`pipeline_wait` per op across 67,165 scheduler-lane ops.

Two consequences, both consistent with everything measured so far:

- **Adding eval threads cannot help**, because the lane is serial. This
  is why phase 2 saw throughput *fall* from 4 → 16 threads, and why
  removing conflicts (which freed CPU that was already idle) changed
  nothing.
- **The lever is fewer rounds, not faster ones.** Making raft faster is
  a much larger project; halving the rounds per eval is a local change.

## 2. Prior art

- **Nomad** batches plan applications through a single plan applier and
  commits evaluation state transitions with the plan itself, rather than
  paying a separate Raft round to dequeue and to ack.
- **Kubernetes** does not replicate its scheduler queue at all — it is
  in-memory per scheduler, and only the binding is written to etcd. One
  round-trip per pod, by construction.
- **Legion's own precedent**: `complete_eval_with_event` already fuses a
  completion with its lifecycle event in one op (`store.rs`, JC-41), and
  the D5 group commit (`coalesce.rs`) already batches N plan applies into
  one transaction. This plan applies the same idea one layer out.

## 3. Jobs to be done

- **Operator:** "I doubled my eval threads and nothing got faster." They
  should not have to know that the queue is replicated, or that threads
  and lanes are different resources.
- **Cluster at scale:** an AZ loss marks ~33% of a fleet lost, and every
  affected node raises an eval. The storm's cost should scale with the
  work, not with a per-row quorum round.
- **Us, measuring:** rounds-per-eval must be visible in an archive, or
  the next campaign re-derives it from wall-clock arithmetic again.

## 4. Goals / non-goals

**Goals.** Halve scheduler-lane rounds per eval in steady state. Make
fan-out enqueues cost one round. Make rounds-per-eval directly
measurable.

**Non-goals.** Making raft itself faster; parallelising a lane's
pipeline; changing `evalThreads` semantics (that stays in
[adaptive-scheduler-concurrency-plan.md](adaptive-scheduler-concurrency-plan.md));
touching plan-apply.

## 5. What exists today

`eval_worker_loop` (`src/manager/daemon.rs`) claims, hands the eval to
`handle_claimed_eval` on the blocking pool, and that function performs
the terminal write itself — one of `complete_eval_with_event`,
`release_eval_for_retry`, or `release_eval_for_retry_with_event`. The
loop then immediately claims again. Claim and complete are therefore two
scheduler-lane rounds bracketing every eval, in a loop where the same
worker is about to ask for more work anyway.

`enqueue_eval_after_delay` is the only enqueue API and is one round per
row. `regenerate_scheduling_evals` (leadership acquisition) and the
node-loss paths fan out over it in plain `for` loops.

## 6. Design

### Phase 1 — fuse the terminal write with the next claim

`handle_claimed_eval` splits in two:

- `resolve_claimed_eval(store, worker_id, eval) -> EvalCompletion`
  decides the outcome without writing it,
- `Store::complete_and_claim_next(eval_id, completion) -> Option<Eval>`
  writes it **and** claims the next eval in one op.

```rust
pub enum EvalCompletion {
    Finished { succeeded: bool, event: EventDraft },
    Released { delay: Duration, event: Option<EventDraft> },
}
```

This is sound because `Store::replicated` short-circuits when already
inside a capture, so the two writes land in one changeset and commit or
roll back together.

The worker loop carries the handed-back eval in `claimed_ahead` and
handles it before the leadership gate — it held leadership when the eval
was claimed, and dropping it there would strand the row `in_flight`.

**Steady-state rounds per eval: 4-5 → 3-4.**

### Phase 2 — batched enqueue

`Store::enqueue_evals(Vec<EvalRequest>)` runs
`enqueue_eval_after_delay_apply` per request inside one op. Applied to
every fan-out in `regenerate_scheduling_evals`: mid-flight rollouts,
active jobs, stop tails, and — the storm path — drained and lost nodes.

**Leadership regeneration and node-loss storms: O(rows) rounds → O(1).**

### Phase 3 — measure it

`collect.sh` now archives `legion_raft_ops_total{lane,op}`,
`legion_raft_pipeline_wait_seconds`, `legion_raft_pipeline_depth`,
`legion_raft_propose_apply_seconds`, the store lock waits, and
`legion_evals_processed_total`. `cell_summary.py --lanes` divides rounds
by evals and prints rounds-per-eval with the mean wait per round.

Read `pipeline_wait` against `propose_apply`: wait >> apply means the
lane is serialized rather than slow, which is the claim this plan rests
on and the thing phase 3 could only infer.

## 7. Testing

- `tests/eval_regen_failover.rs`: the fused call finishes one eval and
  claims the next; the queue draining returns `None`; a delayed
  `Released` goes back to pending and does not re-claim itself; batched
  enqueue creates every row and each stays independently claimable.
- `tests/raft_seam.rs` classifies both new ops onto the scheduler lane —
  the guard that fails the build if a replicated op is unclassified. It
  caught both of these during development, which is the point of it.
- The local SLO gate is a **no-regression** check only: `replicated`
  short-circuits when raft is off, so a single-node run cannot show this
  win at all. Correctness stayed 1.000 across all eight scenarios.

## 8. Rollout

No config, no migration, no wire change — the eval queue schema and the
op semantics are unchanged; only how many quorum rounds they cost
changes. A mixed-version cluster is fine: the ops are per-leader, and a
follower applies changesets either way.

**Exit criterion:** at 5,000 nodes, `cell_summary.py --lanes` shows
scheduler-lane rounds per eval at or below 2.5 (from ~4), and throughput
above the 80.35 alloc/sec phase-3 baseline. If rounds fall and throughput
does not, the lane is not the ceiling either and the next suspect is
`apply` itself (27.9 ms/eval, 67% of plan time).
