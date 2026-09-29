# Plan: Eval-Path Raft Cost

> **Judgement calls:** [`../calls/eval-path-raft-cost-plan_calls.md`](../calls/eval-path-raft-cost-plan_calls.md)

**Status: IN EXECUTION** — phases 1 and 2 landed together (claim/complete
fusion, batched enqueue). Phase 3 (re-measurement on AWS) ran on 2026-08-05
([../aws-phase4-findings.md](../aws-phase4-findings.md)) and its exit
criterion pointed at `apply` itself, which is phase 4 below. Phase 4's code
has landed — batching the queue's own bookkeeping across N evals, plus the
apply-time free-pool cache and the delta read-model refresh — and all of it
is **unmeasured at scale**. The intake phase below (2026-08-06) group-commits
the two projection halves of job submit and is likewise unmeasured at scale.
Deliberately deferred: fusing the status-lane `task_status` projection of the
*apply* path (different lane, so it cannot share a round), the batched *apply*
across evals, which phase 4 explicitly declined because handlers write across
lanes as they go rather than returning a plan, and the control-lane
`handle_job_submit`, which the intake phase declined for the version-derivation
reason below (see the judgement calls).

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

### Phase 4 — take the per-node allocation scan out of the apply commit

Phase 3's exit criterion said that if rounds fell and throughput did not,
the next suspect was `apply` itself. That is where the `dense_fleet` fill
ladder landed: 736.1 → 456.58 → 272.91 alloc/s as live allocations went
50k → 100k → 200k at 5,000 nodes, roughly 1.6x per doubling, while plan
total grew only 22%.

The mechanism is that the serialized plan-apply commit did work
proportional to how full the cluster is while the plan's own work stayed
constant. `Store::free_pool_for_node_tx` rebuilt each placement node's
free pool by selecting **every** allocation row on that node and parsing
every `resources` blob: ~440 rows per eval at 50k allocations, ~1,760 at
200k, for the same ~44 placements.

The pool is now served from a per-node aggregate cached in
`node_alloc_usage`, and **validated on every read** against a fingerprint
of the live rows — the node's allocation epoch, the row count, and the sum
of the allocation ids, all read from `idx_allocation_node_id` without
touching a row body or parsing any JSON. A mismatch rebuilds exactly as
before; a successful apply refreshes the row, but only when the live
fingerprint is precisely the one it predicted. A Fresh plan's own
allocations are gathered once per plan rather than re-filtered per node.

This is deliberately a cache and not a denormalized total: a free-capacity
number that drifts high overcommits a node, and validation makes that
impossible rather than merely unlikely. The reasoning, and the three
alternatives rejected for it, are entry 1 of the calls doc.

It flattens the slope rather than removing it — the fingerprint is still
proportional to allocations-per-node, at roughly a hundredth of the
constant — and the read-model half of the finding is untouched.

Local A/B on the rebuild itself (`bench_local_free_pool`, both arms
against the same store, `drop_own` on):

| allocations/node | row walk | cached |
| --- | --- | --- |
| 10 | 15.573 µs | 8.635 µs |
| 40 | 37.764 µs | 9.358 µs |

The columns are the result: over a 4x change in fill the row walk grows
2.4x and the cached path grows 1.08x. End-to-end throughput is *not*
measurable locally — the cost is a serialized commit on a multi-node
cluster under fill — so the AWS `dense_fleet` ladder remains the exit
criterion.

### Phase 4 — batch the cycle across N evals

Phase 1 divided the queue's fixed cost by two. Phase 4 divides it by the
batch size, because the measured campaign
([../aws-phase4-findings.md](../aws-phase4-findings.md)) says the fixed
cost is what is left. With the wake herd fixed and health checks shown to
produce empty changesets, the only real scheduler-lane quorum traffic is
`complete_and_claim_next` at **44/s** plus ~20/s of plan applies. The
amortization curve says the same thing from the other side — same cluster
(5,000 nodes), same code, same 16 eval threads:

| scenario | allocations per eval | throughput |
| --- | --- | --- |
| `web_fleet` | 6 | 219.25 alloc/s |
| `dense_fleet` | 44 | 736.1 alloc/s |

**7x the work per eval bought 3.4x the throughput.** Throughput tracking
work-*per-eval* rather than work is the signature of a per-eval cost
dominating — if the planner were the ceiling, the two rows would be much
closer together.

So a worker takes N evals through one cycle:

- `Store::claim_evals(limits) -> Vec<Eval>` claims a batch in one op, by
  looping the existing `claim_eval_apply` — same `priority DESC, id ASC`
  head, same superseded-eval sweep, so ordering holds across the batch.
- Each eval is handled exactly as before, serially, applying its own
  plan in its own op.
- `Store::complete_evals_and_claim_next(completions, limits)` settles the
  batch and claims the next one, in one op.

```rust
pub struct EvalBatchLimits { pub max: usize, pub workers: usize }
// share = clamp(pending / workers, 1, max)
```

**Sizing.** The batch is a fair share of the live queue, capped at 16,
and the claim never waits to fill: a shallow queue yields a batch of one,
which is exactly today's cost. The fair share is load-bearing — a greedy
claim would let one worker vacuum a burst that its idle peers could have
run in parallel, which is a throughput *loss* on the fan-out storms this
plan exists for. No config knob: both inputs are already known to the
loop.

**Failure isolation.** `completions` carries only outcomes the worker
observed. A panicked handler, or an eval a mid-batch leadership loss
stopped us reaching, is omitted and stays `in_flight` for
`requeue_in_flight_evals` — phase 1's "write nothing for an unknown
outcome" rule, applied per member. The write is one changeset, so a
rejection settles nothing and leaves the whole batch recoverable; no eval
can be half-completed or completed twice.

**Not done: batched apply.** Handlers write as they go across several
lanes rather than returning a plan, so applying N plans as one commit
means restructuring every handler to be a pure function of a snapshot.
It is also partly redundant: the D5 group commit already batches
*concurrent* applies, and cheaper claim/complete rounds put more workers
in apply at once, which feeds it.

**Steady-state scheduler-lane rounds per eval: 2 → 2/N.**

### Phase 5 — the in-memory fill term, and hardening (SHIPPED 2026-08-06)

The phase-4 AWS re-measurement showed the fill-degradation slope
unchanged (~1.65x per doubling) even though both shipped halves provably
flattened their own paths — so the dominant term was the named next
suspect, `reproject_model_nodes`, which re-aggregated every touched
node's whole allocation list (one JSON normalization per allocation)
after every projected changeset, on every replica, inside the read-model
write lock. This phase:

- **Memoizes per-node usage in the read model** (`node_usage`, the
  in-memory sibling of `node_alloc_usage`): reprojection becomes a
  subtraction over resource dims; the delta path adjusts entries with
  exactly the rows it spliced; every wholesale reload invalidates.
  Local A/B (`bench_local_reprojection_by_fill`, 44 nodes): 414µs → 50µs
  at 10 allocations/node, 1.92ms → 74µs at 40.
- **Batches the per-scope epoch reads** of a refresh (one connection,
  one cached statement, was ~44 acquisitions + parses per eval).
- **Moves the eval loop's claim/settle rounds onto the blocking pool**
  and retries a failed settle (calls #13) — the prerequisite for
  sweeping `evalThreads` past 64.
- **Hardens what phase 4 shipped** (2026-08-06 audit): the
  `node_alloc_usage` cache gains a policy fingerprint (calls #1
  follow-up), the wholesale refresh no longer clobbers a newer delta
  publish (calls #3 follow-up), and the status-projection drain is
  supervised as itself rather than as an eval worker (calls #14).

**Exit criterion:** the `dense_fleet` 50k/100k/200k ladder, re-run on
the rig — the slope should now flatten. If it still does not, the next
suspect is off this path entirely (control-plane CPU at saturation,
phase-4 findings open item 4).

### Intake phase — group-commit the two projection halves of job submit

Everything above attacks what an eval costs once it exists. The AWS
5,000-node profile says the *creation* of an eval is just as expensive,
because a submitted job writes to all three lanes and each write is its
own quorum round:

| op | lane | applied/s |
| --- | --- | --- |
| `enqueue_eval_after_delay` | scheduler | 19.1 |
| `handle_job_submit` | control | 16.6 |
| `seed_task_statuses` | status | 16.6 |
| `complete_evals_and_claim_next` (already batched) | scheduler | 4.2 |

The top three move in lockstep with the submission rate against 21.9
evals/s overall; the fourth is what the phase-4 batching did to a
comparable per-eval round (0.19 rounds/eval). The intake path never got
the same treatment, and the driver submits 32 jobs concurrently — so
there is real concurrency to absorb.

Two of the three halves now take the `coalesce.rs` group commit, one
coalescer per lane (a lane is a separate raft group with its own
pipeline, so a batch physically cannot span two):

- **scheduler** — `enqueue_eval_after_delay` submits an `EvalRequest` to
  the `eval_enqueue` coalescer, whose batch runner proposes one
  `enqueue_evals_isolated` op applying each request in its own savepoint.
- **status** — the submit composite's seed submits its `job_config_id` to
  the `task_status_seed` coalescer, whose runner proposes one
  `seed_task_statuses_batch` op, again savepoint-per-request.

Both keep the pre-batching single op as the batch-of-one fast path, as
the `NotRaftLeader` fallback (so the wire op and the typed rejection the
standing-split tolerance matches on are unchanged), and as the
`schedulerSettings.job_intake_coalescing = false` path. There is no
linger: an uncontended submitter goes straight to the commit slot.

**The control-lane half is deliberately left alone.**
`handle_job_submit_apply` derives a config's next version from
`get_job_config_optional`, which reads committed state through a read
connection; today one submit commits before the next reads because each
is its own replicated op. In one uncommitted batch transaction, two
submits of the same job would both read the same latest version and both
compute N+1 — the file's own long-standing race note, promoted from rare
to routine. The composite is also not one store call:
`submit_rendered_job` interleaves `clear_job_stopped_status`,
`replace_job_kv_references`, `seed_keyed_instances`,
`previous_job_config_id` and rollout creation between the control write
and the eval enqueue. Batching it means making the submit a pure function
of its request first, which is a rewrite of the RPC.

**Exit criterion:** on the same cell, `enqueue_evals_isolated` and
`seed_task_statuses_batch` applied/s well below the submission rate, and
`legion_store_coalesce_batch_size{path="eval_enqueue"}`'s `le="1"` bucket
well below its `_count`.

## 7. Testing

- `tests/eval_regen_failover.rs`: a batch of one is indistinguishable
  from a single claim and an empty queue returns immediately; priority
  order is preserved across a batch; the fused batch write settles every
  member and claims the next batch; an unobserved member stays
  `in_flight` while its siblings settle, and `requeue_in_flight_evals`
  recovers it.
- The intake group commit is tested in `src/storage/store.rs`: a batch of
  one is asserted to be the single enqueue it replaced (same row, same
  priority, same claim order); a request that cannot be applied fails
  alone while its batch-mates commit; a batch preserves
  `priority DESC, id ASC` across mixed priorities and arrival order;
  concurrent submitters each get their own eval exactly once; the batched
  status seed writes what N single seeds would, duplicates included; and
  concurrent `idempotency_record` calls for one key have exactly one
  winner whose response the losers can read. `tests/raft_seam.rs`
  classifies `enqueue_evals_isolated` (scheduler) and
  `seed_task_statuses_batch` (status). Like everything else here, the
  round-trip saving is **structurally unmeasurable locally**.
- `EvalBatchLimits::share_of` is unit-tested in `store.rs` at the
  boundaries that matter: a shallow queue must yield exactly one, a deep
  one must be capped, and degenerate inputs must not produce a zero-sized
  batch.
- `tests/eval_regen_failover.rs`: the fused call finishes one eval and
  claims the next; the queue draining returns `None`; a delayed
  `Released` goes back to pending and does not re-claim itself; batched
  enqueue creates every row and each stays independently claimable.
- `tests/raft_seam.rs` classifies both new ops onto the scheduler lane —
  the guard that fails the build if a replicated op is unclassified. It
  caught both of these during development, which is the point of it —
  and phase 4's `claim_evals` /
  `complete_evals_and_claim_next` are classified the same way.
- The local SLO gate is a **no-regression** check only: `replicated`
  short-circuits when raft is off, so a single-node run cannot show this
  win at all. Correctness stayed 1.000 across all eight scenarios.
- Phase 4 is tested as an **equivalence** against the code it replaced,
  which is kept verbatim as the oracle: the cached free pool must equal
  the row walk's across an empty node, amount-typed, item-typed and
  mixed-kind allocations, a zero-amount demand, both `drop_own` modes, and
  an unreadable row — cold cache and warm. Plus a drift battery that
  drives real inserts, a removal that bumps only the job-config epoch, an
  in-plan eviction and a wholesale job removal through the public store
  API, re-asserting the equivalence after each. `src/resources.rs` carries
  the aggregate's own properties (add/remove are inverses; item counts are
  a multiset, not a set union).

  win at all. Correctness stayed 1.000 across all eight scenarios. This
  holds doubly for phase 4 — with raft off, a batched claim of N and N
  single claims execute the same SQL in the same process, so the batching
  win is **structurally unmeasurable** locally.

## 8. Rollout

Phases 1-3: no config, no migration, no wire change — the eval queue
schema and the op semantics are unchanged; only how many quorum rounds
they cost changes. A mixed-version cluster is fine: the ops are
per-leader, and a follower applies changesets either way.

Phase 4 adds one table (`node_alloc_usage`) in an additive migration with
no backfill, owned by the scheduler lane. Version skew is safe in both
directions because the table is a validated cache: a manager binary
without the feature neither reads nor writes it, so a leader on either
side of an upgrade finds the fingerprints stale and rebuilds.

**Exit criterion:** at 5,000 nodes, `cell_summary.py --lanes` shows
scheduler-lane rounds per eval at or below 2.5 (from ~4), and throughput
above the 80.35 alloc/sec phase-3 baseline. If rounds fall and throughput
does not, the lane is not the ceiling either and the next suspect is
`apply` itself (27.9 ms/eval, 67% of plan time).

**Phase 4 exit criterion:** rerun cells G and H from
[../aws-phase4-findings.md](../aws-phase4-findings.md).
`complete_evals_and_claim_next` should run **well below the eval rate**
(`complete_and_claim_next` was 44/s at ~44 evals/s), and the 3.4x
`web_fleet` / `dense_fleet` throughput gap should narrow. No new metric is
needed: dividing `legion_evals_processed_total` by
`legion_eval_claim_total{outcome="claimed"}` gives evals per claim round,
and `legion_raft_ops_total{lane="scheduler",op=…}` gives the same for the
terminal write. Two failure shapes to watch for and what each means:

- **Batch size stays at 1.** The queue never gets deeper than
  `evalThreads`, so the eval loop is not the constraint at all and the
  submission path is.
- **Rounds fall, throughput does not.** The scheduler lane was not the
  ceiling either, and `apply` is what is left — 354 ms of a 381 ms plan
  total at 100,000 allocations, which is the target the same findings
  already name.

Also check `legion_eval_claim_total{outcome="empty"}` has not risen from
its 0.3/s: the notifier issues one permit per claimable eval, and a batch
that took more than its share would leave woken peers with nothing, which
is the regression the fair-share sizing exists to prevent.
