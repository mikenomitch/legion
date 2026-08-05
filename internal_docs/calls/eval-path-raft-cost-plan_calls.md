# Judgement calls: eval-path-raft-cost-plan

Plan: [`../plans/eval-path-raft-cost-plan.md`](../plans/eval-path-raft-cost-plan.md)

Ranked by project impact; the number is the rank.

## 1. Meter eval wakes by work, not by the eval table moving

- **The call:** count the evals a changeset makes *claimable* and wake
  exactly that many workers with `notify_one`, instead of calling
  `notify_waiters` whenever any `eval` row changed.
- **Alternatives.** (a) Keep the broadcast and make an empty `claim_eval`
  cheap — serve it from a local read instead of a quorum round.
  (b) Have workers back off when they lose the claim race, so the herd
  self-limits after the first round. (c) Leave it and cap `evalThreads`
  low, which is what the phase-2 and phase-3 numbers effectively did.
- **Trade-off.** A targeted wake can under-notify where a broadcast
  cannot: if the wake count is ever wrong, an eval could sit pending with
  every worker parked. That risk is what the broadcast was buying.
- **Why (performance).** Every eval crosses the projection at least three
  times — enqueued, claimed, completed — and only the first is work for a
  parked worker. The broadcast handed all N workers a `claim_eval` for
  each of the three, and a claim is a scheduler-lane quorum round whether
  or not it finds anything, so N-1 of every N were pure waste that scaled
  with the pool. Phase 3 measured it directly: 16,678 scheduler-lane raft
  ops at 4 threads against 47,096 at 16, for the same 25,002 allocations,
  with throughput *falling* from 203.19 to 164.72 alloc/sec. (a) makes the
  waste cheaper without making it smaller and leaves the pool doing N
  wakeups per unit of work. (b) is a heuristic layered over a bug whose
  fix is exact. The under-notification risk in the trade-off is bounded
  rather than accepted: `wait_for_next_eval` sizes its park by
  `next_pending_eval_delay`, which is zero while any eval is ready, so a
  worker that misses a permit re-checks instead of sleeping — the wake is
  an optimization over a poll that was always correct.
- **Surface.** None for the user, except that `evalThreads` now buys
  throughput instead of costing it. The new
  `legion_eval_claim_total{outcome}` is what makes the waste visible:
  a large `empty` share means workers are being woken for work that does
  not exist.

## 2. Fuse claim and complete, rather than making the lane parallel

- **The call:** halve the scheduler lane's per-eval quorum rounds by
  fusing the terminal write with the next claim
  (`Store::complete_and_claim_next`), leaving the lane's single
  `pipeline_loop` thread exactly as it is.
- **Alternatives.** (a) Parallelise the lane pipeline so rounds overlap.
  (b) Stop replicating the eval queue, as Kubernetes does — keep it
  in-memory per scheduler and replicate only the placement.
  (c) Raise `evalThreads` and accept the round count.
- **Trade-off.** The lane is still serial, so this buys a constant factor
  (~2x on the queue's own overhead), not a scaling change. If the ceiling
  is the lane's *throughput* rather than its round count, this moves the
  number less than hoped — which the exit criterion in the plan is
  written to detect.
- **Why (performance, at a risk the evidence supports).** (c) looked
  refuted: phase 2 measured throughput *falling* from 4 → 16 eval threads,
  which was read at the time as threads and lanes being different
  resources. Phase 4 found the real cause — the wake herd in entry 1, which
  made every extra thread cost a wasted quorum round — so (c) was never
  tested on its merits. It is worth retrying now that the herd is gone. (b) is the real fix and is
  also the largest possible change to Legion's durability story — the
  replicated queue is what makes acknowledged work survive a failover
  (JC-41), and phase-3's own data does not justify spending that. (a) is
  a genuine option but it is a change to the raft driver's concurrency
  model, where a bug is a correctness bug rather than a slow cluster.
  Fusing is local, reversible, and provable by unit test, and it attacks
  a cost that is pure overhead: the pair brackets a loop in which the same
  worker immediately asks for more work. Do the cheap, safe half first and
  let the measurement decide whether the expensive half is needed.

## 3. Size the read pool from the readers, not from the cores

- **The call:** `read_only_conn_pool_size` takes `evalThreads` and returns
  `evalThreads + 4`, capped at 16, instead of
  `available_parallelism().max(4)`. On the campaign's 4-vCPU managers that
  is 4 connections today, 8 at the default and 16 at `evalThreads = 16`.
- **Alternatives.** (a) Leave it at the core count — the pool is not the
  throughput gate, so why touch it. (b) A flat constant (8 or 16),
  independent of everything. (c) A new `readConnections` config knob.
  (d) Raise it aggressively (32, 64) on the theory that the pool *was*
  gating the plan path.
- **Trade-off.** `evalThreads` now moves two things at once: planner
  concurrency and read-pool width. An operator who raises it to 64 gets 16
  connections rather than 64, so the coupling is bounded but it is real, and
  it has to be stated wherever `evalThreads` is documented. The cap also
  means a very wide `evalThreads` no longer gets one connection per worker —
  deliberately, see below.
- **Why (performance, honestly bounded).** (d) was the tempting one and the
  evidence does not support it. The plan path was traced end to end first:
  no read connection is held across a quorum round, across an `.await`, or
  across `build_plan` — planning reads the in-memory `SchedulingReadModel`,
  `plan_and_apply_internal` holds no guard when it calls
  `submit_and_apply_plan_with_candidates`, and the apply runs on the WRITE
  connection inside `replicated()`. Every acquisition is one leaf store
  method wide. **The pool cannot serialize two planners against each other**,
  so the 4-cores/4-connections/~4-effective-planners coincidence is a
  coincidence, and the archives already have a better explanation for the
  last term: 347 ms of coalescer follower wait against a 138 ms serialized
  commit. That rules out (d) and most of the reason to care at all.
  What survives is that the *basis* was wrong, which is (a)'s problem.
  Core count justified itself on "these reads are CPU-bound page-cache
  hits", and a saturated 5,000-node control plane measured **8% CPU across
  four cores** — the readers are latency-bound, a cache miss is a `pread`
  holding the mutex through an I/O wait, and
  `cache_refresh_allocation_scopes` holds one connection across ~44 per-node
  queries after every apply on every replica. Worse, the heartbeat body
  (`build_desired_allocations` and everything after it) runs **inline on a
  tokio worker thread**, so a heartbeat queueing for a connection parks a
  runtime thread — and a pool sized at the core count is exactly a pool that
  fills at the same moment the runtime does. (b) fixes the basis but throws
  away the one signal we have about how many concurrent readers exist;
  `evalThreads` is that signal, already in the config, which is also why
  (c) is unnecessary — a knob nobody can size from first principles is worse
  than a derivation from one they already set.
- **Why capped at 16 (simplicity, and memory).** `SQLITE_CACHE_SIZE_KIB` is
  -65536, a **64 MiB page cache per connection**. It is a cap that SQLite
  fills lazily, so a small cluster never reaches it — but at 200,000
  allocations every connection independently pulls in the same hot
  `allocation` pages, the caches do not share, and the worst case becomes
  the real case. 16 connections is a 1 GiB ceiling, asserted by the unit
  test so that raising the page cache forces the cap to be reconsidered
  rather than silently multiplying it. A manager that wants more read
  concurrency than this wants a smaller per-connection cache first.
- **Surface.** None directly. Indirectly, `evalThreads` is now also a
  read-pool control, which the `evalThreads` plan
  ([`../plans/eval-thread-default-plan.md`](../plans/eval-thread-default-plan.md))
  carries as an open question, since its Phase 0 sweep now has a memory
  dimension. `legion_store_read_lock_wait_seconds` remains the way to tell
  whether the pool is actually short: with it sized from readers rather than
  cores, a nonzero wait means a reader is holding a connection too long, and
  the fix for that is fewer or shorter reads.

## 4. The fused claim is handled before the leadership gate

- **The call:** a worker holding an eval handed back by the previous
  fused write processes it even if `is_raft_leader` now reports false;
  the gate only guards a *fresh* claim.
- **Alternatives.** (a) Check leadership first and drop the carried eval,
  letting `requeue_in_flight_evals` rescue it. (b) Do not carry an eval
  across loop iterations at all — i.e. do not fuse.
- **Trade-off.** A worker can start an eval microseconds after losing
  leadership. Its terminal write then fails with `NotRaftLeader`, the row
  stays `in_flight`, and recovery happens on the next leadership change.
- **Why (simplicity, and no new failure mode).** We *held* leadership when
  the row was claimed, so this is the same window the unfused loop
  already had between `claim_eval` returning and the handler running —
  not a new one. (a) looks safer and is worse: it strands a row that this
  node just claimed, guaranteeing the stall it was trying to avoid, on
  every leadership flap. The recovery path is unchanged and already
  tested, so the honest framing is that fusing does not widen the window
  it inherits.

## 5. A panicked handler writes nothing

- **The call:** when `spawn_blocking` returns `Err` (handler panicked or
  was cancelled), the loop logs and writes no outcome, leaving the eval
  `in_flight`.
- **Alternatives.** (a) Mark it failed. (b) Release it for retry.
- **Trade-off.** The row waits for `requeue_in_flight_evals` on the next
  leadership change instead of being resolved immediately.
- **Why (correctness over tidiness).** The outcome is genuinely unknown —
  the handler may have completed its side effects before panicking. (a)
  is the dangerous one: failed evals are never retried, so inventing a
  failure for work that may have half-run is exactly how acknowledged
  work vanishes (the bug class JC-41 and the 2026-07-24 adversarial pass
  both fixed). Writing nothing is the only option that cannot lose work,
  and it matches what a crashed worker already produces.

## 6. Batched enqueue takes a request struct, not N arguments

- **The call:** `enqueue_evals(Vec<EvalRequest>)` with a struct carrying
  kind/job/config/node/priority/delay, plus an `EvalRequest::for_job`
  constructor for the common case.
- **Alternatives.** (a) A variadic-ish `enqueue_evals(&[(kind, job_id,
  config_id, node_id)])` tuple slice. (b) Keep per-row enqueues and batch
  only inside `regenerate_scheduling_evals` with a private helper.
- **Trade-off.** One more public type in `store.rs`.
- **Why (extensibility).** The existing single-row API already takes six
  positional arguments, four of them `Option`, and a tuple slice would
  make every call site an unlabelled six-tuple. The struct also gives
  future callers — the per-node loops in `server.rs`, which are the other
  half of the storm — an obvious thing to build. (b) would have fixed
  today's four loops and left the next one to rediscover the problem.

## 7. Local benchmarks are reported as a no-regression check only

- **The call:** the PR quotes the local SLO gate as "no regression" and
  explicitly declines to claim a throughput win from it.
- **Alternatives.** (a) Quote the batch-burst delta (72.6 → 94.6
  evals/sec) as the improvement. (b) Skip local benchmarks entirely since
  they cannot measure the change.
- **Trade-off.** The PR ships a performance change with no performance
  number, which is unsatisfying and against the letter of the
  "scheduler changes ship with before/after numbers" rule.
- **Why (measurement honesty).** `Store::replicated` short-circuits when
  raft is off, so on a single-node local run the fused op and the unfused
  pair execute *identical* code — the win is structurally unmeasurable
  there, and any delta is laptop noise. (a) would be quoting noise as a
  result, which is precisely the failure mode that invalidated the first
  two AWS campaigns. (b) throws away the correctness signal, which is the
  part local runs *can* give: correctness_ratio stayed 1.000 across all
  eight scenarios. The real number needs the rig, and the plan's exit
  criterion says so.
