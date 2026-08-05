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

## 3. Batch the queue's own bookkeeping; leave plan-apply alone

- **The call:** phase 4 batches exactly two things — the claim
  (`Store::claim_evals`) and the terminal write
  (`Store::complete_evals_and_claim_next`). A worker takes N evals through
  one cycle, but still runs each handler serially and still lets each eval
  apply its own plan in its own op. No plan is merged with another.
- **Alternatives.** (a) The full claim-N / plan-N / **apply-N** / complete-N
  that [aws-phase4-findings.md](../aws-phase4-findings.md) asks for: hold
  every plan back and commit them together under per-plan savepoints, the
  way `submit_and_apply_plans` already does for concurrent submitters.
  (b) Batch only the completion+claim and leave the initial claim alone.
  (c) Do nothing here and attack `apply` directly, which the same findings
  name as the ceiling at 100,000 allocations.
- **Trade-off.** Phase 4 measured `apply` at 354 ms of a 381 ms plan total
  (93%) on the saturated `dense_fleet` cell, and this change does not touch
  it. If apply is the real ceiling, batching the queue moves the number less
  than the amortization curve suggests.
- **Why (correctness, at the same risk budget as entry 2).** (a) is a much
  larger change than it looks. Handlers do not *produce* a plan and hand it
  back; they write as they go — `handle_job_update` clears a restart marker,
  removes allocations, schedules, then enqueues a follow-up, each its own
  replicated op, and several of them cross lanes. Deferring apply means
  restructuring every handler to be a pure function of a snapshot, which is
  a scheduler rewrite, not a loop change. It also fights the mechanism that
  already exists: the D5 group commit in `coalesce.rs` batches
  **concurrent** applies, and the cheaper the claim/complete rounds get, the
  more workers sit in apply simultaneously — so this change *feeds* the
  existing apply batching rather than duplicating it. Serializing a worker's
  applies inside one commit would take work away from it. (b) leaves the
  first claim of every worker's life unbatched for no reason; the two share
  one body. (c) is the right next move and is unblocked by this — it is not
  an alternative to it. The honest framing: this takes the
  `complete_and_claim_next` traffic phase 4 measured at 44/s down to 44/N,
  and leaves the apply question exactly where phase 4 left it.

## 4. The batch is a fair share of the live queue, sized in code, not config

- **The call:** a worker claims `min(pending / evalThreads, 16)` evals, at
  least one, and never waits for the batch to fill. No config knob.
- **Alternatives.** (a) A fixed batch size (claim up to 16 always).
  (b) An `evalBatchSize` manager-config field. (c) A short linger window
  (a few ms) so a batch can fill before the worker commits to it.
- **Trade-off.** The share is computed from a `COUNT(*)` of pending rows
  that includes not-yet-ready ones, so it is an upper bound and a batch can
  come back short. That costs one wasted claim attempt.
- **Why (performance, and simplicity).** (a) is a throughput *regression*
  waiting to happen on exactly the case that matters, and phase 4 is what
  proves it matters: 16 eval threads beat 4 by 23.7% once the wake herd was
  fixed, so concurrency across workers is now a real lever. A greedy claim
  gives it back — one worker takes a 16-eval burst and serializes what 16
  workers could have run at once. It would also undo entry 1: the notifier
  issues one permit per newly-claimable eval, so a worker that swallowed a
  burst leaves its woken peers claiming nothing, which is precisely the
  empty-claim traffic that fix removed. The fair share is what makes
  batching safe to turn on unconditionally. (c) is the classic
  group-commit trick and is
  wrong here: this cluster is idle far more often than saturated, and a
  linger buys throughput nobody is asking for with latency everybody
  notices — dropping it is also what makes "a batch of one costs exactly
  what one eval costs today" literally true rather than approximately.
  (b) is the tempting one, and it is a knob whose correct value is a
  function of two things the loop already knows (queue depth and
  `evalThreads`); shipping it would ask operators to hand-tune a derived
  quantity, and every knob is a support burden and a migration. The one
  number left arbitrary is the cap of 16, and it is chosen against the
  *cost* of a batch (how much already-handled work a crash re-runs), not
  its benefit.

## 5. The fused claim is handled before the leadership gate

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

## 6. A panicked handler writes nothing

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

## 7. The batch's terminal write is all-or-nothing, and an unobserved eval is simply left out

- **The call:** `complete_evals_and_claim_next` takes only the outcomes the
  worker actually observed. An eval whose handler panicked, or that a
  mid-batch leadership loss stopped the worker reaching, is omitted — it
  stays `in_flight`. And the write itself is one changeset: if it is
  rejected, *no* member of the batch is settled and all of them stay
  `in_flight`.
- **Alternatives.** (a) Per-eval savepoint isolation inside the batch, so a
  bad completion cannot take its siblings down with it — the shape
  `submit_and_apply_plans` uses for plans. (b) Abandon the rest of the batch
  the moment one handler dies, rather than carrying on. (c) Write each
  completion as its own op, batching only the claim.
- **Trade-off.** Two, both real. A completion that fails to write drags its
  batch-mates' settled outcomes back with it, so up to 16 already-handled
  evals get re-run after recovery — wasted work, though not duplicated
  effects, since handlers are idempotent convergence steps. And a batch
  member's outcome is not durable until the whole batch finishes, which
  widens the crash window from one eval to sixteen.
- **Why (correctness, cheaply).** Omission is the only answer that cannot
  lose work — it is entry 6's rule applied per member instead of per loop
  iteration, and it lands the batch in the state `requeue_in_flight_evals`
  already recovers rather than inventing an outcome (a *failed* eval is
  never retried, so a fabricated failure silently drops acknowledged work).
  All-or-nothing then costs nothing beyond that: a rejected batch write
  leaves every row in exactly the state a crashed worker leaves them in,
  which is a path that already exists and is already tested. (a) buys
  isolation the failure mode does not need — completions are a row update
  plus an event insert on well-formed data, not the optimistic-concurrency
  race that makes plans reject — and it would add a second, subtly
  different partial-commit path to reason about. (b) throws away sibling
  outcomes we already hold for no gain. (c) gives back most of the win: the
  terminal write is the round phase 4 measured at 44/s.

## 8. Batched enqueue takes a request struct, not N arguments

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

## 9. Local benchmarks are reported as a no-regression check only

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
- **Batching (entries 3, 4, 7) inherits this unchanged.** Batching divides
  raft *rounds*, and on a single node there are no rounds to divide — a
  batched claim of N and N single claims execute the same SQL in the same
  process. The performance claim for batching is **unverified**; what the
  local suite verifies is that batching did not change what the queue does.
