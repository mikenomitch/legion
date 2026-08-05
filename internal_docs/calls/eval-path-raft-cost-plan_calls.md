# Judgement calls: eval-path-raft-cost-plan

Plan: [`../plans/eval-path-raft-cost-plan.md`](../plans/eval-path-raft-cost-plan.md)

Ranked by project impact; the number is the rank.

## 1. The apply-time free pool is a validated cache, not a maintained total

- **The call:** `Store::free_pool_for_node_tx` now reads a node's total
  allocation usage from a new `node_alloc_usage` row, and trusts that row
  only when a **fingerprint of the live allocation rows** — the node's
  allocation epoch, the row count, and the sum of the allocation ids —
  still matches the one stored beside it. A mismatch rebuilds from the
  rows, exactly as before. The plan-apply transaction refreshes the row at
  the end of a successful apply, and only if the live fingerprint is
  precisely the one it predicted (what it read, plus the allocations it
  itself inserted).
- **Alternatives.** (a) A genuinely denormalized per-node capacity row,
  maintained by every path that inserts or removes an allocation. (b) A
  SQL aggregate (`SUM(json_extract(...)) … GROUP BY node_id`) that avoids
  materializing rows and JSON into Rust. (c) Maintain the row from SQLite
  triggers on `allocation`, which fire for any DML whatever the caller.
  (d) Memoize parsed `resources` per allocation id in process memory.
- **Trade-off.** The fingerprint is still an O(rows-on-node) index scan,
  so this does not make the apply cost *flat* in cluster fill the way (a)
  would — it makes the per-row constant roughly two orders of magnitude
  smaller (a b-tree walk over `idx_allocation_node_id` versus a row read
  plus a `serde_json` parse plus a normalize plus a `ResourceRequest`
  build). And the aggregate re-associates a float sum, so the number can
  differ from the row walk's by a rounding step. It is also a new
  replicated table, which is a new thing a future migration has to think
  about (a rebuild of `allocation` that renumbers ids must clear it).
- **Why (performance, without buying it with correctness).** The measured
  problem is that the serialized plan-apply commit does work proportional
  to how full the cluster is while the work per eval stays constant: at
  5,000 nodes, ~44 placements read ~440 allocation rows at 50k allocations
  live and ~1,760 at 200k, and throughput fell 736.1 → 456.6 → 272.9
  alloc/s across those fills — about 1.6x per doubling, while plan time
  grew only 22%.
  **A free-capacity number that drifts high overcommits a node**, which is
  a worse failure than slow scheduling by a wide margin, so the design
  question was not "how fast" but "what happens when the fast path is
  wrong". (a) answers that with discipline: every current and *future*
  allocation writer must remember the update, and a missed one is silent
  overcommit with no test that would catch it — and the audit needed for
  that confidence is not available here. The evidence is concrete: two
  removal paths (`remove_allocation_by_id`, `remove_allocs_for_job_config`)
  already bump only the job-config epoch and not the node's, so even the
  invariant that *is* maintained today is not total. (c) fixes exactly
  that by moving maintenance below the callers — but a changeset-replicated
  schema with no triggers anywhere in it is the wrong place to introduce
  the first one on the scheduler's hot table, since whether
  `sqlite3changeset_apply` re-fires them decides between "works" and
  "every replica aborts", and that cannot be settled here. (d) has the
  same hazard in a nastier form: SQLite reuses a rowid once the highest
  row is deleted, so a memo keyed by allocation id can hand a new
  allocation the resources of a deleted one.
  The validated cache keeps the *asymptotics* of (b) — the residual scan
  is O(rows on the node) either way — while beating (b)'s constant, and it
  makes drift structurally impossible rather than merely unlikely: the
  value is checked against the rows on every read, so the worst a stale,
  wrong, or absent row can do is cost one rebuild. Maintenance is an
  optimization; validation is the guarantee. That is also what makes it
  safe under version skew: an older manager binary neither reads nor
  writes the table, so a leader on either side of an upgrade simply finds
  the fingerprints stale.
  Measured locally on the rebuild itself: 15.573 → 8.635 µs per pool at 10
  allocations/node, 37.764 → 9.358 µs at 40 — i.e. over a 4x change in
  fill the old path grows 2.4x and the new one 1.08x, which is the slope
  the AWS ladder was measuring.
- **The invariant, and how it fails.** `node_alloc_usage.usage_json` is the
  aggregated usage of every allocation on `node_id` as of the
  `(alloc_epoch, alloc_count, alloc_id_sum)` beside it. It is *never*
  read without re-deriving that fingerprint from `allocation`, so the
  failure mode is bounded to "the fingerprint matched but the aggregate is
  wrong", which requires the row content to change under a preserved id
  set — i.e. an in-place `UPDATE` of `allocation.resources`, which nothing
  does today. A future writer that starts mutating `resources` in place
  must clear this table in the same transaction.
- **Surface.** None. No jobspec, CLI, or API change; identical placement
  decisions. The one thing an operator can observe is the table itself,
  and one known wart: a node removed from the cluster leaves its row
  behind (`remove_node` is a control-lane op and may not write a
  scheduler-lane table), inert and ~100 bytes.

## 2. Meter eval wakes by work, not by the eval table moving

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

## 3. Refresh the read model from the changeset delta, giving up self-healing

- **The call:** after a committed changeset, patch `allocations_by_node`
  from the allocation rows the changeset *named* — one primary-key read
  per created or changed row — instead of reloading every touched node's
  entire allocation list. `cache_refresh_allocation_scopes` stays, as the
  fallback and as the path for every non-changeset caller.
- **Alternatives.** (a) Keep the full reload and make it cheaper still
  (one connection and one cached statement, which
  an earlier commit on this branch already did). (b) Rebuild the
  allocation from the changeset's own column values and never touch
  SQLite. (c) Cache the per-node lists behind an epoch so an unchanged
  node's reload can be skipped. (d) Leave it: the refresh is off the
  timed plan path, so it is invisible in the metrics.
- **Trade-off — the property being sold.** The full reload is
  **self-healing**: whatever the in-memory model held, it is overwritten
  by the truth in SQLite, so drift cannot persist. A delta can only ever
  be as right as the entry it patches, and a read model that drifts from
  committed state makes the scheduler plan against a fleet that does not
  exist — placements onto full nodes, or nodes wrongly believed full and
  therefore never written to again. That last case is self-sustaining:
  no later changeset names a node nobody schedules onto, so a delta alone
  would never revisit it. Three things bound the loss, and they are the
  reason this was worth doing: a **per-node fallback** to the full reload
  whenever the delta contradicts the cache (a delete that finds no row,
  an insert that finds one, an in-place update that does not), a
  **whole-changeset fallback** whenever a record cannot be classified or
  names a row committed state does not have, and a **round-robin scrub**
  that reloads one node in full per projected changeset, so any drift is
  repaired within one sweep of the fleet instead of never. The honest
  summary: drift is no longer *impossible*, it is *bounded* — by one
  fleet sweep in time, and by a fallback in every case the projection can
  actually see.
- **Why (performance).** The old cost scaled with how FULL the cluster
  is while the change being projected did not. On the AWS `dense_fleet`
  shape (5,000 nodes, 16 eval threads, no cell driver-bound) a plan
  touching ~44 nodes reloaded ~440 rows at 50,000 allocations and ~1,760
  at 200,000 — to learn about ~44 allocations the changeset had already
  named exactly. Throughput across that ladder was 736.1 → 456.58 →
  272.91 alloc/s, ~1.6x worse per doubling, while plan-phase timers grew
  only 22% over the last step: the missing time is here, outside those
  timers, on **every replica**. (a) divides the constant and keeps the
  scaling, which is the actual defect. (b) saves the primary-key reads
  but has to reconstruct an `Allocation` from a record that carries only
  the columns that changed, and re-derive the `job_id` join — more code
  in the one place where being subtly wrong corrupts capacity, for a
  saving on top of an indexed lookup. (c) is a bigger structure (a
  per-node epoch mirror maintained everywhere allocations are written)
  that would still reload whole lists whenever a node did change. (d) is
  what the last three campaigns did.
- **Surface.** None for the user. Two `warn!` lines are new, and both
  mean the same thing — the cached read model disagreed with committed
  state — naming the allocation ids or the nodes involved. Neither is
  expected in a healthy cluster; either one appearing repeatedly is the
  signal that the delta's premises are wrong somewhere.
- **What is left on the table.** Reprojection (`reproject_model_nodes`)
  still recomputes each touched node's free pool from its whole
  allocation list, so a fill-scaled term remains in memory even though
  the SQL one is gone. Measured locally over 44 touched nodes: at 10
  allocations/node the refresh went 832µs → 496µs, at 40 it went 2.16ms →
  1.03ms — about 2x, with the remainder now dominated by that
  reprojection and by the 44 separate per-scope epoch reads. Making
  either incremental is its own change.

## 4. Fuse claim and complete, rather than making the lane parallel

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
  resources. Phase 4 found the real cause — the wake herd in entry 2, which
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

## 5. Batch the queue's own bookkeeping; leave plan-apply alone

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

## 6. The batch is a fair share of the live queue, sized in code, not config

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

## 7. The fused claim is handled before the leadership gate

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

## 8. A panicked handler writes nothing

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

## 9. The batch's terminal write is all-or-nothing, and an unobserved eval is simply left out

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

## 10. Batched enqueue takes a request struct, not N arguments

## 11. Local benchmarks are reported as a no-regression check only
