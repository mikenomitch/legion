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

## 3. Fuse claim and complete, rather than making the lane parallel

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
