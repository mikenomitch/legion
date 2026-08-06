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
- **Follow-up (2026-08-06): the fingerprint could not see a POLICY
  change.** The aggregate is rows run through
  `normalize_task_resources`, which folds in the manager's
  `taskResourceDefaults` — and the row-set fingerprint is pure row
  identity, so an operator raising a default and restarting kept every
  cached row valid while the walk would have produced different numbers:
  understated usage on every quiet node, indefinitely, i.e. silent
  overcommit (audit finding, 2026-08-06). Each row now also stores
  `ResourcePolicy::fingerprint()` — the canonical rendering of the
  defaults and required custom resources, deliberately the readable
  string rather than a hash so a rebuild storm names the policy that
  wrote it — and the read requires it to equal the running policy's.
  NULL (a pre-column row, or one written by a rolled-back binary whose
  INSERT names only the original columns) never matches, so both skew
  directions degrade to one rebuild per node. The alternative — deleting
  the table at startup on policy change — was rejected because the table
  is replicated scheduler-lane state and a local startup DELETE would
  diverge replicas.
- **Surface.** None. No jobspec, CLI, or API change; identical placement
  decisions. The one thing an operator can observe is the table itself,
  and one known wart: a node removed from the cluster leaves its row
  behind (`remove_node` is a control-lane op and may not write a
  scheduler-lane table), inert and ~100 bytes.

## 2. Job intake group-commits per lane; the control-lane submit does not

- **The call:** the two *projection* halves of the job-submit composite —
  the scheduler-lane placement eval (`Store::enqueue_eval*`) and the
  status-lane `task_status` seed — each get their own `coalesce.rs`
  coalescer, so concurrent submitters pay one quorum round per lane per
  *batch* instead of one per lane per *job*. Each request keeps its own
  result and its own error inside a savepoint. The control-lane
  `handle_job_submit` is deliberately left unbatched.
- **Alternatives.** (a) Batch all three halves, `handle_job_submit`
  included. (b) One coalescer for the whole composite. (c) A short linger
  window so batches fill before the leader commits. (d) An all-or-nothing
  batch that re-runs every request singly when any one fails, instead of
  per-request savepoints. (e) Leave intake alone and keep attacking the
  apply path.
- **Trade-off.** Two of the three rounds, not three: a submitted job still
  costs one control-lane quorum round of its own, so the intake floor is
  1 round/job rather than 0. And a batch is only as large as the client's
  actual concurrency — a serial `for job in *.ncl; legion job run` loop
  batches nothing and is exactly as fast (or slow) as before.
- **Why (performance, without moving a correctness boundary).** The AWS
  5,000-node run measured the intake path as the top of the profile, and
  all three of its rounds move in lockstep with the submission rate:
  `enqueue_eval_after_delay` 19.1 applied/s, `handle_job_submit` 16.6/s,
  `seed_task_statuses` 16.6/s, against an overall 21.9 evals/s — while
  `complete_evals_and_claim_next`, which entry 8 batched, sat at 4.2/s
  (0.19 rounds/eval). The intake path simply never got the treatment the
  eval loop's own bookkeeping got, and the driver submits 32 jobs
  concurrently, so the concurrency to batch is really there.
  (b) is not implementable: a lane is a separate raft group with its own
  single-threaded pipeline, so one batch physically cannot span two of
  them — three lanes means at most three coalescers, and that constraint
  is what shapes this entry.
  (a) is where the judgement is, and it is a no. `handle_job_submit_apply`
  derives the next config version from `get_job_config_optional`, which
  reads *committed* state through a read connection; today one submit
  commits before the next reads, because each is its own replicated op.
  Put two submits of the same job in one uncommitted batch transaction and
  both read the same latest version and both compute version N+1 — the
  file's own long-standing "there is a race condition here with the
  version" note, promoted from rare to routine. The composite is also not
  one store call: `submit_rendered_job` interleaves
  `clear_job_stopped_status`, `replace_job_kv_references`,
  `seed_keyed_instances`, `previous_job_config_id` and rollout creation
  between the control write and the eval enqueue, several of them reading
  what the control half just committed. A coalescer needs its batch runner
  to be one function of the request; making that true here is a rewrite of
  the submit RPC, not a storage change. Left as open work in
  [`../TODO.md`](../TODO.md).
  (c) is rejected for the same reason entry 9 rejects it, and the
  coalescer's own header says so: this cluster is idle far more often than
  it is saturated, and a linger buys throughput nobody asked for with
  latency everybody notices. A lone `legion job run` on an idle cluster
  goes straight to the commit slot, leads a batch of one, and takes the
  single-op path unchanged — asserted by
  `a_batch_of_one_enqueue_is_the_single_enqueue_it_replaced`.
  (d) is cheaper to write and wrong at the wrong moment: one job whose
  eval cannot be enqueued would cost its 31 batch-mates a full redo, and
  the redo is N quorum rounds — the exact cost this change exists to
  remove. Savepoints make a bad request fail alone, which is what
  `submit_and_apply_plans` already does per plan.
  (e) ignores what the profile says is on top.
- **What a batch may NOT do, and why it still cannot.** `claim_eval`
  orders strictly by `priority DESC, id ASC`. The coalescer's queue is
  FIFO and its leader drains from the front, so a batch inserts in arrival
  order (`id ASC` preserved) and each row still derives its own priority
  inside its own savepoint — an explicit `--priority`/`--asap` override,
  else the job's band. Nothing about the queue's semantics is batched.
- **Failure and leadership.** A typed `NotRaftLeader` on the batch means
  this node does not lead that lane and nothing was proposed, so every
  request re-runs through the single-op forwarding path — slower, but it
  keeps the wire op every manager version understands *and* keeps the
  typed rejection in each caller's own error chain, which is what the
  submit path's standing-split tolerance matches on. Any other batch error
  is reported to every member rather than re-run: unlike a local
  rejection, an error out of `replicated` can straddle a commit, and
  re-running would risk enqueueing the batch twice.
- **Surface.** `schedulerSettings.job_intake_coalescing = false` (default
  `true`) restores one op per submitter on both lanes — one switch, not
  two, because they are halves of one composite and an operator taking the
  change out wants all of it out. Batch caps are constants (64 each), not
  config, for the reason entry 9 gives. The coalescer's existing metric
  families gain two `path` values, `eval_enqueue` and `task_status_seed`;
  `legion_store_coalesce_batch_size{path="eval_enqueue"}`'s `le="1"`
  bucket against `_count` is how an operator sees whether batching engaged
  at all.
- **Unverified.** `Store::replicated` short-circuits when raft is off, so
  a single-node run executes the batched and unbatched forms as identical
  code and cannot show the round-trip saving. Correctness is tested
  locally; the throughput claim needs the AWS rig. Same reasoning as the
  last entry in this file.

## 3. Meter eval wakes by work, not by the eval table moving

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

## 4. Refresh the read model from the changeset delta, giving up self-healing

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
- **What is left on the table — closed 2026-08-06.** Reprojection
  (`reproject_model_nodes`) recomputed each touched node's free pool from
  its whole allocation list — one JSON normalization per allocation, per
  touched node, on every replica, inside the read-model write lock every
  planner reads through — so a fill-scaled term survived in memory even
  after the SQL one was gone, and the phase-4 ladder's slope did not
  move (~1.65x per doubling before and after, geometric mean). The read
  model now keeps a per-node `NodeUsage` aggregate beside each cached
  list (the in-memory sibling of the `node_alloc_usage` row, sharing the
  same type and equivalence tests): reprojection subtracts the aggregate
  (O(resource dims)), the delta path adjusts it with exactly the rows it
  spliced, and EVERY wholesale list reload — scope refresh, scrub, drift
  fallback, node removal — drops the entry so the next reprojection
  rebuilds it from the list. An absent entry costs precisely the old
  walk, so the memo cannot be less correct than what it replaced, and
  the model-vs-full-reload equivalence test now also asserts every memo
  entry equals its list's re-aggregation (contributor counts included,
  which pool equality alone cannot see). Measured locally
  (`bench_local_reprojection_by_fill`, 44 touched nodes): walk 414µs →
  memo 50µs at 10 allocations/node, 1.92ms → 74µs at 40 — the memo arm
  grows 1.5x over a 4x fill change where the walk grows 4.6x. The 44
  per-scope epoch reads went the same way: one read connection and one
  cached statement for the whole scope set, instead of one acquisition
  and one statement parse per scope.
- **Follow-up (2026-08-06): the full reload could CLOBBER a newer
  delta.** The rejection-bookkeeping full refresh (submitter thread)
  reads committed state, then publishes under the write lock — and a
  delta projection (raft-apply thread) could publish newer state for the
  same node in between, which the older wholesale insert then erased.
  Pre-delta that lost update was healed by the next full reload; with
  deltas it could persist until the scrub reached the node (audit
  finding, 2026-08-06). The refresh now skips, under the write lock, any
  scope whose mirror epoch is already ahead of the one it read — safe
  precisely because what it would have written is provably older. The
  residual window is a concurrent commit that bumps no scope epoch of
  the node (a pure status UPDATE); placements and removals always bump,
  and the scrub still bounds the rest.

## 5. Fuse claim and complete, rather than making the lane parallel

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
  resources. Phase 4 found the real cause — the wake herd in entry 3, which
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

## 6. The coalescer signals its handoff; the leader does NOT run a second batch

- **The call:** replace the coalescer's poll-and-re-contend wait (a reply
  channel plus a `try_lock` retried every millisecond) with one `Mutex` and
  one `Condvar`, so a submitter that loses the election blocks once and is
  woken exactly when the commit it queued behind ends. The leader still
  runs **one** batch and then returns to its own caller.
- **Alternatives.** (a) The leader loops — drain, run, drain again, all
  inside one lock hold — so requests that arrived during a commit are
  picked up with no handoff at all. (b) Leave the wait alone and let the
  next leader take a bigger batch, on the theory that batch size is the
  problem. (c) Shrink `LEADER_RECHECK_INTERVAL` below a millisecond.
- **Trade-off.** It is a concurrency rewrite of a hot path — the class of
  bug that is a wedged cluster rather than a slow one — for a saving that
  is a few milliseconds per commit boundary, not the 347 ms the finding is
  about. It also changes what `legion_store_coalesce_wait_rounds_total`
  counts (poll ticks -> extra commits sat through), so anything reading it
  needs updating; `docs/metrics.md` is updated in the same change.
- **Why (performance, honestly bounded).** (a) is the shape
  [aws-phase4-findings.md](../aws-phase4-findings.md) proposed and it is a
  **regression**. The leader's own request is already answered by the time
  it re-drains, so every extra batch it runs adds a full commit — 138 ms
  measured — to its own caller's latency, in order to save the arrivals a
  handoff they would have led themselves about a millisecond later. At the
  measured 0.32 arrivals per commit that means roughly a third of
  submitters pay 138 ms so that a third of submitters save ~5 ms, and
  unbounded altruism starves whichever thread keeps winning. Prior art
  agrees: RocksDB's write thread elects a *successor* rather than making
  the leader serve a second group. (b) is what the code already does — the
  queue is FIFO and a leader drains all of it, so the next batch is already
  as big as the arrivals allow. **Batch size 1.32 is the arrival rate, not
  a batching failure**, and no timer-free change can raise it; a timer is
  ruled out because a lone submitter has to keep paying nothing. What was
  genuinely broken is the *shape of the wait*: a loser could not block on
  "my result arrived" and "the commit slot is free" at once, because they
  lived behind two different primitives, so it polled — the lock sat free
  until some waiter's next tick, and every waiter burned a wake-up per
  millisecond of every commit. Measured in the new unit test: 600
  re-contention rounds for six submitters behind one 150 ms commit, now 0.
  (c) buys the same milliseconds by spending CPU, and scales the wrong way
  with thread count.
- **Surface.** None in the jobspec or CLI. Two operator-visible things:
  `legion_store_coalesce_wait_rounds_total` should now read 0 on a healthy
  path (it was never 0 before, because it counted polling), and a
  coalescer whose batch runner panics no longer wedges. The old
  `commit_lock` was a `Mutex` held across `run`, so a panicking runner
  **poisoned** it and every later submitter spun at 1 ms forever —
  `try_lock` cannot tell "busy" from "poisoned". The slot is now a flag
  released by an RAII guard on every exit including unwind, covered by
  `a_panicking_leader_does_not_wedge_its_batch_mates`, which hangs
  indefinitely on the old implementation.

## 7. The fused claim is handled before the leadership gate

## 8. Batch the queue's own bookkeeping; leave plan-apply alone

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
- **Why (correctness, at the same risk budget as entry 3).** (a) is a much
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

## 9. The batch is a fair share of the live queue, sized in code, not config

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
  workers could have run at once. It would also undo entry 3: the notifier
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

## 10. A panicked handler writes nothing

## 11. Batched enqueue takes a request struct, not N arguments

## 12. The batch's terminal write is all-or-nothing, and an unobserved eval is simply left out

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
  lose work — it is entry 7's rule applied per member instead of per loop
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

## 13. Size the read pool from its readers, and publish the negative result

- **The call:** `read_only_conn_pool_size` takes `evalThreads` and returns
  `evalThreads + 4`, capped at 16, instead of
  `available_parallelism().max(4)`. On the campaign's 4-vCPU managers that
  is 4 connections today, 8 at the default and 16 from `evalThreads = 12`
  up. **No throughput improvement is claimed.**
- **Alternatives.** (a) Change nothing — the pool is measurably not the
  gate, so leave it. (b) A flat constant (8 or 16), independent of
  everything. (c) A new `readConnections` config knob. (d) Raise it
  aggressively (32, 64) on the theory that the pool *was* gating the plan
  path.
- **Trade-off.** `evalThreads` now moves two things at once: planner
  concurrency and read-pool width, so it has to be documented wherever
  `evalThreads` is. And the honest cost of not choosing (a) is that this
  ships an unmeasured change to a hot-path resource on a structural
  argument — a category the last three campaigns exist to warn about.
- **Why (a) lost, narrowly.** (d) was the tempting one and the evidence
  refutes it outright. The 2026-08-05 eval-thread sweep ran every cell with
  the pool at 4 and throughput rose monotonically with eval workers:
  177.27 alloc/s at 4 threads, 208.83 at 8, 219.25 at 16, **251.68 at 32**,
  and >= 263.08 at 64 (where the load generator, not Legion, became the
  constraint). Four connections carried 32 concurrent planners plus 5,000
  nodes of heartbeats; a pool capping concurrency near 4 could not have let
  32 threads beat 16 by 14.8%. The code says the same thing: no read
  connection is held across a quorum round, an `.await`, or `build_plan` —
  planning reads the in-memory `SchedulingReadModel`,
  `plan_and_apply_internal` holds no guard when it calls
  `submit_and_apply_plan_with_candidates`, and the apply runs on the WRITE
  connection inside `replicated()`. Every acquisition is one leaf store
  method wide, so **the pool cannot serialize two planners against each
  other**. What survives is that the *basis* was wrong, which is (a)'s
  problem: core count justified itself on "these reads are CPU-bound
  page-cache hits", and a saturated 5,000-node control plane measured **8%
  CPU across four cores**. A page-cache miss is a `pread` holding the mutex
  through an I/O wait that uses no CPU; holds are uneven
  (`cache_refresh_allocation_scopes` keeps one connection across ~44
  per-node queries after every apply, on every replica); and the heartbeat
  body runs inline on tokio worker threads, so a pool sized at the core
  count saturates in lockstep with the runtime by construction. Leaving a
  wrong argument in the tree is how the next person re-derives the wrong
  conclusion from it. (b) fixes the basis but discards the one signal we
  have for how many concurrent readers exist, which is also why (c) is
  unnecessary: `evalThreads` is that signal and it is already in the
  config, whereas a `readConnections` field is a number no operator can
  size from first principles.
- **Why capped at 16 (memory, and an admission).** `SQLITE_CACHE_SIZE_KIB`
  is -65536, a **64 MiB page cache per connection**, filled lazily but not
  shared: at 200,000 allocations every connection independently pulls in
  the same hot `allocation` pages, so the worst case is the real case. 16
  connections is a 1 GiB ceiling, asserted by the unit test so that raising
  the page cache forces the cap to be reconsidered rather than silently
  multiplying. The cap is emphatically **not** a knee — the sweep's
  64-thread cell was driver-bound, so where read concurrency stops paying
  has never been observed, and the comment says so rather than implying a
  measurement that does not exist.
- **The negative result is the valuable half.** The suspicion was
  arithmetic: throughput divided by plan-path latency gave ~3.96 "effective
  workers" against 16 configured, sitting next to 4 cores and 4
  connections. The derivation used the wrong denominator.
  `legion_scheduler_plan_phase_seconds{phase="total"}` times **planning
  only**, so Little's law over it counts evals in the *planning stage*, not
  busy workers — a worker in `claim_eval`, dispatch, post-apply
  bookkeeping, or a conflict-retry backoff is busy and invisible to it.
  3.96 of 16 means planning was about a quarter of an eval's wall time,
  which is exactly what `legion_eval_handle_seconds` was added to expose
  ("the difference is everything an eval does outside planning"). Two
  smaller errors compounded it: allocations-per-eval was taken from the
  scenario's nominal shape rather than measured per cell, and a since-boot
  histogram mean was divided into a steady-window rate. All three are
  written into the `read_only_conn_pool_size` doc comment, because the
  purpose of this entry is that nobody spends a fourth campaign on it.
- **Surface.** None directly. Indirectly, `evalThreads` is now also a
  read-pool control — documented in `docs/state-store.md`,
  `docs/metrics.md`, and the `evalThreads` row of
  `docs/diagnosing-scheduler-performance.md`, and carried as an open
  question by the `evalThreads` plan
  ([`../plans/eval-thread-default-plan.md`](../plans/eval-thread-default-plan.md)),
  whose Phase 0 now has a memory dimension.
  `legion_store_read_lock_wait_seconds` remains the way to tell whether the
  pool is short.

## 14. The eval worker's own rounds move to the blocking pool, and the terminal write retries before abandoning a batch

- **The call:** `claim_evals` and `complete_evals_and_claim_next` run
  under `spawn_blocking` in the eval worker loop (the handler already
  did), and a failed terminal write is retried up to
  `EVAL_SETTLE_MAX_ATTEMPTS` (3) — with an immediate stop on a typed
  `NotRaftLeader` — before the batch is abandoned to in-flight recovery.
- **Alternatives.** (a) Leave the rounds inline: they are "just" store
  calls, and the runtime has as many workers as cores. (b) Make the whole
  eval worker a dedicated OS thread instead of an async task. (c) For the
  retry: add a periodic `requeue_in_flight_evals` sweep on the stable
  leader instead, which also covers handler panics.
- **Trade-off.** Two `spawn_blocking` hops per batch cycle add scheduling
  latency (~µs) to an idle cluster's eval path, and a retried settle can
  triple the time a genuinely-failing write holds a worker before the
  batch lands in recovery. (c) is the more complete fix for orphaned
  evals and remains open — a periodic requeue must not race live
  handlers, which needs an age threshold tied to the worst-case handler
  time, so it is a design of its own.
- **Why (performance, then correctness).** Each inline call parked a
  tokio runtime worker for a whole scheduler-lane quorum round, and
  there are only `available_parallelism()` of those (4 on the campaign
  managers) no matter how high `evalThreads` goes — so past ~4 threads
  the claims themselves serialized the loop, and the TODO already named
  this a prerequisite for sweeping past 64 threads. The retry closes the
  cheaper half of an audit finding: on a leader that never loses
  leadership, `requeue_in_flight_evals` runs only at startup and on
  leadership *acquisition*, so one transient write error orphaned up to
  a whole batch of handled evals forever. Retrying costs nothing when
  the error is real (the attempts fail fast) and saves the batch when it
  was transient.
- **Surface.** None. The failure logs gain a `settle_attempts` field.

## 15. The status-projection drain is not supervised as an eval worker

- **The call:** the drain task moved off the eval workers' `JoinSet` onto
  a plain `tokio::spawn`.
- **Alternative.** Tag the JoinSet's members so the supervisor can
  restart each kind as itself.
- **Trade-off.** A drain that somehow exits is not restarted; every
  subsequent projection send fails into the closed channel and falls back
  to the inline status-lane round — the pre-drain behavior, permanently,
  until restart.
- **Why (correctness of supervision).** The eval-worker supervisor
  replaces ANY exited member with an eval worker, so a dead drain
  silently became a 17th planner — skewing the batch-fairness arithmetic
  keyed to `workers` — while the projection queue kept accepting sends
  into a channel nothing drained (audit finding, 2026-08-06). The drain
  body is panic-free by construction (the projection itself runs under
  `spawn_blocking` with both error arms logged), so a supervisor for it
  buys nothing the fallback does not already guarantee; tagging the
  JoinSet is machinery for a case that degrades gracefully without it.
- **Surface.** None.

## 16. Local benchmarks are reported as a no-regression check only

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

## 17. `RunJob` reports `deduplicated` to the loser of an idempotency race

- **The call:** `run_job` now uses the boolean `Store::idempotency_record`
  already returns. Two submits carrying the same `idempotency_key`
  concurrently both miss the pre-submit lookup; the `INSERT OR IGNORE`
  picks a winner, and the loser now replies `deduplicated: true` carrying
  the winner's recorded message instead of `deduplicated: false` carrying
  its own. Only `RunJob` is changed.
- **Alternatives.** (a) Leave it — the race predates this work.
  (b) Fail the loser with `AlreadyExists`. (c) Fix the same drop in all
  fifteen mutating RPCs that discard the boolean.
- **Trade-off.** The loser pays one extra read (only on the losing path),
  and the fix is now inconsistent with the other mutating RPCs until they
  follow — recorded as open work in [`../TODO.md`](../TODO.md).
- **Why (agent-readiness).** The key exists to promise a retrying agent
  that it did not create two submissions; telling both callers they were
  first is the one answer that breaks that promise, and batching the
  intake path makes the overlap window *more* likely to be hit, not less
  — which is why it belongs in this change rather than a later one.
  (b) is wrong for this RPC: submitting the same spec twice is harmless
  (the second resolves to `Unchanged`), so the loser should be corrected,
  not failed. (c) is the right end state and the wrong blast radius for a
  performance change — each of those RPCs needs its own think about what
  the loser should be handed back.
- **Surface.** `RunJobReply.deduplicated` is now `true` for the loser of a
  concurrent same-key submit, and `message` is the winner's.
