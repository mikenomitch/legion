# Judgement calls: plan-path-scale-levers-plan

Plan: [`../plans/plan-path-scale-levers-plan.md`](../plans/plan-path-scale-levers-plan.md)

## 1. Lazy sample falls back to the eager path instead of consuming a conflict retry

- **The call:** when the generation moved during planning, the scheduler
  silently re-plans once with the eager (O-fleet) sample rather than
  surfacing a `Conflict` into `plan_with_conflict_retries`.
- **Alternatives:** (a) treat generation movement as a plan conflict and
  spend a retry from the eval's bounded budget; (b) loop the lazy path
  until the generation holds still.
- **Trade-off:** the fallback hides one internal retry from the conflict
  metrics (it is counted by its own metric instead), and under constant
  churn every plan pays one wasted `build_plan`.
- **Why:** correctness of the fence must not depend on churn levels, and
  an eval must not *fail terminally* (retries exhausted) merely because
  unrelated jobs kept applying — that would be a new failure mode the
  eager path never had. Option (b) can livelock under storm. One eager
  fallback bounds the cost at exactly today's behavior. (Performance
  with simplicity; no new user-visible failure modes.)

## 2. D6 reshaped: cheap snapshots instead of a claim-streak snapshot cache

- **The call:** implement D6 as snapshot-cost elimination (read model
  keeps a pre-sorted `Arc<Vec<Arc<Node>>>` of available nodes;
  `node_pools` values become `Arc<ResourcePool>` so per-plan clones are
  pointer bumps and the ledger copies-on-write only touched pools) —
  not as the review's "one snapshot shared by consecutive plans in a
  worker's claim streak, invalidated by the generation counter".
- **Alternatives:** (a) the literal streak cache; (b) a streak cache
  that patches itself with the worker's own applied placements.
- **Trade-off:** the snapshot clone stays O(fleet) in entry count
  (pointer bumps), not O(1); truly enormous fleets still pay a small
  linear term.
- **Why:** the streak cache self-defeats — in the mass-submit storm it
  targets, every plan's own apply bumps the generation and invalidates
  the cache, so the hit rate is ~zero exactly when it matters; variant
  (b) fixes that but adds self-patching cache machinery (the plan's
  named cost: "cache invalidation logic") for the same measured win.
  Cheapening the snapshot achieves the same phase-table result for every
  plan with zero staleness and zero invalidation logic. (Simplicity and
  performance both.)

## 3. All three levers default-on with escape hatches, not opt-in flags

- **The call:** `lazy_version_sample`, `plan_apply_coalescing` (and the
  D6 restructuring, which has no flag) default to the new behavior;
  `schedulerSettings` knobs exist to restore the old paths.
- **Alternatives:** ship default-off and flip later (the architecture
  review's generic rollout posture).
- **Trade-off:** a regression this plan's tests missed would ship
  enabled.
- **Why:** the review's flag-gating was "until measured" — this plan
  lands *with* the measurements on the same host, and each lever is
  either provably equivalent (lazy sample falls back on any doubt; D6 is
  a representation change with unchanged values) or byte-identical when
  uncontended (a batch of one takes the single-plan path). A default-off
  flag nobody flips would leave the entire point of the work unshipped.
  (Performance goal; simplicity — no "fast mode" bifurcation for
  operators to discover.)

## 4. The generation counter lives inside the read-model lock, not an atomic

- **The call:** `generation: u64` is a field of `SchedulingReadModel`,
  read and bumped under the existing `RwLock`.
- **Alternatives:** an `AtomicU64` on `Store` (the review sketched "one
  contended atomic").
- **Trade-off:** reading the generation requires the read lock (cheap,
  already taken by every snapshot/sample read anyway).
- **Why:** the fence argument needs *atomicity between the generation
  and the data it versions* — "capacity + generation under one guard"
  and "epochs + generation under one guard". A separate atomic reopens
  exactly the torn-read races the mirror's publish ordering was built to
  prevent, and buys nothing: there is no reader that wants the
  generation without the guarded data. (Correctness first; also zero
  new synchronization.)

## 5. Group commit batches at the store seam, not the raft pipeline

- **The call:** the coalescer wraps `submit_and_apply_plan` (plan-apply
  specific), not the per-lane raft pipeline loop (generic op batching).
- **Alternatives:** drain-batch arbitrary queued ops in
  `pipeline_loop`, merging captures.
- **Trade-off:** other write-heavy ops (evals, status) do not get
  batched by this work.
- **Why:** plan applies are the measured hot spot and the only op with
  per-item verdict semantics already designed for batching (per-plan
  epoch CAS). Generic capture-merging would need per-op isolation
  semantics invented op by op, and the ownership guard forbids
  cross-lane merges anyway. The store seam also wins on local_disk
  (fsync amortization), which the raft-pipeline seam would not.
  (Performance where measured; smallest correct surface.)

## 6. The batch is one transaction and HOLDS the write connection for it

> **Reverses the original entry 6** ("Local backends run batch members
> back-to-back — no outer transaction", phases 1-3, 2026-07-30), which
> deferred local commit amortization because the read-model refresh
> after each local apply reads committed state through the read
> connections: mid-batch those reads see PRE-batch state and publish a
> mirror missing the batch's own placements. Phase 4 (2026-08-03) fixed
> the cause rather than living with the symptom. The original call is
> kept here as history; the current call is below.

- **The call:** the group commit opens ONE write transaction for the
  whole batch and **holds the store's write connection** for its whole
  duration, exactly as every other multi-statement write in this store
  does. To make that possible the plan body gained a
  caller-supplied-connection form (`apply_plan_submission_on(tx, …)` /
  `submit_and_apply_plan_on(tx, …)`), and it now *returns* its side
  effects (`PlanApplyEffects`) instead of performing them, so the
  transaction's owner emits them after the commit. Locally that is one
  fsync for N plans; under raft it is unchanged (the transaction nests
  inside the capture savepoint).
- **Alternatives:** (a) leave it deferred, as phases 1-3 did; (b) an
  outer `SAVEPOINT legion_plan_batch` with the connection released
  between plans (the small change — no `_on` seams, no returned
  effects), plus a thread-local scope that buffers side effects until
  batch end, mirroring what `capture::in_capture()` does for raft;
  (c) an explicit process-wide write barrier (`RwLock`) taken by every
  `write_connection()` caller and held exclusively by the batch.
- **Trade-off:** the `_on` seams and returned effects touch the most
  critical function in the store — `apply_plan_submission_apply` is the
  oversubscription fence — for a change whose whole purpose is to move
  a commit boundary. Holding the connection also means an unrelated
  writer waits for the batch instead of interleaving with it, so a
  batch of `plan_apply_batch_max` plans is now a single write-lock hold.
- **Why:** alternative (b) is a correctness hole, not a smaller version
  of the same thing. With the connection released between plans, any
  other thread's write lands *inside* the batch's open transaction:
  its commit is not durable until the batch's, a batch rollback erases
  it, and — the same JC-6 bug one level out — its own post-commit cache
  refresh reads pre-batch state and publishes a mirror missing its own
  write. Alternative (c) closes that hole but adds a lock to every
  write in the process and a thread-local exemption for the batch
  thread, to avoid a refactor that the codebase's own idiom (a writer
  holds the guard for its transaction) says should exist anyway.
  Returning effects also beats buffering them in a thread-local: the
  effects become a value with a type, the batch merges them explicitly,
  and there is no ambient state to get wrong. The bounded write-lock
  hold is the batch's real cost and is why `plan_apply_batch_max`
  exists. (Correctness first, then performance; simplicity of the
  resulting structure over the smaller diff.)

## 7. The batch leader does the batch's bookkeeping, including ONE status projection

- **The call:** post-apply bookkeeping moved from "every submitter, for
  its own plan" to "the batch leader, once for the batch": one epoch
  self-heal over every rejected plan's scopes, and one status-lane
  `task_status` projection over every allocation the batch placed. Each
  submitter is told whether it was done for it
  (`PlanApplySlot::bookkeeping_done`) and runs its own otherwise.
- **Alternatives:** (a) leave the projection per plan (the shipped
  phase-3 behavior) — a batch of N plans costs 1 plan commit and N
  status writes, which under `lanes` is N more raft rounds; (b) fold
  the status projection INTO the plan's transaction, removing the
  follow-up write entirely.
- **Trade-off:** the leader's critical section grows — it now holds the
  coalescer's commit lock across the status projection, so the next
  batch starts marginally later. And a batched bookkeeping failure is
  attributed to the batch, not to one plan, until the per-plan fallback
  re-runs it.
- **Why:** the N status writes were the larger half of what a batch
  still paid per plan, and they serialize on the same write connection
  anyway — batching them does not lengthen the aggregate critical
  section, it shortens it. (b) is not available: lifecycle-derived
  `task_status` rows are status-lane state and must not ride a
  scheduler-lane changeset (plan §7-C2) — that split is what lets the
  lanes topology exist. The fallback keeps per-plan error attribution,
  and both halves are idempotent so the repeat is free of consequence.
  (Performance, without giving up the lane boundary.)

## 8. Heartbeat batching reuses the same coalescer pattern (see heartbeat plan)

- **The call:** one leader-elect drain-batch primitive, used by both
  plan applies and heartbeats, rather than two bespoke queues.
- **Alternatives:** independent implementations tuned per path.
- **Trade-off:** the shared primitive must stay generic (request in,
  result out, batch function), so path-specific tricks (e.g. dedupe) sit
  in the batch functions, not the primitive.
- **Why:** identical concurrency shape, identical failure semantics,
  one set of race-condition tests. (Simplicity/extensibility.)

## 9. A batch member that fails mid-apply now rolls back its submission row too

- **The call:** every plan in a batch runs inside a
  `legion_plan_batch_item` savepoint on **every** backend, so a plan
  that fails mid-apply rolls back its `plan_submission` insert along
  with its placements. Locally that row used to survive, because the
  insert ran on the connection in autocommit before the apply opened
  its own transaction.
- **Alternatives:** keep the local insert outside the batch's
  transaction so the queued row survives a failure, preserving
  byte-identical local behavior.
- **Trade-off:** a failed plan leaves no forensic `queued` row on local
  backends. Nothing consumes those rows — no recovery path scans for
  `status = 'queued'` — so the loss is diagnostic only, and the failure
  itself is still logged and returned to the submitter.
- **Why:** raft mode already rolled both back together (a failed
  capture proposes nothing), so this makes the two backends agree
  instead of adding a difference. Keeping the old behavior would mean
  deliberately leaving one write outside the transaction that exists to
  contain it. (Simplicity: one apply story, not two.)

## 10. A failed post-commit refresh is logged, never returned

- **The call:** past the batch's commit, `run_plan_batch_txn` cannot
  fail: a read-model refresh that will not publish is logged at `error`
  and the batch's per-plan results stand.
- **Alternatives:** propagate the error like any other batch failure —
  which routes into the re-run-singly fallback.
- **Trade-off:** a failed refresh leaves the mirror stale for the
  affected scopes until the next publish or the rejection self-heal
  refreshes them; plans built against it conflict rather than
  oversubscribe.
- **Why:** re-running a batch that has already COMMITTED is the one
  thing this path must never do. (Each re-run plan would fail its own
  epoch CAS against the state its first run created, so the cluster
  stays consistent — but every submitter would be told `Rejected` for a
  plan that in fact applied, and would replan from scratch.) The
  re-run-singly fallback is only sound before the commit, so the commit
  is the last statement that may use `?`. In practice a refresh failure
  means a poisoned read-model lock, which is not a condition a retry
  improves. (Correctness first.)
