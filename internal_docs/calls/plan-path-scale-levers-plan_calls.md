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

## 6. Heartbeat batching reuses the same coalescer pattern (see heartbeat plan)

- **The call:** one leader-elect drain-batch primitive, used by both
  plan applies and heartbeats, rather than two bespoke queues.
- **Alternatives:** independent implementations tuned per path.
- **Trade-off:** the shared primitive must stay generic (request in,
  result out, batch function), so path-specific tricks (e.g. dedupe) sit
  in the batch functions, not the primitive.
- **Why:** identical concurrency shape, identical failure semantics,
  one set of race-condition tests. (Simplicity/extensibility.)
