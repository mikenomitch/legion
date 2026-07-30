# Plan-Path Scale Levers (lazy version sample, group commit, cheap snapshots)

**Status: IN EXECUTION** — graduates proposals D4, D5, and D6 from
[`scheduler-architecture-review-plan.md`](scheduler-architecture-review-plan.md)
into one implementation plan; they share machinery (the read-model
generation counter) and tests (the oversubscription fence suite).

> **Judgement calls:** [`../calls/plan-path-scale-levers-plan_calls.md`](../calls/plan-path-scale-levers-plan_calls.md)

## Problem

Three per-plan costs on the scheduling hot path grow with fleet size or
repeat per plan when they could be paid once, measured in the 2026-07-29
matrix (`benchmarks/scheduler/README.md`):

1. **The optimistic-concurrency sample is O(fleet)**
   (`allocation_version_vector_for_candidates`,
   `src/storage/store.rs:1940`): every plan builds a
   `BTreeMap<String, i64>` with a formatted `node:<id>` key for *every
   candidate node in the fleet*, because the affected node set is only
   known after planning. Measured ~1.2 ms/plan at 1k nodes; the curve
   projects to ~12 ms at 10k — larger than `build_plan` itself.
2. **Every plan commits alone** (`Store::submit_and_apply_plan`,
   `src/storage/store.rs:2130`): one write transaction, and on raft one
   propose→quorum-commit→apply, per eval. Apply is the largest phase in
   every measured cell — 646 µs on local_disk, 4.4-7.9 ms on raft3.
   Concurrent eval workers serialize on this cost one at a time.
3. **Every plan rebuilds the world snapshot**
   (`Scheduler::available_nodes` + `capacity_snapshot`,
   `src/manager/scheduler.rs:1058`, `:600`): an O(fleet) sort + clone of
   the node list, and a deep clone of every node's `ResourcePool` —
   ~1 ms/plan at 1k nodes, linear, rebuilt even when nothing changed.

## Prior art

- **Nomad** solves (1) by construction: its plan queue serializes applies
  on the leader and the plan optimistically carries only what it touched;
  conflicts are detected per-alloc at apply. It solves (2) the same way —
  the plan applier batches plan evaluation against one state snapshot.
- **Kubernetes** does not have (1) (etcd transactions carry precise
  resource versions per object). Group commit is etcd's own batching
  (raft proposals batch in flight).
- **SQLite lore**: group commit is the classic amortization for
  fsync-bound write paths; WAL mode makes N-in-one-transaction nearly
  free relative to N transactions.

## Jobs to be done

**Cluster operator**
- When my fleet grows from 1k to 10k nodes, I want per-plan scheduling
  cost to stay flat, so a bigger cluster does not mean slower deploys.
- When a node storm hits (mass reschedule after a rack loss), I want the
  scheduler to chew through the eval backlog at full write-path speed,
  so recovery time is bounded by real work, not per-eval overhead.

**Legion maintainer**
- When I read a benchmark phase table, I want no phase whose cost is
  O(fleet) bookkeeping rather than real placement work, so optimization
  effort goes where the product needs it.

**The public claim** (AWS comparative test)
- Measured scaling curves whose per-plan cost does not grow with fleet
  size, without weakening the oversubscription guarantees.

## Goals

- Remove the O(fleet) version-sample term from every plan (D4).
- Amortize commit cost (fsync locally, quorum round-trips on raft) across
  concurrently submitted plans (D5).
- Make the per-plan snapshot cost near-constant (D6, reshaped — see
  judgement calls).
- Preserve the oversubscription fence exactly: the epoch CAS semantics,
  the "sampled no later than the capacity read" invariant, and every
  existing fencing test.

## Non-goals

- Cross-manager scheduling (D2 step 2) — out of scope, unchanged.
- Replacing SQLite (D3) — explicitly rejected by the review.
- Candidate sampling (D1) — its own plan
  ([`bounded-candidate-scoring-plan.md`](bounded-candidate-scoring-plan.md)).
- Changing eval-queue claiming or dedupe semantics.

## What exists today

- The fence: `plan_and_apply_internal` samples the epoch mirror for every
  candidate node **before** the capacity snapshot
  (`src/manager/scheduler.rs:748-776` — the invariant comment), then
  narrows to job scope + affected nodes at submit
  (`expected_versions_for`, `:946`). Apply re-reads epochs in-transaction
  and rejects on mismatch (`src/storage/store.rs:2248`).
- The read model (`SchedulingReadModel`, `src/storage/store.rs:116`)
  keeps `nodes`, `allocations_by_node`, `node_pools`, `indexes`, and the
  `scope_epochs` mirror coherent under one `RwLock`; every allocation
  mutation funnels through `cache_refresh_allocation_scopes`
  (`store.rs:1720`), which reads epochs, reads allocations, and publishes
  both under one write-lock acquisition. **There is no generation
  counter.**
- The apply: `submit_and_apply_plan` (`store.rs:2130`) is one replicated
  op per plan (fused insert+apply, shipped 2026-07-29). Concurrency is N
  eval workers racing; serialization is the write-connection mutex
  (local) or the per-lane raft pipeline thread (`driver.rs:1387`), one
  capture → one `client_write` per plan. `write_tx` uses SQLite
  savepoints (`store.rs:1136`), which nest.
- Snapshots: `available_node_arcs` clones + sorts Arc'd nodes per plan
  (`store.rs:1853`); `plan_capacity_snapshot` clones `allocations_by_node`
  (shallow, Arc values) and `node_pools` (**deep** — every
  `ResourcePool`) per plan (`store.rs:1871`).

## Proposed design

### 1. Read-model generation counter (shared machinery)

`SchedulingReadModel` gains `generation: u64`, bumped inside every write
that publishes allocation/epoch changes (the `cache_refresh_allocation_scopes`
funnel, plus full read-model rebuilds). Because it lives inside the same
`RwLock`, reading `(capacity, generation)` or `(epochs, generation)`
under one read guard is atomic with no new synchronization.

An unchanged generation between two reads proves no allocation mutation
was published between them.

### 2. D4 — lazy version sample

`plan_and_apply` stops pre-sampling every candidate node. New order:

1. Read the capacity snapshot **and** the generation `g0` under one guard.
2. Build the plan (unchanged).
3. Sample the epoch mirror for exactly job scope + affected nodes, and
   re-read the generation, under one guard.
4. If generation == `g0`: the late sample is provably equal to a sample
   taken at the capacity read (nothing was published in between) — the
   invariant holds; submit with it.
5. If generation != `g0`: something changed mid-plan (usually another
   plan applying). Fall back to the eager path — re-sample every
   candidate, re-snapshot, re-plan — i.e. exactly today's behavior, once.

The fallback keeps correctness unconditional: the lazy path is only taken
when it is provably equivalent to the eager path. Under heavy unrelated
churn the cost degrades to today's cost plus one wasted plan, bounded by
one retry (the fallback plan is eager and cannot be invalidated by the
generation again).

`version_sample` stays as the phase name; in the lazy case it times the
late (O(affected)) sample.

Escape hatch: `schedulerSettings.lazy_version_sample: false` restores the
eager sample unconditionally.

### 3. D5 — plan-apply group commit

A coalescer in front of `submit_and_apply_plan`:

- Submitters enqueue `(plan request, reply channel)` and try to become
  the **batch leader** (a `try_lock` on a commit mutex). The leader
  drains everything queued, runs **one** replicated op containing all
  drained plans, distributes per-plan results, releases. Submitters that
  lose the race block on their reply channel; anything arriving during a
  commit forms the next batch — natural batching, no timer, zero added
  latency for a lone plan (batch of one takes the existing single-plan
  path).
- The batched store op runs each plan's existing
  `submit_and_apply_plan_apply` body inside its own nested savepoint
  (savepoints nest; capture already relies on this), inside one outer
  write transaction. One fsync locally; **one capture → one raft
  propose** for the whole batch on raft backends. Per-plan epoch CAS
  verdicts are per-plan outcomes exactly as today; a plan that fails
  its savepoint rolls back alone and cannot poison batch-mates.
- Read-model updates and the status-lane follow-up
  (`plan_apply_bookkeeping`) stay per-plan, after the batch commits.
- Batch size is capped (`plan_apply_batch_max`, default 16) so raft
  entries stay within the append-entries deadline budget.

Escape hatch: `schedulerSettings.plan_apply_coalescing: false`.

Surface: none visible to users beyond throughput; `job plan`/`job run`
behavior, conflict semantics, and event emission are unchanged.

### 4. D6 — cheap snapshots (reshaped from "snapshot reuse")

The review proposed reusing one snapshot across a worker's claim streak,
invalidated by the generation counter. Implementation analysis (see
judgement call #2) showed the streak cache self-defeats: in exactly the
storm scenario it targets, each plan's own apply bumps the generation and
invalidates the cache. Instead this plan makes the snapshot itself
near-free, which helps every plan, not just streaks:

- **Available nodes**: the read model keeps the availability-filtered,
  id-sorted node list as `Arc<Vec<Arc<Node>>>`, rebuilt only when node
  membership/availability changes (rare). `available_nodes()` becomes a
  refcount bump.
- **Capacity pools**: `node_pools` values become `Arc<ResourcePool>`;
  the per-plan clone becomes O(fleet) pointer bumps instead of deep
  `ResourcePool` copies; the plan-time `ResourceLedger` copies-on-write
  only the pools it actually debits (`Arc::make_mut`).

Same effect as the proposal (per-plan snapshot cost stops being a
measured phase), no staleness, no cache invalidation logic.

## Config surface

```jsonnet
// manager config
{
  schedulerSettings: {
    lazy_version_sample: true,     // default true; false = eager O(fleet) sample
    plan_apply_coalescing: true,   // default true; false = one commit per plan
    plan_apply_batch_max: 16,      // max plans per group commit
  },
}
```

No proto, wire, or on-disk changes. No migration.

## Implementation phases

1. **Generation counter + D4 lazy sample** (with eager fallback and the
   config gate). Ships alone; benchmark before/after on
   `version_sample`.
2. **D6 cheap snapshots**. Ships alone; benchmark `available_nodes` +
   `capacity_snapshot` phases.
3. **D5 group commit** (coalescer + batched store op + config gates).
   Ships alone; benchmark with a concurrent driver (see below).

## Testing plan

- Entire existing fence suite stays green, unmodified:
  `concurrent_plans_cannot_oversubscribe_a_node`,
  `plan_submission_rejects_stale_epoch_*`,
  `candidate_version_vector_matches_per_scope_reads_test`, the
  scheduling-index equivalence property, eval-loop suites, raft suites.
- New: generation counter unit tests (bumps on place/remove; stable
  otherwise); lazy-sample fallback test (mutate between snapshot and
  submit → falls back, applies correctly, never oversubscribes);
  group-commit tests (batch with a mid-batch rejection isolates the
  loser; batch of N applies N; raft: one log entry per batch —
  asserted via the existing changeset/ownership test harnesses);
  coalescer unit tests (all submitters get exactly their own result).
- Benchmarks: the scale matrix (`scheduler_scale_benchmark`) before/after
  per phase; a new concurrent driver mode
  (`LEGION_BENCH_CONCURRENCY=N`) so group commit is actually exercised;
  SLO gate stays green.

## Rollout

All three levers are default-on with per-lever escape hatches in
`schedulerSettings` (documented in `docs/scheduler-and-eval-loop.md`).
Rationale for default-on rather than opt-in: the lazy sample and cheap
snapshots are provably equivalent (fallback/COW), group commit's
uncontended path is byte-identical to today's, and this plan lands with
same-host before/after numbers per the AGENTS.md rule. No migration; no
compatibility concern (all state shapes unchanged).

## Acceptance criteria

- Phase 1: `version_sample` phase mean at the `churn-large` scenario
  (1k nodes) drops by ≥10x; fence tests green.
- Phase 2: `available_nodes` + `capacity_snapshot` phase means drop by
  ≥5x at 1k nodes; equivalence tests green.
- Phase 3: with 4-8 concurrent submitters on raft1/raft3, plan throughput
  improves materially (target ≥2x on raft3) with conflict rate and
  correctness ratio unchanged; batch-isolation tests green.
