# Scheduler and Eval Loop

This document covers the control-loop that drives placement in Legion.

## High-Level Model

Legion separates orchestration into two layers:

- Eval queue: decides what kind of reconciliation action to run (job submit, node drop, rollout tick, etc).
- Scheduler: for placement-oriented evals, computes allocation plans and applies them with optimistic concurrency.

## Eval Workers

At manager startup:

- `start_eval_loop` creates `eval_threads` workers (minimum 1).
- Any worker that exits is immediately restarted.
- Startup recovery calls `requeue_in_flight_evals()`.

Each worker loop:

1. `claim_evals()` atomically marks a **batch** of `pending` evals as
   `in_flight`.
2. Executes `Eval::handle(store)` for each one, in claim order, one at a
   time.
3. `complete_evals_and_claim_next()` writes every outcome — success deletes
   the eval row, failure sets status `failed`, a transient rejection returns
   the row to `pending` after a delay — **and** claims the next batch, in one
   operation.

When queue is empty, workers block on either:

- `Notify` wake-up (`enqueue_eval` of zero-delay work), or
- timeout until next delayed eval `ready_at`.

### Batching

Steps 1 and 3 are fixed costs paid per *round*, not per eval. On a
replicated (raft) manager each is a quorum round on the scheduler lane, and
that lane drains through a single pipeline thread — so a cluster doing a
little work per eval hits a ceiling that has nothing to do with the work.
Taking N evals through one cycle divides those two rounds by N.

The batch is sized to the queue, not to a timer:

- **It never waits to fill.** A worker takes whatever is claimable right now
  and starts working. Idle latency is unchanged.
- **It is a fair share, not a grab.** Each worker takes at most
  `pending_evals / evalThreads` rows, so a burst of work spreads across
  workers instead of serializing behind the first one to wake.
- **It is capped at 16** rows per worker regardless of queue depth.
- **A shallow queue yields a batch of one**, which behaves exactly like the
  unbatched loop.

There is no configuration for this; the two inputs — live queue depth and
`evalThreads` — are already known to the loop.

Batching does not change what evals do or the order they run in. Each eval
still runs its own handler and applies its own plan in its own operation,
and the claim still drains in `priority DESC, id ASC` order across the whole
batch, so a low-priority eval can never overtake a high-priority one by
riding in someone's batch.

**Failure isolation.** Only outcomes the worker actually *observed* are
written. If one eval's handler panics or is cancelled, its siblings still
settle normally and that eval is simply omitted — its row stays `in_flight`
and returns to the queue via `requeue_in_flight_evals()` (run on startup and
on raft leadership acquisition). The same applies to the whole batch if the
terminal write itself fails, or if the worker or manager dies mid-batch:
nothing is written, every row stays `in_flight`, and recovery requeues them.
An eval is never marked failed on the strength of an unknown outcome,
because failed evals are not retried.

The cost of that safety is bounded re-execution: an eval handled inside a
batch whose terminal write never lands will run again after recovery. Eval
handlers are idempotent convergence steps, so this is wasted work rather
than duplicated effects, and the batch cap bounds how much of it a single
crash can cause.

**Leadership.** A worker that loses scheduler-lane leadership part-way
through a batch stops there and leaves the remainder `in_flight` for the new
leader, rather than running work whose writes cannot commit. A batch can
never be half-applied across a leadership change: the terminal write for the
whole batch is a single replicated operation that either commits or does
not.

## Eval Queue Semantics

Eval ordering:

- Priority desc, then ID asc.
- An eval submitted without an explicit priority inherits the one implied by
  its job's **priority band** (`critical` 90, `high` 70, `elevated` 60,
  `normal` 50, `low` 30, `background` 15), falling back to the manager's
  `defaultPriority` when no band resolves. So importance orders the queue and
  governs preemption through one field — see
  [preemption-and-eviction.md](preemption-and-eviction.md). `legion job run
  --priority N` / `--asap` still override the queue position for a single
  submission without changing the band.

Dedup-on-claim behavior:

After claiming an eval, other pending evals are deleted when all of these hold:

- same `job_id` or same `node_id`,
- same kind,
- already claimable (`ready_at <= now`).

This keeps the queue from growing with redundant work for the same entity
without discarding work the claimed eval does not actually subsume: a
`job_reschedule` must not drop a job's pending `rollout_tick` (ticks are
self-perpetuating, so nothing would re-drive the rollout) or its `job_stop`,
and an immediate eval must not cancel a delayed one, which is a timer rather
than a queued duplicate (for example a batch task's next scheduled
`job_update`).

## Supported Eval Kinds

`Eval::handle()` supports:

- `node_join`
- `node_drop`
- `job_submit`
- `job_update`
- `job_stop`
- `job_reschedule`
- `rollout_tick`

Unsupported kinds fail immediately.

## Scheduler Entry Points

Main paths in `Scheduler`:

- `plan_only` -> compute placements without DB apply.
- `plan_for_config` -> plan against a transient config (used by plan API).
- `plan_and_apply` (+ variants) -> compute and apply placements.

Modes:

- `Fresh`: place full desired set.
- `Reschedule`: place only missing count relative to existing allocations.

## Placement Application and Conflict Safety

Placement apply uses optimistic concurrency with partitioned epochs.

### Job-config scoped epochs

Legion maintains an epoch per `job_config_id` in `allocation_epoch_partition`.

1. Scheduler reads the current epoch for the target job config.
2. Scheduler writes `plan_submission.expected_epoch` with that value.
3. Apply path re-reads the current epoch for that same job config in-transaction.
4. Apply proceeds only when epochs still match.
5. On successful allocation changes, only that job config epoch is bumped.

This keeps conflicts localized so unrelated jobs can schedule in parallel without invalidating each other's plans.

The plan submission and its apply run as **one replicated store op**
(`Store::submit_and_apply_plan` / `submit_and_apply_plan_with_candidates`):
one write-transaction boundary, and on raft one
propose→quorum-commit→apply instead of two.

### Candidate lists (late-binding apply)

Since the late-binding change
([late-binding-plan-apply.md](late-binding-plan-apply.md)), a plan carries
an ordered **candidate list** per placement instead of a single node, and
the apply transaction binds each placement in two passes: first the
best candidate whose node epoch is unchanged since planning, then — for
placements that found none — the best candidate that still passes the
transaction's own re-validation, epoch or no epoch. A concurrent plan that
touched the first choice costs a fallback rank, or nothing at all, instead
of a whole-plan rejection. Coupled placements (shared `distinct` keys,
multi-task groups) keep exact all-or-nothing semantics; overlapping
candidate lists are not coupling. Defaults on
(`schedulerSettings.plan_candidates = 8`); `0` disables it and restores
single-node submissions. The epoch sample (lazy or eager, below) covers
the union of every placement's candidate list, and candidate submissions
take the same group commit as single-node ones.

### The epoch sample is lazy by default

The fence rule is that epochs must be sampled *no later than* the
capacity snapshot the plan is built against. Sampling every candidate
node up front satisfies that unconditionally but costs O(fleet) per
plan. By default the scheduler instead plans first and samples epochs
for exactly the scopes the plan touches (the job config plus the union of
every placement's candidate list), accepting the late sample only when the read model's
**generation counter** — bumped on every allocation publish — proves
nothing changed since the capacity read. When something did change
mid-plan, the scheduler re-plans once with the eager whole-fleet sample
(unconditionally safe), counted by the
`legion_scheduler_lazy_sample_fallback_total` metric.

`schedulerSettings.lazy_version_sample = false` restores the eager sample
on every plan. Correctness never depends on this setting — the lazy path
falls back on its own whenever it cannot prove equivalence.

### Group commit

Concurrent plan applies (parallel eval workers) coalesce into **one**
store commit: plans queued while a commit is in flight batch into a
single replicated op — one write transaction, and on raft one
propose→quorum round for the whole batch — with every plan keeping its
own epoch-CAS verdict inside a per-plan savepoint. A plan that fails
mid-apply rolls back alone; its batch-mates commit. A lone plan takes
the single-plan path unchanged, so idle-cluster latency is unaffected.

The batch is one transaction on **every** backend, so a local
(`local_disk` / `streaming`) manager pays one fsync for the batch
instead of one per plan, exactly as a raft manager pays one quorum
round. The batch holds the store's write connection for the whole
transaction — the same rule every other multi-statement write follows —
so no unrelated write can land inside it.

What the batch produces is published **once, after that commit**:

- **One read-model refresh** over the union of every plan's allocation
  scopes, read from committed state. Evicted and placed rows refresh
  together, so the mirror never shows an intermediate state.
- **One status-lane `task_status` projection** covering every allocation
  the batch placed, instead of one per plan. Lifecycle-derived task
  statuses are a status-lane write that follows the scheduler-lane apply
  (see [multi-raft.md](multi-raft.md) on lanes); batching it turns N
  follow-up writes — N more raft rounds under `lanes` — into one.
- **Every staged event**, in id order. Nothing is broadcast before the
  batch is durable.

During the batch the read model still shows pre-batch state for the
scopes it is about to change. That is the same window the epoch fence
already covers: a plan built against it fails its compare-and-set at
apply, so a stale read model costs a conflict, never an
oversubscription.

- `schedulerSettings.plan_apply_coalescing = false` restores one commit
  per plan, one status projection per plan, and one read-model refresh
  per plan. This is the only escape hatch — there is no separate switch
  for the local commit amortization or the batched status projection.
- `schedulerSettings.plan_apply_batch_max` (default 16, max 128) caps
  plans per batch.

Known limitation: a batch is bounded by `plan_apply_batch_max`, and a
larger batch holds the write connection (and, on raft, one log entry)
for proportionally longer. Raising it past the default trades tail
latency for other writers against fewer commits; the raft append-entries
deadline (`heartbeatIntervalMs`) is the real ceiling, which is why the
cap exists.

Worker heartbeats batch the same way on the status lane (see
[architecture-managers-workers.md](architecture-managers-workers.md)).

### Observing where plan time goes

Every `plan_and_apply` records a per-phase wall-clock breakdown
(`PlanTimings` on the outcome): node snapshot, epoch sample, task fetch,
capacity snapshot, `build_plan`, and the fused apply. It is logged per
plan at debug, exported as the `legion_scheduler_plan_phase_seconds{phase}`
histogram ([metrics.md](metrics.md)), and aggregated by the benchmark
runners ([testing-and-benchmarks.md](testing-and-benchmarks.md)). When
scheduling latency moves, find the phase that moved first.

### Apply-time re-validation

The epoch fence only covers what bumps an allocation epoch. Node-row
changes — a cordon/drain, a node loss, or a worker re-registration that
shrinks the node's capacity (`memory`, `reservedResources`,
`maxAllocations`, …) — bump none, so the apply transaction re-validates
every placement against the **fresh** node row before inserting it:

- **Availability**: the node must still be `available`.
- **Constraints and permits**: the task's constraint and permit sets must
  still be satisfied.
- **Resource totals**: the node's free pool is rebuilt inside the
  transaction (current capacity minus the allocation rows the transaction
  can see, with a Fresh plan's own replaced job excluded — the same
  arithmetic as the planner's ledger) and every placement must still fit,
  with earlier placements in the same plan debited.

#### The per-node usage cache

Rebuilding that free pool used to read **every** allocation row on the node
and parse every stored `resources` blob, inside the serialized plan-apply
commit. The cost of committing a plan therefore grew with how full the
cluster was while the plan's own work stayed constant: on a 5,000-node
fleet, the same ~44 placements read ~440 allocation rows with 50,000
allocations live and ~1,760 with 200,000.

A node's total allocation usage is now cached in `node_alloc_usage`, one
row per node. The cache is **validated, never trusted**: each row stores a
fingerprint of the allocation rows it was computed from — the node's
allocation epoch, the row count, and the sum of the allocation ids — and
the apply path recomputes that fingerprint (an index-only read that touches
no row bodies and parses no JSON) before using the cached value. If it does
not match, the pool is rebuilt from the rows exactly as before, and the
plan refreshes the row on its way out.

What this means in practice:

- **The number is unchanged.** The same capacity arithmetic, the same
  overcommit policy, the same exclusion of a Fresh plan's own job. Cached
  and rebuilt pools can differ only by a floating-point rounding step,
  because the cache subtracts one summed total where the rebuild subtracts
  each allocation in turn.
- **Nothing has to maintain it.** Any path that adds or removes an
  allocation — a placement, a stop, a preemption, a node removal, a job
  destroy — invalidates the affected rows implicitly, because it moves the
  fingerprint. A stale, wrong, or missing row costs one rebuild; it can
  never inflate a node's free capacity.
- **Version skew is safe in both directions.** A manager binary without
  this feature neither reads nor writes the table, so a leader on either
  side of an upgrade simply finds the fingerprints stale and rebuilds. The
  table is created empty with no backfill.

Known limitation: removing a node leaves its cache row behind (the removal
runs on a different raft lane than the table's writer). The row is inert
and roughly 100 bytes.

A failed re-validation on *every* candidate is what rejects a plan; the
eval handler's conflict retry then re-plans against the node's real
state. Since the two-pass bind, this re-validation — not the node epoch —
is what decides whether a contended placement lands. Port assignments were always re-read in the apply
transaction; these checks give availability, constraints, and resources
the same apply-time guarantee.

### Conflict handling

A rejected plan raises a conflict error, which the eval handler retries
with bounded attempts via `Eval::plan_with_conflict_retries`. A stale
*job-config* epoch always rejects; a stale *node* epoch only rejects when
the re-validation above also fails (or for an edge-boot ratification, which
keeps the hard node-epoch check).

Since the late-binding change, exhausting that in-handler budget no longer
fails the eval: the eval is released back to the pending queue with a
jittered delay (`eval.requeued` event) and runs again. Conflict is
transient by definition; terminal failure remains for non-conflict errors
(for example, a pool with genuinely no capacity).

### Tuning conflict retries

Use manager `schedulerSettings.plan_conflict_max_retries` to tune retry behavior for high contention.

- default: `3`
- minimum: `1`
- maximum: `20`

The requeue backstop is paced by
`schedulerSettings.plan_conflict_requeue_delay_ms` (default `1000`,
clamped to 100–60000, plus uniform jitter of up to the base).

Example manager config:

```nickel
{
  schedulerSettings = {
    plan_conflict_max_retries = 8,
    plan_conflict_requeue_delay_ms = 500,
  },
}
```

With candidate lists on, retries are rarely spent — check
`legion_plan_apply_total{outcome}` and the candidate-rank histogram before
tuning here; see
[late-binding-plan-apply.md](late-binding-plan-apply.md).

## Node Candidate Construction

`available_nodes()` currently includes nodes where:

- `node.availability == "available"`

Nodes are sorted by ID before planning. The sorted list and each node's
free-capacity pool are cached in the scheduler read model and shared by
reference, so a plan's snapshot costs refcount bumps rather than a
fleet-sized filter/sort/copy; any node mutation invalidates the cached
list and the next plan rebuilds it.

Per task, the candidate set is then built one of two ways:

- **Scan** (the baseline): every available node is tested against the
  task's constraints, permit, and volume-driver requirements.
- **Scheduling index** ([scheduling-indexes.md](scheduling-indexes.md)):
  when the fleet is at or above `schedulerSettings.index_min_nodes` and a
  declared index binds the task's equality constraints, candidates come
  from that index — partition shard lookup plus a nearest-fit walk over
  free resources — and only those are re-verified with the same predicate
  the scan applies. The scan remains the authoritative fallback (unusable
  index, empty coverage, or a truncated list running dry), and `job plan`
  reports which path served each task.

When neither path yields a node with room, and the job's band outranks work
running on a node it is otherwise allowed on, a **preemption pass** runs:
victims are chosen, and if one is found the placement and the evictions it
pays for commit in the same epoch-CAS transaction. The pass runs *only* after
normal feasibility has failed, so a cluster that never preempts pays nothing
for it. See [preemption-and-eviction.md](preemption-and-eviction.md).

## Placement Constraints and Scoring

For each task, planner builds:

- `ConstraintSet` (hard filter)
- `PermitSet` (hard allowlist/restriction checks)
- `AffinitySet` (soft scoring)
- `DistinctTracker` (for `distinct` constraints)
- Resource request from normalized task resources

Selection policy:

- built-in `bin_pack` or `spread`, or loaded scheduler plugin
- optional `nodes_considered` random subset before scoring
- optional bounded candidate scoring (`score_sample`, below)
- optional score jitter on reschedule mode for less deterministic stickiness

Resource fitting is enforced before node can be selected.

### Bounded candidate scoring (`score_sample`)

By default every placement scores **every** feasible node — exact
placement, at a per-placement cost that grows with fleet size. On large
fleets, `schedulerSettings.score_sample` bounds it:

```nickel
{
  schedulerSettings = {
    score_sample = { min_nodes = 1024, sample = 128 },
  },
}
```

Below `min_nodes` eligible candidates, behavior is exactly the default
(exact scoring). At or above it, the placement scores `sample` uniformly
drawn candidates and takes the best — best-of-k selection, which keeps
per-placement cost flat at any fleet size. Two properties hold by
construction:

- **Feasibility is never sampled.** Constraints, permits, distinct
  rules, and resource fit always run in full; a sampled candidate is
  always a valid placement. Only the score *ranking* is approximate.
- **The trade is visible.** `job plan` explain output notes
  `scored best of N sampled nodes` on any task whose placements were
  sampled, and the packing-quality cost is bounded by the
  power-of-k-choices property (the repository carries a regression test
  asserting the bound; the measured numbers live in
  `benchmarks/scheduler/README.md`).

Validation: `sample >= 2` and `min_nodes >= sample`, rejected at plan
time otherwise. Only the built-in `bin_pack`/`spread` policies sample;
plugin policies always receive the full eligible list. When both
`nodes_considered` and `score_sample` are set, `nodes_considered`
applies first (it is a hard cap); combining them is not recommended.
This knob is off unless configured — exact scoring is the default at
every fleet size.

## Dependency-Aware Task Readiness

Before placement, tasks are filtered via dependency graph and current task-status map:

- topological ordering rejects cycles
- dependency conditions:
  - `start`
  - `complete`
  - `success`

Only ready tasks are considered in that scheduler pass.

## Eval Triggers into Scheduling

Typical triggers:

- `run_job` -> enqueue `job_submit` / `job_update` / `rollout_tick`.
- Worker failure heartbeat -> enqueue delayed `job_reschedule`.
- Dynamic node updates -> enqueue `job_update` for affected jobs.
- Rollout engine -> enqueue delayed `rollout_tick` while active.

## Observability: the Scheduler Event Stream

Every observable scheduler action — evals enqueued/completed/failed, plan
applies, allocation removals/failures, node and job lifecycle transitions, and
rollout activity — is recorded as a scheduler event and exposed via
`legion events list` / `legion events follow`, the `legion_events` MCP tool,
the UI Events page, and the `ListEvents`/`StreamEvents` gRPC methods.

See [Event Stream](event-stream.md) for the kind catalog and consumption
patterns.

## Known Boundaries

- Queue dedupe is scoped to job/node IDs plus kind, not full payload
  equivalence: two `job_reschedule` evals for the same job collapse even when
  they were enqueued for different reasons.
- Multi-manager shared queue/consensus is not implemented.

