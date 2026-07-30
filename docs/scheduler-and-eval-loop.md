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

1. `claim_eval()` atomically marks one `pending` eval as `in_flight`.
2. Executes `Eval::handle(store)`.
3. `complete_eval()`:
   - success -> delete eval row
   - failure -> set status `failed`

When queue is empty, workers block on either:

- `Notify` wake-up (`enqueue_eval` of zero-delay work), or
- timeout until next delayed eval `ready_at`.

## Eval Queue Semantics

Eval ordering:

- Priority desc, then ID asc.

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
(`Store::submit_and_apply_plan`): one write-transaction boundary, and on
raft one propose→quorum-commit→apply instead of two.

### The epoch sample is lazy by default

The fence rule is that epochs must be sampled *no later than* the
capacity snapshot the plan is built against. Sampling every candidate
node up front satisfies that unconditionally but costs O(fleet) per
plan. By default the scheduler instead plans first and samples epochs
for exactly the scopes the plan touches (the job config plus the placed
nodes), accepting the late sample only when the read model's
**generation counter** — bumped on every allocation publish — proves
nothing changed since the capacity read. When something did change
mid-plan, the scheduler re-plans once with the eager whole-fleet sample
(unconditionally safe), counted by the
`legion_scheduler_lazy_sample_fallback_total` metric.

`schedulerSettings.lazy_version_sample: false` restores the eager sample
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

- `schedulerSettings.plan_apply_coalescing: false` restores one commit
  per plan.
- `schedulerSettings.plan_apply_batch_max` (default 16, max 128) caps
  plans per batch.

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

### Late binding: candidate lists with commit-time node choice

Concurrent planners reading the same snapshot compute the same scores
and pick the same argmax node; without a countermeasure, a burst of N
independent submits degrades into serial conflict-retry rounds. Legion's
answer is **late binding** (Sparrow-style, adapted to the store's apply
transaction): an eligible placement submits a *ranked candidate list*
instead of a single node, and the apply transaction binds to the **first
candidate whose node epoch is unchanged** since the planner sampled it.
A race then costs a rank instead of a conflict retry: racer one binds
the shared first choice, racer two sees its epoch moved and slides to
its next candidate, and so on — self-resolving, no coordination.

- Rank 0 is the planner's true argmax pick (with the usual exact-tie
  shuffle), so **uncontended placements bind rank 0 and pay zero quality
  tax**. Ranks 1..k−1 are drawn from the near-tie score band so racing
  planners decorrelate.
- The epoch is the fit proof: "unchanged" means no allocation was added
  or removed on that node since the planner verified fit there. A node
  that was touched but still fits is skipped conservatively — never a
  correctness risk, at most a near-equivalent lower rank.
- The per-candidate re-validation (availability, constraints, permits,
  network/static-port assignment) also *skips to the next candidate* on
  failure, so a cordon or port collision landing mid-race costs a rank
  instead of a rejection.
- The **job-config epoch stays a hard, all-or-nothing gate** — it is
  what serializes two plans for the same job config.
- **Coupled placements never fall back.** A placement carries a list
  only when its task places exactly one allocation in the plan, sits in
  no co-placement group, declares no `distinct` constraint, claims no
  volumes, and its list shares no node with any other placement in the
  plan. Everything else (count-N services, groups, pinned storage,
  plugin-policy placements, Reschedule mode) keeps the fixed single-node
  submission and all-or-nothing semantics. A plan whose placement
  exhausts its whole list is rejected and retried/requeued as a normal
  conflict.

Configuration (`schedulerSettings`):

```jsonnet
{
  schedulerSettings: {
    plan_candidates: 8,       // candidates per eligible placement; 0 disables
    // plan_candidates: { count: 8, band: 0.01 },  // full form
    plan_conflict_requeue_delay_ms: 1000,  // see "Conflict handling"
  },
}
```

`plan_candidates: 0` restores single-node submissions byte-for-byte —
also the mixed-version rollout hatch: format-2 submissions require every
manager to run a version that can parse them (the usual same-version
upgrade window; see [upgrades.md](upgrades.md)).

Observability: `allocation.placed` events carry a `candidateRank` per
placement and a `fallbacks` count; the
`legion_scheduler_plan_candidate_rank` histogram
([metrics.md](metrics.md)) shows how often placements land off their
first choice. A sustained nonzero conflict-rejection rate *with*
candidates on means the pool is physically contended (one node fits),
not scheduler-contended.

### Conflict handling

A stale epoch causes plan rejection with a conflict error. The eval handler retries conflicts with bounded attempts via `Eval::plan_with_conflict_retries`.

**Conflict exhaustion requeues — it never drops work.** An eval that
loses every retry is released back to `pending` with a jittered delay
(`schedulerSettings.plan_conflict_requeue_delay_ms`, default 1000 ms)
rather than terminally failed: a conflict is transient by definition,
and a saturated pool shows as visibly pending-and-retrying work in the
eval queue instead of a silently missing job. Terminal failure remains
for non-conflict errors. Requeues are counted by
`legion_scheduler_conflict_requeues_total`.

### Tuning conflict retries

Use manager `schedulerSettings.plan_conflict_max_retries` to tune retry behavior for high contention.

- default: `3`
- minimum: `1`
- maximum: `20`

Example manager config:

```jsonnet
{
  schedulerSettings: {
    plan_conflict_max_retries: 8,
  },
}
```

If you observe frequent conflict retries, increase this setting modestly and also scale by partitioning workload across more jobs/configs and spreading eval traffic.

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

```jsonnet
{
  schedulerSettings: {
    score_sample: { min_nodes: 1024, sample: 128 },
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

