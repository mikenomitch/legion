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

- After claiming an eval, pending evals with same `job_id` are deleted.
- Pending evals with same `node_id` are deleted.

This keeps the queue from growing with redundant work for the same entity.

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

### Conflict handling

A stale epoch causes plan rejection with a conflict error. The eval handler retries conflicts with bounded attempts via `Eval::plan_with_conflict_retries`.

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

Nodes are sorted by ID before planning.

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
- optional score jitter on reschedule mode for less deterministic stickiness

Resource fitting is enforced before node can be selected.

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

## Known Boundaries

- Queue dedupe is scoped to job/node IDs, not full payload equivalence.
- Multi-manager shared queue/consensus is not implemented.

