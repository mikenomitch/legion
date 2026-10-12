# Rollouts

A rollout replaces a job's running allocations with allocations from a new version of its config. You control how fast it moves, whether it waits for health checks, and what happens when it gets stuck. This page covers the jobspec fields, how each step is computed, and how to watch and advance a rollout from the CLI, gRPC API, and UI.

## What starts a rollout

Legion creates a rollout when a job update produces a new job config version. This happens both on `job run` and on KV-driven job updates.

On update, the manager:

1. Persists a `rollout` row linking `from_job_config_id` to `to_job_config_id`.
2. Persists rollout config and per-group overrides in `rollout.config_json`.
3. Writes a rollout event (`Rollout started` or `Rollout created; waiting for manual start`).
4. Enqueues `rollout_tick` eval for auto progression, or waits for manual advance in manual mode.

## Rollout config

Rollout config lives on the jobspec:

- Global: `rollout`
- Per-group overrides: `rolloutByGroup.<group_name>`

See a full example in `configs/jobs/maximal.ncl`.

### Fields

| Field | Type | Default | Notes |
|---|---|---|---|
| `strategy` | `string` | `"default"` | Built-in default strategy. Non-default values are plugin names. |
| `progression` | `auto \| manual` | `auto` | `manual` requires explicit `advance` before step 1 and each step. |
| `replacement` | `destroyFirst \| createFirst` | `destroyFirst` | Controls whether old allocations are retired before or after replacement placement. |
| `inPlace` | `bool` | `false` | Prefer replacing on same nodes as retiree allocations. Falls back to cross-node placement when needed. |
| `percentEachStep` | `u32` | unset | Step mode. Repeats fixed percent increments until 100%. |
| `countEachStep` | `u32` | unset | Step mode. Repeats fixed count increments until done, with a smaller final step if needed. |
| `percentSteps` | `u32[]` | unset | Step mode. Cumulative percentages, strictly increasing, must end at 100. |
| `countSteps` | `u32[]` | unset | Step mode. Per-step increments. Final rest step is implicit if counts do not cover desired size. |
| `stepStagger` | `duration` | unset | Delay between finishing one step and starting next step in a group. |
| `allocationStagger` | `duration` | unset | Delay between allocation mutations inside rollout processing. |
| `minHealthyTime` | `duration` | `0s` | New target allocations must remain healthy for at least this window before step advances. |
| `healthyDeadline` | `duration` | unset | Deadline from step start for health gate success. |
| `ignoreHealthChecks` | `bool` | `false` | If true, skips allocation health gating. |
| `capacityBlockTimeout` | `duration` | unset | Timeout while placement is blocked due to capacity/constraints. |
| `onCapacityTimeout` | `rollback \| fail \| completeIncomplete` | `fail` | Action when `capacityBlockTimeout` expires. |
| `rollbackOnFailure` | `bool` | `false` | If true, `fail` actions trigger rollback. |
| `onHealthDeadline` | `rollback \| fail \| pause` | `rollback` | Action when `healthyDeadline` expires. |
| `batchKillAfter` | `duration` | unset | For batch tasks, maximum wait before active old allocations are force-retired during rollout. |
| `slots.order` | `parallel \| sequential` | `parallel` | Slot-mode stateful groups only. `sequential` replaces one slot per step, highest ordinal first, each step health-gated. |
| `slots.updateFrom` | `u32` | unset | Slot-mode stateful groups only. Staged rollout floor: only slots `>= updateFrom` are updated, and lower slots keep their current allocations. Requires `slots.order = "sequential"` and must be `< count.num`. |

### Group override semantics

- `rolloutByGroup` is keyed by task group name.
- Unknown groups are rejected by validation.
- Group overrides are merged onto global rollout config.
- If a group override sets any step mode, it replaces the global step mode selection (and clears a global sequential `slots` block).
- If a group override sets a sequential `slots` block, it replaces the global step mode selection. A `slots` override always replaces the global `slots` block as a whole.

## Validation rules

Legion validates rollout config when you submit the job.

- You cannot define conflicting step modes at once.
- Only one of these may be set:
`percentEachStep`, `countEachStep`, `percentSteps`, `countSteps`.
- `percentEachStep` must be `1..=100`.
- `countEachStep` must be `>= 1`.
- `percentSteps` must be non-empty, strictly increasing, values `1..=100`, and end with `100`.
- `countSteps` must be non-empty and all values `>= 1`.
- `capacityBlockTimeout` and `healthyDeadline` must be greater than zero when set.
- `rollout.strategy` and each `rolloutByGroup.<group>.strategy` are validated at submit time.
- `slots.order = "sequential"` cannot be combined with a step mode in the same scope.
- `slots` is only valid for groups with a slot-mode stateful task.
- `slots.updateFrom` requires `slots.order = "sequential"` and must be less than the slot task's `count.num`.

## Step semantics

- If no step mode is configured, rollout is one step (`100%`).
- `percentEachStep = 10` yields `10,20,30,...,100`.
- `countEachStep = 2` on desired count `5` yields `2,4,5`.
- `percentSteps = [10,20,50,100]` is used directly.
- `countSteps = [1,2]` on desired count `10` yields cumulative `1,3,10` (implicit final rest step).

For each group, step fractions are applied to each task in that group:

- Group base count for step math is the max desired count among tasks in the group.
- Per-task target at each step is `ceil(task_desired * step_fraction)`.

This keeps sidecars and primary tasks in the same group moving together.

## Ordered and staged slot rollouts (stateful groups)

Groups with a slot-mode stateful task can opt into ordinal-aware rollouts:

```nickel
rollout = {
  slots = {
    order = "sequential",  # one slot per step, highest ordinal first, health-gated
    updateFrom = 2,        # optional: only slots >= 2 are updated (staged/canary)
  },
}
```

- `order = "sequential"` works like the K8s StatefulSet `OrderedReady` update. It replaces one slot per step, from `count-1` down to `0`. The next slot starts only after the previous replacement passes the health gate. An unhealthy replacement halts the rollout at that slot. Standard failure behavior applies: the rollout waits, or follows `onHealthDeadline` when `healthyDeadline` is set.
- `updateFrom` works like the K8s `partition` field. The rollout completes with slots below the floor still running their previous config. Resubmit with a lower floor to update the remaining slots (canary-promote).
- Each slot's replacement re-binds the same instance id and storage handle. Replacement is always stop-then-start, and `createFirst` is rejected for stateful groups.
- Rollout events name the slot for each step. `GetRolloutStatus` `total_steps` equals the number of slots being updated.

For full semantics, the canary-promote walkthrough, and validation rules, see "Ordered rollouts for slot pools" in [stateful-workloads.md](stateful-workloads.md).

## Replacement behavior

`replacement` controls order:

- `createFirst`: place replacements, then health-gate, then retire old allocations.
- `destroyFirst`: retire old allocations first, then place replacements, then health-gate.

`inPlace = true` behavior:

- Scheduler first attempts replacement placement constrained to retiree nodes.
- If same-node placement cannot be satisfied, rollout falls back to regular cross-node placement.
- `inPlace = false` still allows same-node placement. It only removes the preference for it.

## Manual vs auto progression

- `auto`: rollout starts immediately after creation.
- `manual`: rollout starts in `waiting_manual_start` and requires explicit first advance.
- Each subsequent manual step also requires advance credit.
- `advance` with no group list advances all groups.
- `advance` with one or more groups targets only those groups.

Surface area:

- gRPC: `AdvanceRollout(id, groups, priority)`
- CLI: `job rollout advance --id <id> [--group <group>]...`
- UI: Rollouts page `Advance` control with `all groups` or specific group selection.

## Health gating and deadline behavior

Health checks come from allocation health snapshots (`allocation_health` table).

- If `ignoreHealthChecks=true`, step health gate is bypassed.
- Otherwise, each step waits until target replacement allocations are healthy.
- `minHealthyTime` requires healthy stability before advancing.
- `healthyDeadline` is measured from step start.
- `onHealthDeadline` actions:
`rollback` (default), `fail`, `pause`.
- If action is `fail`, `rollbackOnFailure=true` upgrades fail to rollback.

Manual progression health visibility:

- gRPC `GetRolloutStatus` includes per-allocation health and status.
- CLI `job rollout status --id <id>` prints group and allocation health/status.
- UI Rollouts detail page shows per-allocation state and health.

## After a rollout fails, rolls back, or is stopped

A rollout that ends in `failed` or `cancelled` leaves the job's latest config
pointing at the new version, whatever is actually running. The manager's
background reconciliation (the pass a new raft leader runs, see
[scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)) converges what the
rollout left behind instead of the latest config:

- **Rolled back** (or failed before placing anything): the new version has no
  allocations, so the previous version is what runs. A rollback removes the
  new version first. If the previous version then cannot be placed back (no
  capacity, a lost conflict race), a `job_reschedule` for the previous
  version is queued 5 seconds out. An automatic rollback still ends the
  rollout `failed`. A `StopRollout` with `revert` returns the error and
  leaves the rollout in its current status, so retry the stop. Reconciliation tops up
  the previous version, for example after a node loss. It never re-places the
  rolled-back version.
- **Failed after replacing everything**: only the new version runs, and
  reconciliation keeps it at count.
- **Stopped part way without a revert** (`stop` with `revert = false`): the
  mix of old and new allocations stays as it is. Reconciliation does not
  pick a side. Resubmit the job or start a new rollout to move it.

Known limitation: a rollback does not create a new job version. Paths that
act on the job's latest config still target the rolled-back version. A forced
reschedule of an unchanged spec (`job run --force-reschedule`), a KV-triggered
restart, and `legion job scale` all place the latest config. To move a
rolled-back job forward, submit a corrected spec.

## A newer version supersedes a running rollout

A rollout always targets the job's latest config when it starts. A newer
version can arrive while it runs: `job run` supersedes it explicitly, but a
KV-triggered restart or in-place update re-renders the job without going
through `job run`. A rollout whose target is no longer the job's latest config
must not keep ticking. It treats every allocation outside its target as old,
so it would retire the newer version's allocations and then place nothing,
because the scheduler skips a superseded config.

So every rollout tick first checks its target. When the target is stale, the
rollout ends `cancelled` with the message `Rollout superseded: job config
<latest> replaced its target <old>` (also written as a `warn` rollout event).
Scale-ins it was deferring are released, and a `job_reschedule` eval for the
latest config re-places anything already retired. A job that was stuck this
way (0 allocations, rollout `running` forever, scale-outs disappearing within
seconds) recovers on the rollout's next tick.

## Capacity blocking and incomplete completion

When replacements cannot place:

- Rollout waits and retries.
- If `capacityBlockTimeout` is set and expires, `onCapacityTimeout` decides outcome:
`rollback`, `fail`, or `completeIncomplete`.

`completeIncomplete` behavior:

- Rollout ends with status `completed_incomplete` and `incomplete=true`.
- Job remains `running`. The rollout metadata shows that it is incomplete.
- Background reconciliation attempts continue when capacity becomes available again.

Reconcile loop:

- Manager node checker periodically calls `reconcile_incomplete_rollouts`.

## Lifecycle-specific behavior

- `service` tasks: standard rollout behavior.
- `batch` tasks: old batch allocations are not killed by default during rollout. The rollout waits for them to finish.
- `batchKillAfter`: if set, old batch allocations are force-retired after that elapsed time in-step.
- `sidecar` tasks: rollout follows the group progression and updates in lockstep with the group.

## Scheduler and eval loop integration

Core flow:

1. Rollout creation records desired config and runtime state envelope.
2. `rollout_tick` eval is claimed by eval workers.
3. Tick builds per-group plans and snapshots allocations/health once per tick.
4. Tick decides mutation/wait/terminal action per group.
5. Scheduler is invoked with per-task target counts and optional in-place exclusions.
6. Allocation dispatch and task status updates run after successful placement.
7. Old allocations are retired according to replacement mode and lifecycle rules.
8. Rollout status/events/runtime JSON are persisted.
9. Next delayed `rollout_tick` is enqueued when rollout must continue.

Eval handler paths:

- `Eval::handle(..., kind="rollout_tick")` -> `manager::rollout::handle_rollout_tick`
- On rollout tick failure, rollout is marked `failed` and an error event is appended.

## Rollout statuses

Common rollout statuses:

- `pending`
- `running`
- `waiting_manual_start`
- `waiting_manual_advance`
- `paused`
- `completed`
- `failed`
- `completed_incomplete`

## APIs and commands

### gRPC

Manager service supports:

- `ListRollouts(job_id?, active_only)`
- `GetRollout(id)`
- `GetRolloutStatus(id)` with per-group and per-allocation details
- `ListRolloutEvents(rollout_id, limit?)`
- `AdvanceRollout(id, groups[], priority?)`

`ListRollouts` supports global listing by leaving `job_id` unset.

### CLI

Commands are under `job rollout`:

- `cargo run -- job rollout list [--job <job_id>] [--active] [--json]`
- `cargo run -- job rollout status --id <rollout_id> [--json]`
- `cargo run -- job rollout events --id <rollout_id> [--limit <n>] [--json]`
- `cargo run -- job rollout advance --id <rollout_id> [--group <group>]... [--priority <n>|--asap] [--json]`

`job rollout list` without `--job` returns all rollouts globally.

### UI

Rollout visibility is in two places:

- Home page shows active rollouts.
- Rollouts page shows active and historical rollouts.

Rollouts page supports:

- Filtering by job and status.
- Active-only toggle.
- Rollout detail with per-group step state.
- Manual advance across all groups or per-group.
- Allocation grid view (small box per allocation) with hover details (`allocation id`, task, role, node, status, health).
- Large-scale fallback when group allocation count is large.
- Filterable rollout event log.

## Retention

Manager setting:

- `rolloutRetentionDays` in manager config.
- Default is `30`.

Behavior:

- Completed rollouts older than retention are pruned at manager startup.
- Rollout events are deleted automatically with rollout rows (`ON DELETE CASCADE`).

## Strategy plugins

Built-in strategy:

- `strategy = "default"` (or omit strategy)

Custom strategies:

- Implement `RolloutStrategyPlugin` and export `get`.
- Place plugin at `plugins/rollouts/<strategy>/target/release/lib<strategy>.<ext>`.
- Strategy resolves effective `RolloutConfig` per group at runtime.

References:

- Plugin contract doc: `docs/rollout-strategy-plugins.md`
- Example plugin: `plugins/rollouts/slow_start/src/lib.rs`
- Example jobspec: `configs/jobs/rollout-plugin-slow-start.ncl`

