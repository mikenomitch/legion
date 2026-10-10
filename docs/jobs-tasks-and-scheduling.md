# Jobs, Tasks, and Scheduling Semantics

This page describes how jobs are represented and how scheduling works, including edge cases.

Jobs reach the manager either as a rendered spec (`legion job run`, which
evaluates [Nickel](configuration-language.md) client-side) or through a
[job template](job-templates.md), where the cluster stores the spec and the
submitter supplies typed variables. Both paths go through the same
validation, admission, and scheduling described below. Templates only
change how the spec is produced and who is allowed to submit it.

## Job Spec Shapes

`JobConfig` supports:

- `task` (single-task shorthand)
- `tasks` as list
- `tasks` as map
- `priority`: the job's importance band (`critical` / `high` / `elevated` /
  `normal` / `low` / `background`, default `normal`). It orders the eval queue.
  It also decides what this job may displace when the cluster is full, and
  what may displace it. See
  [preemption-and-eviction.md](preemption-and-eviction.md).

Task names:

- map keys become task names
- list tasks default to `task-<n>` unless they set `name`

## Task Fields That Affect Placement

Important `TaskConfig` fields:

- `stop`: the signal and grace window used to stop this task, whatever the
  cause ([graceful-stop.md](graceful-stop.md)).
- `disruption.minAvailable`: the minimum number of this task's allocations
  that voluntary disruption (preemption) must leave running
  ([preemption-and-eviction.md](preemption-and-eviction.md)).
- `count`:
  - integer count
  - or `{ num, per }` for per-attribute placement
- `group`: task grouping for rollouts and automatic anti-affinity
- `runtime`: task-runtime plugin (`exec`, `container`, etc.)
- `resources`: normalized by the manager's `ResourcePolicy`
- `constraints`: hard placement filter
- `permit`: hard node restriction policy
- `affinities`: soft scoring preferences
- `dependsOn`: task dependency graph
- `lifecycle`: `service`, `batch`, `sidecar`
- `schedule`: only valid for `batch`
- `checks` and `on_unhealthy`: health and restart/reschedule policy
- `restart`: what happens when the task process exits (`always` /
  `onFailure` / `never`, delay, `successExitCodes`). Shares the
  `on_unhealthy` restart budget. See
  [Task Supervision](task-supervision.md).
- `stateful`: opt-in stable instance identity for the task's replicas. In
  `slot` mode each replica gets an ordinal slot `0..count-1` whose instance id
  and storage handle survive reschedules. Replacements reuse the same slot.
  See [Stateful Workloads](stateful-workloads.md) for the full job-spec
  reference, env vars, and scale-in/out behavior. Tasks without a `stateful`
  block are unaffected.
- `kv` / `env` / `files` (KV injections): KV-backed env vars and rendered
  files with per-reference `watch`/`onChange`/`onDelete` policies that can
  restart, roll out, or fail the job when watched keys change or are
  deleted. The full details (policy precedence, debounce, fail/recovery,
  per-runtime capability matrix) are in the
  ["Reactive updates" section of the KV store doc](kv-store.md#reactive-updates-kv-driven-task-updates).
- `identities`: named workload identities (OIDC). For each declared
  identity, the manager mints a short-lived, asymmetrically signed JWT per
  allocation, with claims taken from the allocation's actual scheduling
  facts. Use it to federate with external systems (AWS
  `AssumeRoleWithWebIdentity`, GCP Workload Identity Federation, Vault JWT
  auth). Each identity has its own audience, TTL, delivery (env and/or
  file), and change mode (`noop`/`signal`/`restart`). See
  [Workload Identity](workload-identity.md) for the full spec reference,
  claim details, and provider examples. Tasks without `identities` are
  unaffected.

## Task Lists and Dependency Readiness

Scheduling uses dependency-aware readiness:

- The DAG is topologically sorted.
- Cycles are rejected.
- Dependencies evaluate against current task statuses and already-planned tasks.

Dependency conditions:

- `start`: the dependency has started or is being placed now.
- `complete`: the dependency is complete or successful (or is a batch task being placed now).
- `success`: the dependency must not have failed and must be successful (or is a batch task being placed now).

Only ready tasks enter the current planning pass.

Lifecycle rules, enforced at submit and by `legion job validate`:

- Dependencies may cross groups for `service` and `batch` tasks (e.g. a
  service in one group waiting on a batch prep task in another).
- `sidecar` tasks are bound to their group. They run alongside their group's
  primary task, so they may only depend on tasks in their own group. A
  violation is rejected as
  `Sidecar task <x> depends on task <y> outside group <g>`. To gate the whole
  group on another group's task, put the dependency on the group's primary
  task instead. A sidecar also requires a non-sidecar primary task in its
  group.
- A `service` task cannot take a `complete` or `success` condition on
  another service, because services never exit. Only `batch` targets can
  satisfy those conditions.

See `configs/jobs/maximal.ncl` and `configs/jobs/dag-exec-mac-sidecars.ncl`
for valid examples of both the cross-group and sidecar shapes.

## Constraints, Permits, and Affinities

### Constraints

Constraints are hard filters from `ConstraintSet`.
Operators include:

- equality and numeric comparisons
- regex match
- semver match
- contains / contains_any
- is_set / is_not_set
- `distinct` (avoid reusing seen values for a key)

Node attribute lookup order:

1. `config_attrs`
2. `fingerprint_attrs`
3. `dynamic_attrs`

### Permit

A permit is a hard allowlist tied to node restrictions.
If `pool` is not specified, the permit defaults to `pool=default`.

Node restrictions are read from:

- implicit `pool`
- optional `config_attrs.restriction` map

### Affinities

Affinities add to a node's score but never exclude it. They include
regular affinity rules plus the `same_node_as` task colocation preference.

## Automatic Anti-Affinity (Subtle Behavior)

On top of explicit affinity rules, policy scoring applies an automatic
anti-affinity penalty.

The penalty for each candidate node is:

- `max(same_task_allocations_on_node, same_group_allocations_on_node)`

It is then weighted by the scheduler setting `automatic_anti_affinity`
(default `0.2`).

As a result, the scheduler prefers not to stack allocations of the same task
or group on one node. This is a soft preference, so it can still happen.

## count.per Behavior

When `count.per` is set:

- eligible nodes are grouped by that attribute's value,
- the scheduler targets `count.num` per group,
- in reschedule mode, it subtracts the count already placed for the same task and group.

If the attribute is missing on all candidates, placement fails.

## Group Co-Placement Behavior

When a job group contains multiple tasks:

- the planner builds group-level candidate sets from the combined constraints,
- assignments may be pinned so tasks in the same group and token land on the same node,
- conflicting earlier placements in the same group are treated as errors.

## Node Add/Loss and Reconciliation

### Node Added

Two paths exist:

A plan that finds no capacity fails its eval, and nothing retries it on a
timer. New capacity is what retries it:

- Worker registration (`worker_register`) inserts the node, then queues a
  capacity pass: one `node_join` eval with no node. Uncordoning a node
  (`legion node uncordon`) queues the same pass.
- The pass re-evaluates, with a `Reschedule` plan, every active job that has
  no live rollout and nothing already queued, when its config has a
  `count.per` task or a service or sidecar task with fewer allocations than
  its effective count. It also tops up rollouts that completed incomplete.
  After a failed or cancelled rollout it converges what the rollout left
  running, never the rolled-back version (see
  [rollouts.md](rollouts.md#after-a-rollout-fails-rolls-back-or-is-stopped)).
- One pass covers any number of joins. While a pass is pending, further
  registrations queue nothing, so a burst of new workers costs one pass.
- The `node_join` eval with a node (used in eval-driven flows and tests) marks
  or inserts the node and then runs the same pass.

Known limitations:

- Batch tasks are not re-evaluated, because a batch task that already ran
  to completion looks the same as one that never placed.
- Capacity freed by stopping or scaling in another job, or by a lost node
  heartbeating again, does not queue the pass. The next registration or
  uncordon does, and so does a manager leadership change, whose
  reconciliation reschedules every active job.

### Node Lost

- The manager's heartbeat checker marks stale nodes as lost.
- Allocations on the lost node are removed and the affected job configs reconciled.
- Reconciliation runs the scheduler in `Reschedule` mode, so replacements
  respect node capacity, constraints, group co-location, and the configured
  policy like any other placement. Each affected job config is reconciled on
  its own, so one job that cannot be placed does not block the others.
- A job whose replacement fails is never force-placed. It gets a
  `job_reschedule` eval delayed by 5 seconds, because the lost node no longer
  holds its allocations and the node monitor will not re-drive it. Drain
  evictions follow the same rule.
- The `node_drop` eval path does the same reconciliation.

## Cordon and Drain

Nodes have two independent settings: `status` (ready/active/lost, which
tracks health) and `availability` (available/drained, which controls
scheduling). All placement goes through `Scheduler::available_nodes`, and it
and the store's reconcile path only consider nodes with
`availability == "available"`. Changing availability removes a node from
**all** new placements without affecting its health, heartbeats, or running
allocations.

The `SetNodeAvailability` RPC (CLI: `legion node cordon|drain|uncordon <ID>`,
MCP: `legion_node_drain`, UI: the Cordon / Drain / Uncordon buttons on the
worker page) drives three operator flows:

- **Cordon** (`availability = "drained"`, `evict = false`): the node stops
  receiving new placements. Existing allocations keep running. Emits
  `node.cordoned`.
- **Drain** (`availability = "drained"`, `evict = true`): a cordon plus a
  `node_drain` eval that evicts the node's existing allocations
  (`Store::reschedule_allocations_for_drained_node`):
  - Stateless allocations are removed immediately, and the affected job
    configs are reconciled onto the remaining available nodes. The node is
    alive and sending heartbeats, so the next heartbeat reply shrinks its
    desired set and the worker stops the tasks cleanly. The lost-node
    confirmation delay does not apply.
  - Stateful allocations go through the same protection gate as node loss
    (`stateful.onLost`). A drained node that is still alive has no
    `lost_at`, so its loss duration is unknown and every stateful
    allocation is **held**. A drain never force-moves protected state. Held
    allocations keep running on the drained node until the node is actually
    lost (the lost-node recheck sweep then applies `onLost.replaceAfter` as
    usual) or an operator moves them with `legion instance replace`.
  - Emits `node.drained` with evicted/held counts.
- **Uncordon** (`availability = "available"`): the node re-enters the
  scheduling pool. Existing allocations elsewhere are not moved back
  automatically. The next reschedule or reconcile may use the node. Emits
  `node.uncordoned`.

The RPC is idempotent (optional `idempotency_key`), returns NotFound for
unknown nodes, and requires `node:write` on the node's pool.

## Dynamic Node Attr Update

`update_node_dynamic`:

- patches the node's dynamic attrs
- finds jobs allocated on that node
- enqueues `job_update` evals for those jobs

## Health Checks, Restarting, and Rescheduling

### Process exits

A worker's task supervisor polls each running allocation's process, reaps it on
exit, and applies the task's `restart` policy: restart in place, final
success, or failure into the reschedule path below. The exit code, not the
placement, decides a batch task's `Success`, so
`dependsOn = { condition = "success" }` means "exited 0". The full details,
including the version-skew rule for workers that predate exit reporting, are in
[Task Supervision](task-supervision.md).

### Worker-side health loop

For each allocation with checks:

- runs boot, readiness, and health checks on intervals
- tracks failure windows (`unhealthy_after_count` and an optional timespan)

When the unhealthy threshold is reached:

1. Restart locally if `on_unhealthy.restart_count/restart_period/restart_delay` allows it.
2. If restarts are exhausted, record an allocation failure.

### Manager-side move logic

The worker reports failed allocations in its heartbeat (from a failed health
check, a failed restart, or a bad exit).
The manager then:

1. removes the failed allocation row
2. checks move history per `(job_config_id, task_config_id)`
3. if `move_count` is exceeded within `move_period`, marks the job failed
4. otherwise, enqueues a delayed `job_reschedule` after `move_delay`

So failures escalate in order:

- local restarts first,
- then a reschedule to another node,
- then job failure once the move budget is used up.

## Batch Scheduling Notes

Batch tasks can set `schedule` (`every` or cron). After a submit, update, or reschedule:

- the scheduler computes the delay until the next run for batch tasks,
- it enqueues a delayed `job_update` eval for the next run.

`task.schedule` is rejected for any lifecycle other than `batch`.

## Job Timestamps: `created_at` and `updated_at`

Every job has two timestamps, both unix seconds on the wire (`Job.created_at`,
`Job.updated_at`). They appear in `GetJobInfo`, `ListJobs`, `legion job list`,
`legion job info`, and the `job` table in the state store.

- **`created_at`** is when the job's first spec version was accepted. It
  never changes. Submitting a new version, stopping the job, and failing it
  all leave it alone.
- **`updated_at`** is the job's mtime: when the job record last changed. The
  UI's **Updated** column shows it, and it is the default sort key for
  `ListJobs` and the Jobs page.

A job that has not changed since submission reports the two as equal.

### What moves `updated_at`

The mtime tracks changes to the job record. It does not track the workload running under it:

| Event | Moves `updated_at` |
| --- | --- |
| First submission | yes (set equal to `created_at`) |
| A new spec version is submitted | yes |
| A byte-identical spec is resubmitted | no (nothing changed) |
| `legion job stop` (status → `stopped`) | yes |
| Job marked `failed`, including a failed KV dependency | yes |
| Job recovered to `done` from `stopped` or `failed` | yes |
| A restart request is acknowledged | yes |
| An allocation is placed, replaced, or removed | **no** |
| Health checks, task-status writes, node heartbeats | **no** |

Allocation changes are excluded on purpose. On a healthy cluster, allocations
are rescheduled, health checked, and replaced all the time. If they moved the
job's mtime, every job would always read as "updated seconds ago" and the
column would be useless. To see workload activity, use the Allocations count
on the Jobs page, the job detail page, or the [event stream](event-stream.md).

In short, `updated_at` tells you when someone last changed the job. It does
not tell you whether the job is healthy.

### Sorting

```
legion job list                      # newest change first
```

`ListJobs.sort_by` accepts `updated_at` (default), `created_at`, `name`, and
`status`, with `sort_order` of `asc` or `desc`. Jobs with the same mtime (for
example, a batch submitted within one second) are sorted by name so paging
stays stable.

### Known limitations

- **Whole-second resolution.** Two changes to the same job within one second
  look the same, and a job changed twice in a second sorts as if it changed
  once.
- **Clusters upgraded from a manager that predates the column** backfill every
  existing job's `updated_at` to its `created_at`. For a job that changed
  before the upgrade, this reads as "not changed since submission" until the
  next change. The original value cannot be recovered because it was never
  recorded.
- **`legion job list -j` reports both timestamps as unix seconds**, matching the
  proto. The human-readable table renders them as RFC3339.
- **Other entities differ.** Nodes have no mtime. Their Updated column shows
  `last_heartbeat`, which changes every couple of seconds. Rollouts, KV
  entries, job templates, ACL policies, instances, and volumes each carry their
  own `updated_at` with entity-specific semantics.
