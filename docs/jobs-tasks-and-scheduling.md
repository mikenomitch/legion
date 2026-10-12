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
- Dependencies evaluate against the task statuses workers report. A task
  whose dependencies are not met yet is **held**: it is not placed, and it
  does not count as short of capacity.

Dependency conditions:

| Condition | Met when the dependency… | Typical use |
| --- | --- | --- |
| `start` | has started, or is being placed in the same pass | a sidecar beside its primary |
| `complete` | has finished, whether it exited 0 or not | a cleanup step that runs either way |
| `success` | has finished with a successful exit (`successExitCodes`, default `0`) | the next step of a pipeline |

`complete` and `success` are met only by a reported exit. A batch dependency
that has merely been placed, or is still running, holds its dependents. A
batch task with several replicas finishes when every replica has: it is
`success` once all of them exited successfully, and `failed` as soon as one
fails for good.

Held tasks move on their own. When a worker reports that a task with
dependents reached a terminal state, the manager queues an eval for the job:
a `job_reschedule`, or a `rollout_tick` when the config is being rolled out
to, so the rollout's step and health gating still apply. That eval places
whatever the exit made ready. A three-step pipeline therefore runs one step
at a time:

```nickel
tasks = [
  { name = "fetch",    lifecycle = "batch", ... },
  { name = "validate", lifecycle = "batch",
    dependsOn = [{ task = "fetch", condition = "success" }], ... },
  { name = "serve",    lifecycle = "service",
    dependsOn = [{ task = "validate", condition = "success" }], ... },
]
```

`validate` is placed only after `fetch` exits 0, and `serve` only after
`validate` does. If `validate` fails for good (it exits unsuccessfully under
`restart = { policy = "never" }`, or its reschedule budget runs out), `serve`
never starts and the job reads `failed`.

A restart of the whole job (a batch `schedule`'s next run,
`legion job run --force-reschedule`, a KV `restart`) resets every task to
`pending` first, so the previous run's results never satisfy the new run's
conditions.

`legion job plan` is a dry run and shows where every task would eventually
land: it treats batch dependencies as if they will succeed.

Diagnostics: an eval that leaves tasks held says so in its outcome
(`legion events list --job <job>`, the `eval.completed` message ends with
`waiting on dependencies: validate, serve`).

Each step needs room only while it runs. A finished allocation (`complete` or
`failed`) keeps its row, so its task's slot stays spent, but it releases its
CPU, memory, disk, allocation count, and static ports the moment its exit is
reported. On one 8 GiB node, a pipeline whose steps each need 6 GiB runs every
step, one after another.

Known limitations:

- A runtime that cannot report exits (`noop`, or a third-party plugin that
  does not supervise a process) reports a batch task complete as soon as it
  starts, so its dependents follow right away (see
  [Task Supervision](task-supervision.md)).

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

## Content Locality

A task whose artifacts declare a `checksum` has a *content set*: the digests
its allocation will need on its node. The scheduler prefers nodes whose
content store already holds them, scored as the fraction of the set's bytes
the node holds (`0.0` to `1.0`). A node that has not reported its content
scores `0`. A task without digest-pinned artifacts skips the term. See
[artifacts.md](artifacts.md#content-locality) for how nodes report.

## Score Weights

Every candidate node's score is a weighted sum. Set the weights under
`schedulerSettings.weights` in the manager config:

```nickel
{
  schedulerSettings = {
    weights = {
      affinities = 1.0,              # affinity rules and same_node_as
      automatic_anti_affinity = 0.2, # subtracted per stacked allocation
      locality = 1.0,                # content locality; 0 disables
      # spread / bin_pack default to 1.0 for the matching policy, else 0.
    },
  },
}
```

```
score = affinities * affinity
      + spread * (emptiness relative to the other candidates)
      + bin_pack * (fullness relative to the other candidates)
      - automatic_anti_affinity * stacked allocations
      + locality * content locality
```

Weights must be non-negative numbers. Unknown keys are ignored. The highest
score wins, with exact ties broken at random.

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

A plan that finds no capacity does not fail its eval: the eval waits
**blocked**, keyed by what it needs (the task's pool and pinned attributes,
the node a resume is pinned to, or `escaped` when there was no node at
all), and new capacity on a node that could host it returns it to the
queue. Capacity means a node registering, a node being uncordoned or having
its pool or attributes change, a node returning to `ready`, or allocations
leaving a node (a stop, a destroy, a scale-in, a suspend, a preemption). A
node in another pool wakes nothing. `legion job info` reports such a job as
`blocked` with what it waits for. The full model, its keys, and its
limitations are in
[Blocked evals](scheduler-and-eval-loop.md#blocked-evals).

The capacity pass remains as a backstop for jobs that are short without a
blocked eval:

- Worker registration (`worker_register`) inserts the node, then queues a
  capacity pass: one `node_join` eval with no node. Uncordoning a node
  (`legion node uncordon`) queues the same pass.
- The pass re-evaluates, with a `Reschedule` plan, every active job that has
  no live rollout and nothing already queued or blocked, when its config has
  a `count.per` task, a service or sidecar task with fewer allocations than
  its effective count, or a keyed instance waiting for placement. It also
  tops up rollouts that completed incomplete. After a failed or cancelled
  rollout it converges what the rollout left running, never the rolled-back
  version (see
  [rollouts.md](rollouts.md#after-a-rollout-fails-rolls-back-or-is-stopped)).
- One pass covers any number of joins. While a pass is pending, further
  registrations queue nothing, so a burst of new workers costs one pass.
- The `node_join` eval with a node (used in eval-driven flows and tests) marks
  or inserts the node and then runs the same pass.

Known limitations:

- Batch tasks are not re-evaluated by the pass, because a batch task that
  already ran to completion looks the same as one that never placed. A
  batch task whose placement blocked is woken like any other.
- A rollout tick that cannot place still fails its rollout rather than
  blocking.

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

The worker never sees the jobspec, so the manager sends each task's
`lifecycle` and `restart` block with every dispatch
(`TaskAllocation.task_lifecycle` / `task_restart`). With no `restart` block,
or one that leaves out `policy`, the policy follows the lifecycle:

| Lifecycle | Policy | Exit 0 (or another `successExitCodes` entry) | Any other exit |
| --- | --- | --- | --- |
| `batch` | `onFailure` | allocation `complete`, task `success`; not restarted | restarted in place while the restart budget lasts, then failed and rescheduled |
| `service`, `sidecar` | `always` | restarted in place | restarted in place, then failed and rescheduled |

A batch task's terminal exit reaches the manager on the worker's next
heartbeat (`HeartbeatRequest.terminated_allocations`), which is what moves its
task status from `running` to `success` (or `failed` for a `never`-policy task
that exited unsuccessfully), the status `dependsOn` conditions read. The
completed allocation keeps its row and is never dispatched again, but it no
longer holds node resources or ports. In
`legion job events`, a finished batch task appears as `allocation.terminated`;
a task that failed for good appears as `allocation.failed` with the exit
status and the last lines the task wrote to stderr (for example `exited with
status 1; last stderr: Error: ... 429 Too Many Requests`).

### Worker-side health loop

For each allocation with checks:

- runs boot, readiness, and health checks on intervals
- tracks failure windows (`unhealthy_after_count` and an optional timespan)

When the unhealthy threshold is reached:

1. Restart locally if `on_unhealthy.restart_count/restart_period/restart_delay` allows it.
2. If restarts are exhausted, record an allocation failure.

Health checks start when the task starts, including tasks that stage
artifacts first (every container image when `containerImageDesugar` is on).
They never start for a task whose staging failed: that allocation fails with
`artifact staging failed: ...` and an `artifact.failed` event per artifact.

While the task's process is down (it exited and the supervisor is about to
restart it, or it has finished), failed checks are not counted. A check
against a dead process can only fail, and the exit is the real cause, so the
supervisor's verdict is the one the allocation reports. After the supervisor
relaunches the task, the checks start over with a fresh boot grace window.

### Manager-side move logic

The worker reports failed allocations in its heartbeat (from a failed health
check, a failed restart, or a bad exit).
The manager then:

1. removes the failed allocation row
2. checks move history per `(job_config_id, task_config_id)`
3. if `move_count` is exceeded within `move_period`, marks the job failed and
   the task's status `failed`
4. otherwise, enqueues a delayed `job_reschedule` after `move_delay`

So failures escalate in order:

- local restarts first,
- then a reschedule, preferably to another node,
- then job failure once the move budget is used up.

The reschedule **avoids** the node the allocation failed on rather than
excluding it. It first plans without that node; whatever that pass cannot
place is planned again with the node allowed. On a one-node cluster, or when
the failed node is the only one with room, the replacement goes back to the
same node instead of failing with `No nodes available to place allocations`.

## Batch Scheduling Notes

Batch tasks can set `schedule` (`every` or cron). After a submit, update, or reschedule:

- the scheduler computes the delay until the next run for batch tasks,
- it enqueues a delayed `job_update` eval for the next run.

`task.schedule` is rejected for any lifecycle other than `batch`.

## Job Status

`Job.status` is what `legion job list`, `legion job info`, `ListJobs`,
`GetJobInfo`, the MCP job tools, and the UI show. The manager works it out
from the job's workload each time it is read, so a service reads `running`
and a finished batch job reads `complete`:

```console
$ legion job list
NAME      STATUS     PRIORITY  ...
web       running    normal
nightly   complete   normal
report    scheduled  normal
migrate   failed     normal
```

The first matching row wins:

| Status | Meaning |
| --- | --- |
| `stopped` | `legion job stop` stopped the job. `legion job run` brings it back. |
| `failed` | A required KV key vanished (`onDelete = "fail"`; see [KV store](kv-store.md#fail-semantics-and-recovery)). |
| `running` | At least one allocation of the job is live: placed and not yet terminal. |
| `failed` | Nothing is live, and a task's last run failed: it spent its restart and move budgets, or exited unsuccessfully under a policy that does not restart it ([task supervision](task-supervision.md)). |
| `blocked` | Nothing is live, and the job's placement found no room: its eval waits [blocked](scheduler-and-eval-loop.md#blocked-evals) for capacity that could host it, and the job places on its own when that capacity appears. `Job.blocked` says what it waits for. |
| `pending` | Nothing is live, and a task that wants an allocation hasn't finished. It is waiting to be placed or waiting on a `dependsOn` condition. |
| `scheduled` | Nothing is live or pending, and a cron task (`schedule`) finished its last run and is waiting for the next. |
| `complete` | Nothing is live, and every task that wants an allocation finished successfully. |
| `suspended` | No task wants an allocation, and at least one keyed instance is [suspended](suspend-and-resume.md). |
| `idle` | No task wants an allocation: every task is scaled to `count = 0`, or a keyed task has no pending, active, or rescheduling instances. |

How the inputs are read:

- **Live allocations count across every version of the job.** A job in the
  middle of a rollout still has old-version allocations serving, so it reads
  `running`.
- **Task outcomes come from the latest version.** Submitting a new version
  of a finished batch job moves it back to `pending` until the new tasks
  place, then `running`, then `complete`.
- **A failed task wins over its blocked dependents.** If `migrate` failed and
  `api` waits on `migrate` succeeding, the job reads `failed`, not `pending`.
- **A task wants an allocation** when its `count` is above zero, or, for a
  keyed stateful task, when it has at least one pending, active, or
  rescheduling instance.
- **`blocked` takes the place of `pending`** only. A job with something live
  reads `running` even while another of its placements waits for room; its
  `blocked` block (`legion job info -j`, the UI's job page) still says what
  that placement waits for.

`legion job list --status <value>` and `ListJobsRequest.status` filter on the
derived status, and `ListJobsReply.status_counts` counts it.

### The stored lifecycle flag

The state store's `job.status` column is not the derived status. It is a
lifecycle flag the derivation starts from: `active` from submit until the job
is stopped, `stopped` after `legion job stop`, and `failed` after a KV
dependency failure. A `legion state query` against the `job` table returns
the flag, so count allocations when you want the workload (see
[Diagnosing scheduler performance](diagnosing-scheduler-performance.md)).

### Known limitations

- **One outcome per task, not per replica.** A task's outcome is the last
  exit any of its allocations reported. In a batch task with `count = 3` where
  one replica failed and the last one to exit succeeded, the job reads
  `complete`. The allocations list has the per-replica truth.
- **Lost nodes.** An allocation on a node that is lost but not yet
  rescheduled still counts as live, so the job reads `running` until the
  manager replaces it.
- **Old workers.** A worker older than protocol version 2 never reports exits.
  Its batch allocations stay placed, so a batch job on such a worker reads
  `running` indefinitely.
- **Read cost.** `ListJobs` derives every job's status with three aggregate
  queries per call: over allocations, latest-version tasks, and instances.

### Re-running a job

`legion job run` with a spec identical to the job's latest version normally
changes nothing. When nothing of the job is running, it starts the job again
instead (a `job_update` eval runs it from the top) and says why:

```text
Job config unchanged; job was stopped, starting it again
Job config unchanged; job had failed, starting it again
Job config unchanged; job was not running, starting it again
Job config unchanged; no update needed (the job is running; use --force-reschedule to restart it)
```

"Not running" means no live allocation and no placing eval queued for the
job, so a finished batch job runs again and a job that could not be placed
gets another attempt.

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
| Job brought back from `stopped` (resubmit) or a KV `failed` (recovery) | yes |
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
