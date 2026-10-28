# Scaling Jobs

`legion job scale` changes how many copies of a task run. It does not create a
new job version or start a rollout. This is the manual half of autoscaling.
The inputs an automatic half needs are collected too: `GetScalingInputs`
(and `legion job info`) reports every task's replicas, measured CPU and memory
against its reservation, and service traffic, cluster-wide
([Scaling inputs](#scaling-inputs)). The `scaling` policy block that acts on
them, and node autoscaling, are not built yet.

## Why it exists

Editing `count` in a jobspec and resubmitting is a deploy. It creates a new
job config version, cancels any in-flight rollout, and starts a fresh one. If
you just want two more replicas during a traffic spike, you shouldn't get a
new version and a deployment. `job scale` instead writes a **count override**,
a separate record layered over the spec, and reconciles the running
allocations to match it.

## The surface

```
$ legion job scale web --count 6 --reason "launch traffic"
web/default/web: 4 -> 6 (spec count 2; override active)

$ legion job scale web --count 6          # idempotent re-issue
$ legion job scale web                    # no --count: print scale status
$ legion job scale web --reset            # drop the override, return to spec count
$ legion job scale multi --task api --count 3   # disambiguate multi-task jobs
```

- `--task` / `--group` are optional when the job has exactly one scalable
  task (the common case). If it has several, the error lists the candidates.
- `--reason` is recorded on the override and in the event stream.
- `--json` / `-j` emits the standard envelope. `--idempotency-key` makes the
  RPC retry-safe (`deduplicated = true` on replay).
- The same operation is available over gRPC (`ScaleTask`) and MCP
  (`legion_job_scale`, write-gated). The job page in the UI has a **Scale**
  card with the same per-task view and Scale / Reset controls
  ([UI architecture](ui-architecture.md)).

`legion job scale <name>` (no `--count`) shows the effective count per
task: `web/default/web: count 6 (spec 2, scaled by operator)`.
`legion job info` shows each task's ready and placed replicas, how many could
not be placed, and its measured usage against its reservation
([Scaling inputs](#scaling-inputs)).

## Semantics

The **effective desired count** is the override if one exists and binds to
the current spec (see [Ownership](#ownership-overrides-vs-resubmits)),
otherwise the spec's `count.num`. Everything that reads desired count uses the
effective value through one accessor: the scheduler's plan paths, slot-mode
stateful retirement, rollout group plans, quota, `GetJobScaleStatus`, and
`GetScalingInputs`.

- **Scale-out** enqueues a `job_scale` eval that plans in reschedule mode.
  Only the shortfall is placed, and existing allocations are untouched.
- **Scale-in** retires surplus allocations. Slot-mode stateful tasks lose
  their highest slot indices first (honoring
  `stateful.storage.retainOnScaleIn`). Stateless tasks lose their newest
  allocations first.
- **Keyed stateful tasks** are rejected because they have no count. Use the
  instance API instead. Tasks with `count = { per = ... }` are also rejected
  in v1.

### Ownership: overrides vs resubmits

An override records the spec count it was laid over (`base_count`). When you
resubmit the job:

- If the new spec count **equals** `base_count`, you didn't touch count, so
  the override survives the deploy.
- If the new spec count **differs**, the spec expresses new intent. The
  override is cleared and the spec wins. The submit reply tells you this
  happened.

So your scale sticks across deploys, and editing the spec still does what it
says. This is a simplified version of Kubernetes' scale-subresource field
ownership. It avoids Nomad's behavior of silently resetting count on
resubmit.

### Rollout interaction

Legion follows the ECS rule: **scale-out applies during a rollout, scale-in
waits.** Rollout ticks recompute their targets from the effective count, so a
scale-out mid-rollout grows the step targets on the next tick. A scale-in
during an active rollout is recorded but only applied when the rollout
completes or is cancelled:

```
$ legion job scale web --count 2
deferred: rollout web-7 is in progress; scale-in to 2 applies when it completes
```

A resubmit that only changes `count` (with no override involved) still
creates a new version and a rollout. Spec changes make versions, and
`job scale` doesn't.

For the same reason, scaling does **not** change the job's `updated_at`. The
jobs list's "Updated" column only tracks spec submissions and status
transitions (see [jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md)).
Scale activity shows up elsewhere: the `scale.applied`, `scale.deferred`, and
`scale.reset` events, and the override's timestamp in
`legion job scale <name>`.

## Scaling inputs

A scaling decision needs to know how busy a task is across the whole
cluster. Workers measure usage and service traffic and send it on their
heartbeats, but a worker talks to one manager, and each manager only hears its
own workers. `GetScalingInputs` is the one read that sees all of it, from any
manager. `legion job info` shows the same numbers per task:

```
$ legion job info shop
...
 TASK        | REPLICAS               | CPU USED / RESERVED              | MEMORY USED / RESERVED  | REQUESTS                     | DATA
-------------+------------------------+----------------------------------+-------------------------+------------------------------+------------------
 default/api | 4/4 ready, 1 unplaced  | 5760 / 4800 MHz (120%, max 180%) | 640 / 1024 MiB (62%)    | 40.0 rps, 20.0% err, 20ms avg | current (3s ago)
```

`legion job info -j` adds the same data as `scalingInputs`, the MCP tool
`legion_job_info` returns it too, and the job page in the UI shows it in a
**Request vs usage** card ([UI architecture](ui-architecture.md)). The table,
the key, and the card are left out when the caller lacks `scale:read`.

### What it returns

One entry per task:

| Field | Meaning |
| --- | --- |
| `desired_replicas` | The effective count: the count override if one binds, else the spec's `count`. 0 for a stopped or failed job. |
| `placed_replicas`, `ready_replicas` | Allocations placed, across every version of the job, and those whose latest health report is healthy. |
| `unplaced_replicas` | `desired - placed`, floored at 0. Always 0 for keyed and `count.per` tasks, and for batch and cron tasks. |
| `rollout_active` | Whether a rollout of the job is in flight. |
| `reservation` | One replica's `cpu` MHz (or `cores`) and `memory` MiB, after `taskResourceDefaults`. |
| `cpu` | Measured millicores and MHz summed over the replicas that reported, their reserved MHz, mean utilization (sum of used over sum of reserved), the busiest replica's utilization, and a per-replica histogram. |
| `memory` | Measured MiB (`memory.current`) against reserved MiB, and the largest single replica's ratio. |
| `requests` | For a task with a `service` block: requests per second, errors per second, error ratio, mean and max latency, summed over every calling node. |
| `freshness` | `state` (`current`, `missing`, `resyncing`), the newest worker sample time, the age of the oldest confirmation, and how many nodes reported. |
| `history` | With `history_seconds`, up to an hour of 10-second totals. |

The CPU arithmetic is `GetNodeUsage`'s. Workers report millicores, and a
replica's MHz is millicores / 1000 x its node's `mhz_per_core`. Utilization
divides in millicores: a `cpu` reservation in MHz becomes millicores through
the node's clock, and a `cores = N` reservation is N x 1000 millicores on any
node. A replica on a node that reports no clock speed (Apple Silicon) still
counts in `used_millicores`, but an MHz reservation there cannot be turned
into a utilization, so it is counted in `unmeasurable_replicas` and left out
of `utilization` and the histogram. The histogram buckets are fixed at
utilization bounds 0.1, 0.25, 0.5, 0.7, 0.85, 1.0, and 1.5, plus one bucket
for everything above.

Ask for several tasks at once, or for every job in some namespaces:

```bash
grpcurl -d '{"targets":[{"job":"shop"},{"job":"payments/ledger","taskName":"db"}]}' \
  ... legion.Manager/GetScalingInputs
grpcurl -d '{"namespaces":["*"],"historySeconds":600}' ... legion.Manager/GetScalingInputs
```

A selector that names no job or task yields an entry with `error` set (`job
not found`, `no task matches ...`). It does not fail the batch. At most 1000
selectors fit in one call.

### Where the numbers come from

Usage never touches the state store. Every 10 seconds each manager folds the
latest usage report of every node it hears from, and the service-graph
samples its workers sent since the last fold, into one rollup per
(node, task). A rollup holds sums, maxima, and the utilization histogram, and
is stamped with the worker's sample time. A manager that does not lead the
status lane sends the rollups that changed to the leader, on the liveness
digest RPC ([node-liveness.md](node-liveness.md#managers-that-do-not-own-liveness)).
The leader keeps the newest rollup per (node, task), whichever manager sent
it. A node that moves to another manager is therefore never counted twice: the
newer sample replaces the older one. The leader adds them up per task when
asked, and keeps an hour of 10-second totals in memory. Other managers
forward `GetScalingInputs` to it, so every manager answers the same.

A single manager does all of this in memory, with no RPC.

### Freshness

The numbers trail the workers by at most one report interval plus one rollup
interval, about 20 seconds.

- **`current`**: the task's rollups are in hand, and every manager holding
  one confirmed it within the last 35 seconds. `age_seconds` says how long
  ago the oldest confirmation was, and is usually under 10.
- **`missing`**: nothing reports usage for the task. It has no replicas, its
  runtime reports no usage (`noop` and third-party runtimes; see
  [resource-enforcement.md](resource-enforcement.md#measuring-usage)), its
  workers run with enforcement off, or the managers that held its rollups
  stopped confirming them. A node whose latest report is older than 30
  seconds contributes nothing.
- **`resyncing`**: the answering manager took the status lane less than one
  rollup interval (10 seconds) ago.

Treat `missing` and `resyncing` as "no data", not as zero load.

### Failover

Nothing here is replicated, so a manager that takes the status lane starts
empty. Each other manager's next rollup sync is a delta the new leader cannot
apply. The leader answers it with a resync request, and the manager sends
its full set at once. Until every manager has ticked once (up to 10
seconds), targets read `resyncing`. The new leader's history starts at the
takeover; `leader_for_seconds` in the reply says how long ago that was.

When a manager stops sending rollups (it crashed, or it cannot reach the
leader), the leader drops its rollups after 35 seconds. Its nodes' workers
fail over to another manager, which reports them from then on.

### Requests count internal callers only

Request rate, errors, and latency come from the service graph
([network-handlers.md](network-handlers.md#service-graph)). Edges are
recorded on the calling side, so only callers inside Legion that go through
the datapath are counted. Traffic from outside the cluster into an advertised
port, and traffic that bypasses the datapath in `host` mode, are invisible
here. A service backed by several tasks reports the service's totals on each
of them. Requests are attributed through the caller's declared upstream key,
or as a service in the caller's own namespace when it calls without
declaring; calls to external hosts are not counted.

### Access

`scale:read` on each job, the same grant as `legion job scale <name>`. The
request is gated before anything is looked up. A request that names a job
outside the caller's grant is refused whole with `PermissionDenied` (not
`job not found`). A namespace listing leaves such jobs out.

## Access control

Scaling has its own ACL resource: **`scale`** (actions `read`/`write`, scoped
by job name). `job:write` does not imply `scale = "write"`, and the reverse is
also true. This lets you give an autoscaler or a teammate the right to resize
a job without the right to redeploy it. See [auth-and-acls.md](auth-and-acls.md).

## Observability

- Events: `scale.applied`, `scale.deferred`, and `scale.reset` in the event
  stream ([event-stream.md](event-stream.md)). Each carries the previous and
  new counts, source, and reason. (`scale.clamped` will arrive later, along
  with the policy block's min/max.)
- The override row itself (principal, reason, timestamp) is the durable
  record. The event journal is capped and best-effort.
- `legion job scale <name>` with no `--count` prints the current override,
  spec count, and effective count.
- `legion_scaling_rollup_syncs_total{outcome}` counts each manager's rollup
  ticks (`local` on the status-lane leader, `ok`, `resync`, `error`
  elsewhere), and `legion_scaling_rollup_rows{kind}` the rollups it built
  (`local`) and holds from every manager (`merged`, on the leader)
  ([metrics.md](metrics.md#scaling-inputs)).
- Allocations retired by a scale-in report the `scale_in` stop cause to the
  worker that ran them. They also carry it in `allocation.removed` event
  metadata ([graceful-stop.md](graceful-stop.md)). A task can use this to
  tell "my replica count shrank" apart from "I am being replaced by a new
  version".

## Failure modes

- **Unknown job / task**: `not_found` / `invalid_argument`, listing the
  candidates.
- **Keyed or `count.per` task**: `invalid_argument` with the reason.
- **No capacity for the new count**: the `job_scale` eval places what fits,
  and the override stays. The rest is placed when capacity appears: a worker
  registering or a node being uncordoned queues a capacity pass that
  reschedules jobs short of their count
  ([jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md#node-added)).
  Capacity freed by other jobs stopping does not trigger it.
- **Manager is not the scheduler-lane leader**: the manager forwards the
  request to the leader and returns its answer. Only when that leader cannot
  be reached does the caller get the standard retryable
  `failed_precondition` envelope with the leader hint.
- **`GetScalingInputs` on a manager that does not lead the status lane**: the
  same forwarding, to the status-lane leader. If it cannot be reached, the
  caller gets the retryable `failed_precondition` with the leader hint, and
  `legion job info` prints the job without its usage table.
- **Inputs that read `missing` for a task that is running**: its runtime
  reports no usage (`noop`, third-party runtimes), its workers have
  `resourceEnforcement` off, or the manager its workers report to cannot
  reach the status-lane leader. `legion_scaling_rollup_syncs_total{outcome="error"}`
  rising on that manager says which.

## Known limitations (v1)

- There is no `scaling` policy block yet. This is the manual surface, plus
  the inputs (`GetScalingInputs`) an automatic one will read. Nothing acts on
  those inputs yet.
- Scaling inputs are held in the status-lane leader's memory. A takeover
  starts them empty for up to 10 seconds and starts the hour of history
  afresh. History is kept for at most 4096 tasks; past that, a task still
  has current inputs but no history.
- Request rate, errors, and latency count only callers inside Legion that go
  through the datapath ([Requests count internal callers only](#requests-count-internal-callers-only)).
  There is no in-flight request (concurrency) signal.
- There are no pause/pin semantics. `--reset` is the only way to return
  ownership to the spec.
- `count.per` tasks cannot be scaled.
- Scale status is computed from store state. There is no evaluator status
  RPC yet (it arrives with the autoscaler).
