# Gang Scheduling

Some workloads only make progress when all of their replicas run at the same
time: data-parallel training, MPI, a coordinated fleet of workers that
exchange state. Six of eight training workers holding GPUs while they wait for
the other two compute nothing, and they keep anyone else from using those
GPUs.

Legion places what fits by default ([partial
placement](scheduler-and-eval-loop.md#partial-placement)): a task with
`count = 8` on room for 6 runs 6 and waits for room for the rest. `gang` turns
that off for one task. Its replicas place **all together or not at all**, a
gang that does not fit waits, holding nothing, until it does, and a gang that
loses a member is restarted whole.

```nickel
{
  name = "trainer",
  tasks = [{
    name = "worker",
    runtime = "container",
    group = "worker",
    count = 8,
    resources = { cpu = 4000, memory = 16384 },
    gang = true,
    config = { image = "registry.example.com/train:v3" },
  }],
}
```

## The surface

| Form | Meaning |
| --- | --- |
| `gang = true` | Every replica places in one plan, or none does. A lost member restarts the whole gang. |
| `gang = { min = 6 }` | Place once at least 6 fit together. The rest place as room appears. `min` must be between 1 and `count`. |
| `gang = { placementTimeout = "30m" }` | Fail the job if the gang has waited 30 minutes without reaching its quorum ([Placement timeout](#placement-timeout)). |
| `gang = { onMemberLoss = "replace" }` | What a lost member does to the rest: `restart` (default), `replace`, or `fail` ([When a member is lost](#when-a-member-is-lost)). |
| `gang = false`, or no `gang` | Replicas place independently (the default). |

The fields combine: `gang = { min = 6, placementTimeout = "30m", onMemberLoss
= "fail" }`. `gang = true` is `{}` with every default.

| Field | Default | Meaning |
| --- | --- | --- |
| `min` | `count` | The gang's **quorum**: the replicas that must run together. If `legion job scale` lowers the count below `min`, the quorum drops with it. |
| `placementTimeout` | unset (wait indefinitely) | How long the gang may wait for its quorum before its job fails. A duration (`"30m"`, `"2h"`) or a number of seconds; at least `1s`. |
| `onMemberLoss` | `"restart"` | What a loss that leaves fewer than `min` running does to the survivors. |

Unknown fields are rejected, not parsed and ignored:

```console
error: unknown field `recovery` in gang, expected one of `min`, `placementTimeout`, `onMemberLoss` (see docs/gang-scheduling.md)
```

`legion job schema` and `configs/jobs/maximal.ncl` describe the field too.

## What a gang does

### Placement is all or nothing, for the whole job

When the scheduler plans a job, it places a gang task's replicas one at a
time, best node first, as it does for any task. If one does not fit and fewer
than `min` replicas are running or placed, **the whole plan places nothing**.
That covers the gang's replicas, every other task of the same job in that
pass, and any preemption the plan had chosen. The cluster after a deferred
gang plan is exactly what it was before.

Keeping the whole plan as one unit is deliberate. Two gang tasks in one job,
a launcher and its workers say, start together without any extra syntax. The
cost is that a job that wants its dashboard up while its trainer waits must
be two jobs.

The commit itself is atomic already: a plan's placements are applied in one
transaction, and one that can no longer bind rejects the whole plan (see
[Conflict handling](scheduler-and-eval-loop.md#conflict-handling)). So the
gang either starts whole or not at all, even under contention.

### A gang that does not fit waits, holding nothing

A deferred gang plan is a plan that placed nothing, so its eval
[blocks](scheduler-and-eval-loop.md#blocked-evals) instead of failing. It is
keyed by what kept the first missing replica out (usually the task's pool
class) and woken when capacity a node could help with appears: a node
registering or being uncordoned, a node's attributes changing, or allocations
leaving a node. Then the plan runs again, and places the whole gang if it now
fits.

Nothing is reserved while the gang waits: no partial placements and no held
capacity. Any number of waiting gangs cost the cluster nothing, and
two waiting gangs can never deadlock each other, because neither holds
anything the other needs. The flip side is in [Limitations](#limitations):
smaller jobs can keep taking the capacity a large gang is waiting for.

Both "the nodes are full" and "no node matches the task's constraints" wait.
A constraint no node satisfies today can be satisfied by a node that joins
tomorrow. `eligibleNodes` in the report (below) tells the two apart.

### The quorum

With `min` below `count`, a fresh placement places once `min` replicas fit
together. The remaining replicas are ordinary unplaced work, and each one
places as soon as a later pass has room for it. A running gang at or above
its quorum tops up one replica at a time, like any other task. A running gang
below its quorum (it lost more members than `count - min`) is
[broken](#when-a-member-is-lost), and its `onMemberLoss` decides what
happens.

### Placement timeout

By default a gang waits until it fits, however long that is. `placementTimeout`
bounds the wait:

```nickel
gang = { placementTimeout = "30m" },
```

The clock starts the **first** time the gang comes up short of its quorum and
keeps running while it waits, however often capacity events wake it and it
re-plans. It is not a per-attempt timer. It stops when an eval of the job
succeeds (the quorum placed), and starts afresh after the job is stopped,
re-run, or updated.

When the deadline passes with the gang still short, the scheduler plans it
once more. If the quorum still does not fit, the job is marked `failed`, every
allocation it still has stops (stop cause `gang`: the survivors of a top-up
that never came), and the eval fails with:

```text
Gang worker did not reach its quorum within placementTimeout 30m (waited 30m 1s); the job is failed and places nothing until `legion job run` restarts it. Last attempt: Failed to place task worker: ...
```

A failed gang job places nothing, even when capacity appears later.
`legion job run` (or `--force-reschedule`) starts it again, with a new clock.
Expiry is checked by the scheduler leader at least once a second, so a gang
fails at most about a second after its deadline.

The timeout covers every wait short of the quorum: a fresh submit, a
`replace` top-up, and the re-placement after a `restart`. A gang that reaches
its quorum and waits only for members above it (with `min` below `count`) is
not timed out.

There is no default timeout. A cluster that expects to grow (an autoscaler, a
node arriving later) should not fail jobs that would have fit in an hour, and
a good value depends on the workload. Set it when a late gang is worse than
no gang, for example a training run with a deadline, or when an agent should
get a definite failure instead of an open-ended wait.

### When a member is lost

A gang is **broken** when fewer than its quorum still run. A member loss that
leaves the quorum running (an elastic gang with `min` below `count`) is not a
break: the member is replaced alone, as for any task, and the rest keep
running. A loss that breaks the gang applies `onMemberLoss`:

| `onMemberLoss` | The survivors | Then |
| --- | --- | --- |
| `"restart"` (default) | Stop, with their normal graceful stop and stop notice (cause `gang`). | The whole gang places again, together, as one unit. |
| `"replace"` | Keep running. | The missing members place together once they all fit. |
| `"fail"` | Stop, with every other allocation of the job. | The job is `failed` and places nothing until `legion job run` restarts it. |

With `gang = true` every loss breaks the gang.

`restart` is the default because the gang's contract is "these replicas run
together", and most frameworks that need a gang (PyTorch distributed, MPI,
JAX) cannot carry on after a member leaves: survivors block in a collective
until the framework times out. Restarting the whole gang gets the job running
again without anyone stepping in. `replace` suits workloads that tolerate a
member rejoining, and it keeps the survivors' work; `fail` suits batch jobs
where an operator or a pipeline should decide what to do next.

A **loss** is one of:

- a member **failing**: its process exits and the worker gives up restarting
  it, or `on_unhealthy` moves it for being unhealthy;
- its node being **lost** or **drained**;
- a **memory-pressure** eviction.

A job stop, a scale-in, a rollout replacing the gang, preemption (which never
goes below the quorum), or a batch member exiting successfully is not a loss.

**When the restart happens.** The gang places again once the survivors'
stop grace (`stop.killAfter`, default 30s) has passed, so new members never
start beside old ones still shutting down. A restart caused by a failed
member also waits out that member's
[move backoff](jobs-tasks-and-scheduling.md) (`on_unhealthy`'s `moveDelay`,
doubling per repeated failure), whichever is longer: the hold on one member
holds the whole gang. A `replace` top-up waits for the backoff the same way;
the other missing members are not placed ahead of a held one, so a backoff
can never leave the gang split.

**The budget.** Restarts and replacements caused by a member failing spend
the task's existing move budget, `on_unhealthy.moveCount` moves per
`movePeriod`. When it runs out the job fails, survivors included, whatever
`onMemberLoss` says: the gang cannot be whole again without help. Node loss,
drain, and eviction spend nothing; they are the infrastructure's failures, not
the workload's.

Each loss is recorded as a `job.gang_member_lost` event naming the member,
the cause, the members still running, and the action taken:

```text
Gang worker of job trainer lost allocation 812 on node gpu-07 (node_lost): 7 run, below its quorum of 8; restarting the whole gang (7 stopped)
```

**Local restarts are not losses.** A worker restarting a crashed process in
place, under the task's `restart` policy, is invisible to the manager, and
the rest of the gang keeps running beside the restarted member. For a
framework that cannot take a member back mid-run, set `restart = { policy =
"never" }` so a crash becomes a member failure and `onMemberLoss` applies.

## Starting together

A gang **places** together: every member is assigned a node in one plan, or
none is. It does not **start** in lockstep. Each worker starts its member as
soon as it receives it, so members start seconds (or, with a large image to
pull, minutes) apart.

That is deliberate, and it matches how other gang schedulers work: the sync
point belongs in the workload, as a rendezvous that waits for every member,
because only the workload knows when it is ready (its weights loaded, its
data opened). Distributed frameworks already have one: PyTorch's
`init_process_group` and torchrun's rendezvous, MPI's launch, JAX's
`jax.distributed.initialize`. Point them at a rendezvous every member can
reach, and give them the world size, which is the gang's count.

Legion's part is fate-sharing. A member that fails or loses its node restarts
the whole gang (the default `onMemberLoss`), so survivors never wait forever
on a peer that is not coming back, and no workload needs to watch its peers'
status. What Legion gives a member today:

- **Its identity.** Every member gets `LEGION_ALLOC_ID`. A slot-mode stateful
  gang also gets `LEGION_INSTANCE_SLOT`, a stable ordinal `0..count-1` that
  works as a rank ([stateful workloads](stateful-workloads.md#environment-variables)).
- **Its peers' addresses.** A gang task with a `service` block registers each
  member, and [service discovery](service-discovery.md) (`connect` upstreams,
  `LEGION_DNS`, templates with `{{ range endpoints "worker" }}`) lists them.
  The catalog lists healthy endpoints only, so a member shows up once its
  checks pass.

Gaps are in [Limitations](#limitations): a stateless gang has no rank or
world-size variable, and a member that is placed but never starts holds the
others at the rendezvous until its start fails.

## Seeing it

### Before submitting: `legion job plan`

The dry run answers "would the whole gang fit right now". A gang that would
not reach its quorum is the plan's answer, not an error: the reply succeeds,
places nothing, and says why.

```console
$ legion job plan trainer.ncl
Job would wait (blocked) and place nothing yet: gang worker needs 8 replicas together and 6 fit now
Job config new; showing placements
No allocations planned
Gang:
  worker: would NOT place — 6 of the 8 it needs fit (all 8 together; 10 of 12 nodes accept it); the job would wait, holding nothing
    Failed to place task worker: no available nodes matching constraints, permit, or resources. Preemption ran but found nothing to displace: every feasible node is full of work at this band or above
```

With `-j`, each gang task has a `gangReports` entry:

```console
$ legion job plan trainer.ncl -j
{"ok":true,"data":{"allocations":[], ...,
 "gangReports":[{"task":"worker","desired":8,"min":8,"running":0,"placeable":6,
   "wouldPlace":false,"eligibleNodes":10,"nodes":12,
   "reason":"Failed to place task worker: no available nodes matching constraints, permit, or resources. ..."}]}}
```

| Field | Meaning |
| --- | --- |
| `desired` | The task's replica count in this plan (after a `job scale` override). |
| `min` | The quorum. |
| `running` | Replicas already running. 0 for a submit or a dry run. |
| `placeable` | Replicas the plan could place before the first that did not fit. |
| `wouldPlace` | Whether `running + placeable` reaches `min`. |
| `eligibleNodes` / `nodes` | Nodes whose constraints, permit, and volume drivers accept the task, out of the schedulable nodes. 0 eligible points at the jobspec, not at capacity. |
| `reason` | Why the first missing replica did not fit. |
| `placementTimeoutSeconds` | The gang's `placementTimeout`, in seconds. 0 when unset. |
| `waitingSince` / `deadline` | `job info` only (0 in a dry run): when the wait started, and when it times out, as Unix seconds. `deadline` is 0 without a timeout. |

A job without `gang` still fails its dry run when a replica does not fit, as
before. The MCP tool `legion_job_plan` returns the same reply.

### While it waits: `legion job info`

```console
$ legion job info trainer
...
Blocked on class:pool=gpu since 2026-10-17T14:02:11Z: gang worker is short 2 replica(s) of cpu 4000MHz, memory 16384MiB each (0 running, 6 more fit, needs 8 together)
  Fails at 2026-10-17T14:32:11Z if still short (placementTimeout)
  Gang worker waits: 6 of its 8 replicas fit, and a gang places all of them together or none (10 of 12 nodes accept the task). Failed to place task worker: ...
```

With `-j`, the job's `blocked` block carries a `gang` report in the same shape
as `gangReports`, from the last attempt:

```console
$ legion job info trainer -j
{"ok":true,"data":{"name":"trainer","status":"blocked",
 "blocked":{"key":"class:pool=gpu","task":"worker",
  "shortfall":{"cpu":"4000MHz","memory":"16384MiB"},
  "message":"Gang worker waits: 6 of its 8 replicas fit, ...",
  "since":1792245731,"evalId":"42",
  "gang":{"task":"worker","desired":8,"min":8,"running":0,"placeable":6,
          "wouldPlace":false,"eligibleNodes":10,"nodes":12,"reason":"...",
          "placementTimeoutSeconds":1800,"waitingSince":1792245731,
          "deadline":1792247531}}, ...}}
```

`shortfall` is what one replica asks for. The gang is short `min - running -
placeable` of them. That vector is what to add capacity for, or what to
shrink. The job's status is `blocked` while nothing of it runs, and `running`
(with the block) while survivors run and a `replace` top-up waits. The UI's
job page shows the same block, with a gang line and the deadline.

Other surfaces:

- `legion eval list` shows the eval with status `blocked`.
- The `eval.blocked` event carries the gang report in its `detail.gang`, and
  `eval.unblocked` names what woke it (`timeout` for a gang past its
  deadline). `job.gang_member_lost` records each [loss](#when-a-member-is-lost)
  and `job.gang_timeout` each [timeout](#placement-timeout)
  ([event stream](event-stream.md)).
- `legion_gang_blocked_jobs` counts jobs whose blocked eval is a gang short of
  its quorum, and `legion_blocked_wait_seconds` records how long blocked evals
  wait ([metrics](metrics.md)). A gauge that stays up while the cluster churns
  is the starvation signal. `legion_gang_outcomes_total{outcome}` counts
  member losses by what they led to (`replace`, `restart`, `fail`) and
  placement timeouts (`timeout`).

## Validation

These are rejected at submit, `legion job validate`, and `legion job plan`:

| Spec | Why |
| --- | --- |
| `gang` with `count.per` | A per-attribute count has no single membership. Use a fixed count and constraints. |
| `gang` on a keyed or session stateful task | A gang needs a fixed membership. Keyed instances come and go through the instance API. Slot-mode stateful tasks can be gangs. |
| `gang.min` of 0, or above `count` | The quorum is between 1 and the count. |
| A gang task that shares its `group` with other tasks | A multi-task group already places all of its tasks together on one node, all or nothing. Give the gang task its own `group`. Tasks default to group `default`, so a multi-task job must name the gang's group. |
| A rollout for the gang's group that takes more than one step | See [Rollouts](#rollouts). |
| `slots.order = "sequential"` on the gang's group | Same. |

```console
$ legion job validate trainer.ncl
error: task worker sets gang but shares group default with launcher. A multi-task group already places all of its tasks together on one node, all or nothing; give the gang task a group of its own (`group = "worker"`)
```

## Interactions

### Rollouts

A gang's replicas place and replace as one unit. A canary that updates two of
eight training workers is not 25% updated, it is a broken gang. So the
rollout for a gang's group must resolve to **one step**:

```console
error: rollout: the step mode for group worker replaces task worker in 4 steps, but the task is a gang (its 8 replicas place and replace as one unit). Remove the step mode for this group (for example `rolloutByGroup.worker = { percentSteps = [100] }`) or remove `gang`
```

`percentSteps = [100]`, `countEachStep` at or above the count, or no step
mode at all are one step. The rollout engine also replaces a gang group in
one step whatever a [rollout strategy plugin](rollout-strategy-plugins.md)
injects, and after a scale-out grows a `countEachStep` rollout past one step.

What one step means for a gang:

- **`replacement = "destroyFirst"`** (the default): the old gang stops, then
  the new gang places whole. If the new gang does not fit, the rollout waits
  for capacity like any other, and `capacityBlockTimeout` /
  `onCapacityTimeout` (including `rollback`) bound the wait. An update is a
  restart, with downtime.
- **`replacement = "createFirst"`**: the new gang places whole while the old
  one still runs, then the old one retires. No downtime, but it needs room for
  both gangs at once, and waits until it has it.

A job with no `rollout` block rolls out with these defaults (one step,
`destroyFirst`). A restart — `legion job run --force-reschedule`, a scheduled
batch run, a KV-triggered restart — works the same way: the job's allocations
are removed, then the plan places the whole gang or blocks, and the job sits
at zero, blocked, until the gang fits.

### Preemption

A gang **preempts as a unit**. A higher-band gang that can fit only by
evicting lower-band work evicts what every one of its replicas needs, in the
same transaction that places them. If the whole gang cannot fit even with
the victims it is allowed to take, nothing is evicted.

A gang is **never preempted below its quorum**. Taking one member would idle
the rest, so the quorum works like a
[`disruption.minAvailable`](preemption-and-eviction.md) floor, and the
stricter of the two binds. With `gang = true` no member can be preempted at
all. With `min` below `count`, the members above the quorum can be. A
preemptor that finds only gang members to take reports the floor in its
placement error (`disruption.minAvailable`).

Drain, node loss, and `job stop` are not preemption. They take members
regardless: drain and node loss are [member losses](#when-a-member-is-lost),
and `job stop` stops the gang.

### Changing `gang` on a running job

Adding or removing `gang`, or changing `min`, is a config change like any
other: the resubmit rolls the job out under the new rule. It is also a
scheduling change, so paths that otherwise move allocations to a new config in
place (a KV-triggered re-render, recovery after a failover) re-plan instead.
Turning `gang` on for a job that runs 6 of 8 places a fresh gang of 8, or
waits for one.

### Stateful slots

Slot-mode stateful tasks can be gangs, and a top-up replaces the missing
slots together. A `restart` stops the surviving slots' allocations and places
every slot again; each slot keeps its instance, its `LEGION_INSTANCE_SLOT`,
and (with network-attached storage) its data. A slot whose data lives on one node (node-local
`stateful.storage`) can only be replaced on that node. If that node is full
or gone, the gang waits for it, and the report's `reason` names the pinned
slot. Prefer network-attached storage for stateful gangs.

### Dependencies

`dependsOn` gates a gang like any task: the gang is placed in the pass where
it becomes ready, and its quorum is checked then. A prep task can run and
finish while the gang waits for it.

### Scaling

`legion job scale` changes a gang's count. Scaling out places the new
replicas together (with `gang = true`, the quorum is the new count). Scaling
in removes replicas and lowers the quorum to the new count if it falls below
`min`.

## Failure modes and diagnostics

| You see | It means | What to do |
| --- | --- | --- |
| `status: blocked`, `gang.eligibleNodes: 0` | No node satisfies the task's constraints, permit, or volume drivers. | Check the constraints (a typo waits forever). `legion job plan` shows this before submitting. |
| `status: blocked`, `eligibleNodes` > 0, `placeable` < `min` | The eligible nodes are full. | Wait, lower `count` or `min`, or add nodes of the blocked class (`blocked.key`). |
| `status: running` with a gang `blocked` block | Survivors run and a `replace` top-up waits. | Same as above. The survivors are untouched. |
| `status: failed`, eval message `did not reach its quorum within placementTimeout` | The gang waited past its `placementTimeout`. A `job.gang_timeout` event has the details. | Add capacity or lower `count`/`min`, then `legion job run`. Raise or remove `placementTimeout` if the wait was expected. |
| Every member stopped with cause `gang`, then the gang placed again | A member was lost and `onMemberLoss = "restart"` restarted the gang. `job.gang_member_lost` names the member and the cause. | Nothing, if losses are rare. A member that keeps failing spends the move budget and fails the job; look at its logs. |
| `status: failed` after `job.gang_member_lost` with `action: fail` | `onMemberLoss = "fail"`, or the task's move budget ran out. | Fix the failing member, then `legion job run`. |
| Members start minutes apart and the workload's rendezvous times out | Members start independently ([Starting together](#starting-together)): a large image or artifact on one node delays its member. | Raise the rendezvous timeout in the workload, or pre-pull the image. |
| `legion_gang_blocked_jobs` stays above zero while jobs keep placing | Smaller jobs take capacity as it frees, and the gang never sees enough at once. | Fence capacity for the gang with a node pool and `permit` ([Jobs, Tasks, and Scheduling](jobs-tasks-and-scheduling.md)), or stop competing work. See [Limitations](#limitations). |
| A higher-band job fails to place, naming `disruption.minAvailable` | Every candidate victim is a gang member at its quorum. | Expected. Stop the gang, or give it `min` below `count`. |
| `job validate` rejects the gang | One of the [validation](#validation) rules. | The error names the fix. |

## Limitations

- **No reservation or backfill.** A waiting gang holds nothing, so a stream of
  small jobs can keep taking freed capacity before a large gang sees enough of
  it at once. Legion does not yet reserve freed capacity for the oldest
  waiting gang. Pools and permits are the way to fence capacity for gangs
  today, `legion_gang_blocked_jobs` shows when it matters, and
  `placementTimeout` turns an endless wait into a failure.
- **No start barrier, rank, or world size.** Members start as their workers
  get them ([Starting together](#starting-together)). A stateless gang gets no
  rank or world-size variable; pass the world size in the task's `env` (it is
  the count) and take a rank from the framework's rendezvous, or use a
  slot-mode stateful gang for `LEGION_INSTANCE_SLOT`. There is no built-in
  coordinator address beyond service discovery, which lists healthy members
  only.
- **No readiness timeout.** `placementTimeout` covers placing the gang, not
  starting it. A member that is placed but hangs before it runs (an image pull
  that never finishes) holds the others at their rendezvous until the worker
  reports its start failed.
- **Restarts are full re-placements.** A `restart` stops every survivor and
  places the gang again, possibly on other nodes. There is no in-place
  restart that keeps the gang's nodes and restarts only its processes.
- **Local restarts are not losses.** A member the worker restarts in place
  under the task's `restart` policy does not restart the gang. Set `restart =
  { policy = "never" }` for frameworks that need it to.
- **Every capacity event re-plans a waiting gang.** A gang is woken by any
  capacity on a node of its class, even one free replica's worth, and re-plans
  in full (gang tasks use the full node list, never a truncated
  [scheduling index](scheduling-indexes.md) candidate list, so "it does not
  fit" is always a real answer). The re-plan is read-only and places nothing,
  but on a large, busy cluster many waiting gangs cost planning time.
- **The whole job waits.** Other tasks of a job with a waiting gang do not
  place either, and a gang `restart` or `fail` stops only the gang task's
  allocations (`fail` stops the whole job). Split unrelated tasks into
  separate jobs.
- **One job per gang.** Two jobs cannot be gang-scheduled together, and there
  is no topology control (same rack, same zone) beyond ordinary constraints
  and affinities.
- **Older managers ignore `gang`.** A manager that predates the field accepts
  a jobspec with `gang` and schedules it without gang semantics.
