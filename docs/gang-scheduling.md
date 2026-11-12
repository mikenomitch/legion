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
| `gang = { readyTimeout = "10m" }` | A placed member that has not come up within 10 minutes is a lost member ([Readiness timeout](#readiness-timeout)). |
| `gang = { onMemberLoss = "replace" }` | What a lost member does to the rest: `restart` (default), `restartInPlace`, `replace`, or `fail` ([When a member is lost](#when-a-member-is-lost)). |
| `gang = { onLocalRestart = "ignore" }` | Whether a member its worker restarts in place is a lost member: `loss` (default) or `ignore` ([Local restarts](#local-restarts)). |
| `gang = { topology = { key = "rack" } }` | Place every member within one value of a node attribute: one rack, one NVLink domain ([Topology](#topology)). `keys = ["rack", "zone"]` falls back level by level. |
| `gang = false`, or no `gang` | Replicas place independently (the default). |

The fields combine: `gang = { min = 6, placementTimeout = "30m", onMemberLoss
= "fail" }`. `gang = true` is `{}` with every default.

| Field | Default | Meaning |
| --- | --- | --- |
| `min` | `count` | The gang's **quorum**: the replicas that must run together. If `legion job scale` lowers the count below `min`, the quorum drops with it. |
| `placementTimeout` | unset (wait indefinitely) | How long the gang may wait for its quorum before its job fails. A duration (`"30m"`, `"2h"`) or a number of seconds; at least `1s`. |
| `readyTimeout` | unset (no bound) | How long each placed member may take to report healthy once before it counts as lost. A duration or seconds; at least `1s`. |
| `onMemberLoss` | `"restart"` | What a loss that leaves fewer than `min` running does to the survivors. |
| `onLocalRestart` | `"loss"` | Whether a member's in-place restart by its worker is a loss. |
| `topology` | unset (any nodes) | `{ key = "<node attribute>" }` or `{ keys = ["<inner>", "<outer>", ...] }`, with `mode = "required" \| "preferred"`. `mode` defaults to `"required"`. |

Unknown fields are rejected, not parsed and ignored:

```console
error: unknown field `recovery` in gang, expected one of `min`, `placementTimeout`, `readyTimeout`, `onMemberLoss`, `onLocalRestart`, `topology` (see docs/gang-scheduling.md)
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

Nothing is placed while the gang waits: no partial placements, and no
capacity held for its first few minutes. Two waiting gangs can never
deadlock each other, because neither holds anything the other needs. A gang
that keeps waiting while smaller jobs take the capacity it needs as it frees
gets a [reservation](#reservations) after five minutes.

**Waking.** A blocked gang is not re-planned for every capacity event on its
class. When an event comes, the scheduler estimates how many members the
nodes that could take the task now hold (free capacity over the task's
request, per node, summed; for a gang whose band can preempt, plus the usage
of work in lower bands), and wakes the gang only when that reaches the
members its quorum still needs (`min - running`). The estimate is an upper
bound (it ignores ports, `distinct`, and anti-affinity, which the plan still
checks), so it never keeps back a gang that would have placed. Each event it
skips is counted in `legion_gang_wakes_skipped_total`.

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
A gang never fails before its full `placementTimeout` has passed. The clock
is kept in whole seconds and the scheduler leader checks it once a second, so
a gang fails within about two seconds after its deadline.

The timeout covers every wait short of the quorum: a fresh submit, a
`replace` top-up, and the re-placement after a `restart`. A gang that reaches
its quorum and waits only for members above it (with `min` below `count`) is
not timed out.

There is no default timeout. A cluster that expects to grow (an autoscaler, a
node arriving later) should not fail jobs that would have fit in an hour, and
a good value depends on the workload. Set it when a late gang is worse than
no gang, for example a training run with a deadline, or when an agent should
get a definite failure instead of an open-ended wait.

### Readiness timeout

`placementTimeout` covers placing the gang. A placed member can still fail to
come up: an image pull that never finishes, an artifact that hangs, a health
check that never passes. Its peers then sit at their rendezvous until the
member's own failure, which may never come. `readyTimeout` bounds that:

```nickel
gang = { readyTimeout = "10m" },
```

A member is **ready** once it has reported healthy: running, and passing its
checks if it has any (a task without checks is healthy as soon as it runs).
Each member has its own clock, from when it was placed. A member still not
ready `readyTimeout` after its placement is a **lost member**: it is removed as
a failed allocation (`allocation.failed`, with the reason `Not ready within
gang.readyTimeout 10m: ...`), its gang's `onMemberLoss` applies (`restart` by
default), and like any failure it spends the task's move budget, so a member
that never comes up eventually fails the job rather than restarting the gang
forever. A member that was ready once is never timed out again; staying healthy
after that is `onUnhealthy`'s job.

The clock is checked by the scheduler leader once a second, in whole seconds:
a member is timed out within about two seconds after its deadline, never
before. There is no default: how long a member may take to start depends on
its image and its node.

### When a member is lost

A gang is **broken** when fewer than its quorum still run. A member loss that
leaves the quorum running (an elastic gang with `min` below `count`) is not a
break: the member is replaced alone, as for any task, and the rest keep
running. A loss that breaks the gang applies `onMemberLoss`:

| `onMemberLoss` | The survivors | Then |
| --- | --- | --- |
| `"restart"` (default) | Stop, with their normal graceful stop and stop notice (cause `gang`). | The whole gang places again, together, as one unit. |
| `"restartInPlace"` | Stay on their nodes; each worker restarts their processes there, under a new `LEGION_GANG_EPOCH`. | The missing members place together once they all fit. |
| `"replace"` | Keep running. | The missing members place together once they all fit. |
| `"fail"` | Stop, with every other allocation of the job. | The job is `failed` and places nothing until `legion job run` restarts it. |

With `gang = true` every loss breaks the gang.

`restartInPlace` is `restart` without the re-placement: nothing is stopped
or moved but the members that were lost, which is much faster for a large
gang with heavy images, and the gang keeps its nodes (and its topology
domain). It suits frameworks that need every process restarted but not new
nodes. The survivors restart in place with the same stop and start a local
restart uses, so their graceful stop applies; each worker does it as soon as
its next heartbeat says so, so the survivors restart within a heartbeat of
each other, not at once.

`restart` is the default because the gang's contract is "these replicas run
together", and most frameworks that need a gang (PyTorch distributed, MPI,
JAX) cannot carry on after a member leaves: survivors block in a collective
until the framework times out. Restarting the whole gang gets the job running
again without anyone stepping in. `replace` suits workloads that tolerate a
member rejoining, and it keeps the survivors' work; `fail` suits batch jobs
where an operator or a pipeline should decide what to do next.

A **loss** is one of:

- a member **failing**: its process exits and the worker gives up restarting
  it, or `onUnhealthy` moves it for being unhealthy;
- a member **restarting in place** under the task's `restart` policy
  ([Local restarts](#local-restarts));
- a member **not ready** within `readyTimeout`;
- its node being **lost** or **drained**, or having a pending
  **interruption** (a spot reclaim notice; [spot
  interruption](spot-interruption.md));
- a **memory-pressure** eviction.

A job stop, a scale-in, a rollout replacing the gang, preemption (which never
goes below the quorum), or a batch member exiting successfully is not a loss.

**When the restart happens.** The gang places again once the survivors'
stop grace (`stop.killAfter`, default 30s) has passed, so new members never
start beside old ones still shutting down. A restart caused by a failed
member also waits out that member's
[move backoff](jobs-tasks-and-scheduling.md) (`onUnhealthy`'s `moveDelay`,
doubling per repeated failure), whichever is longer: the hold on one member
holds the whole gang. A `replace` top-up waits for the backoff the same way;
the other missing members are not placed ahead of a held one, so a backoff
can never leave the gang split.

**The budget.** Restarts and replacements caused by a member failing spend
the task's existing move budget, `onUnhealthy.moveCount` moves per
`movePeriod`, and so do local restarts that `restart` or `fail` the gang and
members past their `readyTimeout`. When it runs out the job fails, survivors
included, whatever `onMemberLoss` says: the gang cannot be whole again without
help. Node loss, drain, interruption, and eviction spend nothing; they are the
infrastructure's failures, not the workload's.

Each loss is recorded as a `job.gang_member_lost` event naming the member,
the cause (`failed`, `local_restart`, `ready_timeout`, `node_lost`, `drain`,
`interruption`, `node_pressure`), the members still running, and the action
taken:

```text
Gang worker of job trainer lost allocation 812 on node gpu-07 (node_lost): 7 run, below its quorum of 8; restarting the whole gang (7 stopped)
```

### Local restarts

A worker restarts a crashed or unhealthy member in place under the task's
`restart` policy (and `onUnhealthy`'s restart budget). For a gang that is a
member gone and back: its peers, blocked in a collective, rarely take it
back. So by default (`onLocalRestart = "loss"`) the worker reports each local
restart of a gang member on its next heartbeat, and the manager treats it as a
lost member:

| `onMemberLoss` | A local restart that leaves fewer than `min` running |
| --- | --- |
| `"restart"` | Is a member failure: the member is removed (spending the move budget) and the whole gang stops and places again. |
| `"restartInPlace"` | Restarts every member in place under a new epoch (the restarted one too, so the whole gang shares one). |
| `"replace"` | Changes nothing: the member is back already. |
| `"fail"` | Is a member failure: the job fails. |

The restarted member counts as down for the quorum check, so an elastic gang
whose other members still make the quorum carries on untouched.
`onLocalRestart = "ignore"` opts out: the member rejoins on its own, as for
any task. Restarts the manager asks for (a `restartInPlace`, a refreshed
view) are never reported back.

### Topology

A gang that runs a collective (all-reduce, MPI) runs at the speed of its
slowest link. `topology` keeps every member inside one **domain**: the nodes
that share one value of a node attribute, such as a rack or an NVLink domain.

```nickel
{
  name = "worker",
  group = "worker",
  count = 8,
  resources = { cpu = 16000, memory = 131072, gpu = 8 },
  gang = { topology = { key = "rack" } },       # mode = "required" by default
}
```

`key` is a node attribute name, looked up the way `count.per` and `distinct`
look one up: the node's config attributes, then its fingerprint, then its
dynamic attributes. Give nodes the attribute in the worker config (`attrs = {
rack = "r12" }`) or let a fingerprint report it. Nodes that do not have it are
in no domain.

**Levels.** `keys` names several attributes, innermost first:

```nickel
gang = { topology = { keys = ["rack", "zone"] } },
```

The gang is tried in one rack first; only when no rack can hold its quorum is
it tried in one zone. `required` then means "inside one domain of some level"
(one rack, else one zone, never across zones). At most four levels; `key =
"rack"` is the one-level form.

| `mode` | Places | When no domain fits the quorum |
| --- | --- | --- |
| `"required"` (default) | Every member in one domain. | The gang waits, holding nothing, like any gang short of its quorum. |
| `"preferred"` | Every member in one domain when one fits. | The fewest domains of the outermost level that together hold the quorum, then any eligible nodes. It never waits for topology. |

**Which domain.** The scheduler estimates how many members fit in each domain
(free capacity on each eligible node over the task's request, summed), then
places the gang inside one domain at a time: first the domains that fit every
member, smallest first, so large domains stay whole for large gangs (best
fit); then the domains that fit the quorum, largest first; then, for a gang
whose band can preempt, the domains that would fit the quorum once the work
in lower bands is preempted, largest first. A domain that cannot hold the
quorum even then is never tried. A domain the estimate admits can still fail
(anti-affinity, a static port, `distinct`, a victim too young to take), in
which case its placement is discarded and the next domain is tried. At most
sixteen domains are tried per gang per plan, across all its levels.

**Top-ups stay put.** A running gang that tops up (`onMemberLoss = "replace"`
or `"restartInPlace"`, or members above `min` placing later) places in the
domain its running members occupy, level by level: under `required`, in their
rack, else in their zone; under `preferred`, there first. A `restart`
re-places the whole gang and may choose a different domain.

**Seeing it.** `legion job plan` names the domain, or the best domain and how
far it got:

```console
$ legion job plan trainer.ncl
Gang:
  worker: places (8 fit, needs all 8 together; 40 of 48 nodes accept it)
    in rack=r12 (6 rack domain(s) have eligible nodes)
$ legion job plan trainer.ncl
Gang:
  worker: would NOT place — 6 of the 8 it needs fit (all 8 together; 40 of 48 nodes accept it); the job would wait, holding nothing
    No single rack fits the 8 member(s) the gang needs together (topology rack, required); the closest, rack=r12, fits 6. Failed to place task worker: ...
```

A `preferred` gang that spans racks prints `across more than one rack: none of
the 6 with eligible nodes fits it alone (topology preferred)`.

With `-j`, each gang report carries `topologyKey` (the level the domain
belongs to), `topologyMode`, `topologyDomain` (the domain chosen, or the best
one tried), and `topologyDomains` (how many domains of that level have eligible
nodes). The blocked job's `gang` report has the same fields. A gang with
several levels that does not place says so with every level: `No single rack
or zone fits the 8 member(s) the gang needs together ...; the closest,
zone=east, fits 6`.

`topology` changes nothing else: the quorum, the timeout, member-loss
handling, preemption floors, and the rendezvous variables are as for any gang.
Preemption runs inside the domain being tried, so a gang that can only fit by
preempting does so within one domain.

## Reservations

A waiting gang holds nothing, and that is what keeps waiting cheap and
deadlock-free. Its cost is starvation: a stream of small jobs can take freed
capacity a node at a time, so a large gang never sees enough of it at once.
So a gang that has waited long enough reserves room:

```nickel
# manager config
schedulerSettings = {
  gangReservation = { after = "5m", hold = "30m" },   # the defaults; `false` turns it off
},
```

When a gang has been short of its quorum for `after` (counted from its first
block, like `placementTimeout`), the scheduler leader picks nodes for the
members its quorum still needs: among the nodes that could take its task
(not interrupted, inside its topology domain when it has one), the ones with
the most free room for it first, each reserving as many members as its
capacity holds less other gangs' reservations. From then on, every plan for a
job of the gang's band or below sees each reserved node's free capacity less
the reserved members' requests. Work beyond the reservation still places
there (backfill); work in a higher band ignores it; the gang's own plan uses
it. As the work on those nodes finishes or moves, the room accumulates for the
gang instead of going to the next small job, and the gang places once it is
all there.

A reservation ends when the gang places, its wait ends (a stop, a restart, an
update, a timeout), or `hold` passes. A wait gets one reservation: a gang that
did not place within `hold` waits on, holding nothing, so one gang cannot
fence capacity forever. A gang that can never fit on the eligible nodes, even
empty, reserves nothing.

`legion job info` shows the reservation under the blocked gang (`Reserved room
for 8 member(s) on 2 node(s) until ...`), and `-j` carries `reservedMembers`,
`reservedNodes`, and `reservationExpires` in the `gang` report.
`legion_gang_reservations` counts the gangs holding one.

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
`jax.distributed.initialize`. Legion's part is to tell each member who it is
and where to meet, and to share fate: a member that fails or loses its node
restarts the whole gang (the default `onMemberLoss`), so survivors never wait
forever on a peer that is not coming back, and no member needs to watch the
others' status.

Every member of a gang starts with:

| Variable | Value |
| --- | --- |
| `LEGION_GANG_RANK` | The member's rank, `0` to `size - 1`, unique among the gang's running members. |
| `LEGION_GANG_SIZE` | The world size: the gang's count (after `legion job scale`). |
| `LEGION_GANG_MIN` | The quorum: `gang.min`, else the count. For an elastic rendezvous, `min:size` is the range of members it may start with. |
| `LEGION_GANG_LEADER_ADDR` | The address of the node running rank 0: the address service discovery reports for its allocation. Unset if that node advertises none. |
| `LEGION_GANG_EPOCH` | The gang's incarnation: `0` for a fresh gang, one higher each time the gang's view changes or it restarts in place, and higher again after a `restart`. Every member of one incarnation shares it; a rendezvous can use it as its run id (torchrun's `--rdzv-id`). |

They are set like `LEGION_ALLOC_ID`: in the task's runtime `env`, at process
start, winning over a jobspec key of the same name
([environment variables](stateful-workloads.md#environment-variables)).

Rank 0 is the natural rendezvous host. With PyTorch, for example:

```nickel
{
  name = "worker",
  runtime = "exec",
  group = "worker",
  count = 8,
  gang = true,
  # The rendezvous port is fixed, so every member can dial rank 0 on it. A
  # container task publishes it with a static port in `network.ports`.
  config = {
    command = ["sh", "-c", m%"
      exec torchrun --nnodes="$LEGION_GANG_SIZE" --nproc-per-node=8 \
        --node-rank="$LEGION_GANG_RANK" \
        --master-addr="$LEGION_GANG_LEADER_ADDR" --master-port=29500 \
        train.py
    "%],
  },
}
```

**Ranks.** A fresh gang (a submit, or a `restart`) is ranked `0..count-1`.
Each member gets the lowest rank no running member holds, so with
`onMemberLoss = "replace"` the replacement of a lost member takes its rank,
and the survivors keep theirs. With `min` below `count`, members placed later
take the ranks above the quorum's.

**Kept current.** A member's process cannot see new environment variables,
so the values cannot change under it. When the gang's view changes (a `legion
job scale` changes the size and quorum, rank 0 is replaced on another node, a
scale-in leaves a rank at or above the new size and the members are re-ranked)
the gang's epoch goes up, and every member restarts **in place**, on its node,
with the new values: the manager sends each node the current view of its gang
members on every heartbeat, and a worker whose member started under an older
epoch restarts it. A `replace` of any member but rank 0 changes nothing for
the others, so they keep running. A scale-out that has to wait for room
refreshes the running members at once; the new members join the same epoch
when they place.

A slot-mode stateful gang also gets `LEGION_INSTANCE_SLOT`, the stable ordinal
of its instance. It usually matches the rank but is not guaranteed to; use
`LEGION_GANG_RANK` for the rendezvous. Service discovery lists the members'
addresses too, for a workload that wants all of them: a gang task with a
`service` block registers each member, and `connect` upstreams or templates
(`{{ range endpoints "worker" }}`) list the healthy ones.

A gang's [`speculativeStart`](edge-boot.md#automatic-downgrades) is capped at
`stage`, because a member's rank is assigned when its placement commits.

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
| `topologyKey` / `topologyMode` | The gang's `topology`, empty without one. |
| `topologyDomain` | The attribute value the gang places in, or for a gang that does not place, the domain that came closest. Empty without `topology`, or when a `preferred` gang spans domains. |
| `topologyDomains` | How many domains have at least one eligible node. |
| `waitingSince` / `deadline` | `job info` only (0 in a dry run): when the wait started, and when it times out, as Unix seconds. `deadline` is 0 without a timeout. |
| `reservedMembers` / `reservedNodes` / `reservationExpires` | `job info` only: the gang's [reservation](#reservations), if it holds one. 0 otherwise. |

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
  member losses by what they led to (`replace`, `restart`, `restartInPlace`,
  `fail`) and placement timeouts (`timeout`). `legion_gang_reservations`
  counts gangs holding a reservation, and `legion_gang_wakes_skipped_total`
  the capacity events too small to wake a blocked gang.

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

A gang is **never preempted below its quorum**, only **whole**. Taking one
member would idle the rest, so on its own a member at the gang's quorum is
not a victim; the quorum works like a
[`disruption.minAvailable`](preemption-and-eviction.md) floor. With `min`
below `count`, the members above the quorum can be taken one at a time, like
any task's.

When no victim set within every floor makes room, a higher-band preemptor may
take a **whole lower-band gang**: every member, on every node, in the same
transaction as its own placement (Volcano's shape). The members on the node
it places on free the room; the rest go with them, and their room is free for
the rest of the plan. The whole gang's size is its cost, so a gang is only
taken when nothing cheaper fits. A gang is taken all or nothing: if any
member is too young (`victimExemptAge`), holds node-local storage, or is not
in a lower band, none is. The evicted gang's job then waits, holding nothing,
and places whole again when there is room, like any gang. A gang that also
sets its own `disruption.minAvailable` keeps that floor: it is never taken
below it, whole or not, and a preemptor that finds nothing else reports the
floor in its placement error (`disruption.minAvailable`).

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
`min`. Either way the running members restart in place with the new
`LEGION_GANG_SIZE` and `LEGION_GANG_MIN` under a new epoch, and a scale-in
re-ranks members so the ranks stay `0..size-1`. A gang that cannot take a
world-size change mid-run sees one restart, not a gang that disagrees about
its size.

## Failure modes and diagnostics

| You see | It means | What to do |
| --- | --- | --- |
| `status: blocked`, `gang.eligibleNodes: 0` | No node satisfies the task's constraints, permit, or volume drivers. | Check the constraints (a typo waits forever). `legion job plan` shows this before submitting. |
| `status: blocked`, `eligibleNodes` > 0, `placeable` < `min` | The eligible nodes are full. | Wait, lower `count` or `min`, or add nodes of the blocked class (`blocked.key`). |
| `status: running` with a gang `blocked` block | Survivors run and a `replace` top-up waits. | Same as above. The survivors are untouched. |
| `status: failed`, eval message `did not reach its quorum within placementTimeout` | The gang waited past its `placementTimeout`. A `job.gang_timeout` event has the details. | Add capacity or lower `count`/`min`, then `legion job run`. Raise or remove `placementTimeout` if the wait was expected. |
| Every member stopped with cause `gang`, then the gang placed again | A member was lost and `onMemberLoss = "restart"` restarted the gang. `job.gang_member_lost` names the member and the cause. | Nothing, if losses are rare. A member that keeps failing spends the move budget and fails the job; look at its logs. |
| `status: failed` after `job.gang_member_lost` with `action: fail` | `onMemberLoss = "fail"`, or the task's move budget ran out. | Fix the failing member, then `legion job run`. |
| Members start minutes apart and the workload's rendezvous times out | Members start independently ([Starting together](#starting-together)): a large image or artifact on one node delays its member. | Raise the rendezvous timeout in the workload, or pre-pull the image. Set `readyTimeout` so a member that never comes up is replaced. |
| `allocation.failed` with `Not ready within gang.readyTimeout` | A member did not report healthy in time, and its gang's `onMemberLoss` applied. | Raise `readyTimeout`, pre-pull the image, or check the member's health checks. |
| Every member restarted in place at once (`LEGION_GANG_EPOCH` went up) | The gang's view changed (a scale, a new rank 0) or `restartInPlace` applied. `job.gang_member_lost` names the loss, if there was one. | Nothing: that is how members learn the new values. |
| The whole gang restarted after one member crashed and came back | `onLocalRestart = "loss"` (the default) counts a local restart as a lost member. | Expected for frameworks that cannot take a member back. Set `onLocalRestart = "ignore"` if yours can. |
| `LEGION_GANG_LEADER_ADDR` is unset | Rank 0's node advertises no `workerAddress` (and its allocation has no network address). | Set the worker's advertised address, or find rank 0 through service discovery. |
| `legion_gang_blocked_jobs` stays above zero while jobs keep placing | Smaller jobs take capacity as it frees, and the gang never sees enough at once. | A [reservation](#reservations) starts after `gangReservation.after`; lower it, or fence capacity with a node pool and `permit` ([Jobs, Tasks, and Scheduling](jobs-tasks-and-scheduling.md)). |
| A small job blocks though a node shows free room | The room is a waiting gang's [reservation](#reservations) (`legion job info` on the gang shows it). | Wait for the gang, raise the small job's band, or turn `gangReservation` off. |
| A whole gang stopped with cause `preempted` | A higher-band job could only fit by taking the whole lower-band gang ([Preemption](#preemption)). | Expected. Raise the gang's band, or give it a `disruption.minAvailable` floor. |
| A higher-band job fails to place, naming `disruption.minAvailable` | Every candidate victim is protected by a floor the user set. | Expected. |
| Gang members stopped with cause `interruption` | Their node has a pending interruption ([spot interruption](spot-interruption.md)). | Nothing: the gang's `onMemberLoss` moved them before the node went. |
| `job validate` rejects the gang | One of the [validation](#validation) rules. | The error names the fix. |
| `status: blocked`, the reason says `No single <key> fits` | A `required` topology gang: the eligible nodes have room, but no one domain holds the quorum. `topologyDomain` is the closest. | Free room in one domain, lower `count`/`min`, or switch to `mode = "preferred"`. |
| `topologyDomains: 0` | No eligible node has the topology attribute. | Check the key's spelling and that the nodes report it (`legion node info`). |

## Limitations

- **Reservations are coarse.** A reservation holds room per node, sized to
  each node's capacity, for at most `hold`; it does not predict when the work
  on those nodes will leave (Legion has no runtime estimates), so it can hold
  room that frees late while other nodes free early. There is no backfill by
  expected finish time: work beyond the reservation places, work inside it
  does not.
- **No start barrier.** Members start as their workers get them, and the
  workload's rendezvous is where they meet
  ([Starting together](#starting-together)). The rendezvous port is the
  workload's to fix: Legion gives the leader's address, not a port.
- **A changed view restarts every member.** Members cannot be told a new
  size or leader without restarting them, so a scale or a replaced rank 0
  restarts the gang in place. A workload that could take a new world size
  live has no way to ask for it.
- **In-place restarts are not simultaneous.** Under `restartInPlace` each
  worker restarts its members on its own next heartbeat, so the survivors
  restart within a heartbeat interval of each other.
- **The whole job waits.** Other tasks of a job with a waiting gang do not
  place either, and a gang `restart` or `fail` stops only the gang task's
  allocations (`fail` stops the whole job). Split unrelated tasks into
  separate jobs.
- **One job per gang.** Two jobs cannot be gang-scheduled together.
- **Topology levels are tried in order, not combined.** `keys = ["rack",
  "zone"]` prefers any one rack over a zone, without preferring racks in the
  same zone when it has to fall back, and at most sixteen domains are tried
  per plan across the levels.
- **Interruption notices come from AWS or an operator.** The worker's watcher
  reads EC2's spot notice only; other clouds' notices need an external
  handler that runs `legion node interrupt` ([spot
  interruption](spot-interruption.md)).
- **Older managers ignore `gang`.** A manager that predates the field accepts
  a jobspec with `gang` and schedules it without gang semantics.
