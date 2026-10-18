# Preemption and eviction

Each Legion job has a priority band that says how important it is. When the
cluster is full, the scheduler displaces lower-band work so higher-band work
can run.

Without preemption, whoever gets capacity first keeps it. A batch backfill that
grabbed capacity an hour ago keeps it, and an urgent production deploy fails
placement with "no available nodes". The only fixes are manual. With
preemption, the more important job wins by policy, and you can run the cluster
fuller without managing capacity by hand.

## Priority bands

```nickel
{
  name = "api",
  "priority" = "high",   # critical | high | elevated | normal | low | background
  tasks = [{ name = "server", runtime = "exec", config = { command = ["./api"] } }],
}
```

| Band | For | Eval priority | Preemptible by |
| --- | --- | --- | --- |
| `critical` | Cluster-critical work: incident-response capacity, control-plane jobs, anything whose loss is an outage. | 90 | nothing |
| `high` | Production work that pages someone when it stops. | 70 | `critical` |
| `elevated` | Production-adjacent work that matters more than the default but is not on the paging path: async workers behind a user-visible queue, near-real-time pipelines, release-gating staging. | 60 | `critical`, `high` |
| `normal` | **The default.** Everything that does not say otherwise. | 50 | everything above |
| `low` | Work that may be displaced when the cluster is full: batch backfills, nightly reports, CI fleets. | 30 | everything above |
| `background` | Fill-the-gaps work with no completion expectation: speculative jobs, opportunistic re-encodes, cache warmers. | 15 | everything above |

The set of bands is fixed. Three rules make preemption predictable:

- **Preemption only crosses bands.** An allocation can only be a victim of a
  job in a strictly higher band, never a peer. `critical` never preempts
  `critical`. A displaced `low` job can in turn displace `background` work
  and nothing further. Cascades are bounded by the number of bands and only
  run downward.
- **Choosing a band below `normal` means you accept being displaced.** There
  is no separate opt-in switch.
- **The band also sets the eval-queue priority.** One field orders the queue
  and governs preemption. `legion job run --priority N` and `--asap` still
  override the queue position for a single submission. They do not change the
  band, so they do not affect preemption.

A misspelled band is a submit error. Legion does not fall back to `normal`,
because a job marked `critical` would then quietly lose every contention:

```console
$ legion job run api.ncl
error: Error parsing jobspec: unknown priority band "urgent"; expected one of
critical, high, elevated, normal, low, background
```

### Governing who may claim a band

If anyone can use the top band, everyone will, and it stops meaning anything.
Control which bands a caller may submit with a `bands` selector on `job` ACL
rules. It sits alongside the existing `namespaces` / `names` / `pools` /
`templates` selectors:

```json
{"resources": ["job"], "actions": ["write"], "names": ["prod-*"],
 "bands": ["critical", "high"]}
```

Standard selector semantics apply:

- A rule that omits `bands` covers every band. Bands are open by default and
  are restricted only where a policy says so.
- A rule that names `bands` grants only those. A submit outside them fails
  with `PermissionDenied`, and the message names the band.
- A job with no band is a `normal` job. The submit path sets the default band
  before enforcing, so a `bands` policy still matches ordinary submissions.

The check covers every write path that can put a spec into the cluster. That
includes submitting through a template that renders a band, so templates
can't be used to get around it. See [auth-and-acls.md](auth-and-acls.md).

## How preemption works

When a task cannot be placed, the planner checks whether displacing lower-band
work on any node the task is allowed on would open a slot. This pass runs only
after normal placement has failed, so a cluster that never preempts pays
nothing for it.

### Which allocations are eligible

An allocation on a candidate node is a possible victim only if all of these
hold:

- Its job's band is strictly below the preemptor's.
- It is older than `preemption.victimExemptAge` (default `60s`). The one
  exception is described in [Which of them are taken](#which-of-them-are-taken).
- It does not hold node-local storage (see
  [Stateful workloads](#stateful-workloads)).
- Evicting it would not take its task below its `disruption.minAvailable`
  floor, counting evictions already chosen in the same plan.

### Which of them are taken

On each node, the planner sorts eligible allocations by band, lowest first. It
then sorts by how closely each one matches the outstanding request, then by
allocation ID so the result is deterministic. It takes victims in order until
the task fits. A reprieve pass then gives back any victim the placement turns
out not to need.

Size matching means Legion won't kill a 32 GiB allocation to free 1 GiB when a
right-sized victim exists.

The age guard never pushes a placement up the band ladder. Suppose the only
old-enough victims on a node are in a more important band than younger work
that would also make room, such as a `normal` service next to a `low` job
placed 30 seconds ago. Then the younger, lower-band work is taken instead.
Otherwise the displaced `normal` job would requeue and, once the `low` work
aged past the guard, preempt it to come back: two evictions where one does.
The guard never makes a preemption possible that otherwise isn't. When only
young work could make room, it stays protected and the placement waits.

The wait is on the clock, not on the cluster. A victim getting older is not a
capacity change, so nothing would wake a blocked eval for it. When the guard
is the only thing in the way, the eval retries once the youngest victim it
needs is old enough, and preempts then. The `eval.blocked` event says when:

```text
Failed to place task vm: no available nodes matching constraints, permit, or
resources; preemption found no eligible victims yet (4 below victimExemptAge).
The victims it would take age past victimExemptAge in 23s, and placement
retries then
```

A plan that places some replicas and leaves one waiting this way queues a
delayed `job_reschedule` for the same moment.

Across nodes, the plan with the fewest victims wins. Ties go to the plan that
takes the least important work. Remaining ties go to the normal scoring policy,
so preemption does not undo the cluster's packing strategy. A placement that
needs no victims never reaches this pass.

### How it commits

Victims are chosen during planning. They are committed in the same
epoch-compare-and-set transaction that inserts the preemptor's allocations.
That one transaction:

- Deletes the victim rows.
- Inserts the preemptor's allocations.
- Enqueues one `job_reschedule` eval per victim job, at the victim's own band
  priority.
- Records each victim's stop notice with cause `preempted` and a link to the
  preemptor.
- Stages the `allocation.preempted` and `allocation.stopping` events.

This has three consequences:

- **No other job can grab the freed capacity.** The preemptor's rows occupy it
  the moment it is freed. In Kubernetes, `nominatedNodeName` is not a
  reservation, and it took three KEPs to partly fix that. In Legion, the
  reservation comes from the single transaction.
- **Evictions cannot outlive a failed placement.** They are part of the same
  write. A plan that cannot complete commits nothing.
- **No victim is forgotten.** Requeue is driven by the job's desired count,
  not by remembering the eviction. Nothing has to survive a crash for a victim
  to come back, and batch victims requeue the same way service victims do.

On the worker side, victims go through [graceful stop](graceful-stop.md). The
victim's removal and the preemptor's placement arrive in the same heartbeat
diff. The worker stops the victims with grace before it starts the
preemptor.

## Protecting a task: `disruption.minAvailable`

```nickel
{
  name = "web",
  count = 6,
  disruption = {
    # Voluntary disruption never takes this task below the floor.
    # Absolute count or percent of count. Default 0.
    minAvailable = "50%",
  },
}
```

The floor has two properties:

- **It applies only to voluntary disruption.** Today that means preemption.
  A future rebalancer or scale-in policy would respect it too. Drain, node
  loss, and `job stop` ignore it. A floor that can block mandatory maintenance
  can wedge a cluster. In Kubernetes, `maxUnavailable = 0` blocking an upgrade
  is common enough that it got an escape hatch. A floor that only limits
  preemption can't wedge anything. At worst, the preemptor stays pending,
  which is what would happen without preemption.
- **It is a hard rule, not a score.** The scheduler cannot cross it. Nomad's
  budget is a penalty rather than a limit, and it is documented as being
  violated in practice, including by Nomad's own drainer.

Percentages round up: `minAvailable = "50%"` of 5 protects 3. The floor is
measured against the task's live allocation count, and it holds across a whole
plan. Eight placements cannot each take one allocation below the floor and
together take eight.

When the floor (or anything else) leaves only some of a job's placements
possible, the plan places those and reports the rest as unplaced (see
[Partial placement](scheduler-and-eval-loop.md#partial-placement)). For
example, a `high` job that wants 2 replicas on a node full of a 2-allocation
`low` task with `minAvailable = "50%"` evicts one `low` allocation, places one
replica, and leaves the other unplaced with `preemption found no eligible
victims (1 protected by disruption.minAvailable)`.

A [gang](gang-scheduling.md)'s quorum is a floor of the same kind: preemption
never leaves fewer than `gang.min` members running (all of them for `gang =
true`), because taking one member idles the rest. The stricter of the two
floors binds, and the placement error names `disruption.minAvailable` either
way. A gang that preempts does so as a unit: if every replica cannot fit with
the victims it may take, the plan evicts nothing.

**What this does not protect you from:** an operator draining half the
cluster can take your service below `minAvailable`. A Kubernetes
PodDisruptionBudget would block that. Legion accepts this gap so the floor can
never block maintenance.

## Operator controls

```nickel
{
  schedulerSettings = {
    preemption = {
      mode = "enforce",          # enforce | observe | off   (default enforce)
      disabledPools = ["gpu"],   # pools where preemption never runs
      victimExemptAge = "60s",
    },
  },
}
```

| Mode | Behavior |
| --- | --- |
| `enforce` | **Default.** Select victims, evict, and place, in one atomic step. |
| `observe` | Run the same victim-selection pass, emit `allocation.preempt_planned` and the metric, and commit nothing. The placement fails the same way it does with preemption off. |
| `off` | Never run the pass. Bands still order the eval queue. |

Use `observe` to try preemption safely. The selection you see is the one
`enforce` would make, so you can check it before turning eviction on.

`disabledPools` exempts a whole pool, whatever the bands say. Use it for a
pool whose jobs each hold hours of un-checkpointed state.

`victimExemptAge` is a cheap guard against thrashing. Without it, one plan
could undo the placement a previous plan made seconds earlier, and the
follow-up eval could undo that. An allocation with no recorded placement time
(written before this feature existed) counts as old enough. The guard yields
when honoring it would mean evicting a more important band instead (see
[Which of them are taken](#which-of-them-are-taken)).

A misspelled `mode` is a startup error. Legion does not fall back to
`enforce`, because a typo should not evict work you meant to protect.

## Previewing before you displace anyone

`legion job plan` names the victims a submit would take:

```console
$ legion job plan api.ncl
Job config new; showing placements
+---------+----------------+-----------+
| node id | task config id | task name |
+---------+----------------+-----------+
| node-d  | 7              | web       |
+---------+----------------+-----------+
Preemption: would preempt
  node-d: batch-transcode/worker (alloc 412, priority low) — 30s grace, requeued
```

In `observe` mode, the same command reports what `enforce` would do and labels
it that way. When preemption is off, the output says so, instead of showing an
empty list that looks like "nothing would be displaced".

When the victims are younger than `victimExemptAge` and the guard is all that
stands in the way, the plan still shows the placement and the victims, with
how long the submit will wait before taking them:

```console
Preemption: would preempt
  node-b: band-fill/vm (alloc 88, priority low) — 30s grace, requeued, once past victimExemptAge in 23s
```

`job plan -j` carries the same wait as `eligibleInSeconds` on each entry of
`preemptions` (0 for a victim the submit takes right away). Before this, the
plan failed with "no available nodes" in that window while the submit went on
to preempt a minute later.

The plan runs against a snapshot and reserves nothing. The real submit plans
again, so it may take different victims if the cluster changed in between.

## When preemption still cannot place

The error explains why, in terms of bands:

```console
error: Failed to place task "web": no available nodes matching constraints,
permit, or resources; preemption found no eligible victims (2 protected by
disruption.minAvailable, 1 below victimExemptAge). Job stays under its
desired count.
```

Other endings you may see, each naming the limit that applied:

| Ending | Meaning |
| --- | --- |
| `This job is in the lowest priority band (background)…` | `background` is the lowest band. There is nothing below it to displace. |
| `Preemption is off for this cluster…` | `schedulerSettings.preemption.mode` is `off`. |
| `Preemption is in observe mode: it identified N evictable allocation(s) but committed none…` | Switch to `enforce` to act on it. |
| `every candidate node is in a pool where preemption is disabled…` | `disabledPools` covers every feasible node. |
| `…3 holding node-local storage (never preemptible)` | A node full of node-pinned stateful work can't be preempted. |

If a job's band can't displace anything, the submit also prints a warning:

```console
$ legion job run api.ncl
Job submit success
warning: job api is priority `high`, but preemption is off for this cluster
(schedulerSettings.preemption.mode). The band will order the eval queue and
cannot displace running work. See docs/preemption-and-eviction.md
```

## Observability

**Events** ([event-stream.md](event-stream.md)):

| Kind | Level | When |
| --- | --- | --- |
| `allocation.preempted` | warn | Per victim job: what was displaced, its band, and the preempting job and band. |
| `allocation.preempt_planned` | info | `observe` mode: what enforce would have taken. |
| `allocation.stopping` | info | The victims entered their grace window, with the window length. |

```console
$ legion events list --kind allocation.preempted -j
{
  "ok": true,
  "data": { "events": [{
    "kind": "allocation.preempted",
    "level": "warn",
    "jobId": "batch-transcode",
    "message": "Preempted 1 allocation(s) of job batch-transcode (low) to place job api (high)",
    "metadata": {
      "count": 1, "band": "low",
      "preemptorJob": "api", "preemptorBand": "high",
      "allocations": [{"allocationId": 412, "nodeId": "node-d", "taskConfigId": 7}]
    }
  }]}
}
```

The event links each victim to its preemptor and back. A user who only sees
"preempted" can't tell which job displaced them, so they can't act on it.

**Metrics** ([metrics.md](metrics.md)):

- `legion_preemptions_total{pool,band}`: victims evicted, by pool and band.
- `legion_preemption_plans_total{outcome}`: preemption passes that reached a
  decision. Outcomes are `placed` (victims evicted, preemptor placed),
  `observe` (the pass ran under `observe` mode and committed nothing), or
  `no_victims` (preemption is enabled and still could not help). Alert on
  `no_victims`. It separates "preemption is not placing this" from
  "preemption never ran".

**In the job list**, the band is a column, so you can see contention outcomes
without reading specs:

```console
$ legion job list
+-----------------+--------+----------+----------------------+----------------------+
| name            | status | priority | created at           | updated at           |
+-----------------+--------+----------+----------------------+----------------------+
| api             | done   | high     | 2026-07-28T18:00:00Z | 2026-07-31T09:12:00Z |
| batch-transcode | done   | low      | 2026-07-28T17:02:00Z | 2026-07-28T17:02:00Z |
+-----------------+--------+----------+----------------------+----------------------+
```

## Stateful workloads

An allocation whose instance storage is node-local (`local-dir`) is never a
victim. Evicting one would either free nothing or abandon the data. It frees
nothing if the instance is recreated on the same node, where its data lives.
This is a fixed rule, not a scoring penalty.

Stateful allocations on cluster-reachable storage (`nfs`, and future network
drivers) are eligible. Preemption stops them cleanly, the same way a rollout
replacement does, and their replacement reattaches the same storage elsewhere.

Plan around this: a node filled with low-band, node-pinned stateful work can't
be preempted. The placement error says so.

## Known limitations

- **No fair share, hierarchical queues, or quota.** Bands decide who wins a
  contention. Nothing here decides who may enter. You can limit which
  bands a caller may submit with the `bands` ACL selector, combined with
  `names` or `pools`, but there is no quota system.
- **No ordering within a band.** Two `high` jobs contending for one slot are
  resolved by timing. If one job needs to beat another, put it in a different
  band.
- **`low` evals can starve behind sustained higher-band load.** The queue is
  strict priority with no aging. At typical queue depths this doesn't happen
  today, but it is not fixed.
- **Node-pressure eviction is a separate mechanism.** Preemption makes room
  for a placement, decided by the scheduler. When a node runs short of memory
  because tasks burst past their reservation (`resources.memoryMax`), or
  because its worker type oversubscribes memory, the node's own memory
  pressure response acts instead. It uses the same order (lowest band first)
  and the same rule that node-pinned storage is never evicted, and its
  evictions stop with `cause = node_pressure`. Before it evicts anything it
  suspends the instances whose task sets `suspend.onPressure = true`, which
  keeps their state (see
  [suspend and resume](suspend-and-resume.md#under-memory-pressure)). See
  [Memory pressure](resource-enforcement.md#memory-pressure) and
  [Oversubscription](resource-enforcement.md#oversubscription).
- **No suspend- or checkpoint-based preemption.** Preemption victims are
  stopped with grace and requeued, even when their task sets
  `suspend.onPressure`: that opt-in covers node memory pressure only, where
  the alternative is an eviction, not a placement decision.
- **No descheduler-style rebalancing.** Evicting work just to improve packing
  carries different risks. The eviction code is built so a future rebalancer
  can reuse it, and `disruption.minAvailable` is already defined as the budget
  it would respect.
- **Victim selection ignores network and devices.** Victims are selected on
  the built-in resource vector (cpu / cores / memory / disk / allocations,
  plus custom amounts). Device-aware victim selection is where comparable
  schedulers have had many bugs, so Legion does not attempt it.
- **No preemption plugin interface.** `SchedulingPolicy` keeps its ABI, and
  preemption is built in. Victim selection is written as a pure function so a
  plugin interface can be added later.
- **Preemption for multi-task groups is limited.** A group already assigned to
  a node can preempt on that node. A group being placed fresh preempts for its
  first member. If a later member then cannot fit, the whole plan fails and
  commits nothing. No partial group is ever left running.
- **Preemption history is best-effort.** The event journal is bounded (about
  10k events), so treat `allocation.preempted` as an observability signal, not
  an audit log. Use the metrics for rates.
- **A victim's stop cause can be lost.** Stop notices are advisory (see
  [graceful-stop.md](graceful-stop.md) § Delivery guarantees). The
  victim→preemptor link a task reads in its notice file is best-effort in the
  same way. The event journal's copy is not.

## Upgrading

All of these changes are additive and keep existing defaults working:

- A missing `priority` means `normal`. Existing job configs migrate to
  `normal`, which is what a missing value already meant.
- A missing `disruption` means a floor of 0.
- A missing `stop` means `SIGTERM` + `30s`. Earlier releases hard-killed
  tasks, so this is a gentler behavior change. See
  [graceful-stop.md](graceful-stop.md).

**Version skew.** A new manager with old workers still works. Old workers
ignore the stop ladder and hard-kill as before, so preemption still works
without the grace period. This means a node can briefly run the victim and
the preemptor at the same time. Preemption runs in `enforce` mode by
default, and managers must be upgraded before workers (see
[upgrades.md](upgrades.md)). So a newly upgraded manager starts preempting
while old workers are still in the fleet. To avoid that window, set
`schedulerSettings.preemption.mode = "observe"` in the manager config before
you upgrade the managers. Switch it to `enforce` once every worker is
upgraded. New workers with an old manager receive no stop context and behave
as before.

## See also

- [Graceful stop](graceful-stop.md): the ladder every victim is stopped with.
- [Scheduler and eval loop](scheduler-and-eval-loop.md): where the preemption
  pass sits in planning.
- [Auth and ACLs](auth-and-acls.md): the `bands` selector.
- [Stateful workloads](stateful-workloads.md): instance storage and why
  node-pinned allocations are exempt.
- [Metrics](metrics.md), [Event stream](event-stream.md): the observability
  surfaces above.
