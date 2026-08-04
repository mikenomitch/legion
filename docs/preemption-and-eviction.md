# Preemption and eviction

Legion has one importance axis — the job's **priority band** — and one thing
it does when a cluster is full: the scheduler displaces lower-band work so
higher-band work can run.

Without this, a full cluster is first-come-first-kept. A batch backfill that
grabbed capacity an hour ago keeps it, and an urgent production deploy fails
placement with "no available nodes"; the only remedies are manual. With it,
importance wins contention by policy, and you can run the cluster hotter
without hand-managing capacity.

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

Three rules follow from the vocabulary being closed, and they are what make
preemption safe to reason about:

- **Preemption is strictly cross-band.** An allocation is only ever a victim
  of a job in a *strictly higher* band, never a peer. `critical` never
  preempts `critical`; a displaced `low` job can cascade into `background` and
  no further. Cascades are bounded by the band count and only ever run
  downward.
- **Choosing a band below `normal` is the consent to be displaced.** There is
  no separate opt-in switch, because a field that usually means nothing is
  worse than one that always means what it says.
- **The band sets the eval-queue priority too.** One field, both meanings:
  importance orders the queue *and* governs preemption. `legion job run
  --priority N` and `--asap` still override the queue position for a single
  submission; they do not change the band, and so they do not affect
  preemption.

A typo'd band is a **submit error**, not a silent `normal`. A job its author
marked `critical` quietly losing every contention fight is exactly the class
of bug a closed vocabulary exists to prevent:

```console
$ legion job run api.ncl
error: Error parsing jobspec: unknown priority band "urgent"; expected one of
critical, high, elevated, normal, low, background
```

### Governing who may claim a band

An ungoverned top band means nothing — everyone sets it. Which bands a caller
may submit is a `bands` selector on `job` ACL rules, alongside the existing
`namespaces` / `names` / `pools` / `templates` selectors:

```json
{"resources": ["job"], "actions": ["write"], "namespaces": ["prod"],
 "bands": ["critical", "high"]}
```

Standard selector semantics apply, and they cut both ways:

- A rule that **omits** `bands` covers every band. Bands are open by default
  and become a privilege exactly where a policy says so.
- A rule that **names** `bands` grants only those. A submit outside them is
  `PermissionDenied`, with the band named in the message.
- An **unbanded** job is a `normal` job. The submit path defaults the band
  before enforcing, so a `bands` policy still matches ordinary submissions.

The gate covers every write path that can put a spec into the cluster,
including submitting **through a template** that renders a band — rendering is
not a way around it. See [auth-and-acls.md](auth-and-acls.md).

## How preemption works

When a task cannot be placed, the planner asks: would displacing lower-band
work on any node this task is *allowed* on open a slot? The pass runs **only**
after normal feasibility has already failed, so a cluster that never preempts
pays nothing for the feature.

### Which allocations are eligible

An allocation on a candidate node is a possible victim only if all of these
hold:

- its job's band is **strictly below** the preemptor's;
- it is older than `preemption.victimExemptAge` (default `60s`);
- it does **not** hold node-local storage (see
  [Stateful workloads](#stateful-workloads));
- evicting it would not take its task below its `disruption.minAvailable`
  floor, counting evictions already chosen in the same plan.

### Which of them are taken

Per node: sort eligible allocations by band ascending (take from the least
important work first), then by how closely each is sized to the outstanding
ask, then by allocation id for determinism. Take greedily until the task fits,
then run a **reprieve pass** that gives back any victim the placement turns
out not to need.

"Closely sized" matters: Legion will not kill a 32 GiB allocation to free
1 GiB when a right-sized victim exists.

Between nodes: fewest victims wins, then the plan that takes the least
important work, then the normal scoring policy decides among equally costly
options — so preemption does not undo the cluster's packing strategy. A
placement that needs no victims at all never reaches this pass.

### How it commits

This is the central correctness property, and it is a property of
*transactions*, not of scoring. Victims are chosen during planning and
committed in the **same** epoch-compare-and-set transaction that inserts the
preemptor's allocations. That one transaction:

- deletes the victim rows;
- inserts the preemptor's allocations;
- enqueues one `job_reschedule` eval per victim job, at the victim's own band
  priority;
- records each victim's stop notice with cause `preempted` and a preemptor
  link;
- stages the `allocation.preempted` and `allocation.stopping` events.

Consequences worth stating plainly:

- **There is no nomination race.** The freed capacity is occupied by the
  preemptor's rows the instant it is freed. Kubernetes' `nominatedNodeName` is
  not a reservation, which took three KEPs to partially fix; Legion's store
  shape makes the reservation free.
- **Evictions cannot outlive a failed placement.** They are the same write. A
  plan that cannot complete commits nothing at all.
- **No victim is forgotten.** Requeue is driven by the job's *desired count*,
  not by remembering the eviction, so nothing has to survive a crash for a
  victim to come back — and batch victims requeue exactly like service ones.

The worker side is [graceful stop](graceful-stop.md): the victim's removal and
the preemptor's placement arrive in the same heartbeat diff, and the worker
stops victims with grace before starting the preemptor.

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

Two deliberate properties:

- **It binds only voluntary disruption** — preemption today, a future
  rebalancer or scale-in policy tomorrow. Drain, node loss and `job stop`
  ignore it. A floor that can block mandatory maintenance is the documented
  cluster-wedge (`maxUnavailable = 0` blocking a Kubernetes upgrade is so
  common it grew an escape hatch); a floor that only shrinks preemption's
  options can never wedge anything — worst case the preemptor stays pending,
  which is the status quo.
- **It is a hard rule, not a score.** The scheduler cannot cross it. Nomad's
  penalty-not-limit budget is documented as violated in practice, including by
  its own drainer, and a budget that can be outbid is not a budget.

Percentages round **up**: `minAvailable = "50%"` of 5 protects 3. The floor is
measured against the task's live allocation count, and it holds across a whole
plan — eight placements cannot each take "one below the floor" and together
take eight.

**Understand what this does not protect you from:** an operator draining half
the cluster can take your service below `minAvailable`. That is a real gap
versus a Kubernetes PodDisruptionBudget, and it is the deliberate trade.

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
| `enforce` | **Default.** Select victims, evict, and place — atomically. |
| `observe` | Run the *identical* victim-selection pass, emit `allocation.preempt_planned` and the metric, commit nothing. The placement fails exactly as it does with preemption off. |
| `off` | Never run the pass. Bands still order the eval queue. |

`observe` is the cautious-adoption posture: the selection an operator sees is
the selection `enforce` would make, so the evidence is real before the action
is taken.

`disabledPools` exempts a pool wholesale, whatever the bands say — for a pool
whose jobs each hold hours of un-checkpointed state.

`victimExemptAge` is the cheapest thrash guard there is: without it, one plan
can undo the placement a previous plan made seconds earlier, and the
follow-up eval can undo that. An allocation with no recorded placement time
(written before this feature) is treated as old enough.

A mistyped `mode` is a **startup error**, not a silent `enforce`. Evicting
production work an operator explicitly tried not to evict is not an acceptable
failure mode for a typo.

## Previewing before you displace anyone

`legion job plan` names the exact victims a submit would take:

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

In `observe` mode the same command reports what `enforce` *would* do, labelled
as such. When the cluster has preemption off, it says so rather than showing
an empty list that reads as "nothing would be displaced".

This is a plan against a snapshot, not a reservation: the real submit
re-plans, so the victims it takes may differ if the cluster moved in between.

## When preemption still cannot place

The error says why, in band terms:

```console
error: Failed to place task "web": no available nodes matching constraints,
permit, or resources; preemption found no eligible victims (2 protected by
disruption.minAvailable, 1 below victimExemptAge). Job stays under its
desired count.
```

Other endings you may see, each naming the thing that actually bound:

| Ending | Meaning |
| --- | --- |
| `This job is in the lowest priority band (background)…` | `background` is the floor; there is nothing below it to displace. |
| `Preemption is off for this cluster…` | `schedulerSettings.preemption.mode` is `off`. |
| `Preemption is in observe mode: it identified N evictable allocation(s) but committed none…` | Switch to `enforce` to act on it. |
| `every candidate node is in a pool where preemption is disabled…` | `disabledPools` covers every feasible node. |
| `…3 holding node-local storage (never preemptible)` | A node hoarded by node-pinned stateful work is un-preemptible. |

Submitting a job whose band cannot act also warns at submit time, so the
expectation is corrected where it is formed:

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

The bidirectional victim↔preemptor link is the point. "Preempted", on its own,
is the complaint every batch platform eventually retrofits an answer to: a
user who cannot tell *which* policy displaced them cannot act on it.

**Metrics** ([metrics.md](metrics.md)):

- `legion_preemptions_total{pool,band}` — victims actually evicted, sliced by
  the two axes an operator reasons about.
- `legion_preemption_plans_total{outcome}` — preemption passes that reached a
  decision: `placed` (victims evicted, preemptor placed), `observe` (the pass
  ran under `observe` mode and committed nothing), or `no_victims` (preemption
  is enabled and still could not help). The last one is the interesting alert:
  it separates "preemption is not placing this" from "preemption never ran".

**In the job list**, the band is a column, so contention outcomes are legible
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

An allocation whose instance storage is **node-local** (`local-dir`) is
**never** a victim, categorically. Evicting one either frees nothing — the
instance can only be recreated on the same node, because that is where its
data is — or abandons the data. Both are indefensible, so the rule is
structural rather than a scoring penalty.

Stateful allocations on cluster-reachable storage (`nfs`, and future network
drivers) **are** eligible. Preemption stops them the same clean way a rollout
replacement does, and their replacement reattaches the same storage elsewhere.

The consequence to plan around: a node filled with low-band, node-pinned
stateful work is un-preemptible. The placement error says so explicitly rather
than leaving you to work it out.

## Known limitations

Stated as plainly as the features:

- **No fair share, no hierarchical queues, no quota.** Bands answer "who
  wins"; nothing here answers "who may enter". Per-namespace band caps are
  expressible today through the `bands` ACL selector; a real quota economy is
  not built.
- **No intra-band ordering.** Two `high` jobs contending for one slot resolve
  by timing. If you need "slightly more important than my other batch job",
  the answer today is a different band, not a dial.
- **`low` evals can starve behind sustained higher-band load.** The queue is
  strict priority with no aging. Real queue depths make this theoretical
  today; it is a known shape, not a fixed one.
- **No oversubscription and no node-pressure eviction.** Legion resources are
  reservations with no request/limit split, so there is no platform-created
  pressure to relieve.
- **No suspend- or checkpoint-based preemption.** Victims are stopped with
  grace and requeued. Suspend holds memory hostage; checkpoint-based schemes
  have a poor survival record.
- **No descheduler-style rebalancing.** Voluntary eviction to improve packing
  is a separate risk class. The eviction primitive is built so a future
  rebalancer can reuse it, and `disruption.minAvailable` is already worded as
  the budget it would respect.
- **Victim selection ignores network and devices.** Victims are selected on
  the builtin resource vector (cpu / cores / memory / disk / allocations, plus
  custom amounts). Device-aware victim selection is exactly where comparable
  schedulers accumulated their bugs, and it is not attempted here.
- **No preemption plugin seam.** `SchedulingPolicy` keeps its ABI; preemption
  is built in. Victim selection is factored as a pure function so the seam can
  open later around semantics that have proven out.
- **Preemption for multi-task groups is limited.** A group already assigned to
  a node can preempt on that node; a group being placed fresh preempts for its
  first member and, if a later member then cannot fit, the whole plan fails
  and commits nothing. No partial group is ever left running.
- **Preemption history is best-effort.** The event journal is bounded
  (~10k events), so `allocation.preempted` is an observability surface, not an
  audit log. Use the metrics for rates.
- **A victim's stop cause can be lost.** Stop notices are advisory (see
  [graceful-stop.md](graceful-stop.md) § Delivery guarantees), so the
  victim→preemptor link a task reads in its notice file is best-effort in the
  same way. The event journal's copy is not.

## Upgrading

Everything here is additive and default-compatible:

- Absent `priority` is `normal`; existing job configs migrate to `normal`,
  which is what absent already meant.
- Absent `disruption` is a floor of 0.
- Absent `stop` is `SIGTERM` + `30s` — a behavior change from the previous
  hard kill, deliberate and strictly gentler. See
  [graceful-stop.md](graceful-stop.md).

**Version skew.** A new manager with old workers degrades cleanly: old workers
ignore the ladder and hard-kill as before, so preemption still functions, just
gracelessly. Because that means a node can briefly run victim and preemptor
concurrently, upgrade workers before enabling `enforce` on a mixed fleet — or
run `observe` until the fleet is uniform. New workers with an old manager
receive no stop context and behave as before. See [upgrades.md](upgrades.md).

## See also

- [Graceful stop](graceful-stop.md) — the ladder every victim is stopped with.
- [Scheduler and eval loop](scheduler-and-eval-loop.md) — where the preemption
  pass sits in planning.
- [Auth and ACLs](auth-and-acls.md) — the `bands` selector.
- [Stateful workloads](stateful-workloads.md) — instance storage and why
  node-pinned allocations are exempt.
- [Metrics](metrics.md), [Event stream](event-stream.md) — the observability
  surfaces above.
