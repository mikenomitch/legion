# Judgement Calls: Gang Scheduling Plan

Decisions made for
[`plans/gang-scheduling-plan.md`](../plans/gang-scheduling-plan.md): first
while writing it, then while executing Phase 1 (2026-10-17), when blocked
evals and partial placement had already shipped for every job (see the plan's
"Revision: 2026-10-17"). **Entries are ranked by project impact, highest
first — the number is the rank.** Execution-time calls are inserted at their
rank and marked *(Phase 1)*; superseded entries keep their text, are marked
SUPERSEDED, and sit at the bottom.

## 1. A waiting gang holds nothing — defer the whole plan, commit zero

**Call.** When a gang cannot reach membership, the planner returns a
`Deferred` outcome and **nothing is committed**: no partial placements, no
reservation rows, no held capacity of any kind. The cluster after a deferred
plan is byte-identical to the cluster before it.

**Alternatives.** (a) Reserve what fits and wait for the rest — what every
Kubernetes-world implementation does in some form (coscheduling holds
permit-stage reservations, Kueue holds quota, native KEP-4671 holds
resources through the scheduling cycle), each therefore needing timeout
machinery to release holds and un-deadlock itself. (b) Place the partial set
and block for the remainder — Nomad's shape, which for a gang is precisely
the deadlock the feature exists to prevent.

**Trade-off.** No incremental progress guarantee: a large gang can starve
behind small jobs that keep taking freed capacity, and v1 ships no
reservation/backfill to prevent it (a named non-goal, with a
`legion_gang_blocked_jobs` gauge as the tripwire and reservation/backfill
named as the eventual fix). Also every retry replans from scratch — accepted
because a deferred plan is read-only and cheap.

**Why (simplicity, and it's structural).** Every hold-something design needs
a second mechanism to bound the hold; hold-nothing needs none — deadlock
between waiting gangs is impossible by construction, which is explainable in
one sentence at 3am. Legion can make this choice cheaply where K8s cannot
because the plan CAS already makes the eventual commit atomic; the
comparators hold resources precisely because their placement isn't.

## 2. The waiting state is the shipped blocked-eval queue, not a gang-specific backoff *(Phase 1)*

**Call.** A gang short of its quorum makes its plan place nothing and return
the same typed `PlacementBlocked` any plan that places nothing returns, so its
eval blocks in the existing blocked-eval queue: keyed by the class of the
gang's first missing replica, woken by registration, node changes, readiness,
or freed allocations on a node that could help, with missed-unblock tracking
and failover replay already built. The gang adds only its report to the
blocked detail (`detail.gang`). The plan's own design — `attempts` and
`blocked_reason` eval columns, `block_eval_for_capacity`, a 5s-to-2m jittered
backoff, a `node_join` clause — is not built.

**Alternatives.** (a) Build the plan's §3 as written, beside the shipped
queue. (b) Make the gang wake smarter than the queue: wake only when the
class's aggregate free capacity could fit the gang's shortfall.

**Trade-off.** A blocked gang is woken by *any* capacity on its class — one
freed replica's worth for a gang that needs eight — and re-plans in full each
time (read-only, and over the whole eligible list, call 12). On a large,
churning cluster with many waiting gangs that is wasted planning and a
block/unblock entry pair per wake. Accepted, documented as a limitation, and
open in `TODO.md` as "gang-aware wake"; (b) is the fix when a measurement
says it matters.

**Why (simplicity, and it is strictly better than the plan).** One waiting
mechanism for every job, keyed by what can actually help, with its failover
story already proven. (a) would have been a second, worse implementation of
the same lifecycle (time-polled rather than event-woken). It also delivers the
plan's Phase 2 allocation-removal wake for free, and it is what the
sandbox-scaling plan (its calls entry 13) built the queue to be reused for.
Supersedes call 17.

## 3. A gang that doesn't fit is *blocked*, not *failed* — and "no matching node" blocks too

**Call.** A deferred gang plan re-releases its eval as `pending` with
backoff (`release_eval_for_retry` + new `attempts`/`blocked_reason`
columns), woken by `node_join`. Both "nodes match but are full" and "no node
matches constraints" defer — distinguished loudly in the shortfall report,
bounded by `placementTimeout` (Phase 2), but never insta-failed:

```
trainer   blocked (gang)   since 14:02:11 (9 attempts)
  worker: 0/8 placed — short 2× { cpus: 4, memory: 16Gi }
```

**Alternatives.** (a) Keep today's contract — placement failure marks the
eval `failed` terminally (`src/manager/daemon.rs:1590`) and the user
resubmits; gang would then only change *what* fails, not *whether*. (b)
Block on capacity but fail fast on constraint infeasibility, so a typo'd
constraint errors immediately.

**Trade-off.** A jobspec typo (`gpu.kind: a100` on a fleet with no such
attribute) waits forever instead of failing loudly — accepted because the
shortfall report names the constraint tally on the first attempt, `job plan`
catches it pre-submit, and `placementTimeout` is the backstop. (b) lost
because the same constraint becomes satisfiable when a matching node joins —
whether a spec is "infeasible" depends on fleet timing, and a semantic that
flips on timing is exactly what this plan removes.

**Why (simplicity/ease).** "Submit and it runs when there's room" is the
mental model every queue-shaped system (Slurm, K8s pending pods, Kueue)
converges on. It also finally gives Legion the blocked-eval lifecycle Nomad
has and Legion's eval loop visibly lacks — scoped narrowly (see call 18, now moot).

## 4. The gang unit is a task's `count.num` replicas, and a gang shortfall defers the *whole* plan

**Call.** `gang` is a task-level field; membership is that task's replica
set. When any gang task in a planning pass cannot reach membership, the
entire plan defers — non-gang tasks in the same job also don't place in that
pass. Multi-task jobs where each task sets `gang: true` therefore get
cross-task atomicity for free, without a naming surface.

**Alternatives.** (a) A new group-level container ("task group" à la
Nomad/Volcano PodGroup) holding member tasks and a group `minAvailable`. (b)
A job-level `atomic: true`. (c) Per-task partial commit: place the non-gang
tasks, defer only the gang task.

**Trade-off.** A job that wants its dashboard up while its trainer waits
must be two jobs — accepted and documented. Cross-task gangs with *distinct*
quorums (launcher `min: 1` + workers `min: 8` treated as one named unit)
have no surface yet (open question 5). (c) lost because partial-commit
plans are new machinery with new observable states — today's plans already
succeed or don't as a unit, and keeping that invariant is what makes call
1 cheap.

**Why (simplicity).** Legion deliberately has no group *container* —
`group` is a co-location label (`src/manager/scheduler.rs:1035-1163`), and
reusing it for gangs would collide with its one-node meaning. A task's
`count` is already the replica-set concept users know; gang annotates it
rather than introducing a fourth grouping notion (task, group, job, gang).

**Revisited in Phase 1.** When this was written every plan was already all
or nothing, and alternative (c) lost partly because partial-commit plans
would be new machinery. Partial placement has since shipped for every job, so
(c) is now cheap: drop the gang's own placements and let the job's other
tasks place. Whole-plan deferral was kept anyway, for two reasons that did not
change: it gives two gang tasks in one job (launcher plus workers) cross-task
atomicity with no naming surface, and "a short gang places nothing" is one
sentence where "a short gang places nothing, but its siblings do" is a
special case users have to learn. The cost is unchanged and documented: other
tasks of the job wait with the gang.

## 5. Preemption never takes a gang below its quorum *(Phase 1)*

**Call.** A gang's quorum (`gang.min`, else its count) is a disruption floor
for victim selection, combined with `disruption.minAvailable` by taking the
stricter. A `gang = true` job cannot be preempted at all; with `min` below
`count`, members above the quorum can be. The preemptor's placement error
names the floor the way it already does:

```
Failed to place task api: no available nodes matching constraints, permit, or
resources; preemption found no eligible victims (2 protected by
disruption.minAvailable). ...
```

**Alternatives.** (a) Defer gang-as-victim entirely, as the plan did: a
preemptor may take one member like any allocation. (b) Whole-gang victim
selection (Volcano v1.15): taking any member takes the whole gang, and the
victim's cost is its whole size.

**Trade-off.** A gang in a preemptible band is, in practice, not preemptible:
a `critical` job that needs its GPUs waits for the gang to finish or be
stopped. That surprises anyone who chose `low` precisely to be displaceable.
Documented in both the gang and preemption docs; (b) is the eventual fix and
stays open.

**Why (correctness of the feature, then simplicity).** (a) recreates the exact
failure gang scheduling exists to prevent: seven members holding GPUs and
computing nothing because the eighth was taken — reached through preemption
instead of through placement. The floor reuses the existing
`min_available`/`task_running` check with one more term, so it costs one
column read and no new machinery, and it fails safe. (b) is a real victim
selection redesign (gang membership in the victim index, cost model, partial
eviction fallback) that belongs to the preemption plan.

## 6. `gang.min` ships with the contract, and above the quorum the rest trickle in *(Phase 1)*

**Call.** The Phase 2 quorum came forward into Phase 1. The check is the same
one with a different number: a plan defers when `running + placed < min`.
Above the quorum, the gang's remaining replicas are ordinary unplaced work
and place one at a time as room appears (open question 4's lean). A scale-in
below `min` lowers the quorum to the new count.

```nickel
gang = { min = 6 },   # count = 8: places once 6 fit together, then 7, then 8
```

**Alternatives.** (a) Ship `gang = true` alone, `min` in Phase 2 as planned.
(b) Wave above the quorum too: place the remaining replicas only all at once.

**Trade-off.** One more knob in v1, and a quorum gang can run at 6 of 8 for a
long time. (b) would hold capacity hostage for no correctness reason: the
workload declared that 6 is enough.

**Why (simplicity of the execution).** With partial placement shipped, the
quorum is three lines in the same check; splitting it into a later phase
would have meant shipping the check twice. Elastic training is the most common
gang shape after fixed MPI, and `min` is what Volcano, coscheduling, and Kueue
all converged on.

## 7. Updating a gang restarts it — `job_update` keeps remove-then-plan, blue-green is a documented pattern, not the default

**Call.** For gang jobs, the update path keeps its existing
remove-allocations-then-plan order (`src/structs/eval.rs:170-171`); what
changes is the failure mode — a deferred replan leaves the job **blocked at
zero and retrying** (`blocked (gang): updating — 0/8 placed`) instead of
today's stranded-at-zero with a dead eval. Zero-downtime updates are the
Phase 4 `createFirst` single-step pattern, opt-in via rollout config.

**Alternatives.** (a) Plan-first for gang updates — but the fresh plan then
competes with the *old gang's own allocations* for capacity, so any
same-size update on a busy cluster would falsely defer; the old gang must
free its capacity for the new gang to take it. (b) Make blue-green
(`createFirst`) the gang default — hides a 2× capacity requirement in a
default, the kind of surprise Legion's defaults avoid.

**Trade-off.** A gang update has downtime by default, and growing a gang on
a tight cluster can block at zero for a while. Accepted: for the workloads
gangs serve (training runs, batch fleets), update-equals-restart matches
reality — mixed-version gangs are broken gangs — and the blocked state is
honest and visible rather than wedged.

**Why (performance/correctness of capacity math, then ease).** (a) is
simply wrong on capacity accounting; between the remaining options, the
default that never needs 2× capacity is the one an operator can reason
about. Call 1's zero-commit defer is what makes remove-then-plan safe to
keep — the failure mode it used to have is gone.

## 8. `gang` is its own bool-or-object field, not an extension of `count`

**Call.**

```jsonnet
gang: true,                    // short form
gang: { min: 6, placementTimeout: '30m', recovery: 'gang' },  // long form
```

using the same untagged number/bool-or-object serde pattern as `count`
(`TaskCountInput`, `src/structs/job.rs:208`).

**Alternatives.** Fold it into `count` — `count: { num: 8, min: 6, atomic:
true }` — one placement-cardinality object, which the code recon even
suggested as the "natural place".

**Trade-off.** Two adjacent knobs both about replica cardinality; a reader
must learn that `count` is *how many* and `gang` is *how they arrive (and
leave)*. Accepted.

**Why (extensibility of the surface).** `placementTimeout` and `recovery`
are lifecycle policy, not cardinality — inside `count` they'd be squatters,
and `count`'s bare-number short form (`count: 8`) couldn't carry them
without contortions. `gang: true` also reads as exactly what it is; the
k8s/Volcano/Ray world made "gang" the recognized term, and discoverability
beats taxonomy.

## 9. A gang may not share a multi-task group *(Phase 1)*

**Call.** `gang` on a task that shares its `group` with any other task is
rejected at submit. The plan's version ("gang on a sidecar is rejected; the
flag on the group's primary covers the group") assumed groups co-place per
replica. They do not: a multi-task group places *every* replica of every
member on one node (one group token), all or nothing, so a gang inside one
adds nothing and its replicas could never spread.

```
error: task worker sets gang but shares group default with launcher. A
multi-task group already places all of its tasks together on one node, all
or nothing; give the gang task a group of its own (`group = "worker"`)
```

**Alternatives.** Accept `gang` in a multi-task group as a no-op, or define
gang-of-groups semantics (all tokens of the group or none).

**Trade-off.** Every task defaults to group `default`, so a multi-task job
must name its gang's group, which is one more line and one more thing to
learn. Accepted: the error says exactly what to type.

**Why (simplicity/ease — fail at submit).** A flag that silently does nothing
is the failure mode validation exists to prevent, and the error surfaces a
co-location rule many users do not know they are subject to. Gang-of-groups
has no user asking for it.

## 10. Rollout step modes on a gang group are rejected at submit

**Call.** `percentSteps`/`countSteps`/`percentEachStep`/`countEachStep` and
`slots.order: sequential` on a gang task's group fail validation:

```
error: rollout: percentSteps cannot apply to group "default": task "worker"
is a gang (its 8 replicas place and replace as one unit); remove the step
mode or remove `gang`
```

Single-step rollouts (the default) keep working, including
`capacityBlockTimeout`/`onCapacityTimeout` and both `replacement` orders.

**Alternatives.** Accept the config and coerce to one step with a warning —
nothing breaks, fewer submit errors.

**Trade-off.** A shared jobspec template with steps configured needs editing
before a task in it can become a gang. Accepted.

**Why (simplicity/ease — fail at submit, not in production).** A canary of
a gang is semantically meaningless (a training job half on old code is not
90% updated, it is 100% broken), and a config silently meaning something
other than what it says is the failure mode Legion's validation exists to
prevent. Same posture as the existing `createFirst`-rejected-for-stateful
rule.

**Amended in Phase 1.** The rule as built rejects a rollout for a gang's group
only when it *resolves to more than one step*, and `slots.order:
sequential`. `percentSteps: [100]` (or `countEachStep` at or above the count)
is one step and is accepted — which is what lets `maximal.ncl` keep a global
`percentSteps` and override it for the gang's group. The rollout engine also
forces one step for a gang group at runtime, because a rollout strategy plugin
can inject step modes after validation, and a scale-out can grow a
`countEachStep` rollout past one step. The error now names the step count and
the override that fixes it:

```
error: rollout: the step mode for group worker replaces task worker in 4
steps, but the task is a gang (its 8 replicas place and replace as one unit).
Remove the step mode for this group (for example `rolloutByGroup.worker = {
percentSteps = [100] }`) or remove `gang`
```

## 11. `job plan` reports a gang that would wait as a successful reply, not an error *(Phase 1)*

**Call.** A dry run whose gang cannot reach its quorum succeeds: no
allocations, `gangReports[i].wouldPlace: false` with `placeable`, `min`,
`eligibleNodes`, and the reason, and a message that leads with "Job would wait
(blocked) and place nothing yet". A dry run of a job without `gang` still
errors when a replica does not fit, as before.

**Alternatives.** Keep every dry-run shortfall an error, and put the gang
numbers in the error message.

**Trade-off.** Two dry-run behaviors for "doesn't fit": a gang job's plan
succeeds while a plain job's fails. Clients that treat `ok: true` as "it will
place" are wrong for gangs and must read `gangReports`.

**Why (ease, and agent-friendliness).** For a gang, waiting is the defined
behavior, not a failure — the submit would succeed and queue. An error
envelope would make an agent parse prose to recover `placeable` and `min`,
and would hide that the submit is safe. The S4 scenario (an agent decides to
submit, resize, or escalate) needs the structured answer. Making plain jobs'
dry runs succeed too would change an existing contract for no gang reason.

## 12. Gang tasks are excluded from scheduling-index truncation

**Call.** Gang tasks join `count.per`, `distinct`, volume-pinned, and
multi-task-group tasks on the no-truncation list
(`src/manager/scheduler.rs:1558-1564`); their candidates come from the full
scan path.

**Alternatives.** Keep index-served truncated candidates plus the existing
mid-task refill loop (`:1690-1740`), refilling until fit is proven or
candidates run dry.

**Trade-off.** Gang planning is O(fleet) per attempt rather than
O(candidates), and blocked gangs replan on every retry. Accepted: deferred
plans are read-only, retries are backoff-capped, and gangs are rare and
large — the constant matters less than the answer being right.

**Why (performance in the honest direction).** "Does the whole gang fit"
is a universally-quantified question; a truncated list can prove *fit* but
never *non-fit*, and a wrong "doesn't fit" turns real capacity into a
blocked job — a correctness bug wearing a performance optimization. The
plan's bench gate (no regression for gang-free workloads) keeps this cost
strictly inside the feature that pays for it.

## 13. `placementTimeout` defaults to unset — a blocked gang waits forever

**Call.** Absent `placementTimeout`, a blocked gang waits indefinitely,
loudly (status, events, gauge). The timeout is opt-in, and its only action
is `fail` (no `onTimeout` enum).

**Alternatives.** (a) Kueue's posture — a default timeout (5m) that evicts
and requeues, on the theory that unbounded waits hide problems. (b) A
timeout with a choice of actions (`fail | keepWaiting | rollback`).

**Trade-off.** An unattended blocked job can sit for days if nobody watches
the gauge — accepted, because Legion holds *nothing* while it waits (call
1), so the wait costs the cluster zero; Kueue's default exists because its
admitted workloads hold quota, a pressure Legion doesn't have. (b) lost on
knob count: with nothing held there is nothing to roll back, and
`keepWaiting` is the default spelled longer.

**Why (simplicity).** Wait-forever is the batch-queue behavior users
already understand (Slurm, pending pods), and it makes the default
consistent with the design's core invariant: waiting is free, so the
platform never gives up on your behalf — you tell it when to.

**Phase 1: `placementTimeout` is not built at all.** The field is rejected as
unknown. A blocked eval's `blocked_at` resets on every wake and re-block, so
"blocked since" for a timeout needs a durable start that survives those
cycles, plus a timer wake (blocked evals only wake on capacity) and a decided
meaning for "fail the job". Wait-forever was already the default, so shipping
without the opt-in bound changes no default; an agent can read
`blocked.since` and stop the job meanwhile. Open in `TODO.md`.

## 14. A reschedule backoff on one member holds the whole gang top-up *(Phase 1)*

**Call.** When any missing member of a gang waits out a reschedule backoff
(`reschedule_hold`), the planner withholds the *whole* top-up, not just that
member. The others place together with it on the reschedule that runs when
the last hold lapses.

**Alternatives.** Withhold only the held member, as for any task, and let the
quorum check see the rest.

**Trade-off.** Members that could run now wait out another member's backoff,
up to `move_delay` per failure. A crash-looping member delays the top-up of
members lost to an unrelated node failure.

**Why (correctness).** Withholding one member leaves `placed < needed` with
no placement *failure*, so the quorum check does not fire, and the others
place: the backoff becomes a side door to exactly the partial gang the feature
forbids — and if the held member then does not fit, the gang is stuck short
with capacity spent. Holding the whole top-up keeps "missing members place
together" true without the check having to reason about backoffs.

## 15. Changing `gang` is a scheduling change *(Phase 1)*

**Call.** `task_schedule_equivalent` now compares `gang`, so paths that move
allocations to a new config in place (a KV-triggered re-render, failover
recovery's stale-config cleanup, `job_submit` of a schedule-equivalent
config) re-plan when `gang` or `min` changed.

**Alternatives.** Leave `gang` out of the comparison: it changes how
replicas *will* place, not where existing ones run, so carrying them across
is harmless when the job is whole.

**Trade-off.** A job that only gained `gang` (and is fully placed) restarts
on those paths instead of keeping its allocations.

**Why (correctness, rarely paid).** The interesting case is a job running 6
of 8 that gains `gang`: carrying the 6 across leaves exactly the hoarding
shape the user just asked to forbid. Ordinary resubmits go through a rollout
either way, so the cost lands only on the in-place paths.

## 16. No gang-specific events; one gauge *(Phase 1)*

**Call.** The plan's `job.gang_blocked`, `job.gang_placed`, and
`job.gang_timeout` events are not built. `eval.blocked` already fires when the
gang blocks and now carries its report in `detail.gang`; `eval.unblocked`
plus `legion_blocked_wait_seconds` cover "placed after waiting this long".
One new metric, `legion_gang_blocked_jobs`, isolates gangs among blocked
evals as the starvation tripwire call 1 relies on.

**Alternatives.** Build the three events as planned, for an event-stream
consumer that wants gang lifecycle without parsing eval events.

**Trade-off.** "When did this gang start after waiting" is two events to
correlate rather than one, and `eval.blocked` fires on every re-block, not
only on a reason change as the plan's `job.gang_blocked` would have.

**Why (simplicity).** Two event kinds that restate an existing pair, staged
in the same transactions, are journal noise and two more kinds to document;
the gauge is the one signal the existing instruments cannot give.

## 17. Wake-up is `node_join` + a 2-minute-capped backoff; removal-driven wake waits for Phase 2 — SUPERSEDED by call 2

> **SUPERSEDED (Phase 1).** Blocked evals with event-driven, keyed wake on
> every capacity event shipped before this was built; see call 2. Kept as
> written for the record.


**Call.** v1 wakes blocked gangs on `node_join` (one new clause in
`reschedule_allocations_for_new_node`, `src/storage/store.rs:7950`) and
otherwise retries on jittered exponential backoff capped at 2 minutes.
Capacity freed by allocation removal is picked up by the next backoff tick,
so worst-case start latency after capacity frees is 2 minutes in v1.

**Alternatives.** Full event-driven unblocking from day one — every
allocation-removal path (job stop, batch completion, drain, failure
handling, future preemption) pokes blocked evals, a Nomad-style blocked-eval
index.

**Trade-off.** Up to 2 minutes of avoidable idle capacity per gang start in
v1, on a fleet where big capacity mostly frees via allocation removal, not
node joins. Accepted with the mitigation half-built: Phase 2 adds the
removal wake; until then the cap bounds the damage and the `gang_placed`
event records actual wait times so the latency is measurable, not
anecdotal.

**Why (simplicity, deliberately staged).** Removal paths are many and hot;
instrumenting all of them correctly (including under raft lanes) is real
surface area, and doing it under Phase 1's deadline risks the part of the
plan that matters most (the contract). A capped poll is dumb, bounded, and
obviously correct — the right v1 shape for a wake optimization.

## 18. Blocked-eval semantics are gang-opt-in for v1 — every other job keeps terminal failure — SUPERSEDED (moot)

> **SUPERSEDED (Phase 1).** Blocked semantics shipped for every job through
> the sandbox-scaling plan before gang execution, so there is no gang-only
> waiting behavior to scope. The one contract gang still changes is partial
> placement: plain jobs place what fits, gangs place all or nothing. Kept as
> written for the record.


**Call.** Only jobs with a `gang` task get defer/block/retry. A plain job
that doesn't fit still fails its eval terminally, exactly as today.

**Alternatives.** Fix the terminal-failure contract for everyone in the same
change — arguably "failed forever because the cluster was momentarily full"
is a bug for every job, and the machinery will exist.

**Trade-off.** v1 knowingly leaves the old sharp edge in place for non-gang
jobs, and the codebase temporarily has two capacity-failure behaviors.
Accepted: flipping every existing job's observable failure contract inside a
feature plan is how regressions ship. Promoted to standing open question 1,
leaning yes as its own follow-up with its own regression pass.

**Why (simplicity of rollout over reach).** A workload-opt-in feature can
ship default-on with zero operator action and zero blast radius; a
contract change for all jobs cannot. The gang work builds and proves the
machinery; the universal flip becomes a small, separately-reviewable diff.
