# Judgement Calls: Gang Scheduling Plan

Decisions made while writing
[`plans/gang-scheduling-plan.md`](../plans/gang-scheduling-plan.md), before
any code exists. **Entries are ranked by project impact, highest first — the
number is the rank.** Implementation-time calls get inserted at their rank as
the phases execute.

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

## 2. A gang that doesn't fit is *blocked*, not *failed* — and "no matching node" blocks too

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
has and Legion's eval loop visibly lacks — scoped narrowly (see call 4).

## 3. The gang unit is a task's `count.num` replicas, and a gang shortfall defers the *whole* plan

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

## 4. Blocked-eval semantics are gang-opt-in for v1 — every other job keeps terminal failure

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

## 5. Updating a gang restarts it — `job_update` keeps remove-then-plan, blue-green is a documented pattern, not the default

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

## 6. Wake-up is `node_join` + a 2-minute-capped backoff; removal-driven wake waits for Phase 2

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

## 7. `gang` is its own bool-or-object field, not an extension of `count`

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

## 8. Rollout step modes on a gang group are rejected at submit

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

## 9. Gang tasks are excluded from scheduling-index truncation

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

## 10. `placementTimeout` defaults to unset — a blocked gang waits forever

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
