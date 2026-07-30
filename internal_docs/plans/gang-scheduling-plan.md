# Plan: Gang Scheduling

> **Judgement calls:** [`calls/gang-scheduling-plan_calls.md`](../calls/gang-scheduling-plan_calls.md)

**Status: PLANNED.** Nothing here is built. Deliberately deferred from v1:
capacity reservation / backfill anti-starvation, gang-grained preemption (both
directions — coordinated with the
[preemption plan](preemption-and-eviction-plan.md)), cross-job gangs, named
cross-task gangs, topology-aware placement (same rack/zone), autoscaler
integration, and any extension of blocked-eval semantics to non-gang jobs
(each is a named non-goal or open question below).

## Problem Statement

A distributed workload — data-parallel training, MPI, a coordinated fleet of
agent sandboxes — only makes progress when **all N replicas run at the same
time**. Legion today has no way to say that, and its failure behavior for
"doesn't all fit" is the worst of the available options:

- **Placement failure is terminal.** When any single allocation of a job
  cannot be placed, `place_task_allocations` bails
  (`src/manager/scheduler.rs:1743`: *"Failed to place task …: no available
  nodes matching constraints, permit, or resources"*), the error aborts the
  whole plan, and the eval worker marks the eval **`failed` — never retried**
  (`src/manager/daemon.rs:1590`, comment: *"failed evals are never
  retried"*). A job submitted 30 seconds before capacity frees up fails
  exactly like a job that could never fit. The user's only remedy is to
  resubmit and hope.
- **Nothing wakes a job when capacity arrives.** The only new-capacity
  reconciliation, `reschedule_allocations_for_new_node`
  (`src/storage/store.rs:7950`), re-plans `count.per` jobs and incomplete
  rollouts — an under-placed plain job is not on its list. There is no
  blocked-eval queue, no backoff retry, nothing between "placed" and "failed".
- **Partial gangs arise over a job's lifetime and then wedge.** A node loss
  takes a training job from 8/8 to 7/8; the `job_reschedule` eval tries to
  place the missing replica on a full cluster, bails, and fails terminally.
  The job now holds 7 GPUs **forever**, computes nothing (the framework is
  waiting on rank 7), and nothing will ever retry. This is the classic gang
  deadlock, reached through the side door.
- **A config update can strand a job at zero.** `handle_job_update` removes
  the job's allocations *before* planning (`src/structs/eval.rs:170-171`);
  if the new plan cannot place, the job is left with no allocations and a
  terminally-failed eval.

What a user has to do instead: size the cluster so the whole job always fits
(defeating the point of a shared cluster), babysit submits and resubmit on
failure, or run an external controller that polls `job status` and re-drives
Legion — i.e. reimplement the missing scheduler feature outside the
scheduler.

One structural fact makes this plan small rather than large: **Legion's plan
commit is already all-or-nothing.** The scheduler computes every placement
for a job config in one pass (`build_plan`, `src/manager/scheduler.rs:987`)
and applies them in one transaction under one epoch CAS
(`apply_plan_submission_apply`, `src/storage/store.rs:2176`). There is no
pod-at-a-time incremental placement to retrofit atomicity onto — the missing
pieces are the *contract* (a jobspec way to say "all of this task or
nothing"), the *waiting state* (blocked, retried, woken by capacity), and
the *diagnostics* (what is blocking, and by how much).

## Prior Art

The strongest arguments in the
[judgement calls](../calls/gang-scheduling-plan_calls.md) come from this
section. One vocabulary note up front: Slurm's feature *named* "gang
scheduling" ([time-sliced oversubscription](https://slurm.schedmd.com/gang_scheduling.html))
is a different thing. This plan uses the term the way the Kubernetes, Volcano,
and ML-platform world does: **all-or-nothing co-scheduling of a set of
replicas**.

### Kubernetes (the cautionary tale, then the fix)

Core kube-scheduler is pod-at-a-time; distributed training on vanilla K8s
partially schedules, and the placed pods **hold their GPUs while waiting for
peers that never come** — the canonical gang deadlock, and the reason gang
scheduling was, per the KEP,
["implemented outside kube-scheduler at least 4 times"](https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/4671-gang-scheduling):

- The [coscheduling plugin](https://github.com/kubernetes-sigs/scheduler-plugins)
  (PodGroup + `minMember`): pods that pass filtering wait at the Permit stage
  until quorum arrives — **holding their reserved node resources while they
  wait**, released only by timeout.
- [Volcano's gang plugin](https://volcano.sh/docs/scheduler/plugins/gang/)
  (PodGroup `minAvailable`, later
  [per-task `minAvailable`](https://github.com/volcano-sh/volcano/blob/master/docs/design/task-minavailable.md)):
  the mature standalone implementation; its
  [v1.15 gang-granularity preemption](https://volcano.sh/blog/volcano-1.15.0-release/)
  is the lesson that gang and preemption must eventually understand each
  other.
- [Kueue](https://kueue.sigs.k8s.io/docs/tasks/manage/setup_wait_for_pods_ready/)
  moved the gang to *admission*: quota is reserved all-or-nothing, and because
  quota admission still cannot guarantee node-level feasibility, it needed a
  second mechanism — "all-or-nothing with ready pods" (`waitForPodsReady`):
  if the workload's pods aren't all ready within a timeout, **evict the whole
  workload and requeue it with exponential backoff**. Two levels because
  Kueue's admission cannot see nodes.
- Native [KEP-4671 gang scheduling](https://github.com/kubernetes/enhancements/issues/4671)
  (beta targeted v1.37): a Workload API; the scheduler holds the group at a
  common stage and, on timeout, **releases all held resources** so other
  workloads can use them.

**Take:** `min`-style quorum as the one generalization worth having; the
requeue-with-backoff shape; the whole-workload recovery idea
(`waitForPodsReady`'s `recoveryTimeout`) for Phase 3. **Reject:** every
variant of *hold-resources-while-waiting*. All four K8s implementations hold
something (permit-stage reservations, quota, scheduling-cycle holds) and
therefore all four need timeout machinery to un-deadlock themselves. Legion's
plan CAS lets a waiting gang hold **nothing** by construction — the central
simplification of this design.

### Nomad

No gang scheduling; the request is an open, explicitly-invited feature ask
([#18773 "Add Gang Scheduling (Feedback Wanted)"](https://github.com/hashicorp/nomad/issues/18773)).
What Nomad *does* have that Legion lacks is the **blocked-eval queue**: an
eval that cannot fully place is parked and re-evaluated when capacity
changes, rather than failing terminally. Nomad places what fits and blocks
for the remainder — fine for stateless services, wrong for gangs (the
partial placement runs and hoards).

**Take:** the blocked-eval lifecycle — it is the piece of Nomad's scheduler
Legion's eval loop is visibly missing, and this plan builds it (scoped to
gang jobs in v1). **Reject:** partial-then-block as the *gang* semantic.

### Borg / Omega / Mesos

Borg ([2015 paper](https://storage.googleapis.com/gweb-research2023-media/pubtools/pdf/43438.pdf))
schedules task-at-a-time and leaves quorum tolerance to frameworks. The
[Omega paper](https://dl.acm.org/doi/10.1145/2465351.2465386) supplies the
classic argument this plan leans on: in offer-based two-level schedulers
(Mesos), a framework that wants gang placement must **hoard offers** until it
has enough — hoarding deadlocks the cluster when two frameworks each hold
half of what the other needs. A monolithic scheduler that plans the whole
gang in one decision does not have the problem. Legion is exactly such a
scheduler; the gang just has to be expressed to it.

### Slurm and HPC batch systems

The oldest prior art: an HPC job requests N nodes and **starts only when the
whole allocation is available**; while it waits it holds nothing and sits in
a queue. Anti-starvation is solved by
[backfill scheduling](https://slurm.schedmd.com/sched_config.html) — small
jobs fill holes only when they would not delay the reservation-holding big
job. This is decades of proof that *wait-whole, hold-nothing* is a workable
default — and that large gangs starve without a reservation story, which is
why reservation/backfill is this plan's most prominent named deferral rather
than an accident.

### Ray

[Placement groups](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html)
are the API users seem to like: declare a set of resource bundles, creation
is atomic — "all bundles or none" — with pack/spread strategies. Evidence
that the *surface* should be a small declarative flag on the workload, not
an operator-configured scheduler mode.

## Jobs to Be Done

**Job author / ML engineer**

- When I run a training job that needs all 8 workers exchanging gradients, I
  want either all 8 running or none — 6 of 8 holding GPUs while the job makes
  zero progress is the worst outcome, worse than waiting.
- When the cluster is briefly full, I want my job to wait its turn and start
  by itself when there is room, so "submit and walk away" works at night the
  way it works at 2pm.
- When my job cannot start, I want to see exactly what it is short of — how
  many more nodes of what shape — so I can decide to wait, shrink the job, or
  go ask for capacity.

**Cluster operator**

- When teams run distributed jobs on my shared cluster, I never want to find
  GPUs pinned by half-placed jobs that are computing nothing, so utilization
  reflects real work.
- When several big jobs are queued, I want waiting jobs to hold zero
  resources while they wait, so a queue of blocked work cannot itself wedge
  the cluster.
- When something has been waiting a long time, I want that loudly visible
  (status, events, a metric I can alert on), so a starving job is a page or
  a dashboard line, not a surprise in a weekly report.

**On-call responder**

- When a job is not running, I want `job status` to tell me *why* in resource
  terms — "needs 2× {4 CPUs, 16 GiB} more than the cluster has free" — so I
  can tell "waiting on capacity" from "broken" in one look, mid-incident.

**Autonomous agent**

- When I am about to submit a large job, I want a dry-run that tells me
  whether the whole gang fits *right now*, so I can choose to submit, resize,
  or escalate — before mutating the cluster.
- When my job is blocked, I want the shortfall as structured output, not
  prose, so I can act on it programmatically (wait, scale the fleet, or
  report upstream).

*Out of scope for the persona list:* security/platform admin — gang
scheduling adds no new permission surface (no new RPCs, no new ACL
vocabulary); existing `job:write` governs it.

## User Scenarios

**S1 — Training job on a briefly-full cluster.** A cluster has 6 free GPU
slots; a `gang: true, count: 8` training job is submitted. Today: the eval
fails terminally, nothing tells the user when to try again. With this plan:
nothing places, the job shows `blocked (gang): 0/8 placed — short 2× {cpus:
4, memory: 16Gi}`, a `job.gang_blocked` event fires, and when a batch job
frees two slots the gang places whole — all 8 start together, no resubmit.

**S2 — Two training jobs, capacity for one.** Jobs A and B each need 8 of
the 8 free slots. Today the epoch CAS already prevents interleaved plans from
splitting the capacity, but the loser fails terminally. With this plan: one
places whole; the other waits at zero cost and starts whole when the winner
finishes. Cluster throughput is two jobs run back-to-back — instead of one
failed submit, or (in the pod-at-a-time world) two 4/8 deadlocks.

**S3 — Node loss under a running gang.** A node dies under replica 7 of 8.
The reschedule eval cannot place the replacement (cluster full). Today: the
eval fails forever; 7 GPUs idle indefinitely. With this plan: the top-up is
itself atomic and *blocked*, retried on backoff, and woken by capacity; the
job status says `7/8 running — blocked replacing 1`. (Whether the 7 keep
running while waiting is the Phase 3 `recovery` knob; the default keeps
them, and says so in the status line.)

**S4 — Agent checks fit before submitting.** CI or an MCP agent runs
`legion job plan train.jsonnet -j` and reads `gang_reports[0].placeable:
6 < required: 8` plus the bottleneck resource vector, and decides not to
submit — or to submit knowing it will queue. The submit is a decision, not
a coin flip.

## Goals

- **A jobspec contract**: `gang` on a task makes its `count.num` replicas
  place all-or-nothing — at submit, at update, and at every reschedule
  top-up thereafter.
- **Blocked, not failed**: a gang that does not fit *waits* — the eval
  requeues with backoff, wakes on new capacity, and the job is visibly
  `blocked` with a resource-shortfall diagnosis, not `failed`.
- **A waiting gang holds nothing.** No partial placements, no reservations,
  no permit-stage holds. Zero cluster capacity is consumed by any number of
  blocked gangs — deadlock between waiting gangs is impossible by
  construction, with no timeout machinery needed to guarantee it.
- **First-class diagnostics**: `job plan` answers "would the gang fit right
  now"; `job status` answers "what exactly is it short of"; events and a
  metric make long waits observable. All of it structured for agents.
- **Composes with what exists**: group co-placement, stateful slot
  identity, dependencies, rollouts (with validation where a combination is
  meaningless), the scheduling index, and the planned preemption design.
- **Zero cost when unused**: a cluster with no gang jobs plans byte-identically
  and benches identically.

## Non-Goals

- **No reservation or backfill in v1.** A blocked large gang can starve
  behind a stream of small jobs that keep taking freed capacity — the
  known cost of *hold-nothing* waiting, and HPC's reason for backfill.
  v1 ships without it: pools/permits already give operators capacity
  fencing, and the honest fix (reserve freed capacity for the oldest
  blocked gang, backfill around it) deserves its own plan on top of this
  one's primitives. The blocked-gang metric is designed to tell us when
  this is actually hurting.
- **No gang-grained preemption, either direction.** A high-band gang using
  preemption to clear space, and whole-gang victim selection (evicting one
  member idles the rest — Volcano learned this the hard way), both belong
  to the [preemption plan](preemption-and-eviction-plan.md)'s machinery once
  it lands; §8 of the design records the integration points so the two
  plans stay compatible. The preemption plan's v1 already names gang victim
  grain as *its* deferral; this plan keeps the promise symmetric.
- **No cross-job gangs.** The gang boundary is within one job. Two jobs that
  must start together are, for now, one job with two tasks.
- **No named cross-task gangs in v1** (`gang: { name: … }` spanning tasks).
  Whole-plan deferral already gives multi-task jobs cross-task atomicity
  (§2); an explicit grouping surface can come later without breaking the
  bool form.
- **No topology-aware gang placement** (same rack / same fabric / spread
  across zones). Constraints and affinities already express node selection;
  gang adds the *cardinality* contract only.
- **No autoscaler integration yet.** A blocked gang's shortfall vector is
  exactly a scale-up demand signal, and §7's structured shortfall is
  designed to be consumed by the [autoscaler plan](autoscaler-plan.md) —
  but wiring it is that plan's work, not this one's.
- **No time-slicing / oversubscription** (Slurm's other "gang scheduling").
  Legion resources are reservations; nothing here changes that.

## Background: What Exists Today

Code recon (verified against `main` at the time of writing):

- **Plans are computed whole and committed whole.** `build_plan`
  (`src/manager/scheduler.rs:987`) plans every task of a job config in one
  pass into a local `placements` vec (`:1008`); `apply_plan_submission_apply`
  (`src/storage/store.rs:2176`) inserts all of them in one transaction,
  fenced by the epoch CAS sampled before the capacity read
  (`src/manager/scheduler.rs:697-723`). Concurrent plans for overlapping
  capacity conflict and retry (`plan_with_conflict_retries`,
  `src/structs/eval.rs:293`; test `concurrent_plans_cannot_oversubscribe_a_node`,
  `src/manager/scheduler.rs:5143`). **The all-or-nothing write exists; this
  plan adds no new commit machinery.**
- **Placement failure is a hard error, and errors are terminal.** The bail
  sites: no eligible node (`scheduler.rs:1743`), disk fit (`:1774`),
  volume-pinned node unavailable (`:1795`), group pin ineligible (`:1813`),
  `count.per` attribute empty (`:1196`), no group candidates
  (`:1224`, `:1273`), empty fleet (`:497`, `:586`, `:693`). Every one
  propagates as an `anyhow` error; `plan_with_conflict_retries` retries only
  `StorageError::Conflict` (`eval.rs:350`); `handle_claimed_eval` marks
  everything else `failed` terminally (`src/manager/daemon.rs:1590`). There
  is no partial commit — but also no distinction between "unsatisfiable"
  and "full right now".
- **The requeue primitive already exists, used once.** `release_eval_for_retry`
  (`src/storage/store.rs:9540`) flips an eval back to `pending` with a
  future `ready_at`; today only the `NotRaftLeader` path uses it
  (`daemon.rs:1600`). Its doc comment (`store.rs:9525-9538`) is practically
  a design note for this plan: failed evals are dropped forever, so work
  that must survive gets re-released instead. The `eval` table has
  `priority` and `ready_at` but no attempt counter and no blocked metadata.
- **New capacity wakes almost nobody.** `reschedule_allocations_for_new_node`
  (`src/storage/store.rs:7950`) reconciles only `count.per` jobs and
  incomplete rollouts on `node_join`. `regenerate_scheduling_evals`
  (`store.rs:9250`) is a failover/startup sweep, not a steady-state one.
- **Desired counts and top-ups.** `required_counts_for_mode`
  (`scheduler.rs:916`): `Fresh` plans the full desired set, `Reschedule`
  plans `desired − placed`. A gang top-up rides `Reschedule` unchanged —
  the atomicity requirement just extends to the missing subset.
- **`group` means co-location, not gang.** Tasks sharing a `group` string
  are pinned to one node per replica token (`build_plan` group candidates,
  `scheduler.rs:1035-1163`; pin enforcement `:1806-1824`). The natural gang
  unit is therefore **a task's `count.num` replicas** (`TaskCount`,
  `src/structs/job.rs:190`), which spread across nodes via automatic
  anti-affinity (`policy.rs:300`). Reusing `group` for gangs would collide
  with its meaning.
- **The update path can strand a job.** `handle_job_update` removes
  allocations then plans fresh (`src/structs/eval.rs:170-171`); a plan
  failure leaves zero allocations. `handle_job_submit` plans first
  (`:139`) — the asymmetry matters once a gang plan can *defer*.
- **Rollouts own the only "blocked on capacity" model.**
  `try_place_for_group` (`src/manager/rollout.rs:866`) treats a zero-placement
  `Ok` and a hard `Err` identically as `GroupAction::Wait` with
  `blocked_since` stamping (`:1002`, `:1017-1026`) and a
  `capacityBlockTimeout` / `onCapacityTimeout` escape hatch (`:1028-1046`).
  This is the waiting-state template — and it means rollouts absorb a
  gang-aware planner with almost no change, since "gang deferred" surfaces
  to the rollout engine the same way "no capacity" already does.
- **The scheduling index truncates candidate lists** when safe
  (`candidate_budget`, `scheduler.rs:1335`; mid-task refill `:1690-1740`),
  and already disables truncation for `count.per`, `distinct`,
  volume-pinned, and multi-task groups (`:1558-1564`). "Does the whole gang
  fit" needs the same exclusion — a truncated list cannot prove a negative.
- **Keyed stateful tasks have no static count** (`src/structs/job.rs:1866-1877`
  rejects `count` on keyed tasks; cardinality is live instances). A gang
  needs a knowable membership, so keyed tasks are out in v1.
- **Preemption plan adjacency.** Its design extends `PlanOutcome` and
  `plan_submission` with evictions committed in the same transaction
  (`preemption-and-eviction-plan.md` §3). Gang scheduling touches the same
  seams (plan outcome, eval retry, placement failure taxonomy); §8 keeps
  the two composable.
- **Benchmarks that gate this work**: `scheduler_speed` and
  `candidate_construction` groups in `benches/scheduler_bench.rs`
  (`:113`, `:206`), plus `eval_claim_apply_complete_latency`
  (`benches/eval_critical_path_bench.rs:50`). Scheduler changes ship with
  before/after numbers (AGENTS.md).

## Proposed Design

### 1) The jobspec surface

```jsonnet
{
  name: 'trainer',
  tasks: [{
    name: 'worker',
    runtime: 'container',
    count: 8,
    resources: { cpus: 4, memory: 16384 },

    // Short form: all `count.num` replicas place together or not at all.
    gang: true,

    // Long form (fields land across phases; all optional):
    // gang: {
    //   min: 6,                  // Phase 2: quorum — place atomically when
    //                            // >= min fit; top up toward count later.
    //   placementTimeout: '30m', // Phase 2: stop waiting; job -> failed.
    //                            // Default: wait indefinitely.
    //   recovery: 'member',      // Phase 3: member (default) | gang — what
    //                            // losing a member takes down.
    // },
  }],
}
```

`gang: true` is sugar for `gang: { min: count.num }` — deserialized with the
same untagged number-or-object pattern `count` already uses
(`TaskCountInput`, `src/structs/job.rs:208`). Serialization round-trips the
short form, like `TaskCount` does.

Validation at submit (and `job validate` / `job plan`):

- `gang` with `count.per` — rejected: *"gang cannot be combined with
  count.per (a per-attribute count has no single membership); use explicit
  constraints and a fixed count"*. Revisit as "gang per partition" only with
  a real use case.
- `gang` on a keyed stateful task — rejected: *"gang requires a fixed
  membership; keyed instance sets are dynamic"*. Slot-mode stateful is
  supported.
- `gang` on a sidecar — rejected; the gang flag lives on the group's primary
  task, and sidecars ride group co-placement.
- `min` must be `1 ..= count.num`; `min == count.num` is exactly
  `gang: true`.
- Rollout step modes on a gang task's group — rejected (§5).

### 2) Planner semantics: defer instead of bail

A new plan-level outcome joins success and error. Planning a job whose
gang task cannot reach membership does not error — it **defers**:

- `place_task_allocations` and the sibling bail sites, when the failing task
  is a gang task, record a **shortfall** instead of bailing: how many were
  required, how many could place, and a per-node tally of why the rest could
  not (constraints / permit / resources / disk / volume-pin), in the same
  taxonomy `job plan`'s report already uses.
- The plan attempt returns `Deferred(GangShortfall)` up through
  `plan_and_apply_internal`. **Nothing is committed** — no partial subset,
  no reservation, no plan-submission row. The cluster is exactly as if the
  plan never ran.
- Non-gang tasks in the same job also do not place in that pass: the plan
  stays one atomic unit, as today. A job that wants its frontend up while
  its trainer waits is two jobs. (Ranked judgement call #3.)
- Tasks gated by `dependsOn` are unaffected: gang membership is evaluated in
  the planning pass where the task is *ready* (`ready_tasks`,
  `scheduler.rs:2088`), so a prep task can run and complete while the gang
  task's pass comes later.
- **Both "cluster is full" and "no node matches constraints" defer.** A
  constraint no node currently satisfies can become satisfiable when a node
  joins (that is what `node_join` reconciliation is for); failing fast on
  one and blocking on the other would make the outcome depend on fleet
  timing. The shortfall report distinguishes them loudly, and
  `placementTimeout` (Phase 2) is the backstop for a jobspec typo that
  would otherwise wait forever.
- Gang tasks are added to the index-truncation exclusion list
  (`scheduler.rs:1558-1564`). Deciding "the gang does not fit" requires
  having seen every candidate; a truncated list plus refill loop can prove
  fit but never non-fit. Gang tasks take the scan path; everything else
  keeps the index. (Ranked judgement call #8.)
- In `Reschedule` mode the missing count *is* the membership delta: place
  all of it or defer. Running members are untouched by a deferred top-up.

### 3) The blocked eval lifecycle

When the plan defers, the eval does not complete and does not fail — it
**blocks**:

- The eval handler calls a new `Store::block_eval_for_capacity(id, delay,
  shortfall)`: built on `release_eval_for_retry`
  (`src/storage/store.rs:9540`), plus two new `eval` columns —
  `attempts` (incremented per blocked retry) and `blocked_reason` (the
  serialized shortfall, for status surfaces). The eval returns to `pending`
  with `ready_at = now + backoff(attempts)`.
- Backoff: 5s base, doubling, capped at **2 minutes**, with jitter. The cap
  is the worst-case wake latency when no explicit wake fires (below) —
  small enough that "capacity freed at 3:00, gang started by 3:02" holds,
  large enough that a hundred blocked gangs are a negligible planning load
  (a deferred plan is read-only and cheap).
- Existing machinery already covers the edges: `claim_eval`'s dedupe sweep
  (`store.rs:9087-9104`) collapses a redundant `job_update` enqueued while a
  blocked eval waits (same job, same kind); `requeue_in_flight_evals`
  (`store.rs:9159`) recovers a blocked-mid-handling eval across failover;
  `regenerate_scheduling_evals` (`store.rs:9250`) re-derives the eval if it
  is ever lost, because the job's placed count sits below desired.
- **Wake on new capacity**: `reschedule_allocations_for_new_node`
  (`store.rs:7950`) gains a third clause — job configs with a
  capacity-blocked eval get their eval's `ready_at` pulled to *now* on
  `node_join`. Allocation-removal wake (a big job freeing half the cluster
  mid-backoff) is Phase 2; until then the backoff cap bounds that latency
  at 2 minutes. (Ranked judgement call #5.)
- **The terminal-failure contract is unchanged for everything else.**
  Non-gang placement failures still fail terminally in v1. Extending
  blocked semantics to all capacity failures is the standing open question
  (#1) — this plan deliberately ships the machinery scoped to jobs that
  opted in.

The `job_update` hazard gets a scoped fix: for a job whose config has any
gang task, `handle_job_update` keeps its remove-then-plan order (an update
*is* a gang restart — the old gang's capacity must free for the new gang to
use it), but a deferred plan now leaves the job **blocked at zero and
retrying** instead of stranded at zero with a dead eval. The status line
says so: `blocked (gang): updating — 0/8 placed`. Zero-downtime gang
updates are the Phase 4 blue-green pattern. (Ranked judgement call #6.)

### 4) What the user sees

`job plan` (and the `PlanJob` RPC / MCP tool) gains a gang report:

```
$ legion job plan train.jsonnet
worker: gang of 8 — would NOT place (6 of 8 fit)
  short: 2× { cpus: 4, memory: 16Gi }
  nodes: 12 total → 6 fit; 4 full (resources); 2 excluded (constraints)
plan: defers — nothing would be placed; the job would wait as blocked
```

```
$ legion job plan train.jsonnet -j     # for agents; abbreviated
{ "ok": true, "data": { "gang_reports": [ {
    "task": "worker", "required": 8, "placeable": 6,
    "short": [ { "cpus": 4, "memory": 16384 } ],
    "node_tally": { "fit": 6, "resources": 4, "constraints": 2 },
    "would_place": false } ], ... } }
```

`job status` for a blocked job (the shortfall comes from the eval's
`blocked_reason`, so it is the *last real attempt*, not a guess):

```
$ legion job status trainer
trainer   blocked (gang)   since 14:02:11 (9 attempts)
  worker: 0/8 placed — short 2× { cpus: 4, memory: 16Gi }
  nodes: 6 fit, 4 full (resources), 2 excluded (constraints)
```

Events (staged in-transaction per the event stream's performance rules):

- `job.gang_blocked` (warn) — on the *first* deferral and whenever the
  shortfall reason changes; not on every retry (the journal is bounded).
- `job.gang_placed` (info) — a previously-blocked gang placed; carries the
  wait duration and attempt count.
- `job.gang_timeout` (error, Phase 2) — `placementTimeout` expired.

Metrics: `legion_gang_blocked_jobs` (gauge) and
`legion_gang_placements_total{outcome="placed|timeout"}` (counter) — the
starvation early-warning instruments named in the non-goals.

### 5) Rollouts and updates

A gang cannot be updated two replicas at a time — a training job running
half old code and half new is not a canary, it is a broken gang. Submit
validation therefore rejects step modes for gang groups:

```
error: rollout: percentSteps cannot apply to group "default": task "worker"
is a gang (its 8 replicas place and replace as one unit); remove the step
mode or remove `gang`
```

What remains works, and works better than today:

- **Single-step rollouts** (the default when no step mode is set) with
  `replacement: destroyFirst`: stop the old gang, place the new gang
  atomically. The rollout engine's existing wait model
  (`try_place_for_group` → `GroupAction::Wait`) absorbs a deferred gang
  plan with no new states; `capacityBlockTimeout` / `onCapacityTimeout`
  (including `rollback`) keep working as the bounded-wait escape hatch.
- `replacement: createFirst` places the *new* gang atomically before
  retiring the old one — zero-downtime, needs 2× capacity, defers (and
  waits) when 2× is not there. Documented as the blue-green pattern in
  Phase 4.
- `slots.order: sequential` on a gang group is rejected for the same reason
  as step modes.

### 6) Stateful, dependencies, and groups

- **Slot-mode stateful + gang composes.** Membership is the slot set; a
  top-up replacing slots 3 and 5 places both-or-neither. One real
  interaction is called out rather than hidden: a slot pinned to a node by
  `local-dir` storage (`pinned_placements_for_task`,
  `scheduler.rs:1657-1668`) can make a gang **permanently** blocked when
  its node is full or gone — the shortfall report names the pinned slot,
  and the public doc tells stateful gang users to prefer network-attached
  storage. The `stateful.onLost` hold gate always wins over gang recovery
  (Phase 3): a gang restart waits for holds to resolve, never force-moves
  protected state.
- **Groups**: `gang` on a group's primary task covers the group — sidecars
  co-place with their primary per token, so "all primaries or none" already
  implies "all sidecars or none".
- **Dependencies**: unchanged. Gang membership is enforced in the pass
  where the gang task is ready (§2).

### 7) Structured shortfall (the agent and autoscaler surface)

The shortfall is one JSON shape used everywhere — `blocked_reason` on the
eval, `gang_reports` in the plan reply, event metadata, and `job status
-j`:

```json
{
  "task": "worker",
  "required": 8, "placeable": 6,
  "short": [ { "cpus": 4, "memory": 16384 } ],
  "node_tally": { "fit": 6, "resources": 4, "constraints": 2,
                  "permit": 0, "disk": 0, "volume_pin": 0 },
  "since": "2026-07-29T14:02:11Z", "attempts": 9
}
```

`short` is deliberately a *list of resource requests* (what is missing),
not a node count — it is exactly the demand vector a future autoscaler
needs, and exactly what an agent needs to decide "add 2 nodes of shape X".

### 8) Preemption and future integration points (design constraints, not v1 work)

Recorded so the two plans stay composable:

- **A gang as preemptor**: when preemption lands, its victim-selection pass
  runs where this plan's defer decision is made — a gang task that cannot
  reach membership *with* eligible victims still defers; one that can
  places-with-evictions atomically (the preemption plan's transaction
  already carries both). Nothing in this plan's shapes blocks that: defer
  is decided after feasibility, exactly where the preemption pass slots in.
- **A gang as victim**: whole-gang victim grain (Volcano v1.15's lesson)
  needs the victim's gang membership visible to the preemption planner —
  the `gang` field on `TaskConfig` provides it. The preemption plan's v1
  explicitly defers this; when it revisits, the rule is: evicting any gang
  member counts as evicting the gang (its `min` floor makes the cheap
  cases free).
- **Autoscaler**: consumes §7's shortfall from blocked-gang events/metrics.
  Its plan, not this one.

## Proto, Struct, Config, and Persistence Changes

The jobspec is JSON-over-gRPC (`RunJobRequest.parsed_jobspec`,
`proto/legion.proto:956`), so `gang` itself is a struct change, not a proto
change. Proto changes are the read surfaces, all with full public doc
comments per repo rule:

- `PlanJobReply` += `repeated GangReport gang_reports` (`task`, `required`,
  `placeable`, `short` resource vectors, `node_tally`, `would_place`).
- `Eval` message (`proto/legion.proto:3778`) += `attempts`,
  `blocked_reason` (empty when not capacity-blocked).
- Job status reply += the blocked detail (per-gang shortfall + since +
  attempts), mirroring §7's shape.
- **No new RPCs** → no new entries in `tests/acl_endpoints.rs` (the
  classification test will confirm nothing was missed).

Structs and store:

- `src/structs/job.rs`: `TaskConfig.gang: Option<GangConfig>`;
  `GangConfig { min, placement_timeout, recovery }` with the untagged
  bool-or-object deserializer; validation per §1.
- `eval` table migration: `attempts INTEGER NOT NULL DEFAULT 0`,
  `blocked_reason TEXT` (nullable). Eval rows are transient; the migration
  is additive and failover-safe.
- Planner: a `Deferred(GangShortfall)` arm on the plan-attempt result
  threaded through `plan_and_apply_internal` / `plan_only_with_report`;
  bail sites gain the gang branch; index-truncation exclusion extended.
- `Store::block_eval_for_capacity` (wrapping `release_eval_for_retry`);
  `reschedule_allocations_for_new_node` gains the blocked-eval wake clause;
  eval handler maps `Deferred` → block instead of complete/fail.
- Events: `job.gang_blocked` / `job.gang_placed` / `job.gang_timeout`
  draft functions in `src/storage/events.rs`, staged in-transaction;
  `docs/event-stream.md` kind table updated.
- CLI/MCP: `job plan` and `job status` render §4; JSON envelopes carry §7
  verbatim; no new commands → `catalog()` in `src/cmd/capabilities.rs`
  unchanged (summaries of `job plan`/`job status` updated to mention gang
  reports).
- `configs/jobs/maximal.jsonnet`: `gang` on a task, both forms shown, with
  the standard explanatory comments.
- Public doc: new page `docs/gang-scheduling.md` (surface, defaults,
  blocked lifecycle, failure modes and their diagnostics, limitations:
  starvation without reservation, local-dir pin interaction, 2-minute
  worst-case wake in v1, member-recovery default); indexed from
  `docs/README.md` and `README.md`; `docs/scheduler-and-eval-loop.md` gains
  the blocked-eval state; `docs/jobs-tasks-and-scheduling.md` gains the
  gang section; `docs/agent-readiness.md` gains the structured shortfall.

## Implementation Phases

### Phase 1 — The gang contract: atomic placement, blocked evals
`gang: true` (+ validation set), planner defer with shortfall report,
blocked-eval lifecycle (columns, backoff, dedupe/failover interplay),
`node_join` wake, atomic reschedule top-ups, gang-aware `job_update`
blocking, rollout step-mode rejection (single-step rollouts work via the
existing wait model), index-truncation exclusion, `job plan` / `job status`
/ events / metrics, `docs/gang-scheduling.md`, maximal.jsonnet, scheduler
benches before/after. **Win standalone:** S1, S2, S3 — no distributed job
ever again sits half-placed or terminally failed for want of capacity;
submit-and-wait works.

### Phase 2 — Quorum and bounded waiting
`gang: { min }` (place atomically at ≥ min, top up opportunistically toward
`count.num` — the Volcano/coscheduling `minAvailable` analog for elastic
training), `placementTimeout` (expiry → job `failed` + `job.gang_timeout`;
default remains wait-forever), allocation-removal wake (freed capacity
wakes blocked gangs immediately instead of within the backoff cap), UI
blocked-gang badge on the Jobs page. **Win standalone:** elastic workloads
start sooner; operators can bound queues; wake latency drops to ~0.

### Phase 3 — Gang runtime recovery
`gang: { recovery: 'gang' }`: a member failure or loss stops the remaining
members and re-places the whole gang as one atomic wave (Kueue
`recoveryTimeout`'s shape, made explicit per task) — for frameworks that
cannot survive membership churn. One gang restart consumes one
`on_unhealthy.move_count` budget unit, so the existing escalation ladder
(restart → move → job failed) still terminates. `stateful.onLost` holds
gate the wave. Default stays `member` (today's per-replica behavior).
**Win standalone:** the 7-idle-GPUs shape of S3 becomes impossible for jobs
that opt in.

### Phase 4 — Updates and ecosystem polish
The documented blue-green gang update pattern (`createFirst` single-step,
2× capacity, atomic swap) with an integration test making it a supported
path; preemption integration per §8 when that plan's Phase 3 lands;
shortfall-as-demand handed to the autoscaler plan; UI gang panel (blocked
reason, wait time, attempts). Each item independently droppable.

## Testing Plan

Unit (Phase 1 unless noted):

- Surface: bool/object forms round-trip; `min` bounds; rejections
  (`count.per`, keyed stateful, sidecar, step modes, sequential slots) each
  produce the documented error text.
- Planner: gang shortfall at each converted bail site (constraints, permit,
  resources, disk, volume-pin, group candidates); defer commits nothing (no
  plan-submission row, ledger untouched); non-gang tasks in the same job do
  not place on defer; `Reschedule` top-up is all-or-none; `min` quorum math
  (Phase 2); index truncation disabled for gang tasks while non-gang tasks
  keep index-served candidates.
- Eval: defer → `pending` with backoff `ready_at`, `attempts` incremented,
  `blocked_reason` set; backoff caps at 2m; dedupe sweep does not drop a
  blocked eval's identity; `node_join` pulls `ready_at` forward;
  `placementTimeout` expiry fails the job (Phase 2).

Concurrency / property tests (extending the existing suite):

- Two gangs contending for capacity that fits one: exactly one places
  (extends `concurrent_plans_cannot_oversubscribe_a_node`,
  `src/manager/scheduler.rs:5143`); the loser is blocked, not failed, and
  holds zero resources (assert no allocation rows, no reservation state).
- A blocked gang plus a stream of small jobs: small jobs keep placing
  (nothing is held), and the gang places on the first retry after capacity
  fits — the "hold nothing" invariant under churn.
- Failover with a blocked eval: `requeue_in_flight_evals` /
  `regenerate_scheduling_evals` converge to exactly one pending eval for
  the gang job; `blocked_since`/attempts survive or regenerate sanely.

Integration (`tests/`, on the fake-node harness in
`tests/scheduler_integration.rs`):

- S1 end to end: 8-gang on 6 slots → blocked status with correct shortfall
  + `job.gang_blocked` event; add two nodes → whole gang starts;
  `job.gang_placed` carries the wait.
- S3: kill a node under one member on a full cluster → top-up blocks,
  survivors keep running, status shows `7/8 — blocked replacing 1`; free
  capacity → member placed. With `recovery: 'gang'` (Phase 3): survivors
  stop, one atomic re-place wave.
- Update of a running gang: deferred update leaves the job blocked-at-zero
  and retrying (not stranded); single-step `destroyFirst` rollout replaces
  the gang whole; `capacityBlockTimeout: rollback` restores the old gang.
- `job plan` gang report equals what submit then does (would_place false →
  submit blocks; true → places).
- Stateful: slot gang with network storage replaces across nodes; with
  `local-dir`, the shortfall names the pinned slot.

Regression:

- Jobs without `gang` behave byte-identically (plan output equality on the
  bench fixtures); placement failures for non-gang jobs still fail
  terminally in v1 (deliberate — see open question #1).
- **Scheduler benches before/after** (`scheduler_speed`,
  `candidate_construction`, `eval_claim_apply_complete_latency`) on base
  and branch, numbers in the PR per AGENTS.md. The gang branch must cost
  nothing when no task sets `gang` (the defer path is behind the flag; the
  only shared-path work is a per-task boolean).
- `cargo test --test acl_endpoints` (standing rule; no new RPCs expected
  to classify).

## Rollout and Backward Compatibility

- **Purely additive.** No `gang` field → no behavior change, including the
  existing terminal-failure semantics. No data migration beyond the two
  nullable/default eval columns; no stored jobspec rewrite.
- **Version skew** (per `docs/upgrades.md`): gang scheduling is entirely
  manager-side (planner + eval loop + store); workers are untouched, so
  mixed-version fleets are unaffected. The one sharp edge is *older
  managers*: `TaskConfig` does not reject unknown fields
  (`src/structs/job.rs:1308-1310` — no `deny_unknown_fields`), so a jobspec
  with `gang` submitted to a pre-gang manager is **silently accepted and
  scheduled without gang semantics**. The public doc states this, and the
  CLI's `job validate` on a new client warns when the target manager
  predates gang support (the capabilities/version handshake already exists
  for this kind of check).
- **Operator posture**: nothing to enable — the feature is workload-opt-in
  (a job says `gang`), and a cluster with no gang jobs is bit-for-bit
  unchanged. The metrics/events land with Phase 1 so the first blocked gang
  is observable from day one.
- The event journal is bounded, so gang wait *history* is best-effort;
  rates and current state live in the metrics. Stated in the public doc.

## Open Questions

1. **Should blocked-eval semantics replace terminal failure for *all*
   capacity failures, not just gangs?** The machinery will exist, and
   "failed forever because the cluster was momentarily full" is arguably a
   bug for every job. Lean: yes, as a follow-up change with its own
   regression pass — not smuggled into v1, because it changes the observable
   contract of every existing job and deserves its own announcement (and
   possibly its own TODO entry the moment Phase 1 lands).
2. **Backoff constants** (5s base, 2m cap). Lean: ship these, revisit with
   the Phase 1 integration numbers; the cap only matters until Phase 2's
   removal-wake lands.
3. **Should long-blocked gangs age up in eval priority** to resist
   starvation before a real reservation design exists? Lean: no — aging is
   a half-measure that muddies the (planned) priority-band semantics; let
   the blocked-gauge metric tell us whether starvation is real, then do
   reservation/backfill properly.
4. **Is `min < count.num` top-up allowed to trickle** (place 1 whenever 1
   fits) **or should it also wave** (place remaining k atomically)? Lean:
   trickle above `min` — elasticity is the whole point of `min`, and the
   atomic contract only ever promised the quorum.
5. **Named cross-task gangs** (`gang: { name: 'training' }` spanning a
   launcher task and a worker task with different counts). Whole-plan
   deferral already covers today's shapes; lean: wait for a concrete
   multi-role demand before adding a naming surface.

## Acceptance Criteria

- **Phase 1:** S1 and S2 pass end to end on the integration harness; a
  deferred plan leaves zero allocation rows and a `pending` eval with
  backoff; `node_join` wakes a blocked gang within one eval cycle;
  `job status` and `job plan` show the §4 surfaces with correct shortfall
  numbers; step-mode rollout on a gang group is rejected at submit with the
  documented error; benches show no regression for gang-free workloads;
  `docs/gang-scheduling.md` exists and covers surface, defaults, blocked
  lifecycle, failure modes, and the named limitations.
- **Phase 2:** a `min: 6, count: 8` gang places at 6 and tops up to 8 as
  capacity frees; `placementTimeout: '1m'` fails the job with
  `job.gang_timeout` after a minute blocked; an allocation removal wakes a
  blocked gang without waiting out its backoff.
- **Phase 3:** with `recovery: 'gang'`, killing one member's node stops the
  survivors and re-places all 8 atomically; the restart consumes exactly one
  move-budget unit; a held stateful slot delays the wave rather than being
  force-moved.
- **Phase 4:** the blue-green update integration test passes (old gang
  serves until the new gang is placed whole); the UI shows a blocked gang's
  reason and wait; preemption integration notes are reconciled with that
  plan's shipped shape.
