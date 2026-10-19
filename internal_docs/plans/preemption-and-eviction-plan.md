# Plan: Preemption and Eviction

> **Judgement calls:** [`calls/preemption-and-eviction-plan_calls.md`](../calls/preemption-and-eviction-plan_calls.md)

**Status: SHIPPED.** Phase 1 was split into 1a (stop causes) and 1b (graceful
termination); **1a, 1b, 2 and 3 have all shipped**, along with Phase 4's UI
item. Public docs:
[`docs/graceful-stop.md`](../../docs/graceful-stop.md) and
[`docs/preemption-and-eviction.md`](../../docs/preemption-and-eviction.md).

Two deviations from the plan as written, both recorded in the
[judgement calls](../calls/preemption-and-eviction-plan_calls.md):

- **Six bands, not four.** `elevated` and `background` were added between and
  below the original four, which kept their names and their eval priorities
  exactly (call 5).
- **Phase 1b broke the `stop_with` ABI** — `R<()>` → `R<bool>` — and added the
  plugin ABI version symbol this plan listed as a to-do, so a stale dylib
  fails at load rather than mismatching silently (call 8).

Still open from Phase 4, deliberately: `legion node drain` rate limiting via
the disruption vocabulary (evaluated and not taken — drain has its own
deadline-bounded semantics, and giving it a budget re-opens exactly the
cluster-wedge risk call 10 exists to avoid), and the fair-share revisit note.

Deliberately deferred from v1: fair-share / hierarchical queues,
oversubscription and node-pressure eviction, suspend-based preemption,
descheduler-style rebalancing, network/device-aware victim selection, and a
preemption plugin seam (each is a named non-goal below, and each is restated
in the public doc's Known limitations).

**Phase 1 was split during implementation.** The original Phase 1 bundled the
stop *cause* with the stop *ladder*. Two findings forced them apart:

1. **Grace cannot be added where `stop()` is called today.** The worker's
   `reconcile_allocations` runs while holding the task-manager mutex, inside
   the async heartbeat loop (`src/worker/daemon.rs`). A `stop()` that waits
   for a deadline holds that lock — the one heartbeats, the self-fence
   watchdog, health reporting and log maintenance all need — for up to
   `killAfter`. At a 2s heartbeat cadence, a 30s grace window would let the
   manager declare the node lost and reschedule everything on it, which is
   strictly worse than the hard kill it replaces. Graceful stop therefore
   needs a *terminating set* on the worker (signal, record deadline, return;
   a reaper finishes the job), not merely a richer argument to `stop()`.
2. **The cause needed no grace to be useful, and the notice file needed grace
   to be useful at all.** A stop notice written immediately before SIGKILL is
   a file no task can read. So the cause, the wire delivery and the event
   metadata shipped as 1a; the ladder, the notice file and
   `stop { signal, killAfter }` are 1b.

See [Phase 1a](#phase-1a--stop-causes-shipped) and
[Phase 1b](#phase-1b--graceful-termination) below, and the
[judgement calls](../calls/preemption-and-eviction-plan_calls.md) for the
reasoning.

## Problem Statement

Legion has no notion of workload importance and no way for the platform to
take resources back. Concretely, today:

- **A full cluster is first-come-first-kept.** When nodes are full, a new job
  — no matter how urgent — fails placement with "no available nodes matching
  constraints, permit, or resources" (`src/manager/scheduler.rs:1354`) and its
  eval is marked `failed`. The batch backfill that grabbed the capacity an
  hour ago keeps it. The only remedies are manual: stop someone else's job or
  add nodes.
- **`priority` exists only on evals** and orders the eval *queue*
  (`src/storage/store.rs:8146`), not placement. Two jobs contending for one
  node slot resolve by timing, never by importance.
- **Every stop is a hard kill.** `TaskPlugin::stop`
  (`src/task_plugins/mod.rs:370`) takes no signal, no grace, no deadline;
  `stop_allocation_inner` calls `stop()` then `destroy()` back to back. A
  rollout replacement, a drain, a `job stop`, and a reschedule all terminate
  tasks with no notice. There is no `killAfter`, no SIGTERM→SIGKILL ladder,
  no way for a task to flush, checkpoint, or drain connections.
  *(Still true after Phase 1a — that phase made stops **explicable**, not
  gentler. The kill is as hard as it was; Phase 1b is what changes it.)*
- **The TODO already names this gap**: "Priorities and eviction/preemption,
  with eviction toggled at the pool level" and "at least think through
  fair-share policies — find the 80/20" (`internal_docs/TODO.md`).

What a user has to do instead: run important and unimportant work in separate
clusters (or separate pools sized for peak), and accept that any platform-
initiated stop may corrupt or lose in-flight work.

This plan adds three things, in dependency order: a **graceful-stop contract**
for every platform-initiated termination (whose first half — the stop cause —
has shipped); a **job priority** expressed as a
small set of named bands; and **scheduler preemption** — when a higher-band
job cannot place, the scheduler evicts lower-band allocations atomically
inside the placement plan, with grace, requeue, and full observability.

## Prior Art

The research below drives most of the design; the strongest arguments in the
[judgement calls](../calls/preemption-and-eviction-plan_calls.md) cite it.

### Nomad (closest comparator)

Job [`priority`](https://developer.hashicorp.com/nomad/docs/job-specification/job)
is an integer 1–100 (default 50). Preemption is
[operator config, not jobspec](https://developer.hashicorp.com/nomad/api-docs/operator/scheduler)
— default on for system jobs only, off for service/batch (Enterprise-only
until [0.12 open-sourced it](https://www.hashicorp.com/en/blog/announcing-hashicorp-nomad-0-12-beta)).
Victim selection (`scheduler/feasible/preemption.go`): only allocations
**≥10 priority points below** the preemptor are eligible ("to prevent
cascading preemptions"), grouped lowest-priority-first, then chosen by a
normalized resource-distance score (prefer victims sized like the ask), with
a final pass dropping unneeded victims. The plan applier marks victims
evicted and creates a
[follow-up evaluation per preempted job](https://developer.hashicorp.com/nomad/docs/concepts/scheduling/preemption)
so service victims reschedule elsewhere. `nomad plan` previews would-be
victims.

Where it hurts in practice:

- **Discoverability**: priority does nothing until an operator flips a
  cluster API most users never find; "preemption not working" has its own
  cottage industry of troubleshooting guides, and users repeatedly expect
  priority to order the queue
  ([#11434](https://github.com/hashicorp/nomad/issues/11434),
  [#16285](https://github.com/hashicorp/nomad/issues/16285)).
- **Requeue holes**: preempted dispatch/batch allocations are never replaced
  ([#9890](https://github.com/hashicorp/nomad/issues/9890)).
- **No real disruption budget**: `migrate.max_parallel` acts on preemption
  only as a 50-point score *penalty*, and even drains violate it
  ([#12109](https://github.com/hashicorp/nomad/issues/12109),
  [#25979](https://github.com/hashicorp/nomad/issues/25979)).
- **Resource-model gaps**: preemption misses reserved `cores`
  ([#12712](https://github.com/hashicorp/nomad/issues/12712)), multi-device
  asks ([#27820](https://github.com/hashicorp/nomad/issues/27820)), and once
  segfaulted on GPU jobs
  ([#11342](https://github.com/hashicorp/nomad/issues/11342)) — device and
  network preemption is where its bugs accumulated.

**Take:** victim scoring shape (lowest priority first, closest fit, minimal
set), follow-up evals for victims, plan-time preview. **Reject:** the raw
integer + hidden 10-point rule (make the band structural instead), the
operator-config off-switch as the *only* enablement (discoverability
failure), score-penalty "budgets".

### Kubernetes

[PriorityClass + preemption](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/)
is always-on: an int32 space up to 1e9 (system classes above that), victims
chosen per node minimizing PDB violations then priority, and — the two
famous sharp edges — **PDBs are only best-effort against preemption** (the
KEP to guarantee them was
[abandoned](https://github.com/kubernetes/enhancements/issues/3280)), and
the preemptor gets a `nominatedNodeName` that is **not a reservation**: the
freed capacity can be stolen while victims drain, which took three KEPs
([1923](https://github.com/kubernetes/enhancements/blob/master/keps/sig-scheduling/1923-prefer-nominated-node/README.md),
[4832](https://github.com/kubernetes/enhancements/issues/4832),
[5278](https://github.com/kubernetes/enhancements/issues/5278)) to
partially fix and still produced an
[8,000-eviction preemption loop](https://github.com/kubernetes/kubernetes/issues/130429).
Beyond the scheduler, K8s has **~9 distinct eviction paths** (scheduler
preemption, API eviction/drain, kubelet node-pressure, taint manager, …)
with inconsistent PDB/grace semantics —
[Ahmet Balkan's taxonomy](https://ahmet.im/blog/kubernetes-evictions/) is
the canonical indictment, and kubelet pressure eviction ignores PDBs
entirely and races the kernel OOM killer. PDBs themselves are the
documented way to wedge a cluster upgrade (`maxUnavailable: 0`,
crash-looping pods counting against the budget — fixed only in v1.31 by
[`unhealthyPodEvictionPolicy: AlwaysAllow`](https://kubernetes.io/blog/2023/01/06/unhealthy-pod-eviction-policy-for-pdbs/)).
The unowned 2^31 priority space produces the "everyone sets 1000000000"
arms race that platforms then patch with
[quota scoped to PriorityClass](https://kubernetes.io/docs/concepts/policy/resource-quotas/#limit-priority-class-consumption-by-default)
and admission webhooks. Batch platforms (Kueue, Volcano, YuniKorn)
reimplement preemption wholesale because the scheduler's is pod-at-a-time
with no queueing, reclaim, or gang awareness.

**Take:** always-meaningful priority (no hidden enablement switch);
graceful termination during preemption; events with cause. **Reject:**
best-effort budgets (worst of both worlds), nomination-without-reservation
(Legion's plan CAS can do strictly better), floor-style budgets gating
mandatory maintenance, the giant integer space, and any second eviction
path with different semantics.

### Borg

The [2015 paper](https://storage.googleapis.com/gweb-research2023-media/pubtools/pdf/43438.pdf)
and [2020 follow-up](https://john.e-wilkes.com/papers/2020-EuroSys-BorgTNG.pdf):
priority collapsed into **non-overlapping bands** (monitoring > production >
batch > best-effort) after fine-grained priorities caused preemption
cascades — the fix was structural: **no preemption within the production
band**. Quota is sold *at a priority* (admission control), victims go back
to the pending queue (no migration/hibernation), and tasks can request a
SIGTERM notice before SIGKILL — delivered ~80% of the time, i.e. best-effort
notice was a known weakness. Production-tier eviction is genuinely rare
(<0.2% of prod collections ever saw one) because eviction *rate* is an SLO.

**Take:** bands with a no-preemption-within-the-top-band rule; victims
requeue; the observation that admission (quota) and preemption (priority)
are separable — we defer the quota half. **Reject:** best-effort notice
(make grace a contract), the org-scale quota economy.

### YARN, Slurm, Mesos, HTCondor

- **YARN** capacity-scheduler preemption needed four interacting dampeners
  (deadzone, natural-termination factor, per-round cap, observe-only mode)
  to stop thrashing, and its notify-the-AM-then-kill protocol went mostly
  unimplemented by applications
  ([YARN-45](https://issues.apache.org/jira/browse/YARN-45)). Lesson: one
  structural damper beats four tunable ones; work-preserving preemption
  that requires app cooperation mostly doesn't happen.
- **[Slurm](https://slurm.schedmd.com/preempt.html)** has the cleanest
  victim contract: `PreemptMode` (CANCEL / REQUEUE / SUSPEND), **GraceTime**
  (guaranteed signal-then-kill window), and **PreemptExemptTime** (minimum
  runtime before a job is preemptible at all — the cheapest thrash guard in
  any system). SUSPEND holds memory hostage and its checkpoint mode died
  with BLCR: requeue is the only victim story that survives contact.
- **Mesos** proves the negative: no scheduler-owned preemption, an
  experimental revocable-resources bolt-on nobody adopted, and Aurora
  rebuilt Borg-style bands + quota on top. Preemption belongs in the one
  scheduler brain.
- **HTCondor**'s four interacting preemption expressions are famously hard
  to reason about; many pools just disable it. Knob count is the enemy.

### Batch/ML platforms (Kueue, Volcano, YuniKorn, and practice)

- **[Kueue](https://kueue.sigs.k8s.io/docs/concepts/preemption/)** preempts
  whole *workloads* (its admission is gang/all-or-nothing, so partial
  eviction just breaks jobs), distinguishes *reclaim* (take back my quota)
  from *priority* preemption, and its three-knob × cohort × fair-sharing
  policy matrix generates a steady stream of "unexpected preemption" issues
  ([#3210](https://github.com/kubernetes-sigs/kueue/issues/3210),
  [#3948](https://github.com/kubernetes-sigs/kueue/issues/3948)). Its
  [#1874](https://github.com/kubernetes-sigs/kueue/issues/1874) — users
  couldn't tell *which* policy preempted them — is direct evidence that a
  bare "Preempted" reason is not enough.
- **[Volcano](https://volcano.sh/blog/volcano-1.15.0-release/)** spent years
  evicting per-pod, breaking victim gangs *and* failing to place the
  preemptor; v1.15 moved to gang-granularity victims plus a placement
  **simulation before committing evictions**.
  [#5022](https://github.com/volcano-sh/volcano/issues/5022) — evictions
  applied even when the preemption then failed — is the single most
  instructive bug for us: **eviction must be transactional with placement.**
- **[YuniKorn](https://yunikorn.apache.org/docs/design/preemption/)**'s
  invariant "preemption may never push a queue below its guaranteed
  resources" prevents cascades with one rule; it also lets workloads opt
  out.
- **ML practice** ([OpenAI's 7,500-node cluster](https://openai.com/index/scaling-kubernetes-to-7500-nodes/),
  [Ray Train](https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html),
  SkyPilot, every spot-instance guide): the universal contract is
  **checkpoint-and-requeue with a guaranteed notice**. The
  [AWS 2-minute spot notice](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html)
  spawned a whole handler ecosystem because the signal is *uniform and
  machine-readable*; GCP's 30 seconds is widely considered too short to
  build on.

**Take:** transactional evict-with-place; per-cause preemption reasons and
victim↔preemptor links; a machine-readable stop notice; requeue that doesn't
lose victims. **Reject (for v1):** whole-workload/gang victim grain (Legion
has no gang admission — allocations are the natural unit here), fair-share
reclaim, and any policy matrix with more than one preemption semantic.

## Jobs to Be Done

**On-call responder**

- When production is degrading and the cluster is full of less-important
  work, I want to scale or deploy the critical service without first hunting
  down whose jobs to kill, so I can stop the bleeding in minutes, not after a
  capacity negotiation.
- When an allocation of my service disappeared, I want to see what displaced
  it, when, and on whose behalf, so I can tell an incident from expected
  behavior at a glance.

**Job author / app developer**

- When I run work that matters less than production, I want to say so once in
  the job file, so the platform can borrow my capacity without me being paged
  about it.
- When my work gets displaced, I want it to come back on its own once there
  is room, so a temporary squeeze does not become a silent outage of my
  pipeline.
- When the platform stops my task for any reason, I want a warning and a few
  seconds to flush, checkpoint, or drain connections, so a routine stop never
  corrupts my data.
- When I donate capacity, I want to keep a floor under my service, so being
  helpful cannot mean being wiped out entirely.

**Cluster operator**

- When teams share one cluster, I want important work to win contention by
  policy rather than by timing or by who I stopped manually, so I can run the
  cluster hotter without hand-managing capacity.
- When I am not yet sure I trust displacement, I want to watch what *would*
  have been displaced before turning it on, and to exempt specific pools
  entirely, so I can adopt it with a small blast radius.
- When I drain a node for maintenance, I want application protections to slow
  nothing down past my deadline, so an upgrade can never be held hostage by a
  misconfigured job.

**Security / platform admin**

- When anyone can mark their own work as most important, importance means
  nothing — I want claiming the top level to require a permission I control,
  so priority stays a policy tool instead of an arms race.

**Autonomous agent**

- When I am about to submit work that could displace something, I want a
  dry-run that names the exact victims first, so I can decide (or ask) before
  causing disruption.
- When my task is told to stop, I want the reason and the deadline in a file
  I can parse, not just a signal, so I can react programmatically —
  checkpoint, hand off, or exit clean.

## User Scenarios

**S1 — Urgent capacity on a full cluster.** A cluster runs a fleet of
`priority: 'low'` batch transcodes. An incident forces the on-call to scale
the `priority: 'high'` API job from 4 to 12. Today: 8 placements fail. With
this plan: the scheduler evicts enough transcode allocations (SIGTERM, 30s
grace, `allocation.preempted` events naming the preemptor), places the API
tasks in the same plan, and requeues the transcodes, which resume as
capacity frees up. Nobody edits or stops the batch job by hand.

**S2 — Every stop becomes clean (even with preemption never used).** An
operator drains a node for a kernel upgrade. Today each task is hard-killed
mid-write. After Phase 1b, every task gets its `stop.signal` and
`stop.killAfter` window — drain, rollout replacement, `job stop`, and
reschedule all use the same contract, and a task can flush, checkpoint, or
drain connections.

**S2a — Every stop becomes explicable (shipped).** The same operator, after
the fact, asks why a task disappeared. `legion events list --kind
allocation.removed` answers `drain` rather than leaving them to correlate
timestamps against a maintenance window; the worker's own log says the same.
This is Phase 1a, and it needed none of the grace machinery.

**S3 — Preview before displacing anyone.** CI (or an agent over MCP) runs
`legion job plan api.jsonnet` before submitting and sees exactly which
allocations of which jobs would be evicted, with their bands and grace. The
submit is a decision, not a surprise.

**S4 — A low-band service that can't be wiped out.** A team runs an internal
tool at `priority: 'low'` to donate its capacity, but sets
`disruption: { minAvailable: '50%' }`. Preemption can take up to half the
replicas and no more; the scheduler looks elsewhere for the rest.

## Goals

- A **stop cause** on every platform-initiated termination, recorded with the
  removal and delivered to the worker that ran the task, shared by job stop,
  rollout replacement, drain, reschedule, destroy, node loss, and preemption.
  *(Phase 1a — shipped.)*
- A **graceful-stop contract** on top of it: configurable signal and
  `killAfter` per task, a machine-readable stop notice file, one ladder for
  every cause. *(Phase 1b.)*
- **Job priority as four named bands** (`critical` / `high` / `normal` /
  `low`), driving both eval-queue order and preemption, meaningful the
  moment it is set.
- **Scheduler preemption** that is atomic with placement: victims are chosen
  during planning, committed (removed) in the same epoch-CAS transaction
  that inserts the preemptor's allocations, and requeued via follow-up
  evals in that same transaction. No nomination races by construction.
- **Structural anti-thrash**: preemption strictly across bands (never
  within one), a minimum victim age, minimal victim sets, and a per-task
  disruption floor that binds voluntary disruption.
- **Operator control**: cluster mode (`enforce` / `observe` / `off`) and
  per-pool disable, per the TODO's "eviction toggled at the pool level".
- **Debuggability from day one**: `job plan` preemption preview,
  victim↔preemptor links in events, per-cause event kinds, and a
  preemption counter metric.

## Non-Goals

- **No fair-share or hierarchical queues.** The 80/20 (per the TODO) is
  bands + (later) per-namespace band caps. Kueue's issue history is what
  the full policy matrix costs. Revisit only with real multi-tenant demand.
- **No oversubscription and no node-pressure eviction.** Legion resources
  are reservations; there is no request/limit split, so there is no
  platform-created pressure to relieve. When pluggable node statuses (disk
  pressure etc., TODO) arrive, they must trigger *this plan's* eviction
  pipeline rather than growing a kubelet-style parallel path.
- **No suspend- or checkpoint-based preemption.** Victims are stopped with
  grace and requeued. (Slurm's SUSPEND holds RAM hostage; its checkpoint
  mode is dead; YARN's work-preserving protocol went unimplemented.)
- **No descheduler-style rebalancing.** Voluntary eviction to improve
  packing is a separate risk class; the eviction primitive is designed so a
  future rebalancer can reuse it.
- **No network/device-aware victim selection.** Exactly where Nomad
  accumulated segfaults and TODOs. Victims are selected on the builtin
  resource vector (cpu/cores/memory/disk/allocations + custom amounts).
- **No preemption plugin seam in v1.** `SchedulingPolicy` keeps its current
  ABI; preemption is built-in. This deliberately keeps a seam closed (see
  judgement calls — extensibility is a core goal and this is a conscious
  deferral, with the seam sketched for later).
- **No new admission control / quota.** Borg separates "who may enter"
  (quota) from "who wins" (priority); we build only the second half now.

## Background: What Exists Today

Code recon (verified against `main` at the time of writing):

- **Placement pipeline.** `plan_and_apply_internal`
  (`src/manager/scheduler.rs:593`): candidate nodes → epoch sample for all
  candidates (`:633`, the CAS fence) → capacity snapshot from the read model
  → `build_plan` (`:896`) → `insert_plan_submission` +
  `apply_plan_submission` (`src/storage/store.rs:1676`, `:1726`). Apply
  re-reads epochs in-transaction and re-checks constraints/permit but *not*
  resources — cross-plan resource safety **is** the epoch CAS.
- **Plans can only add.** `PlannedAllocation` (`scheduler.rs:30`) is
  `{node_id, task_config_id}`; `PlanOutcome` (`:58`) is
  `{allocations, deltas}`; the `plan_submission` table has a `placements`
  column and nothing else. There is no way to express a removal inside a
  plan — the structural gap this plan closes.
- **Feasibility and scoring.** Hard filter in `eligible_nodes`
  (`scheduler.rs:1578`) via `can_fit_task_request`; when every node fails,
  `place_task_allocations` (`:1354`) bails. Scoring lives in
  `policy.rs:275` with the pluggable `SchedulingPolicy` trait at
  `policy.rs:187` (bypassed for built-in bin_pack/spread).
- **Priority is eval-only.** `eval.priority` orders the queue
  (`store.rs:8146`, `ORDER BY priority DESC, id ASC`); default 50 from
  manager config; `legion job run --priority N` / `--asap` set it. No
  priority field exists on `JobConfig` or `TaskConfig`
  (`src/structs/job.rs`). `proto/legion.proto` even reserves the RunJob
  field as "future eval-priority support."
- **Allocations have no state machine.** Stopping *is* row deletion
  (`remove_allocations`, `store.rs:7035` and siblings), which bumps epochs
  and the read model; the worker notices via the desired-set diff on its 2s
  heartbeat and `reconcile_allocations` (`src/worker/task_manager.rs:523`)
  stops whatever is no longer desired.
- **Stops are hard kills.** `TaskPlugin::stop`/`destroy`
  (`src/task_plugins/mod.rs:370`) take no grace parameters;
  `stop_allocation_inner` (`task_manager.rs:557`) runs them back to back.
  exec has no SIGTERM ladder; container is asymmetric (Apple `container
  stop` waits; OCI path is `delete --force`). No jobspec field for any of
  this exists.
- **Drain is the eviction prior art.** `node_drain` eval →
  `reschedule_allocations_for_drained_node` (`store.rs:2792`): stateless
  allocations removed immediately (all at once — no rate limit), stateful
  ones held by the `onLost` gate; `drain_evict` is persisted so failover
  regeneration replays intent; `node.drained` event carries evicted/held
  counts. Preemption follows this template (persisted intent, idempotent
  eval, event with counts) — except preemption needs *no* persisted intent
  because its evictions commit atomically with the plan.
- **Reschedule-after-failure machinery.** `on_unhealthy` restart/move
  budgets, delayed `job_reschedule` evals, `regenerate_scheduling_evals`
  (`store.rs:8339`) as the idempotent recovery sweep. Victim requeue rides
  exactly this.
- **Stateful protections.** `stateful.onLost` fencing (worker
  self-terminate + manager hold) governs *lost nodes*; a preemption stop on
  a live node is a clean, manager-driven stop — the same shape as a rollout
  replacement, which stateful tasks already survive. But `local-dir`
  storage pins an instance to its node, so evicting it can only recreate it
  on the same node.
- **Rollout engine owns "blocked on capacity"** (`blocked_since`,
  `capacityBlockTimeout`, `onCapacityTimeout`) — the closest existing
  blocked-work model; preemption gives rollouts a way to *unblock*.
- **Events.** One draft function per kind in `src/storage/events.rs`;
  hot-path emission is in-transaction staged (see
  `docs/event-stream.md` § Performance — earlier designs cost 25–30% on the
  eval bench, so new plan-apply events must use `stage_event_locked`).
- **Pools** are a string on the node consumed by `permit` (default
  `pool=default`) plus an ACL scope. There is no pool config object; the
  per-pool preemption toggle therefore lives in `schedulerSettings`.
- **Known adjacent hole.** The read-model refresh happens just after the
  epoch-bump commit (`internal_docs/TODO.md` § Scheduling). Preemption
  raises the stakes on that microsecond window (freed capacity must not be
  double-planned), so Phase 3 lands after — or together with — that fix.

## Proposed Design

### 1) Graceful stop: the one eviction pipeline

Every platform-initiated termination — `job stop`, rollout replacement,
drain, reschedule, preemption — flows through one worker-side stop ladder,
configured per task:

```jsonnet
{
  name: 'web',
  runtime: 'container',
  // How this task is stopped, whoever initiates it. Both fields optional.
  stop: {
    signal: 'SIGTERM',   // default; the notice signal
    killAfter: '30s',    // default; SIGKILL (hard destroy) after this grace
  },
}
```

- Validation caps `killAfter` at `5m` (an operator ceiling: grace can slow a
  drain, never wedge it — the K8s PDB lesson inverted).
- The worker writes a **stop notice** before signaling: a JSON file in the
  task directory (path exported to the task at start as
  `LEGION_STOP_NOTICE_PATH`):

```json
{
  "cause": "preempted",            // stop | update | drain | reschedule | preempted
  "deadline": "2026-07-28T21:04:05Z",
  "preemptor": { "job": "api", "allocation": "a1b2…" }   // preempted only
}
```

  This is the spot-interruption-notice pattern: one uniform, machine-readable
  signal that tooling (and agents) can build on. The SIGTERM itself remains
  the trigger; the file carries the *why* and the *when*.

- `TaskPlugin::stop` gains a `StopContext { signal, deadline, cause }`
  (plugin ABI change; in-tree plugins updated in the same commit). exec
  implements signal → wait-until-deadline → SIGKILL; container maps to the
  runtime's stop-with-timeout; noop ignores it.
- **Stops order before starts.** `reconcile_allocations` applies removals
  first and delays starting *new* allocations on that node until the
  removals' processes have exited (bounded by the `killAfter` cap). This is
  what makes plan-carried eviction safe physically: the manager's plan is
  atomic logically (see §3), and the worker serializes the handover so the
  node is never running victim + preemptor beyond the grace window.
- Events: `allocation.stopping` (info, with cause and deadline) joins the
  catalog; `allocation.removed` metadata gains the cause.

Phase 1 ships this alone — it fixes today's universal hard-kill and is what
the mesh's "drain-before-SIGTERM" item (TODO) needs anyway.

### 2) Job priority: four named bands

```jsonnet
{
  name: 'api',
  priority: 'high',   // critical | high | normal (default) | low
  tasks: [ … ],
}
```

- **Bands, not integers.** Every mature system either started with bands
  (Borg, Aurora) or grew an implicit band rule (Nomad's ≥10-point delta).
  Bands make the anti-cascade rule *structural*: *preemption is strictly
  cross-band* — an allocation is a candidate victim only if its job's band
  is strictly lower than the preemptor's. `critical` never preempts
  `critical` (Borg's production-band rule), and a preempted `low` job can
  cascade into nothing. Maximum theoretical cascade depth is 2, and only
  downward.
- **One field, both meanings.** The band sets the job's default eval
  priority (`critical`=90, `high`=70, `normal`=50, `low`=30), so priority
  orders the queue *and* governs preemption — the single most-requested fix
  to Nomad's model ([#11434](https://github.com/hashicorp/nomad/issues/11434)).
  `legion job run --priority N` and `--asap` remain as explicit eval-queue
  overrides (they do not change the job's band).
- **Governance: a `bands` selector on `job` ACL rules.** Which bands a caller
  may submit is expressed in the ACL vocabulary that already exists, following
  the `templates` / `action_names` / `pools` selector precedent:

  ```json
  {"resources": ["job"], "actions": ["write"], "namespaces": ["prod"], "bands": ["critical", "high"]}
  ```

  Rejected alternative: gating `critical` behind `operator:write`, which was
  this plan's original proposal. `operator` is documented as "raft, manager
  configs, observability" — so it would make everyone who can submit a
  critical job a raft administrator and vice versa, conflating cluster
  administration with workload importance. It is also all-or-nothing: no way
  to say "team A may use `critical` in their namespace". The selector costs
  more up front and is the same work as Phase 4's per-namespace band caps,
  done once in the right place. See the
  [judgement calls](../calls/preemption-and-eviction-plan_calls.md).

  Three things this must get right, none of which the original one-line
  proposal had to think about:

  - **Normalize the band before enforcing.** Selector semantics are
    documented in `src/structs/acl.rs`: *"a rule with a selector never matches
    a request without the attribute"*. Since absent `priority` means `normal`,
    RunJob must populate the band attribute (defaulted) before `enforce`, or
    band rules silently grant nothing and existing policies silently stop
    matching.
  - **Gate the update and template paths, not just RunJob.** A job edited to a
    higher band after submit, or a template that renders `priority: 'critical'`,
    bypasses a RunJob-only check. The `templates` selector docs are explicit
    that submitting *through* a template is gated separately, so the band
    check has to run post-render.
  - **Test it where a regression will be caught.** `every_manager_rpc_is_classified`
    will not: this is *conditional* enforcement inside an existing endpoint, so
    dropping the check leaves the suite green. Needs an explicit test (a
    non-privileged token submitting `critical` gets PermissionDenied) living
    next to the endpoint table in `tests/integration/acl_endpoints.rs`.

  `high` and below stay open by default: the band only becomes a privilege
  where a policy says so. This subsumes Phase 4's band caps, so Open Question
  5 is resolved.
- Future system jobs (TODO § Job types) slot in at `critical` when they
  exist; the band exists first.

### 3) Scheduler preemption: victims ride the plan

When placement for a task finds no feasible node **and** the job's band is
above `low`'s floor (i.e. some band exists below it) **and** preemption is
enabled for the job's pool, the planner runs a preemption pass instead of
bailing:

**Victim eligibility** — an allocation on a candidate node is eligible only
if all of these hold:

- its job's band is **strictly below** the preemptor's band;
- it is older than `schedulerSettings.preemption.victimExemptAge`
  (default `60s`; Slurm's `PreemptExemptTime`, the cheapest thrash guard);
- it is **not** a stateful allocation whose storage is node-pinned (a
  `local-dir` instance can only be recreated on the same node, so evicting
  it frees nothing and costs a restart);
- evicting it would not take its task below the task's
  `disruption.minAvailable` floor (§4), counting evictions already chosen
  in this plan.

**Victim selection per node** (Nomad's shape, kept simple): sort eligible
allocations by band ascending, then by normalized resource distance to the
outstanding ask (prefer victims sized like the need — never kill a 32 GiB
allocation to free 1 GiB); take greedily until the task fits; then a
reprieve pass drops any victim not actually required.

**Node choice among preemption-feasible nodes**: fewest victims, then lowest
summed victim band weight, then the normal scoring policy as tiebreak. A
placement that needs *no* victims always wins over one that needs any.

**Atomic commit.** `PlanOutcome` gains `evictions: Vec<Allocation>`;
`plan_submission` gains an `evictions` column; `apply_plan_submission_apply`
deletes the victims and inserts the placements **in the same transaction,
fenced by the same epoch CAS** (victim nodes are already in the sampled
candidate set, so any concurrent plan touching them conflicts and retries).
In that same transaction, the store enqueues one `job_reschedule` eval per
victim job config and stages the `allocation.preempted` events. This is the
central correctness claim of the design:

- No nomination race: the freed capacity is *occupied by the preemptor's
  rows* the instant it is freed — other plans see it via the read model and
  the CAS, exactly like any other placement. K8s needed three KEPs to
  approximate this; Legion's store shape gives it in one transaction.
- No Volcano-#5022: evictions cannot be applied without the placement they
  pay for, because they are the same write.
- No lost victims: the requeue evals are in the transaction too, and
  `regenerate_scheduling_evals` already covers the under-count job on
  failover. Victims requeue **unconditionally**, batch included (the
  Nomad #9890 hole is closed by construction — reschedule is driven by the
  job's desired count, not by remembering the eviction).

**The worker side** is §1: the victim's removal and the preemptor's
placement arrive in the same heartbeat diff; the worker stops victims with
grace, then starts the preemptor.

**Cluster and pool control** (manager config; the TODO's pool-level toggle):

```jsonnet
{
  schedulerSettings: {
    preemption: {
      mode: 'enforce',          // enforce | observe | off   (default enforce)
      disabledPools: ['gpu'],   // pools where preemption never runs
      victimExemptAge: '60s',
    },
  },
}
```

`observe` runs the full victim-selection pass, emits
`allocation.preempt_planned` events and the metric, but commits no
evictions (the placement fails as today) — YARN's `observe_only`, for
operators who want evidence before enabling. Default is `enforce`: bands
make preemption workload-opt-in (nothing is a victim unless its author
chose a lower band), so unlike Nomad there is no hidden switch between
"priority set" and "priority means something".

**Dry run.** `legion job plan` (and the plan RPC / MCP tool) reports
would-be preemptions:

```
$ legion job plan api.jsonnet
web: 8 to place
  6 placed without preemption (node-a ×3, node-b ×2, node-c ×1)
  2 placed by preempting (mode: enforce):
    node-d: transcode/worker ×2 (job batch-transcode, priority low) — 30s grace, requeued
```

**When preemption still can't place** (no eligible victims, floors bind,
pool disabled): the error message says *why*, in band terms:

```
error: no placement for task "web": 3 nodes feasible by constraints but full;
preemption found no eligible victims (2 allocations protected by
disruption.minAvailable, 1 below victimExemptAge). Job remains at 6/8.
```

**Submit-time honesty** (the Nomad discoverability fix, inverted): if a job
sets a band above `normal` and preemption is `off` for the cluster or its
pool, submit and `job validate` print a warning that priority will order
the queue but cannot displace running work.

### 4) The disruption floor

```jsonnet
{
  name: 'web',
  count: 6,
  disruption: {
    // Voluntary disruption (preemption today; a future rebalancer/scale-in
    // tomorrow) never takes this task below the floor. Absolute count or
    // percent of count. Default 0: choosing a preemptible band donates the
    // whole task by default.
    minAvailable: '50%',
  },
}
```

Two deliberate properties, both from the research:

- **It binds only voluntary disruption.** Drain keeps its own semantics
  (deadline-bounded, stateful-held); node loss and `job stop` obviously
  ignore it. A floor that can block mandatory maintenance is the K8s PDB
  wedge; a floor that only shrinks preemption's options can never wedge
  anything — the preemptor simply doesn't place, which is the status quo.
- **It is enforced in victim selection, not scored.** Nomad's
  penalty-not-limit approach is documented as violated in practice; a floor
  the scheduler cannot cross is explainable at 3am.

### 5) Observability

- **Events** (`src/storage/events.rs` + `docs/event-stream.md` table):
  - `allocation.preempted` (warn) — per victim: job, task, node, and the
    preemptor's job/allocation (the bidirectional link Kueue had to
    retrofit in [#1874](https://github.com/kubernetes-sigs/kueue/issues/1874)).
  - `allocation.preempt_planned` (info) — observe mode.
  - `allocation.stopping` (info) — §1, all causes.
  - `allocation.placed` metadata gains `preempted: [...]` when the plan
    evicted.
  All hot-path emissions use the in-transaction staging path per the event
  stream's performance rules.
- **Metrics**: `legion_preemptions_total{pool, band}` (victims),
  `legion_preemption_plans_total{outcome}` (placed / observe / no-victims).
- **CLI**: victims are visible in `legion events list --kind
  allocation.preempted`; `legion job status` for a victim job shows the
  recent preemption count. The event journal is bounded (~10k events), so
  this is an observability surface, not an audit log — stated in the docs.

## Proto, Struct, Config, and Persistence Changes

Proto (`proto/legion.proto`, every field documented publicly per repo rule):

- `JobConfig` message += `priority` (string band; absent = `normal`).
- Plan RPC response += repeated `PlannedPreemption { node_id, job_name,
  task_name, count, band, kill_after }`.
- No new RPCs, so `every_manager_rpc_is_classified` needs no new rows. It
  will NOT catch a dropped band check either — that check is conditional
  enforcement inside existing endpoints — so `tests/integration/acl_endpoints.rs` gains
  explicit band cases (non-privileged token + `critical` → PermissionDenied,
  on RunJob *and* on the update and template-render paths).
- `AclRule` += `bands` selector (+ `has_scope_selectors`), and the band
  attribute is populated on the request scope before `enforce`.

Structs and store:

- `src/structs/job.rs`: `JobConfig.priority: Band` (serde default
  `normal`), `TaskConfig.stop: Option<StopConfig>`,
  `TaskConfig.disruption: Option<DisruptionConfig>`; validation (band
  names, `killAfter <= 5m`, `minAvailable` shape).
- `job_config` table += `priority` column (migration; default `normal`).
- `plan_submission` table += `evictions` column (JSON allocation IDs);
  `apply_plan_submission_apply` deletes victims + inserts placements +
  enqueues victim `job_reschedule` evals + stages events, one transaction.
- `PlanOutcome` += `evictions`; planner gains the preemption pass behind
  the config gate.
- Eval priority derivation: band → default eval priority in
  `enqueue`-side helpers (explicit `--priority` still wins).
- Worker: `StopContext` through `TaskPlugin::stop` (ABI break, in-tree
  plugins updated together); stop-notice file; stops-before-starts ordering
  in `reconcile_allocations`; protocol-version note (below).
- Manager config: `schedulerSettings.preemption { mode, disabledPools,
  victimExemptAge }`.
- `configs/jobs/maximal.jsonnet`: `priority` at top level, `stop` and
  `disruption` on a task, each with the standard explanatory comments.

## Implementation Phases

### Phase 1a — Stop causes (SHIPPED)

Shipped. Public doc: [`docs/graceful-stop.md`](../../docs/graceful-stop.md).

- `StopCause` vocabulary (`stop` / `update` / `drain` / `reschedule` /
  `destroy` / `node_lost` / `preempted` / `unknown`) in `src/structs/stop.rs`.
  `preempted` is defined but unused, so workers deployed now already
  understand it when Phase 3 lands.
- `StopContext` + `TaskPlugin::stop_with`, whose default delegates to
  `stop()`. **Deliberately not an ABI break** — out-of-tree plugins compiled
  against the old trait keep working. `StopContext` is `#[non_exhaustive]` so
  1b can add `signal` and `deadline` without touching call sites again.
- `allocation_stop_notice` table, written **in the same transaction as the
  `DELETE`** at all eight removal sites, so a notice can never describe a stop
  that was rolled back. Scheduler-lane owned (every writer proposes on that
  lane); expiry rides those same transactions rather than the heartbeat read
  path, which keeps a write — and in raft mode a proposal — off the hottest
  path in the system.
- `HeartbeatReply.stop_notices` delivers them; the worker mirrors the set
  wholesale and consumes each cause at the stop it describes.
- `cause` in `allocation.removed` event metadata; one worker log line per stop.
- `ForwardableLaneWrite::RemoveAllocationById` carries the cause, `#[serde(default)]`
  so a forward from an older manager still applies.

**Win standalone:** "why did my task go away?" has an answer — in the event
journal and in worker logs — for drain, rollout replacement, `job stop`,
destroy and node loss. Nothing waits on preemption.

**Explicitly not in 1a:** any change to *when* a task dies. Termination is
still immediate.

### Phase 1b — Graceful termination (SHIPPED)

The stop ladder, which 1a deliberately left out. Ordered by what blocks what:

1. **A terminating set on the worker.** `stop_with` signals and records a
   deadline, then returns; a reaper (or the next reconcile tick) does the
   SIGKILL and the teardown. This is the load-bearing piece — see the mutex
   argument at the top of this plan. Teardown must move behind actual process
   exit, which matters most for `volumes.unmount_for_allocation`: the existing
   "release storage after the process is gone, never before" comment in
   `stop_allocation_inner` is currently true only because SIGKILL is
   synchronous. Grace decouples "stop returned" from "process is gone", and
   unmounting under a live task is how a database gets corrupted.
2. **`stop { signal, killAfter }`** on `TaskConfig` (+ validation capping
   `killAfter` at 5m, + `configs/jobs/maximal.jsonnet`). `StopContext` gains
   the two fields.
3. **Runtime ladders**: exec implements signal → wait-to-deadline → SIGKILL;
   container maps to the runtime's stop-with-timeout (the OCI path's
   `delete --force` is the asymmetry to fix); noop ignores it.
4. **The stop-notice file** + `LEGION_STOP_NOTICE_PATH`. Deferred from 1a
   because a notice written immediately before SIGKILL is a file no task can
   read; it only becomes useful once there is a window in which to read it.
5. **`allocation.stopping` event.** Also deferred from 1a on purpose: with no
   grace there is no "stopping" state — a stop is instantaneous, and an event
   announcing an interval that does not exist is noise.
6. **Stops-before-starts** reconcile ordering, which Phase 3 needs so a node
   never runs victim and preemptor concurrently.

**Open question for 1b, from the review that split this phase — resolved by
taking neither option.** Stops-before-starts puts the victim's shutdown on the
preemptor's critical path, so a task that ignores its stop signal delays an
incident-response scale-up by up to `killAfter`. A preemption-specific grace
would have made the ladder cause-dependent, which is exactly the per-path
divergence call 9 exists to prevent; starting the preemptor on *accounted*
rather than *physically free* capacity would let a container runtime briefly
run victim and preemptor concurrently on the same reserved memory. Shipped as
specified — the 5m `killAfter` cap is the backstop, and a job that wants
faster displacement sets a shorter `killAfter` on the work it expects to be
displaced. Documented as a known limitation.

**Version-skew note for 1b** (1a needs none — an old worker simply ignores
`stop_notices`): stops-before-starts is worker-side, so a new manager with an
old worker may start the preemptor while the victim still runs — a transient
oversubscription, not merely a graceless stop. **Shipped tolerating the
overlap** rather than gating `enforce` on a worker protocol version: a version
gate would make preemption silently inert on any fleet with one stale worker,
which is the discoverability failure this plan spends its Nomad section
arguing against. `docs/preemption-and-eviction.md` § Upgrading tells operators
to upgrade workers first, or run `observe` until the fleet is uniform.

### Phase 2 — Priority bands (SHIPPED)
`priority` on JobConfig (+ column, proto, validation), band → eval-priority
mapping, `critical` ACL gate, CLI/UI showing the band (`job list`, `job
status`, Jobs page), submit-time warning when preemption is off, maximal
jsonnet + docs. **Win standalone:** important work dequeues first under
contention, and the vocabulary for preemption exists and is governed.

### Phase 3 — Preemption (SHIPPED)
Victim eligibility + selection + reprieve pass, node choice, `evictions`
through `PlanOutcome`/`plan_submission`/apply (one transaction: deletes,
inserts, requeue evals, events), `preemption { mode, disabledPools,
victimExemptAge }`, `disruption.minAvailable`, `job plan` preview, observe
mode, events + metrics, the no-placement error text, public doc
`docs/preemption-and-eviction.md`, and the read-model/epoch gap fix (TODO §
Scheduling) as a prerequisite or co-landing. **Win:** S1 end to end.

### Phase 4 — Governance and operations polish (PARTIAL)
Preemption history on the UI job page (**shipped**, along with the band badge
on the job header and the `priority` column in `job list`), `legion node drain` gaining optional
rate limiting via the same disruption vocabulary (evaluate then — deliberately
not v1), fair-share revisit note. Each item independently droppable.

Per-namespace band caps are **no longer a Phase 4 item**: the `bands` ACL
selector in Phase 2 delivers them, scoped by namespace and by job name, with
no second policy surface.

## Testing Plan

Unit (Phases 1–3):

- Stop ladder per runtime: signal delivered, deadline honored, SIGKILL
  after `killAfter`, notice file contents per cause; `killAfter` cap
  validation.
- Band parsing/validation, band → eval-priority mapping, `--priority`
  override precedence, `critical` ACL rejection.
- Victim selection: strict cross-band only; `victimExemptAge` exclusion;
  node-pinned stateful exclusion; `minAvailable` floors (absolute and
  percent, counting same-plan evictions); distance ordering (never evict
  the 32 GiB alloc for a 1 GiB ask when a right-sized victim exists);
  reprieve pass drops unneeded victims; zero-victim placements preferred.
- Observe mode: identical selection, zero evictions, events emitted.

Concurrency / property tests (Phase 3, extending the existing suite):

- A preemption plan and a competing plan for the victim's capacity cannot
  both apply (extends `concurrent_plans_cannot_oversubscribe_a_node`).
- Freed-capacity double-count: a plan sampling between a preemption apply
  and the read-model refresh must conflict, not oversubscribe (this is the
  TODO's fencing-window test, promoted to release-blocking here).
- Crash between apply and anything else loses nothing: victim requeue evals
  and events are in the apply transaction; failover regeneration converges
  the victim job.

Integration (`tests/`):

- End to end S1: fill two fake nodes with `low` allocations, submit `high`,
  assert victims stopped with grace (SIGTERM observed, then exit), the
  preemptor runs, victim job reschedules when a node frees, and
  `allocation.preempted` events carry the preemptor link.
- Batch victim requeue: a preempted batch task reschedules and runs to
  completion (the Nomad #9890 regression test).
- Drain + Phase 1: drained node's tasks receive the stop ladder.
- Stateful: slot-mode task with `nfs` storage is preemptible and its
  replacement reattaches elsewhere; with `local-dir` it is never selected.
- `job plan` preview matches what enforce mode then does.

Regression:

- Jobs without `priority`/`stop`/`disruption` behave exactly as today;
  `preemption.mode: 'off'` yields byte-identical plans on the scheduler
  benchmarks.
- Scheduler benches (`docs/testing-and-benchmarks.md`) before/after: the
  preemption pass must cost nothing when it doesn't trigger (it only runs
  after normal feasibility fails), and event staging stays on the
  in-transaction path. Per AGENTS.md intent, scheduler changes ship with
  the before/after numbers.
- `cargo test --test integration acl_endpoints::` after merge (standing rule).

## Rollout and Backward Compatibility

- All three surfaces are additive and default-compatible: absent `priority`
  = `normal`; absent `stop` = SIGTERM/30s (a behavior change from hard-kill
  — deliberate, documented, and strictly gentler); absent `disruption` =
  floor 0.
- Existing job rows migrate with `priority = 'normal'`; no rewrite of
  stored configs.
- **Version skew** (per `docs/upgrades.md`): a new manager with old workers
  degrades cleanly — old workers ignore `StopContext` and hard-kill as
  today (the protocol handshake reports the skew); preemption still
  functions, just gracelessly, so the docs recommend upgrading workers
  before enabling `enforce` on mixed fleets. New workers with an old
  manager receive no stop context and behave as today.
- `preemption.mode` gives the operator story: ship `enforce` as default,
  document `observe` as the cautious-upgrade posture, `off` as the
  kill switch, `disabledPools` for surgical exemption.
- The event journal being bounded means preemption *history* is best-effort;
  the public doc says so and points at metrics for rates.

## Open Questions

1. ~~**Default `killAfter` — 30s or 15s?**~~ **Resolved: 30s**, matching K8s
   muscle memory and safer for flush-on-exit. Preemption latency is bounded by
   it, and a job that wants faster displacement of its own work sets a shorter
   `killAfter` on that work.
2. **Strict priority DESC eval ordering can starve `low` evals** behind a
   sustained stream of higher ones. Today's queue depth makes this
   theoretical. Lean: accept, add priority aging only if a real queue shows
   it.
3. **Should `disruption.minAvailable` also gate future autoscaler scale-in
   and rebalancing?** Lean: yes — it is the "voluntary disruption" budget,
   and saying so now keeps the vocabulary from forking later.
4. ~~**Notice delivery beyond the file+signal**~~ **Resolved: file is enough.**
   Shipped as `LEGION_STOP_NOTICE_PATH` + the signal. Revisit when a real
   consumer (the mesh's drain-before-SIGTERM) exists.
5. ~~**Per-namespace band caps shape** (Phase 4): config map
   (`priority.maxBandByNamespace`) vs ACL-native.~~ **Resolved: ACL-native.**
   A `bands` selector on `job` rules (see § Job priority) covers both the
   `critical` gate and per-namespace caps in one surface, so no config map is
   added. The original lean toward a config map was wrong: it would have
   shipped a second policy vocabulary alongside ACLs, then needed migrating
   onto them the moment anyone asked for per-team rather than per-namespace
   scoping.

6. **Still open. Should the per-pool preemption toggle be ACL-adjacent rather
   than manager config?** `disabledPools` lives in `schedulerSettings`, so a
   pool-scoped operator cannot exempt their own pool without cluster-config
   access — even though `pools` already exists as an ACL selector. Lean:
   config for v1, named here so the asymmetry is a decision rather than an
   oversight.

## Acceptance Criteria

- **Phase 1a (met):** `job stop`, rollout replacement, drain, destroy and node
  loss each record their own cause; the owning node receives it on the next
  heartbeat and logs it; `allocation.removed` metadata carries it; a stop with
  no notice still stops (`unknown`); a plugin implementing only `stop()` still
  compiles and still stops.
- **Phase 1b:** a task with `stop { killAfter: '5s' }` receives SIGTERM and
  exits cleanly on `job stop`, rollout replacement, drain, and reschedule; a
  task ignoring SIGTERM is SIGKILLed at the deadline; the notice file names
  the correct cause; a preempting reconcile starts the new task only after
  victims exit; no change for tasks without `stop`; **and the heartbeat keeps
  its cadence throughout a 30s grace window** (the regression that split the
  phase).
- **Phase 2:** two pending evals dequeue in band order; a token whose policy
  does not allow the `critical` band is rejected at submit with a clear error,
  on RunJob *and* via a template that renders `critical`; `job list` shows
  bands; submitting `high` with preemption off prints the warning.
- **Phase 3:** S1 passes end to end; `job plan` preview equals enforce-mode
  behavior; observe mode evicts nothing and emits `preempt_planned`;
  `minAvailable` floors hold under concurrent plans; benches show no
  regression when preemption never triggers; `docs/preemption-and-eviction.md`
  exists and covers surface, defaults, failure modes, and limitations.
- **Phase 4:** UI shows a victim job's preemption history. (The namespace-cap
  criterion moved to Phase 2 with the `bands` selector: a policy scoped to
  `namespaces: ["team-a"]`, `bands: ["normal"]` cannot submit `high`.)
