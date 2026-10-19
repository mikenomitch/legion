# Plan: Adaptive Scheduler Concurrency (Congestion Control for the Plan-Apply Path)

> **Judgement calls:** [`calls/adaptive-scheduler-concurrency-plan_calls.md`](../calls/adaptive-scheduler-concurrency-plan_calls.md)

**Status: IN EXECUTION** — Phases 1, 2 and 3 shipped (2026-08-04, rebased
onto the batched eval loop 2026-08-05 and onto the audit-fix/mesh/KV-encryption
main 2026-10-11). The public page is
[`docs/adaptive-scheduler-concurrency.md`](../../docs/adaptive-scheduler-concurrency.md).

**Read §1 with [`aws-phase4-findings.md`](../aws-phase4-findings.md) next
to it.** The throughput numbers in §1 — 4 planners beating 16, 354.6 vs
310.4 alloc/sec — are retracted: the rig's submitter was capped and that
build's eval notifier woke every worker on every change. On an uncontended
cluster throughput now rises with `evalThreads` at every step measured
(`docs/diagnosing-scheduler-performance.md`, "Corrected 2026-08-05"). The
*rejection shares* in §1 were real for that build, and the two-pass bind
has since removed the mechanism behind them. What survives of §1's argument
is the OCC-thrashing theory and the case candidate lists cannot absorb —
coupled replicas on a tight pool, a physically full pool — which is
exactly what the goodput guard (§7.4) exists for. The consequences for
what was built:

- The controller **starts at the ceiling**, not at `min(ceiling, 4)`
  (§7.3, §15). A cold start has no evidence to back off on, and starting
  low would cost `(ceiling - 4) x 2s` of throttling for nothing on the
  common case. Judgement call 2.
- `HIGH_WATERMARK = 0.20` keeps its value but loses its "best measured
  operating point" derivation; it is now justified against the
  diagnosis guide's 5–20%/20%+ bands and marked fitted. Judgement call 6.
- **Phase 4 is further away than it was.** The default flip needs the
  §11 matrix on the current build, and that build has yet to show a
  conflict-bound cell at all.

What exists: the pure controller (`src/manager/concurrency.rs`) with its
rule suite and the §11.1 replay; the `ConflictWindow` wired to both
`legion_plan_apply_total` call sites; the 500 ms tick loop on the
scheduler-lane leader; `GetSchedulerConcurrency` +
`legion scheduler concurrency` + the MCP tool; the `PlannerGate` with
per-worker, per-batch permits; `ClaimScope::NonPlanning` on the batch
claim; and `mode = "enforce"`.

**Not done, and why:**

- **Phase 0 (measurement) was skipped**, and the measurement it would
  have re-fitted against has since been retracted, so every constant is a
  fitted starting value. Nothing downstream of Phase 0 was built.
- **Phase 4 (default `enforce`) was not built.** Default is `observe`.
- **The `pool` label on `legion_plan_apply_total` (§7.7) is deferred**
  with Phase 0; its only consumer is a per-pool controller Phase 5 has
  not justified.
- One deviation from §7.2: only `job_stop` bypasses the gate, because in
  this codebase `rollout_tick` and the `node_*` kinds all reach the
  planner. Judgement call 1.

**Plan-path cost (AGENTS.md before/after rule).** Measured on the
pre-rebase base `8e90745`, `cargo bench --bench scheduler_bench --
--exact "scheduler_speed/bin_pack/nodes_1000_allocs_1000"`, which drives
`plan_and_apply` — the path that gained the two `ConflictWindow` atomic
adds:

| | mean | 95% CI |
| --- | --- | --- |
| base | 341.56 ms | [338.42, 344.93] |
| this change | 343.06 ms | [338.26, 348.63] |

Criterion: **no change in performance detected**, +0.44%
[-1.28%, +2.15%], p = 0.64 — §8.6's "`observe` must be free", measured.
The `enforce`-mode gate is not on this benchmark's path (it sits in the
eval worker's batch loop, which the microbenchmark does not run); its
cost is one relaxed load plus a CAS per batch.

Deliberately out of scope from the outset: per-pool controllers, any
change to `evalThreads` semantics, and any change to the plan-apply
transaction itself. §10 still stands — striped disjoint candidate lists
([late-binding-plan-apply-plan.md §8 Phase 4](late-binding-plan-apply-plan.md))
attack the *cause* of the measured conflict storm, and this plan manages
the symptom.

---

## 1. Problem Statement

Legion's plan-apply path is optimistic (Omega-style): a planner works
against a snapshot, submits placements plus a version vector, and the
applier rejects the plan if any sampled epoch moved
(`src/storage/store.rs:3340` increments
`legion_plan_apply_total{outcome="rejected"}`). A rejected plan costs a
full replan — `plan_with_conflict_retries` (`src/structs/eval.rs:322`)
re-runs the entire `plan_and_apply_with_exclusions`
(`src/manager/scheduler.rs:1189`) after a 25–500 ms backoff, up to
`plan_conflict_max_retries` (default 3, `src/structs/eval.rs:413`), and
then requeues the eval with a further ~1 s jittered delay
(`src/manager/daemon.rs:2314`).

How many planners run at once is a static operator guess:
`evalThreads` (`src/structs/config.rs:1381`), defaulting to
`available_parallelism()` (`src/structs/config.rs:1889`), fixes the
eval-worker pool size once at startup (`src/manager/daemon.rs:426` →
`:1457` → the `for worker_id in 0..max_threads` spawn at `:1575`). The
operator has no way to know the right number, because the right number
depends on fleet size, job shape, and offered load — all of which move.

**Measured on the AWS rig, 2026-08-03/04** (Legion A: 3-voter raft,
`lanes` topology, `web_fleet` scenario,
`benchmarks/aws/configs/legion/manager-a.ncl.tpl`):

| `evalThreads` | Fleet | Applied | Rejected | Rejection rate |
| --- | --- | --- | --- | --- |
| 4 | 1,000 nodes | 2,503 | 472 | **15.9%** |
| 4 | through 5,000 nodes | — | — | **31.9%** |
| 16 | through 5,000 nodes | 2,090 | 5,839 | **73.6%** |

And raising `evalThreads` from 4 to 16 at 1,000 nodes made throughput
**worse** — 354.6 → 310.4 alloc/sec — while control-plane CPU rose from
~29% to ~85%. Two thirds of a box was spent computing plans that were
thrown away. That is the textbook signature of congestion collapse:
past the knee, added offered load reduces goodput.

Three properties of the measurement matter for the design, and one of
them is uncomfortable:

- **Conflict rate tracks concurrency, not fleet size.** Going 1,000 →
  5,000 nodes at fixed concurrency roughly doubled the rate (15.9% →
  31.9%). More nodes do not dilute contention, because `bin_pack` and
  `spread` both pick an *argmax* — every concurrent planner reading the
  same snapshot computes the same best node. The argmax is a contention
  magnet and it does not get less magnetic as the fleet grows.
- **The failure is superlinear in concurrency.** 4× the planners took
  the rate from ~16% to ~74%, not to ~64%. The extra comes from retry
  amplification: every rejection re-enters the planner, so rejections
  are themselves offered load.
- **The rejections were not candidate-starved.** The recorded reject
  reason was *"no candidate for task N could bind: all 1 candidate(s)
  were stale or invalid"* while
  `legion_scheduler_index_candidates{index="fit"}` showed 33–64 feasible
  nodes per task. One candidate, not eight, because `web_fleet`'s jobs
  are `task_count_per_task: 4` and `2`
  (`benchmarks/scheduler/scenarios/web_fleet.json`) on a single pool —
  which makes each job **one coupling component**, and
  `decouple_plan_components` (`src/manager/scheduler.rs:2090`) shrinks
  every non-singleton component's list to its chosen node. Late binding
  is switched off by design on exactly this workload. **§10 is where
  that leads.**

What an operator has to do today: pick `evalThreads` by hand, restart
the manager to change it, and re-pick it every time the fleet or the
workload mix changes. There is no setting that is right at both 1,000
and 5,000 nodes, and the failure mode of guessing high is not "slightly
slower" — it is 74% wasted work and a saturated box.

**The proposal.** Have the manager measure its own conflict rate and
tune how many planners it admits: back off when conflicts climb, probe
back up when they subside. Congestion control (AIMD) applied to
scheduler parallelism, with a goodput guard so a physically full
cluster does not get throttled for conflicts it would have anyway.

---

## 2. Prior Art

### TCP congestion control (AIMD, Van Jacobson 1988)

Maintains a congestion window; **additive increase** (+1 MSS per RTT)
while the network is healthy, **multiplicative decrease** (×0.5) on a
loss signal. AIMD is the rule that converges to fairness and stability
under distributed feedback (Chiu & Jain, 1989), which is why every
congestion controller since is a variant of it.

- **Take:** the AI/MD asymmetry — probe up cheaply, back off hard. The
  reasoning transfers exactly: over-admission is expensive and
  self-amplifying (a rejected plan replans), under-admission costs only
  latency, so the controller must be biased toward backing off.
- **Take:** the "conservative response to an ambiguous signal" instinct.
  Conflicts, like packet loss, have more than one cause; the controller
  must not assume its own actuator is the cure (§7.4).
- **Reject:** loss-based-only signalling. Modern controllers (BBR, Vegas)
  measure *delivery rate* rather than inferring congestion from loss
  alone, because loss has non-congestion causes. Legion's equivalent of
  "loss with a non-congestion cause" is a saturated pool, and it is
  common, not exotic. The design therefore guards every decrease with a
  goodput check (§6.4) — Vegas's instinct, not Reno's.

### Database optimistic concurrency control backoff

OCC systems (and the "thrashing" literature around them — Agrawal,
Carey & Livny, 1987) show that OCC throughput is non-monotonic in
multiprogramming level: it rises to a knee and then *collapses*, because
restart work is itself contention. The standard remedy is an
**admission-control MPL limiter** — cap the number of concurrently
active transactions and queue the rest.

- **Take:** the whole shape. Legion is an OCC system with a
  multiprogramming level (`evalThreads`) that is currently a constant.
  The measured 354.6 → 310.4 alloc/sec is precisely the past-the-knee
  regime this literature describes.
- **Take:** limit *admission*, not the worker pool. The literature's
  limiter gates entry to the critical section; it does not destroy and
  recreate workers.
- **Reject:** per-transaction adaptive backoff as the primary lever.
  Legion already has it (`adaptive_retry_delay`,
  `src/structs/eval.rs:459`) and the measurement shows it is not enough:
  backoff spaces retries but does nothing about the *arrival* rate of
  first attempts.

### Kubernetes scheduler `percentageOfNodesToScore` and `parallelism`

kube-scheduler is single-threaded at the *binding* decision by design;
its `parallelism` (default 16) only fans out the feasibility/scoring
pass over nodes within one pod's scheduling cycle. Racing schedulers are
explicitly unsupported — the documented answer is "do not run competing
schedulers against one pool". `percentageOfNodesToScore` bounds scoring
cost, not conflict.

- **Take:** nothing about the concurrency model. The serial binder is
  the design Legion deliberately does not have.
- **Take, negatively:** it is worth stating that "one planner" is
  always a correct answer to the conflict problem and always a bad
  answer to the throughput problem. Legion's floor of 1 is the K8s
  design, reached only under sustained congestion.
- Legion's `score_sample` / `nodes_considered` settings are already the
  `percentageOfNodesToScore` analogue and are orthogonal to this plan.

### Nomad's scheduler workers

`num_schedulers` (default: number of CPU cores) sets how many scheduler
workers a server runs, and `enabled_schedulers` partitions them by type.
Plans go to a single central **plan applier** which rejects conflicting
node updates; rejected placements are replanned. Nomad also lets you
*dedicate* workers to a scheduler type so batch work cannot starve
service work.

- **Take:** the observation that the count is static and per-server, and
  the reason people re-tune it is exactly the symptom we measured. This
  is the incumbent behavior Legion improves on.
- **Take:** the kind-partitioning idea, in weakened form — Legion should
  never let planning-kind throttling starve `job_stop` / `rollout_tick`
  (§6.5).
- **Reject:** a dedicated static pool per kind. That trades one guessed
  constant for several.

### In-repo prior art: the heartbeat cadence controller

Legion already has an adaptive control loop with all the properties this
one needs. `src/manager/cadence.rs` is **pure functions over a
snapshot** — no store access, no clock reads (`cadence.rs:1-19`) —
driven by a daemon loop that applies the verdict. It steps on a
**quantized ladder** rather than continuously (`CADENCE_LADDER`,
`cadence.rs:32`) so the served value is predictable and memorizable, and
its `advance_ladder` (`cadence.rs:323`) is **asymmetric**: it steps in
the safe direction immediately and requires a
`LADDER_STEP_DOWN_DWELL` of 300 s (`cadence.rs:37`) before stepping in
the risky one. It ships with a read-only diagnostic command,
`legion node cadence` (`src/cmd/node.rs:263`), specifically because a
time-varying control loop is otherwise unexplainable at 3am.

- **Take:** all of it — module shape, purity, quantization, asymmetric
  dwell, and the diagnostic command. This plan deliberately copies the
  cadence controller's architecture rather than inventing a second one.
  The only inversion is *which* direction is safe: for cadence, backing
  off (widening) is safe and narrowing dwells; for concurrency, backing
  off (throttling) is safe and *increasing* dwells.

---

## 3. Jobs To Be Done

**Cluster operator**
- When I stand up a control plane, I want to not have to guess how many
  things it should schedule at once, so that a number I picked on day
  one does not quietly cap my cluster on day ninety.
- When my fleet grows from one thousand machines to five thousand, I
  want placement to keep up without me editing a config and restarting
  managers, so growth is not a maintenance event.
- When I give the control plane a bigger box, I want the extra cores to
  turn into more placements, not into more wasted work.

**On-call responder**
- When placements are slow, I want to know in one command whether the
  scheduler is holding itself back and why, so I do not spend twenty
  minutes proving the cluster is not broken.
- When I think the control plane is doing the wrong thing, I want a
  switch that turns the cleverness off and pins it to what I say, so I
  can stop the bleeding first and understand it afterwards.

**Job author / platform builder (sandbox service)**
- When I fire a burst of work at a cluster with room, I want it all to
  start promptly, so my users are not waiting behind a scheduler
  arguing with itself.
- When the cluster is genuinely full, I want to be told it is full,
  rather than watching things get slower for reasons nobody can name.

**Autonomous agent**
- When I ask what the scheduler is doing, I want a structured answer I
  can act on — is it throttled, at what level, and because of what — so
  I can decide between waiting, resubmitting, and asking for capacity.

*Not in scope:* the security / platform admin persona. This feature
introduces no new tenant-visible surface, no ACL resource, and no
per-namespace behavior. It also has **no job-author-facing surface**:
nothing changes in the jobspec, so `configs/jobs/maximal.ncl` is
untouched.

### User scenarios

1. **The box that got bigger.** An operator moves managers to a 16-core
   instance and leaves `evalThreads` at its default (now 16). Under a
   1,000-node burst the controller measures a 40% conflict rate within
   two seconds, backs off to 4, and holds. Throughput lands at or above
   the hand-tuned 4-thread number; CPU sits near 30% instead of 85%.
   `legion scheduler concurrency` says: *limit 4 of 16, conflict rate
   12%, throttled for 41s*.
2. **The fleet that grew.** The same cluster grows to 5,000 nodes and
   the workload with it. The controller drifts down to 2–3 without
   anyone touching a config or restarting a manager, and drifts back up
   overnight when the submit burst ends.
3. **The pool that is actually full.** A tenant submits 500 allocations
   into a pool with room for 60. Conflicts spike, but goodput does not
   improve when the controller backs off — so the saturation guard
   trips, the limit is restored, and the manager logs and emits
   *"conflicts are not concurrency-caused; the pool is saturated"*
   instead of throttling a cluster that needs nodes, not patience.
4. **Nothing configured, nothing changed.** An operator upgrades and
   sets nothing. In Phase 2 the controller runs in `observe` and
   actuates nothing: identical behavior, plus a metric that tells them
   what it *would* have done.

---

## 4. Goals

- **`evalThreads` becomes a ceiling, not a guess.** Setting it too high
  costs nothing; the controller finds the level. The advice becomes
  "set it to your core count and forget it".
- **No configuration and no restart** is required for the control plane
  to track a changing fleet size or workload mix.
- **Goodput never regresses** versus the best static setting for a given
  scenario by more than a small margin (target: within 10%), and
  improves materially versus a *badly chosen* static setting (the
  16-thread cases above).
- **Saturation is never mistaken for contention.** A full pool does not
  get throttled, and the operator is told which one they have.
- **Explainable.** One command answers "is it throttled, how much, since
  when, and why", the way `legion node cadence` does for heartbeats.
- **Fully overridable and fully disableable**, with `off` restoring
  today's behavior exactly.

## 5. Non-Goals

- **No change to the plan-apply transaction, the epoch semantics, or
  candidate lists.** This plan manages a symptom; it must not touch
  correctness machinery. `concurrent_plans_cannot_oversubscribe_a_node`
  and the epoch-fencing suites must pass unmodified with the controller
  at every limit.
- **No per-pool or per-namespace controllers in v1.** Conflicts are
  node-scoped, so a pool is the natural blast-radius unit and one global
  controller is a real approximation (§7.3). It is deferred, not denied.
- **No dynamic resizing of the eval-worker pool.** Workers are cheap and
  do non-planning work; the actuator gates *admission to planning*, not
  the number of threads (§6.3).
- **No fairness or QoS mechanism.** The controller reduces the rate at
  which planning-kind evals are claimed; it never reorders the queue
  among them (§7.2).
- **No job-spec surface.** Manager `schedulerSettings` only.
- **Not a fix for the cause.** Striped disjoint candidate lists are the
  cause-side fix and are a different plan's Phase 4 (§10).
- **Not a fix for the raft round-trip cost** — the ~6 scheduler-lane
  proposals per eval and the 206 s of `legion_raft_pipeline_wait_seconds`
  measured in the same run are a separate TODO item. Throttling will
  *reduce* that queueing as a side effect, which is a confound the
  validation must control for (§11).

---

## 6. Background — What Exists Today

**Concurrency is set once, at startup.**
`ManagerConfigFull::eval_threads` (`src/structs/config.rs:1381`, default
`available_parallelism()` at `:1889`) flows to
`start_eval_loop(store, full_config.eval_threads)`
(`src/manager/daemon.rs:426`). `start_eval_loop`
(`src/manager/daemon.rs:1457`) clamps it to `>= 1`, emits
`scheduler_started` (`src/storage/events.rs:1325`, meta
`{"evalThreads": n}`), then spawns exactly that many workers
(`:1575`) and supervises them forever, replacing any that exit
(`restart_eval_worker_after_exit`, `:1590`). There is no path that
changes the count afterwards.

**Each worker is a claim-execute loop.** `eval_worker_loop`
(`src/manager/daemon.rs:1623`) checks scheduler-lane raft leadership
(`:1637`, followers idle), calls `store.claim_eval()` (`:1653`), and
runs the handler on the blocking pool (`:1671`) because the handler is
synchronous end-to-end and its conflict-retry backoff can park a thread
for hundreds of ms. An empty queue parks the worker on a shared `Notify`
or until the next delayed eval is due (`wait_for_next_eval`, `:1702`).
So the natural throttle point already exists: the worker decides, once
per iteration, whether to pull work.

**Claiming is priority-then-FIFO, and collapses duplicates.**
`Store::claim_eval` (`src/storage/store.rs:12183`) is a replicated
scheduler-lane op wrapping `claim_eval_apply` (`:12187`), which does an
`UPDATE ... WHERE id = (SELECT id ... ORDER BY priority DESC, id ASC
LIMIT 1)` (`:12200`) — so equal-priority work is strict FIFO by
insertion. It then deletes pending evals the claim genuinely subsumes
(same entity, same kind, already claimable — `:12231`) and updates
`legion_eval_queue_depth` from inside the transaction (`:12253`). There
is **no kind filter** on the claim today; a worker takes whatever is at
the head.

**Conflict handling is per-eval, reactive, and already tuned to its
limit.** `plan_with_conflict_retries` (`src/structs/eval.rs:322`) loops
`plan_and_apply_with_exclusions` (`src/manager/scheduler.rs:1189`) up to
`max_plan_conflict_retries` (`src/structs/eval.rs:413`; default 3,
clamped to 20) with `adaptive_retry_delay` (`:459`; base 25 ms, cap
500 ms, 35 ms jitter, `:426`). On exhaustion the eval is *not* failed —
`handle_claimed_eval` (`src/manager/daemon.rs:2257`) matches the
conflict arm at `:2314` and calls `release_eval_for_retry`
(`src/storage/store.rs:12683`) with
`plan_conflict_requeue_delay` (`src/structs/eval.rs:397`; default 1 s,
jittered), counting `legion_eval_conflict_requeues_total`
(`src/manager/telemetry.rs:713`). All of this reacts *after* a plan has
already been computed and discarded.

**The measurement the controller needs already exists.**
`legion_plan_apply_total{outcome}` (`src/manager/telemetry.rs:709`,
defined at `:846`) is incremented at exactly two places:
`src/storage/store.rs:3340` for `rejected`, and `src/storage/store.rs:3693`
for `applied` (every placement bound rank 0) versus `fallback` (at least
one placement slid down its candidate list). Alongside it,
`legion_scheduler_plan_candidate_rank`
(`src/manager/telemetry.rs:711`, defined at `:854`) records the bound
rank per placement. **`fallback` is a race that was absorbed; `rejected`
is a race that was lost.** That distinction is the controller's whole
input signal, and it is already free.

**Candidate lists, and where they are switched off.**
`plan_candidates` defaults to `{count: 8, band: 0.01}`
(`src/manager/scheduler.rs:60`). `ranked_candidate_list` (`:2959`) puts
the true argmax at rank 0 and fills the tail from the near-tie band,
extending below the band in score order when the band is thin (`:3029`)
— so lists are length 8 whenever 8 nodes are feasible. But
`decouple_plan_components` (`:2090`) shrinks every coupling component of
size > 1 back to singleton lists: **any job with `count > 1` on one pool
gets no fallback at all.** This is why the measured rejects said "all 1
candidate(s) were stale".

**The control-loop pattern to copy.** `src/manager/cadence.rs` (pure
decision functions; `advance_ladder` at `:323`; asymmetric
`LADDER_STEP_DOWN_DWELL` at `:37`), driven from the status checker in
`daemon.rs`, surfaced by `legion node cadence`
(`src/cmd/node.rs:263`) over a dedicated read-only RPC.

**Where settings live.** `scheduler_settings: Option<Value>`
(`src/structs/config.rs:1387`) is free-form JSON read ad hoc by each
consumer (`Eval::max_plan_conflict_retries` at `src/structs/eval.rs:413`
is the pattern). `preemption.mode = enforce | observe | off`
(`configs/manager.ncl`) is the house precedent for a three-state
"evidence before action" switch.

---

## 7. Proposed Design

### 7.1 The signal: conflict rate, conditioned on progress

Per tick, from the same call sites that already move
`legion_plan_apply_total`, the controller reads three deltas:

```
applied_d    — plans that bound every placement at rank 0
fallback_d   — plans that bound, but slid down at least one list
rejected_d   — plans that lost (all candidates stale/invalid)
```

Define, per tick:

```
progress = applied_d + fallback_d           // plans that actually placed
attempts = progress + rejected_d
conflict = rejected_d / attempts            // undefined when attempts == 0
```

`fallback` counts as **progress**, not as conflict. That is the whole
point of late binding: a race that cost a rank is a race that was
absorbed, and throttling to avoid absorbed races would be throttling to
avoid the feature working. Fallbacks *are* still tracked as a secondary
pressure signal for the diagnostic output and for §10's decision.

Smoothing: `ĉ ← α·conflict + (1-α)·ĉ` with `α = 0.3`
(≈ a 3-tick time constant). Ticks with `attempts < MIN_ATTEMPTS` (16)
do not update `ĉ` and never trigger an action — at low volume a single
rejection reads as a 100% conflict rate and a controller that believes
it will throttle an idle cluster.

**Tick period `T = 500 ms.** Fast enough to react inside a burst (the
measured collapse is fully developed in seconds), slow enough that a
tick carries a usable sample at realistic rates (~350 alloc/s ⇒ ~175
attempts/tick).

Where the counts come from: a process-global `ConflictWindow` of
relaxed atomics in `manager::concurrency`, incremented from the two
existing metric call sites (`src/storage/store.rs:3340` and `:3693`).
No new plumbing through the store, and no extra work on the apply path
beyond two atomic adds.

### 7.2 The actuator: an admission gate on planning, not a resized pool

The controller owns one integer, `limit ∈ [floor, ceiling]`:

- **ceiling** = `evalThreads` (the operator's existing knob, now read as
  "the most parallelism I am willing to spend"), overridable by
  `adaptive_concurrency.max`.
- **floor** = 1, overridable by `adaptive_concurrency.min`.

`limit` gates **admission to planning**, via a permit counter checked in
`eval_worker_loop` (`src/manager/daemon.rs:1623`) immediately before
`claim_eval` (`:1653`):

```
loop {
    match gate.try_acquire() {
        Some(permit) => {
            // Full budget: claim anything.
            let eval = store.claim_eval()?;
            if !eval.is_planning_kind() { drop(permit); }   // free it immediately
            handle(eval);                                   // permit held across
                                                            // plan_with_conflict_retries
        }
        None => {
            // Throttled: take only work that cannot conflict.
            match store.claim_eval_non_planning()? {
                Some(eval) => handle(eval),
                None       => park_until(notify | gate_release | delayed_due),
            }
        }
    }
}
```

Three properties this shape buys:

- **The pool is never resized.** Workers, their supervision
  (`:1590`), and raft-leadership idling (`:1637`) are untouched. A
  change of `limit` takes effect on the next claim, with no thread
  churn and nothing to unwind.
- **The permit is held across the whole `plan_with_conflict_retries`
  call**, including its backoff sleeps. A retrying planner still counts
  against the budget: its next attempt is exactly the arrival the
  controller is trying to space out, and admitting a fresh planner into
  the slot a sleeper vacated re-creates the over-admission we are
  removing.
- **Throttled workers do not block, they narrow.**
  `claim_eval_non_planning` is `claim_eval_apply` (`:12187`) with
  `AND type NOT IN (<planning kinds>)` added to the inner `SELECT`.
  Stops, rollout ticks, node drops and KV-driven reconciliation keep
  draining at full parallelism while planning is throttled — they do not
  touch node epochs and therefore cannot conflict. Blocking the worker
  instead would starve those kinds outright under sustained throttling
  (§8.2).

Ordering within planning kinds is **exactly unchanged**: the gate
changes *how many* workers are pulling, never *which row* the
`ORDER BY priority DESC, id ASC` (`:12200`) hands back. Throttling is a
rate reduction, not a reordering.

### 7.3 The rule: AIMD with a dead band and asymmetric dwell

Every tick, with `ĉ` from §7.1:

| Condition | Action |
| --- | --- |
| `attempts < MIN_ATTEMPTS` (16) | hold; do not update `ĉ` |
| `progress == 0` | hold — see §8.4, this is the saturation case |
| `ĉ > HIGH` (0.20) and not in refractory | **MD:** `limit ← max(floor, ⌊limit × 0.5⌋)`; enter refractory for `COOLDOWN` (3 ticks = 1.5 s) |
| `ĉ < LOW` (0.05) for `PROBE_DWELL` (4 consecutive ticks = 2 s) | **AI:** `limit ← min(ceiling, limit + 1)`; reset the dwell counter |
| otherwise (`LOW ≤ ĉ ≤ HIGH`) | hold; reset the dwell counter |

Constants, and why:

- **`HIGH = 0.20`.** The 4-thread/1,000-node point measured 15.9% while
  *delivering the best throughput observed*. So ~16% is inside the
  healthy regime and the trip point must sit above it. 20% is the
  nearest round number that does not throttle the known-good operating
  point.
- **`LOW = 0.05`** gives a 15-point dead band. A dead band, not a single
  threshold, is the primary anti-hunting mechanism: with one threshold
  the controller oscillates across it forever by construction.
- **`β = 0.5`** (classic AIMD). Over the realistic ceiling range this is
  16 → 8 → 4 → 2 → 1: four steps, ~6 s to the floor including
  cooldowns. A gentler 0.7 takes eight steps and ~12 s, which is slower
  than the collapse it is fighting.
- **`COOLDOWN = 3 ticks`** (refractory after a decrease). This is the
  single most important damping term: a decrease takes ~1 tick to show
  up in `plan_apply_total` and ~1 more to work through the EWMA, so
  without it the controller cuts to the floor on the first burst before
  its first cut has been measured. Same instinct as TCP not halving
  twice for losses in one window.
- **`PROBE_DWELL = 4 ticks`** (2 s of calm before probing up). The
  cadence controller's asymmetry (`LADDER_STEP_DOWN_DWELL`,
  `cadence.rs:37`), inverted: here the *safe* direction is down, so
  down is immediate and up must dwell.
- **Integer quantization** caps hunting amplitude at ±1 in the AI
  direction, and makes the value memorizable and loggable — the same
  argument the cadence ladder makes for rungs over a continuous
  function (`cadence.rs:25-31`).

Startup: `limit` begins at `min(ceiling, 4)`, not at `ceiling`. The
measurement says 4 was the best static setting at the fleet size where
we have a clean comparison, AI reaches the ceiling in `(ceiling-4)×2 s`
if the cluster is calm, and starting at the ceiling means every cold
start pays one congestion collapse before the first MD lands.

### 7.4 The guard: goodput, not conflict rate, is the objective

Conflict rate is a fast *leading indicator*. Goodput —
`progress / second` — is the thing we actually want. They diverge in
exactly the case that matters, so every decrease is verified:

- On each MD, record `goodput_before` (the EWMA at decision time) and
  the pre-decrease `limit`.
- After `2 × COOLDOWN` (3 s), compare. If goodput has **not** improved
  by at least `GOODPUT_EPSILON` (5%), the decrease is judged not to have
  helped:
  1. **Revert** `limit` to its pre-decrease value (once — reverts do not
     stack).
  2. Enter **`saturated` state** for `SATURATION_HOLD` (30 s): no
     further MD, AI still permitted. The conflicts are being caused by
     something the actuator cannot fix.
  3. Emit `scheduler.concurrency_saturated` once per entry, and set
     `legion_scheduler_concurrency_state`.
- Leaving `saturated`: the hold expires, or goodput rises above the
  pre-saturation level (the pool drained).

This is what makes the design honest about §8.4. Backing off a genuinely
full cluster is strictly harmful — the work is doomed either way, and
fewer planners means the winner arrives later — so the controller must
be able to notice that its own remedy did nothing and stop applying it.

### 7.5 Where the loop lives

Copy the cadence architecture exactly:

- **`src/manager/concurrency.rs`** — pure functions and plain data. No
  store access, no clock reads, no leadership checks:
  ```rust
  pub struct ConcurrencyState { limit, ewma_conflict, ewma_goodput,
                                refractory_ticks, calm_ticks,
                                pending_probe, mode: State }
  pub struct TickInput { applied, fallback, rejected, elapsed,
                         floor, ceiling, settings }
  pub fn advance(state: &ConcurrencyState, input: &TickInput) -> ConcurrencyState;
  ```
  Every constant above is a `pub const` in this module, and every rule
  is unit-testable without a cluster — the property that makes the
  cadence tests possible.
- **`ConflictWindow`** — the atomic counters, incremented at
  `src/storage/store.rs:3340` and `:3693`.
- **`start_concurrency_controller`** in `src/manager/daemon.rs` — a
  500 ms tick that drains the window, calls `advance`, and publishes
  `limit` into the gate. Scheduler-lane-leader-only, for the same reason
  the eval workers are (`:1637`): a follower does no planning, so its
  window is empty and its state would be noise. On leadership
  acquisition the state resets to the startup value.
- **`PlannerGate`** — an `AtomicUsize` limit plus an `AtomicUsize`
  in-flight count and a `Notify` for release wakeups. `try_acquire` is a
  CAS loop; no async, no `tokio::Semaphore` resize dance.

### 7.6 The surface

**Manager config** (`schedulerSettings`, no jobspec surface):

```nickel
schedulerSettings = {
  # Adaptive scheduler concurrency (docs/adaptive-scheduler-concurrency.md).
  # The manager measures its own plan-apply conflict rate and tunes how
  # many evals it will PLAN at once, between `min` and `max`. Other eval
  # kinds (stops, rollout ticks, node drops) are never throttled.
  adaptiveConcurrency = {
    # enforce — measure and actuate.
    # observe  — measure, export what it WOULD do, actuate nothing.
    # off      — do not run the controller at all (today's behavior).
    mode = "observe",

    # Floor and ceiling on admitted planners. `max` defaults to
    # `evalThreads`; setting evalThreads high is now free.
    min = 1,
    # max = 16,

    # Trip points on the conflict rate, with the dead band between them
    # that stops the controller hunting. Defaults shown.
    # highWatermark = 0.20,   # above this, halve the limit
    # lowWatermark  = 0.05,   # below this for 2s, add one back
  },
}
```

**Read-only diagnostic** — `legion scheduler concurrency`, backed by a
new `GetSchedulerConcurrency` RPC, modelled line for line on
`legion node cadence` (`src/cmd/node.rs:263`) including the
"this manager does not lead the lane, ask the one that does" branch:

```
$ legion scheduler concurrency
Admitted planners  4 of 16          (adaptive, THROTTLED for 41s)
Conflict rate      12.4%            (rejected 31 / attempts 250, 500ms window)
Fallback rate      18.0%            (races absorbed by candidate lists)
Goodput            348 placements/sec
State              throttled        (last decrease 8→4, 41s ago, goodput +18%)
Floor / ceiling    1 / 16           (evalThreads = 16)

Non-planning evals are not throttled.
```

The saturated case says so in the words an operator needs:

```
State              SATURATED       (backing off did not improve goodput;
                                    conflicts are capacity-driven, not
                                    concurrency-driven — add nodes)
```

`--json` / `-j` emits the standard envelope via `output::print_success`
per the agent-readiness rule, and the command goes in `catalog()`
(`src/cmd/capabilities.rs`, `mutates: false`, `read_only_safe: true`)
and as a read-only MCP tool.

**Overrides, in order of bluntness.** `mode = "off"` disables the
controller. `min = max = N` pins the limit to N. `evalThreads` still
caps everything. All three are config-file changes requiring a manager
restart, consistent with every other `schedulerSettings` key — there is
no live-reload path today and this plan does not add one (§13).

### 7.7 Observability

- `legion_scheduler_concurrency_limit` (gauge) — currently admitted
  planners. In `observe` mode this is the *shadow* limit, which is what
  makes Phase 2 useful on its own.
- `legion_scheduler_concurrency_in_flight` (gauge) — permits held.
- `legion_scheduler_concurrency_state` (gauge, labelled
  `state=steady|throttled|probing|saturated`).
- `legion_scheduler_concurrency_adjustments_total{direction}` —
  `increase` | `decrease` | `revert`.
- `legion_scheduler_admission_skips_total` — times a throttled worker
  took a non-planning eval instead of the queue head. This is the
  metric for the one ordering deviation the design introduces (§8.2).
- `legion_plan_apply_total` gains a `pool` label. Required for §8.3 and
  for any future per-pool controller; it is a cardinality increase
  bounded by the number of pools.
- Events: `scheduler.concurrency_adjusted` (rate-limited to state
  transitions, not every tick) and `scheduler.concurrency_saturated`.
- The controller's decision is logged at `info` **only on change**, at
  `debug` per tick — the tracing rule about high-frequency loops.

---

## 8. Failure Modes, Honestly

### 8.1 Oscillation and hunting

The classic failure of any AIMD loop: the limit sawtooths, and if the
period resonates with the workload the cluster spends its life in
transients. Four independent damping terms, none of which is sufficient
alone:

- The **15-point dead band** between `LOW` and `HIGH`. With one
  threshold, oscillation is guaranteed by construction.
- The **EWMA** (α = 0.3) means one unlucky tick cannot trip MD.
- The **refractory period** after each decrease stops a burst being
  read as N independent congestion events.
- The **asymmetric dwell** (immediate down, 2 s of calm before up)
  means the loop spends its uncertainty budget on the safe side.

Residual risk we accept: a *bimodal* workload — alternating bursts of
conflict-prone and conflict-free work with a period near 2–4 s — will
sawtooth between two adjacent limits. The cost is bounded by AI's ±1
quantization, and the sawtooth is visible in
`legion_scheduler_concurrency_adjustments_total`. If the rig shows it,
the fix is a longer `PROBE_DWELL`, not more mechanism. **A hunting test
is an explicit acceptance criterion (§14, Phase 1)**, driven by a
recorded conflict-rate trace through the pure `advance` function.

### 8.2 Queue ordering and starvation

`claim_eval` is `ORDER BY priority DESC, id ASC`
(`src/storage/store.rs:12200`), so equal-priority work is FIFO.

- **Within planning kinds, nothing is reordered.** The gate reduces the
  rate of claims; each claim still takes the head. A throttled cluster
  is a slower cluster, not an unfair one, and the FIFO guarantee that
  keeps a burst from starving its own tail is preserved exactly.
- **Across kinds, there is one real deviation.** A throttled worker
  claiming via `claim_eval_non_planning` will take a *lower-priority*
  `job_stop` ahead of a higher-priority `job_submit` sitting at the
  head. This is deliberate and, we argue, correct: stops, rollout ticks
  and node drops do not touch node epochs, cannot conflict, and are
  frequently the *remedy* for the congestion (a `job_stop` frees
  capacity). Blocking those behind a throttled planner is how a
  throttled cluster becomes a stuck cluster — an operator's `job stop`
  during an incident must not queue behind the storm it is trying to
  end. The deviation is counted
  (`legion_scheduler_admission_skips_total`), documented in the public
  page, and bounded: it is a priority inversion between *classes* of
  work, never within one.
- **Delayed evals are unaffected.** The `ready_at <= :now` predicate and
  `wait_for_next_eval` (`src/manager/daemon.rs:1702`) are untouched; a
  throttled worker that finds no non-planning work parks exactly as
  today, with the gate-release notify added to its wakeup set.
- **A second-order benefit worth naming:** throttling lengthens the
  queue, and a longer queue collapses *more* redundant work at claim
  time — the dedupe sweep at `src/storage/store.rs:12231` deletes
  pending same-entity same-kind evals. Under a burst of repeated
  reconciliation for the same jobs, throttling reduces total work, not
  just concurrent work.
- **Starvation risk we accept and must test:** if planning is pinned at
  the floor for a long time *and* non-planning work is abundant, all
  workers drain the non-planning queue and planning proceeds at exactly
  `floor` concurrency. That is the intended behavior, but it means
  `min` is a real liveness parameter. Floor is 1, never 0, and the
  gate's `try_acquire` is checked before the non-planning path so a
  freed permit is always taken by the next worker to loop.

### 8.3 Big jobs versus small jobs

This is where the design is weakest, and the measurement is squarely
in it.

An all-or-nothing plan over `n` nodes survives with probability ≈ `qⁿ`,
where `q` is the per-node chance the epoch does not move. That is why
`web_fleet`'s `count: 4` jobs conflict so much harder than single-task
jobs would: at the measured 4-thread point, 84.1% applied implies
`q ≈ 0.958` per node; scaling per-node collision ~linearly with 4×
concurrency gives `q' ≈ 0.83` and `q'⁴ ≈ 0.48`, i.e. ~52% rejected
against the 73.6% measured. The model gets the direction and the order
of magnitude; the residual is retry amplification. **Conflict-proneness
is superlinear in replica count**, and a global controller cannot see
that.

Consequences we accept in v1, stated plainly:

- **A single large job can throttle everyone.** One `count: 50` service
  on a contended pool can hold `ĉ` above `HIGH` and pin the whole
  manager at the floor, including single-task sandbox submits that were
  placing fine.
- **Backing off helps the big job more than the small one, in relative
  terms.** Its survival is `qⁿ`, so a given improvement in `q`
  compounds. That is a fair outcome, but it is the opposite of the
  intuition that throttling punishes big work.
- **It does not help the big job *enough*.** No amount of throttling
  makes a count-50 all-or-nothing plan cheap; only striping does
  (§10).
- **The mitigation is deferred, not denied.** Conflicts are node-scoped
  and nodes belong to pools, so a **per-pool controller** is the right
  blast-radius unit: a GPU pool's storm should not throttle the web
  pool. That needs the `pool` label on `legion_plan_apply_total`
  (§7.7), which Phase 2 adds precisely so the data exists to justify
  Phase 5. Shipping it in v1 would mean N controllers, N sets of
  constants, and a cross-pool fairness question, before we have
  evidence that one controller is insufficient.

### 8.4 When the conflicts are capacity, not parallelism

The most important failure mode, and the one a naive AIMD design gets
wrong.

When a pool is physically full — only one node fits, or none does —
candidate lists collapse to length 1 by necessity and every concurrent
plan for that pool races for the same slot. Conflict rate goes to ~100%
and **backing off is exactly the wrong response**: the losers were
doomed regardless, so reducing concurrency does not convert them into
winners; it only makes the one winner per round arrive later. Worse,
throttling here would suppress the *other* pools' work too (§8.3).

Three defenses, in order of precedence:

1. **A tick with `progress == 0` never triggers MD.** If nothing placed,
   there is no evidence that fewer planners would have placed more.
   Ticks like this are the pure-saturation signature.
2. **The goodput guard (§7.4)** catches the mixed case, which is the
   common one: some pools placing, one pool saturated. If a decrease
   does not improve goodput within 3 s, it is reverted and the
   controller enters `saturated` for 30 s.
3. **The operator is told which situation they are in**, in the
   diagnostic output and in an event, because "add nodes" and "wait"
   are very different responses and today's metrics do not distinguish
   them. This directly serves the on-call job to be done, and it is
   arguably worth more than the actuator: even in `observe` mode
   (Phase 2) the manager can say "your conflicts are capacity-driven".

Known gap: capacity exhaustion where the planner finds *no* eligible
node still fails the eval terminally (TODO: "Capacity exhaustion still
fails evals terminally"; the requeue backstop covers only conflict
errors). This plan does not fix that and must not appear to — the
blocked-eval machinery in
[gang-scheduling-plan.md](gang-scheduling-plan.md) owns it.

### 8.5 Confounding with raft lane queueing

The same AWS run measured 206 s of `legion_raft_pipeline_wait_seconds`
on the scheduler lane at 3.07 ms/op (versus 0.09 ms/op on the status
lane), across ~6 replicated proposals per eval. Some of the measured
"conflict-driven" collapse is really lane-queueing collapse, and
throttling planners will reduce lane pressure *whether or not conflicts
were the cause*. So a throughput improvement from this feature is not
by itself evidence that the conflict theory is right. The validation
must control for it (§11) by reporting per-op lane wait alongside
conflict rate, and by running the fixed-limit sweep that separates
"fewer planners" from "adaptively chosen planners".

### 8.6 Smaller ones, enumerated

- **Two managers, one controller.** Only the scheduler-lane leader
  plans, so only it runs the controller. State resets on leadership
  acquisition — a new leader starts at the startup limit and re-derives
  within ~2 s. Acceptable; the alternative (replicating controller
  state) buys 2 s and costs a replicated op per tick.
- **Cold start.** An empty window means `attempts < MIN_ATTEMPTS`, so
  the controller holds at the startup limit until real traffic arrives.
- **Metric double-counting.** `plan_apply_coalescing`
  (`src/storage/store.rs:857`) group-commits plans; the counters are
  per-plan-outcome and stay so. The controller must read outcomes, not
  commits, or a batch of 16 reads as one attempt.
- **`observe` mode must be free.** The shadow limit is computed but the
  gate is never consulted, so the only cost is two atomic adds on the
  apply path and one 500 ms timer.

---

## 9. Proto, Struct, Config, and Persistence Changes

- **Proto:** one new read-only manager RPC,
  `GetSchedulerConcurrency(GetSchedulerConcurrencyRequest) returns
  (GetSchedulerConcurrencyReply)` with fields for limit, floor, ceiling,
  conflict rate, fallback rate, goodput, state, seconds-in-state, mode,
  and `is_scheduler_lane_leader`. Full public-facing comments on every
  field, per the documentation rule. ACL:
  `check_acl(.., AclResource::Operator, AclAction::Read,
  AclScope::any())`, plus an entry in `tests/integration/acl_endpoints.rs`
  (`endpoint_table` + request builder) — the
  `every_manager_rpc_is_classified` test fails the build otherwise.
- **Manager config:** `schedulerSettings.adaptiveConcurrency`
  (`{mode, min, max, highWatermark, lowWatermark}`), read from the
  existing free-form `scheduler_settings: Option<Value>`
  (`src/structs/config.rs:1387`) with the clamped-getter pattern of
  `Eval::max_plan_conflict_retries` (`src/structs/eval.rs:413`).
  Documented in `configs/manager.ncl`.
- **Job config:** none. Nothing goes in `configs/jobs/maximal.ncl`.
- **New module:** `src/manager/concurrency.rs` (pure decision
  functions), mirroring `src/manager/cadence.rs`.
- **Store:** one new read path, `claim_eval_non_planning` — the existing
  `claim_eval_apply` (`src/storage/store.rs:12187`) with a kind filter
  on the inner `SELECT`, sharing the dedupe sweep and the queue-depth
  gauge. Still a replicated scheduler-lane op. No schema change, no
  migration, nothing persisted.
- **Metrics:** §7.7. The `pool` label on `legion_plan_apply_total` is
  the only change to an existing series and is additive.
- **CLI:** `legion scheduler concurrency` (`src/cmd/`), `--json`
  envelope, `catalog()` entry, MCP read-only tool.
- **Docs:** new public page `docs/adaptive-scheduler-concurrency.md`;
  updates to `docs/scheduler-and-eval-loop.md` (the `evalThreads`
  section becomes "ceiling, not target"), `docs/metrics.md`,
  `docs/agent-readiness.md`, `docs/README.md`, and `README.md`'s doc
  list.

---

## 10. Relationship to the Real Fix (Striped Candidate Lists)

**Striping attacks the cause. This plan manages the symptom. Striping
should ship first.**

The measured conflicts are overwhelmingly from *coupled components*:
`web_fleet`'s jobs are `count: 4` and `count: 2` on one pool
(`benchmarks/scheduler/scenarios/web_fleet.json`), so
`decouple_plan_components` (`src/manager/scheduler.rs:2090`) shrinks
their candidate lists to singletons and the plan reverts to today's
all-or-nothing epoch CAS. The reject reason recorded in the run — *"all
1 candidate(s) were stale"* — is that mechanism, verbatim. Late binding
is off on the workload we measured.

**Striped disjoint candidate lists**
([late-binding-plan-apply-plan.md §8, Phase 4](late-binding-plan-apply-plan.md))
give identical replicas of one job *disjoint* lists, which removes the
overlap coupling edge and lets each replica walk independently. On this
workload that converts a `q⁴` survival probability into four
independent `q` survivals with 8-deep fallback each. If it works as
designed, most of the 15.9 / 31.9 / 73.6% collapses on its own — and a
conflict-rate controller tuned against pre-striping numbers would be
tuned against a distribution that no longer exists. The TODO already
ranks striping "the highest-value item of the four", and this plan's
recon agrees with that ranking rather than competing with it.

**Is adaptive concurrency still worth building afterwards? Yes — but
smaller, and only on evidence.** Three arguments survive striping:

1. **Striping has a hard floor that concurrency control does not.**
   Disjoint lists need enough feasible nodes to go around: `count × k`
   candidates for one job, and the whole cluster's concurrent demand
   summed. On a tight or physically-full pool, lists shorten toward 1
   and conflicts return. That is precisely the regime where an
   admission limiter still has something to say — and precisely where
   §8.4's saturation guard has to stop it from making things worse.
2. **The knee exists regardless of conflicts.** The 4 → 16 thread
   throughput *regression* (354.6 → 310.4 alloc/sec, CPU 29% → 85%) is
   partly conflicts and partly the scheduler lane's single
   `pipeline_loop` thread (§8.5). Striping fixes the first and nothing
   about the second. An operator who sets `evalThreads = 64` on a
   64-core box still needs something to stop them.
3. **"Do not make the operator guess" is the durable product claim.**
   Nomad and Kubernetes both hand you a static worker count and wish you
   luck. A control plane that finds its own level is a differentiator
   that outlives whichever conflict mechanism is current.

**Therefore the ordering, explicitly:**

- **Striping first.** It is a different plan's phase, it is already
  ranked highest in TODO.md, and it changes the numbers this plan is
  designed against.
- **Phase 0 of this plan (§12) runs after striping and is the gate.**
  If post-striping conflict rates sit below `HIGH` (20%) across the
  measured matrix, Phases 3–5 do not get built; Phases 1–2 (the pure
  controller and the observe-mode diagnostic) are still worth having,
  because "your conflicts are capacity-driven, add nodes" is valuable
  on its own and costs almost nothing.
- If striping slips, Phase 2 in `observe` mode is still shippable
  immediately and is the cheapest way to keep measuring.

---

## 11. Validation

Tonight's three points are the test set. The design has to predict them,
and the rig has to reproduce them.

**Rig:** `benchmarks/aws`, Legion A (3-voter raft, `lanes`), scenarios
from `benchmarks/scheduler/scenarios/` via
`benchmarks/aws/workloads/translate.py`. `evalThreads` is set in
`benchmarks/aws/configs/legion/manager-a.ncl.tpl:21` (currently 16) and
`manager-b.ncl.tpl:18` (4).

### 11.1 The design must retrodict the measured points

Run the controller in `observe` mode at each measured configuration and
require the **shadow limit** to land where the static sweep says it
should:

| Configuration | Measured rejection | Shadow limit must settle at |
| --- | --- | --- |
| `evalThreads = 4`, 1,000 nodes | 15.9% | **4** (below `HIGH`; do not throttle a good operating point) |
| `evalThreads = 4`, 5,000 nodes | 31.9% | **2** (one MD, then hold) |
| `evalThreads = 16`, 5,000 nodes | 73.6% | **≤ 3** within 10 s of the burst starting |

The first row is the one that can fail quietly and matters most: a
controller that throttles the best-performing measured configuration is
worse than no controller. If the shadow limit drops below 4 there,
`HIGH` is wrong.

### 11.2 Enforce mode must beat the bad static setting and not lose to the good one

- **`evalThreads = 16`, 1,000 nodes, `enforce`:** throughput ≥ 354.6
  alloc/sec (the 4-thread number — i.e. the controller recovers what the
  bad setting lost), control-plane CPU ≤ 45%, steady-state conflict rate
  ≤ 20%.
- **`evalThreads = 16`, 5,000 nodes, `enforce`:** rejection rate down
  from 73.6% to **< 20%**, and alloc/sec within 10% of the best static
  setting found by a fixed-limit sweep over {1, 2, 4, 8, 16}. The sweep
  is mandatory: without it, "adaptive beat a bad guess" is not evidence
  that adaptive beat a *good* guess.
- **`evalThreads = 4`, 1,000 nodes, `enforce`:** within noise of the
  `off` baseline on throughput, latency, and CPU. Nothing regresses
  where nothing was wrong.

### 11.3 The guard must fire on saturation and not otherwise

- **Saturated pool:** demand ≈ 8× capacity in one pool. Required: the
  controller enters `saturated`, `limit` does **not** sit at the floor,
  `scheduler.concurrency_saturated` is emitted, and time-to-place for
  the work that *can* place is no worse than the `off` baseline. This is
  the anti-goal test and it is a hard gate — a design that throttles a
  full cluster is worse than no design.
- **Mixed:** one saturated pool alongside a pool with headroom. The
  headroom pool's throughput must not regress more than 10% versus
  `off`. This is the honest measurement of §8.3's global-controller
  limitation, and its result decides whether Phase 5 (per-pool) gets
  built.

### 11.4 Controls for the confounds

- Report `legion_raft_pipeline_wait_seconds` per op alongside every
  throughput number, so §8.5's lane-queueing contribution is visible
  rather than absorbed into the feature's credit.
- Report `legion_scheduler_plan_candidate_rank` and the fallback rate,
  so the striping interaction (§10) is measurable in the same runs.
- Run every scenario both with and without striping once it lands; the
  delta between those two is the honest measure of what this feature
  adds on top of the real fix.

### 11.5 Stability

Over a 10-minute mixed-load soak: no more than **6**
`legion_scheduler_concurrency_adjustments_total` transitions per minute
in steady state, and no sustained two-value sawtooth. Hunting is a
failure, not a cosmetic issue.

**The feature is "working" when:** §11.1 retrodicts all three points,
§11.2 recovers the lost throughput at `evalThreads = 16` without
regressing the `evalThreads = 4` baseline, §11.3's saturation gate
passes, and §11.5 shows no hunting. Anything less and it ships as
`observe` only.

---

## 12. Implementation Phases

Each phase ships and stands alone.

- **Phase 0 — Re-measure, after striping (the gate).** Add the `pool`
  label to `legion_plan_apply_total`, extend the AWS campaign to record
  conflict rate, fallback rate, candidate-rank histogram, goodput, and
  per-op lane wait per step, and run the fixed-limit sweep over
  `evalThreads ∈ {1, 2, 4, 8, 16}` at 1,000 and 5,000 nodes. Deliverable:
  a table saying what the best static limit *is* at each point and how
  much conflict remains post-striping. **If conflict rates sit below
  `HIGH` across the matrix, stop after Phase 2.** Valuable regardless:
  it is the "which static number should I set" answer operators want
  today, and it is the data every later constant is fitted to.
- **Phase 1 — The controller, as pure functions.** `ConflictWindow` +
  `src/manager/concurrency.rs` + `advance()`, with the full unit suite
  including the recorded-trace hunting test. Wired to nothing. Ships a
  tested, reviewable decision rule, and lets Phase 0's traces be replayed
  through it offline.
- **Phase 2 — Observe mode and the diagnostic.** Run the tick loop on
  the scheduler-lane leader, export the shadow limit and state metrics,
  add `GetSchedulerConcurrency` + `legion scheduler concurrency` +
  the MCP tool + the public doc. Actuates nothing. **Independently
  valuable:** it answers "is my cluster conflict-bound or
  capacity-bound", which no existing command answers, and it is how
  §11.1 gets run.
- **Phase 3 — The gate and enforce mode.** `PlannerGate`,
  `claim_eval_non_planning`, admission in `eval_worker_loop`, the
  goodput guard and saturation state, `mode = "enforce"` available but
  **not** the default. Ships the actual behavior for operators who opt
  in.
- **Phase 4 — Default it on.** Flip the default to `enforce` on the
  evidence from §11, and rewrite the `evalThreads` guidance in
  `docs/scheduler-and-eval-loop.md` from "target" to "ceiling". A
  separate phase because changing a default on the placement path is a
  separate decision from making the behavior available.
- **Phase 5 — Backlog, each separately justified by Phase 0/2 data:**
  per-pool controllers (gated on §11.3's mixed-pool result); feeding the
  fallback rate in as a secondary pressure signal; coupling the limit to
  scheduler-lane pipeline wait as a second congestion signal (§8.5);
  live reload of `adaptiveConcurrency` without a restart.

---

## 13. Testing Plan

- **Unit (Phase 1), on `advance()` — no cluster, per the cadence
  precedent:** MD fires above `HIGH` and only once per refractory
  window; AI fires only after `PROBE_DWELL` consecutive calm ticks;
  the dead band holds; floor and ceiling are never violated; ticks below
  `MIN_ATTEMPTS` are inert; `progress == 0` never decreases; the
  goodput guard reverts exactly once and enters `saturated`;
  `saturated` expires and permits AI meanwhile. Plus the **replay
  tests**: the three measured AWS traces from §11.1 driven through
  `advance()` must produce the limits in that table — the design's
  retrodiction, as a unit test.
- **Unit (Phase 3):** `PlannerGate` acquire/release under concurrent
  callers; limit changes take effect on the next acquire; a permit is
  never leaked on the error, conflict-requeue, or panic paths (the
  leak failure mode is a wedged scheduler, so this gets a dedicated
  test).
- **Store (Phase 3):** `claim_eval_non_planning` skips planning kinds,
  preserves `ORDER BY priority DESC, id ASC` among the rest, and applies
  the same dedupe sweep and queue-depth gauge as `claim_eval`
  (`src/storage/store.rs:12231`, `:12253`).
- **Integration (Phase 3):** a burst against a headroom pool with the
  limit pinned to 1 versus the ceiling — same allocations placed, same
  resource validity, different wall time; a `job stop` issued while
  planning is pinned at the floor completes promptly (the §8.2
  starvation guarantee); a saturated pool does not drive the limit to
  the floor (§11.3, as an integration test as well as a rig test).
- **Regression, mandatory and unmodified:**
  `concurrent_plans_cannot_oversubscribe_a_node`,
  `concurrent_planners_cannot_oversubscribe_under_raft[_lanes]`, and the
  epoch-fencing suites, run at limit 1 and at the ceiling. The
  controller must be invisible to correctness.
- **ACL:** `tests/integration/acl_endpoints.rs` entry for
  `GetSchedulerConcurrency`; `every_manager_rpc_is_classified` stays
  green.
- **Benchmarks:** `benchmarks/micro/scheduler_bench.rs` before/after,
  proving `observe` mode costs nothing measurable on the plan path (the
  AGENTS.md before/after rule), plus the §11 AWS matrix in the Phase 3
  and Phase 4 PRs.

---

## 14. Rollout and Backward Compatibility

- **No wire, schema, on-disk, or job-spec change.** The new RPC is
  additive; the new store read path is a query variant; nothing is
  persisted. A manager running this code against an old peer behaves
  identically — the controller is per-manager and leader-local.
- **Default is `observe` through Phase 3.** Nothing actuates until an
  operator opts in, and Phase 4 flips the default only with §11's
  numbers in the PR. This is the same "evidence before action" posture
  as `preemption.mode = observe` (`configs/manager.ncl`).
- **`mode = "off"` restores today exactly**, byte for byte: no tick
  loop, no gate consultation, `claim_eval` unchanged.
- **Existing `evalThreads` values keep working** and keep meaning
  something: they are now the ceiling. Nobody has to change a config,
  and the upgrade note is "you can stop tuning this".
- **Raft version skew:** none — no replicated op changes. A follower
  runs no controller; a new leader re-derives state within ~2 s of
  acquisition (§8.6).
- **The one behavior change operators must be told about** (in
  `docs/adaptive-scheduler-concurrency.md`, once Phase 3 is enabled):
  under sustained throttling, non-planning evals can be claimed ahead of
  higher-priority planning evals (§8.2). It is counted, it is bounded to
  a cross-class inversion, and the alternative is worse.

---

## 15. Open Questions

- **Should this be built at all before striping lands? (Lean: no, for
  Phases 3–5; yes, for Phases 1–2.)** §10 is the argument. Phase 0 is
  the decision procedure, and it is deliberately structured so that a
  "no" is a cheap and honest outcome.
- **Is conflict rate the right signal, or should the controller
  optimize goodput directly with a hill climb? (Lean: conflict rate with
  a goodput guard, as designed.)** A pure goodput hill-climb needs no
  thresholds and no fitted constants, which is genuinely attractive —
  but it is slow (it must try a level to evaluate it) and it hunts
  badly on noisy load. Conflict rate is a fast leading indicator and
  goodput is the ground truth, so the design uses each for what it is
  good at. Revisit if the constants prove workload-sensitive in §11.
- **Should `fallback` feed in as a secondary pressure signal? (Lean: not
  in v1.)** A rising fallback rate is early warning that lists are being
  consumed and rejections are coming. But it is also just late binding
  doing its job, and throttling on it would throttle the success case.
  Deferred to Phase 5, gated on whether §11's traces show fallback
  leading rejection by enough ticks to be actionable.
- **Startup limit: `min(ceiling, 4)` or the ceiling? (Lean: 4.)** 4 is
  the best-measured static value at the one point with a clean
  comparison, and starting at the ceiling means paying one collapse per
  cold start. Against: 4 is fitted to one scenario at one fleet size,
  and AI takes `(ceiling-4) × 2 s` to recover on a genuinely calm
  cluster. Phase 0's sweep should settle it.
- **Per-pool controllers: Phase 5 or never? (Lean: Phase 5, gated.)**
  §11.3's mixed-pool result decides. If a saturated GPU pool costs the
  web pool more than 10% throughput, one global controller is not good
  enough.
- **Live reload of `adaptiveConcurrency` without a restart? (Lean: no,
  and not this plan's job.)** No `schedulerSettings` key hot-reloads
  today; adding the first one is a separate design with its own
  consistency questions.

---

## 16. Acceptance Criteria

- **Phase 0:** the conflict/goodput/lane-wait matrix and the fixed-limit
  sweep are recorded in `benchmarks/scheduler/README.md`, at 1,000 and
  5,000 nodes, with and without striping; `legion_plan_apply_total`
  carries a `pool` label; the write-up states explicitly whether Phases
  3–5 are justified.
- **Phase 1:** `advance()` passes the full rule suite; the three
  measured AWS traces replay to the limits in §11.1's table; the
  recorded-trace hunting test shows ≤ 6 transitions/minute in steady
  state. No production code path calls it yet.
- **Phase 2:** on a cluster under load, `legion scheduler concurrency`
  reports a shadow limit, conflict rate, fallback rate, goodput, and
  state; `--json` emits the standard envelope; the command is in
  `catalog()` and exposed over MCP as read-only; a saturated pool is
  reported as `SATURATED` and a contended one is not; the plan-path
  microbenchmark shows no measurable cost versus `mode = "off"`.
- **Phase 3:** `enforce` recovers the throughput lost at
  `evalThreads = 16` (§11.2) without regressing the `evalThreads = 4`
  baseline; the saturation gate (§11.3) passes; a `job stop` completes
  promptly while planning is pinned at the floor; every
  oversubscription and epoch-fencing suite passes unmodified at limit 1
  and at the ceiling; `mode = "off"` is proven equivalent to baseline by
  an integration test.
- **Phase 4:** the default flip ships with the full §11 matrix in the
  PR, `docs/scheduler-and-eval-loop.md` describes `evalThreads` as a
  ceiling, and `docs/adaptive-scheduler-concurrency.md` documents the
  cross-class priority inversion (§8.2), the saturation behavior
  (§8.4), and the global-controller limitation (§8.3) as honestly as it
  documents the wins.
