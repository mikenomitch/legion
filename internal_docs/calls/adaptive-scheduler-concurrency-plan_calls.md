# Judgement Calls: Adaptive Scheduler Concurrency

Decisions behind
[`plans/adaptive-scheduler-concurrency-plan.md`](../plans/adaptive-scheduler-concurrency-plan.md).
**Entries are ranked by project impact, highest first; the number is the
rank.** Entries were added during implementation (2026-08-04) and revised on
the rebase onto the batched eval loop and the retraction of the plan's
motivating measurement (2026-08-05).

## 1. Only `job_stop` bypasses the gate — the plan's exempt-kind list was wrong for this codebase

**Call.** The set of eval kinds a throttled worker may still claim is an
**allowlist of one**: `job_stop`. Every other kind holds an admission
permit.

```rust
// src/manager/concurrency.rs
pub const NON_PLANNING_EVAL_KINDS: &[&str] = &["job_stop"];
pub fn is_planning_kind(kind: &str) -> bool { !NON_PLANNING_EVAL_KINDS.contains(&kind) }
```

**Alternative.** The plan's §7.2 names four exempt classes: "stops, rollout
ticks, node drops and KV-driven reconciliation … they do not touch node epochs
and therefore cannot conflict." Implementing that list verbatim was the obvious
move, and it makes the non-throttled lane much wider.

That claim does not hold in this tree. `rollout_tick` reaches the planner
through `manager::rollout::handle_rollout_tick` →
`plan_and_apply_with_targets`. All four `node_*` kinds reach it through
`Store::reschedule_allocations_for_{lost,drained,new}_node` →
`reconcile_job_config_allocations` → `plan_and_apply`. Each of those moves node
epochs and each can be rejected. Exempting them would have left most planning
arrivals outside the very budget the controller exists to set.

**Trade-off.** The escape hatch is narrow. Under sustained throttling a
rollout tick and a lost-node reschedule are throttled alongside ordinary
submits, so recovery from a node loss proceeds at the admitted rate rather than
at full parallelism. That is a real cost on the path where latency is most
visible. It is bounded by the floor (never 0) and it is the same slowdown
every other planning eval takes.

**Why.** Correctness of the actuator beats width of the exemption. The plan's
§13 names exactly one integration test for the starvation guarantee — "a
`job stop` issued while planning is pinned at the floor completes promptly" —
and `job_stop` genuinely cannot conflict (`remove_allocs_for_job` +
`stop_instances_for_job`, no epoch touched). It is also the case that matters:
a stop frees capacity and is frequently the remedy for the congestion, so an
operator's `legion job stop` during an incident must not queue behind the
storm it is trying to end.

The classification is an **allowlist rather than a denylist** for the same
reason: a kind added later is throttled until someone proves it cannot
conflict, which fails safe.

## 2. The controller starts at the ceiling, not at 4 — the plan's startup limit rested on a retracted number

**Call.** `ConcurrencyState::new(floor, ceiling)` starts at `ceiling`, in
state `steady`. The gate admits everyone until the controller publishes a
limit.

**Alternative.** The plan's `min(ceiling, 4)` (§7.3), argued from "4 was the
best static value at the one fleet size with a clean comparison" and "starting
at the ceiling means every cold start pays one congestion collapse".

**Trade-off.** If the first burst after a cold start *is* congested, the
controller pays one descent from the ceiling — at `β = 0.5` and a 3-tick
refractory, 16 → 4 in ~3 s — before it is where the plan would have started.

**Why.** The number 4 came from the measurement
[`aws-phase4-findings.md`](../aws-phase4-findings.md) retracts: the rig's
submitter was capped and the notifier woke every worker, so "4 beat 16" was
the load generator's rate divided among more idle claimers. With both fixed,
throughput on an uncontended cluster rises with `evalThreads` at every step
measured. On that cluster — the common one on the current build — starting at
4 would throttle for `(ceiling - 4) × 2 s` on no evidence at all, which is
the exact failure §11.1 calls "the row that can fail quietly and matters
most". A controller that backs off on evidence must start where there is
none to back off from. Performance on the common case won over a few seconds
of descent on the rare one.

## 3. A permit is per worker, per batch, and held across the fused claim-next

**Call.** In the batched eval loop, a worker asks the gate once per batch. An
admitted worker holds one `PlannerPermit` across every eval in the batch —
conflict-retry sleeps included — and releases it after the fused
settle-and-claim. A refused worker claims with `ClaimScope::NonPlanning`. A
worker whose carried-ahead batch plans while the gate refuses **waits** for a
permit before running it.

```rust
// src/manager/daemon.rs — one iteration of eval_worker_loop
let (batch, mut permit, throttled) = if carried_ahead {
    (take(claimed_ahead), wait_for_permit(..).await /* if any of it plans */, false)
} else {
    let admission = gate().admit();
    (claim_eval_batch(store, admission.is_throttled(), limits)?, admission.into_permit(), throttled)
};
if !batch.iter().any(|eval| is_planning_kind(&eval.kind)) { permit = None; }
```

**Alternatives.** (a) A permit per planning eval, acquired inside the batch
loop. Finer, but the limit is meant to count *concurrent planners*, and a
worker handles its batch strictly one eval at a time — so per-eval permits
are per-worker permits with more churn. (b) Hold the permit across
iterations for as long as the worker keeps getting planning batches. Fewer
gate calls, but a lowered limit would then take effect only when load
subsided — the actuator would be inert exactly when it was needed.

**Trade-off.** A worker parked on the gate holds `in_flight` rows another,
admitted worker could have run. Under a limit of 1 with 16 workers that is up
to 15 batches waiting in worker hands rather than in the queue. Throughput is
the same either way — one planner at a time was the point — but ordering
across workers is looser than the queue's, which batching had already made
so.

**Why.** Correctness first: the limit must mean what the diagnostic says it
means, and "N workers may be inside planning work" is the only reading that
survives batching. Re-asking at every batch boundary is what makes a limit
change land within one batch instead of one load cycle. And waiting rather
than running a carried batch over the limit is the only option that keeps
the fused settle-and-claim — the eval loop's whole throughput win — while
still honouring the limit.

## 4. A permit is an RAII guard, not a manual release

**Call.** `PlannerGate::admit()` returns an `Admission`; a granted one carries
a `PlannerPermit` whose `Drop` decrements the in-flight count and wakes the
gate. There is no `release()` to forget.

**Alternative.** An explicit counter with paired increment/decrement, matching
`GaugeVec::add`'s "callers must pair their +1 with a -1" convention in
`telemetry.rs`.

**Trade-off.** `permit = None` reads as a value being discarded when it is
really a slot being handed back, and needs the comment it has.

**Why.** The failure mode is asymmetric to the point of being decisive: a
leaked permit is not a slow scheduler, it is a **permanently wedged** one,
because the budget never comes back. The batch loop has an error path, a
requeue path, a `NotRaftLeader` path, a mid-batch leadership-loss path and a
panic path, and the pairing convention has to hold on all of them forever.
`Drop` holds on all of them by construction, including unwind — which is what
`a_permit_is_released_even_when_its_holder_panics_test` pins.

## 5. Throttled workers narrow their claim; they never block

**Call.** A worker denied a permit claims with `ClaimScope::NonPlanning` —
the same `claim_eval_apply` with a kind filter on the inner `SELECT`, sharing
the dedupe sweep and the queue-depth gauge — and parks on the gate only when
that finds nothing. The fused settle-and-claim carries the same scope.

**Alternative.** Block the worker on the gate until a permit frees. Simpler by
a wide margin: no scope, no kind filter, no queue-ordering deviation to
document.

**Trade-off.** It costs the one behavior change this feature has: a throttled
worker can take a low-priority `job_stop` ahead of a high-priority
`job_submit` at the queue head. It is a cross-class inversion only — within
planning kinds, `ORDER BY priority DESC, id ASC` is untouched — and it is
counted in `legion_scheduler_admission_skips_total`, but it is a documented
deviation from strict priority order.

**Why.** Blocking turns a throttled cluster into a stuck one. Under sustained
throttling with `min = 1`, fifteen of sixteen workers would sit idle holding
no work while `job_stop` evals piled up behind planning evals they may not
run — and the stop is the thing that ends the congestion. Simplicity lost to
liveness, which is the right way round.

## 6. `HIGH_WATERMARK` keeps its value and loses its derivation

**Call.** The trip point is still 0.20, now justified against the boundary
`docs/diagnosing-scheduler-performance.md` draws between "5–20%: worth acting
on" and "over 20%: collapse", and documented — in the module, the public page
and the plan — as a fitted starting value against retracted data.

**Alternative.** Re-fit the watermarks against the current build. The numbers
the plan fitted against (15.9% / 31.9% / 73.6%) were real rejection shares
for a build without the two-pass bind, on a rig whose throughput readings are
retracted; the current build places the same scenarios with zero rejections
at the rates the rig can produce. A watermark fitted to that distribution may
never trip.

**Trade-off.** The controller may be tuned for a distribution that no longer
exists, which would make it a well-tested no-op. `observe` mode is the
mitigation and the reason it is the default; the honest statement is that the
constants are unvalidated on the current build.

**Why.** Re-fitting requires the AWS campaign, which is measurement work out
of scope here — and the rig cannot currently produce a conflict-bound cell to
fit against. Guessing a *new* constant would be strictly worse than keeping
one whose provenance is written down and consistent with the project's own
operator guidance.

## 7. Sustained congestion keeps cutting while a goodput verification is armed

**Call.** The refractory period (3 ticks) and the goodput verification window
(6 ticks) run independently. A second decrease can fire before the first one's
verdict lands, replacing the pending verification. Under a constantly hostile
trace the limit descends 16 → 8 → 4 → 2 → 1 on the refractory clock, and the
guard then reverts the last step and enters `saturated`.

**Alternative.** Suppress further decreases until the armed verification
resolves — apply the remedy once and judge it before applying it again, which
is arguably what §7.4's "notice that its own remedy did nothing" implies.

**Trade-off.** A genuinely saturated cluster spends a few seconds near the
floor before the revert pulls it back, so §11.3's "`limit` does **not** sit at
the floor" holds after the guard resolves, not throughout.

**Why.** The plan's own sizing argument decides it: §7.3 justifies `β = 0.5`
because 16 → 1 in four steps takes ~6 s, and "a gentler 0.7 takes eight steps
and ~12 s, which is slower than the collapse it is fighting". Gating decreases
on the 6-tick verification doubles the descent and reintroduces the lag β was
chosen to avoid. The saturation case still gets its correct terminal state,
a few seconds later.

## 8. A tick below `MIN_ATTEMPTS` is fully inert, but wall-clock timers still run

**Call.** A tick with fewer than 16 attempts updates no average, advances no
dwell, and triggers nothing — but the refractory countdown, the saturation
hold and the verification countdown all still tick down. A verification that
expires having seen **no** measured tick is dropped rather than resolved.

**Alternative.** Make low-volume ticks inert for everything, timers included.
One rule instead of two.

**Trade-off.** Two categories of state advancing at different rates, plus the
`samples == 0` drop as a third rule.

**Why.** Timers are promises about wall-clock time ("no second cut for 1.5 s",
"no decrease for 30 s"). Freezing them on an idle cluster means a burst after
a quiet minute is judged against a refractory period set a minute ago. The
`samples == 0` drop exists because the alternative is worse in the direction
that matters: reverting on silence would restore a limit on no evidence, and
`resolve_pending` is the function that decides whether a cluster is called
saturated.

## 9. The mode lives on the gate, so `off` costs one relaxed load

**Call.** `PlannerGate` carries the mode as an `AtomicU8`. `off` returns
`Admission::Unrestricted` before touching anything else; `observe` counts
in-flight permits but never refuses; only `enforce` runs the CAS loop.

**Alternative.** One code path with the limit set to `usize::MAX` when the
controller is off. Less branching, one fewer mode to reason about.

**Trade-off.** Three behaviors instead of one, and
`legion_scheduler_concurrency_in_flight` reads 0 under `off` — a small
diagnostic gap in the mode nobody is debugging.

**Why.** §14 promises `mode = "off"` restores today's behavior "byte for
byte", and that promise is what makes the feature safe to ship with a
retracted motivation. A `usize::MAX` limit still runs a CAS and still bumps a
shared cache line on the hottest loop in the manager. The `observe` middle
state exists so the shadow limit is comparable to what enforcement would
admit — an in-flight count of 0 would make the shadow number unfalsifiable.

## 10. A throttled worker with no work polls the gate every 100 ms rather than sleeping on it

**Call.** Both waits on the gate — `wait_for_next_eval` for a worker with
nothing, `wait_for_permit` for a worker holding a carried batch — cap their
park at `THROTTLED_GATE_POLL` (100 ms) and add `PlannerGate::released()` to
the wakeup set.

**Alternative.** Sleep purely on the gate's `Notify`, with no cap.

**Trade-off.** Up to `workers × 10` wakeups per second while throttled with a
non-empty queue. Negligible against the loop's other work, but a poll where
an event would be nicer.

**Why.** Two reasons, and the first is a bug rather than a preference.
Without the cap a throttled worker spins: `next_pending_eval_delay()` returns
zero (the head *is* ready, it is just not admissible), the pre-existing code
returned immediately, and the loop re-claimed at full speed. Second,
`Notify::notify_waiters` stores no permit, so a release landing between the
refusal and the registration of the waiter is missed entirely — the poll
bounds that race to 100 ms instead of to `IDLE_WAIT_CAP`.

## 11. `claim_eval_batch` is public so tests drive the real rule

**Call.** "Pick the scope from the admission, claim, and count the skip" is
one public function in `manager::daemon`, called by `eval_worker_loop` and by
`tests/adaptive_scheduler_concurrency.rs`.

**Alternative.** Keep it inline and have the integration test re-implement
it, the way `tests/integration/plan_contention.rs` re-implements the claim loop around
the already-public `handle_claimed_eval`.

**Trade-off.** One more public item on `daemon`, exported for testing rather
than for the API.

**Why.** The first version of the test did re-implement it, and it passed the
ordering assertion while `legion_scheduler_admission_skips_total` stayed at
zero — the copy had no reason to count anything. A test that re-implements
the rule tests its own copy, which is precisely the class of bug the counter
exists to prevent.
