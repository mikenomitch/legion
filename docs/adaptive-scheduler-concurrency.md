# Adaptive Scheduler Concurrency

The manager measures its own plan-apply conflict rate and decides how many
evals it will **plan** at once. `evalThreads` stays what it is — the size of
the worker pool — and becomes the *ceiling* on planners rather than a number
you have to get right.

> **Default: `observe`.** Out of the box the controller measures, publishes
> the limit it *would* enforce, and touches nothing. Turning it on is one
> line of config, and the reason to do so is written down below — as is the
> reason not to.

- **Read next:** [Scheduler and Eval Loop](scheduler-and-eval-loop.md) for
  the loop this sits in, [Late-Binding Plan Apply](late-binding-plan-apply.md)
  for what a "conflict" is, and
  [Diagnosing Scheduler Performance](diagnosing-scheduler-performance.md) for
  the symptom this answers.

---

## The problem it solves

Legion's plan-apply path is optimistic. A planner works against a snapshot,
submits placements plus a version vector, and the applier rejects the plan if
a node epoch it depended on moved underneath it. A rejected plan costs a full
replan, a backoff sleep, and — after the retry budget — a requeue. Every one
of those is offered load that comes back.

That is the shape of every optimistic-concurrency system, and the literature
on it is unambiguous: throughput rises with the number of concurrent
transactions up to a knee, then *falls*, because restarts are themselves
contention. Where the knee is depends on fleet size, job shape, and arrival
rate, none of which hold still — so a static thread count is right at one
point on that curve and wrong everywhere else.

Two honest notes on the evidence, because this feature was planned against
numbers that have since been corrected:

- The measurement that first motivated it — 4 planners beating 16 on a
  5,000-node cluster at a 73.6% rejection share — was retracted. The load
  generator was capped below what the scheduler could absorb, and the eval
  notifier of that build woke every worker on every change, so added threads
  spent their time discovering an empty queue. With both fixed, throughput
  on an **uncontended** cluster rises with `evalThreads` at every step
  measured (4 → 32). The full account is the "Corrected" note in
  [Diagnosing Scheduler Performance](diagnosing-scheduler-performance.md#symptom-throughput-fell-when-i-scaled-up).
- The rejection shares in that campaign *were* real for that build. The
  [two-pass bind](late-binding-plan-apply.md) that shipped since revalidates a
  moved-epoch candidate instead of rejecting the plan, and the same scenarios
  now place with zero rejections at the arrival rates the rig can produce.

So what is left for a controller to do? Exactly the case the corrected data
does not cover: a workload whose conflicts are **not** absorbed by candidate
lists — coupled multi-task jobs on a tight pool, a burst of identical
replicas, a physically full pool — where every extra planner is a plan that
will be thrown away. The controller's job is to notice that from the
manager's own counters, back off *only then*, and never mistake a full pool
for a crowded one. On a cluster that is not conflicting it must do nothing,
and it is built so that "nothing" is the cheap path.

---

## What it does

Every 500 ms the manager drains the plan outcomes since its last look:

```
progress = applied + fallback        # plans that placed
attempts = progress + rejected
conflict = rejected / attempts
```

**A `fallback` is progress, not conflict.** A plan that slid down its
candidate list is a race that late binding absorbed; throttling on those
would be throttling to stop the feature working. The fallback rate is still
reported — it is early warning that lists are being consumed — it just never
enters the decision.

The conflict rate is smoothed (EWMA, α = 0.3) and compared against two
watermarks:

| Tick | Action |
| --- | --- |
| fewer than 16 attempts | **inert** — nothing moves, nothing acts; an idle cluster is never throttled on one unlucky rejection |
| nothing placed at all | hold — this is the saturation signature, see below |
| conflict above `highWatermark` (0.20) | **halve** the limit, then hold for 1.5 s |
| conflict below `lowWatermark` (0.05) for 2 s | **add one** back |
| in between | hold |

Additive increase, multiplicative decrease — the rule TCP uses, and for the
same reason: over-admission is expensive and self-amplifying, under-admission
costs only latency, so the loop is biased toward backing off. Four things keep
it from hunting, and none is sufficient alone: the 15-point dead band between
the watermarks, the smoothing, the refractory period after each decrease, and
the asymmetry — backing off is immediate, climbing back needs 2 s of calm.
The limit is an integer, so oscillation is bounded to ±1.

A fresh controller — at startup, and again whenever this manager acquires
scheduler-lane leadership — starts **at the ceiling**. It backs off on
evidence, and a cold start has none.

### The goodput guard

Conflict rate is a fast leading indicator. Placements per second is the thing
you actually want. They diverge in exactly the case that matters.

When a pool is physically full, candidate lists collapse to length one by
necessity and every concurrent plan for that pool races for the same slot.
The conflict rate goes to ~100% and **backing off is the wrong response**:
the losers were doomed regardless, so fewer planners does not turn them into
winners — it only makes the one winner per round arrive later.

So every decrease is checked. Three seconds after backing off, the controller
compares goodput to what it was before the cut. If it has not improved by at
least 5%:

1. The decrease is **reverted**.
2. The controller enters `saturated` for 30 s: no further decreases; increases
   still allowed, because the pool may drain. The hold also lifts early if
   goodput climbs meaningfully above where it was.
3. It says so — a warning in the log, a `scheduler.concurrency_saturated`
   event, and the word `SATURATED` in `legion scheduler concurrency`.

That last part is worth more than the actuator. "Add nodes" and "wait" are
opposite responses, no other signal in Legion tells them apart, and the
controller can tell you which one you need even in `observe` mode.

### How the limit is applied

The limit gates **admission to planning**. It is not a pool size: eval
workers, their supervision, and their leadership idling are untouched, and a
new limit takes effect at the next batch boundary with no thread churn.

Eval workers claim [batches](scheduler-and-eval-loop.md#eval-workers) and
handle them one eval at a time, so one permit per worker is exactly one
concurrent planner. A worker asks the gate before each batch:

- **Admitted** — it claims normally and holds the permit across the whole
  batch, including every conflict-retry backoff sleep. A retrying planner
  still counts against the budget; its next attempt is precisely the arrival
  the controller is spacing out. A batch of nothing but stops gives the
  permit straight back.
- **Refused** — it does not block. It claims only eval kinds that never reach
  the planner (today, `job_stop`), runs those, and parks until a permit frees
  up or new work arrives. Blocking would starve those kinds outright under
  sustained throttling.
- **Already holding a batch** — the fused settle-and-claim hands each worker
  its next batch. If that batch plans and the gate refuses, the worker waits
  for a permit rather than run over the limit. The rows are `in_flight` to it
  either way; parking holds them exactly as the queue would have.

The permit is a guard that releases itself when dropped, on every path —
error, requeue, panic. A leaked permit would be a permanently wedged
scheduler, which is why there is no manual release to forget.

The controller runs only on the manager that leads the scheduler lane, since
only that manager plans. A new leader starts fresh at the ceiling.

---

## Configuration

Manager config only. Nothing changes in the job spec.

```nickel
# configs/manager.ncl
{
  # The ceiling. Set it to your core count; the controller decides how much
  # of it is spent on planning at any moment.
  evalThreads = 16,

  schedulerSettings = {
    adaptiveConcurrency = {
      # enforce — measure and actuate.
      # observe  — measure, publish what it WOULD do, actuate nothing (default).
      # off      — do not run the controller at all.
      mode = "observe",

      # Bounds on admitted planners. `max` defaults to evalThreads and can
      # never exceed it; `min` must be at least 1.
      min = 1,
      # max = 16,

      # Trip points on the conflict rate. The gap between them is the dead
      # band that stops the controller hunting; `lowWatermark` must be below
      # `highWatermark`. Defaults shown.
      # highWatermark = 0.20,
      # lowWatermark  = 0.05,
    },
  },
}
```

Every value is validated at startup, and a bad one is a **hard error** rather
than a silent fallback: a mistyped `mode = "enforc"` quietly becoming the
default would leave you believing you had turned on something you had not.

### Choosing a mode

| You want | Set |
| --- | --- |
| Today's behavior, byte for byte | `mode = "off"` |
| To know whether you have a concurrency problem at all | `mode = "observe"` (the default) — watch `legion scheduler concurrency` under your real load |
| The controller acting | `mode = "enforce"` |
| A fixed limit of N, forever | `min = N`, `max = N` |
| A hard cap on everything | `evalThreads` |

**When `enforce` is the right call:** `observe` shows a shadow limit that
sits below the ceiling under load, its state reads `throttled` or `probing`,
and `legion_plan_apply_total{outcome="rejected"}` is a real share of the
total. That is a cluster spending planner time on plans it throws away.

**When it is not:** the shadow limit hugs the ceiling and rejections are
noise. Enforcing there changes nothing, and any nonzero cost is a cost.
`observe` is the default precisely because that is the common case on the
current build.

All of these are config-file changes and require a manager restart, like
every other `schedulerSettings` key.

---

## Reading it

```console
$ legion scheduler concurrency
Admitted planners  4 of 16          (adaptive, THROTTLED for 41s)
Conflict rate      12.4%            (rejected 31 / attempts 250, 500ms window)
Fallback rate      18.0%            (races absorbed by candidate lists)
Goodput            348 placements/sec
State              throttled        (last decrease 8->4, 41s ago, goodput +18%)
Floor / ceiling    1 / 16           (evalThreads = 16)

Non-planning evals (job stops) are not throttled.
```

`--json` / `-j` emits the standard envelope. The same report is the
`legion_scheduler_concurrency` MCP tool and the `GetSchedulerConcurrency`
RPC (`operator:read`).

| State | Meaning | What to do |
| --- | --- | --- |
| `steady` | At the ceiling; nothing is held back. | Nothing. |
| `throttled` | Below the ceiling; conflicts have not subsided. | Nothing — work is draining at a rate the cluster can absorb. |
| `probing` | Below the ceiling, accumulating calm ticks toward an increase. | Nothing. |
| `saturated` | Backing off did **not** improve goodput. | **Add nodes.** These conflicts are capacity-driven; no scheduler setting will help. |

A manager that does not lead the scheduler lane does no planning and says so
rather than guessing:

```console
$ legion scheduler concurrency
This manager does not plan (it does not lead the scheduler lane).
Ask the manager that does — `legion raft status` names it.
```

### Metrics and events

| Series | What it is |
| --- | --- |
| `legion_scheduler_concurrency_limit` | Admitted planners. In `observe`, the shadow limit — the whole point of observe mode. |
| `legion_scheduler_concurrency_in_flight` | Permits held. |
| `legion_scheduler_concurrency_state{state}` | 1 on the current state. `saturated` is the one to alert on. |
| `legion_scheduler_concurrency_conflict_rate` / `_fallback_rate` / `_goodput` | The smoothed inputs. |
| `legion_scheduler_concurrency_adjustments_total{direction}` | `increase`, `decrease`, `revert`. A sustained two-value sawtooth is hunting — report it. |
| `legion_scheduler_admission_skips_total` | Non-planning evals claimed ahead of the queue head while throttled (next section). |

Events: `scheduler.concurrency_adjusted` on each change (never per tick), and
`scheduler.concurrency_saturated` when the guard trips. See
[Metrics](metrics.md#adaptive-scheduler-concurrency) and the
[Event Stream](event-stream.md).

---

## The one behavior change

**Under sustained throttling, a non-planning eval can run ahead of a
higher-priority planning eval.**

The queue is `ORDER BY priority DESC, id ASC`, so equal-priority work is
FIFO. Within planning kinds nothing is reordered: the gate reduces the *rate*
of claims, and each claim still takes the head. A throttled cluster is a
slower cluster, not an unfair one.

Across kinds there is one deviation. A refused worker claims only kinds that
cannot conflict, so it can take a low-priority `job_stop` while a
high-priority `job_submit` waits at the head. This is deliberate: stops touch
no node epoch, cannot conflict, and are frequently the *remedy* for the
congestion — an operator's `legion job stop` during an incident must not
queue behind the storm it is trying to end. The inversion is between classes
only, never within one, and it is counted in
`legion_scheduler_admission_skips_total`.

---

## Limitations, stated plainly

**The constants are fitted, not derived, and the fit is against retracted
data.** `highWatermark = 0.20` sits on the boundary this project's own
diagnosis guide draws between "worth acting on" and "collapse"; that is a
defensible starting point, not a measurement. The campaign that would settle
the watermarks, the dwell, and whether `enforce` beats the best static
`evalThreads` on a contended workload has not been run. Until it has,
`observe` is the honest way to find out what the controller would do on
*your* workload, and it is the default.

**Only `job_stop` is exempt from throttling.** Every other eval kind reaches
the planner on at least one path — `rollout_tick` through the rollout
handler, the four `node_*` kinds through allocation reconciliation — so all
of them hold a permit. Recovery from a lost node therefore proceeds at the
admitted rate under sustained throttling. The classification is an
allowlist, so a kind added later is throttled until proven harmless.

**One global controller.** Conflicts are node-scoped and nodes belong to
pools, so a storm in one pool can throttle planning for every other. Per-pool
controllers are deferred, not denied; the data that would justify them is the
same campaign as above.

**A single large job can throttle everyone.** An all-or-nothing plan over `n`
nodes survives with probability roughly `qⁿ`, so conflict-proneness is
superlinear in replica count and a global controller cannot see which job is
responsible. Throttling helps that job in relative terms and does not make
its plan cheap; disjoint candidate lists per replica are the cause-side fix
([Late-Binding Plan Apply](late-binding-plan-apply.md)).

**Capacity exhaustion still fails evals terminally.** A planner that finds no
eligible node at all fails the eval; the requeue backstop covers conflict
errors only. This feature does not change that.
