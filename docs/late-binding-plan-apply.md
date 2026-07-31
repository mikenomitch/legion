# Late-Binding Plan Apply (Candidate Lists)

Legion applies placement plans with optimistic concurrency: the planner
works against a snapshot, and the apply transaction rejects the plan if
any node it placed on changed in the meantime (see
[scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)). That model is
cheap and correct, but concurrent planners reliably pick the *same*
best-scoring node — a burst of N independent submits used to degenerate
into N serial apply rounds of conflict-and-replan, and an eval that lost
too many races was failed terminally.

Late-binding apply changes what a plan *is*: each placement carries an
ordered **candidate list** instead of a single node, and the final node
choice happens at commit time. The applier binds each placement to the
first candidate that is still free; a race costs a rank instead of a
retry. Losing every candidate — now the rare case — requeues the eval
instead of failing it, so acknowledged work is never silently dropped.

Both halves default on. Nothing changes in job specs; the knobs are
manager-level `schedulerSettings`.

## The model

- **"Free" means "epoch unchanged".** The scheduler samples every
  candidate node's allocation epoch before planning. At apply, a
  candidate whose epoch still matches provably has the same allocation
  set the planner verified fit against — the epoch *is* the fit proof,
  carried forward. The applier never re-derives free capacity from
  competing allocations.
- **What the epoch cannot prove is rechecked.** A worker
  re-registration that *shrinks* a node's totals (or changes
  `reservedResources` / `maxAllocations`) bumps no allocation epoch, so
  the walk re-reads the fresh node row and re-checks that the task still
  fits inside the node's current totals before binding to it.
- **The walk is conservative.** A candidate whose epoch moved is skipped
  even if it would still fit; the placement falls back to the next
  candidate, a near-equivalent by score (see below). Skips also absorb
  cordons/drains, node loss, constraint changes, shrunken node totals,
  and static-port collisions that landed between snapshot and apply —
  each costs a rank where it used to reject the plan.
- **The job-config epoch stays a hard check.** Two plans for the same job
  config still serialize all-or-nothing; candidate lists never weaken
  that.
- **Races self-resolve.** Applies are serialized (locally, and by log
  order under raft), and each walk runs against committed state: racer 1
  binds the shared first choice, racer 2 sees its epoch moved and slides
  to its second candidate, and so on. No coordination, no retry.
- **The walk is deterministic.** All randomness (tie-breaking, tail
  sampling) runs at plan time on the planning manager. Apply is a pure
  function of the submission and committed state, so raft replicas hold
  identical results.
- **Preemptive placements carry a singleton list.** When a placement only
  fits because the planner chose to displace lower-band work
  ([preemption-and-eviction.md](preemption-and-eviction.md)), it is pinned
  to the node whose victims it paid for — there is no fallback, because a
  fallback would land the placement on a node the eviction did not free.
  The plan's evictions are applied in the same transaction as the walk, and
  the walk compares each candidate's epoch against its value *before* those
  evictions, so a preemptive plan never conflicts with itself.
- **Candidate submissions group-commit like any other plan.** Plans
  queued while a commit is in flight batch into one replicated op, each
  keeping its own verdict inside a per-plan savepoint (see
  [scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)); the walk is
  unchanged inside a batch.

## Candidate list construction

Rank 0 is exactly the node the scheduler would have picked before this
feature — uncontended plans pay zero quality tax. The remaining ranks are
sampled score-weighted, without replacement, from the **near-tie band**
(nodes within `band` of the best score, default 1%); if the band is
thinner than the list, remaining slots fill straight down the ranking in
score order. Sampled tails decorrelate concurrent planners, so a short
list (default 8) survives bursts much larger than 8.

Fallback placements land inside the band by construction, so the quality
cost of a race is bounded — and paid only when a race actually happened.

## Coupled placements

Two placements in one plan are **coupled** when independent fallbacks
could invalidate their combined fit math:

- their candidate lists overlap (two fallbacks onto the shared node could
  oversubscribe it, or collide static ports);
- they belong to a task with a `distinct` constraint (the constraint
  couples that task's placements even across disjoint node lists — two
  different nodes can share a zone);
- they are in the same multi-task co-placement group (the group is one
  scheduling unit).

Coupling is transitive (connected components). Placements in a coupled
component keep today's exact, all-or-nothing apply on their chosen nodes.
Singleton components — including every single-task job, and tasks of one
job constrained to disjoint pools — walk independently.

Practical consequence: a count-N service on one pool behaves exactly as
before this feature, while the motivating workloads (many independent
single-task jobs; heterogeneous jobs whose tasks target different pools)
get full fallback behavior.

## The requeue backstop

Independently of candidates: when an eval exhausts its in-handler
conflict-retry budget (`plan_conflict_max_retries`, default 3), it is
released back to the pending queue with a jittered delay instead of being
terminally failed. The requeue emits an `eval.requeued` event, so a
contended pool shows as visibly pending-and-retrying work. "Failed
forever after three lost races" no longer exists.

Terminal failure remains for non-conflict errors. In particular, a pool
with genuinely **no remaining capacity** still fails the eval with a
placement error ("no available nodes …") — conflict means *transient*
contention, not missing capacity. Turning capacity exhaustion into
blocked-and-retried evals is the gang-scheduling plan's territory, not
this feature's.

## Configuration

Manager-level `schedulerSettings` (no job-spec surface):

```jsonnet
{
  schedulerSettings: {
    // Candidates per placement. 0 disables candidate lists entirely and
    // restores single-node plan submissions (today's pre-candidate
    // behavior, and the mixed-version kill switch). Shorthand:
    plan_candidates: 8,

    // Full form when the defaults need overriding:
    // plan_candidates: {
    //   count: 8,    // list length per placement (clamped to 0..=32)
    //   band: 0.01,  // score band the fallback tail is sampled from
    // },

    // Delay before a conflict-exhausted eval is retried (ms; default
    // 1000, clamped to 100..=60000; uniform jitter of up to the base is
    // added so requeued racers do not re-collide in lockstep).
    plan_conflict_requeue_delay_ms: 1000,
  },
}
```

`plan_conflict_max_retries` and the `plan_conflict_backoff_*` settings
keep their existing meaning (see
[scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)); with
candidates on, those retries are rarely spent.

## Observability

- **`legion job plan`** shows the ranked candidate list per placement
  (`candidate_nodes` in the JSON reply; a "Placement candidates" block in
  the table output).
- **Events:** `allocation.placed` metadata carries `candidateRank` per
  placement (0 = first choice) and a per-plan `fallbacks` count;
  `eval.requeued` records each conflict-exhaustion requeue. See
  [event-stream.md](event-stream.md).
- **Metrics** ([metrics.md](metrics.md)):
  - `legion_scheduler_plan_candidate_rank` — histogram of the rank each
    placement bound at;
  - `legion_plan_apply_total{outcome}` — `applied` (all first-choice),
    `fallback` (applied, at least one placement slid down its list), and
    `rejected` are mutually exclusive, so contention absorbed vs. plans
    lost is visible at a glance;
  - `legion_eval_conflict_requeues_total` — requeue-backstop trips.

Reading the signals: sustained `fallback` outcomes mean races are being
absorbed (working as designed); sustained `rejected` on a pool that also
shows deep candidate ranks means the pool is effectively full — add
capacity rather than tuning the scheduler.

## Rollout and version compatibility

Candidate-carrying plans are stored as a new submission format
(format 2) in the replicated `plan_submission` row. Old shapes parse
forever, so upgrading is seamless — but an *older* manager cannot parse
format-2 rows. Under raft, follow the standard same-version upgrade order
([upgrades.md](upgrades.md)). If you must run a mixed-version manager
fleet beyond an upgrade window, set `plan_candidates: 0` until the fleet
is level: it holds format-2 emission off entirely.

`plan_candidates: 0` also restores the previous placement behavior
byte-for-byte (single-node submissions, all-or-nothing apply). The
requeue backstop is independent of the setting and always on.

## Known limitations

- **Physically-full pools.** When only one node fits, candidate lists
  have length 1 and contention behaves as before (plus the requeue
  backstop). When a replan finds *no* capacity at all, the eval still
  fails terminally with a placement error — capacity exhaustion is not a
  conflict.
- **Coupled components don't fall back.** A count-N service on one pool
  gains nothing yet; striping disjoint lists across identical replicas
  and per-component partial commit are deliberate backlog items in the
  design plan.
- **Policy plugins.** Third-party scheduling-policy plugins select a
  single node and expose no ranking, so their placements carry singleton
  lists (exact-CAS semantics). The built-in `bin_pack` and `spread`
  policies build full lists in both Fresh and Reschedule modes.
- **Conservative skips spread bursts.** Under contention, epoch skips
  spread allocations across the near-tie band even where `bin_pack`
  would happily have stacked them; capacity-aware skipping is a named
  backlog item, gated on fallback-rank metrics.
