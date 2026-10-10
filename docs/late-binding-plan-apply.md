# Late-Binding Plan Apply (Candidate Lists)

When many jobs are submitted at once, concurrent planners tend to pick the
same best-scoring node and then conflict with each other. Late-binding apply
lets those races resolve at commit time instead of through retries. Each
placement carries a ranked list of acceptable nodes, and the manager picks
the final node when it commits the plan.

Legion applies placement plans with optimistic concurrency. The planner
works against a snapshot, and the apply transaction rejects the plan if any
node it placed on changed in the meantime (see
[scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)). That model is
cheap and correct. Without candidate lists, though, a burst of N independent
submits turned into N serial rounds of conflict and replan, and an eval that
lost too many races failed for good.

With late-binding apply, each placement carries an ordered **candidate
list** instead of a single node. The applier binds each placement to the
best candidate that still fits, and it decides what "fits" from committed
state inside the apply transaction. A race costs a rank instead of a retry.
A race that changed nothing material costs nothing. If every candidate is
lost, which is rare and means the pool is out of room, the eval is requeued
instead of failed, so acknowledged work is never silently dropped.

Both halves are on by default. Job specs don't change. The settings are
manager-level `schedulerSettings`.

## The model

- **The bind runs in two passes.** Pass 1 (`clean`) walks the candidates
  in rank order and binds the first whose node allocation epoch is
  unchanged since the plan snapshot. An uncontended placement therefore
  lands on the planner's first choice, the same as with no contention.
  Pass 2 (`revalidated`) revisits, still in rank order, only the candidates
  pass 1 skipped. It binds the first that passes the full in-transaction
  recheck.
- **The epoch is a ranking hint. It doesn't prove fit.** A moved epoch
  means someone else placed something on this node. That doesn't mean the
  placement no longer fits. So the apply transaction rechecks everything
  against committed rows: the fresh node row and its availability,
  constraints and permit, free capacity rebuilt from the node's current
  totals (`reservedResources` and `maxAllocations` included, with this
  plan's own binds debited as it goes), static ports, and the `distinct`
  tracker. A candidate that passes all of that fits, whoever else touched
  the node.
- **Rejection means "does not fit".** A plan is rejected only when no
  candidate survives both passes. The reason names the actual cause ("no
  longer has capacity for task 41", "does not satisfy constraints"), not
  staleness. A rising `rejected` counter signals a capacity or constraint
  problem, not contention.
- **Skips absorb other changes too.** Cordons and drains, node loss,
  constraint changes, shrunken node totals, and static-port collisions
  that happen between snapshot and apply each cost a rank instead of
  rejecting the plan.
- **Ratifications keep the hard epoch check.** A plan carrying a
  `target_instance` ([edge boot](edge-boot.md)) asks the applier to bind
  this sandbox on this node or report who got there first. A moved epoch
  there may mean another plan already owns the sandbox. No capacity
  recheck can rule that out, so pass 2 does not run for it.
- **The job-config epoch stays a hard check.** Two plans for the same job
  config still serialize all-or-nothing. Candidate lists never weaken
  that.
- **Races resolve themselves.** Applies are serialized (locally, and by log
  order under raft), and each walk runs against committed state. Racer 1
  binds the shared first choice. Racer 2 sees its epoch moved and tries its
  other candidates. If none of them is clean either, it comes back to the
  shared node and binds it too, as long as it still has room. This needs
  no coordination and no retry.
- **The walk is deterministic.** All randomness (tie-breaking, tail
  sampling) runs at plan time on the planning manager. Apply is a pure
  function of the submission and committed state, so raft replicas hold
  identical results.
- **Preemptive placements carry a single candidate.** When a placement only
  fits because the planner chose to displace lower-band work
  ([preemption-and-eviction.md](preemption-and-eviction.md)), it is pinned
  to the node whose victims it paid for. It has no fallback, because a
  fallback would land the placement on a node the eviction did not free.
  The plan's evictions are applied in the same transaction as the walk.
  The walk compares each candidate's epoch against its value before those
  evictions, so a preemptive plan never conflicts with itself.
- **Candidate submissions group-commit like any other plan.** Plans
  queued while a commit is in flight batch into one replicated op. Each
  keeps its own verdict inside a per-plan savepoint (see
  [scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)). The walk is
  unchanged inside a batch.

## Candidate list construction

Rank 0 is the node the scheduler would pick without candidate lists, so
uncontended plans lose no placement quality. The remaining ranks are
sampled by score weight, without replacement, from the **near-tie band**
(nodes within `band` of the best score, default 1%). If the band has fewer
nodes than the list needs, the remaining slots are sampled, also without
replacement, from the best 128 nodes below the band and kept in score
order. Sampled tails keep concurrent planners from all choosing the same
fallbacks, so a short list (default 8) survives bursts much larger than 8.

Fallback placements come from inside the band, so the quality cost of a
race is bounded. You pay it only when a race happens.

## Coupled placements

Two placements in one plan are **coupled** when independent fallbacks
could break their combined fit math:

- They belong to a task with a `distinct` constraint. The constraint
  couples that task's placements even across disjoint node lists, because
  two different nodes can share a zone.
- They are in the same multi-task co-placement group (the group is one
  scheduling unit).

Coupling is transitive (connected components). Placements in a coupled
component keep an exact, all-or-nothing apply on their chosen nodes.
Everything else walks independently, including every single-task job and
every replica of a count-N service.

**Overlapping candidate lists don't count as coupling.** Two placements of
one plan may name the same node, and both may land on it. The walk debits
each bind from that node's free pool before considering the next
placement, and re-reads its static ports, so the second placement binds
only if the node still fits it. (Treating overlap as coupling would have
excluded the common case, since replicas of one service in one pool
overlap by construction.)

Within one plan, a task's replicas prefer not to stack on a node the plan
already bound them to, but only when falling back. Rank 0 is whatever the
planner's own capacity ledger chose, so `bin_pack` still packs replicas
together when that is what it decided.

## The requeue backstop

This works independently of candidate lists. When an eval uses up its
in-handler conflict-retry budget (`plan_conflict_max_retries`, default 3),
it goes back to the pending queue with a jittered delay instead of failing.
The requeue emits an `eval.requeued` event, so a contended pool shows up as
pending work that is being retried. An eval no longer fails because it lost
three races.

Non-conflict errors still fail the eval. In particular, a pool with **no
remaining capacity** still fails the eval with a placement error ("no
available nodes …"). A conflict means short-lived contention, and running
out of capacity is a different problem. This feature does not turn
capacity exhaustion into blocked and retried evals.

## Configuration

Manager-level `schedulerSettings` (no job-spec surface):

```nickel
{
  schedulerSettings = {
    # Candidates per placement. 0 disables candidate lists entirely and
    # restores single-node plan submissions (the behavior before candidate
    # lists, and the mixed-version kill switch). Shorthand:
    plan_candidates = 8,

    # Full form when the defaults need overriding:
    # plan_candidates = {
    #   count = 8,    # list length per placement (clamped to 0..=32)
    #   band = 0.01,  # score band the fallback tail is sampled from
    # },

    # Delay before a conflict-exhausted eval is retried (ms; default
    # 1000, clamped to 100..=60000; uniform jitter of up to the base is
    # added so requeued racers do not re-collide in lockstep).
    plan_conflict_requeue_delay_ms = 1000,
  },
}
```

`plan_conflict_max_retries` and the `plan_conflict_backoff_*` settings
keep their existing meaning (see
[scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)). With
candidates on, those retries are rarely spent.

## Observability

- **`legion job plan`** shows the ranked candidate list per placement
  (`candidate_nodes` in the JSON reply, and a "Placement candidates" block
  in the table output).
- **Events:** `allocation.placed` metadata carries `candidateRank` per
  placement (0 = first choice) and a per-plan `fallbacks` count.
  `eval.requeued` records each conflict-exhaustion requeue. See
  [event-stream.md](event-stream.md).
- **Metrics** ([metrics.md](metrics.md)):
  - `legion_scheduler_plan_candidate_rank`: histogram of the rank each
    placement bound at.
  - `legion_scheduler_plan_bind_pass{pass}`: `clean` (the candidate's
    node epoch was untouched) vs `revalidated` (its epoch had moved and
    the in-transaction recheck admitted it anyway). A rising
    `revalidated` share means contention is being absorbed.
  - `legion_plan_apply_total{outcome}`: `applied` (all first-choice),
    `fallback` (applied, with at least one placement further down its
    list), and `rejected`. The outcomes are mutually exclusive, so you can
    see absorbed contention and lost plans at a glance.
  - `legion_eval_conflict_requeues_total`: requeue-backstop trips.

How to read the signals: sustained `fallback` outcomes and a healthy
`revalidated` share mean races are being absorbed as designed. Sustained
`rejected` means placements no longer fit anywhere they were allowed to
go. Add capacity or look at the constraints instead of tuning the
scheduler.

## Rollout and version compatibility

Candidate-carrying plans are stored in a new submission format (format 2)
in the replicated `plan_submission` row. Older formats still parse, so
upgrading needs no migration. An older manager cannot parse format-2 rows,
though. Under raft, follow the standard same-version upgrade order
([upgrades.md](upgrades.md)). If you must run a mixed-version manager fleet
beyond an upgrade window, set `plan_candidates = 0` until every manager is
on the same version. That stops format-2 rows from being written at all.

`plan_candidates = 0` also restores single-node submissions. It does not
turn off the two-pass bind: a single-candidate plan whose node was touched
is still revalidated and bound if it still fits. The requeue backstop
doesn't depend on the setting and is always on.

## Known limitations

- **Full pools.** When only one node fits, candidate lists have length 1,
  so there is nothing to fall back to. A conflict on that node is handled
  like a `plan_candidates = 0` plan: the node is revalidated and bound if it
  still fits. Otherwise the eval retries, and one that uses up its conflict
  retries is requeued (see [the requeue backstop](#the-requeue-backstop)).
  When a replan finds no capacity at all, the eval still fails with a
  placement error. Capacity exhaustion is not a conflict.
- **Coupled components don't fall back.** A task with a `distinct`
  constraint and a multi-task co-placement group still apply
  all-or-nothing on their chosen nodes. Spreading disjoint lists across
  identical replicas and committing part of a component are not
  implemented.
- **Policy plugins.** Third-party scheduling-policy plugins select a
  single node and expose no ranking, so their placements carry a single
  candidate (exact-CAS semantics). The built-in `bin_pack` and `spread`
  policies build full lists in both Fresh and Reschedule modes.
- **`spread` loosens slightly under contention.** Pass 2 can bind a node
  that received an allocation moments earlier, where a walk that skipped
  moved epochs would have gone on to an emptier one. The effect is
  bounded: pass 2 only runs where pass 1 bound nothing, and it walks the
  same score-ordered list. The same mechanism makes `bin_pack` pack
  tighter. Apply does not yet take the policy into account.
- **A fallback discards speculative staging.** A task that opted into
  [edge boot](edge-boot.md)'s `speculativeStart` has its payload sent to
  the planner's first choice while the apply is in flight. If the apply
  falls back to a later candidate, the workload starts on a node that
  staged nothing, and the claim on the first choice expires unratified.
  The result is correct, but the head start is lost. Watch `fallbacks` on
  jobs that use both features.
