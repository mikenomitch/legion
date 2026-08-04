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
best candidate that still fits, re-deriving "fits" from committed state
inside the apply transaction; a race costs a rank instead of a retry, and
a race that changed nothing material costs nothing at all. Losing every
candidate — now the rare case, and one that means the pool is out of
room — requeues the eval instead of failing it, so acknowledged work is
never silently dropped.

Both halves default on. Nothing changes in job specs; the knobs are
manager-level `schedulerSettings`.

## The model

- **The bind runs in two passes.** Pass 1 (`clean`) walks the candidates
  in rank order and binds the first whose node allocation epoch is
  unchanged since the plan snapshot — so an uncontended placement lands
  on the planner's first choice, exactly as it would with no contention
  at all. Pass 2 (`revalidated`) revisits, still in rank order, only the
  candidates pass 1 skipped, and binds the first that passes the full
  in-transaction recheck.
- **The epoch is a ranking hint, not the fit proof.** A moved epoch means
  *somebody else placed something on this node*, which is not the same as
  *this no longer fits*. Everything the epoch used to stand in for is
  re-derived inside the apply transaction, against committed rows: the
  fresh node row and its availability, constraints and permit, free
  capacity rebuilt from the node's current totals (`reservedResources`
  and `maxAllocations` included, with this plan's own binds debited as
  it goes), static ports, and the `distinct` tracker. A candidate that
  passes all of that fits, whoever else touched the node.
- **Rejection means "does not fit".** A plan is rejected only when no
  candidate survives *both* passes, and the reason names the real cause
  ("no longer has capacity for task 41", "does not satisfy constraints")
  rather than staleness. A rising `rejected` counter is a capacity or
  constraint signal, not a contention signal.
- **Skips still absorb the rest.** Cordons and drains, node loss,
  constraint changes, shrunken node totals, and static-port collisions
  that landed between snapshot and apply each cost a rank rather than
  rejecting the plan.
- **Ratifications keep the hard epoch check.** A plan carrying a
  `target_instance` ([edge boot](edge-boot.md)) asks "bind this sandbox
  on this node, or tell me who beat me to it". A moved epoch there may
  mean another plan already owns the sandbox, which no capacity recheck
  can disprove, so pass 2 does not run for it.
- **The job-config epoch stays a hard check.** Two plans for the same job
  config still serialize all-or-nothing; candidate lists never weaken
  that.
- **Races self-resolve.** Applies are serialized (locally, and by log
  order under raft), and each walk runs against committed state: racer 1
  binds the shared first choice; racer 2 sees its epoch moved, tries its
  other candidates, and — if none of them is clean either — comes back to
  the shared node and binds it too, provided it still has room. No
  coordination, no retry.
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
thinner than the list, the remaining slots are sampled — again without
replacement — from the best 128 nodes below the band, and kept in score
order. Sampled tails decorrelate concurrent planners, so a short list
(default 8) survives bursts much larger than 8.

Fallback placements land inside the band by construction, so the quality
cost of a race is bounded — and paid only when a race actually happened.

## Coupled placements

Two placements in one plan are **coupled** when independent fallbacks
could invalidate their combined fit math:

- they belong to a task with a `distinct` constraint (the constraint
  couples that task's placements even across disjoint node lists — two
  different nodes can share a zone);
- they are in the same multi-task co-placement group (the group is one
  scheduling unit).

Coupling is transitive (connected components). Placements in a coupled
component keep an exact, all-or-nothing apply on their chosen nodes.
Everything else — including every single-task job, and every replica of a
count-N service — walks independently.

**Overlapping candidate lists are not coupling.** Two placements of one
plan may name the same node, and both may land on it: the walk debits
each bind from that node's free pool before considering the next
placement, and re-reads its static ports, so the second placement binds
only if the node still fits it. (This was a coupling edge once. Because
replicas of one service into one pool overlap by construction, it
collapsed every such plan back to a single-node, all-or-nothing apply —
the common case getting none of this feature's benefit.)

Within one plan, a task's replicas prefer *not* to stack on a node the
plan already bound them to — but only when falling back. Rank 0 is
whatever the planner's own capacity ledger chose, so `bin_pack` still
packs replicas together when that is what it decided.

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

```nickel
{
  schedulerSettings = {
    # Candidates per placement. 0 disables candidate lists entirely and
    # restores single-node plan submissions (today's pre-candidate
    # behavior, and the mixed-version kill switch). Shorthand:
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
  - `legion_scheduler_plan_bind_pass{pass}` — `clean` (the candidate's
    node epoch was untouched) vs `revalidated` (its epoch had moved and
    the in-transaction recheck admitted it anyway). A rising
    `revalidated` share is contention being absorbed;
  - `legion_plan_apply_total{outcome}` — `applied` (all first-choice),
    `fallback` (applied, at least one placement slid down its list), and
    `rejected` are mutually exclusive, so contention absorbed vs. plans
    lost is visible at a glance;
  - `legion_eval_conflict_requeues_total` — requeue-backstop trips.

Reading the signals: sustained `fallback` outcomes and a healthy
`revalidated` share mean races are being absorbed (working as designed);
sustained `rejected` means placements no longer fit anywhere they were
allowed to go — add capacity, or look at the constraints, rather than
tuning the scheduler.

## Rollout and version compatibility

Candidate-carrying plans are stored as a new submission format
(format 2) in the replicated `plan_submission` row. Old shapes parse
forever, so upgrading is seamless — but an *older* manager cannot parse
format-2 rows. Under raft, follow the standard same-version upgrade order
([upgrades.md](upgrades.md)). If you must run a mixed-version manager
fleet beyond an upgrade window, set `plan_candidates = 0` until the fleet
is level: it holds format-2 emission off entirely.

`plan_candidates = 0` also restores single-node submissions. It does not
turn the two-pass bind off: a single-candidate plan whose node was
touched is still revalidated and bound if it still fits. The requeue
backstop is independent of the setting and always on.

## Known limitations

- **Physically-full pools.** When only one node fits, candidate lists
  have length 1 and contention behaves as before (plus the requeue
  backstop). When a replan finds *no* capacity at all, the eval still
  fails terminally with a placement error — capacity exhaustion is not a
  conflict.
- **Coupled components don't fall back.** A task with a `distinct`
  constraint, and a multi-task co-placement group, still apply
  all-or-nothing on their chosen nodes. Striping disjoint lists across
  identical replicas and per-component partial commit are deliberate
  backlog items in the design plan.
- **Policy plugins.** Third-party scheduling-policy plugins select a
  single node and expose no ranking, so their placements carry singleton
  lists (exact-CAS semantics). The built-in `bin_pack` and `spread`
  policies build full lists in both Fresh and Reschedule modes.
- **`spread` loosens slightly under contention.** Pass 2 can bind a node
  that received an allocation moments earlier, where the old
  epoch-skipping walk would have moved on to an emptier one. The effect
  is bounded (pass 2 only runs where pass 1 bound nothing, and walks the
  same score-ordered list), and it is the same mechanism that makes
  `bin_pack` pack *better* than it used to. Policy-aware apply is a named
  backlog item, gated on fallback-rank metrics.
- **A fallback discards speculative staging.** A task that opted into
  [edge boot](edge-boot.md)'s `speculativeStart` has its payload
  dispatched to the planner's first choice while the apply is in flight.
  Falling back to a later candidate means the workload starts on a node
  that staged nothing, and the claim on the first choice expires
  unratified. Correct, but the head start is lost — a reason to watch
  `fallbacks` on jobs that use both features.
