# Scheduler and Eval Loop

This document covers the control loop that drives placement in Legion.

## High-Level Model

Legion separates orchestration into two layers:

- Eval queue: decides what kind of reconciliation action to run (job submit, node drop, rollout tick, etc).
- Scheduler: for placement-oriented evals, computes allocation plans and applies them with optimistic concurrency.

## Eval Workers

At manager startup:

- `start_eval_loop` creates `eval_threads` workers (minimum 1).
- Any worker that exits is immediately restarted.
- Startup recovery calls `requeue_in_flight_evals()`.

Each worker loop:

1. Ask the admission gate for a permit
   ([adaptive-scheduler-concurrency.md](adaptive-scheduler-concurrency.md)).
   With the controller `off`, and with today's default `observe`, this never
   refuses.
2. `claim_evals()` atomically marks a **batch** of `pending` evals as
   `in_flight`. A worker the gate refused claims only eval kinds that cannot
   conflict (today, `job_stop`).
3. Executes `Eval::handle(store)` for each one, in claim order, one at a
   time, holding the permit across the whole batch, conflict-retry backoffs
   included.
4. `complete_evals_and_claim_next()` writes every outcome and claims the
   next batch in one operation. Success deletes the eval row, failure sets
   status `failed`, and a transient rejection returns the row to `pending`
   after a delay. The permit is released, and asked for again at the next
   batch.

When the queue is empty, workers block on either:

- `Notify` wake-up (`enqueue_eval` of zero-delay work), or
- timeout until next delayed eval `ready_at`, or
- (when throttled) the admission gate releasing a permit.

### `evalThreads` is a ceiling, not a target

`evalThreads` fixes the size of the worker pool, and the pool is never resized
at runtime. What *is* adaptive is how many of those workers may be inside a
**planning** eval at once: the manager measures its own plan-apply conflict rate
and moves that limit between a floor and `evalThreads`.

The practical consequence is that setting `evalThreads` too high stops being
expensive — the controller finds the level — while setting it too low still
caps the cluster. Set it to your core count.

This is `observe` by default: the controller publishes the limit it *would*
enforce and actuates nothing until you set
`schedulerSettings.adaptiveConcurrency.mode = "enforce"`. The full surface,
including the states, metrics and the one queue-ordering deviation it
introduces, is in
[adaptive-scheduler-concurrency.md](adaptive-scheduler-concurrency.md).

### Batching

Steps 1 and 3 are fixed costs paid per round, not per eval. On a replicated
(raft) manager each is a quorum round on the scheduler lane, and that lane
drains through a single pipeline thread. Without batching, a cluster doing a
little work per eval hits a ceiling set by those rounds instead of by the
work itself. Taking N evals through one cycle divides those two rounds by N.

The batch is sized to the queue, not to a timer:

- **It never waits to fill.** A worker takes whatever is claimable right now
  and starts working. Idle latency is unchanged.
- **Each worker takes a fair share.** Each worker takes at most
  `pending_evals / evalThreads` rows, so a burst of work spreads across
  workers instead of queuing behind the first one to wake.
- **It is capped at 16** rows per worker regardless of queue depth.
- **A shallow queue yields a batch of one**, which behaves the same as the
  unbatched loop.

Batching has no configuration. Its two inputs, live queue depth and
`evalThreads`, are already known to the loop.

Batching does not change what evals do or the order they run in. Each eval
still runs its own handler and applies its own plan in its own operation.
The claim still drains in `priority DESC, id ASC` order across the whole
batch, so a low-priority eval can never overtake a high-priority one by
riding in someone else's batch.

**Failure isolation.** Only outcomes the worker observed are written. If
one eval's handler panics or is cancelled, the others in the batch still
settle normally and that eval is left out. Its row stays `in_flight` and
returns to the queue via `requeue_in_flight_evals()` (run on startup and on
raft leadership acquisition). The same applies to the whole batch if the
terminal write fails, or if the worker or manager dies mid-batch: nothing
is written, every row stays `in_flight`, and recovery requeues them. An eval
is never marked failed based on an unknown outcome, because failed evals
are not retried.

The cost of that safety is bounded re-execution. An eval handled inside a
batch whose terminal write never lands runs again after recovery. Eval
handlers are idempotent convergence steps, so this wastes work but does not
duplicate effects. The batch cap limits how much work a single crash can
cause to repeat.

**Leadership.** A worker that loses scheduler-lane leadership partway
through a batch stops there. It leaves the rest `in_flight` for the new
leader instead of running work whose writes cannot commit. A batch can
never be half-applied across a leadership change, because the terminal
write for the whole batch is a single replicated operation that either
commits or does not.

## Eval Queue Semantics

Eval ordering:

- Priority desc, then ID asc.
- An eval submitted without an explicit priority inherits the one implied by
  its job's **priority band** (`critical` 90, `high` 70, `elevated` 60,
  `normal` 50, `low` 30, `background` 15). If no band resolves, it falls
  back to the manager's `defaultPriority`. One field orders the queue and
  governs preemption (see
  [preemption-and-eviction.md](preemption-and-eviction.md)). `legion job run
  --priority N` / `--asap` still override the queue position for a single
  submission without changing the band.

Dedup-on-claim behavior:

After claiming an eval, other pending evals are deleted when all of these hold:

- same kind,
- same scope: the same `job_id`, `job_config_id`, and `node_id` (an empty
  field only matches another empty field),
- already claimable (`ready_at <= now`).

This keeps the queue from filling with redundant work for the same entity.
It only discards work the claimed eval covers. Node-scoped kinds
(`node_drain`, `node_lost_reschedule`) carry no job, so they collapse per
node. Job-scoped kinds collapse per job config, so reschedules for different
jobs on the same node all run, and a stale tick for an older config never
sweeps the tick driving a newer rollout. A `job_reschedule` must not
drop a job's pending `rollout_tick` (ticks re-enqueue themselves, so nothing
else would drive the rollout forward) or its `job_stop`. An immediate eval
must not cancel a delayed one either, since a delayed eval is a timer and
not a queued duplicate (for example, a batch task's next scheduled
`job_update`).

## Supported Eval Kinds

`Eval::handle()` supports:

- `node_join` (with no node, the capacity pass queued by a worker
  registration or an uncordon)
- `node_drop`
- `job_submit`
- `job_update`
- `job_stop`
- `job_reschedule`
- `rollout_tick`

Unsupported kinds fail immediately.

`job_update` (a restart: KV `restart`, `job run --force-reschedule`, a batch
task's next run) removes all of the job's allocations and then places the
config fresh. Two guards keep that from leaving a job at zero:

- An update for a stopped job, or for a config the job has moved past (a
  stale batch follow-up after a resubmit), is skipped before anything is
  removed. A stale `job_submit` is skipped the same way.
- When the placement fails for any reason other than a plan conflict or a
  raft leadership change (both re-run the eval), a `job_reschedule` for the
  config is queued 5 seconds out, so the eval's failure is not the end of
  it. A rollback that cannot place the previous version back queues the same
  retry for that version.

## Scheduler Entry Points

Main paths in `Scheduler`:

- `plan_only` -> compute placements without DB apply.
- `plan_for_config` -> plan against a transient config (used by plan API).
- `plan_and_apply` (+ variants) -> compute and apply placements.

Modes:

- `Fresh`: place full desired set.
- `Reschedule`: place only missing count relative to existing allocations.

## Placement Application and Conflict Safety

Placement apply uses optimistic concurrency with partitioned epochs.

### Job-config scoped epochs

Legion maintains an epoch per `job_config_id` in `allocation_epoch_partition`.

1. Scheduler reads the current epoch for the target job config.
2. Scheduler writes `plan_submission.expected_epoch` with that value.
3. Apply path re-reads the current epoch for that same job config in-transaction.
4. Apply proceeds only when epochs still match.
5. On successful allocation changes, only that job config epoch is bumped.

This keeps conflicts local, so unrelated jobs can schedule in parallel without invalidating each other's plans.

### Stale plans place nothing

Evals wait in a queue, and a job can change while its eval waits. A
`job stop` or a newer config does not move the planned config's epoch, so
the planner checks separately that the config is still wanted, once before
planning and again inside the apply transaction. A config is wanted when its
job exists and is not stopped, and the config is one of these:

- the job's latest config,
- an older config that still has allocations of its own (a staged rollout's
  floor, or a rollout stopped part way), so a lost node is still replaced in
  kind,
- the source of a rollout to the latest config that has not completed. A
  live rollout's rollback re-places it, and a failed or cancelled rollout
  left it running.

A plan that fails the check places nothing and the eval completes normally.
The apply-time rejection is recorded as an `allocation.plan_rejected` event with a
reason starting `plan no longer wanted:`. For example, a delayed reschedule
that fires after `legion job stop`, or a reschedule for v1 that runs after
the v2 rollout finished, is a no-op instead of placing the full count.

The plan submission and its apply run as **one replicated store op**
(`Store::submit_and_apply_plan` / `submit_and_apply_plan_with_candidates`).
That means one write-transaction boundary, and on raft one
propose→quorum-commit→apply instead of two.

### Candidate lists (late-binding apply)

A plan carries an ordered **candidate list** per placement instead of a
single node ([late-binding-plan-apply.md](late-binding-plan-apply.md)). The
apply transaction binds each placement in two passes. First it tries the
best candidate whose node epoch is unchanged since planning. For placements
that found none, it then tries the best candidate that still passes the
transaction's own re-validation, whatever its epoch. A concurrent plan that
touched the first choice costs a fallback rank, or nothing, instead of a
whole-plan rejection.

Coupled placements (shared `distinct` keys, multi-task groups) keep exact
all-or-nothing semantics. Overlapping candidate lists are not coupling.
Candidate lists are on by default (`schedulerSettings.plan_candidates = 8`),
and `0` disables them and restores single-node submissions. The epoch
sample (below) covers the union of every placement's candidate list, and
candidate submissions take the same group commit as single-node ones.

### The epoch sample comes from the plan's own snapshot

The fence rule is that epochs must be sampled no later than the capacity
snapshot the plan is built against. The scheduler meets it by taking the
epochs with that snapshot. The read model keeps its allocations, free
pools, and scope epochs in persistent maps, so one read-lock acquisition
captures all three at O(1) cost, whatever the fleet size.

After planning, the scheduler reads the epochs of only the scopes the plan
touches (the job config plus the union of every placement's candidate list)
from that captured copy. Anything that commits after the snapshot is
invisible to it, so the plan-apply compare-and-set rejects only the plans
whose scopes moved. A plan never re-runs because something unrelated
committed.

`schedulerSettings.lazy_version_sample` is **retired**. It still parses,
so existing configs keep loading, but it has no effect, and the manager
logs one warning when it is set:

```text
WARN schedulerSettings.lazy_version_sample is retired and has no effect: plans now sample the fence epochs from their own snapshot
```

### Group commit

Concurrent plan applies (parallel eval workers) coalesce into **one**
store commit. Plans queued while a commit is in flight batch into a single
replicated op: one write transaction, and on raft one propose→quorum round
for the whole batch. Each plan keeps its own epoch-CAS verdict inside a
per-plan savepoint. A plan that fails mid-apply rolls back alone, and the
rest of its batch commits. A lone plan takes the single-plan path
unchanged, so idle-cluster latency is unaffected.

The batch is one transaction on **every** backend. A local (`local_disk` /
`streaming`) manager pays one fsync for the batch instead of one per plan,
the same way a raft manager pays one quorum round. The batch holds the
store's write connection for the whole transaction, as every other
multi-statement write does, so no unrelated write can land inside it.

What the batch produces is published **once, after that commit**:

- **One read-model refresh** over the union of every plan's allocation
  scopes, read from committed state. Evicted and placed rows refresh
  together, so the mirror never shows an intermediate state.
- **One status-lane `task_status` projection** covering every allocation
  the batch placed, instead of one per plan. Lifecycle-derived task
  statuses are a status-lane write that follows the scheduler-lane apply
  (see [multi-raft.md](multi-raft.md) on lanes). Batching it turns N
  follow-up writes (N more raft rounds under `lanes`) into one.
- **Every staged event**, in id order. Nothing is broadcast before the
  batch is durable.

During the batch, the read model still shows pre-batch state for the
scopes it is about to change. The epoch fence already covers that window:
a plan built against it fails its compare-and-set at apply. A stale read
model costs a conflict and never an oversubscription.

- `schedulerSettings.plan_apply_coalescing = false` restores one commit
  per plan, one status projection per plan, and one read-model refresh
  per plan. This is the only escape hatch. There is no separate switch
  for the local commit amortization or the batched status projection.
- `schedulerSettings.plan_apply_batch_max` (default 16, max 128) caps
  plans per batch.

Known limitation: a batch is bounded by `plan_apply_batch_max`, and a
larger batch holds the write connection (and, on raft, one log entry)
for proportionally longer. Raising it past the default trades tail
latency for other writers against fewer commits. The raft append-entries
deadline (`heartbeatIntervalMs`) is the hard ceiling, which is why the cap
exists.

Worker heartbeats batch the same way on the status lane (see
[architecture-managers-workers.md](architecture-managers-workers.md)).

### Job intake group-commits too

Submitting a job writes to all three raft lanes. The **control** lane writes
the job and its config, the **status** lane seeds one `pending`
`task_status` row per task, and the **scheduler** lane enqueues the
placement eval. Under `lanes` that is three quorum rounds per submitted job,
and a client that submits many jobs at once pays all three per job.

All three lanes batch. Concurrent submitters coalesce their control-lane
job write into one `handle_job_submits` commit, their scheduler-lane
enqueue into one op, and their status-lane seed into another. This is the
same group commit plans use, with per-request savepoints so one bad
submission rolls back alone and the rest of its batch still commits. Each
lane is its own raft group with its own pipeline, so a batch can never span
lanes. That is why there is one coalescer per lane.

The control-lane batch is safe because version derivation reads through the
batch transaction. Two submits of the same job in one batch derive
successive versions (N+1 then N+2), because the second sees the first's
in-transaction write. The same property fixes the version race for
concurrent single submits. A `NotRaftLeader` rejection re-runs each request
singly, so each caller gets the typed error its split-leadership handling
expects. With all three lanes batched, a 32-wide submit burst costs roughly
1.5 quorum rounds per job instead of 3.

What does **not** change:

- **A lone submit is untouched.** There is no accumulation window. A
  single `legion job run` on an idle cluster goes straight to the commit
  slot and takes the single-op path.
- **Queue order.** Evals are still claimed strictly by
  `priority DESC, id ASC`. A batch inserts in arrival order and each eval
  still derives its own priority (an explicit `--priority`/`--asap`
  override, else the job's band).
- **Idempotency.** `RunJob`'s `idempotency_key` still decides before
  anything is written, and the loser of a concurrent same-key submit is
  told so (`deduplicated: true`, carrying the winner's message).

`schedulerSettings.job_intake_coalescing = false` restores one op per
submitter on all three lanes. Batch size is capped at 64 per lane and is
not configurable.

To check whether batching is engaging, look at
`legion_store_coalesce_batch_size{path="job_submit"}`,
`{path="eval_enqueue"}`, and `{path="task_status_seed"}`, and compare the
`le="1"` bucket against `_count` ([metrics.md](metrics.md)). Serial
submitters never overlap, so a `for` loop over jobspecs will sit at 1 and
there is nothing to tune.

### Observing where plan time goes

Every `plan_and_apply` records a per-phase wall-clock breakdown
(`PlanTimings` on the outcome): job lookups, node set, task fetch, capacity
snapshot, `build_plan`, epoch sample, speculative dispatch, the fused apply,
and the post-apply bookkeeping. It is logged per plan at debug, exported as
the `legion_scheduler_plan_phase_seconds{phase}` histogram
([metrics.md](metrics.md)), and aggregated by the benchmark runners
([testing-and-benchmarks.md](testing-and-benchmarks.md)). Rejected plans are
observed too, so a plan that loses the fence still shows up. When scheduling
latency changes, find the phase that moved first.

A plan is only part of an eval. `legion_eval_phase_seconds{kind, phase}`
splits each eval's handler time into the wait for a blocking-pool thread
(`queue_wait`), scheduler construction, the kind's own pre-plan work
(`prepare`), every plan attempt (`plan`), the sleeps between conflicted
attempts (`conflict_backoff`), building the workers' payloads (`dispatch`),
and post-plan bookkeeping (`finish`). `legion_eval_queue_round_seconds{op}`
times the claim and settle rounds between handlers. Together they account
for the gap between `legion_eval_handle_seconds` and the plan's `total`.

### Apply-time re-validation

The epoch fence only covers changes that bump an allocation epoch. Node-row
changes bump none: a cordon or drain, a node loss, or a worker
re-registration that shrinks the node's capacity (`memory`,
`reservedResources`, `maxAllocations`, …). So the apply transaction
re-validates every placement against the **fresh** node row before
inserting it:

- **Availability**: the node must still be `available`.
- **Constraints and permits**: the task's constraint and permit sets must
  still be satisfied.
- **Resource totals**: the node's free pool is rebuilt inside the
  transaction, and every placement must still fit, with earlier placements
  in the same plan debited. The free pool is current capacity minus the
  allocation rows the transaction can see, excluding a Fresh plan's own
  replaced job. This is the same arithmetic as the planner's ledger.

#### The per-node usage cache

Rebuilding that free pool means reading every allocation row on the node
and parsing every stored `resources` blob, inside the serialized plan-apply
commit. Without a cache, the cost of committing a plan grows with how full
the cluster is, even when the plan's own work stays constant. On a
5,000-node fleet, the same ~44 placements read ~440 allocation rows with
50,000 allocations live and ~1,760 with 200,000.

So a node's total allocation usage is cached in `node_alloc_usage`, one row
per node. The cache is always validated before use. Each row stores a
fingerprint of the allocation rows it was computed from: the node's
allocation epoch, the row count, and the sum of the allocation ids. The
apply path recomputes that fingerprint (an index-only read that touches no
row bodies and parses no JSON) before using the cached value. If it does
not match, the pool is rebuilt from the rows, and the plan refreshes the
row on its way out.

What this means in practice:

- **The number is unchanged.** The cache uses the same capacity arithmetic,
  the same overcommit policy, and the same exclusion of a Fresh plan's own
  job. Cached and rebuilt pools can differ only by a floating-point
  rounding step, because the cache subtracts one summed total where the
  rebuild subtracts each allocation in turn.
- **Nothing has to maintain it.** Any path that adds or removes an
  allocation (a placement, a stop, a preemption, a node removal, a job
  destroy) moves the fingerprint, which invalidates the affected rows. A
  stale, wrong, or missing row costs one rebuild. It can never inflate a
  node's free capacity.
- **Version skew is safe in both directions.** A manager binary without
  this feature neither reads nor writes the table, so a leader on either
  side of an upgrade finds the fingerprints stale and rebuilds. The table
  is created empty with no backfill.
- **A resource-policy change invalidates it.** Each row also records a
  fingerprint of the resource policy (`taskResourceDefaults`,
  `customResources`) that normalized it. A row from any other policy,
  including one written before the fingerprint existed, is rebuilt instead
  of served. Without this, raising a task default and restarting would keep
  serving totals computed under the old default on every node whose rows
  had not moved, understating usage indefinitely. A policy change therefore
  costs one rebuild per node, the same as a cold start.

The scheduler's **in-memory read model keeps the same total per node**,
next to each node's cached allocation list. Re-deriving a node's free pool
and index projections after a committed change happens on every replica,
inside the write lock every planner reads through. With the cached total,
that work is a subtraction over resource dimensions instead of a re-parse
of every allocation on the node. The cached total is maintained only by
splices that know the exact rows they moved. Every wholesale list reload
drops it, and the next reprojection rebuilds it from the list. It shares
`NodeUsage` (and the equivalence tests) with the persistent cache above.

Known limitation: removing a node leaves its cache row behind (the removal
runs on a different raft lane than the table's writer). The row is inert
and roughly 100 bytes.

A plan is rejected when re-validation fails on every candidate. The eval
handler's conflict retry then re-plans against the node's current state.
With the two-pass bind, this re-validation (not the node epoch) decides
whether a contended placement lands. Port assignments are re-read in the
apply transaction too, so availability, constraints, resources, and ports
all get the same apply-time check.

### Conflict handling

A rejected plan raises a conflict error, which the eval handler retries
with bounded attempts via `Eval::plan_with_conflict_retries`. A stale
job-config epoch always rejects. A stale node epoch only rejects when the
re-validation above also fails, or for an edge-boot ratification, which
keeps the hard node-epoch check.

Running out of that in-handler retry budget does not fail the eval. The
eval goes back to the pending queue with a jittered delay (`eval.requeued`
event) and runs again. Conflicts are short-lived by nature. Non-conflict
errors still fail the eval (for example, a pool with no capacity left).

### Tuning conflict retries

Use manager `schedulerSettings.plan_conflict_max_retries` to tune retry behavior for high contention.

- default: `3`
- minimum: `1`
- maximum: `20`

The requeue backstop is paced by
`schedulerSettings.plan_conflict_requeue_delay_ms` (default `1000`,
clamped to 100–60000, plus uniform jitter of up to the base).

Example manager config:

```nickel
{
  schedulerSettings = {
    plan_conflict_max_retries = 8,
    plan_conflict_requeue_delay_ms = 500,
  },
}
```

With candidate lists on, retries are rarely spent. Check
`legion_plan_apply_total{outcome}` and the candidate-rank histogram before
tuning here (see
[late-binding-plan-apply.md](late-binding-plan-apply.md)).

## Node Candidate Construction

`available_nodes()` currently includes nodes where:

- `node.availability == "available"`

Nodes are sorted by ID before planning. The sorted list, a by-ID lookup over
it, and each node's free-capacity pool are cached in the scheduler read model
and shared by reference. Taking the node set is one `Arc` clone, so a plan's
node set costs the same at 1,000 nodes as at 50,000. Before the lookup was
cached, every plan built a hash map over every available node and copied the
list, which was the last per-plan cost that grew with the fleet.

Any node mutation (registration, attribute change, drain, loss, removal, or a
state-store swap such as a snapshot install) invalidates the cached set, and
the next plan rebuilds it. The rebuild runs outside the read-model write lock,
so it never stalls heartbeats or other planners, and it is installed only if no
node changed while it was being built. `legion_scheduler_node_list_rebuilds_total`
counts rebuilds ([metrics.md](metrics.md)). It should sit near zero while the
fleet is stable.

A plan that excludes nodes (a reschedule away from a failed node) shares the
cached set unless an excluded node is actually in it. Only then does it build a
filtered copy, which costs O(fleet). In practice the excluded node is usually
already unavailable.

The read model also keeps a per-job index of allocations by node,
maintained by the same writes that maintain the per-node allocation lists.
A plan takes its own job's entry with the snapshot and derives everything
it needs about its existing allocations from it:

- the rows a fresh plan replaces (and so does not count against capacity)
- the counts a reschedule still has to place
- group pinning
- `distinct` seeding
- the scoring cache's per-node task presence

Nothing in a plan walks every allocation in the cluster, so planning cost
grows only with the job being placed, not with how full the cluster is.
After a plan commits, its placed and evicted rows are published into the
read model as a delta (one indexed read per placed row) on every backend.
This is the same projection the raft apply path runs from a committed
changeset.

Per task, the candidate set is then built one of two ways:

- **Scan** (the baseline): every available node is tested against the
  task's constraints, permit, and volume-driver requirements.
- **Scheduling index** ([scheduling-indexes.md](scheduling-indexes.md)):
  when the fleet is at or above `schedulerSettings.index_min_nodes` and a
  declared index binds the task's equality constraints, candidates come
  from that index (partition shard lookup plus a nearest-fit walk over
  free resources). Only those candidates are re-verified with the same
  predicate the scan applies. The scan remains the authoritative fallback
  (unusable index, empty coverage, or a truncated list running dry), and
  `job plan` reports which path served each task.

When neither path yields a node with room, and the job's band outranks work
running on a node it is otherwise allowed on, a **preemption pass** runs.
It chooses victims, and if it finds one, the placement and the evictions it
pays for commit in the same epoch-CAS transaction. The pass runs only after
normal feasibility has failed, so a cluster that never preempts pays
nothing for it. See [preemption-and-eviction.md](preemption-and-eviction.md).

## Placement Constraints and Scoring

For each task, the planner builds:

- `ConstraintSet` (hard filter)
- `PermitSet` (hard allowlist/restriction checks)
- `AffinitySet` (soft scoring)
- `DistinctTracker` (for `distinct` constraints)
- Resource request from normalized task resources
- Content locality scores, when the task has a content set (below)

Selection policy:

- built-in `bin_pack` or `spread`, or loaded scheduler plugin
- optional `nodes_considered` random subset before scoring
- optional bounded candidate scoring (`score_sample`, below)
- optional score jitter on reschedule mode for less deterministic stickiness

Resource fit is checked before a node can be selected.

Constraints, permit, and volume-driver support are applied once, when the
task's candidate list is built. The per-placement passes (eligibility, load
bounds, scoring) only re-test what changes between placements: remaining
capacity and `distinct` coverage. They index their per-node state by
candidate position instead of by node id. Per-placement cost is therefore a
small constant per candidate, and on large fleets `score_sample` (below)
bounds the candidate count itself.

### Bounded candidate scoring (`score_sample`)

By default every placement scores **every** feasible node. That gives exact
placement, at a per-placement cost that grows with fleet size. On large
fleets, `schedulerSettings.score_sample` bounds it:

```nickel
{
  schedulerSettings = {
    score_sample = { min_nodes = 1024, sample = 128 },
  },
}
```

Below `min_nodes` eligible candidates, behavior is the same as the default
(exact scoring). At or above it, the placement scores `sample` uniformly
drawn candidates and takes the best. This best-of-k selection keeps
per-placement cost flat at any fleet size. Two properties always hold:

- **Feasibility is never sampled.** Constraints, permits, distinct rules,
  and resource fit always run in full, so a sampled candidate is always a
  valid placement. Only the score ranking is approximate.
- **The trade-off is visible.** `job plan` explain output notes
  `scored best of N sampled nodes` on any task whose placements were
  sampled. The packing-quality cost is bounded by the power-of-k-choices
  property (the repository carries a regression test asserting the bound,
  and the measured numbers live in `benchmarks/scheduler/README.md`).

Validation: `sample >= 2` and `min_nodes >= sample`, rejected at plan time
otherwise. Only the built-in `bin_pack`/`spread` policies sample. Plugin
policies always receive the full eligible list. When both
`nodes_considered` and `score_sample` are set, `nodes_considered` applies
first (it is a hard cap). Combining them is not recommended. This setting
is off unless configured, and exact scoring is the default at every fleet
size.

### Content locality

When a task declares digest-pinned artifacts, the planner looks its content
set up once per task in the manager's in-memory content location index and
gets back the fraction of the set each node already holds
([artifacts.md](artifacts.md#content-locality)). The lookup is one hash probe
per digest plus one map write per node holding it. It never walks the
candidate list. Each candidate's score then reads its fraction from the
per-candidate state, so the per-placement cost does not change. The term is
weighted by `schedulerSettings.weights.locality` (default `1.0`) in both the
built-in policies and the context handed to scheduler plugins. A task with
no content set, or a weight of `0`, skips the lookup entirely.

The index is soft state on the manager that receives a node's heartbeats,
rebuilt from full reports after a restart or failover. Nodes it has not
heard from score `0`, which costs placement quality and never correctness.

## Dependency-Aware Task Readiness

Before placement, tasks are filtered via dependency graph and current task-status map:

- topological ordering rejects cycles
- dependency conditions:
  - `start`
  - `complete`
  - `success`

Only ready tasks are considered in that scheduler pass.

## Eval Triggers into Scheduling

Typical triggers:

- `run_job` -> enqueue `job_submit` / `job_update` / `rollout_tick`.
- Worker failure heartbeat -> enqueue delayed `job_reschedule`.
- Dynamic node updates -> enqueue `job_update` for affected jobs.
- Rollout engine -> enqueue delayed `rollout_tick` while active.

## Observability: the Scheduler Event Stream

Every observable scheduler action is recorded as a scheduler event. That
includes evals enqueued, completed, and failed, plan applies, allocation
removals and failures, node and job lifecycle transitions, and rollout
activity. Events are exposed via `legion events list` /
`legion events follow`, the `legion_events` MCP tool, the UI Events page,
and the `ListEvents`/`StreamEvents` gRPC methods.

See [Event Stream](event-stream.md) for the kind catalog and consumption
patterns.

## Known Boundaries

- Queue dedupe is scoped to the job, config, and node IDs plus kind, not full
  payload equivalence. Two `job_reschedule` evals for the same job config and
  node collapse even when they were enqueued for different reasons.
- With a replicated (raft) store, the eval queue is replicated, but only the
  scheduler lane's leader claims and runs evals. Other managers do not share
  eval work.
