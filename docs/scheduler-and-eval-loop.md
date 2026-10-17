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
- Startup recovery calls `requeue_in_flight_evals()`, which forgets every
  claim this manager held and arms the blocked-eval wake replay (below).

Each worker loop:

1. Ask the admission gate for a permit
   ([adaptive-scheduler-concurrency.md](adaptive-scheduler-concurrency.md)).
   With the controller `off`, and with today's default `observe`, this never
   refuses.
2. `claim_evals()` claims a **batch** of `pending` evals. A claim is a mark
   in the manager's memory, not a write: the rows stay `pending` in the
   store, and listings on the claiming manager show them as `in_flight`. A
   worker the gate refused claims only eval kinds that cannot conflict
   (today, `job_stop`).
3. Executes `Eval::handle(store)` for each one, in claim order, one at a
   time, holding the permit across the whole batch, conflict-retry backoffs
   included.
4. `complete_evals_and_claim_next()` writes every outcome in one operation
   and claims the next batch (in memory). Success deletes the eval row,
   failure sets status `failed`, a placement that found no room sets status
   `blocked` (see [Blocked evals](#blocked-evals)), and a transient
   rejection returns the row to `pending` after a delay. The permit is
   released, and asked for again at the next batch.

**An eval can settle with its own plan.** An eval whose plan is its last
write carries its settle into that plan's apply: the applied plan deletes
the eval row, deletes the duplicates its claim subsumed, and records
`eval.completed` in the same entry. The worker then has nothing to write for
it. On an idle cluster this makes a placement one scheduler-lane entry (the
plan) instead of three (claim, plan, settle). These handlers settle with
their plan:

- `job_reschedule`, the placement path of every keyed create and resume.
- `job_submit` for a job's first config. A resubmit removes the previous
  config's allocations after its plan, so it settles on its own.
- `job_update`, whose removals and resets all happen before its plan.
- `job_scale` when it scales out. A scale-in queues a capacity pass after
  its plan.

None of them settles with its plan when a scheduled batch task needs a
follow-up queued after it. A plan that is rejected, places nothing, or
leaves replicas unplaced also leaves the eval to the ordinary settle: after
a partial plan the handler may still plan the rest with an avoided node
allowed, or queue a retry for preemption victims that are too young, and
the eval must stay queued until those writes land.

**Why claims are memory.** A claim used to be a replicated write
(`pending → in_flight`). The only thing it bought was recovery: a new
leader recognized a dead leader's half-handled evals as `in_flight` and
moved them back to `pending`. An unclaimed `pending` row gets that for
free, because the next leader simply runs it. Handlers are idempotent
convergence steps, so running an eval again after a failover wastes work
but never duplicates effects; the old recovery relied on the same property.
The batch cap bounds how much one crash makes repeat.

When the queue is empty, workers block on either:

- `Notify` wake-up (an eval became claimable: an enqueue of zero-delay
  work, a release, or a blocked eval woken), or
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

Step 4 is a fixed cost paid per round, not per eval. On a replicated
(raft) manager it is a quorum round on the scheduler lane, and that lane
drains through a single pipeline thread. Taking N evals through one cycle
divides that round by N, and a batch whose every member settled with its
own plan writes nothing at all. (Claims were the other fixed round until
they moved to memory.)

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
settle normally and that eval is left out. Its claim is released and its
row, still `pending`, runs again. The same applies to the whole batch if
the terminal write fails, or if the worker or manager dies mid-batch:
nothing is written, and the rows are still `pending` for whichever manager
leads next. An eval is never marked failed based on an unknown outcome,
because failed evals are not retried.

The cost of that safety is bounded re-execution. An eval handled inside a
batch whose terminal write never lands runs again. Eval handlers are
idempotent convergence steps, so this wastes work but does not duplicate
effects. The batch cap limits how much work a single crash can cause to
repeat.

**Leadership.** A worker that loses scheduler-lane leadership partway
through a batch stops there and releases its claims; a manager that is not
the scheduler-lane leader holds none. A batch can never be half-applied
across a leadership change, because the terminal write for the whole batch
is a single replicated operation that either commits or does not.

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

A claim subsumes the other pending evals that match all of these. They are
held with the claim (no other worker can take them) and deleted by its
settle, in the same write:

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

A `job_update` also resets the config's task statuses to `pending` (only for
a config with `dependsOn` edges), so a restarted DAG runs from its first
step instead of finding the previous run's `success` statuses.

A `job_reschedule` that carries a node (queued after an allocation failed
there) **avoids** that node: it plans without it first, then plans whatever
that pass could not place with the node allowed. On a cluster where the
failed node is the only one with room, the replacement returns to it.

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

### Partial placement

A plan places what fits. When no node has room for a replica (capacity, a
static port, a `distinct` rule, a stateful instance's pinned node), the
planner stops placing that task and reports the remaining replicas as
**unplaced**, with the reason, instead of failing the whole plan. The other
tasks of the job, and the replicas of this task that did fit, are applied.

- The eval succeeds and says what it left undone:
  `Job web rescheduled with 1 placements; 1 unplaced (Failed to place task
  web: static port 8080 is already in use on every node it is allowed on that
  has room for it. ...)`. A plan log line at `warn` carries the same reasons.
- A plan that can place **nothing** does not fail its eval: the eval
  blocks on what the first unplaced task needs and wakes when capacity that
  could help appears (see [Blocked evals](#blocked-evals)). The error it
  blocks with carries the same message, so `job plan`, the eval outcome and
  the `eval.blocked` event all name the same reason.
- A **partially placed** plan succeeds, and its unplaced remainder is placed
  by the next pass that has room: the **capacity pass**, a `node_join` eval
  with no node that re-plans every active job short of its count. It is
  queued when a node registers or is uncordoned, and, while some plan has
  left replicas unplaced, whenever capacity frees up: a worker reports an
  allocation finished, a failed allocation is removed, a job stops, or a job
  scales in. One pass covers any number of frees (it is not queued again
  while one is pending), and when nothing is waiting a free queues nothing.
- Only the manager that leads the scheduler lane queues the pass. After a
  restart or a leadership change the first freed capacity runs one pass,
  which finds out whether anything is waiting.
- A job with an eval already queued, or blocked, is skipped by the pass
  (that eval runs against the new capacity anyway).
- A batch task is retried only while it has not finished (its status is not
  `success`, `complete`, or `failed`).

Tasks in a multi-task group (co-placed on one node) stay all-or-nothing: a
member that cannot fit fails the plan. `legion job plan` also keeps
all-or-nothing, so a dry run reports the first replica that would not fit as
its error.

A task that sets [`gang`](gang-scheduling.md) opts out of partial placement:
when one of its replicas does not fit and fewer than its quorum (`gang.min`,
else the whole count) would run, the plan places nothing at all, for any task
of the job, and blocks. Above the quorum the rest of the gang is ordinary
unplaced work. A dry run reports a gang that would wait in
`PlanJobReply.gang_reports` rather than as an error.

### Static ports

A task that declares a static port (`network.ports[].static`, or an upstream
`localPort`) can run once per node. The planner reads the host ports each
node's allocations already hold, plus the ports this plan has claimed so
far, and a node where one of the task's static ports is taken is not a
candidate for it. The apply transaction makes the same check, so the planner
no longer builds plans the apply has to reject.

What this means in practice:

- `count = 2` with a static port on a one-node cluster places one replica
  and reports the other unplaced, naming the port. It used to reject the
  whole plan, requeue the eval about twice a second, and place nothing.
- A replica on a node that is already running the port (another job's, or
  another replica's) is placed on a node where the port is free, or not at
  all.
- Preemption never targets a port-blocked node: evicting work frees
  capacity, not a port.
- A rollout that cannot place a replacement because the version it replaces
  holds the port stops the old allocation first (see
  [rollouts.md](rollouts.md#static-ports-and-surge)).

Ports a Fresh plan's own previous allocations hold count as taken until the
handler removes those rows, matching what the apply sees. Ports assigned
dynamically by this same plan are not known until apply, so a static port
inside the dynamic range can still collide at apply time with a dynamic port
handed out in the same plan; the candidate walk then falls back to the next
candidate.

### Conflict handling

A rejected plan raises a conflict error, which the eval handler retries
with bounded attempts via `Eval::plan_with_conflict_retries`. A stale
job-config epoch always rejects. A stale node epoch only rejects when the
re-validation above also fails, or for an edge-boot ratification, which
keeps the hard node-epoch check.

Running out of that in-handler retry budget does not fail the eval. The
eval goes back to the pending queue with a jittered delay (`eval.requeued`
event) and runs again. Conflicts are short-lived by nature. A plan that
finds no room at all blocks the eval instead (see
[Blocked evals](#blocked-evals)); other errors still fail it.

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

A plan that excludes nodes (the first pass of a reschedule away from a failed
node, or a rollout's in-place pass) shares the cached set unless an excluded
node is actually in it. Only then does it build a filtered copy, which costs
O(fleet).

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

Before placement, tasks are filtered via the dependency graph and the task
statuses workers reported:

- topological ordering rejects cycles
- dependency conditions:
  - `start`: the dependency started, or is placed in this pass
  - `complete`: the dependency's worker reported it finished, either way
  - `success`: the dependency's worker reported a successful exit

Only ready tasks are considered in that scheduler pass. The others are
**held**: reported on the plan outcome (`waiting on dependencies: ...` in the
eval's outcome), not counted as unplaced, and not a capacity block for a
rollout. A batch dependency that is merely placed in the same pass does not
satisfy `complete` or `success`; only a reported exit does. The dry run
(`legion job plan`) is the exception: it treats batch dependencies as
satisfied so it can show the whole job.

When a worker reports a terminal exit for a task that other tasks depend on,
the manager queues an eval for the job (a `rollout_tick` while a rollout
targets the config, a `job_reschedule` otherwise), which places what the exit
made ready. See [jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md#task-lists-and-dependency-readiness).

## Blocked evals

A placement that finds no room does not fail its eval. The eval is written
`status = 'blocked'`, keyed by what it waits for, in the same write that
used to mark it `failed`, and it returns to `pending` on its own when
capacity that could help appears. Nothing else re-plans it, so a node
joining one pool costs evals only for the jobs blocked on that pool, and a
job never stays short just because no event happened to re-run it.

### What blocks

The job evals that place work block: `job_submit`, `job_update`,
`job_reschedule`, and `job_scale`. They block when the plan finds no node
for a task:

- no node the task may run on has room, after preemption had its chance
  (the same diagnosis the failure used to carry, which still names
  whether preemption was off, in observe mode, or found no victim);
- no node has free disk for the task's node-local volumes;
- no node has room for a memory checkpoint of a task that suspends to
  memory;
- the node holding an instance's node-local storage, or a resume's
  checkpoint, is unavailable;
- the cluster has no schedulable node at all;
- a [gang](gang-scheduling.md) task cannot reach its quorum. The plan then
  places nothing, and the eval blocks under the key of the gang's first
  missing replica, with the gang's report in its detail.

One placement waits on the clock instead of blocking: preemption that could
make room, but only once its victims age past `preemption.victimExemptAge`.
No capacity event marks a victim getting older, so a blocked eval would wait
for an unrelated one. The eval goes back to `pending` with a delay that ends
when the youngest victim it needs is old enough, and records `eval.blocked`
with a message that names the wait. A plan that placed part of the job and
left a replica waiting this way queues a delayed `job_reschedule` instead
([Preemption and eviction](preemption-and-eviction.md#which-of-them-are-taken)).

Node-scoped passes (`node_lost_reschedule`, `node_drain`, the capacity pass)
already queue a per-job `job_reschedule` for each job they could not place,
and that reschedule is what blocks. A `rollout_tick` that cannot place still
fails its rollout, as before. A pending keyed instance is covered like any
other placement: its `job_reschedule` blocks, and wakes when room appears.

**One blocked eval per job.** A job that blocks again (another instance was
created, the job was updated) replaces its older blocked eval in the same
write, and any eval of the job that succeeds deletes it: the job has
nothing left to wait for.

### Keys

The key says what the eval waits for:

| Key | Meaning | Woken by |
| --- | --- | --- |
| `class:pool=<pool>[,<attr>=<value>...]` | The task's pool and the attributes its constraints pin with `=`. `pool=*` when the permit's pool rule is not a plain `=` | Capacity on a node the task's permit and constraints accept |
| `node:<id>` | A resume or restore pinned to the node holding its checkpoint or node-local storage | Capacity on that node only |
| `escaped` | No schedulable node at all, so no class to wait on | Any capacity anywhere |

The match is exact rather than by key string: the wake pass re-runs the
blocked task's own permit and constraint checks against the node whose
capacity changed, so operators the key cannot spell (`>`, `matches`,
`version_matches`, `contains`) still wake only on nodes that satisfy them.
Whether the room is *enough* is not checked: the plan that runs next
decides that, and blocks again if not.

### Wake events

A capacity event is any committed change that may have made room:

- a node registering (`register`);
- a node's pool, availability (an uncordon), or attributes changing
  (`node`);
- a node's liveness status changing, as when it returns to `ready`
  (`ready`);
- allocations leaving a node: a stop, a destroy, a suspend, a preemption
  (`freed`).

The changeset projection records these on every manager. The scheduler-lane
leader's wake pass runs within 50 ms of an event (a burst costs one pass),
matches the events against the blocked evals, and returns every eval a node
could help to `pending` in one scheduler-lane entry, with an
`eval.unblocked` event each. The eval loop then runs them like any other
work. An event on a node that is not available (cordoned, drained) wakes
nothing.

**An unblock during the job's own eval is not lost.** A claim records the
event log's position. An eval that is about to block first checks the
events since its claim: if capacity a node could help with appeared while
its plan ran, it goes straight back to `pending` (`eval.unblocked` with
cause `missed`) instead of blocking on a picture that is already stale.

**Leadership changes and restarts.** The blocked rows are replicated
state. The event log is in memory, but every manager keeps its own copy. A
manager that takes the scheduler lane replays the last 60 seconds of events
against the blocked rows, which covers whatever the old leader had not
acted on when it died. A manager process that starts as the scheduler's
writer has no log to replay, so it returns every blocked eval to `pending`
once (`eval.unblocked` cause `startup`); each blocked job costs one
re-plan, and blocks again if there is still no room.

### Seeing it

`legion job info <job>` and `ListJobs` report a `blocked` block for a job
with a blocked eval, and `status: blocked` where the
[derived job status](jobs-tasks-and-scheduling.md) would otherwise read
`pending` (nothing of the job is live). A job with live allocations keeps
`running` and still carries the block. The human table prints the block as
a line under the job:

```console
$ legion job info agents -j
{"ok":true,"data":{"id":"agents","name":"agents","status":"blocked",
 "blocked":{"key":"class:pool=agents","task":"workspace",
  "shortfall":{"cpu":"500MHz","memory":"3000MiB"},
  "message":"Failed to place task workspace: no available nodes matching constraints, permit, or resources; ...",
  "since":1791836651,"evalId":"42"}, ...}}
```

`shortfall` is what one placement of the task asks for, not the job's total.
Both are read-side overlays: nothing is stored on the job, and a stopped
job never shows `blocked`. The UI's job page shows
the same block as a note, the MCP `legion_job_info` tool returns it, and
`legion eval list` shows the eval with status `blocked`.

Events: `eval.blocked` (warn, with the key and detail) when an eval blocks,
and `eval.unblocked` (info, with the cause and node) when it wakes. Metrics:
`legion_evals_blocked{key_kind}`, `legion_evals_unblocked_total{event}`, and
`legion_blocked_wait_seconds` (see [metrics](metrics.md)).

### Limitations

- A checkpoint budget report and a completed `snapshot warm` do not wake
  evals blocked on checkpoint room: those are worker reports, not store
  changes. A node event on the same node (allocations leaving it) does.
- A blocked eval for a job whose stored status is `failed` (for example, a
  KV-reference failure) still wakes and runs; it changes nothing the failure
  did not.
- A stale blocked eval (its config superseded by a newer submit) is removed
  by the stale-eval prune that runs on leadership acquisition, not at once.

## Eval Triggers into Scheduling

Typical triggers:

- `run_job` -> enqueue `job_submit` / `job_update` / `rollout_tick`.
- Worker failure heartbeat -> enqueue delayed `job_reschedule`.
- Worker terminal report for a task with dependents -> enqueue
  `job_reschedule` (or `rollout_tick` during a rollout).
- Dynamic node updates -> enqueue `job_update` for affected jobs.
- Rollout engine -> enqueue delayed `rollout_tick` while active.
- A node registering or being uncordoned -> one cluster-wide `node_join`
  capacity pass, which re-plans only jobs short of their count that have no
  pending or blocked eval (a backstop: a job that could not place normally
  has a blocked eval, which the wake pass handles).
- Capacity events -> blocked evals back to `pending` (see
  [Blocked evals](#blocked-evals)).

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
  eval work. Claims are in that leader's memory, so `legion eval list` on
  another manager shows a claimed eval as `pending`.
