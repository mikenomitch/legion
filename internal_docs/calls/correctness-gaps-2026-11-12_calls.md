# Judgement calls: closing the TODO's correctness gaps (2026-11-12)

This work had no plan: it closed the three items under "Correctness gaps" in
[`TODO.md`](../TODO.md): audit SC7 (Fresh plans overcommit while the old
config runs), the rest of audit SC4 (capacity-pass triggers), and the missing
diagnostic for a partly placed job's unplaced envelopes. The audit's own calls
are [architecture-audit-2026-10-09_calls.md](architecture-audit-2026-10-09_calls.md)
(#10 and #17 are revised by entries 1 and 2 here). Entries are ranked by
project impact, so an entry's number is its rank. Limitations a user needs to
know are also stated in the public docs each entry links.

## 1. A Fresh plan removes the version it replaces inside its own apply, after the walk binds

**The call.** The apply transaction of a Fresh plan deletes every allocation
of the planned job that belongs to another config, after the candidate walk
has bound every placement and before the transaction commits
(`Store::retire_replaced_allocations_tx`). Stop notices (`cause: update`), the
`allocation.stopping` and `allocation.removed` events, epoch bumps, and the
stateful instance rebinding all happen in that transaction. `handle_job_submit`
still calls `remove_allocs_for_job_config` afterwards; it finds nothing unless
the plan applied nothing (every task held by `dependsOn`, or nothing to place).
Public surface: [scheduler-and-eval-loop.md](../../docs/scheduler-and-eval-loop.md)
("Modes").

**Alternatives.** (a) Delete the replaced rows *before* the walk, as
evictions do, with pre-deletion node epochs captured for the walk's fence. (b)
Keep counting them as used and require headroom for both versions (the
original audit wording's literal reading). (c) Pass the replaced config id in
the plan submission instead of deriving it inside the transaction.

**Trade-off.** During the walk the old rows still exist, so the walk's
in-transaction free pools keep excluding them by job (`drop_own`) rather than
by their absence, and static ports they hold still count as taken for the new
version (a resubmit that keeps a static port needs another node or a restart,
as before). Every node an old row sat on has its usage cache invalidated, so
the next plan there rebuilds one pool. The planned config's own rows are not
touched, so the rare re-run of a `job_submit` whose plan already applied still
plans its full count on top of them, as it did before.

**Why (simplicity, correctness).** After the walk, the deletions are this
plan's own writes and cannot trip the walk's node-epoch fence, so none of the
pre-eviction epoch bookkeeping (a) needs is required. More importantly, the
new allocations already exist when the old ones go, so
`release_instance_bindings_tx` rebinds a stateful slot straight to its
replacement (`MAX(id)` of the surviving rows). Deleting first would send the
instance through `rescheduling` with nothing bound and depend on the walk
re-binding it, which is the "care for stateful release and rebind" the TODO
warned about. (b) would make every resubmit of a job using more than half a
node fail. (c) adds a field to a raft-logged request for something the
transaction can read itself; deriving it also covers a job left with rows of
two older configs (a rollout that completed incomplete), which `SnapshotDrop::Job`
already counted as free.

## 2. Every capacity event on a schedulable node can queue the capacity pass, from the blocked-eval wake loop

**The call.** The blocked-eval wake pass (`Store::wake_blocked_evals`, run on
the scheduler-lane leader for every batch of capacity events) also calls
`request_capacity_reconcile_if_waiting` when any new event names a node that
is `available` and not `lost`. That covers a lost node heartbeating again
(it flips availability back without registering) and frees that never pass
through the terminal-report handlers (preemption, drains, gang losses). The
existing triggers stay. Public surface:
[scheduler-and-eval-loop.md](../../docs/scheduler-and-eval-loop.md) (partial
placement).

**Alternatives.** (a) Call `request_capacity_reconcile_if_waiting` in
`restore_availability_after_loss` and `reconcile_node_availability`, the two
recovery paths. (b) Retire the capacity pass and block a partial plan's
remainder as a blocked eval (spot-check calls #21's open follow-up).

**Trade-off.** A free while something is waiting now queues the pass from two
places (the report handler and the wake loop); the pending-pass check
collapses them to one eval. The wake loop runs on its 50 ms coalesce, so the
pass from a recovery arrives up to that much later than an inline call would.
The pass still re-plans every short job, not only the ones the event could
help.

**Why (simplicity).** The capacity-event log is already the one place every
source of room is recorded, on every replica, with leader replay. Hooking it
once means a new source of room is covered without remembering a call site;
(a) would have fixed the one named gap and left the next one. (b) is the right
end state but is a scheduler change with its own benchmark.

**A consequence, also decided here.** With passes now queued more often, the
pass's old failure handling showed: a job it re-planned that still placed
nothing got a 5-second `job_reschedule` retry, and that queued eval made the
*next* pass skip the job, so a runtime enable or a recovery right after did
not place it until the delay ran out. The capacity pass now leaves a
no-room result alone (nothing was removed, and the plan already set
`capacity_waiters` again); only other failures get the retry. The node-loss
and drain passes keep the retry, since they removed the allocations first.

## 3. Unplaced work is a warn-level `plan.unplaced` event, per eval and per capacity-pass replan

**The call.** A plan that applied and left tasks unplaced writes one
`plan.unplaced` warn event: the job, the eval when there is one, and per task
its name, config id, count and the planner's reason. Eval handlers emit it
from `outcome.unplaced`; `reconcile_job_config_allocations` (the capacity pass
and node-loss replans) emits it too. Public surface:
[event-stream.md](../../docs/event-stream.md).

```text
warn plan.unplaced Job agents left 1 allocation(s) unplaced: workspace (1): Failed to place task workspace: ...
```

**Alternatives.** (a) Only eval handlers, as the TODO suggested. (b) Raise
`eval.completed` to warn when its outcome has a shortfall. (c) Add `count.per`
tasks to the pending/capacity-demand view.

**Trade-off.** A job that stays short emits the event again on every capacity
pass that re-plans it, so a busy cluster with a long-short job writes one
warn per pass. The journal is bounded, so this costs history depth, not
growth.

**Why (simplicity).** Envelopes reach new nodes through the capacity pass, so
(a) would miss exactly the case the TODO was about. (b) mixes "this eval
finished" with "this job is short" in one kind, and a filter on level would
then match every routine completion that happened to leave a dependency held.
(c) changes what node autoscaling sizes against: a `count.per` task follows
nodes, and counting a full node's missing envelope as demand would ask for a
node the envelope would also follow onto. Left as is.

## 4. A session start with no envelope quotes the recorded reason, and keeps its code

**The call.** When no node runs an envelope for the task, the refusal reads
the reason the scheduler recorded (the job's blocked eval if it blocked on
that task, else the newest `plan.unplaced` event naming it) and quotes it:

```text
FAILED_PRECONDITION no node runs an envelope for agents/workspace: the scheduler
could not place one (Failed to place task workspace: ...); it retries when
capacity frees up or a node joins (see `legion events list --kind plan.unplaced
--job agents`)
```

Without a recorded reason it keeps the old text, now saying "and has room for
it" and naming the events query.

**Alternatives.** (a) Return `RESOURCE_EXHAUSTED` when the reason is a lack of
room. (b) Compute eligibility on the session path (constraints, capability,
free room) to say exactly which node is eligible and full.

**Trade-off.** The reason is the last one recorded, so after the cluster
changes it can describe a state that no longer holds until the next plan
records a new one. The event read is an unindexed walk of the bounded journal,
on the error path only.

**Why (simplicity).** The planner already wrote the exact sentence; reading it
back keeps one source of truth instead of a second eligibility computation on
the session path (b). The code stays `FAILED_PRECONDITION` because the reason
is not always room (a constraint or a port) and clients already branch on it.
