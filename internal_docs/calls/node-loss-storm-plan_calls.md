# Judgement calls — node-loss storms

Plan: [node-loss-storm-plan.md](../plans/node-loss-storm-plan.md).
Ranked by project impact, highest first.

## 1. Reschedule evals run one band below the default, not at it

**The call.** `node_lost_reschedule` evals enqueue at
`default_priority − NODE_LOST_RESCHEDULE_PRIORITY_DROP` (50 − 10 = 40 at
the defaults), where they used to inherit the plain default.

**The alternatives.** (a) Keep equal priority and add cross-kind fairness
to `claim_eval` (round-robin across eval kinds). (b) Inherit the affected
*jobs'* band priorities, like Nomad's node-update evals.

**The trade-off.** During a storm, recovering workloads now wait behind
new submissions instead of the reverse. A dead allocation's replacement is
delayed by however deep the submission stream is — where before, a
*submission* was delayed by however deep the storm was (measured: ~6 s per
job behind 2,752 reschedules). (b) was rejected because a reschedule eval
is per-*node* (the handler owns the stateful-loss confirmation gates), so
there is no single job whose band it could inherit without splitting the
eval per job — a bigger reshape for the same outcome. (a) is a scheduler
semantics change that affects every eval kind to fix one; deferred until a
case the band cannot express shows up.

**Why it won.** Recovery is bulk background work the monitor re-drives
every tick; a submission is a person (or agent) waiting on an RPC.
Performance and simplicity both point the same way: the queue already
orders by priority, so one constant fixes the starvation with no new
mechanism. User-facing surface: none directly — `legion eval list` shows
reschedules at priority 40, and an operator's explicit `--priority` still
beats everything it beat before.

## 2. The empty-node filter runs at enqueue time, trusting the recheck to catch races

**The call.** `enqueue_lost_node_reschedules` skips nodes with no
allocation rows (one chunked indexed query per batch), instead of letting
the handler discover emptiness after the eval has been through three
quorum rounds.

**The alternative.** Keep enqueueing unconditionally — the handler's
`allocs.is_empty()` early-return is already correct, just late.

**The trade-off.** A plan in flight during the loss can commit an
allocation onto a node *after* the filter ran, so a storm's first tick can
skip a node that then has something to reschedule. The recheck loop
re-runs the filter every tick, so the miss lasts at most one tick (2 s by
default); the handler's own early-return stays as the backstop.

**Why it won.** On the measured storm most lost nodes were empty, and each
empty eval cost an enqueue round plus a claim plus a terminal write on the
scheduler lane. A 2 s worst-case delay on a path whose stateful half
already waits out `onLost.replaceAfter` windows is noise; thousands of
no-op quorum rounds are not.

## 3. Batched enqueue falls back to per-node forwarding instead of growing the wire enum

**The call.** `enqueue_evals` (one scheduler-lane round) is tried first;
a `NotRaftLeader` rejection falls back to the per-node forwarding-aware
`enqueue_eval` loop. No `EnqueueEvals` variant was added to
`ForwardableLaneWrite`.

**The alternative.** Add the batched variant to the forwarding enum so the
split-leadership case is also one round.

**The trade-off.** Under `lanes` topology with the status and scheduler
lanes led by different managers, a storm still pays one forwarded round
per node — the pre-change cost, in the corner where it is least likely to
matter (the fallback exists at all only for split leadership, which is
transient).

**Why it won.** `ForwardableLaneWrite` is a closed wire enum versioned
with the manager protocol; a new variant is a cross-version message that
an in-place downgrade cannot decode. Extensibility cuts against paying
that for a transient corner: if split-leadership storms show up in a
campaign, the variant can be added then, with the version-skew note it
deserves.

## 4. Recovery is counted where the leader observes it, not in the replicated apply

**The call.** `legion_nodes_recovered_total` increments in
`restore_availability_after_loss` (heartbeat path) and `insert_node`
(re-registration path) — leader-side wrapper code — not inside the
replicated apply functions.

**The alternative.** Count inside the applies, so followers' counters
move too.

**The trade-off.** Followers report 0 recoveries, so the counter must be
read on (or summed across) leaders — same as `nodes_marked_lost_total`,
which counts in the status checker.

**Why it won.** Symmetry with the metric it exists to be subtracted from:
`marked_lost` counts on the leader that decided the loss, so `recovered`
counts on the leader that observed the return. Counting in the apply
would also make replicated state transitions carry metrics side effects,
which the capture/apply machinery otherwise avoids.
