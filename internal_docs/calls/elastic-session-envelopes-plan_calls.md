# Judgement calls: elastic session envelopes

Calls made building [elastic-session-envelopes-plan.md](../plans/elastic-session-envelopes-plan.md)
Phase 1. Ordered by project impact, highest first: the entry number is its
rank.

## 1. The node decides when to resize, not a manager-side controller

**Call.** Each node runs the resize controller for its own envelopes and
asks the manager on its heartbeat. The manager only checks bounds and, for a
grow, free room.

```text
HeartbeatRequest.envelope_resizes = [{ allocation_id: 88, from: {"cpu":6000,...}, to: {"cpu":8000,...} }]
HeartbeatReply.envelope_resizes   = [{ allocation_id: 88, outcome: "applied", size: {"cpu":8000,...} }]
```

**Alternatives.**
- (a) A controller on the scheduler-lane leader that reads the session
  index and resizes envelopes, the VPA shape.
- (b) A controller on every manager for the nodes that heartbeat to it.

**Trade-off.** Resizes move no faster than heartbeats (2 s on fleets up to
2,500 nodes), and the policy ships in the worker, so changing it needs a
worker upgrade. A misbehaving worker can ask for any size inside its job's
bounds.

**Why (correctness, then simplicity).** A shrink is only safe if no session
is admitted past the new size between the decision and the commit. Sessions
start at the edge with no manager on the path, so only the node knows its
usage at every instant. (a) sees the index as of the last heartbeat and
would need a second round trip (tell the node to cap, wait for its ack,
then shrink). (b) has the same gap, and splits one decision across every
manager. With the node deciding, the cap is a local variable under the lock
that admits.

## 2. A resize is an in-place UPDATE of the envelope's allocation row

**Call.** The size lives in `allocation.resources`. A resize rewrites it
and bumps the node and job-config allocation epochs, in one scheduler-lane
entry, group-committed with other small writes.

**Alternatives.**
- (a) A second allocation per node for the elastic part, placed and
  stopped by the scheduler like any allocation.
- (b) Count elastic room outside the allocation table, in a per-node
  "borrowed" number the scheduler subtracts.

**Trade-off.** Legion now has an in-place allocation resize, which
sandbox-scaling calls entry 11 avoided. Anything that caches an allocation's
resources keyed by its id has to tolerate them changing. The read model,
the usage memo, and the work log already did (an UPDATE is an upsert), and
the epoch bump invalidates the persisted `node_alloc_usage` cache.

**Why (simplicity and correctness).** Every capacity reader already reads
the row: plan apply, preemption, `node usage`, the router, `session
nodes`. (a) doubles the envelopes in every listing, needs a placement pass
per grow, and makes the node track two ledgers for one task's sessions.
(b) puts capacity outside the plan-apply compare-and-set, which is how a
node gets overcommitted.

## 3. A shrink is guarded by the node capping its own admission first

**Call.** Before asking to shrink, the node caps admission at the target
and keeps the cap until the manager answers. The manager does not check a
shrink against usage.

**Alternatives.**
- (a) The manager checks the shrink against the session index.
- (b) Shrink only an envelope with no sessions at all.

**Trade-off.** While a shrink is in flight (one heartbeat, longer if the
manager is unreachable), the node admits against the smaller size even if a
burst arrives. The target keeps a quarter of the envelope free, so a burst
in that window has room, and a larger one is a retryable refusal.

**Why (correctness).** (a) is the race entry 1 rejects. (b) almost never
fires on a busy node and would hold the peak size all day.

## 4. Fixed thresholds (90, 75, 50) and one knob, `shrinkAfter`

**Call.** Grow at 90% fill to 75%, shrink below 50% (held for
`shrinkAfter`) to 75%. Only `max` and `shrinkAfter` are in the jobspec.

```nickel
envelope = { resources = { cpu = 6000, memory = 6144 }, max = { cpu = 30000, memory = 30720 }, shrinkAfter = "10m" }
```

**Alternatives.**
- (a) Expose `headroom` (the target fill) and the grow threshold.
- (b) Headroom in sessions rather than a fraction.

**Trade-off.** A platform with very bursty arrivals cannot ask for more
free room per envelope without raising the floor. A diurnal simulation of
the shipped controller (plan §9) shows that with `T = 0.8` instead of 0.75
an envelope strands about 15% less room at about 1.7 times the resizes, so
the knob would move numbers, not outcomes.

**Why (simplicity).** The floor already buys instant burst room, and it is
a number operators understand. Every threshold is a knob someone has to
reason about at 3am, and a fraction is the same for a 4-session envelope
and a 400-session one. Revisit when a platform shows refusals during grows
that a larger floor would not fix.

## 5. A grow takes what fits, and never preempts

**Call.** A grow is checked against the node's free room with the
envelope's current size given back, and takes what fits on each key
(`partial`). It refuses (`refused`, no write) when nothing fits, and never
evicts other work.

**Alternatives.**
- (a) All or nothing.
- (b) Let a grow preempt lower-priority allocations, as a placement would.

**Trade-off.** A partial grow can leave keys unevenly grown (more memory
than CPU) when one resource is short. A grow on a full node waits for room
to free up rather than taking it, so sessions on a busy node are refused
while lower-priority work keeps running.

**Why (simplicity).** (a) refuses grows that would mostly help. (b) makes
every resize a preemption decision, with victims, stop notices, and
requeues, driven by a node's heartbeat, and a burst of sessions could evict
keyed sandboxes. Preempting work to grow belongs in the scheduler's
planner, if anywhere (plan Phase 2).

## 6. A replacement envelope starts at its floor

**Call.** A job update places each new envelope at `envelope.resources`,
like any placement, and the node grows it on its next heartbeat.

**Alternatives.**
- (a) Carry the outgoing envelope's size into its replacement's placement
  on the same node.
- (b) Resize envelopes in place on an update instead of replacing them.

**Trade-off.** Between the update and the node's next grow, the node's
sessions can hold more than their envelope, and a placement in that window
can overcommit the node until sessions end. A refused grow (the room was
taken) leaves the node overcommitted for longer. Stated in the public
doc's limitations, and a TODO.

**Why (scope).** (a) needs plan apply to look up a sibling allocation on
the node and size a placement from it, special-cased for one runtime,
inside the scheduler's hottest transaction. (b) changes how rollouts
treat envelopes. A fixed envelope already has a shorter window of the same
kind (old stopped, new not yet placed). Revisit with Phase 2, which needs
the scheduler to understand an envelope's elastic part anyway.

## 7. Payload sizes lose to the node's own record for 60 seconds after a resize

**Call.** After any answer, the node ignores the size in desired-set
payloads for 60 seconds, then trusts them again.

**Alternatives.**
- (a) A resize version on the allocation row, compared on the node.
- (b) Always trust the payload.

**Trade-off.** A worker that restarts within a shrink's commit, and whose
first desired set comes from a lagging replica, can admit past the shrunk
size for one heartbeat. Stated in the public doc's limitations.

**Why (simplicity).** (a) is a migration and a new column for a window of
milliseconds of replica lag. (b) lets a follower's lagging desired set undo
a shrink the leader just committed, which is the overcommit the cap exists
to prevent.

## 8. Shrinks emit a `freed` capacity event from the changeset

**Call.** The changeset projection emits `freed` for any allocation UPDATE
that lowers an amount in `resources`, which also covers a suspend dropping
an instance to its reserve.

**Alternatives.**
- (a) Emit from the resize op itself.
- (b) Emit on every allocation epoch bump.

**Trade-off.** The projection parses the old and new `resources` of every
allocation UPDATE that changes them. Status-only updates, the common case,
carry no `resources` column and cost nothing.

**Why (correctness).** Capacity events are derived on every replica from
changesets so a new scheduler-lane leader can replay them. (a) would only
reach the leader that ran the op. (b) wakes blocked work on every placement.

## 9. On the reserving router, the "growing" hint keys off the task, not each envelope's room

**Call.** Ported onto the router PR #43 shipped (room reserved at the pick,
a first attempt that asks the least full node even when the view says every
envelope is full). Both of its room refusals, "every envelope ... is full"
and "no node accepted the session", end with the elastic hint whenever the
task sets `envelope.max`:

```text
no node accepted the session (n1: ...; n2: ...); envelopes below
stateful.envelope.max grow within a heartbeat as their nodes report the load,
so retry in a few seconds, or raise stateful.envelope.max
```

**Alternatives.**
- (a) Keep the branch's check: give the hint only when some envelope in the
  router's view is below its ceiling.
- (b) Leave the router's messages alone.

**Trade-off.** A task whose envelopes have all reached `max` still says
they grow. The rest of the message names `envelope.max` as the knob to
raise, which is the right advice there too.

**Why (simplicity).** On the new router the view check had nowhere to live.
`reserve` no longer exposes the view, and with the least-full fallback the
"every envelope is full" branch is all but unreachable. The refusal callers
actually see comes after the nodes said no, where the view is a second
behind them anyway. (b) would keep telling an elastic task to raise
`envelope.resources`, its floor, which is the wrong knob. The reservation
accounting needed no change: in-flight starts are kept apart from the view,
and a resized row reaches the view on its next one-second refresh.
