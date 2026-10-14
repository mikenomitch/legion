# Edge admission control: judgement calls

Calls made building [edge-admission-control-plan.md](../plans/edge-admission-control-plan.md).
Ordered by project impact, highest first. The number is the rank.

## 1. Nodes shed from their own ratify latency, not the managers

Each edge node watches how long its own ratifications take and refuses new
edge starts when they come back late. The managers do nothing new.

**Alternatives.** The plan's TODO named two sources. The managers could
refuse `RatifyClaim` with `overloaded` once their queue latency passes a
fraction of the lease, or publish an overload flag on heartbeat replies that
nodes obey.

**Trade-off.** Each node learns alone. A node that takes few starts learns
slowly, and a pool of N edge nodes always keeps at least N claims in flight,
one probe each. A manager has the best view of its own queue, and this
design ignores it.

**Why.** Only the node can refuse *before boot*. A manager hears about an
edge start when the node asks to ratify it, which is after the node admitted
it and started booting, so a manager-side refusal still kills a booted
workload. It only kills it sooner. A heartbeat flag lags by a heartbeat
interval and needs a new wire field. The node's own latency is end to end:
it includes the network, a manager failover, and the scheduler lane, and it
needs no wire change (performance and simplicity).

## 2. The window is AIMD over unratified claims, not a latency switch or a rate limit

The node caps concurrent unratified edge claims. The cap halves on a late
verdict and grows by `1 / window` on a timely one, TCP's control law.

**Alternatives.** An on/off breaker: refuse everything while recent ratify
latency is above the threshold. A rate limit on edge starts. A gradient
limiter like Netflix's concurrency-limits.

**Trade-off.** Recovery is additive. From a window of 1 back to 32 takes
about 500 timely verdicts. That costs little, because a concurrency window
only binds when latency is high (throughput per node is window over
latency), but a node does refuse more of a burst for a while after a slow
patch.

**Why.** A breaker has no good way to recover. Once it refuses everything it
gets no new latency samples, so it needs a timed probe, and the probe
re-admits a whole burst that the still-slow managers then drop. That is the
same oscillation TCP's slow start was designed out of. A rate limit is the
wrong unit: at healthy latency it caps throughput for no reason, and at bad
latency it does not know how far to drop. Gradient limiters need a stable
latency baseline, and ratification through a raft commit does not have one.
AIMD is self-probing (the floor of one), needs no baseline, and converges to
a fair share of the managers across nodes without coordination. In the model
a static cap of 32 alone collapses once N × 32 claims outrun the lease
(goodput 124 a second of 500 at 100 nodes and 3x load), and the window holds
goodput near capacity.

```
$ legion sandbox start ... -j
{ "ok": false, "error": { "code": "OVERLOADED", "retryable": true, ... } }
```

## 3. Only edge claims count, not manager-originated speculation or sessions

**Alternatives.** Count every unratified claim, which is what
`maxProvisionalPerNode` was documented to bound ("concurrent unratified
claims per node").

**Trade-off.** Manager-originated speculative dispatch still collapses under
load: a speculative claim whose apply outlives its lease dies on the node,
and its committed allocation boots again later. That is Phase 2.

**Why.** A manager-originated claim was placed by the scheduler, which
already checked the node's capacity. Capping it on the node would refuse a
plan that puts more than 32 speculative placements on one node, every time,
with a warning per refusal. Its verdict is the post-commit dispatch, and a
plan that is rejected or falls back to another candidate leaves the claim to
expire for reasons unrelated to load. Those expiries would cut the window
and refuse edge starts the managers could have ratified. A session is never
ratified at all, and the launch pool already bounds its runtime work
(simplicity).

## 4. `maxProvisionalPerNode` moves to the worker type's `edgeBoot` block

```nickel
# before: manager config            # after: worker type config
edgeBoot = {                        edgeBoot = {
  enabled = true,                     acceptClaims = true,
  maxProvisionalPerNode = 32,         maxProvisionalPerNode = 32,
}                                     overloadThreshold = 0.3,
                                    }
```

**Alternatives.** Keep it on the manager and mirror it to nodes on the
heartbeat reply or the dispatch, as the TODO suggested.

**Trade-off.** A config that set it on the manager no longer parses there.
It is now per worker type rather than one value for the cluster.

**Why.** It bounds what one node holds, so it should be sized per node
class, next to `launchConcurrency` and `queueTimeout`, which already live
there. Worker type config reaches the node at registration, before any edge
start can arrive. Mirroring from the manager needs a new wire field and
leaves the node unlimited until its first heartbeat reply, and two managers
with different configs would flip it back and forth. AGENTS.md says to move
fields freely rather than keep aliases (simplicity).

## 5. `overloadThreshold` defaults to 0.3 of the lease, not half

**Alternatives.** 0.5, the "half the lease" the TODO suggested and the
first default this work shipped with in the model.

**Trade-off.** A node cuts its window at 1.5s of a 5s lease rather than
2.5s, so a cluster whose ratifications legitimately take over 1.5s (a slow
cross-region quorum) sheds starts that would have ratified. The fix there is
a longer `ratifyTimeout`, which moves the threshold with it.

**Why.** A node only notices trouble at the threshold, and claims it admits
until then keep waiting while the queue grows. With offered load `r` times
what the managers ratify, the claims admitted at the moment of the cut wait
about `r × threshold`. At 0.5 that blows the lease once `r` passes 2, at 0.3
it holds to about 3.3. The model agrees: at 200 nodes and 3x load, 0.5
ratified 308 starts a second and killed 192 after boot, 0.3 ratified 449 and
killed 51. Healthy ratify latency is tens of milliseconds, so 30% of a
multi-second lease never fires by accident (performance).

## 6. A full window waits `queueTimeout` before refusing

**Alternatives.** Refuse at once when the window is full, which returns the
refusal faster.

**Trade-off.** An `OVERLOADED` refusal takes up to `queueTimeout` (1s by
default), and a start can wait twice: once for window room, once for a
launch slot.

**Why.** In a healthy cluster a burst larger than the window drains within
one ratify round trip, tens of milliseconds. Refusing at once would turn
every burst above 32 into refusals that a router has to retry. Reusing
`queueTimeout` keeps one knob for "how long a start may wait on this node".
The wait for room happens before the launch slot, so a start blocked on
ratification never holds a slot that sessions also need.

## 7. The refusal is `RESOURCE_EXHAUSTED` with the code `OVERLOADED`

**Alternatives.** `UNAVAILABLE`, which gRPC describes as transient and
retryable with backoff. A new `RefusalReason::Overloaded` sent as a stream
frame.

**Trade-off.** `RESOURCE_EXHAUSTED` reads as "a quota" to some clients, and
a caller has to read the `legion-error-code` metadata to tell this refusal
from a full launch pool.

**Why.** The refusal happens before admission, so there is no claim and no
stream to send a frame on, and a frame would arrive after the caller assumed
the start was admitted. `RESOURCE_EXHAUSTED` is already this endpoint's
"this node will not take more right now" (a full launch pool), the CLI
already marks it retryable, and gRPC lists "flow-control resource limits
reached" under it. A stable code in metadata is the pattern
`WORKER_UNREACHABLE` set, so the CLI and MCP envelopes carry
`code: "OVERLOADED"` with no new client code.

## 8. A failed ratify call counts as late, and a local failure counts as nothing

**Alternatives.** Judge every claim by its timing alone.

**Trade-off.** A transient transport error halves the window once.

**Why.** When the managers are unreachable, ratify calls fail fast. Judged
by timing, each fast failure would look timely and grow the window while
every claim it admits dies. A failed call means the managers cannot ratify
right now, which is the signal. A launch that fails on the node says nothing
about the managers, so it frees the slot and changes nothing. A refusal from
a manager is an answer and is judged by how fast it came.

## 9. An overdue claim counts as late before its verdict arrives

When the node next decides on a start, any claim past its threshold without
a verdict is counted late at once.

**Alternatives.** Learn only from verdicts as they arrive.

**Trade-off.** Each admission decision scans the in-flight claims, at most
`maxProvisionalPerNode` of them.

**Why.** Learning only from verdicts reacts a whole ratify latency late, up
to the lease, which is the window in which the collapse happens. The scan is
bounded and runs under a lock held for microseconds (performance).

## 10. Ported whole on top of PR #43, with the window ahead of the launch slot

**Call.** Nothing in the window was dropped when it moved onto the edge
path PR #43 shipped. `Worker.StartClaim` still reserves window room first,
then takes a launch slot, then admits.

**Alternatives.**
- (a) Drop the window and lean on what PR #43 added: the launch pool
  (`edgeBoot.launchConcurrency`, slot held until the start is ready) and the
  router's least-full fallback.
- (b) Take the launch slot first and check the window inside it.

**Trade-off.** A start can wait twice, once for window room and once for a
slot, each up to `queueTimeout`. That is calls entry 6, unchanged.

**Why (performance).** Neither PR #43 piece answers slow ratification. The
launch pool bounds runtime work in flight, so it frees a slot as soon as a
claim has booted, while the claim goes on waiting for its verdict. Under a
slow control plane the pool stays mostly free and the node keeps booting
work that dies at its lease. The least-full fallback lives in the manager's
session router, and sessions are never ratified. (b) would hold a slot,
which sessions on the same node need, while a claim waits on managers that
are behind.
