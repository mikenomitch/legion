# Judgement Calls: Liveness Pings

Decisions made while planning and building
[`../plans/liveness-ping-split-plan.md`](../plans/liveness-ping-split-plan.md)
(status SHIPPED 2026-11-12).

**Ordered most-impactful first. The entry number is its rank.**

---

## 1. A ping is its own unary RPC, not a frame on `StreamWork` and not HTTP/2 PING

**Call.** Liveness travels as `Manager.Ping(PingRequest) -> PingReply`, a
unary call on the worker's existing channel, authenticated per call exactly as
a heartbeat is.

```proto
rpc Ping (PingRequest) returns (PingReply);
```

**Alternatives.**

- **A `ping` frame on `StreamWork`.** The stream is already open and already
  authenticated, so a frame skips the per-call HTTP/2 HEADERS, HPACK, tower
  stack, and credential check. It is the cheapest transport there is.
- **HTTP/2 PING / TCP keepalive.** Free, and already on.

**Trade-off.** A ping pays the per-call transport cost a frame would not, and
measured, that is most of it: 116 µs of client and server CPU per ping over
loopback against a 0.6 µs handler (an idle full heartbeat: 199 µs and 45 µs).
End to end a ping is only ~1.7x cheaper than the cheapest full heartbeat. That
is what holds the default `pingRatePerSecond` at 10,000 (#4), and so a 25s
window at 20,000 nodes instead of 10s.

**Why.** *Simplicity, then correctness.* A stream frame ties liveness to the
stream being up: a worker whose stream is down (a manager restart, a network
blip, `--sim-no-work-stream`) would need a second liveness path, and the
manager would have to know which path each node is on to size its window. A
ping on the unary channel works the same whether or not the stream is up, rides
the existing manager-failover bookkeeping, and the worker's self-fence clock
gets an answer from the manager that recorded it. HTTP/2 PING is answered by
the transport even when the worker's loop is wedged, carries no served
intervals, and is invisible to the self-fence clock, so it proves the wrong
thing. Moving the ping onto the stream later is a worker-and-handler change
with no surface change, and is filed as a deferral.

## 2. The lost window follows the ping interval, and `next_interval_seconds` keeps meaning it

**Call.** `HeartbeatReply.next_interval_seconds` is now the LIVENESS interval
(what windows derive from), and the full-heartbeat interval is a new field,
`sync_interval_seconds`. Both replies carry both.

**Alternative.** Keep `next_interval_seconds` as the full-heartbeat interval
(its old meaning, "how often to heartbeat") and add `ping_interval_seconds`.

**Trade-off.** A worker from before pings reads `next_interval_seconds` as
its heartbeat interval, so against a new manager it sends FULL heartbeats at
the ping cadence: correct, but the full-heartbeat load the controller sized
for is exceeded until workers are upgraded.

**Why.** *Safety over load.* With the alternative, an old worker would beat
at the slow full-heartbeat interval while its window was sized from the fast
ping interval, and every old worker on a large fleet would be marked lost.
The chosen shape fails toward load, never toward false loss, which is the
same rule adaptive cadence already follows ("a worker is never penalized for
honoring the served value").

## 3. One worker loop decides ping or heartbeat per tick

**Call.** The worker keeps one loop on the liveness interval; each tick sends
a full heartbeat when one is due and a ping otherwise
(`full_heartbeat_due`: `since_last_full + liveness/2 >= sync`).

**Alternative.** Two independent loops: a ping loop on the liveness interval
and the existing heartbeat loop on the sync interval.

**Trade-off.** A slow full heartbeat delays the next ping by however long it
took, where an independent ping loop would keep its own beat.

**Why.** *Simplicity, and the liveness signal means something.* With equal
intervals the single loop sends no pings at all, so every fleet under the
back-off point behaves exactly as before with no special case. A worker
stuck mid-heartbeat (a held task-manager lock, a hung reconcile) stops
pinging too, so the manager detects a wedged worker instead of a separate
ping loop vouching for it. And one loop owns the self-fence clock, the
re-phase, and the failover bookkeeping, so there is no race between two
writers.

## 4. `targetRatePerSecond` stays at 2,500; detection moves to a new `pingRatePerSecond` (10,000)

**Call.** The TODO this closes proposed rerunning 20k nodes at 5,000 and
10,000 and raising the default `targetRatePerSecond`. Instead the default
stays, and detection is sized from a separate rate defaulting to four times it.

```nickel
heartbeat = {
  targetRatePerSecond = 2500,    # full heartbeats: unchanged
  pingRatePerSecond   = 10000,   # liveness: new
}
```

**Alternatives.** Raise `targetRatePerSecond` to 5,000 (20k nodes: 8s, 40s
window) or 10,000 (5s, 25s window), as the TODO planned. And, for the ping
rate itself, 25,000 (ten times the target): the first default written, which
keeps the 10s window to 25,000 nodes.

**Trade-off.** At 10,000 a 20k-node fleet gets a 25s window, not 10s. Reports
(health transitions, failures) still reach the manager at the 21s sync
interval there; raising the target would have brought that to 8s or 5s. And
the default rests on the in-process and loopback measurement, not a rig run.

**Why.** *Performance, by measurement.* Raising the full-heartbeat rate buys
detection at full-heartbeat prices: ~100 µs of manager CPU per idle beat
(10,000 a second was a core of the rig's leader), more on a busy node. A ping
costs about 60 µs of it, nearly all transport, so 25,000 (up to 12,500 pings a
second, ~0.75 core) would have spent six times the CPU the full-heartbeat
default deliberately budgets. 10,000 caps pings at 5,000 a second, about a
third of a core spread over the managers workers dial, and still cuts the
20k-node window from 105s to 25s and holds 10s to 10,000 nodes. Report latency
is the "start reports wait for a heartbeat" TODO's problem, better solved by
moving reports onto the stream than by heartbeating harder. The rig rerun is
filed in the AWS rig plan's campaign runs; it, or pings on `StreamWork` (#1), can raise either rate.

## 5. A ping recovers a lost node and asks for a full heartbeat (`sync_now`)

**Call.** A ping from a node marked lost runs the same replicated
`lost -> active` transition a heartbeat does (availability restored, cordon
untouched, `legion_nodes_recovered_total{path="heartbeat"}`), and its reply
sets `sync_now` so the worker sends a full heartbeat immediately.

**Alternative.** Pings only refresh active nodes; a lost node stays lost until
its next full heartbeat recovers it.

**Trade-off.** A ping is no longer always memory-only: the recovering one
commits, on the blocking pool, like a heartbeat.

**Why.** *One disposition rule.* The ping and the heartbeat share
`beat_disposition`; making a ping refuse recovery would add a third state and
keep a returned node unschedulable for up to a sync interval (21s at 20k nodes)
for no benefit. `sync_now` closes the gap the other way: the manager hears the
node's allocations at once.

## 6. Freshness is judged per node, against the interval its window records

**Call.** A node is fresh when its age is within 1.2x its OWN liveness
interval, recovered as `window / missedBeatsBeforeLost`. The fleet ratio and
every pool ratio use the same rule.

**Alternative.** Keep judging against the one served interval, and only add a
per-pool breakdown.

**Trade-off.** A window an operator set by hand that is not a multiple of
`missedBeatsBeforeLost` is rounded down to whole seconds.

**Why.** *Correctness for free.* Against one served interval a 60s batch pool
reads as late on every tick, so per-pool numbers would just repeat the
mistake per pool. The window is already the per-node truth the lost decision
uses; deriving the interval from it needs no new state and keeps the ratio
and the lost decision in the same units.

## 7. Per-pool freshness is a new metric family, with series only for measurable pools

**Call.** `legion_heartbeat_pool_freshness_ratio{pool}` sits beside the
unlabeled `legion_heartbeat_freshness_ratio`. Only pools with 20+ active nodes
have a series, and a pool's series is dropped (not zeroed) when it stops
qualifying.

**Alternative.** Add a `pool` label to the existing gauge.

**Trade-off.** Two families to alert on, where one labeled family could have
served `min()` and `sum()` alike.

**Why.** *Simplicity for the operator who already alerts.* Existing alerts and
dashboards on the unlabeled ratio keep working, and the fleet figure stays
one series instead of being recomputed from pool series. Dropping rather than
zeroing matters because 0 is the alarming value: a pool that shrank under the
floor must not read as "nobody is on time". Needed `FloatGaugeVec::replace_all`
in `crates/common/src/telemetry.rs`.

## 8. One warning, on the lowest of the fleet and pool ratios, naming the pool

**Call.** The debounced warning compares the minimum of the fleet ratio and
every measurable pool ratio with 0.5, and when a pool is the lower one, the
message names it ("30% of pool latency (40 nodes, 2s interval) ... (99% of the
whole fleet)").

**Alternative.** A separate per-pool warning with its own debounce.

**Trade-off.** Two pools falling behind at once produce one line naming the
worse one; the CLI and the metric show both.

**Why.** *One page, not two.* The warning exists to reach an operator before a
collapse; two debounced streams for one control-plane problem would double the
noise without adding a decision.

## 9. The ping's fast path stays on the async worker

**Call.** An active node's ping is handled inline: one read-model lookup and
one liveness-table write, no `spawn_blocking`. Only a ping that may commit
takes the blocking-pool hop.

**Alternative.** The heartbeat's shape: everything on the blocking pool.

**Trade-off.** A read-model lock contended by a writer would briefly hold an
async worker.

**Why.** *Performance.* The hop costs more than the work it would protect; the
heartbeat takes it because its writes may park a thread for a quorum round,
and the fast path of a ping never writes.

## 10. The graceful-stop drain window follows the sync interval

**Call.** `set_drain_window` gets the sync interval, set before each full
heartbeat, instead of the beat interval.

**Alternative.** Keep the beat (liveness) interval.

**Trade-off.** On a fleet whose full heartbeat has backed off, stops drain
longer (up to the sync interval) than the beat would suggest.

**Why.** The window is "how long every other node needs to learn an endpoint is
gone", and endpoint feeds ride the full heartbeat (or a stream push), never a
ping. Draining for the liveness interval would cut connections before peers
heard.
