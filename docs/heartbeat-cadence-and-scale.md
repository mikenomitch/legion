# Heartbeat Cadence and Scale

Every worker checks in with the manager on an interval. This page covers how
that interval is chosen, what happens when there are more workers than the
control plane can answer, how to size for that ahead of time, and how Legion
keeps a busy control plane from being mistaken for a dead fleet.

Related: [Node liveness](node-liveness.md) covers where liveness state lives
and what survives an election. This page covers how often a worker checks in
and how long it may be silent.

---

## The one relationship

The model in one sentence:

> The manager tells workers how often to check in, and a node is lost after
> five missed check-ins.

The rest of this page follows from that.

```
lost window = missedBeatsBeforeLost x that node's liveness interval
```

At the defaults (`5 x 2s`), the window is 10 seconds. A check-in is either
a full heartbeat or a [ping](#pings-and-heartbeats), a much cheaper call
that only proves the worker is alive. A cluster that sets nothing runs
[adaptive cadence](#adaptive-cadence): up to 2,500 nodes every check-in is a
full heartbeat every 2 seconds, as in earlier releases. Past that, the
manager slows the full heartbeat down and workers ping in between, so the
2-second interval and the 10-second window hold up to 10,000 nodes. Past
10,000 the manager slows the pings too, more gently than the full
heartbeat, and widens each window to match: 25 seconds at 20,000 nodes,
where it would be 105 without pings.

The window is per node. It is stored on the node's own record instead of
being one global constant. A cluster with a 2-second latency-sensitive pool
and a 60-second batch pool gets a 10-second window for the first and a
300-second window for the second. This means you can raise
`heartbeatSeconds` to relieve manager load without losing detection margin.
Under a fixed 10-second window, a 13-second cadence would mean one missed
beat marks a node dead.

### What a beat carries, and what it no longer gates

The interval decides liveness, not delivery. A worker receives its work over
`Manager.StreamWork` ([Delivery](architecture-managers-workers.md#delivery)):
a placement reaches it one round trip after it commits, so stretching the
interval to relieve the manager no longer delays new work. The same holds
for suspend, resume, and fork: checkpoint requests are pushed on the stream,
and a worker reports a finished capture, restore, publish, or warm on a
heartbeat it sends early
([How it works](suspend-and-resume.md#how-it-works)).

Those early beats sit outside the cadence, and adaptive cadence does not
count them. They cost a beat per finished piece of checkpoint work, two per
suspend and resume, so they add the cluster's checkpoint rate to `N / I`. That rate
is bounded by the scheduler lane, which on the reference host completes
about 50 suspend-and-resume lifecycles a second: about 100 early beats a
second at the ceiling, small next to the scheduled beats of a fleet large
enough to need adaptive cadence.

The beat's size does not grow with what the node runs either. A worker on
the stream reports its stream position, a digest of the allocation ids it
holds, and only the allocation health that changed since the manager last
acknowledged it; the reply leaves the desired set out. A steady beat from a
300-allocation node is a 42-byte request and a 7-byte reply, and costs the
manager 0.5 ms (debug build), where the full desired set was 5,154 bytes,
102,670 bytes, and 26 ms. Measured before the stream at 1,000 and 10,000
allocations per node (release build), the full set was 17,761 and 179,762
request bytes, 439,771 and 4,408,772 reply bytes, and 27 and 344 ms of
manager time per beat. A beat whose health did not change writes no rows.

A worker whose stream is down reports no position and gets the full desired
set, as every worker did before the stream; sizing that case is what the
rest of this page still describes.

---

## Pings and heartbeats

A full heartbeat does two jobs. It tells the manager the node is alive, and
it carries the node's reports (allocation health, failures, volume and
checkpoint outcomes) and fetches what the manager has for it (endpoint and
policy feeds, claim verdicts, stop notices). The second job is what costs
the manager real work, so on a large fleet adaptive cadence slows the full
heartbeat down. Before pings, the lost window slowed down with it: a
20,000-node fleet beat every 21 seconds and took up to 105 seconds to notice
a dead machine.

A ping (`Manager.Ping`) is the first job alone. The manager answers it from
memory: it checks the node's credential, looks the node up, and records the
beat. There is no store write, no hop to the blocking pool, and nothing in
the reply but the two intervals. So the manager serves two intervals:

| Interval | Sized from | What it governs |
| --- | --- | --- |
| **Liveness** | `pingRatePerSecond` (10,000/s) | How often a worker checks in, with a ping or a full heartbeat. Every lost window derives from it. |
| **Sync** | `targetRatePerSecond` (2,500/s) | How often a worker sends a full heartbeat. Never shorter than the liveness interval. |

The worker runs one loop on the liveness interval. On each tick it sends a
full heartbeat if one is due (the sync interval has passed since the last
one) and a ping otherwise. While the two intervals are equal, which at the
defaults is every fleet up to 2,500 nodes, every tick is a full heartbeat
and the worker sends no pings at all. At 10,000 nodes it beats every 2
seconds and sends one full heartbeat about every 8 seconds, so three beats in
four are pings.

A worker still sends a full heartbeat early when it has something the
manager is waiting on (a stream message that stopped an allocation, a
finished checkpoint), as before.

What a ping covers:

- **Detection.** A ping counts as a beat for the lost decision, the
  freshness ratio, and the liveness digest a non-leader manager sends the
  leader. A node that pings is alive.
- **The self-fence.** A successful ping resets the worker's stateful
  self-fence clock just as a full heartbeat does. The manager has recorded the
  node as alive either way.
- **Recovery.** A ping from a node marked lost brings it back, exactly as a
  heartbeat does (replicated, schedulable again, a cordon left alone), and
  its reply asks the worker for a full heartbeat at once (`sync_now`), so the
  manager hears the node's allocations now and not one sync interval later.
- **Unknown nodes.** A ping from a node the manager does not know is refused
  with `NotFound`, like its heartbeat, and its self-fence clock keeps running.

What a ping does not cover: anything a full heartbeat reports. Allocation
health, failures, and endpoint feeds move at the sync interval, or sooner
over `StreamWork` and early beats. A ping goes out from the same loop as the
full heartbeat, so a worker stuck in the middle of one stops pinging too and
is detected as it would be without pings.

### What a ping costs

Measured with `tests/integration/heartbeat_ping_measure.rs` (release build,
one process on a 4-vCPU container, a node with no allocations, so the full
heartbeat is the cheapest one there is):

| Per call | Ping | Full heartbeat |
| --- | ---: | ---: |
| Manager handler CPU, in-process | 0.6 µs | 45 µs |
| Client + server CPU over loopback gRPC | 116 µs | 199 µs |
| Calls/s over loopback, 16 callers | 19,500 | 13,100 |

The handler is about 70 times cheaper, but transport is most of what a ping
costs: HTTP/2 framing, protobuf on both ends, and the credential check are
paid per call whatever the call does. End to end a ping is about 1.7 times
cheaper than the cheapest full heartbeat, and the gap grows with what the
node runs, since a full heartbeat's handler work does and a ping's does not.
Roughly half of the loopback figure is the server's, so a manager spends on
the order of 60 µs per ping against about 100 µs per idle full heartbeat,
which matches the rig's ~10,000 full heartbeats a second for a little over a
core.

That is why the default `pingRatePerSecond` is four times
`targetRatePerSecond` and not more. At its ceiling (5,000 pings a second) the
ping load is about a third of a core, spread over the managers the workers
dial, against about an eighth of a core for the full-heartbeat ceiling.

To turn pings off, set `pingRatePerSecond` equal to `targetRatePerSecond`.
The two intervals are then always equal, every beat is a full heartbeat,
and the lost window backs off with the full heartbeat as it did before.

---

## Sizing a control plane

Adaptive cadence does this arithmetic for you from `targetRatePerSecond`.
The formula matters when you size a control plane, choose a lower target for
a small one, or run with `adaptive = false`.

```
sustainable_nodes  ~=  R x I

  R = heartbeat commits per second this control plane sustains
  I = heartbeat interval in seconds
```

And its inverse, with a headroom factor `H`. The default is 2, which runs at
half the measured ceiling so a burst does not cause an outage:

```
heartbeatSeconds  >=  H x N / R
lost window        =  missedBeatsBeforeLost x heartbeatSeconds
```

`R` is not a constant, so measure it on your own control plane. Per-manager
throughput is roughly `worker_threads / heartbeat_handler_latency`. A
steady-state heartbeat no longer commits to the store (see
[Node liveness](node-liveness.md)), so `R` depends on the gRPC handler, not
the disk. Two cases still pay the older per-commit fsync cost: a cluster in
the mixed-version window of an upgrade, and one whose status-lane leader
predates liveness digests.

### The measurement recipe

1. Point a simulated fleet at the real control plane:
   `legion start worker <TOKEN> --simulate N`. The simulated fleet shares the
   real cadence-resolution and jitter helpers, so it beats the same way a real
   worker does.
2. Ramp `N` and watch `legion_heartbeat_freshness_ratio` (or run
   `legion node cadence`).
3. `R` is the largest `N / I` at which the ratio holds at or above 0.95.
4. Cross-check against `legion_raft_propose_apply_seconds{lane="status"}` and
   `legion_store_coalesce_batch_size{path="heartbeat"}`. If either is
   climbing, the ceiling you found is the commit path, not the handler.

### The measured table

This table applies the formula above with a fixed `heartbeatSeconds`
(`adaptive = false`). It uses `R ~= 80`, measured on an AWS control plane
(3x m7i.xlarge, 3-voter `lanes`, EBS gp3) before the liveness rework, when
every heartbeat still committed to raft. It is the floor for a cluster still
paying that cost, such as one mid-upgrade. A current release answers a
heartbeat from memory, and the same rig handled about 10,000 heartbeats/s at
20k nodes for a little over one core of a 4-vCPU leader, so `R` is now in the
thousands. Measure your own:

| Fleet | Min interval (H=2) | Set `heartbeatSeconds` | Resulting lost window |
| ---: | ---: | ---: | ---: |
| 100 | 2.5s | 3 | 15s |
| 160 | 4s | 4 | 20s |
| 250 | 6.3s | 7 | 35s |
| 500 | 12.5s | 13 | 65s |
| 1000 | 25s | 25 | 125s |
| 2000 | 50s | 50 | 250s |

The rows above 3 seconds are safe only because the lost window grows with the
cadence.

Two limits apply to the table:

- `heartbeatSeconds` is clamped to `1..=300`.
- The manager refuses to start if `missedBeatsBeforeLost x
  heartbeatSeconds` reaches the stateful self-fence
  (`stateful.onLost.selfTerminateAfter`, 90s by default). Otherwise a healthy
  worker could kill its own stateful tasks between beats. Legion catches this
  at config load so you don't discover it in production. See
  [Stateful workloads](stateful-workloads.md).

---

## Is the fleet keeping up?

### The freshness ratio

```
freshness_ratio = active nodes that beat within 1.2 x their own liveness interval
                  ──────────────────────────────────────────────────────────────────
                  all active nodes
```

`1.0` means every node checked in on time. The `1.2` covers the +-10%
steady-state jitter plus some slack. A ping and a full heartbeat both count
as a check-in. Each node is judged against its own interval, the one its
lost window records, so a 60-second batch pool is on time at 50 seconds of
silence instead of reading as late against a 2-second interval.

False node loss begins when the ratio falls below
`served_interval / lost_window`. At the defaults, that is `2s / 10s = 0.20`.
The ratio and its danger threshold use the same units, so you can reason about
the number directly.

The ratio comes from the store's view of the fleet, not from an RPC counter.
Under `lanes` topology, a per-process `legion_heartbeats_total` rate only
shows one manager's share. The ratio is suppressed below 20 active nodes,
where it is too noisy to be useful.

### Per pool

One ratio for the whole fleet blends its pools: a 200-node latency pool
falling behind can hide inside a 5,000-node batch pool that is keeping up,
because the blended figure stays above the warning line while every node in
the small pool is a few beats from being marked lost. So the same ratio is
reported per node pool:

- `legion_heartbeat_pool_freshness_ratio{pool}`, for every pool with 20 or
  more active nodes;
- a pool table in `legion node cadence` (below) and a `pools` list in its
  JSON and in `GetHeartbeatCadence`;
- the warning, which fires on the lowest measurable ratio, fleet-wide or in
  any one pool, and names the pool when it is a pool that tripped it.

### `legion node cadence`

```console
$ legion node cadence
Fleet             10000 nodes
Liveness interval 2s  (adaptive)
Full heartbeat    every 8s  (pings in between)
Lost window       10s  (5 missed beats)
Freshness         0.99  (false loss below 0.20)
Sustainable       ~9900 nodes at 2s on this control plane
Suggested         heartbeatSeconds: 5   (lost window 25s)

POOL                   NODES  INTERVAL  LOST WINDOW  FRESHNESS
batch                   2000       60s         300s       1.00
default                 7960        2s          10s       0.99
latency                   40        2s          10s       0.30
```

The full-heartbeat line reads `(every beat; no pings)` while the two
intervals are equal. With `adaptive = false`, the liveness line reads
`(workers use their worker-type heartbeatSeconds)` and there is no
full-heartbeat line. The pool table appears when there is more than one
pool; a pool under 20 nodes shows `-` for freshness.

`--json` / `-j` emits the same fields through the standard envelope. There is
also a `legion_node_cadence` MCP tool.

You can ask any manager. A manager that does not lead the status lane does not
own liveness, and says so instead of guessing:

```console
$ legion node cadence
This manager does not own node liveness (it does not lead the status lane).
Ask the manager that does — `legion raft status` names it.
```

`sustainable_nodes` is a lower bound, not a ceiling. A control plane that is
keeping up comfortably can't show how much more it could take, so on a
healthy cluster this reads as roughly the current fleet size.

### The warning

The status-lane leader logs this warning when the freshness ratio, fleet-wide
or in any measurable pool, stays below 0.5 for three consecutive ticks. It
logs at most once a minute:

```
WARN nodes=1000 served_interval_secs=2 freshness=0.22 lost_window_secs=10
  Heartbeat cadence is not being sustained: 22% of this fleet checked in
  within one interval. Healthy nodes will be marked lost when freshness falls
  below 0.20. Raise heartbeatSeconds to >= 19 for a fleet this size, or add
  control-plane capacity. See docs/heartbeat-cadence-and-scale.md
```

When one pool is behind a fleet that is otherwise keeping up, the warning
names it:

```
WARN nodes=10000 served_interval_secs=2 freshness=0.3 lost_window_secs=10
  Heartbeat cadence is not being sustained: 30% of pool latency (40 nodes, 2s
  interval) checked in within one interval (99% of the whole fleet). ...
```

The warning computes the specific `heartbeatSeconds` value to set for your
fleet size.

---

## Adaptive cadence

Adaptive cadence is on by default. The manager computes each worker's
liveness and sync intervals from fleet size and sends them on every
heartbeat and ping reply, so the full heartbeat backs off as the fleet grows
instead of the leader spending its CPU answering heartbeats, and detection
stays fast because the pings in between keep each node's window tight.

Why on by default? A fixed 2-second cadence is `N / 2` heartbeats a second.
At 20k nodes that is 10,000 RPCs a second into one leader. On a 4-vCPU AWS
leader it took more than a full core and slowed scheduling for everything
else. Nomad reached the same conclusion long ago: its servers scale the
heartbeat TTL so the cluster sees about `max_heartbeats_per_second` (50 by
default). Kubernetes renews node leases every 10 seconds with a 40-second
grace, whatever the fleet size.

```nickel
# configs/manager.ncl. Every value shown is the default.
heartbeat = {
  # The manager sizes and serves the interval. `false` turns it off, and
  # workers keep their worker-type `heartbeatSeconds` at any fleet size.
  adaptive = true,

  # Aggregate FULL-heartbeat rate the sync interval is sized for. Lower it
  # on a small control plane; measure yours with `legion node cadence`.
  targetRatePerSecond = 2500,

  # Aggregate liveness-PING rate the liveness interval, and so every lost
  # window, is sized for. Set it equal to targetRatePerSecond to turn pings
  # off.
  pingRatePerSecond = 10000,

  # Floor and ceiling on both served intervals. The floor is never crossed
  # however small the fleet; the ceiling is where the manager gives up
  # backing off and warns instead. It defaults to 55s so a served interval
  # never reaches the stateful self-fence (90s); a ceiling at or past 90s
  # is accepted with a startup warning.
  minInterval = "2s",
  maxInterval = "55s",

  # Missed beats before a node is lost. The lost window is ALWAYS this times
  # that node's liveness interval — 5 x 2s = the historical 10s.
  missedBeatsBeforeLost = 5,

  # Headroom in the sizing formula: 2 runs at half the target rate.
  headroom = 2,
}
```

### What the defaults serve

```
liveness = ceil(2 x N / 10000) = ceil(N / 5000)  seconds, snapped to the ladder
sync     = ceil(2 x N / 2500)  = ceil(N / 1250)  seconds, snapped, never below liveness
```

| Fleet | Liveness interval | Lost window | Full heartbeat | Heartbeats/s | Pings/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 2s | 10s | 2s | 500 | 0 |
| 2,500 | 2s | 10s | 2s | 1,250 | 0 |
| 5,000 | 2s | 10s | 5s | 1,000 | 1,500 |
| 10,000 | 2s | 10s | 8s | 1,250 | 3,750 |
| 20,000 | 5s | 25s | 21s | ~950 | ~3,050 |
| 50,000 | 13s | 65s | 55s | ~910 | ~2,940 |

Every fleet up to 2,500 nodes keeps the 2-second full heartbeat of earlier
releases and sends no pings. Every fleet up to 10,000 nodes keeps the
10-second lost window. The steady full-heartbeat load never exceeds
`targetRatePerSecond / headroom` (1,250 a second), about an eighth of what
took a core at 20k nodes, and the ping load never exceeds
`pingRatePerSecond / headroom` (5,000 a second). Without pings the 20,000
and 50,000 rows would have 105-second and 275-second windows.

The default rates are deliberately below what a current control plane can
answer. They buy back CPU for scheduling rather than chasing the ceiling.
Raise `targetRatePerSecond` if you want reports (health, failures) to reach
the manager sooner on a large fleet and have measured the headroom; raise
`pingRatePerSecond` for faster detection past 10,000 nodes (25,000 keeps the
10-second window to 25,000 nodes, at up to 12,500 pings a second). Lower both if
your control plane is smaller than a 4-vCPU leader, or if it still commits
heartbeats to raft (see [the measured table](#the-measured-table)): a ping
from a cluster in that fallback commits like a heartbeat, so set the two
equal there.

### Turning it off

```nickel
heartbeat = { adaptive = false }
```

With `adaptive = false`, the manager serves nothing, every worker beats at
its worker-type `heartbeatSeconds` (2s by default) with a full heartbeat
every time and no pings, and the window stays at
`missedBeatsBeforeLost x heartbeatSeconds`. This is the behavior of releases
before adaptive cadence was the default. Both rates are ignored while
`adaptive` is false, so switching back is a one-line edit. A rate of `0` is
not an off switch: the manager refuses to start and names the one that is.

Turning it off on a running cluster needs a manager restart, not a worker
restart. A worker whose reply carries no interval goes back to its
worker-type value. A worker on a build older than adaptive-by-default keeps
the last interval it was served until it restarts. Lost windows that adaptive
cadence widened stay wide until each node registers again, which delays
detection but never marks a healthy node lost.

### The controller

```
target = clamp(ceil(H x active_nodes / rate), minInterval, maxInterval)
served = the next rung >= target on [2, 3, 5, 8, 13, 21, 34, 55, 89, 120]s
```

The controller runs this twice: once with `pingRatePerSecond` for the
liveness interval, and once with `targetRatePerSecond` for the sync
interval, which is then raised to the liveness interval if it is below it.
Only the liveness interval moves lost windows. The controller has three
properties:

- **The input is fleet size, not saturation.** Deriving cadence from observed
  latency would create a feedback loop: back off, load drops, freshness
  recovers, speed up, load returns, back off. It would oscillate, and the lost
  window would change with it. Fleet size changes slowly and is driven from
  outside, so `interval = f(N)` is a pure function you can work out on paper.
- **Quantized to a ladder.** The rungs are roughly Fibonacci, about 1.6x
  apart. A fleet has to grow about 60% to move a rung, so the per-node lost
  window is not rewritten every tick. Stepping up is immediate. Stepping down
  waits for five minutes of a lower target, because narrowing a window is the
  direction that can cost a healthy node.
- **Bounded, with a warning at the bound.** Past `maxInterval`, the manager
  serves the ceiling and emits the freshness warning. Beyond that point, add
  control-plane capacity.

The status-lane leader runs the controller. Every other manager learns both
served intervals from the leader's reply to its liveness digest and serves
the same values to its own workers, so a worker gets one answer whichever
manager it talks to. The leader's `heartbeat` settings are the ones that
count, so keep them the same on every manager.

### What a worker does with it

The served liveness interval is a floor, not an override. A worker beats at
the larger of the served liveness interval and its worker-type
`heartbeatSeconds`. A 60-second batch pool keeps beating every 60 seconds
while a small fleet is served 2 seconds, and only slows further once the
served interval passes 60. A worker type configured faster than
`minInterval` is slowed to the floor, and its window widens to match. Set
`minInterval` lower if you need it. A worker sends a full heartbeat at the
larger of the served sync interval and its own beat, so the 60-second pool
never pings: every one of its beats is already a full heartbeat.

When the interval a worker should use changes, it sleeps once for a uniform
random time in `[0, interval)`, then resumes its normal jittered cadence.
This one-time re-phase spreads the fleet evenly across the new period in one
beat. Without it, every worker would learn the new value within one old
interval and re-phase onto the same boundary.

Legion does not use Nomad's permanent `[base, 2*base)` stagger. That stagger
costs a third of the target rate permanently, throws off the `N/R` arithmetic
by a variable factor, and doubles worst-case detection time.

### Windows move safely

A worker that ignores the served interval, such as an older build, is never
penalized:

- When the served interval increases, every node whose window is narrower
  than `missedBeatsBeforeLost x served` widens immediately, before any worker
  has been told. The manager repeats the check every tick, so a node that
  registers after a step up is covered too. A node whose own window is
  already wider keeps it.
- When the served interval decreases, a node's window narrows only after the
  manager sees that node beating at the faster cadence, and never below the
  cadence it was seen beating at. A worker that missed the speed-up is never
  punished for following the last instruction it received.

### Upgrading to adaptive-by-default

A cluster that never set `targetRatePerSecond` gets adaptive cadence when its
managers upgrade. Up to 2,500 nodes nothing observable changes: workers are
served the same 2 seconds they already used. To keep the old fixed behavior
at any size, set `heartbeat = { adaptive = false }` before upgrading.

During the roll:

- **Old worker, new manager.** A worker from before adaptive cadence ignores
  the served interval and keeps its configured cadence. Its window is widened
  anyway, so it is never marked lost for it. It just contributes more load
  than the controller assumed, which shows up as a freshness ratio that does
  not recover after a rung change. A worker from before pings reads the
  liveness interval as its heartbeat interval and sends a full heartbeat on
  every beat: never lost, but full-heartbeat load at the ping cadence.
- **New worker, old manager.** No interval arrives, so the worker uses its
  worker-type `heartbeatSeconds`, as it always did. A manager from before
  pings serves no sync interval, so the worker sends a full heartbeat on every
  beat and never pings.
- **Mixed managers.** An old manager serves nothing and relays nothing, so
  workers attached to it keep their configured cadence until it is upgraded.
  The leader still widens their windows. Upgrade managers first, as
  [docs/upgrades.md](upgrades.md) already requires.

### Benchmark fairness

Slowing heartbeats can make a system look better on a throughput benchmark.
Adaptive cadence is the default because the fixed 2-second cadence was
spending a leader's CPU at scale, not to flatter a benchmark. It is a pure
function of fleet size, so a benchmark and a production cluster of the same
size are served the same interval. Three rules keep Legion's numbers honest:

1. **Benchmarks run the default.** Scale runs use adaptive cadence at the
   default `targetRatePerSecond` unless the cadence itself is the subject of
   the run. A tuned rate is a non-default and is stated with the result.
2. **Every benchmark run records the served interval, the lost window, and
   the freshness ratio** alongside throughput
   (`legion_heartbeat_served_interval_seconds`,
   `legion_heartbeat_lost_window_seconds`,
   `legion_heartbeat_freshness_ratio`). Legion does not publish a number
   without its cadence, because the lost window is what the slower cadence
   costs.
3. **Comparative runs compare defaults and say so.** Nomad also adapts by
   default, scaling its heartbeat TTL to about 50 heartbeats a second, and
   Kubernetes renews leases every 10 seconds. A comparison reports each
   system's achieved cadence and detection window, or pins all of them to
   the same interval and says that it did.

---

## Safety valves

Two guards keep a control-plane problem from looking like a fleet-wide
outage. Both are on by default, and neither depends on adaptive cadence.

**The failover grace.** A manager that has just become responsible for
liveness marks nothing lost for `liveness.failoverGrace` (60s). See
[Node liveness](node-liveness.md) for details.

**The mass-disruption circuit breaker.** When more than 50% of the active
fleet would be marked lost in a single tick, the manager marks none of them
and logs at `error`:

```
ERROR would_be_lost=600 active_nodes=1000
  More than half this fleet would be marked lost in one tick; marking NONE.
  A control plane does not lose half its fleet in one tick — believe the
  control plane broke, not the fleet
```

This follows the same reasoning as Kubernetes' `stateFullDisruption`, applied
at detection instead of at eviction because Legion has no zone concept. The
breaker is off on fleets under 20 nodes, where losing half the cluster is an
ordinary event and not a sign of a bug.

Every suppression is counted, so you can tell "nothing was marked lost" apart
from "nothing was wrong":

```
legion_liveness_loss_suppressed_total{reason="mass_disruption"}
```

---

## Metrics

| Metric | Meaning |
| --- | --- |
| `legion_heartbeat_freshness_ratio` | **The number to alert on.** Fraction of active nodes that checked in within their own interval. |
| `legion_heartbeat_pool_freshness_ratio{pool}` | The same, per pool with 20 or more nodes. Alert on its minimum too. |
| `legion_heartbeat_served_interval_seconds` | The liveness interval this manager is telling workers to use. |
| `legion_heartbeat_sync_interval_seconds` | The full-heartbeat interval. Workers ping in between when it is longer. |
| `legion_heartbeat_lost_window_seconds` | The window derived from the liveness interval. |
| `legion_pings_total` | Pings the manager accepted (and `legion_worker_pings_total{outcome}` on workers). |
| `legion_heartbeat_sustainable_nodes` | Observed sustainable fleet size at the current interval (a lower bound). |
| `legion_nodes_marked_lost_total` | Nodes marked lost ("nodes went away"), as distinct from `legion_nodes{status="lost"}` ("nodes are away"). |
| `legion_liveness_loss_suppressed_total{reason}` | Would-be losses declined, by reason. |

All are reported on the status-lane leader. A manager that does not own
liveness reports zero. See [Metrics](metrics.md).

---

## Known limitations

- **`missedBeatsBeforeLost` is cluster-wide**, not per worker type. Per-type
  cadence already gives per-type windows, and a second per-type setting would
  make the window arithmetic hard to follow.
- **Detection slows past 10,000 nodes.** Pings keep the 10-second window up
  to 10,000 nodes at the default `pingRatePerSecond`. Past that the liveness
  interval backs off too, a rung or two behind the full heartbeat: 20,000
  nodes get a 25-second window, 50,000 nodes 65 seconds. With pings turned off (the two rates equal), detection
  slows with the full heartbeat: 105 seconds at 20,000 nodes and the default
  `targetRatePerSecond`. Nomad pays 100–200s at 5k nodes.
- **A ping is still an RPC.** It skips the manager's handler work, not the
  transport: HTTP/2 framing, protobuf, and the credential check cost a ping
  what they cost any call (see [What a ping costs](#what-a-ping-costs)).
- **Reports still move at the sync interval.** A health transition a worker
  sees between full heartbeats reaches the manager on the next full heartbeat
  (up to 21 seconds at 20,000 nodes), or sooner on an early beat. Pings
  speed up detection, not reporting.
- **The ceiling is set by the stateful self-fence, not by the ladder.** A
  worker kills its stateful tasks after `stateful.onLost.selfTerminateAfter`
  (90s by default) without a successful heartbeat or ping. A failed beat is
  retried within seconds, so what matters is how much of those 90 seconds the
  liveness interval itself uses. `maxInterval` defaults to 55s for that
  reason, and caps both served intervals: at the 55s rung a worker still has
  roughly 30 seconds of retries before it fences. The full heartbeat reaches
  that rung past about 42,500 nodes at the default `targetRatePerSecond`, and
  past about 68,750 the fleet sends full heartbeats faster than that rate.
  The liveness interval reaches it only past about 170,000 nodes at the
  default `pingRatePerSecond`, and that is when the manager reports it is at
  the ceiling (`at_max_interval`). Raising `maxInterval` to 90s or more is accepted, with a startup
  warning, because a jittered interval that long lets a healthy worker
  fence between beats; do it only with `selfTerminateAfter` raised to
  match. The check is against the default fence: a job that sets a shorter
  `selfTerminateAfter` needs a ceiling below its own value. See
  [Stateful workloads](stateful-workloads.md).
- **The 50% disruption threshold is not configurable.** Open an issue if your
  cluster needs to tune it.
- **A node's cadence comes from what it reported at registration.** If a
  worker type's `heartbeatSeconds` changes, the worker picks up the new window
  at its next registration, not immediately.

---

## See also

- [Node liveness](node-liveness.md): where heartbeat state lives, the
  failover grace, the peer digest, and the display checkpoint
- [Managers and workers](architecture-managers-workers.md)
- [Stateful workloads](stateful-workloads.md): the self-fence interaction
- [Metrics](metrics.md), [Testing and benchmarks](testing-and-benchmarks.md)
