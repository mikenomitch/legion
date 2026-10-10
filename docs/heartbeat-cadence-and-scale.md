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
lost window = missedBeatsBeforeLost x that node's heartbeat interval
```

At the defaults (`5 x 2s`), the window is 10 seconds. A cluster that sets
nothing runs [adaptive cadence](#adaptive-cadence). Up to 2,500 nodes it
keeps the 2-second interval and 10-second window of earlier releases. Past
that, the manager slows the fleet down and widens each window to match.

The window is per node. It is stored on the node's own record instead of
being one global constant. A cluster with a 2-second latency-sensitive pool
and a 60-second batch pool gets a 10-second window for the first and a
300-second window for the second. This means you can raise
`heartbeatSeconds` to relieve manager load without losing detection margin.
Under a fixed 10-second window, a 13-second cadence would mean one missed
beat marks a node dead.

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
freshness_ratio = active nodes that beat within 1.2 x the served interval
                  ─────────────────────────────────────────────────────────
                  all active nodes
```

`1.0` means every node checked in on time. The `1.2` covers the +-10%
steady-state jitter plus some slack.

False node loss begins when the ratio falls below
`served_interval / lost_window`. At the defaults, that is `2s / 10s = 0.20`.
The ratio and its danger threshold use the same units, so you can reason about
the number directly.

The ratio comes from the store's view of the fleet, not from an RPC counter.
Under `lanes` topology, a per-process `legion_heartbeats_total` rate only
shows one manager's share. The ratio is suppressed below 20 active nodes,
where it is too noisy to be useful.

### `legion node cadence`

```console
$ legion node cadence
Fleet             1000 nodes
Served interval   2s  (adaptive)
Lost window       10s  (5 missed beats)
Freshness         0.22 ── FALLING BEHIND  (false loss below 0.20)
Sustainable       ~220 nodes at 2s on this control plane
Suggested         heartbeatSeconds: 19   (lost window 95s)
```

With `adaptive = false`, the served-interval line reads
`(workers use their worker-type heartbeatSeconds)` instead.

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

The status-lane leader logs this warning when the freshness ratio stays below
0.5 for three consecutive ticks. It logs at most once a minute:

```
WARN nodes=1000 served_interval_secs=2 freshness=0.22 lost_window_secs=10
  Heartbeat cadence is not being sustained: 22% of this fleet checked in
  within one interval. Healthy nodes will be marked lost when freshness falls
  below 0.20. Raise heartbeatSeconds to >= 19 for a fleet this size, or add
  control-plane capacity. See docs/heartbeat-cadence-and-scale.md
```

The warning computes the specific `heartbeatSeconds` value to set for your
fleet size.

---

## Adaptive cadence

Adaptive cadence is on by default. The manager computes each worker's
interval from fleet size and sends it on the heartbeat reply, so cadence
backs off as the fleet grows instead of healthy nodes being marked lost or
the leader spending its CPU answering heartbeats.

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

  # Aggregate heartbeat rate the served interval is sized for. Lower it on
  # a small control plane; measure yours with `legion node cadence`.
  targetRatePerSecond = 2500,

  # Floor and ceiling on the served interval. The floor is never crossed
  # however small the fleet; the ceiling is where the manager gives up
  # backing off and warns instead.
  minInterval = "2s",
  maxInterval = "120s",

  # Missed beats before a node is lost. The lost window is ALWAYS this times
  # that node's served interval — 5 x 2s = the historical 10s.
  missedBeatsBeforeLost = 5,

  # Headroom in the sizing formula: 2 runs at half the target rate.
  headroom = 2,
}
```

### What the defaults serve

```
interval = ceil(2 x N / 2500) = ceil(N / 1250) seconds, snapped to the ladder
```

| Fleet | Served interval | Lost window | Steady heartbeats/s |
| ---: | ---: | ---: | ---: |
| 1,000 | 2s | 10s | 500 |
| 2,500 | 2s | 10s | 1,250 |
| 5,000 | 5s | 25s | 1,000 |
| 10,000 | 8s | 40s | 1,250 |
| 20,000 | 21s | 105s | ~950 |
| 50,000 | 55s | 275s | ~910 |

Every fleet up to 2,500 nodes keeps the 2-second floor and the 10-second
window, so small and medium clusters see no change from earlier releases.
Past that, the steady-state load never exceeds `targetRatePerSecond /
headroom` (1,250 a second), about an eighth of what took a core at 20k
nodes.

The default rate is deliberately below what a current control plane can
answer. It buys back CPU for scheduling rather than chasing the heartbeat
ceiling. Raise it if you want faster detection on a large fleet and have
measured the headroom. Lower it if your control plane is smaller than a
4-vCPU leader, or if it still commits heartbeats to raft (see
[the measured table](#the-measured-table)).

### Turning it off

```nickel
heartbeat = { adaptive = false }
```

With `adaptive = false`, the manager serves nothing, every worker beats at
its worker-type `heartbeatSeconds` (2s by default), and the window stays at
`missedBeatsBeforeLost x heartbeatSeconds`. This is the behavior of releases
before adaptive cadence was the default. `targetRatePerSecond` is ignored
while `adaptive` is false, so switching back is a one-line edit.
`targetRatePerSecond = 0` is not an off switch: the manager refuses to start
and names `adaptive = false` instead.

Turning it off on a running cluster needs a manager restart, not a worker
restart. A worker whose reply carries no interval goes back to its
worker-type value. A worker on a build older than adaptive-by-default keeps
the last interval it was served until it restarts. Lost windows that adaptive
cadence widened stay wide until each node registers again, which delays
detection but never marks a healthy node lost.

### The controller

```
target = clamp(ceil(H x active_nodes / targetRatePerSecond), minInterval, maxInterval)
served = the next rung >= target on [2, 3, 5, 8, 13, 21, 34, 55, 89, 120]s
```

The controller has three properties:

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

The status-lane leader runs the controller. Every other manager learns the
served interval from the leader's reply to its liveness digest and serves
the same value to its own workers, so a worker gets one answer whichever
manager it talks to. The leader's `heartbeat` settings are the ones that
count, so keep them the same on every manager.

### What a worker does with it

The served interval is a floor, not an override. A worker beats at the
larger of the served interval and its worker-type `heartbeatSeconds`. A
60-second batch pool keeps beating every 60 seconds while a small fleet is
served 2 seconds, and only slows further once the served interval passes 60.
A worker type configured faster than `minInterval` is slowed to the floor,
and its window widens to match. Set `minInterval` lower if you need it.

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

- **Old worker, new manager.** The worker ignores the served interval and
  keeps its configured cadence. Its window is widened anyway, so it is never
  marked lost for it. It just contributes more load than the controller
  assumed, which shows up as a freshness ratio that does not recover after a
  rung change.
- **New worker, old manager.** No interval arrives, so the worker uses its
  worker-type `heartbeatSeconds`, as it always did.
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
| `legion_heartbeat_freshness_ratio` | **The number to alert on.** Fraction of active nodes that checked in within one interval. |
| `legion_heartbeat_served_interval_seconds` | The interval this manager is telling workers to use. |
| `legion_heartbeat_lost_window_seconds` | The window derived from it. |
| `legion_heartbeat_sustainable_nodes` | Observed sustainable fleet size at the current interval (a lower bound). |
| `legion_nodes_marked_lost_total` | Nodes marked lost ("nodes went away"), as distinct from `legion_nodes{status="lost"}` ("nodes are away"). |
| `legion_liveness_loss_suppressed_total{reason}` | Would-be losses declined, by reason. |

All are reported on the status-lane leader. A manager that does not own
liveness reports zero. See [Metrics](metrics.md).

---

## Known limitations

- **The freshness ratio is fleet-wide, not per pool.** A cluster with a slow
  batch pool and a fast latency pool has two populations and one blended
  ratio, which can hide a problem in the fast pool. Watch
  `legion_nodes_marked_lost_total` alongside it.
- **`missedBeatsBeforeLost` is cluster-wide**, not per worker type. Per-type
  cadence already gives per-type windows, and a second per-type setting would
  make the window arithmetic hard to follow.
- **Detection slows in proportion to the back-off.** At the default rate a
  20k-node fleet is served 21s, so the lost window is `5 x 21s = 105s` and
  Legion takes up to 105 seconds to notice a dead machine, instead of 10. At
  1000 nodes with `targetRatePerSecond = 80`, the ladder serves 34s and the
  window is 170s. This is the cost of adaptive cadence. Nomad pays a similar
  cost (100–200s at 5k nodes).
- **Very large fleets reach the stateful self-fence.** A worker kills its
  stateful tasks after `stateful.onLost.selfTerminateAfter` (90s by default)
  without a successful heartbeat. A failed beat is retried within seconds,
  so what matters is how much of those 90 seconds the interval itself uses.
  At the 55s rung (past about 42,500 nodes at the default rate) a worker has
  roughly 30 seconds of retries before it fences. At the 89s rung (past about
  68,750 nodes) a jittered interval can exceed 90 seconds, and a healthy
  worker fences between beats. The startup check against the fence covers
  worker-type `heartbeatSeconds`, not the served interval. In fleets that
  large, set `maxInterval = "55s"` or raise `selfTerminateAfter` on stateful
  jobs. See [Stateful workloads](stateful-workloads.md).
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
