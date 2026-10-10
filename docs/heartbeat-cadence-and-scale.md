# Heartbeat Cadence and Scale

Every worker checks in with the manager on a fixed interval. This page covers
what happens when there are more workers than the control plane can answer,
how to size for that ahead of time, and how Legion keeps a busy control plane
from being mistaken for a dead fleet.

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
nothing behaves the same as earlier releases with their fixed 10-second
window.

The window is per node. It is stored on the node's own record instead of
being one global constant. A cluster with a 2-second latency-sensitive pool
and a 60-second batch pool gets a 10-second window for the first and a
300-second window for the second. This means you can raise
`heartbeatSeconds` to relieve manager load without losing detection margin.
Under a fixed 10-second window, a 13-second cadence would mean one missed
beat marks a node dead.

---

## Sizing a control plane

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

This table applies the formula above with a fixed `heartbeatSeconds` (no
adaptive cadence). It uses `R ~= 80`, measured on an AWS control plane
(3x m7i.xlarge, 3-voter `lanes`, EBS gp3) before the liveness rework, when
every heartbeat still committed. Current releases should measure a higher
`R`, so treat these rows as conservative and measure your own:

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
Served interval   2s  (workers use their worker-type heartbeatSeconds)
Lost window       10s  (5 missed beats)
Freshness         0.22 ── FALLING BEHIND  (false loss below 0.20)
Sustainable       ~220 nodes at 2s on this control plane
Suggested         heartbeatSeconds: 19   (lost window 95s)
```

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

Adaptive cadence is off by default. When you set
`heartbeat.targetRatePerSecond`, the manager computes each worker's interval
from fleet size and sends it on the heartbeat reply. Cadence then backs off as
the fleet grows, instead of healthy nodes being marked lost.

```nickel
# configs/manager.ncl
heartbeat = {
  # Aggregate heartbeat rate this control plane will accept. Unset (the
  # default) = workers keep their worker-type `heartbeatSeconds` and nothing
  # adapts. Measure yours with `legion node cadence`.
  targetRatePerSecond = 80,

  # Floor and ceiling on the served interval. The floor is never crossed
  # however small the fleet; the ceiling is where the manager gives up
  # backing off and warns instead.
  minInterval = "2s",
  maxInterval = "120s",

  # Missed beats before a node is lost. The lost window is ALWAYS this times
  # that node's served interval — 5 x 2s = the historical 10s.
  missedBeatsBeforeLost = 5,

  # Headroom in the sizing formula: 2 runs at half the measured ceiling.
  headroom = 2,
}
```

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

### What a worker does with it

A worker that receives `next_interval_seconds` adopts it. It sleeps once for a
uniform random time in `[0, interval)`, then resumes its normal jittered
cadence. This one-time re-phase spreads the fleet evenly across the new period
in one beat. Without it, every worker would learn the new value within one old
interval and re-phase onto the same boundary.

Legion does not use Nomad's permanent `[base, 2*base)` stagger. That stagger
costs a third of the target rate permanently, throws off the `N/R` arithmetic
by a variable factor, and doubles worst-case detection time.

A worker that ignores the field, such as an older build, is never penalized:

- When the served interval increases, every node's window widens
  immediately, before any worker has been told. A worker that never gets the
  message is protected.
- When it decreases, a node's window narrows only after the manager sees that
  node beating at the faster cadence. A worker that missed the speed-up is
  never punished for following the last instruction it received.

### Benchmark fairness

Slowing heartbeats can make a system look better on a throughput benchmark.
To keep Legion's benchmarks fair, three rules apply:

1. **Off by default.** If `targetRatePerSecond` is unset, nothing adapts.
2. **Every benchmark run records the served interval and the freshness
   ratio** alongside throughput. Legion does not publish a number without its
   cadence.
3. **Comparative runs pin the cadence**, or report every system's achieved
   cadence. Nomad's own `n/rate` scaling means an unpinned Nomad adapts its
   cadence too.

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
- **Detection slows in proportion to the back-off.** At 1000 nodes,
  `targetRatePerSecond = 80`, and the default headroom of 2, the target is
  25s and the ladder serves the next rung, 34s. The lost window is then
  `5 x 34s = 170s`, so Legion takes up to 170 seconds to notice a dead
  machine, instead of 10. This is the cost of adaptive cadence. Nomad pays a
  similar cost (100–200s at 5k nodes).
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
