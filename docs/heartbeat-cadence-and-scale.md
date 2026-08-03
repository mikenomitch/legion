# Heartbeat Cadence and Scale

Every worker checks in with the manager on a fixed interval. This page is
about what happens when there are more workers than the control plane in
front of them can answer, how to size for that before it happens, and how
Legion keeps a busy control plane from being mistaken for a dead fleet.

Related: [Node liveness](node-liveness.md) decides *where* liveness lives and
what survives an election. This page decides *how often* a worker checks in
and *how long* it may be silent.

---

## The one relationship

Two sentences describe the whole model:

> The manager tells workers how often to check in, and a node is lost after
> five missed check-ins.

Everything below derives from that.

```
lost window = missedBeatsBeforeLost x that node's heartbeat interval
```

At the defaults — `5 x 2s` — that is the historical fixed 10-second window,
exactly. A cluster that sets nothing behaves bit for bit as it always did.

The important change is that the window is **per node**, recorded on the
node's own record, rather than one global constant. A cluster with a 2-second
latency-sensitive pool and a 60-second batch pool gets a 10-second window for
the first and a 300-second window for the second. Before, `heartbeatSeconds`
was a knob that relieved manager load and destroyed detection margin with the
same turn: at 13-second cadence against a fixed 10-second window, one missed
beat was a dead node.

---

## Sizing a control plane

```
sustainable_nodes  ~=  R x I

  R = heartbeat commits per second this control plane sustains
  I = heartbeat interval in seconds
```

and its inverse, with a headroom factor `H` (the default is 2 — run at half
the measured ceiling so a burst is not an outage):

```
heartbeatSeconds  >=  H x N / R
lost window        =  missedBeatsBeforeLost x heartbeatSeconds
```

`R` is **not a constant**. Per-manager throughput is roughly
`worker_threads / heartbeat_handler_latency`, and handler latency used to be
dominated by the per-commit fsync. Since a steady-state heartbeat no longer
commits at all (see [Node liveness](node-liveness.md)), `R` on a modern
Legion is a property of the gRPC handler rather than the disk — but a
cluster in the mixed-version window of an upgrade, or one whose status-lane
leader predates liveness digests, still pays the old cost. Measure yours.

### The measurement recipe

1. Point a simulated fleet at the real control plane:
   `legion start worker --simulate --count N`. The simulated fleet shares the
   real cadence-resolution and jitter helpers, so it beats exactly as a real
   worker does — the number it produces is not a parallel implementation's.
2. Ramp `N` and watch `legion_heartbeat_freshness_ratio` (or run
   `legion node cadence`).
3. `R` is the largest `N / I` at which the ratio holds at or above **0.95**.
4. Cross-check against `legion_raft_propose_apply_seconds{lane="status"}` and
   `legion_store_coalesce_batch_size{path="heartbeat"}` — if either is
   climbing, the ceiling you found is the commit path, not the handler.

### The measured table

For the AWS-benchmarked pre-liveness-rework control plane (3x m7i.xlarge,
3-voter `lanes`, EBS gp3), `R ~= 80`:

| Fleet | Min interval (H=2) | Set `heartbeatSeconds` | Resulting lost window |
| ---: | ---: | ---: | ---: |
| 100 | 2.5s | 3 | 15s |
| 160 | 4s | 4 | 20s |
| 250 | 6.3s | 7 | 35s |
| 500 | 12.5s | 13 | 65s |
| 1000 | 25s | 25 | 125s |
| 2000 | 50s | 50 | 250s |

Every row above 3 seconds is legal advice **only** because the lost window
moves with the cadence. Under the old fixed window it would have been a
recipe for marking a healthy fleet dead.

Two numbers bound the table from outside:

- `heartbeatSeconds` is clamped to `1..=300`. It used to stop at 30.
- The manager **refuses to start** if `missedBeatsBeforeLost x
  heartbeatSeconds` reaches the stateful self-fence
  (`stateful.onLost.selfTerminateAfter`, 90s by default). A healthy worker
  that kills its own stateful tasks between beats is the worst failure this
  page can cause, so it is caught at config load rather than discovered in
  production. See [Stateful workloads](stateful-workloads.md).

---

## Is the fleet keeping up?

### The freshness ratio

```
freshness_ratio = active nodes that beat within 1.2 x the served interval
                  ─────────────────────────────────────────────────────────
                  all active nodes
```

`1.0` means every node checked in on time. The `1.2` covers the +-10%
steady-state jitter plus a beat's worth of slop.

**False node loss begins when the ratio falls below
`served_interval / lost_window`** — `2s / 10s = 0.20` at the defaults. The
ratio and its danger threshold are in the same units, which is what makes the
number explainable rather than something to look up.

It is derived from the store's view of the fleet, not from an RPC counter: a
per-process `legion_heartbeats_total` rate is only ever one manager's share
under `lanes` topology. It is suppressed below 20 active nodes, where the
ratio is pure noise.

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

Ask any manager. One that does not lead the status lane does not own liveness
and says so rather than guessing:

```console
$ legion node cadence
This manager does not own node liveness (it does not lead the status lane).
Ask the manager that does — `legion raft status` names it.
```

`sustainable_nodes` is a **lower bound**, not a ceiling: a control plane that
is comfortably keeping up never reveals how much more it could take, so on a
healthy cluster it reads as roughly the current fleet size.

### The warning

On the status-lane leader, when the freshness ratio sits below 0.5 for three
consecutive ticks, at most once a minute:

```
WARN nodes=1000 served_interval_secs=2 freshness=0.22 lost_window_secs=10
  Heartbeat cadence is not being sustained: 22% of this fleet checked in
  within one interval. Healthy nodes will be marked lost when freshness falls
  below 0.20. Raise heartbeatSeconds to >= 19 for a fleet this size, or add
  control-plane capacity. See docs/heartbeat-cadence-and-scale.md
```

The remedy is computed, not generic. An operator staring at a collapsing
cluster is handed the number to set.

---

## Adaptive cadence

Off by default. When `heartbeat.targetRatePerSecond` is set, the manager
computes each worker's interval from **fleet size** and serves it on the
heartbeat reply, so cadence backs off as the fleet grows instead of healthy
nodes being marked lost.

```jsonnet
// configs/manager.jsonnet
heartbeat: {
  // Aggregate heartbeat rate this control plane will accept. Unset (the
  // default) = workers keep their worker-type `heartbeatSeconds` and nothing
  // adapts. Measure yours with `legion node cadence`.
  targetRatePerSecond: 80,

  // Floor and ceiling on the served interval. The floor is never crossed
  // however small the fleet; the ceiling is where the manager gives up
  // backing off and warns instead.
  minInterval: "2s",
  maxInterval: "120s",

  // Missed beats before a node is lost. The lost window is ALWAYS this times
  // that node's served interval — 5 x 2s = the historical 10s.
  missedBeatsBeforeLost: 5,

  // Headroom in the sizing formula: 2 runs at half the measured ceiling.
  headroom: 2,
}
```

### The controller

```
target = clamp(ceil(H x active_nodes / targetRatePerSecond), minInterval, maxInterval)
served = the next rung >= target on [2, 3, 5, 8, 13, 21, 34, 55, 89, 120]s
```

Three properties are deliberate:

- **The input is fleet size, not saturation.** Deriving cadence from observed
  latency would be a closed control loop: back off, load drops, freshness
  recovers, speed up, load returns, back off. It would oscillate and the lost
  window would breathe with it. Fleet size is slow and externally driven, so
  `interval = f(N)` is a pure function an operator can evaluate on paper.
- **Quantized to a ladder.** Roughly Fibonacci, ~1.6x per rung: a fleet has to
  grow ~60% to move a rung, so the per-node lost window is not rewritten every
  tick. Stepping **up** is immediate; stepping **down** waits five minutes of
  a lower target, because narrowing a window is the direction that can cost a
  healthy node.
- **Bounded, and it warns at the bound.** Past `maxInterval` the manager
  serves the ceiling and emits the freshness warning. Backing off is not
  infinite; past a point the answer is more control plane.

### What a worker does with it

A worker that receives `next_interval_seconds` adopts it and sleeps one
uniform `[0, interval)` **once** before resuming its normal jittered cadence.
That one-shot re-phase spreads a fleet uniformly across the new period in
exactly one beat; without it, every worker learns the new value within one old
interval and re-phases onto the same boundary.

Legion deliberately does **not** adopt Nomad's permanent `[base, 2*base)`
stagger: it costs a third of the target rate forever, makes the `N/R`
arithmetic wrong by a variable factor, and doubles worst-case detection time.

A worker that ignores the field — an older build — is never penalized:

- When the served interval **increases**, every node's window widens
  **immediately**, before any worker has been told. A worker that never gets
  the message is protected.
- When it **decreases**, a node's window narrows **only after the manager
  observes that node actually beating at the faster cadence**. A worker that
  missed the speed-up is never punished for obeying the last instruction it
  received.

### Benchmark fairness

A system that quietly slows its own heartbeats to score better on a
throughput benchmark is cheating. Three rules:

1. **Off by default.** `targetRatePerSecond` unset means nothing adapts.
2. **Every benchmark run records the served interval and the freshness ratio**
   alongside throughput. A number without its cadence is not publishable.
3. **Comparative runs pin it**, or report all systems' *achieved* cadence —
   noting that Nomad's own `n/rate` scaling means an unpinned Nomad is doing
   the same thing.

---

## Safety valves

Two guards keep a control-plane problem from presenting as a fleet-wide
outage. Both are on by default and neither depends on adaptive cadence.

**The failover grace.** A manager that has just become responsible for
liveness marks nothing lost for `liveness.failoverGrace` (60s). This is owned
by [Node liveness](node-liveness.md) — see that page.

**The mass-disruption circuit breaker.** When more than **50%** of the active
fleet would be marked lost in a single tick, the manager marks **none of
them** and logs at `error`:

```
ERROR would_be_lost=600 active_nodes=1000
  More than half this fleet would be marked lost in one tick; marking NONE.
  A control plane does not lose half its fleet in one tick — believe the
  control plane broke, not the fleet
```

This is Kubernetes' `stateFullDisruption` reasoning, applied at *detection*
rather than at eviction because Legion has no zone concept. It is suppressed
on fleets under 20 nodes, where losing half the cluster is an ordinary event
rather than evidence of a bug.

Every suppression is counted, because "nothing was marked lost" must never be
indistinguishable from "nothing was wrong":

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
| `legion_nodes_marked_lost_total` | Nodes marked lost — "nodes went away", as distinct from `legion_nodes{status="lost"}`'s "nodes are away". |
| `legion_liveness_loss_suppressed_total{reason}` | Would-be losses declined, by reason. |

All are reported on the status-lane leader; a manager that does not own
liveness reports zero. See [Metrics](metrics.md).

---

## Known limitations

- **The freshness ratio is fleet-wide, not per pool.** A cluster with a slow
  batch pool and a fast latency pool has two populations and one blended
  ratio, which can mask a problem in the fast pool. Watch
  `legion_nodes_marked_lost_total` alongside it.
- **`missedBeatsBeforeLost` is cluster-wide**, not per worker type. Per-type
  cadence already gives per-type windows, and a second per-type knob would
  make the window arithmetic unexplainable.
- **Detection gets slower in exact proportion to the back-off.** At 1000 nodes
  and `R = 80`, a genuinely dead machine takes up to 125 seconds to notice
  instead of 10. This is the real price of adaptive cadence, and it is the
  same price Nomad pays (100–200s at 5k nodes).
- **The 50% disruption threshold is not configurable.** Start there; open an
  issue if a real cluster needs to tune it.
- **A node's cadence is read from what it reported at registration.** A worker
  whose worker-type `heartbeatSeconds` changes picks up the new window at its
  next registration, not immediately.

---

## See also

- [Node liveness](node-liveness.md) — where heartbeat state lives, the
  failover grace, the peer digest, and the display checkpoint
- [Managers and workers](architecture-managers-workers.md)
- [Stateful workloads](stateful-workloads.md) — the self-fence interaction
- [Metrics](metrics.md), [Testing and benchmarks](testing-and-benchmarks.md)
