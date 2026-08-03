# Heartbeat Cadence at Scale

**Status: PLANNED** — nothing here is built. Follows
[`heartbeat-scale-plan.md`](heartbeat-scale-plan.md) (SHIPPED 2026-07-30),
which made one heartbeat cheap; this plan decides what happens when there
are too many of them for the control plane in front of you. It picks up
that plan's first named deferral — *"per-node lost windows negotiated at
registration"* — because every option below needs it.

Deliberately deferred, and named as non-goals below: gossip/SWIM liveness
and moving liveness out of raft into leader-local soft state (architecture
item D8, still awaiting the AWS campaign), control-plane autoscaling, and
per-node operator-set cadence.

> **Judgement calls:** [`../calls/heartbeat-cadence-at-scale-plan_calls.md`](../calls/heartbeat-cadence-at-scale-plan_calls.md)

---

## 1. Problem

A 3-voter raft control plane under the `lanes` topology (3× m7i.xlarge,
4 vCPU each) sustained **~80 heartbeat RPCs/second** in the AWS benchmark
rig. Heartbeat cadence defaults to **2 seconds**
(`WORKER_HEARTBEAT_DURATION_SEC`, `src/constants.rs:80`). Those two
numbers multiply out to about **160 nodes** before heartbeats start
falling behind, on hardware an operator would reasonably call "the
control plane".

At **1000 nodes** on that same control plane, the fleet achieved **22% of
the expected cadence**. That number is not a performance footnote. Work
it through:

- Configured interval: 2s. Achieved effective interval: `2s / 0.22 ≈ 9.1s`.
- Lost-node threshold: **10s**, a hard-coded constant
  (`DEF_LOST_NODE_AT_SEC`, `src/constants.rs:72`), compared against a
  single global cutoff in `nodes_to_mark_lost` (`src/manager/daemon.rs:974`).

The fleet was running **9.1 seconds into a 10-second window**. Healthy
nodes with healthy workers began being marked lost — not because anything
failed, but because the control plane could not answer fast enough to
prove they had not. Marking a node lost costs a reschedule eval per
allocation on it, which is *more* control-plane load, which lengthens the
effective interval, which marks more nodes lost. **This is a correctness
failure with a positive feedback term, not a slow-request problem.**

The breakeven is worth stating exactly, because it is the whole design
constraint:

```
false-loss begins when   achieved_ratio  <  configured_interval / lost_window
default cluster:                         <  2s / 10s  =  0.20
measured at 1000 nodes:     0.22
```

The fleet was operating **10% of margin away** from mass false loss.

### What an operator has to do today

Nothing available to them is adequate:

- `heartbeatSeconds` exists on the worker-type config
  (`src/structs/config.rs:1535`), clamped to `1..=30`
  (`heartbeat_interval_for`, `src/worker/daemon.rs:585`), jittered ±10%.
  But the lost window **does not move with it**. Setting
  `heartbeatSeconds: 8` means one missed beat is a lost node. The manager
  already warns above 3s (`src/manager/daemon.rs:1944`) precisely because
  the 10s window is fixed — so the one knob that relieves manager load is
  the same knob that destroys detection margin.
- There is no published sizing guidance. Nothing tells an operator that
  160 nodes is where their control plane runs out.
- There is no signal that it is happening. `legion_heartbeats_total`
  counts accepted heartbeats; nothing reports whether the fleet is
  *keeping up*. The first symptom an operator sees is
  `Node <id> heartbeat too slow - marking as lost` — one line per node,
  at whatever rate the collapse is running.

### Two adjacent hazards this exposes

Both are load-bearing on the design and neither is currently guarded:

1. **Status-lane leadership change has no grace period.**
   `nodes_to_mark_lost` suppresses marking for the first
   `DEF_LOST_NODE_AT_SEC` of *process uptime*
   (`src/manager/daemon.rs:978`), measured from `started_at` in
   `start_node_status_checker`. Under `lanes` topology, a manager that has
   been up for hours can *acquire* status-lane leadership mid-life and
   immediately begin marking lost, with no window for the fleet to
   re-establish contact through an election that just cost it several
   seconds of failed heartbeats. Nomad has a dedicated
   `failover_heartbeat_ttl` for exactly this.
2. **Stateful self-fencing is calibrated to a 2s cadence.**
   `DEFAULT_SELF_TERMINATE_AFTER` is 90s (`src/structs/job.rs:1050`) — the
   worker kills its own stateful tasks after that long without manager
   contact. Any cadence at or near 90s makes a *healthy* worker self-fence
   between beats. Whatever raises the interval ceiling must validate
   against this.

---

## 2. Prior art

### Kubernetes — shrink the write, do not slow the beat

Kubernetes hit this wall at ~2000 nodes, and the failure was etcd, not
CPU. From [KEP-589](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/589-efficient-node-heartbeats/README.md):
node objects "may easily exceed 15kB; even though the patch sent over
network is small, in etcd we store the whole Node object", with kubelets
"sending heartbeats every 10s" — enough to push etcd into alert mode and
stop accepting writes entirely.

The fix was **not** a longer interval. It was splitting liveness from
state:

- A tiny `coordination.k8s.io/v1` **Lease** object per node in a dedicated
  `kube-node-lease` namespace, renewed at a **fixed 10s**
  (`nodeLeaseDurationSeconds` default **40**, renewed at
  `nodeLeaseRenewIntervalFraction = 0.25` of it, jitter **0.04**).
- The heavy `NodeStatus` write demoted to a keepalive:
  `nodeStatusUpdateFrequency` **10s** for *computing* status, but
  `nodeStatusReportFrequency` **5m** for reporting it when nothing
  changed. Jitter on that path is much larger, `±50%`
  (`calculateDelay()` in `kubelet_node_status.go`), explicitly "so that we
  can avoid all the periodic node status updates to reach the apiserver at
  the same time".
- Result, verbatim: "in 5k-node clusters, we are reducing etcd write
  throughput from 150MB/min to 30MB/min."

Controller side, `--node-monitor-grace-period` is **50s** — raised from
40s in **v1.32** ([#126287](https://github.com/kubernetes/kubernetes/pull/126287),
for [#121793](https://github.com/kubernetes/kubernetes/issues/121793)) because
the HTTP/2 health-check timeouts (30s ping + 15s idle) summed to 45s, so a
broken apiserver connection marked **every node** NotReady before the
connection was even torn down. That is the same class of bug as our §1:
a control-plane-side delay presenting as fleet-wide failure.

Kubernetes' safety valve is downstream, at eviction, not at detection.
`--node-eviction-rate` **0.1/s**; a zone with `>2` not-ready nodes and
`≥ --unhealthy-zone-threshold` (**0.55**) not-ready drops to
`--secondary-node-eviction-rate` (**0.01/s**), and to **zero** below
`--large-cluster-size-threshold` (**50**) nodes; if every zone is fully
disrupted, eviction stops entirely. Documented ceiling: **5,000 nodes**.

**What we take:** the eviction-side circuit breaker (when *most* of the
fleet looks lost, believe the control plane is broken, not the fleet), and
the large jitter on the low-frequency path. **What we reject for now:** the
lease-object split. Legion's per-heartbeat cost is already one *batched*
raft commit, not a 20KB object write — group commit shipped in the
predecessor plan. Splitting storage is D8, and it is a non-goal here.
**What we explicitly note:** Kubernetes does **not** vary cadence per node
or with cluster size. Every value above is static operator config.

### Nomad — the server dictates the interval, scaled by fleet size

This is the closest prior art and the one option (c) is modelled on. The
Nomad client does not choose its interval: it heartbeats
`Node.UpdateStatus`, and the **server returns the TTL in the reply**
(`reply.HeartbeatTTL = ttl` in `nomad/node_endpoint.go`; the client does
`c.heartbeatTTL = resp.HeartbeatTTL` in `client/client.go`).

`nomad/heartbeat.go`, `resetHeartbeatTimer`:

```go
n := len(h.heartbeatTimers)
ttl := helper.RateScaledInterval(h.srv.config.MaxHeartbeatsPerSecond,
                                 h.srv.config.MinHeartbeatTTL, n)
ttl += helper.RandomStagger(ttl)
h.resetHeartbeatTimerLocked(id, ttl+h.srv.config.HeartbeatGrace)
return ttl
```

with `RateScaledInterval(rate, min, n) = max(min, n/rate seconds)`. So:

```
base = max(min_heartbeat_ttl, n / max_heartbeats_per_second)
ttl  = base + U[0, base)          // uniform in [base, 2*base)
server expiry = ttl + heartbeat_grace     // client is NOT told the grace
```

| Knob | Default |
| --- | --- |
| `min_heartbeat_ttl` | 10s |
| `max_heartbeats_per_second` | 50.0 |
| `heartbeat_grace` | 10s |
| `failover_heartbeat_ttl` | 5m |

Three details matter for us. **`n` is read fresh on every heartbeat** from
the leader's live timer map, so it tracks fleet size continuously with no
separate accounting. **The grace is server-side and hidden from the
client**, giving a correctness margin without inflating the stated
interval. And on leader election, `initializeHeartbeatTimers()` arms
*every* node at `failover_heartbeat_ttl` (5m) rather than its computed
TTL — while clients independently back off when they see `ErrNoLeader`
(`getHeartbeatRetryIntv`, comment: "In the case that there is a leadership
loss, we will have our heartbeat timer reset to a much larger threshold,
so do not put unnecessary pressure on the new leader"). Both halves are
needed; one alone still produces a herd.

Worked at defaults: 500 clients → floor still binds, 10–20s effective.
5,000 clients → base 100s, **100–200s effective**. Steady-state aggregate
lands near `max_heartbeats_per_second / 1.5` (~33/s) because the uniform
doubling stagger costs half the target rate.

**What we take:** server-computed interval carried on the heartbeat reply,
derived from fleet size; a floor; a failover grace. **What we reject:** the
`[base, 2*base)` stagger — it throws away a third of the target rate and
makes the effective interval unpredictable to the operator (see §6.4).

### Consul — a different architecture, and a log2 answer for the write path

Consul deliberately runs no central heartbeat write path. Liveness is
Serf/SWIM gossip: `probe_interval` **1s** LAN / **5s** WAN,
`probe_timeout` 500ms/3s, `suspicion_mult` 4/6, `gossip_nodes` 3/4. Per
node message rate is flat regardless of fleet size; suspicion timeout
scales as `4 * log(N+1) * ProbeInterval`, so **detection time grows
logarithmically while per-node cost stays constant**. Lifeguard adds a
self-throttle (`AwarenessMaxMultiplier: 8`) when the local node is CPU- or
network-starved.

The part of Consul that *does* hit the servers — anti-entropy state sync —
scales its own cadence, `agent/ae/ae.go`:

```go
const scaleThreshold = 128
func scaleFactor(nodes int) int {
    if nodes <= scaleThreshold { return 1.0 }
    return int(math.Ceil(math.Log2(float64(nodes))-math.Log2(float64(scaleThreshold))) + 1.0)
}
```

Base interval 1m, doubling per octave above 128 nodes, randomly staggered
per run. Consul session TTLs (range 10s–24h) are **doubled server-side**
before arming the expiry timer, with the contract stated as a lower bound
only: "Consul will not expire the session before the TTL is reached, but
it is allowed to delay the expiration past the TTL". Documented ceiling:
**5,000 client agents per datacenter**, with the caveat that the real
constraint is churn, watched via `consul.serf.queue.Intent`.

Consul also ships the *identical* `RateScaledInterval` / `RandomStagger`
helpers as Nomad in `lib/cluster.go`. The `n/rate` shape is a HashiCorp
house pattern, tested at scale in two products.

**What we take:** the "server may extend, never shorten without warning"
TTL contract, and the idea that a quantized/stepped scaling function
(Consul's octaves) is calmer than a continuous one. **What we reject for
now:** gossip. It is the right answer to this problem and it is D8; it
changes where liveness lives, and this plan deliberately does not.

> **Now owned elsewhere.** Moving liveness off the raft path is specified
> in [`liveness-off-the-raft-path-plan.md`](liveness-off-the-raft-path-plan.md),
> written after this one. That plan owns the failover-grace behavior this
> plan's phase 3 describes; where the two disagree, it wins. This plan
> still owns cadence itself — the interval workers are served, the sizing
> guidance, and the warnings.

**Sources.** [KEP-589](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/589-efficient-node-heartbeats/README.md) ·
[K8s nodes concept](https://kubernetes.io/docs/concepts/architecture/nodes/) ·
[kubelet config v1beta1](https://kubernetes.io/docs/reference/config-api/kubelet-config.v1beta1/) ·
[kube-controller-manager flags](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/) ·
[large-cluster guidance](https://kubernetes.io/docs/setup/best-practices/cluster-large/) ·
[Nomad server config](https://developer.hashicorp.com/nomad/docs/configuration/server) ·
[nomad/heartbeat.go](https://github.com/hashicorp/nomad/blob/main/nomad/heartbeat.go) ·
[helper/cluster.go](https://github.com/hashicorp/nomad/blob/main/helper/cluster.go) ·
[Consul gossip](https://developer.hashicorp.com/consul/docs/concept/gossip) ·
[Consul scale](https://developer.hashicorp.com/consul/docs/architecture/scale) ·
[consul/agent/ae/ae.go](https://github.com/hashicorp/consul/blob/main/agent/ae/ae.go)

---

## 3. Jobs to be done, by persona

**Cluster operator**

- When I am deciding how much control-plane hardware to buy for a fleet of
  a given size, I want a number I can multiply, so I find out my limit
  before my nodes do.
- When I add the four-hundredth machine to a cluster that was fine at
  three hundred, I want to be told that I have crossed a line, in a place
  I will actually see it, so I am not debugging a fake outage at 3am.
- When my control plane cannot keep up, I want machines that are plainly
  healthy to stay in the cluster, so I do not lose a thousand running
  services to a bookkeeping problem.
- When I run one big pool of cheap batch machines and one small pool of
  latency-sensitive ones, I want the batch machines to check in less often
  without making the small pool slower to react, so one pool's size does
  not set the other's failure-detection speed.

**On-call responder**

- When the dashboard fills with nodes going away, I want to know
  immediately whether the machines are gone or the control plane is just
  behind, so I stop the right thing.
- When I am told the cluster is "backed up", I want one number that says
  how far behind, so I can tell whether it is getting better.

**Job author**

- When my service is running fine on a machine that is running fine, I do
  not want it restarted somewhere else because two computers were slow at
  talking to each other.

**Legion maintainer**

- When I publish a throughput number against Nomad or Kubernetes, I want
  the heartbeat cadence that number was achieved at printed next to it, so
  nobody can accuse us of buying throughput by quietly checking in less
  often.

*Not in scope:* the security/platform admin persona (cadence is not an
authorization surface) and the autonomous-agent persona beyond the
ordinary `--json` requirement on the one new read-only command.

### User scenarios the design must serve

1. **Sizing before deploying.** An operator with 3 medium managers and
   plans for 800 nodes reads one page, does one multiplication, sets one
   config value, and is correct on the first day.
2. **Growing past the line.** A cluster grows from 150 to 400 nodes over a
   quarter. Somewhere in there, the operator gets a warning naming the
   fleet size, the achieved fraction, and the value to set — before any
   node is falsely marked lost.
3. **Surviving the line anyway.** The operator missed both. The cluster
   hits 1000 nodes. Nodes must **not** be falsely marked lost; the cluster
   must degrade to slower failure detection and say so, loudly.

---

## 4. Goals and non-goals

### Goals

- **No false loss under control-plane saturation.** A healthy node with a
  healthy worker is never marked lost because the manager is busy. This is
  the one hard goal; everything else serves it.
- The lost-node window is **derived from the cadence that node is actually
  running at**, per node, never from a global constant.
- An operator can size a fleet from published numbers plus one
  measurement of their own hardware.
- One metric answers "is the fleet keeping up?" and one warning fires
  before, not during, the collapse.
- A published Legion throughput number always carries the cadence it was
  achieved at.

### Non-goals

- **Gossip/SWIM liveness, or moving liveness out of raft into leader-local
  soft state.** That is architecture item D8, it awaits the AWS campaign,
  and it changes where liveness lives. This plan makes the current shape
  survivable and observable; it does not relocate it.
- **A Kubernetes-style lease/state split.** Same reason: it is a storage
  decision, and group commit already removed the per-heartbeat write from
  the critical path that the K8s split was aimed at.
- **Autoscaling the control plane.** Legion does not manage its own
  managers.
- **Changing the *default* 2s cadence or 10s window** for clusters that
  are not near their ceiling. Small clusters keep exactly today's
  behavior, bit for bit.
- **Per-node operator-set cadence.** Cadence is per worker type (already
  true) or manager-derived. A third source would make the window
  arithmetic unexplainable.
- **Adapting cadence to measured latency or saturation.** Explicitly
  rejected in §6.4 — it is a control loop and it will oscillate.

---

## 5. Background — what exists today

| Thing | Where | Today |
| --- | --- | --- |
| Default cadence | `src/constants.rs:80` | `WORKER_HEARTBEAT_DURATION_SEC = 2` |
| Lost threshold | `src/constants.rs:72` | `DEF_LOST_NODE_AT_SEC = 10`, global const |
| Cadence resolution | `src/worker/daemon.rs:585` | `heartbeat_interval_for`, worker-type `heartbeatSeconds` clamped `1..=30`, else default |
| Jitter | `src/worker/daemon.rs:600` | `jittered_heartbeat_interval`, ±10% per beat |
| Worker loop | `src/worker/daemon.rs:619-670` | `start_heartbeats`; failures never exit the loop, exponential backoff |
| Config validation | `src/manager/daemon.rs:1944` | rejects outside `1..=30`, warns above 3 (naming the fixed 10s window) |
| Handler | `src/manager/server.rs:5095` | `heartbeat`; worker-plane ACL, `update_node_heartbeat`, then reply construction |
| Liveness tick | `src/manager/daemon.rs:~683` | status-lane leader only; `nodes_to_mark_lost` → `mark_nodes_as_lost` → `recheck_lost_nodes` |
| Lost decision | `src/manager/daemon.rs:974-1010` | one global `unix_cutoff`; startup grace keyed on **process** uptime |
| Cadence served to worker | `src/manager/server.rs:5248` | `worker_pre_register` copies `heartbeat_seconds` from the worker type |
| Reply carrier | `proto/legion.proto:2670` | `HeartbeatReply` — already carries desired allocations, endpoints, stop notices, claim verdicts |
| Group commit | shipped | concurrent heartbeats coalesce into one status-lane commit, ≤128 |
| Metrics | `src/manager/telemetry.rs:678` | `legion_heartbeats_total`; `legion_nodes{status}`; `legion_store_coalesce_*{path="heartbeat"}` |
| Sim fleet | `src/worker/simulate.rs:282` | shares `heartbeat_interval_for` + jitter with the real worker "so the cadence contract is shared, not forked" |
| Self-fence | `src/structs/job.rs:1050` | `DEFAULT_SELF_TERMINATE_AFTER = 90s` |

Two facts from that table shape everything below. **The reply already
exists and is already a rich, per-node, every-beat channel** — adding a
served interval to it is nearly free in protocol terms. And **the
cadence-resolution helper is deliberately shared with the simulated
fleet**, which means any change here is automatically exercised by the
benchmark rig rather than needing a parallel implementation.

---

## 6. Proposed design

The three options in the brief are not alternatives. They are layers, and
the order matters: **you cannot ship an adaptive controller before you can
measure whether it is working**, and the metric that powers the warning is
the same metric that validates the controller. Each layer is independently
valuable and independently shippable; a cluster can stop at any of them.

### 6.1 The one metric everything hangs off: heartbeat freshness

Both the warning and the controller need to know "is the fleet keeping
up?". The cheap, correct answer is already sitting in the liveness tick,
which scans every node's heartbeat clock every 2 seconds anyway:

```
freshness_ratio = |{ active nodes with last_heartbeat_unix >= now - 1.2 × served_interval }|
                  ─────────────────────────────────────────────────────────────────────────
                  |{ active nodes }|
```

`1.2` covers the ±10% jitter plus a beat's worth of slop. `1.0` means every
node checked in on time. The measured AWS run reads **0.22**. False loss
begins at `served_interval / lost_window`, so the ratio and the danger
threshold are in the same units, which is what makes it explainable.

This is derived from the *store's* view, not from an RPC counter, so it is
correct across managers and across a forwarded/leader split — a per-process
`legion_heartbeats_total` rate is not, under `lanes`. It costs one integer
comparison per node per tick, on a scan that already happens.

New metrics (all gauges, on the status-lane leader; `0` elsewhere):

- **`legion_heartbeat_freshness_ratio`** — the above. **The number to
  alert on.**
- **`legion_heartbeat_served_interval_seconds`** — the interval the
  manager is currently telling workers to use (in phases 1–2, the
  configured one).
- **`legion_heartbeat_lost_window_seconds`** — the window derived from it.
- **`legion_heartbeat_sustainable_nodes`** — `freshness-derived` estimate
  of this control plane's ceiling at the current interval; the sizing
  formula, computed live.

Plus one counter: **`legion_nodes_marked_lost_total`**, so "nodes went
away" is distinguishable from "nodes are away" (`legion_nodes{status}`).

### 6.2 Option (a): the sizing formula and table — *ship it, but it is not enough alone*

**What the user experiences.** A new page,
`docs/heartbeat-cadence-and-scale.md`, opening with one multiplication:

```
sustainable_nodes  ≈  R × I

  R = heartbeat commits/second this control plane sustains
  I = heartbeat interval in seconds
```

and its inverse, with a headroom factor `H` (recommend 2, i.e. run at 50%
of ceiling so a burst does not become an outage):

```
heartbeatSeconds  ≥  H × N / R
lost window        =  missedBeatsBeforeLost × heartbeatSeconds   (§6.4)
```

The measured table, for the benchmarked `R ≈ 80` (3× m7i.xlarge, 3-voter
`lanes`, EBS gp3):

| Fleet | Min interval (H=2) | Set `heartbeatSeconds` | Resulting lost window |
| ---: | ---: | ---: | ---: |
| 100 | 2.5s | 3 | 15s |
| 160 | 4s | 4 | 20s |
| 250 | 6.3s | 7 | 35s |
| 500 | 12.5s | 13 | 65s |
| 1000 | 25s | 25 | 125s |
| 2000 | 50s | **no legal value** — clamp is 30 | — |

Plus the measurement recipe, because `R` is not a constant: throughput per
manager is roughly `worker_threads / heartbeat_handler_latency`, and
handler latency is dominated by per-commit fsync. Faster storage and more
vCPUs move `R`; a 5-voter raft moves it down. The recipe uses the existing
simulated fleet (`legion start worker --simulate`, which shares the real
cadence path) ramped against the real control plane, reading
`legion_heartbeat_freshness_ratio` — `R` is the largest `N / I` at which
the ratio holds ≥ 0.95, cross-checked against
`legion_raft_propose_apply_seconds{lane="status"}` and
`legion_store_coalesce_batch_size{path="heartbeat"}`.

**Interaction with lost-node detection.** This is where documentation
alone breaks. Every row of that table above 3s is *illegal today*: the
lost window is a fixed 10s, so `heartbeatSeconds: 13` means one missed
beat is a dead node. Option (a) is only truthful if the window moves with
the cadence — which is phase 2's work. **A sizing table shipped against
today's fixed window would be actively wrong advice.**

**Failure modes.** The operator does not read it. The operator reads it,
uses our `R` instead of measuring theirs, and is wrong by 3× in either
direction. The cluster grows past the size the operator computed for and
nothing tells them. `R` degrades under an unrelated load spike and the
static number no longer holds.

**Cost to build.** Small: one doc page, plus the metrics in §6.1 to make
the recipe runnable. Days, not weeks. The real cost is that it is a
*prerequisite* — the table cannot be published until the window is coupled.

**Verdict: necessary, insufficient.** It converts an unknown into a known,
and it is how the operator sizes hardware, which nothing automatic can do
for them. It does nothing at 3am and its last row is "there is no answer".

### 6.3 Option (b): the warning — *ship it, and it is the honest floor*

**What the user experiences.** Two things.

At config load (an upgrade of the existing warning, which currently cites
a window that phase 2 makes movable):

```
WARN worker_type=batch heartbeat_seconds=13 lost_window=65s
  heartbeatSeconds 13 gives a 65s lost-node window (5 missed beats).
  Real node failures on this worker type take up to 65s to detect.
```

At runtime, on the status-lane leader, when
`legion_heartbeat_freshness_ratio < 0.5` for 3 consecutive ticks,
rate-limited to once per 60s:

```
WARN nodes=1000 served_interval=2s freshness=0.22 lost_window=10s
  Heartbeat cadence is not being sustained: 22% of this fleet checked in
  within one interval. Healthy nodes will be marked lost when freshness
  falls below 0.20. Raise heartbeatSeconds to >= 25 for a fleet this size,
  or add control-plane capacity.
  See docs/heartbeat-cadence-and-scale.md
```

The remedy is *computed*, not generic: `ceil(H × N / R_observed)`, where
`R_observed` is derived from the freshness ratio and current fleet size.
The operator is handed the number, not the formula.

And a read-only command, so the answer is available without a Prometheus
scrape:

```console
$ legion node cadence
Fleet             1000 nodes (987 active)
Served interval   2s
Lost window       10s  (5 missed beats)
Freshness         0.22  ── FALLING BEHIND (false loss below 0.20)
Sustainable       ~160 nodes at 2s on this control plane
Suggested         heartbeatSeconds: 25   (lost window 125s)
```

`--json` emits the same fields through `output::print_success`, per the
agent-readiness rule.

**Interaction with lost-node detection.** None directly — a warning
changes no behavior. That is both its virtue (zero risk) and its flaw: it
fires *while the cluster is already breaking*, and the remedy
(`heartbeatSeconds` in a worker-type config) requires an edit plus workers
picking it up at pre-registration. On a fleet that is already saturated,
that propagation is not fast.

**Failure modes.** Alert fatigue if the threshold is too generous
(mitigated by the 3-tick debounce and the 60s rate limit). Ratio is noisy
on tiny fleets — suppress below 20 nodes. And the honest one: **the
warning is correct and the cluster still falls over**, because knowing is
not fixing.

**Cost to build.** Small-to-moderate: the metrics from §6.1, a debounced
warn on an existing loop, one new RPC + CLI verb + capabilities entry +
MCP tool + `tests/acl_endpoints.rs` row (`node`/`read`/`any`).

**Verdict: ship it, and it is the last line of defense even after (c).**
An adaptive controller that has hit its `maxInterval` ceiling needs
exactly this warning, saying exactly the same thing.

### 6.4 Option (c): manager-dictated adaptive cadence — *ship it, opt-in, derived from fleet size only*

**What the user experiences.** One manager-config block, off unless set:

```jsonnet
// Heartbeat cadence and node liveness (docs/heartbeat-cadence-and-scale.md).
heartbeat: {
  // Aggregate heartbeat rate this control plane will accept. When set, the
  // manager computes each worker's interval from fleet size and serves it
  // on the heartbeat reply, so cadence backs off as the fleet grows instead
  // of healthy nodes being marked lost. Unset (the default) = workers keep
  // their worker-type `heartbeatSeconds` and nothing adapts.
  // Measure yours with `legion node cadence`; the benchmarked 3-voter
  // m7i.xlarge control plane sustains ~80.
  // targetRatePerSecond: 80,

  // Floor and ceiling on the served interval. The floor is never crossed
  // however small the fleet; the ceiling is where the manager gives up and
  // warns instead of backing off further.
  minInterval: "2s",
  maxInterval: "120s",

  // Missed beats before a node is lost. The lost window is ALWAYS this
  // times that node's served interval — 5 x 2s = the historical 10s.
  missedBeatsBeforeLost: 5,

  // After acquiring status-lane leadership, mark nothing lost for this
  // long, so an election is not a fleet-wide outage (Nomad's
  // failover_heartbeat_ttl). Default: max(60s, 2 x maxInterval).
  // failoverGrace: "240s",
}
```

Wire surface, one new optional field on the reply the worker already gets
every beat:

```proto
message HeartbeatReply {
  // ...
  // The interval the manager wants this worker to heartbeat at, in
  // seconds. Present only when the manager runs adaptive cadence
  // (`heartbeat.targetRatePerSecond`); absent means "keep using your
  // worker-type `heartbeatSeconds`". The manager derives this node's
  // lost-node window from the same value, so a worker that honors it is
  // never penalized for the slower cadence. A worker that ignores it
  // (older build) is also never penalized: the manager only shortens a
  // node's window after observing a beat at the faster rate.
  optional uint32 next_interval_seconds = N;
}
```

**The controller.** Fleet size in, interval out. Nothing else:

```
target  = clamp(ceil(H × active_nodes / targetRatePerSecond), minInterval, maxInterval)
served  = next rung ≥ target on the ladder [2,3,5,8,13,21,34,55,89,120]s
```

Recomputed once per liveness tick from the active-node count the tick
already has. Three properties are deliberate:

- **The input is fleet size, not saturation.** This is the single most
  important choice in the plan. Deriving cadence from *observed latency or
  freshness* would be a genuine closed control loop: back off → load drops
  → freshness recovers → speed up → load returns → back off. It would
  oscillate, its period would depend on the tick rate, and the lost window
  would breathe with it. Fleet size is a slowly-changing, externally-driven
  input, so `interval = f(N)` is a **pure function**, not a controller. It
  is reproducible, it is predictable from the config, and an operator can
  compute what the manager will do without running it. This is Nomad's
  choice too (`n / max_heartbeats_per_second`), and it is why Nomad's
  version has no oscillation story to tell.
- **Quantized to a ladder, not continuous.** A continuous `N/R` changes the
  served interval on almost every tick, which means the per-node lost
  window changes on almost every tick, which means every node's row is
  rewritten constantly. The ladder (roughly Fibonacci, ~1.6× per rung —
  Consul's octave trick with finer steps) means a fleet has to grow ~60%
  to move a rung. Hysteresis on top: step **up** immediately when `target`
  exceeds the current rung; step **down** only after `target` has stayed
  below the next-lower rung for 5 consecutive minutes.
- **Bounded by `maxInterval`, and it *warns* at the bound.** When the
  computed target exceeds `maxInterval`, the manager serves `maxInterval`
  and emits the §6.3 warning. Backing off is not infinite; past a point,
  the answer is more control plane.

**Interaction with lost-node detection — the correctness core.** Backing
off cadence *must* move the window, and the asymmetry is what makes it
safe:

1. `node_health` gains `lost_after_seconds INTEGER` (nullable; NULL falls
   back to `DEF_LOST_NODE_AT_SEC`, so pre-migration rows behave exactly as
   today and age out). `nodes_to_mark_lost` compares each node against
   **its own** window instead of one global cutoff.
2. When the served interval **increases**, a node's window is widened
   **immediately** — the manager writes the new window the moment it
   decides to slow down, *before* the worker has been told. A worker that
   never gets the message is protected.
3. When the served interval **decreases**, a node's window is narrowed
   **only after the manager observes a beat from that node at the faster
   cadence**. A worker that missed the speed-up is never punished for
   obeying the last instruction it received. This is Consul's session-TTL
   contract — "will not expire before the TTL, but may delay past it" —
   applied per node.
4. Per-node windows are *required* regardless, because `heartbeatSeconds`
   is per **worker type**: a cluster with a 2s latency-sensitive pool and a
   60s batch pool cannot have one global window. This is the deferred item
   from the predecessor plan, and it is what makes the operator's fourth
   job-to-be-done possible at all.

**Thundering herd.** Real, and specific: when the served interval changes,
every worker learns it within one old-interval and would otherwise
re-phase onto the same boundary. The existing ±10% steady-state jitter is
a slow random walk and will not decorrelate a fleet quickly. The fix is a
**one-shot full-width re-phase**: on the beat where a worker's served
interval changes, it sleeps `U[0, new_interval)` once, then returns to
`new_interval ± 10%`. That spreads the fleet uniformly across the new
period in exactly one beat, at the cost of one interval of unevenness.

We deliberately **do not** adopt Nomad's permanent `[base, 2×base)`
stagger. It costs a third of the target rate forever (Nomad's effective
aggregate is ~`rate/1.5`), it makes the operator's `N/R` arithmetic wrong
by a variable factor, and it doubles the worst-case detection time. A
one-shot re-phase buys the same decorrelation without paying steady-state.

**Feedback loops — the ones that remain.** Deriving from `N` removes the
cadence↔saturation loop, but two couplings survive and are handled:

- *Loss → reschedule → load → loss.* Already present today, and this is
  what made the 1000-node run cascade. Cadence adaptation dampens it
  (fewer heartbeats under the same load) but does not remove it. The
  Kubernetes eviction-side circuit breaker is the right complement: **when
  more than 50% of active nodes would be marked lost in one tick, mark
  none of them and log at `error`.** A control plane does not lose half its
  fleet in 2 seconds; if it looks like it did, the control plane is what
  broke. This is `stateFullDisruption` reasoning, applied at detection
  rather than eviction because Legion has no zone concept.
- *Leadership change → mass loss.* `nodes_to_mark_lost`'s grace is keyed on
  process uptime and does not fire on a mid-life status-lane leadership
  acquisition (§1). Fix: the grace clock resets on leadership **acquisition**,
  not just process start, and runs for `failoverGrace`. Straight from
  Nomad's `initializeHeartbeatTimers`.

**Benchmark fairness.** The concern is legitimate: a system that quietly
slows its own heartbeats to score better on throughput is cheating. Three
rules, and they are non-negotiable design constraints, not documentation:

1. **Off by default.** `targetRatePerSecond` unset means nothing adapts.
   Same posture as `score_sample` and `preemption.mode` — Legion's house
   style for anything that trades quality for scale.
2. **Every benchmark run records the served interval and the freshness
   ratio** in its result JSON, alongside throughput. A number without its
   cadence is not publishable.
3. **Comparative runs pin it.** Legion-vs-Nomad-vs-K8s runs in the AWS
   campaign either pin all three to the same interval, or report all three
   systems' *achieved* cadence — noting that Nomad's own `n/rate` scaling
   means an unpinned Nomad is doing the same thing, so a Legion that does
   not adapt is being compared *unfavourably*, not fairly.

**Failure modes.** Detection of genuine failures gets slower in exact
proportion to the back-off — at 1000 nodes and `R=80`, a dead machine
takes up to 125s to notice instead of 10s. This is the real price and it
is the same price Nomad pays (100–200s at 5k nodes). Second: a worker on
an older build ignores `next_interval_seconds`, keeps beating at 2s, and
contributes load the controller thinks it removed — handled by rule 3
above (its window is widened anyway) and visible as a freshness ratio that
does not recover after a rung change. Third: cadence changes make cluster
behavior time-varying, so an operator debugging "why did detection take
90s" needs `legion node cadence` to answer it — which is why (b) ships
first.

**Cost to build.** The largest of the three, but bounded: one proto field,
one nullable column plus migration, a ~40-line pure controller function
with the ladder and hysteresis, per-node window comparison in
`nodes_to_mark_lost`, the re-phase in the worker sleep, the leadership
grace, the disruption circuit breaker. No new subsystem, no new store
lane, no new RPC. The predecessor plan's shared
`heartbeat_interval_for`/jitter helpers mean the simulated fleet picks it
all up for free.

**Verdict: ship it, opt-in, third.**

### 6.5 Recommendation

**All three, phased in the order (a)+metrics → (b) → (c), with (c)
opt-in.** They are layers, not alternatives, and each is a prerequisite
for the next being trustworthy.

The reasoning, tied to the product goals:

- **Correctness first.** The 1000-node run was 10% of margin from marking a
  healthy fleet dead. Nothing in options (a) or (b) prevents that — a doc
  and a warning are advice. Only (c), and specifically the per-node window
  that moves with the cadence, makes the failure mode impossible rather
  than merely announced. That is the reason (c) is on the list at all.
- **Simplicity / ease.** The operator-facing model stays two sentences:
  *"the manager tells workers how often to check in, and a node is lost
  after 5 missed check-ins."* Every number in every layer derives from
  that one relationship. `missedBeatsBeforeLost: 5 × 2s` reproduces
  today's 10s exactly, so a small cluster's behavior is unchanged and the
  new concept costs nothing to a user who never hits the wall. Compare
  Kubernetes, where an operator reconciles `nodeLeaseDurationSeconds`,
  `nodeStatusUpdateFrequency`, `nodeStatusReportFrequency`,
  `--node-monitor-grace-period`, and four eviction-rate flags — and where
  the flag help says one "must be N times more than" another with nothing
  enforcing it.
- **Performance.** Adapting on **fleet size** rather than measured
  saturation keeps this a pure function of a slow input, so it neither
  oscillates nor games a benchmark. Off-by-default plus mandatory cadence
  reporting in every result means Legion's published numbers stay
  defensible — which matters more here than in most features, because the
  AWS comparative campaign is the point.
- **Extensibility.** Nothing here forecloses D8. Gossip liveness and a
  lease/state split both remain open, and both become *easier* with a
  per-node window already in the schema and a freshness metric already
  measuring whether liveness works. If the AWS campaign says the answer is
  gossip, this plan's phases 1–2 are still the instrumentation gossip would
  need, and phase 3 becomes the fallback path for clusters that do not run
  it.

The one thing this plan will not do is ship (c) *first* because it is the
interesting one. An adaptive controller you cannot observe is worse than a
fixed interval you can.

---

## 7. Proto, struct, config, and persistence changes

**Proto** (`proto/legion.proto`), with full public comments per the
documentation rule:

- `HeartbeatReply.next_interval_seconds` — `optional uint32`. New field
  number; absent = "keep your configured interval". Phase 3.
- `GetHeartbeatCadenceRequest` / `GetHeartbeatCadenceReply` and
  `rpc GetHeartbeatCadence` — read-only fleet cadence report backing
  `legion node cadence`. Fields: `active_nodes`, `served_interval_seconds`,
  `lost_window_seconds`, `freshness_ratio`, `sustainable_nodes_estimate`,
  `suggested_heartbeat_seconds`, `adaptive_enabled`, `at_max_interval`.
  Phase 2.

**Manager config** (`src/structs/config.rs`, `configs/manager.jsonnet`):
the `heartbeat { targetRatePerSecond, minInterval, maxInterval,
missedBeatsBeforeLost, failoverGrace }` block from §6.4. All optional;
omitting the block reproduces today exactly.

**Worker config:** none. `heartbeatSeconds` keeps its meaning as the
worker-type cadence and becomes the value used when the manager serves
nothing. Its clamp widens from `1..=30` to `1..=300` in phase 2, with
validation that rejects any value where
`missedBeatsBeforeLost × heartbeatSeconds` would exceed the smallest
`stateful.onLost.selfTerminateAfter` in play (default 90s,
`src/structs/job.rs:1050`) — the §1 hazard, caught at config load rather
than discovered as spontaneous stateful task death.

**Persistence:** `ALTER TABLE node_health ADD COLUMN lost_after_seconds
INTEGER` (nullable; NULL = `DEF_LOST_NODE_AT_SEC`). Additive, no backfill,
no downgrade hazard — an older manager ignores the column.

**Job config:** none. `configs/jobs/maximal.jsonnet` is unaffected; the
only jobspec interaction is the `stateful.onLost.selfTerminateAfter`
validation above, which is a new *check* on an existing field.

**Capabilities / MCP:** `node cadence` gets a `catalog()` entry
(`read_only_safe: true`, `mutates: false`) and an MCP tool, per the
agent-readiness rule. `GetHeartbeatCadence` gets its
`tests/acl_endpoints.rs` row: `AclResource::Node`, `AclAction::Read`,
`AclScope::any()`.

---

## 8. Implementation phases

**Phase 1 — Measure (no behavior change).**
The four gauges and one counter from §6.1, computed on the existing
liveness tick. `docs/heartbeat-cadence-and-scale.md` with the formula, the
measurement recipe, and the AWS numbers — but **not** the sizing table
above 3s, which is not legal advice until phase 2. `docs/metrics.md`
updated. *Valuable alone:* an operator can find their ceiling before it
finds them.

**Phase 2 — Couple the window, and warn.**
`lost_after_seconds` column + migration; `nodes_to_mark_lost` compares
per-node windows; `missedBeatsBeforeLost` config with the default 5
reproducing 10s; clamp widened to `1..=300` with the `selfTerminateAfter`
cross-check; the runtime freshness warning; `GetHeartbeatCadence` +
`legion node cadence` + capabilities + MCP; the sizing table published in
full. *Valuable alone:* `heartbeatSeconds` becomes a knob an operator can
actually use, which it is not today, and the warning fires before the
collapse. **A cluster can stop here permanently and be correct.**

**Phase 3 — Safety valves.**
Status-lane leadership resets the grace clock (`failoverGrace`); the
mass-disruption circuit breaker (>50% would be lost in one tick → mark
none, log `error`). *Valuable alone:* both fix live hazards that exist
today with or without adaptive cadence, and neither depends on it.

**Phase 4 — Adaptive cadence.**
`next_interval_seconds` on the reply; the ladder + hysteresis controller;
the widen-now / narrow-on-confirmation window rule; the one-shot re-phase
in the worker sleep; benchmark result JSON carries served interval and
freshness. Off unless `targetRatePerSecond` is set. *Valuable alone:*
clusters past their documented ceiling stop being a correctness problem.

Phase 3 before phase 4 is deliberate: the leadership grace and the
circuit breaker are the things that keep phase 4's back-off from having a
sharp edge, and they are worth having even if phase 4 never ships.

---

## 9. Testing plan

**Unit.**
- The controller as a pure function: `(N, target_rate, min, max, H)` →
  rung, across the table in §6.2; monotone in `N`; never below `min`,
  never above `max`; hysteresis does not step down before its dwell.
- Per-node window: node A at 2s and node B at 60s in the same tick — A
  lost at 10s, B alive at 55s, B lost at 305s. This is the test that would
  have failed today.
- Widen-immediately / narrow-on-confirmation: a node whose served interval
  drops keeps the wide window until a beat arrives at the new cadence.
- NULL `lost_after_seconds` behaves exactly as `DEF_LOST_NODE_AT_SEC`.
- Re-phase bounds: first sleep after a change ∈ `[0, new)`, subsequent ∈
  `[0.9×new, 1.1×new]`.
- Circuit breaker: 51% would-be-lost marks none; 49% marks all 49%.
- Leadership grace: acquisition at `t` suppresses marking until
  `t + failoverGrace`, then resumes.
- Freshness ratio arithmetic, including the <20-node suppression.
- Config validation: `heartbeatSeconds × missedBeatsBeforeLost` vs.
  `selfTerminateAfter`.

**Integration.**
- A worker that ignores `next_interval_seconds` (simulating an older
  build) is never marked lost while beating at its own cadence.
- Existing grace-window tests in `src/manager/daemon.rs`, the
  `raft_lanes.rs` status-lane suites, and the
  `metrics_endpoint.rs` heartbeat-counter test all stay green unchanged —
  the default path must be bit-for-bit identical.
- `tests/acl_endpoints.rs` gains the `GetHeartbeatCadence` row; the
  `every_manager_rpc_is_classified` test must pass without adding an
  `Open` classification.

**Scale / regression.** The heartbeat benchmark
(`tests/heartbeat_scale_benchmark.rs`) gains a cadence-collapse case:
ramp the simulated fleet past the control plane's `R` and assert
**zero** nodes marked lost with adaptive on, versus the reproduced
collapse with it off. That assertion is the whole plan in one test.
Per the repo rule, phase 4 ships with before/after benchmark numbers on
both freshness ratio and scheduler throughput (the latter must not
regress — heartbeats getting cheaper should not make planning slower).

---

## 10. Rollout and backward compatibility

Every phase is additive and default-inert.

- **Existing clusters see no change** until an operator sets something.
  `missedBeatsBeforeLost: 5` × the 2s default is the historical 10s; the
  new column is NULL and falls back to the same constant.
- **Mixed versions.** An old worker against a new manager ignores
  `next_interval_seconds` and keeps its configured cadence — safe, because
  the manager widens windows without waiting for acknowledgement. A new
  worker against an old manager receives no field and uses its worker-type
  value, which is exactly today's behavior. No upgrade ordering is
  required beyond the existing managers-first rule in `docs/upgrades.md`.
- **The migration is additive** (`ALTER TABLE ... ADD COLUMN`), so an
  operator can roll a manager back; the older binary ignores the column.
- **Enabling adaptive cadence is a live, reversible edit.** Unsetting
  `targetRatePerSecond` returns every node to its worker-type cadence
  within one served interval, and windows narrow back on confirmation.
- **Documentation:** new page `docs/heartbeat-cadence-and-scale.md`
  (linked from `docs/README.md` and the root `README.md` doc list);
  updates to `docs/architecture-managers-workers.md` (its heartbeat
  section currently states the fixed 10s window as fact),
  `docs/metrics.md` (five new families), `docs/agent-readiness.md` (the
  new MCP tool and CLI verb), `docs/stateful-workloads.md` (the
  `selfTerminateAfter` interaction), and `docs/testing-and-benchmarks.md`
  (the cadence-collapse benchmark case, and the rule that published
  numbers carry their cadence). `internal_docs/TODO.md` gets the open
  items and, per phase, the checked-off ones.

---

## 11. Open questions

1. **Should adaptive cadence eventually default on?** *Lean: yes, after
   the AWS campaign, with `targetRatePerSecond` auto-derived from measured
   `R` rather than configured.* Off-by-default is right for the first
   release because the benchmark-fairness argument is strongest while the
   comparative numbers are being produced. The criterion for flipping: a
   campaign run showing a cluster past its ceiling surviving with adaptive
   on, with detection latency inside the documented bound, and the same
   run reproducing the collapse with it off.
2. **Ladder or continuous with hysteresis?** *Lean: ladder.* The ladder's
   virtue is that an operator can memorize it and predict the manager.
   Revisit if real fleets sit near a rung boundary and the 60%-growth
   granularity proves too coarse for the sustainable-node estimate.
3. **Should `missedBeatsBeforeLost` be per worker type rather than
   cluster-wide?** *Lean: no.* Cluster-wide keeps the model to one
   sentence, and the per-type cadence already gives per-type windows.
   Revisit if someone wants a fast-detect pool that beats slowly, which
   seems contradictory.
4. **Is 50% the right disruption threshold?** *Lean: yes, start there.*
   Kubernetes uses 55% per zone with a 3-node minimum; Legion has no zone
   concept, so a single cluster-wide 50% with a minimum fleet size of 20 is
   the closest honest analog. Should be configurable if anyone ever needs
   to tune it, but not in phase 1.
5. **Does the freshness ratio need to be per pool?** *Lean: not yet.* A
   cluster with a slow batch pool and a fast latency pool has two
   populations and one blended ratio, which could mask a problem in the
   fast pool. Deferred, and named as a known limitation on the public page.

---

## 12. Acceptance criteria

**Phase 1.** `legion_heartbeat_freshness_ratio` reads ≈1.0 on an idle
cluster and drops measurably under a simulated fleet past the ceiling.
`docs/heartbeat-cadence-and-scale.md` exists, and a reviewer can follow the
measurement recipe end-to-end on a local cluster and produce an `R`.

**Phase 2.** A worker type with `heartbeatSeconds: 20` runs for ten
minutes without being marked lost (impossible today). Two worker types at
2s and 60s coexist, each with its own window. The runtime warning fires
exactly once per 60s while freshness is below 0.5 and names a suggested
`heartbeatSeconds` that satisfies the formula. `legion node cadence` and
`legion node cadence -j` return the documented fields; `cargo test --test
acl_endpoints` is green.

**Phase 3.** A forced status-lane leadership change on a 200-node cluster
marks zero nodes lost. Killing 60% of a fleet at once marks zero nodes
lost and logs one `error`; killing 40% marks exactly those 40%.

**Phase 4.** The reproduction: 1000 simulated nodes against a control
plane measured at `R ≈ 80`. With `targetRatePerSecond` unset, the run
reproduces the collapse (freshness ≈0.22, nodes marked lost). With it set
to 80, **zero nodes are marked lost**, the served interval settles on the
25s-or-above rung within two ticks and does not oscillate over 30 minutes,
and the benchmark result JSON carries the served interval and freshness
alongside throughput. Scheduler throughput on the same run is within noise
of the phase-3 baseline.
