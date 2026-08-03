# Liveness Off the Raft Path

**Status: SHIPPED** (2026-08-03) — phases 2, 3 and 4 are built; phase 1's AWS
measurement is **not run** (see below). What shipped is on the public pages
[`docs/node-liveness.md`](../../docs/node-liveness.md) and
[`docs/heartbeat-cadence-and-scale.md`](../../docs/heartbeat-cadence-and-scale.md);
this plan stays as history.

**Deliberately not done, and open:**

- **Phase 1's attribution of the residual 16%** (`heartbeat-commit-probe.sh`
  on gp3 and NVMe at 1000 and 4000 nodes) and every re-measurement in §10.1.
  They need the AWS rig. `liveness.batchWindow` therefore ships parsed,
  validated, and **inert** — see the judgement-calls doc.
- **The `lost -> active` recovery does not flip `node.availability`.** The
  plan named `mark_node_ready` as the composite; that method writes
  `status = "ready"`, which every liveness query filters out, so the recovery
  keeps the status-lane write it always used. A recovered node becomes
  schedulable at re-registration, exactly as before. Normalizing `"ready"` to
  `"active"` is a separate change and an open TODO item.

This is architecture item **D8** from
[`scheduler-architecture-review-plan.md`](scheduler-architecture-review-plan.md),
promoted to its own plan now that the AWS campaign has produced the number
it was waiting on. It decides **where a node's liveness lives** and **what
about liveness is replicated**. It does not decide how often a worker
heartbeats or how long a node may be silent — that is
[`heartbeat-cadence-at-scale-plan.md`](heartbeat-cadence-at-scale-plan.md),
which deliberately named this question a non-goal ("Gossip/SWIM liveness,
or moving liveness out of raft into leader-local soft state … it changes
where liveness lives, and this plan deliberately does not").

Deliberately deferred, and named as non-goals in §4: gossip/SWIM liveness,
any change to the worker-plane wire protocol, any change to the default
cadence or lost window, and per-pool liveness reporting.

> **Judgement calls:** [`../calls/liveness-off-the-raft-path-plan_calls.md`](../calls/liveness-off-the-raft-path-plan_calls.md)

---

## 0. Which plan owns which decision

Two plans now touch node liveness. Neither is a superset of the other, and
a reader arriving mid-incident needs to know which one to open.

| Decision | Owner |
| --- | --- |
| How often a worker heartbeats (`heartbeatSeconds`, adaptive cadence) | cadence plan |
| How long a node may be silent before it is lost (`missedBeatsBeforeLost`, per-node windows) | cadence plan |
| The freshness metric, the sizing formula, `legion node cadence` | cadence plan |
| The mass-disruption circuit breaker (>50% would be lost → mark none) | cadence plan §6.4 — **this plan depends on it** |
| **Where the heartbeat timestamp is stored, and what replicates** | **this plan** |
| **What a manager that does not lead the status lane knows about liveness** | **this plan** |
| **What a newly elected status-lane leader believes about the fleet** | **this plan** |
| **The failover grace clock (`daemon.rs:978`)** | **this plan** — see below |

**One item moves.** The cadence plan's phase 3 includes "status-lane
leadership resets the grace clock". That hazard is unfixable in isolation
here: once liveness is leader-memory, a new leader inherits *nothing*, so
the grace clock is not a safety valve bolted on the side — it is the
rebuild mechanism. **This plan takes it.** The cadence plan's phase 3 keeps
the mass-disruption circuit breaker, which is independent and which this
plan consumes rather than duplicates. The cadence plan is uncommitted work
owned by another session and is not edited by this one; the cross-reference
is deliberately one-way.

Neither plan blocks the other. They compose: the cadence plan decides *when*
a node is late, this plan decides *who knows* it is late and how that
knowledge is kept.

---

## 1. Problem

Every worker heartbeat costs a full replicated raft commit, and a raft
commit fsyncs.

The path: `Manager::heartbeat` (`src/manager/server.rs:5095`) calls
`Store::update_node_heartbeat` (`src/storage/store.rs:5239`), which submits
to the heartbeat coalescer (`src/storage/coalesce.rs:62`) and then commits
`replicated(Lane::Status, "update_node_heartbeats", …)`
(`src/storage/store.rs:5266`). That entry is proposed, replicated to a
quorum, fsync'd to the status lane's log, and applied — to write one
integer per node.

### Measured, on real AWS (2026-08-03)

3-voter `lanes` topology, 1000 simulated nodes at the 2s default cadence,
via `benchmarks/aws/experiments/heartbeat-commit-probe.sh`. 500/sec is what
the fleet needs.

| Storage | Worker threads | Sustained hb/sec | Fraction of cadence |
| --- | ---: | ---: | ---: |
| gp3 EBS | 1 | 48.3 | 9% |
| gp3 EBS | 4 | 60.5 | 12% |
| gp3 EBS, **after the `spawn_blocking` offload** (`f24c6e0`) | 1 | 349.8 | 69% |
| gp3 EBS, **after the offload** | 4 | 422.6 | 84% |
| Local NVMe, pre-offload code | 1 | ~2000 | 100% |

Three results rule out the easy explanations:

- **It is not handler concurrency.** *Before* the offload, raising tokio
  worker threads 1 → 4 moved throughput 48 → 60/sec — a 4× increase in
  concurrency bought 25%, because every thread was blocked.
- **It is not replication.** `legion.RaftTransport/AppendEntries` p99 was
  **3 ms**. The network and the peers are fine.
- **It is the commit.** The 25× gap between gp3 and NVMe on identical code
  is the fsync, and the residual serialization is that **one OS thread per
  lane** serves every replicated op on that lane — `pipeline_loop`
  (`src/storage/raft/driver.rs:1449`), spawned once per lane at
  `src/storage/raft/driver.rs:3026-3038`. Heartbeats do not merely cost a
  commit; they queue behind, and in front of, every other status-lane write.

### What changed while this plan was being written

Commit `f24c6e0` ("Offload the heartbeat store write off the tokio runtime")
landed the cheapest available fix and it is a large one: `Manager::heartbeat`
called the blocking `Store::update_node_heartbeat` inline on a tokio worker
thread, which parked that thread for the whole commit *and* left every other
heartbeat stuck in the gRPC layer where the coalescer could not see it — so
batches never formed and `HEARTBEAT_BATCH_MAX = 128` was never approached.
Moved to `spawn_blocking` (`src/manager/server.rs:5108-5130`), the queue
builds, batches form, and one fsync amortizes across many beats. **7× on
gp3, to 84% of cadence.**

That changes the urgency of this plan but not its argument, and it is worth
being precise about which:

- **The correctness emergency is over.** At 0.84 achieved ratio the fleet is
  four times clear of the 0.20 false-loss threshold, where it was at 0.22.
  This plan is no longer racing a live incident.
- **The architecture is unchanged.** `R` is still a property of the storage
  device: gp3 reaches 84% of cadence for *1000* nodes, and the campaign's
  documented scale steps go to **10,000**. The control plane is still on
  local NVMe (`m6id.xlarge`, commit `a774fb0`) to buy the rest.
- **The remaining 16% is exactly what this plan removes.** The TODO entry
  for `f24c6e0` names it: *"the single `pipeline_loop` OS thread per lane
  plus the fsync itself"*. Both exist only because a heartbeat is a
  replicated write.

The probe's own hypothesis, written before it was run
(`benchmarks/aws/experiments/heartbeat-commit-probe.sh:1-31`), was: *"If the
ceiling reproduces here, it is commit-cost-bound and the fix is amortizing
fsync (bigger coalescer batches)."* It reproduced, and `f24c6e0` collected
most of that amortization. The rest of this plan is the argument that
amortizing the fsync has a ceiling, because the fsync should not be on this
path at all.

### Why a low achieved ratio is a correctness failure, not a slow request

Worked through in the cadence plan §1 and not re-derived here: false node
loss begins when `achieved_ratio < configured_interval / lost_window`,
which at defaults is `2s / 10s = 0.20`. The pre-offload 1000-node run
achieved **0.22** — 10% of margin away from marking a thousand healthy
machines dead, with a positive feedback term, because each mark costs a
reschedule eval per allocation, which is more control-plane load, which
lowers the ratio further.

That specific incident is closed by `f24c6e0` (0.84 today). What is *not*
closed is that the ratio is still a function of `R`, `R` is still a function
of the disk, and the fleet sizes in the campaign plan still divide into `R`
badly. The threshold does not move; only the distance to it does, and that
distance shrinks with every node added.

The mitigation currently in the tree is commit `a774fb0`, "Put the
control-plane raft store on local NVMe". That is a hardware workaround for a
software problem: it says the control plane may not use network-attached
storage, which is what almost every cloud operator will reach for first.

### The architectural asymmetry

| System | Steady-state heartbeat cost |
| --- | --- |
| **Nomad** | Resets an **in-memory** timer on the leader (`resetHeartbeatTimer` over `h.heartbeatTimers`, `nomad/heartbeat.go`). No raft write at all. |
| **Kubernetes** | Writes a small `Lease` object to etcd. Durable, but tiny after [KEP-589](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/589-efficient-node-heartbeats/README.md) shrank it from the 15 kB Node object, and etcd group-commits. |
| **Legion** | A full replicated commit through the status lane, fsync included. |

Legion pays the most, by architecture, for the least durable fact in the
system. That is the problem.

### What an operator has to do today

- Put the raft log on local NVMe, accepting that a lost instance store is a
  lost replica (`docs/raft-operations.md`) — which is what the campaign rig
  now does, or
- raise `heartbeatSeconds` — which cannot exceed 3 without shrinking
  detection margin to nothing, because the lost window is the fixed
  `DEF_LOST_NODE_AT_SEC = 10` (`src/constants.rs:72`), or
- cap the fleet at roughly `R × I` nodes per control plane, where `R` is a
  number they have to measure on their own hardware and which moves with
  their disk.

None of those is an answer to "I have 10,000 machines and three managers on
the network storage my cloud gave me", which is the campaign's top scale
step and a perfectly ordinary deployment.

---

## 2. Prior art

### Nomad — liveness is leader memory, rebuilt on election

This is the model the recommended design follows, and the closest fit for
Legion's shape.

`Node.UpdateStatus` on a steady-state heartbeat does **not** write raft. It
calls `resetHeartbeatTimer(id)`, which resets an in-memory `time.AfterFunc`
in `h.heartbeatTimers` on the leader. Raft is written only when the node's
*status* changes — `ready → down`, `down → ready`, `initializing → ready`.
Steady state is free.

The two halves that make it safe:

- **`initializeHeartbeatTimers()`** runs on leadership acquisition and arms
  **every** node at `failover_heartbeat_ttl` (**5m** by default) rather than
  its computed TTL. A new leader inherits no timer state, so it assumes
  everyone is alive and lets incoming heartbeats rebuild the truth.
- **Clients back off independently** when they see `ErrNoLeader`
  (`getHeartbeatRetryIntv`, comment: *"In the case that there is a
  leadership loss, we will have our heartbeat timer reset to a much larger
  threshold, so do not put unnecessary pressure on the new leader"*).

Nomad's leader is a single server, and every heartbeat RPC is forwarded to
it — Nomad has no lane concept, so "the leader" is unambiguous. That is the
one place Legion cannot copy directly (§6.3).

**What we take:** liveness in leader memory; replicate transitions only;
arm-everything-generously on election; the client-side back-off already
exists in Legion as `HEARTBEAT_RETRY_MIN`/`_MAX`.
**What we reject:** `failover_heartbeat_ttl` at 5 minutes. Legion's default
window is 10s, not 30s, and five minutes of blind trust after every election
is a larger regression in detection time than this plan needs to buy.

### Kubernetes — shrink the write, keep it durable

KEP-589's answer was a separate tiny `coordination.k8s.io/v1` Lease per node
in `kube-node-lease`, renewed at a fixed 10s (`nodeLeaseDurationSeconds`
40, renewed at `nodeLeaseRenewIntervalFraction` 0.25), with the heavy
`NodeStatus` write demoted to `nodeStatusReportFrequency` **5m** when
nothing changed. Result, verbatim: *"in 5k-node clusters, we are reducing
etcd write throughput from 150MB/min to 30MB/min."*

Kubernetes keeps liveness durable because the API server is stateless and
etcd is the only shared memory — there is no "the leader" whose RAM could
hold it. Legion has one (per lane), so the constraint does not bind.

**What we take:** the *shape* of the Lease split — a small, dedicated,
high-frequency record separated from the large low-frequency one. Legion
already has this split in the schema: `node_health` (status lane) versus
`node` (control lane), from the multi-raft persistence work. This plan
finishes the split by taking the highest-frequency column out of the
replicated record entirely.
**What we reject:** keeping the high-frequency record durable. It buys
nothing Legion needs — see §6.1 on what a heartbeat timestamp is actually
worth — and it is exactly the cost being measured in §1.

### Consul — gossip, and why the cost profile differs

Consul runs no central heartbeat write path. Liveness is Serf/SWIM:
`probe_interval` **1s** LAN, `probe_timeout` 500ms, `suspicion_mult` 4,
`gossip_nodes` 3. Per-node message rate is flat regardless of fleet size;
suspicion time grows as `4 × log(N+1) × ProbeInterval`, so **detection time
grows logarithmically while per-node cost stays constant**. Lifeguard adds a
self-throttle (`AwarenessMaxMultiplier: 8`) when the local agent is
starved. Documented ceiling: **5,000 client agents per datacenter** — note
that gossip does not remove the ceiling, it moves it.

**What we take:** nothing, structurally, in this plan. **What we reject and
why:** §6.6(d).

### Sources

[KEP-589](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/589-efficient-node-heartbeats/README.md) ·
[K8s nodes concept](https://kubernetes.io/docs/concepts/architecture/nodes/) ·
[nomad/heartbeat.go](https://github.com/hashicorp/nomad/blob/main/nomad/heartbeat.go) ·
[nomad/node_endpoint.go](https://github.com/hashicorp/nomad/blob/main/nomad/node_endpoint.go) ·
[Nomad server config](https://developer.hashicorp.com/nomad/docs/configuration/server) ·
[Consul gossip](https://developer.hashicorp.com/consul/docs/concept/gossip) ·
[Consul scale](https://developer.hashicorp.com/consul/docs/architecture/scale)

---

## 3. Jobs to be done, by persona

**Cluster operator**

- When I run a thousand machines, I want to keep my control plane on the
  ordinary network disks my cloud gives me, so I am not forced onto
  ephemeral local storage to make check-ins keep up.
- When one of my three control-plane machines takes over from another, I
  want every machine in my fleet to stay in the cluster, so a routine
  handover is not an outage.
- When I restart my control plane one machine at a time to upgrade it, I
  want nothing to be restarted anywhere else in the fleet, so a Tuesday
  upgrade is boring.
- When my machines and my control plane cannot reach each other for a
  moment, I want to be told that the *control plane* had a problem, rather
  than being told a thousand machines died at the same second.

**On-call responder**

- When I open the node list, I want the "is it alive" answer to be right,
  even if the "when did it last check in" clock is a little behind, and I
  want the page to tell me which is which so I do not chase a stale number.
- When I ask any of my managers about a node, I want the same answer, so I
  do not have to find out which one is in charge first.

**Job author**

- When my service is running fine on a machine that is running fine, I do
  not want it moved because the control plane was busy writing to disk.

**Legion maintainer**

- When I publish a placement-throughput number, I want it to measure the
  scheduler, not the cost of a thousand machines saying hello.

*Not in scope:* the security/platform admin persona — liveness is not an
authorization surface and no ACL vocabulary changes — and the autonomous
agent persona beyond keeping the existing `--json` node output shape
correct and honest about staleness.

### User scenarios the design must serve

1. **The gp3 cluster.** An operator runs three managers on ordinary EBS
   gp3 with 1000 workers. The fleet sustains full cadence, the raft log
   carries no heartbeat entries, and the operator never learns that local
   NVMe was ever a consideration.
2. **The handover.** The status lane's leader is drained for a kernel
   patch. Leadership moves to a manager that has been up for six hours.
   **Zero** nodes are marked lost, no allocation moves, and the new leader
   has a complete liveness picture within one heartbeat interval.
3. **The partitioned manager.** Manager C is cut off from A and B for 20
   seconds. The 300 workers that dial C keep heartbeating to C successfully
   the whole time. When the partition heals, **zero** of those 300 have been
   marked lost, and the operator has a log line and a metric naming C as the
   manager that went quiet — not the workers.

---

## 4. Goals and non-goals

### Goals

- **A steady-state heartbeat costs no raft commit and no fsync.** This is
  the one hard goal; everything else serves or protects it.
- **No false node loss** across leader election, rolling manager restart, or
  a manager-to-manager partition. Stronger than today, not merely equal:
  today's process-uptime grace (`src/manager/daemon.rs:978`) already fails
  the election case.
- **Every manager still answers "is this node alive?" identically**, from
  replicated state, with no read forwarding.
- **The worker plane does not change.** No proto field, no
  `protocol_version` bump, no worker behavior change, no upgrade-ordering
  constraint beyond the existing managers-first rule.
- The change is **observable**: an operator can see how many nodes the
  leader is tracking, which peer managers are reporting, and every reason a
  loss decision was suppressed.

### Non-goals

- **Gossip/SWIM liveness.** Weighed in §6.6(d) and rejected for v1, with the
  seam left open.
- **Changing the cadence, the lost window, or their defaults.** Owned by the
  cadence plan. This plan is cadence-neutral: it works at 2s and at 25s.
- **Worker-plane protocol changes.** A worker cannot tell the difference.
- **Moving any other status-lane write off raft.** Health-check payloads,
  allocation-health snapshots, and status transitions all stay replicated.
  Liveness timestamps are singled out because of what they are (§6.1), not
  because they are on the status lane.
- **Removing the fsync from raft generally.** Group-committing or relaxing
  raft durability is a different plan with a different risk profile.
- **Per-pool or per-namespace liveness reporting.** Named as a known
  limitation on the public page.
- **Multi-region / federated liveness.** Out of scope, unchanged.

---

## 5. Background — what exists today

### 5.1 The three facts, and where each lives

The multi-raft persistence work already split the node record across lanes.
That split is the reason this plan is small — the schema is most of the way
there already.

| Fact | Table / column | Lane | Write frequency |
| --- | --- | --- | --- |
| Registration, pool, attributes, fingerprint | `node` | Control | Once, then on fingerprint change |
| **Schedulability** (`available` / `drained` / `unavailable`) | `node.availability` | **Control** | On transition only |
| Liveness **status** (`active` / `lost`), `lost_at` | `node_health.status` | Status | On transition only |
| Health-check payload | `node_health.health_checks` | Status | ~`WORKER_NODE_CHECK_DURATION_SEC` (30s) |
| **Last heartbeat timestamp** | `node_health.last_heartbeat`, `.last_heartbeat_unix` | Status | **Every beat — 2s per node** |

Only the last row is high-frequency. It is also the only row that is
*derived* rather than reported.

### 5.2 The finding that makes this tractable: the scheduler never reads liveness

`Store::available_node_arcs` (`src/storage/store.rs:2094`) is the
scheduler's per-plan node snapshot, and it filters on
`node.availability == "available"` — a **control-lane** column. So does
`available_node_ids` (`src/storage/store.rs:2079`), and so does the
reconcile path. `set_node_availability`'s own doc comment names it:
*"the scheduler's single placement chokepoint … and the store's reconcile
path both filter on `availability == \"available\"`"*
(`src/storage/store.rs:4570-4573`).

`node_health.last_heartbeat` has **no** consumer in the scheduler, the eval
loop, the dispatcher, or the rollout engine. Its only readers are:

| Reader | Where | Uses it for |
| --- | --- | --- |
| The liveness tick | `src/manager/daemon.rs:711` via `get_node_ids_and_heartbeats` (`src/storage/store.rs:4777`) | The lost decision |
| `GET_NODES` → `NodeMsg.last_heartbeat` | `src/storage/queries.rs:18` | Display |
| `legion node list --sort-by last_heartbeat` | `src/manager/server.rs:5398` | Display |
| UI nodes page / deep view / homepage | `ui/src/pages/Nodes.tsx:85,101`, `ui/src/components/NodeDeepView.tsx:619`, `ui/src/pages/Homepage.tsx:204` | Display |
| `legion state` dump | `src/cmd/state.rs:34` | Display |

**One decision consumer, four display consumers.** That is the entire blast
radius, and the decision consumer already runs only on the status-lane
leader.

### 5.3 The write path, precisely

```
Manager::heartbeat                       src/manager/server.rs:5095
  └─ spawn_blocking (f24c6e0)            src/manager/server.rs:5123-5130
     └─ Store::update_node_heartbeat     src/storage/store.rs:5239
       └─ Coalescer::submit("heartbeat") src/storage/coalesce.rs:62
            └─ run_heartbeat_batch       src/storage/store.rs:5253
                 ├─ batch of 1  → update_node_heartbeat_forwarding  :5313
                 │                  └─ NotRaftLeader → ForwardLaneWrite
                 │                       (ForwardableLaneWrite::NodeHeartbeat,
                 │                        src/storage/raft/forward.rs:54)
                 └─ batch of N  → replicated(Lane::Status,
                                    "update_node_heartbeats")        :5266
                                   └─ pipeline_loop (ONE thread/lane)
                                        src/storage/raft/driver.rs:1449
```

Two properties of the coalescer matter for §6.6(b). It has
**`max_batch = HEARTBEAT_BATCH_MAX = 128`** (`src/storage/store.rs:684`),
and it has **no timer** — by design: *"Requests arriving while a commit is
running form the next batch — natural batching with no timer"*
(`src/storage/coalesce.rs:8-9`). Timer-free batching needs a queue to batch
*from*, which is the insight `f24c6e0` acted on: before the offload the
queue lived in the gRPC layer where the coalescer could not see it, so
batches stayed at ~1 and `max_batch` was never approached. Whether it is
approached *now*, at load, is exactly what phase 1 measures.

### 5.4 The liveness tick and its grace hazard

`start_node_status_checker` (`src/manager/daemon.rs:683`) runs every
`worker_check_seconds` (default 2, `src/constants.rs:63`). It gates on
`store.is_raft_leader(Lane::Status)` (`:705`) and calls
`nodes_to_mark_lost(store.get_node_ids_and_heartbeats()?, started_at.elapsed())`
(`:711-712`).

`started_at` is set once, at `src/manager/daemon.rs:685`, from **process
start**. `nodes_to_mark_lost` (`src/manager/daemon.rs:974`) opens with:

```rust
if manager_uptime < Duration::from_secs(DEF_LOST_NODE_AT_SEC as u64) {
    return Vec::new();
}
```

Under `lanes`, a manager that has been up for hours can *acquire* status-lane
leadership mid-life and begin marking lost on its very next tick — right
after an election during which the fleet's heartbeats were failing. This is
already an open item in `internal_docs/TODO.md:369-378`. **Any design that
puts liveness in leader memory makes this hazard fatal rather than merely
bad**, because the new leader's memory is empty. Fixing it is the load-
bearing half of §6.4, not a side quest.

### 5.5 Lane leadership under `lanes`

`Lane` is `Control | Scheduler | Status` (`src/storage/raft/mod.rs:41-48`),
three independent raft groups. The engine's convergence loop pulls lanes
onto the control anchor, but convergence is explicitly **best effort** —
openraft 0.9 has no leader-transfer API — and `tests/raft_lanes.rs` asserts
correctness of the *standing split*, not its absence
(`lanes_that_settle_on_different_leaders_stay_correct`, `tests/raft_lanes.rs:1602`).
So the status-lane leader and the scheduler-lane leader **can be different
managers**, indefinitely. Any design must be correct in that state.

This is already documented public behavior, and it is the sentence this plan
changes. `docs/multi-raft.md:314-323`:

> **Worker liveness keeps working, by forwarding.** Workers dial one fixed
> manager; when that manager does not lead the status lane, heartbeat,
> health-check, allocation-health and failure-report writes are FORWARDED
> over the authenticated manager plane to the owning lane's leader
> (`ForwardLaneWrite` on the raft transport), and **the worker's RPC
> succeeds only after the forwarded write commits**.

Cross-lane composites already exist and already forward:
`mark_nodes_as_lost` (`src/storage/store.rs:4805`) commits the status half,
then `sync_node_availability(ids, "unavailable")` on the control lane, then
one scheduler-lane reschedule eval per node. `mark_node_ready`
(`src/storage/store.rs:4370`) is the mirror image. Both wrap their halves in
`ForwardLaneWrite` so they complete from whichever manager runs them.

### 5.6 Manager-to-manager transport

`RaftTransport` (`src/storage/raft/server.rs`) already exists as an
authenticated manager-plane service — shared-secret auth in
`authenticate()` (`src/storage/raft/server.rs:70`), payloads versioned by
`PROTOCOL_VERSION`. `ForwardLaneWrite` rides it. Adding a manager-plane RPC
here costs no new listener, no new auth scheme, and no ACL vocabulary; it
degrades against an older peer exactly the way `ForwardLaneWrite` does
(`docs/upgrades.md:152-158`).

### 5.7 Metrics that exist

`legion_heartbeats_total` (`src/manager/telemetry.rs:678`),
`legion_nodes{status}` (`:681`),
`legion_store_coalesce_batch_size{path}` / `_wait_seconds{path,role}` /
`_commit_seconds{path}` (`:686-693`),
`legion_raft_propose_apply_seconds{lane}` (`:714`),
`legion_raft_pipeline_wait_seconds{lane}` / `_depth{lane}` (`:718-721`).

`legion_raft_pipeline_depth{lane="status"}` is the metric that should go to
approximately zero when this plan ships, and it is already there.

---

## 6. Proposed design

### 6.1 What actually needs durability

A heartbeat timestamp has three properties that no other write in the system
shares:

- **It is derived, not reported.** The worker does not send a timestamp;
  the manager stamps `now`. Nothing is lost by regenerating it — the next
  beat regenerates it, 2 seconds later.
- **It is worthless after a few seconds.** Its entire semantic content is
  "was this within the last `lost_window`". A ten-minute-old heartbeat
  timestamp answers exactly one question ("no"), which the *status*
  transition already answers durably.
- **It cannot be trusted across a control-plane gap anyway.** The existing
  startup-grace comment (`src/manager/daemon.rs:956-964`) says it outright:
  *"time the manager spent down must not count as missed heartbeats: right
  after a manager restart every persisted heartbeat looks stale."* Legion
  **already** throws the persisted timestamps away after a restart. Making
  them durable is paying an fsync for a value the code deliberately ignores
  at the one moment durability would matter.

So the split is:

**Stays replicated (unchanged).** Node registration and identity, pool,
config/fingerprint/dynamic attributes, `node.availability`, allocations,
`node_health.status` and `lost_at`, `node_health.health_checks`, join
tokens, everything else.

**Leaves raft.** `node_health.last_heartbeat` and `.last_heartbeat_unix`
refreshes on an already-active node. Nothing else.

**Still replicated, because it is a transition:** `lost → active`. When a
heartbeat arrives from a node the store believes is lost, that is *new
information no reconciler can re-derive*, and it takes the existing
`mark_node_ready` composite (`src/storage/store.rs:4370`) — status lane
then control lane, so the node becomes schedulable again. This is the
`active` branch of `update_node_heartbeats_apply`'s existing
`transitioning` set (`src/storage/store.rs:5372-5392`), lifted out of the
common path. Transitions are rare by construction: a node transitions twice
per outage, not 30 times a minute.

### 6.2 The liveness table

The status-lane leader owns an in-memory table:

```rust
/// Node liveness on the status-lane leader. Not persisted, not
/// replicated, and never read back from disk: a heartbeat timestamp is
/// derived and worthless across a control-plane gap (plan §6.1), so a
/// new leader REBUILDS this from incoming heartbeats under the failover
/// grace rather than inheriting a stale one.
struct LivenessTable {
    /// node id -> last observed beat, monotonic.
    seen: DashMap<String, Instant>,
    /// When this manager became responsible for liveness: process start
    /// OR status-lane leadership acquisition, whichever is later. THE
    /// FIX for the process-uptime hazard (plan §5.4).
    responsible_since: AtomicInstant,
    /// Per-peer-manager last digest (plan §6.3).
    peers: DashMap<String, Instant>,
}
```

A heartbeat that arrives at the status-lane leader becomes one map write.
No lock beyond the shard, no commit, no fsync, no pipeline-thread hop.

The liveness tick (`src/manager/daemon.rs:711`) reads the table instead of
`get_node_ids_and_heartbeats()`, and `nodes_to_mark_lost` keeps its exact
shape — a pure function over `(node id, last seen, grace)` — so its existing
unit tests (`src/manager/daemon.rs:1459`) survive as-is with the clock
swapped.

### 6.3 The lanes complication, and where liveness lives

This is the part with no prior art, because Nomad has one leader and
Kubernetes has none.

**The answer: liveness lives in exactly one place — the status-lane
leader's memory — and the scheduler never reads it.**

That is safe because of §5.2: placement gates on `node.availability`, a
**control-lane** column, which the existing `mark_nodes_as_lost` composite
already flips as a replicated write. The scheduler-lane leader consumes the
*transition*, not the liveness. So the cross-lane read that the brief worried
about does not exist and does not need to be created.

Concretely, under a standing status/scheduler split:

```
worker ──heartbeat──▶ manager C (leads nothing)
                        │
                        │  in-memory: C notes "node-7 seen at T"
                        │  every `digestInterval`, one RPC:
                        ▼
                     manager A (status-lane leader)
                        │  LivenessTable.seen["node-7"] = T
                        │
                        │  on transition only (rare):
                        ├──status lane──▶  node_health.status = lost
                        ├──control lane─▶  node.availability = unavailable   (forwarded)
                        └──scheduler lane▶ node_lost_reschedule eval          (forwarded)
                                              │
                                              ▼
                                        manager B (scheduler-lane leader)
                                        reads node.availability from its
                                        OWN replica. Never asks anyone.
```

**The digest.** A new manager-plane RPC on `RaftTransport`:

```
ReportNodeLiveness(NodeLivenessDigest) -> NodeLivenessDigestAck
```

Each manager that does **not** lead the status lane batches the node ids it
personally received heartbeats from and sends one RPC per `digestInterval`
(default **1s**) to the status-lane leader. The cost is
**`managers × 1 RPC/second`**, not `nodes × 1 RPC per interval`. At 1000
nodes across 3 managers that is 2 RPCs/second carrying ~333 ids each,
against today's ~500 forwarded raft writes per second.

**Why push a digest rather than have the leader pull.** The alternative —
every manager tracks its own beats and the leader unions them at tick time —
was genuinely on the table and is rejected on failure semantics. A pull
makes the *lost decision itself* depend on a fan-out RPC completing: a
manager that is slow or unreachable at tick time contributes nothing, and
its nodes look dead. A push inverts that into a fail-safe: the leader has
the last digest and knows how old it is, and can decline to decide (below).

**Peer-gap suppression — the failure mode this design has to own.** If
manager C's digests stop arriving, C's nodes look silent to the leader even
though C is happily receiving their beats. Handled explicitly:

> When a peer manager's last digest is older than `peerDigestGrace`
> (default **30s**), **the node ids that peer last claimed are not eligible
> to be marked lost** until it reports again or the grace expires with the
> peer confirmed down. The leader logs once per peer per gap and raises
> `legion_liveness_peer_stale{peer}`.

This is the same reasoning as Kubernetes' `stateFullDisruption` and the
cadence plan's mass-disruption circuit breaker, applied one level down: *a
manager-to-manager problem must not present as fleet-wide node loss*. It
also composes with that circuit breaker rather than replacing it — peer-gap
suppression catches the partial case (one manager's share of the fleet),
the circuit breaker catches the total case.

**Single-manager and `single` topology get all of this for free.**
`is_raft_leader()` reports leadership for non-raft backends, and under
`single` topology one group carries every lane. In both cases there is no
peer and no digest path; liveness is process-local memory and the RPC never
engages. The 90% of clusters that are one manager on a laptop see a pure
deletion of work.

### 6.4 Rebuild on election — the correctness core

A newly elected status-lane leader has an **empty** liveness table. If it
runs the tick, it marks the entire fleet lost. Nomad's answer, adapted:

1. **`responsible_since` resets on leadership acquisition**, not only on
   process start. The status checker already detects acquisition — it logs
   *"Raft status-lane leadership acquired; node status checker resuming"*
   at `src/manager/daemon.rs:708` — so the hook exists and is one line from
   the fix. **This supersedes the process-uptime grace at
   `src/manager/daemon.rs:978`** and closes `internal_docs/TODO.md:369-378`.
2. **On acquisition, seed every registered active node at `now`.** The set
   comes from replicated state (`node` LEFT JOIN `node_health` WHERE status
   is active — the existing `get_node_ids_and_heartbeats` query minus its
   timestamp columns). Seeding at `now`, not at the persisted timestamp, is
   deliberate: the persisted value is stale by construction and seeding
   from it would mark the fleet lost immediately. Nomad seeds at
   `failover_heartbeat_ttl` for the same reason.
3. **Mark nothing lost for `failoverGrace`** (default **60s**) after
   `responsible_since`. One heartbeat interval would be enough in theory;
   60s covers an election, a worker's retry backoff (`HEARTBEAT_RETRY_MAX`
   is 10s), and a slow reconnect, and is a sixth of Nomad's 5 minutes.
4. **The grace is a floor, not a blindfold.** A node that heartbeats during
   the grace is tracked normally from that moment. The grace only suppresses
   *marking lost*; it does not suppress `lost → active` recovery, which is
   the transition an operator most wants during a recovery.

Every suppression is counted:
`legion_liveness_loss_suppressed_total{reason="failover_grace"|"peer_digest_gap"|"mass_disruption"}`.
"Nothing was marked lost" must never be indistinguishable from "nothing was
wrong".

### 6.5 Reads from followers — a public-surface change, stated plainly

**The status is unchanged and stays exact everywhere.** `node_health.status`
is still replicated, so `legion node list`, `legion node status <id>`, the
UI status pill, and `legion_nodes{status}` are byte-for-byte correct on
every manager, leader or not. This is the important half and it does not
regress.

**The last-heartbeat *timestamp* becomes approximate on followers.** Three
options were weighed:

| Option | Read path cost | Follower accuracy | Verdict |
| --- | --- | --- | --- |
| (i) Forward the read to the status leader | An RPC on every `node list` and UI poll, forwarding the whole node list *before* ACL filtering and pagination | Exact | Rejected — turns a local read into a fan-out and makes a display field a hard dependency on the status leader being reachable |
| (ii) **Slow replicated checkpoint** | Unchanged, zero | Within `checkpointInterval` | **Recommended** |
| (iii) Gossip the timestamps | New subsystem | Near-exact | Rejected with (d) in §6.6 |

**(ii), the liveness checkpoint.** Every `checkpointInterval` (default
**30s**) the status-lane leader writes one batched status-lane commit
stamping `last_heartbeat_unix` for every active node with **its actual last
observed beat time**, not `now`. That is 2 commits/minute in place of ~500
commits/second — a reduction of roughly **15,000×** — and it lands in one
batch on the same path that already handles 128-node batches.

Two rules make it safe, and they are non-negotiable:

- **A checkpoint is never read back as liveness.** Not by the tick, not on
  election, not ever. It is display state. The seed-at-`now` rule in
  §6.4(2) exists precisely so that a 30s-stale checkpoint cannot become a
  loss decision.
- **It records the observed time, not the write time.** A downgraded or
  older manager reading the column sees a truthful (if coarse) value rather
  than a value that claims everyone beat 30 seconds ago in lockstep.

**The user-visible consequence**, and it is a real one:

```console
$ legion node list
ID          POOL     STATUS   AVAILABILITY  LAST HEARTBEAT
node-0007   default  active   available     3s ago
node-0008   default  active   available     ~24s ago (checkpoint)
```

On the status-lane leader the column is live. On any other manager it is
marked `(checkpoint)` and is accurate to within `checkpointInterval`. A new
`NodeMsg.heartbeat_source` field (`live` | `checkpoint` | `none`) carries
this, `--json` emits it, and the UI renders the same distinction. Sorting by
`last_heartbeat` (`src/manager/server.rs:5398`) keeps working; it sorts on
whatever value that manager holds, which is documented.

Setting `checkpointInterval: "0s"` turns checkpoints off entirely — zero
heartbeat-derived writes reach raft at all, and followers report
`heartbeat_source: none` with an empty column. Offered because some
operators will prefer an honestly blank field to an approximate one.

### 6.6 The alternatives, weighed

#### (a) Replicate transitions only; steady-state beats in leader memory — **recommended**

*What it is:* §6.1–6.5.

*What breaks:* the last-heartbeat timestamp becomes approximate on
non-leaders (§6.5). A new manager-plane RPC and a soft-state table are new
surface to reason about. Detection depends on the digest path under a
standing lane split, which is new failure surface that peer-gap suppression
has to cover.

*What it costs:* the largest design surface of the four, though a small
diff: a table, one RPC, a grace clock, a checkpoint timer. No new subsystem,
no new lane, no schema change, no worker change.

*What it buys:* heartbeats leave the disk entirely. `R` stops being a
property of the storage device and becomes a property of the gRPC handler —
the 25× gp3/NVMe gap should collapse to nothing. Expected `R` on gp3: the
NVMe number or better, **≥2000/sec**, which is 4× the 500/sec that 1000
nodes at 2s requires, i.e. real headroom rather than a survived ceiling. It
also removes heartbeat entries from the raft log entirely, which removes a
compaction and snapshot load nobody has measured yet.

#### (b) Keep replicating, but batch harder — **mostly already done by `f24c6e0`; the residue is a measurement, not a plan**

*What it is:* make one fsync serve many beats. The coalescer has no
accumulation window by design (`src/storage/coalesce.rs:8-9`) — a batch forms
only from requests that arrive *while a commit is in flight*. That is a
self-limiting equilibrium, and it was the arithmetic behind the original
number:

```
commit latency ≈ 1/80 ≈ 12ms                 (measured, gp3)
arrivals during one commit ≈ 48/s × 0.012s ≈ 0.6
=> batch ≈ 1.6 — a stable LOW-throughput equilibrium
```

**`f24c6e0` broke that loop**, and not with a timer. The blocking store call
was holding tokio worker threads, so heartbeats piled up *in the gRPC layer*
where the coalescer could not drain them. `spawn_blocking` let the queue
reach the coalescer, batches formed on their own, and gp3 went 60 → 422/sec.
The cheap win in this row has been collected.

*What is left:* the residual 16% at 4 threads. Two candidates, and they need
separating by measurement before either is worth building:

1. **The fsync itself** — irreducible while heartbeats are replicated, and
   the thing (a) removes.
2. **The single `pipeline_loop` thread per lane**
   (`src/storage/raft/driver.rs:1449`) — every status-lane op, heartbeat or
   not, serialized through one OS thread.

An explicit accumulation window (`liveness.batchWindow`) is still available
as a lever if `legion_store_coalesce_batch_size{path="heartbeat"}` shows
batches well below `HEARTBEAT_BATCH_MAX` at load. It is cheap — one
`Duration`, one sleep before the drain, one knob — but it is now a **tuning
option contingent on a reading**, not a headline fix, and shipping it blind
would be adding a knob to solve a problem that may no longer exist.

*What breaks even if it works perfectly:* everything else on the status lane
still queues behind heartbeat commits. The raft log still carries every
heartbeat forever, so snapshot and compaction load is unchanged and
unmeasured. And **`R` remains a property of the disk** — which is why the
control plane is on `m6id.xlarge` local NVMe today, and why 84% of cadence
at 1,000 nodes does not answer the campaign's 10,000-node step.

*Verdict:* **largely spent.** What remains of this row is phase 1 of §8:
measure, and only then decide whether the window is worth a config knob.

#### (c) A separate non-raft durable store, local and unreplicated

*What it is:* per-manager sqlite/mmap file for liveness, fsync'd locally, not
replicated.

*What breaks:* the point of durability is surviving failover, and an
unreplicated local file does not transfer to the new leader. So on election
the new leader has nothing — **the exact position (a) is in** — and it still
paid an fsync per beat to get there. It also adds a second on-disk format
with its own corruption, migration, and backup story
(`docs/raft-operations.md` covers exactly one store today).

*What it buys:* liveness survives a restart of *the same* manager. Legion
already handles that case, and handles it better: the startup grace
(`src/manager/daemon.rs:978`) deliberately discards persisted timestamps
after a restart, because a manager that was down cannot distinguish "the
node died" from "I was not listening".

*Verdict:* **rejected.** It is (a)'s failure mode plus (b)'s cost, and it
adds a file format. There is no configuration in which it is the best
answer.

#### (d) Full gossip/SWIM, Consul's model

*What it is:* workers gossip liveness among themselves; managers learn
membership from the mesh. Per-node cost flat in `N`, detection
`O(log N)`, no central write path at all.

*What breaks:* everything about the deployment model. Today a worker needs
to reach *the managers*; gossip needs every worker to reach a random subset
of *every other worker*, which in practice means a routable flat network or
a relay. It brings its own membership protocol, its own encryption keys and
key rotation, its own split-brain semantics, and its own security boundary
— none of which Legion has today, and all of which are permanent. It also
does not remove the ceiling: Consul documents **5,000 client agents per
datacenter**, and warns the real limit is churn.

*What it buys:* the flat per-node cost curve, which is genuinely the right
long-term answer at 10k+ nodes.

*Verdict:* **out of scope, and the seam stays open.** (a) is a strict
prerequisite for it in any case: gossip's output is exactly what the
`LivenessTable` in §6.2 holds. If gossip ever ships, it replaces the *input*
to that table and leaves the transition, grace, checkpoint, and read-path
machinery intact. Building (a) makes (d) cheaper, not harder — that is the
main reason it is the recommendation rather than a compromise.

### 6.7 Recommendation

**(a), the architecture. (b) is largely already collected by `f24c6e0` and
its residue is a measurement (phase 1), (c) is rejected outright, (d) is
deferred with the seam left open.**

The reasoning, tied to the product goals:

- **Performance.** (a) is the only option that takes heartbeats off the
  disk. Everything else moves a constant factor while leaving `R` a property
  of the storage device, which is what commit `a774fb0` is currently
  compensating for with hardware.
- **Simplicity / ease.** The operator model stays one sentence: *"the
  manager that owns node status remembers who checked in; only changes are
  written down."* Three concepts vanish from an operator's life — the NVMe
  requirement, the fsync-vs-cadence arithmetic, and the sizing table's
  dependence on a disk benchmark. Against that, one honest new concept
  appears: on a manager that does not own node status, the *time* of the
  last check-in is approximate and says so. That is a good trade, and it is
  a trade rather than a free win, which is why it is a judgement call.
- **Correctness.** (a) forces the failover-grace fix rather than leaving it
  as a nice-to-have, and it forces the peer-gap suppression that today's
  design has no analogue of. Both make the cluster strictly *safer* than
  today under election and partition — today's process-uptime grace fails
  the election case outright (§5.4).
- **Extensibility.** (a) is the exact substrate gossip would need. (c) and
  (b)-alone are both dead ends for (d).

The one thing this plan will not do is ship (a) before phase 1's
measurement. `f24c6e0` moved gp3 from 12% to 84% of cadence in one commit
that nobody had predicted would be that large; a plan that goes straight to
the architecture without first attributing the residual 16% risks building
the wrong thing for the second time in a week.

---

## 7. Proto, struct, config, and persistence changes

**Proto** (`proto/legion.proto`), with full public comments per the
documentation rule:

- `RaftTransport` service gains
  `rpc ReportNodeLiveness(NodeLivenessDigestRequest) returns (NodeLivenessDigestReply)`.
  Request: `protocol_version`, `observer_manager_id`, `observed_at_unix_ms`,
  `repeated string seen_node_ids`. Reply: `accepted`, `not_status_leader`
  (with `leader_hint`, so a stale sender re-targets exactly as
  `ForwardLaneWrite` callers do). **Manager-plane, not user-facing.**
  Authenticated by the existing `RaftTransport` shared secret
  (`src/storage/raft/server.rs:70`) — it is not a `Manager` RPC, so it takes
  no ACL row and `tests/acl_endpoints.rs::every_manager_rpc_is_classified`
  is unaffected. That exemption is stated in the proto comment so the next
  reader does not have to infer it.
- `NodeMsg` gains `string heartbeat_source` — `"live"` (this manager owns
  liveness and observed the beat), `"checkpoint"` (replicated checkpoint,
  accurate to within `liveness.checkpointInterval`), or `""` (checkpoints
  disabled). Additive; an older client ignores it and sees today's field.

**Manager config** (`src/structs/config.rs`, `configs/manager.jsonnet`) —
one new block, every field optional, omitting the block reproducing today:

```jsonnet
// Node liveness (docs/node-liveness.md). Heartbeats are tracked in the
// status-lane leader's memory; only status TRANSITIONS are replicated.
liveness: {
  // Accumulation window for the heartbeat group commit. CONTINGENT on
  // phase 1's measurement - ship it only if batches are still small under
  // load after the spawn_blocking offload. Once liveness leaves raft
  // (phase 3) heartbeats do not commit at all and this applies solely to
  // the checkpoint. "0ms" = commit-while-you-can batching, the default.
  batchWindow: "0ms",

  // After acquiring status-lane leadership (or starting the process),
  // mark nothing lost for this long. A new leader inherits no liveness
  // state and rebuilds it from incoming heartbeats, so an election must
  // not be a fleet-wide outage. Nomad's failover_heartbeat_ttl (5m); we
  // use a sixth of that because Legion's lost window is 10s, not 30s.
  failoverGrace: "60s",

  // How often a manager that does NOT lead the status lane reports the
  // nodes it heard from to the leader. ONE RPC per manager per interval,
  // whatever the fleet size.
  digestInterval: "1s",

  // How long the leader keeps trusting a peer manager's last digest.
  // Nodes last claimed by a silent peer are not marked lost inside this
  // window: a manager-to-manager problem must not present as fleet-wide
  // node loss.
  peerDigestGrace: "30s",

  // How often the leader replicates a batched heartbeat checkpoint so
  // every manager can show an approximate "last heartbeat" time. DISPLAY
  // ONLY - a checkpoint is never read back as liveness, including on
  // election. "0s" disables checkpoints entirely; non-leaders then show
  // no heartbeat time rather than an approximate one.
  checkpointInterval: "30s",
}
```

Validation, rejected at config load with the offending values named:
`failoverGrace >= lost window` (a grace shorter than the window is not a
grace); `digestInterval < peerDigestGrace` (a peer must get several chances
before it is considered quiet); `checkpointInterval` is either `0` or
`>= 5s` (anything faster is re-creating the problem this plan removes).

**Worker config:** none. **Job config:** none —
`configs/jobs/maximal.jsonnet` is unaffected.

**Persistence:** **no migration.** `node_health.last_heartbeat` and
`.last_heartbeat_unix` keep their columns and their meaning; they are simply
written by the checkpoint instead of by every beat. This is the single
largest reason to prefer this shape over a schema change — a plan that
touches no on-disk format has no downgrade hazard beyond staleness (§10).

**Capabilities / MCP:** no new CLI verb and no new user-facing RPC, so
`catalog()` (`src/cmd/capabilities.rs`) and `src/cmd/mcp.rs` are unchanged.
`docs/agent-readiness.md` gets one paragraph on `heartbeat_source`, because
an agent reading node output needs to know the freshness caveat.

**Metrics** (`src/manager/telemetry.rs`, `docs/metrics.md`):

- `legion_liveness_tracked_nodes` — gauge, nodes in the leader's table; `0`
  on non-leaders. Should equal `legion_nodes{status="active"}`.
- `legion_liveness_responsible_seconds` — gauge, seconds since
  `responsible_since`. Makes "we just took over" visible in a dashboard.
- `legion_liveness_digest_rpcs_total{outcome}` — counter,
  `ok|not_leader|error|unimplemented`.
- `legion_liveness_digest_nodes` — histogram, ids per digest.
- `legion_liveness_peer_stale{peer}` — gauge `0|1`. **The metric to alert
  on** for scenario 3.
- `legion_liveness_loss_suppressed_total{reason}` —
  `failover_grace|peer_digest_gap|mass_disruption`.
- `legion_liveness_checkpoint_seconds` — histogram of checkpoint commit
  duration.

---

## 8. Implementation phases

**Phase 1 — Attribute the residual 16% (§6.6(b)). No behavior change.**
Re-run `benchmarks/aws/experiments/heartbeat-commit-probe.sh` post-`f24c6e0`
on **both** gp3 and NVMe, at 1000 and 4000 nodes, and read
`legion_store_coalesce_batch_size{path="heartbeat"}` against
`legion_raft_pipeline_wait_seconds{lane="status"}` and `_depth` to split the
remaining gap between the fsync and the single-pipeline-thread serialization
(`docs/metrics.md` § "Diagnosing a manager throughput ceiling" is the read
order). Ship `liveness.batchWindow` **only if** the batch-size histogram
shows batches well below `HEARTBEAT_BATCH_MAX` under load. *Valuable alone:*
it is the baseline every later phase is measured against, and it decides
whether one config knob is worth adding at all.

**Phase 2 — Fix the grace clock and make suppression visible.**
`responsible_since` resets on status-lane leadership acquisition (the hook
at `src/manager/daemon.rs:708` already exists); `failoverGrace` config;
seed-active-nodes-at-`now` on acquisition;
`legion_liveness_loss_suppressed_total` and
`legion_liveness_responsible_seconds`. Still fully raft-backed. *Valuable
alone:* closes `internal_docs/TODO.md:369-378` — a live hazard today,
independent of everything else in this plan. **This phase is worth shipping
even if the plan is abandoned after it.**

**Phase 3 — Liveness in leader memory.**
`LivenessTable`; the heartbeat handler updates memory instead of committing;
`lost → active` still takes the `mark_node_ready` composite; the tick reads
the table; the checkpoint timer and `NodeMsg.heartbeat_source`; CLI and UI
render the `(checkpoint)` marker. **Single-manager and `single` topology
complete here** — no digest path engages. *Valuable alone:* the whole
performance goal lands for the overwhelmingly common topology.

**Phase 4 — The digest, for `lanes` with a standing split.**
`ReportNodeLiveness` on `RaftTransport`; non-leader managers batch and send;
peer tracking and `peerDigestGrace` suppression;
`legion_liveness_peer_stale`; fall back to today's per-heartbeat
`ForwardLaneWrite::NodeHeartbeat` against a peer that answers
`unimplemented`. *Valuable alone:* `lanes` clusters get phase 3's benefit
under a standing lane split, which phase 3 alone does not give them.

Phase 2 before phase 3 is not negotiable: phase 3 without the grace fix
marks the entire fleet lost on the first election.

Phase 4 after phase 3 is deliberate. Between them, a `lanes` cluster with a
standing split falls back to forwarding each heartbeat as a raft write —
i.e. today's behavior, with phase 1's batching — so the intermediate state
is a performance regression relative to phase 3, never a correctness one.

---

## 9. Testing plan

**Unit.**

- `nodes_to_mark_lost` keeps its pure-function shape; its existing test
  `nodes_to_mark_lost_waits_for_startup_heartbeat_grace`
  (`src/manager/daemon.rs:1459`) must pass unchanged in meaning with
  `responsible_since` swapped for `started_at`.
- The grace clock: acquisition at `t` on a process that started at `t - 6h`
  suppresses marking until `t + failoverGrace`, then resumes. **This is the
  test that fails today.**
- Seeding: acquisition seeds every active node at `now`, and a node whose
  persisted `last_heartbeat_unix` is an hour old is not marked lost on the
  first tick.
- Peer-gap suppression: peer C's digest at `t`, nothing after; at
  `t + peerDigestGrace - 1s` C's nodes are suppressed and A's are not; at
  `t + peerDigestGrace + 1s` C's become eligible.
- Checkpoint isolation: a checkpoint written at `t` with observed times of
  `t - 25s` never produces a loss decision on a leader whose
  `responsible_since` is `t`.
- Coalescer with `batchWindow`: a drain waits the window, batch size is
  capped at `HEARTBEAT_BATCH_MAX`, and `batchWindow: 0` reproduces today's
  drain exactly.
- Config validation: each rejected combination in §7, with the values named
  in the error.

**Integration**, on the `tests/raft_lanes.rs` harness — `start_cluster`
(`:312`), `move_lane_leadership` (`:502`), `wait_for` (`:111`) — extending
the existing `worker_heartbeats_forward_across_a_status_lane_split`
(`:1797`), which is the closest precedent:

- `status_lane_election_marks_no_nodes_lost` — 3-node lanes cluster, 200
  registered nodes heartbeating; `move_lane_leadership(status)` (`:502`,
  which campaigns because openraft 0.9 has no leader-transfer API —
  `docs/raft-operations.md:234-237`); assert **zero** marked lost across the
  election and all still active 30s later. This is the plan in one test.
- `rolling_manager_restart_marks_no_nodes_lost` — restart each manager in
  turn with the fleet beating; zero losses at every step.
- `liveness_reaches_the_leader_from_a_manager_that_leads_nothing` —
  heartbeats delivered only to the non-leading manager; the leader keeps
  those nodes active. Forces the split with `set_convergence(nodes, false)`
  (`tests/raft_lanes.rs:492`), the same primitive
  `lanes_that_settle_on_different_leaders_stay_correct` uses. Phase 4.
- `a_silent_peer_manager_does_not_kill_its_nodes` — stop peer C's digests
  (drop the RPC) while its workers keep beating to C; assert zero of C's
  nodes marked lost inside `peerDigestGrace` and
  `legion_liveness_peer_stale{peer="C"}` reads 1. Phase 4.
- `a_partitioned_status_leader_hands_over_without_loss` — isolate the
  status-lane leader, let the lane re-elect, heal; zero losses, and the
  content hashes converge (the harness's `hashes_converged`, `:616`).
- `a_genuinely_dead_node_is_still_marked_lost` — the negative control.
  Stop one worker; it is marked lost within the window, its allocations
  reschedule, and `node.availability` flips on the control lane. Without
  this test the rest are satisfied by "never mark anything lost".
- `a_recovered_node_transitions_through_raft` — kill and restart a worker;
  assert the `lost → active` transition is a replicated write visible on all
  three replicas, not just leader memory.
- Existing `tests/raft_lanes.rs`, `tests/raft_lanes_durability.rs`,
  `tests/node_drain.rs`, and the `metrics_endpoint` heartbeat-counter test
  stay green unchanged.

**Scale / regression.**

- `tests/heartbeat_scale_benchmark.rs` gains a case asserting **zero
  replicated status-lane ops** for `N` steady-state heartbeats on an active
  fleet (phase 3+), and a per-heartbeat cost assertion. Its liveness phase
  (`tests/heartbeat_scale_benchmark.rs:128-141`) currently calls
  `nodes_to_mark_lost(rows, Duration::from_secs(3600))` — a fake one-hour
  uptime to bypass the startup grace. That call site is exactly where the
  grace clock changes shape, so it is the first thing to update in phase 2,
  and the fake value should become a fake `responsible_since` rather than a
  fake uptime.
- `benchmarks/aws/experiments/heartbeat-commit-probe.sh` re-run on **gp3**
  at each of phases 1, 3, 4, with the numbers in the PR. The acceptance
  number is the gp3/NVMe ratio, which should approach 1.0.
- Per the repo rule, scheduler benchmark numbers accompany the PR *even
  though this plan does not touch the scheduler* — the claim being defended
  is that placement throughput does not regress, and after phase 3 it should
  measurably improve, since the campaign's placement numbers are currently
  contaminated (§10.1). Run the `candidate_construction` group of
  `benchmarks/micro/scheduler_bench.rs` on base and on the change.

---

## 10. Rollout and backward compatibility

Every phase is additive and default-inert until an operator sets something,
with one deliberate exception: the phase-2 grace fix changes behavior on
purpose, and it changes it strictly in the safe direction (fewer nodes
marked lost, never more).

**Version skew — workers.** **None.** No worker-plane proto field, no
behavior change, no `protocol_version` bump. A worker built before this plan
and a worker built after it are indistinguishable to a manager on either
side. This is the plan's biggest compatibility asset and it is the reason
the design puts the digest on the manager plane rather than asking workers
to do anything differently.

**Version skew — managers.** `docs/upgrades.md:6` mandates managers first,
then workers; managers within a raft cluster tolerate skew during the roll
(`docs/upgrades.md:148-158`). Specifically:

- **New manager, old peers.** `ReportNodeLiveness` returns `unimplemented`
  from an older peer. The sender falls back to today's per-heartbeat
  `ForwardLaneWrite::NodeHeartbeat` (`src/storage/raft/forward.rs:54`) and
  counts `legion_liveness_digest_rpcs_total{outcome="unimplemented"}`. This
  is the identical degradation `ForwardLaneWrite` itself shipped with, and
  the identical remedy: finish the roll.
- **Old manager leads the status lane, new followers.** The old leader
  never asks for digests and keeps applying replicated heartbeat writes;
  new followers detect a leader that does not accept digests and forward
  per-heartbeat. Correct, just slower. Resolves the moment leadership lands
  on an upgraded manager.
- **New manager leads, old followers.** Old followers forward heartbeats as
  raft writes, which the new leader applies exactly as before *and* records
  in its liveness table on apply. Both paths converge on the same table, so
  the mixed state is correct without a special case.

**Downgrade.** No migration means no format to roll back. A downgraded
manager reads `last_heartbeat_unix` from whatever checkpoint last landed and
applies the fixed 10s window. Because checkpoints record the **observed**
time, the worst case is a value up to `checkpointInterval` (30s) stale —
older than the 10s window. This is why the downgrade rule is stated
explicitly on the public page:

> Downgrade **all** managers together. During the downgrade, workers resume
> writing heartbeats through raft within one interval (≤2s), and the
> downgraded binary's own 10s startup grace covers the gap. A *partial*
> downgrade that leaves an old manager leading the status lane while a new
> one holds the checkpoint is the one unsafe combination; it resolves within
> one lost window and is bounded by the mass-disruption circuit breaker.

**Enabling and disabling.** Everything is live-editable and reversible.
Setting `checkpointInterval: "0s"` stops checkpoint writes immediately;
raising `failoverGrace` takes effect on the next acquisition. Reverting the
`liveness` block entirely returns a cluster to the built-in defaults, which
are the values above.

**Documentation.** A feature is not finished until these are:

- **New page `docs/node-liveness.md`** — what liveness is, where it lives,
  the `liveness` config block, `heartbeat_source` and what `(checkpoint)`
  means, the failover grace and why an election is quiet, peer-gap
  suppression, the downgrade rule, and the known limitations (no per-pool
  reporting; the timestamp is approximate off the status leader). Linked
  from `docs/README.md` and the root `README.md` doc list.
- **`docs/architecture-managers-workers.md:75-99`** — the "Heartbeat cadence
  and scale" section currently states *"concurrent heartbeats coalesce into
  one status-lane store commit"* as the mechanism. After phase 3 that is
  wrong; it must say heartbeats do not commit at all in steady state. Its
  lost/recovery section (`:132-144`) describes the startup grace in terms of
  process uptime and must describe leadership acquisition instead.
- **`docs/multi-raft.md:312-348`** — the divergent-leadership cost list says
  liveness works "by forwarding … the worker's RPC succeeds only after the
  forwarded write commits". Phase 4 replaces that with the digest; the
  fallback-to-forwarding path stays documented for the mixed-version window.
- **`docs/raft-operations.md`** — the local-NVMe guidance that commit
  `a774fb0` motivated should be re-stated as a performance preference rather
  than a requirement once phase 3's numbers are in.
- **`docs/metrics.md`** — the seven new `legion_liveness_*` families, and an
  addition to its "Diagnosing a manager throughput ceiling" section, since
  `legion_raft_pipeline_depth{lane="status"}` now means something different.
- **`docs/upgrades.md`** — the `ReportNodeLiveness` entry in the
  manager-plane skew list (`:148-166`), alongside the existing
  `ForwardLaneWrite` entry it mirrors, and the all-managers-together
  downgrade rule.
- **`docs/agent-readiness.md`** — `heartbeat_source` and its freshness
  caveat, so an agent reading node output does not treat an approximate
  timestamp as exact.
- **`docs/testing-and-benchmarks.md`** — the re-measurement list in §10.1
  and the rule that a heartbeat number states which storage it was taken on.
- **`internal_docs/TODO.md`** — check off the status-lane grace item
  (`:369-378`) with phase 2, note the D8 resolution against `:199-201` and
  `:361-368`, and open new items for anything deferred here.

### 10.1 What this does to the AWS campaign

Every placement-throughput number measured **before `f24c6e0`** is
contaminated: at 1000 nodes the control plane was spending most of its
status-lane commit budget on heartbeats, and the reschedule evals from
falsely-lost nodes were adding scheduler load on top. The TODO entry said it
directly — *"a run at 1k+ nodes today measures this ceiling, not the
scheduler."*

`f24c6e0` makes the 1000-node steps trustworthy again (84% of cadence, four
times clear of false loss). It does **not** clear the 5,000- and
10,000-node steps, where the required rate is 2,500 and 5,000 heartbeats per
second against a measured 422. Those steps stay blocked on phase 3.

Re-measure, after phase 1 (as a baseline) and again after phase 3:

- **`heartbeat-commit-probe.sh`** — the direct before/after, on **gp3**, at
  250 / 1000 / 2500 nodes. The headline result is the gp3-to-NVMe ratio
  going from ~25× to ~1×.
- **`packing-density.sh`** — was unrunnable at scale. It should produce a
  real number at 1000 nodes immediately post-`f24c6e0`, and that number is
  what the 5,000/10,000-node steps get sized from until phase 3 lands.
- **`run-campaign.sh`** — the campaign runner's defaults are
  `STEPS="1000 2000 5000 10000"` across three scenarios, i.e. **twelve
  cells, every one of them measuring placement against a manager
  simultaneously absorbing `N/2` heartbeats per second**. Every cell at
  1000+ nodes is currently measuring §1's ceiling. All twelve are re-run.
- **`storm.sh`** — placement throughput. This is the number that has been
  misattributed to the scheduler; it should improve without a single
  scheduler change, which is itself the evidence that the old number was
  measuring the wrong thing.
- **`soak-24h.sh`** — re-run for the raft log growth and snapshot cadence,
  which change materially when heartbeats stop producing log entries. Nobody
  has measured this and it may be a larger win than the throughput.
- **`failover-drill.sh`** — must be extended with the phase-2 assertion:
  zero nodes marked lost across the drill. Today this drill would pass while
  silently marking nodes lost.

**Fairness record.** Every re-measured number must state that it was taken
with liveness off the raft path, because Nomad's numbers are also taken with
liveness in leader memory — that is the comparison being made, and it is now
like-for-like rather than Legion paying a cost Nomad does not.

**Supersedes a workaround.** Commit `a774fb0` put the control-plane raft
store on local NVMe. After phase 3, gp3 should be a supported choice again,
and the campaign should re-validate that explicitly rather than assume it.

---

## 11. Open questions

1. **Should `failoverGrace` be 60s or one lost window?** *Lean: 60s.* A
   grace equal to the window is theoretically sufficient — one interval is
   enough for every live worker to check in — but it leaves no room for the
   worker's own retry backoff (`HEARTBEAT_RETRY_MAX` is 10s) or for an
   election that itself took several seconds. Revisit if 60s of blind trust
   proves to matter in a real incident; the metric to look at is
   `legion_liveness_loss_suppressed_total{reason="failover_grace"}` firing
   for real losses rather than phantom ones.
2. **Should the checkpoint default to on?** *Lean: yes, at 30s.* Off would
   be simpler and strictly cheaper, but "the node list on two of my three
   managers has a blank column" is the kind of thing that gets filed as a
   bug regardless of what the docs say. Revisit if the checkpoint's commit
   shows up in `legion_raft_pipeline_wait_seconds{lane="status"}` at scale.
3. **Should the digest carry last-seen *times* rather than just ids?**
   *Lean: ids only, for now.* Ids plus the digest's own timestamp is enough
   to decide liveness at `digestInterval` resolution, and it keeps the
   message small and the merge trivial (`max`). Times would make the
   checkpoint more accurate on non-leader-observed nodes. Revisit if the
   checkpoint proves visibly wrong for nodes that dial a follower.
4. **Does the peer set need to come from raft membership, or from
   observation?** *Lean: raft membership.* The leader already knows its
   voters, so a peer that has never sent a digest is distinguishable from a
   peer that does not exist. Observation-only would silently lose the
   suppression guarantee for a manager that was partitioned before it ever
   reported.
5. **Should `heartbeat_source` be surfaced in the UI, or only in `--json`?**
   *Lean: both, but quietly* — a tooltip on the relative time rather than a
   column. The status pill is the answer to the operator's actual question;
   the freshness caveat should be findable, not loud.
6. **Is `single` topology worth a fast path?** *Lean: no.* Under `single`
   the digest never engages and the checkpoint is one commit per 30s, which
   is already nothing. Adding a branch to skip the checkpoint entirely on a
   one-manager cluster saves nothing and adds a code path.

---

## 12. Acceptance criteria

**Phase 1.** A written attribution of the residual gap: what fraction of the
16% at 4 threads is fsync and what fraction is the status lane's single
`pipeline_loop` thread, with the batch-size histogram and
`legion_raft_pipeline_wait_seconds{lane="status"}` as evidence, at 1000 and
4000 nodes on both storage classes. A go/no-go on `liveness.batchWindow`
follows from that reading; if it ships, `batchWindow: "0ms"` must reproduce
the post-`f24c6e0` numbers exactly.

**Phase 2.** On a 200-node 3-manager `lanes` cluster, forcing status-lane
leadership onto a manager that has been up for hours marks **zero** nodes
lost, where today it marks all 200 within one tick.
`legion_liveness_responsible_seconds` resets at the acquisition.
`internal_docs/TODO.md:369-378` is checked off and moved to Done.

**Phase 3.** A steady-state fleet produces **zero** status-lane replicated
ops attributable to heartbeats — `legion_raft_pipeline_depth{lane="status"}`
sits at 0 between checkpoints — while `legion_nodes{status="active"}` stays
correct on all three managers. `heartbeat-commit-probe.sh` on gp3 reaches
within 20% of the same probe on local NVMe. `legion node list` shows
`(checkpoint)` on non-leaders and a live value on the leader, and `-j` emits
`heartbeat_source`.

**Phase 4.** With the status lane and the scheduler lane deliberately split
across two managers (the harness's
`lanes_that_settle_on_different_leaders_stay_correct` setup), a 500-node
fleet dialing all three managers sustains full cadence with **zero** false
losses over 30 minutes, and dropping one manager's digest RPC for 25 seconds
marks zero of its nodes lost while raising
`legion_liveness_peer_stale{peer}`.

**All phases.** `cargo test --all --locked` green; `cargo test --test
acl_endpoints` green with no new classification;
`benchmarks/micro/scheduler_bench.rs candidate_construction` within noise of
base.
