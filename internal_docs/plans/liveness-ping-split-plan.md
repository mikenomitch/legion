# Liveness Pings: Split Detection From Desired-State Sync

> **Judgement calls:** [`../calls/liveness-ping-split-plan_calls.md`](../calls/liveness-ping-split-plan_calls.md)

**Status: SHIPPED** (2026-11-12) — all three phases are built. The public
page is [`docs/heartbeat-cadence-and-scale.md`](../../docs/heartbeat-cadence-and-scale.md)
("Pings and heartbeats", "Per pool").

**Deliberately not done, and open:**

- **The AWS rig rerun.** The ping and heartbeat costs are measured in-process
  and over loopback (§9); the fleet-level claim (a 20,000-node fleet gets a
  25s window for about a third of a core of pings at most) is argued from
  those numbers, not measured on the rig. Filed in the rig plan's "Campaign runs still owed" ([aws-comparative-benchmark-plan.md](aws-comparative-benchmark-plan.md)).
- **Raising `targetRatePerSecond`.** The TODO item this plan closes proposed
  rerunning 20k nodes at 5,000 and 10,000 and raising the default. With
  detection split off, the full-heartbeat rate governs report latency and
  load, not detection, so the default stays at 2,500 until that rerun says
  otherwise ([calls #4](../calls/liveness-ping-split-plan_calls.md)).
- **Pings as `StreamWork` frames.** A ping is a unary RPC and pays per-call
  transport cost, which the measurement shows is most of its cost (116 µs
  client and server on loopback, against a 0.6 µs handler); a frame on the
  already-open stream would not. It is what would let the default ping rate
  rise far enough for a 10s window at 20,000 nodes
  ([calls #1](../calls/liveness-ping-split-plan_calls.md)).

Follows [`heartbeat-cadence-at-scale-plan.md`](heartbeat-cadence-at-scale-plan.md)
(adaptive cadence, per-node lost windows) and
[`liveness-off-the-raft-path-plan.md`](liveness-off-the-raft-path-plan.md)
(liveness in the leader's memory). It closes the two "Heartbeats and liveness"
items in `internal_docs/TODO.md`: *tighten failure detection at large fleets*
and *per-pool heartbeat freshness*.

---

## 1. Problem

Adaptive cadence backs the heartbeat off as the fleet grows, and the lost
window moves with it, because the window is `missedBeatsBeforeLost x the
node's interval`. At the default `targetRatePerSecond` (2,500/s) a 20,000-node
fleet is served 21 seconds, so Legion takes up to **105 seconds** to notice a
dead machine, against 10 on a small fleet.

That trade was made because a heartbeat costs the manager real work, and the
cost is not liveness. Proving a node is alive is one map write in the leader's
memory (`LivenessTable::observe`). Everything else a beat does — the
blocking-pool hop, applying health and failure reports, building the endpoint
and policy feeds, claim verdicts, stop notices, checkpoint requests, runtime
disables, owner notices (`crates/manager/src/manager/server.rs`, `heartbeat`,
some 450 lines) — is desired-state sync. The two jobs share one RPC and so
one interval, and slowing the expensive one slows the cheap one with it.

A second, smaller problem: `legion_heartbeat_freshness_ratio` judges every
node against the one served interval. A 60-second batch pool beside a 2-second
latency pool reads as late on every tick, and a struggling small pool hides
inside a large healthy one. Operators have one blended number.

## 2. Prior art

- **Kubernetes** split exactly these two jobs (KEP-589): a tiny `Lease`
  object per node renewed every 10s carries liveness, and the heavy
  `NodeStatus` update is reported every 5 minutes when nothing changed. Etcd
  write volume in a 5k-node cluster fell from 150 MB/min to 30. We take the
  split; we do not take the separate object, because Legion's liveness is
  already in memory and needs no write at all.
- **Nomad** has one heartbeat whose TTL scales with fleet size to hold about
  `max_heartbeats_per_second` (50) at the servers. Detection at 5k nodes is
  100–200s. This is what Legion's adaptive cadence already matched, and what
  this plan moves away from.
- **Consul / Serf (SWIM)** separate failure detection from state entirely:
  cheap gossip probes detect, anti-entropy syncs. Gossip stays a non-goal
  (it is a different trust and membership model), but the principle — the
  detector must be cheaper than the sync — is the one we take.
- **HTTP/2 PING** frames and TCP keepalive prove a connection is open, not
  that the worker's loop is running; neither can carry the served intervals
  or feed the worker's self-fence clock. Rejected
  ([calls #1](../calls/liveness-ping-split-plan_calls.md)).

## 3. Jobs to be done

**Cluster operator**
- When my fleet grows past a few thousand machines, I want a dead machine
  noticed in seconds, not minutes, so its work moves before anyone pages me.
- When I run a slow batch pool beside a fast latency pool, I want to see each
  pool's health on its own, so a problem in the small pool does not hide
  inside the big one.
- When I size a control plane, I want the cost of detection and the cost of
  status reporting to be separate numbers, so I can buy one without the other.

**On-call responder**
- When nodes start being marked lost, I want to tell "the control plane is
  behind" from "the machines are gone" for the pool that is actually
  affected, so I fix the right thing.

**Autonomous agent**
- When I check whether the fleet is keeping up, I want the per-pool numbers
  in the same structured reply, so I do not misread a blended ratio.

*Not in scope:* the job author. Nothing in a jobspec changes; tasks see the
same lost-node behavior, only sooner.

### User scenarios

1. A 20,000-node fleet at the defaults loses a machine. Before: up to 105s
   to mark it lost. After: up to 25s, while the leader answers no more full
   heartbeats than before. A 10,000-node fleet goes from 40s to 10s.
2. A cluster has 4,000 nodes in a `batch` pool beating every 60s and 40 in a
   `latency` pool where 28 are falling behind. Before: the freshness ratio
   reads 0.99 (or, worse, the batch pool reads as late). After: `legion node
   cadence` shows `latency 0.30`, the warning names the pool, and
   `legion_heartbeat_pool_freshness_ratio{pool="latency"}` alerts.

## 4. Goals and non-goals

**Goals**
- Lost windows stop growing with the full-heartbeat interval: detection is
  sized from its own, much higher, rate.
- No behavior change for fleets under the point where the full heartbeat
  backs off (2,500 nodes at the defaults): no pings, same intervals.
- Freshness judged per node against its own interval, and broken down per
  pool in the metric, the CLI, the RPC, and the warning.

**Non-goals**
- Gossip or SWIM between workers.
- Moving reports off the heartbeat (the "start reports wait for a heartbeat"
  TODO owns that).
- A per-pool cadence knob. Per-pool cadence already exists as the worker-type
  `heartbeatSeconds`.
- Changing `missedBeatsBeforeLost` semantics.

## 5. What exists today

- `crates/manager/src/manager/cadence.rs` — the pure controller:
  `target_interval`, `ladder_rung`, `advance_ladder`, `freshness_ratio`,
  `cadence_report`, `decide_lost_nodes`.
- `crates/manager/src/manager/daemon.rs`, `start_node_status_checker` — one
  ladder, sized from `targetRatePerSecond`; widens and narrows per-node
  windows; publishes `set_served_interval`.
- `crates/manager/src/storage/liveness.rs` — `LivenessTable`, holding the
  served interval for the handlers, relayed to non-leaders on the digest
  reply (`NodeLivenessDigestReply.served_interval_seconds`).
- `crates/manager/src/storage/store.rs`, `update_node_heartbeat` and
  `beat_disposition` — Active (memory only) / Transition (replicated) / Ghost.
- `HeartbeatReply.next_interval_seconds` — the one served interval.
- `crates/worker/src/worker/daemon.rs`, `start_heartbeats` — one loop, one
  interval (`adopt_served_interval`), self-fence clock fed by heartbeat
  success.
- `crates/worker/src/worker/simulate.rs` — the simulated fleet mirrors it.

## 6. Design

### 6.1 Two intervals

| Interval | Sized from | Governs |
| --- | --- | --- |
| Liveness | `heartbeat.pingRatePerSecond` (new, default 10,000) | How often a worker checks in. Every lost window. |
| Sync | `heartbeat.targetRatePerSecond` (unchanged, 2,500) | How often a worker sends a full heartbeat. Raised to the liveness interval when below it. |

Same formula, same ladder, same `minInterval`/`maxInterval`, two rates:

```
liveness = rung(clamp(ceil(H x N / pingRatePerSecond),   min, max))
sync     = max(liveness, rung(clamp(ceil(H x N / targetRatePerSecond), min, max)))
```

Only the liveness ladder widens and narrows windows. `at_max_interval` is
about the liveness interval (it is the one the self-fence cares about).

### 6.2 The surface

```nickel
heartbeat = {
  targetRatePerSecond = 2500,    # full heartbeats
  pingRatePerSecond   = 10000,   # liveness; set equal to the above to turn pings off
}
```

```proto
rpc Ping (PingRequest) returns (PingReply);
message PingRequest { string node_id = 1; }
message PingReply {
  optional uint32 next_interval_seconds = 1;  // liveness
  optional uint32 sync_interval_seconds = 2;
  bool sync_now = 3;                          // this ping recovered a lost node
}
// HeartbeatReply: next_interval_seconds now means the liveness interval;
//                 new optional uint32 sync_interval_seconds = 28.
// NodeLivenessDigestReply: new served_sync_interval_seconds = 6.
// GetHeartbeatCadenceReply: new sync_interval_seconds = 12,
//                           repeated PoolHeartbeatCadence pools = 13.
```

```console
$ legion node cadence
Fleet             10000 nodes
Liveness interval 2s  (adaptive)
Full heartbeat    every 8s  (pings in between)
Lost window       10s  (5 missed beats)
...
POOL                   NODES  INTERVAL  LOST WINDOW  FRESHNESS
batch                   4000       60s         300s       1.00
latency                   40        2s          10s       0.30
```

New metrics: `legion_pings_total`, `legion_heartbeat_sync_interval_seconds`,
`legion_heartbeat_pool_freshness_ratio{pool}`, `legion_worker_pings_total{outcome}`.

### 6.3 The manager's ping handler

Authenticate exactly as a heartbeat (worker plane, node binding), then
`Store::observe_ping`: `beat_disposition` plus `LivenessTable::observe` on the
async worker for an active node. Anything that may commit — a `lost -> active`
transition, or the mixed-version fallback — goes to the blocking pool through
`update_node_heartbeat`, so a ping recovers a node exactly as a heartbeat does,
and sets `sync_now` when it did. A ghost gets `NotFound`.

### 6.4 The worker loop

One loop on the liveness interval. Each tick: a full heartbeat if one is due
(`since_last_full + liveness/2 >= sync`, or none yet, or an early wake, or
`sync_now`), a ping otherwise. Either success resets the self-fence clock,
records manager contact, and adopts the served intervals (liveness as a floor
over `heartbeatSeconds`, sync as a floor over liveness). The drain window for
graceful stops follows the sync interval, because endpoint feeds ride the
full heartbeat.

With equal intervals every tick is a full heartbeat: a fleet under 2,500 nodes
sends no pings and behaves exactly as before.

### 6.5 Per-node and per-pool freshness

`NodeLiveness` gains the node's `pool`; a node's interval is its window over
`missedBeatsBeforeLost`. Freshness counts a node fresh when its age is within
1.2x its own interval. `pool_cadence` groups the snapshot by pool (median
interval, freshness when the pool has 20+ nodes). The warning fires on the
lowest of the fleet and pool ratios and names the pool when a pool tripped it.

## 7. Proto, config, persistence

- Proto as in §6.2, all documented in `proto/legion.proto`.
- Manager config `heartbeat.pingRatePerSecond`, validated positive; the error
  names the off switch. In `configs/manager.ncl`. No job-config field.
- Persistence: none. `active_node_liveness_rows` selects `node.pool`.
- `crates/manager/src/manager/rpc_access.rs` and
  `tests/integration/acl_endpoints.rs` classify `Ping` as worker plane.

## 8. Phases

1. **Per-node and per-pool freshness** (manager only). Freshness against each
   node's own interval, the pool breakdown in the report, metric, RPC, CLI,
   and warning. Valuable alone: fixes the blended ratio.
2. **The `Ping` RPC and the second ladder** (manager). Serves both intervals;
   workers that do not know `sync_interval_seconds` keep beating full
   heartbeats at the liveness interval, never falsely lost.
3. **Workers ping** (worker and simulated fleet). Detection decouples.

All three shipped together.

## 9. Testing

- Unit (`cadence.rs`): a slow pool judged against its own interval; a
  struggling pool showing through a healthy fleet and named in the warning;
  the defaults table for both rates (`default_ping_rate_keeps_detection_fast_while_sync_backs_off_test`).
- Unit (worker `daemon.rs`): `sync_interval_for` floors, and
  `full_heartbeat_due` landing one full heartbeat every ~ten beats at 2s/21s.
- Integration (`tests/integration/node_liveness.rs`): a ping refreshes
  liveness with no commit and returns both intervals; a ping recovers a lost
  node with `sync_now`; a ghost ping is `NotFound`; the cadence report's pool
  rows on a real store.
- Integration (`tests/integration/acl_endpoints.rs`): `Ping` classified,
  refused without a node token and with a foreign one.
- Measurement (`tests/integration/heartbeat_ping_measure.rs`, ignored): ping
  vs. an idle node's full heartbeat, handler-only and over loopback gRPC.
  Release build, 4 vCPU: handler 0.6 µs per ping against 45 µs per idle full
  heartbeat; over loopback, 116 µs against 199 µs client and server. The
  public page carries the table, and the default ping rate follows from the
  loopback figure (calls #4).

## 10. Rollout

On by default: a cluster that sets nothing gets pings once it passes 2,500
nodes. Under that, nothing observable changes. Upgrade managers first, as
`docs/upgrades.md` already requires:

- An old worker against a new manager reads `next_interval_seconds` (now the
  liveness interval) as its heartbeat interval and sends full heartbeats at
  that cadence: never falsely lost, just more load than the controller sized
  for.
- A new worker against an old manager gets no `sync_interval_seconds` and
  sends a full heartbeat every beat, as before.

A cluster still in the mixed-version raft fallback commits pings like
heartbeats; the public page says to set the two rates equal there.

## 11. Acceptance criteria

- At the defaults, the served liveness interval stays 2s and the lost window
  10s up to 10,000 nodes, and 20,000 nodes get 25s (unit test), while the sync
  interval follows the old table.
- A worker served 2s/21s sends nine pings to one full heartbeat (2s/8s: three
  to one), and either resets its self-fence clock.
- `legion node cadence` and `GetHeartbeatCadence` show per-pool rows;
  `legion_heartbeat_pool_freshness_ratio{pool}` is published for pools of 20+.
- Under 2,500 nodes, `legion_pings_total` stays zero.
