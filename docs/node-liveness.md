# Node Liveness

Liveness answers one question: is this node still there?

Legion keeps that answer in the memory of the manager that owns node status,
and writes down only the changes. A worker checking in costs no replicated
commit and no disk write. This page explains what that buys you, what it
costs, and how each failure mode is handled.

Related: [Heartbeat cadence and scale](heartbeat-cadence-and-scale.md) covers
how often a worker checks in and how long it may be silent. This page covers
where that knowledge lives and how it survives a control-plane change.

---

## The model, in one sentence

> The manager that owns node status remembers who checked in, and only changes
> are written down.

Three facts live in three places:

| Fact | Where | Written |
| --- | --- | --- |
| Registration, pool, attributes, schedulability | `node` (control lane) | Once, then on change |
| Liveness **status** (`active` / `lost`), `lost_at` | `node_health` (status lane) | On transition only |
| **Last heartbeat time** | The status-lane leader's memory | Every beat, with no commit and no fsync |

Only the last row changes often. It is also the only one the manager derives
instead of the worker reporting it.

### Why a heartbeat timestamp does not earn durability

The heartbeat timestamp differs from other Legion state in three ways:

- **The manager derives it.** The worker does not send a timestamp. The
  manager stamps `now`. Losing it costs nothing, because the next beat
  regenerates it two seconds later.
- **It is useless after a few seconds.** All it tells you is whether the node
  checked in within the last lost window. A ten-minute-old timestamp can only
  say "no", and the durable status already says that.
- **It can't be trusted across a control-plane gap.** Legion throws persisted
  heartbeat timestamps away after a manager restart. A manager that was down
  cannot tell "the node died" apart from "I was not listening". Making the
  timestamps durable would pay an fsync for a value Legion ignores at the one
  moment durability would matter.

Everything else stays replicated: node registration and identity, pool,
attributes, `availability`, allocations, `node_health.status` and `lost_at`,
and health-check payloads. So does the `lost -> active` transition, which is
new information no reconciler can re-derive.

### What a recovery does

Node loss touches two lanes. The status lane records `lost`, and the control
lane flips `node.availability` to `unavailable`. The scheduler gates placement
on `availability`. Recovery undoes both halves, because loss did both.

A worker that comes back resumes heartbeating. It does not need to restart or
re-register. Its first beat clears the loss (`status` back to `active`,
`lost_at` cleared) and puts the node back in the scheduling pool. If recovery
only cleared the status, a returned node would show `active` in
`legion node list` but never receive work again. That silent gap in the fleet
would look like a scheduler bug.

A heartbeat will not un-cordon a node. It clears only
`availability: "unavailable"`, the value node loss sets. A node an operator
drained with `legion node cordon` or `legion node drain` stays drained when it
comes back. That was a person's decision, and a heartbeat does not overrule it.

Legion counts recoveries in `legion_nodes_recovered_total{path}`. It uses
`path="heartbeat"` for the heartbeat path above and `path="reregistration"`
when a worker re-registers instead of resuming.
`legion_nodes_marked_lost_total` minus this counter is the fleet's net loss. A
flapping fleet shows both counters climbing.

### What a mass loss costs

When many nodes go lost in one tick, Legion marks them in batches. Each tick
uses one status-lane commit to record the losses, one control-lane commit to
flip their availability, and one scheduler-lane commit to enqueue the
reschedule evals, however many nodes dropped. Three more things keep recovery
cheap and complete:

- **Only nodes that hold allocations get a reschedule eval.** A lost node with
  nothing on it costs nothing until something lands on it (re-checked every
  tick).
- **Reschedule evals run one priority band below the default** (40 against
  50 at the defaults). New job submissions overtake the recovery backlog
  instead of queueing behind it. Recovery still proceeds, since the monitor
  re-drives it every tick, but it doesn't block submissions while it runs. An
  explicit `--priority` on a submission behaves as before.
- **A replacement that loses a race is not dropped.** One lost node often
  holds allocations from many jobs, and a fleet-wide loss fans out into
  several reschedule evals that touch the same job. When two of them race,
  one plan is rejected on the job's version fence. The losing replan retries
  on a fresh snapshot, and if it keeps losing it hands the job to a
  `job_reschedule` eval, which converges it like any other job-level eval.
  Look for `Reconcile kept losing plan races; handing off to a
  job_reschedule eval` in the manager log.

---

## What an election does

A newly elected status-lane leader starts with an empty liveness table. This
is the main hazard in the design, and three rules handle it.

1. **The grace clock starts when the manager takes responsibility.** The
   startup grace resets on leadership acquisition, and on process start (the
   same event for a single manager). It is not tied to process uptime. A
   manager that has been up for six hours can still win status-lane
   leadership right after an election in which the fleet's heartbeats were
   failing. Without the reset, it would start marking nodes lost on its next
   tick.
2. **Acquisition seeds every active node at `now`.** It does not use the
   persisted timestamp. That value is always stale on a manager that has just
   taken over, and seeding from it would mark the whole fleet lost on the
   first tick. Nomad seeds every node at `failover_heartbeat_ttl` for the same
   reason.
3. **Nothing is marked lost for `liveness.failoverGrace`** (60 seconds). That
   covers an election, a worker's retry backoff (capped at 10s), and a slow
   reconnect. Nomad uses five minutes. Legion's lost window is 10 seconds
   instead of 30, so a sixth of that is enough.

The grace sets a minimum wait. It does not stop tracking. A node that
heartbeats during the grace is tracked normally from that moment. The grace
also does not block `lost -> active` recovery, which is the transition an
operator most wants to see during an incident.

`legion_liveness_responsible_seconds` resets at every acquisition, so you can
see a recent takeover on a dashboard.

---

## Managers that do not own liveness

Under `lanes` topology, the status-lane leader and the scheduler-lane leader
can be different managers indefinitely. Each worker dials one fixed manager,
so a manager that leads nothing still receives heartbeats.

**The digest.** Every `liveness.digestInterval` (1 second), each manager that
does not lead the status lane sends the leader one manager-plane RPC. The RPC
lists the node ids it has heard from. The cost is `managers x 1 RPC/interval`,
regardless of fleet size. At 1000 nodes across 3 managers, that is two RPCs a
second carrying about 333 ids each.

```
worker ──heartbeat──▶ manager C (leads nothing)
                        │  in memory: C notes "node-7 seen at T"
                        │  every digestInterval, ONE RPC:
                        ▼
                     manager A (status-lane leader)
                        │  liveness table: node-7 seen at T
                        │  on transition only (rare):
                        ├──status lane──▶  node_health.status = lost
                        ├──control lane─▶  node.availability = unavailable
                        └──scheduler lane▶ reschedule eval
                                              │
                                              ▼
                                        manager B (scheduler-lane leader)
                                        reads node.availability from its OWN
                                        replica. Never asks anyone.
```

The scheduler never reads liveness. Placement gates on `node.availability`, a
control-lane column. Marking a node lost already flips it as a replicated
write, so the scheduler never needs to read across lanes.

**Single-manager clusters and `single` topology need none of this.** There is
no peer and no digest path. Liveness is process-local memory and the RPC is
never used.

### Peer-gap suppression

If a peer manager's digests stop arriving, its nodes look silent to the
leader, even though that manager still receives their beats. A problem between
managers should not look like fleet-wide node loss, so:

> When a peer manager's last digest is older than one digest interval, the
> node ids that peer last claimed can't be marked lost. This lasts until
> `liveness.peerDigestGrace` (30 seconds) expires. Then the peer counts as gone
> and its nodes become eligible again, since workers dialing a dead manager are
> unreachable.

The leader raises `legion_liveness_peer_stale{peer}` for a peer that has
missed several digests. Alert on this metric for this failure mode. Without
it, one quiet manager looks like its share of the fleet dying.

Peer-gap suppression works alongside the mass-disruption circuit breaker. It
catches the partial case (one manager's share of the fleet), and the breaker
catches the total case.

---

## Reading the last-heartbeat time

**The node's status is exact everywhere.** `node_health.status` is still
replicated. `legion node list`, `legion node status`, the UI status pill, and
`legion_nodes{status}` are correct on every manager, leader or not.

**The last-heartbeat timestamp is approximate on other managers.** Every
`liveness.checkpointInterval` (30 seconds), the status-lane leader writes one
batched status-lane commit. It records each active node's actual last observed
beat time. That is two commits a minute in place of roughly 500 commits a
second.

```console
$ legion node list
ID          POOL     STATUS   AVAILABILITY  LAST HEARTBEAT
node-0007   default  active   available     2026-08-03T09:14:21Z
node-0008   default  active   available     2026-08-03T09:13:58Z (checkpoint)
```

`NodeMsg.heartbeat_source` carries the distinction. `--json` emits it, and the
UI shows it as a tooltip on the relative time:

- `live`: this manager owns liveness and observed the beat. Current.
- `checkpoint`: from the replicated checkpoint, accurate to within
  `checkpointInterval`.
- empty: no beat observed, or checkpoints are disabled.

Two rules keep the checkpoint safe:

- **A checkpoint is never read back as liveness.** Not by the liveness tick,
  and not on election. It is display state only. The seed-at-`now` rule above
  makes sure a 30-second-stale checkpoint can never lead to a loss decision.
- **It records the observed time, not the write time.** A downgraded or older
  manager reading the column sees a true (if coarse) value. It won't see one
  that claims the whole fleet beat at the same instant.

Set `checkpointInterval: "0s"` to turn checkpoints off. No heartbeat-derived
writes reach raft at all, and managers that don't own liveness report
`heartbeat_source: ""` with an empty column. Use this if you prefer a blank
field to an approximate one.

Sorting `legion node list` by last heartbeat still works. It sorts on whatever
value that manager holds.

---

## Configuration

```nickel
# configs/manager.ncl — every field optional; omitting the block
# reproduces the defaults below.
liveness = {
  # Accumulation window for the heartbeat group commit. "0ms" (the default)
  # is commit-while-you-can batching. A steady-state heartbeat does not
  # commit at all, so this applies only to the checkpoint and to the
  # mixed-version forwarding fallback.
  batchWindow = "0ms",

  # After acquiring status-lane leadership (or starting the process), mark
  # nothing lost for this long.
  failoverGrace = "60s",

  # How often a manager that does NOT lead the status lane reports the nodes
  # it heard from. ONE RPC per manager per interval, whatever the fleet size.
  digestInterval = "1s",

  # How long the leader keeps trusting a silent peer manager's last digest.
  peerDigestGrace = "30s",

  # How often the leader replicates a display checkpoint. DISPLAY ONLY.
  # "0s" disables checkpoints entirely.
  checkpointInterval = "30s",
}
```

The manager refuses to start, naming the offending values, if:

- `failoverGrace` is shorter than the lost window. A grace shorter than the
  window it protects does nothing.
- `digestInterval` is at or above `peerDigestGrace`. A peer must get several
  chances before it counts as quiet.
- `checkpointInterval` is between `0s` and `5s`. Anything faster brings back
  the per-heartbeat commit load this design removes.

---

## Metrics

| Metric | Meaning |
| --- | --- |
| `legion_liveness_tracked_nodes` | Nodes in this manager's table. Equals `legion_nodes{status="active"}` on the owner and 0 elsewhere. |
| `legion_liveness_responsible_seconds` | Seconds since this manager became responsible. Resets at every acquisition. |
| `legion_liveness_digest_rpcs_total{outcome}` | `ok` / `not_leader` / `error` / `unimplemented`. |
| `legion_liveness_digest_nodes` | Node ids per digest. |
| `legion_liveness_peer_stale{peer}` | Alert on this for a quiet peer manager. |
| `legion_liveness_loss_suppressed_total{reason}` | `failover_grace` / `peer_digest_gap` / `mass_disruption`. |
| `legion_liveness_checkpoint_seconds` | Wall-clock of one checkpoint commit. |
| `legion_nodes_recovered_total{path}` | Lost nodes that came back: `heartbeat` / `reregistration`. Read beside `legion_nodes_marked_lost_total` for net loss. |

On a steady-state fleet, `legion_raft_pipeline_depth{lane="status"}` should
sit near zero between checkpoints. If it doesn't, heartbeats are still
committing. Check `legion_liveness_digest_rpcs_total{outcome="unimplemented"}`.

---

## Upgrades and downgrades

**Workers are unaffected.** There is no worker-plane proto field, no behavior
change, and no protocol-version bump. A manager on either side of the upgrade
can't tell an older worker from a newer one.

**Upgrade managers first**, as always ([Upgrades](upgrades.md)). During the
roll:

- **New manager, old peers.** `ReportNodeLiveness` returns `Unimplemented`
  from an older peer. The sender falls back to one replicated write per
  heartbeat and counts
  `legion_liveness_digest_rpcs_total{outcome="unimplemented"}`. This is
  correct, just slower. `ForwardLaneWrite` degrades the same way, and the fix
  is the same: finish the roll.
- **Old manager leads the status lane, new followers.** The old leader never
  asks for digests and keeps applying replicated heartbeat writes. New
  followers detect this and forward each heartbeat. This resolves as soon as
  leadership moves to an upgraded manager.
- **New manager leads, old followers.** Old followers forward heartbeats as
  raft writes. The new leader applies them as before and also records them in
  its liveness table. Both paths feed the same table, so the mixed state is
  correct without a special case.

**No migration.** `node_health.last_heartbeat` keeps its column and its
meaning. The checkpoint writes it instead of every beat.

> **Downgrade all managers together.** During the downgrade, workers resume
> writing heartbeats through raft within one interval, and the downgraded
> binary's own startup grace covers the gap. Older binaries hold off marking
> nodes lost until the manager has been up for one fixed 10-second lost window. A partial downgrade
> that leaves an old manager leading the status lane while a new one holds the
> checkpoint is the one unsafe combination. It resolves within one lost window,
> and the mass-disruption circuit breaker limits the damage.

---

## Known limitations

- **No per-pool liveness reporting.** `legion_liveness_tracked_nodes` and
  `legion_heartbeat_freshness_ratio` are fleet-wide, with no pool label. See
  [heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md) for the
  freshness ratio.
- **The last-heartbeat timestamp is approximate on any manager that does not
  own liveness**, by up to `checkpointInterval`. The status is exact.
- **The digest carries ids, not per-node times.** Liveness for those nodes is
  decided from the ids plus the digest's own timestamp, at `digestInterval`
  resolution. The checkpoint is therefore coarser for nodes that dial a
  follower.
- **Liveness is not gossiped.** Per-node cost on the manager still grows
  linearly with fleet size. Gossip or SWIM would flatten that curve and is not
  implemented. It would replace only the input to the liveness table. The
  transition, grace, checkpoint, and read-path logic would stay the same.
- **Multi-region and federated liveness are out of scope.**

---

## See also

- [Heartbeat cadence and scale](heartbeat-cadence-and-scale.md)
- [Managers and workers](architecture-managers-workers.md)
- [Multi-raft](multi-raft.md): lane leadership and divergent-leadership costs
- [Raft operations](raft-operations.md), [Upgrades](upgrades.md)
- [Metrics](metrics.md), [Agent readiness](agent-readiness.md)
