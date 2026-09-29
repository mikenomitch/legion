# Node Liveness

Liveness is the answer to one question: *is this node still there?*

Legion keeps that answer in the **memory of the manager that owns node
status**, and writes down only the changes. A worker checking in costs no
replicated commit and no disk write. This page explains what that buys, what
it costs, and every failure mode it has to own.

Related: [Heartbeat cadence and scale](heartbeat-cadence-and-scale.md) decides
how often a worker checks in and how long it may be silent. This page decides
where that knowledge lives and how it survives a control-plane change.

---

## The model, in one sentence

> The manager that owns node status remembers who checked in; only changes are
> written down.

Concretely, three facts live in three places:

| Fact | Where | Written |
| --- | --- | --- |
| Registration, pool, attributes, schedulability | `node` (control lane) | Once, then on change |
| Liveness **status** (`active` / `lost`), `lost_at` | `node_health` (status lane) | On transition only |
| **Last heartbeat time** | The status-lane leader's memory | Every beat — no commit, no fsync |

Only the last row is high-frequency, and it is the only one that is *derived*
rather than reported.

### Why a heartbeat timestamp does not earn durability

It has three properties nothing else in Legion shares:

- **It is derived, not reported.** The worker does not send a timestamp; the
  manager stamps `now`. Nothing is lost by regenerating it — the next beat
  regenerates it two seconds later.
- **It is worthless after a few seconds.** Its entire semantic content is "was
  this within the last lost window". A ten-minute-old timestamp answers
  exactly one question ("no"), which the durable *status* already answers.
- **It cannot be trusted across a control-plane gap anyway.** Legion has
  always thrown persisted heartbeat timestamps away after a manager restart,
  because a manager that was down cannot distinguish "the node died" from "I
  was not listening". Making them durable pays an fsync for a value the code
  deliberately ignores at the one moment durability would matter.

What stays replicated: node registration and identity, pool, attributes,
`availability`, allocations, `node_health.status` and `lost_at`, health-check
payloads — everything else. And the `lost -> active` transition, which is new
information no reconciler can re-derive.

### What a recovery does

Node loss is a **cross-lane composite**: the status lane records `lost`, and
the control lane flips `node.availability` to `unavailable` — and
`availability` is the column the scheduler gates placement on. So recovery
undoes both halves, because loss did both.

A worker that comes back simply resumes heartbeating; it does **not** need to
restart or re-register. Its first beat clears the loss (`status` back to
`active`, `lost_at` cleared) and puts the node back in the scheduling pool.
Recovering only the status half would leave a returned node reading `active`
in `legion node list` while never receiving work again — a silent hole in the
fleet that looks like a scheduler bug.

One thing a heartbeat will **not** do: un-cordon a node. Only
`availability: "unavailable"` — the value node loss sets — is cleared. A node
an operator drained with `legion node cordon` or `legion node drain` is still
drained when it comes back, because that is a human's intent and a heartbeat
does not get to overrule it.

Recoveries are counted: `legion_nodes_recovered_total{path}` moves on the
heartbeat path above (`path="heartbeat"`) and when a worker re-registers
instead of resuming (`path="reregistration"`).
`legion_nodes_marked_lost_total` minus this is the fleet's *net* loss — a
flapping fleet shows as both counters climbing, where before it read as
steady loss.

### What a mass loss costs

When many nodes go lost in one tick, marking them is **batched**: one
status-lane commit records the losses, one control-lane commit flips their
availability, and one scheduler-lane commit enqueues the reschedule evals —
per tick, regardless of how many nodes dropped. Two more things bound the
storm:

- **Only nodes that hold allocations get a reschedule eval.** A lost node
  with nothing on it costs nothing until something lands on it (re-checked
  every tick).
- **Reschedule evals run one priority band below the default** (40 against
  50 at the defaults), so new job submissions overtake the recovery
  backlog instead of queueing behind it. Recovery still proceeds — the
  monitor re-drives it every tick — it just no longer freezes the
  submission path while it does. An explicit `--priority` on a submission
  behaves exactly as before.

---

## What an election does

A newly elected status-lane leader inherits **nothing**. This is the design's
central hazard, and three rules handle it.

1. **The grace clock runs from responsibility, not uptime.** Before, the
   startup grace was keyed on *process* uptime, so a manager that had been up
   for six hours could acquire status-lane leadership mid-life and start
   marking nodes lost on its very next tick — right after an election during
   which the fleet's heartbeats were failing. The clock now resets on
   **leadership acquisition** (and on process start, which is the same event
   for a single manager).
2. **Acquisition seeds every active node at `now`.** Not at its persisted
   timestamp: that value is stale by construction on a manager that has just
   taken over, and seeding from it would mark the whole fleet lost on the
   first tick. Nomad seeds every node at `failover_heartbeat_ttl` for the same
   reason.
3. **Nothing is marked lost for `liveness.failoverGrace`** (60 seconds). That
   covers an election, a worker's retry backoff (capped at 10s), and a slow
   reconnect. Nomad uses five minutes; Legion's lost window is 10 seconds
   rather than 30, so a sixth of that is enough.

The grace is a floor, not a blindfold. A node that heartbeats during it is
tracked normally from that moment, and it does **not** suppress `lost ->
active` recovery — which is the transition an operator most wants during a
recovery.

`legion_liveness_responsible_seconds` resets at every acquisition, so "we just
took over" is visible in a dashboard.

---

## Managers that do not own liveness

Under `lanes` topology the status-lane leader and the scheduler-lane leader
can be different managers, indefinitely. Workers dial one fixed manager
forever, so a manager that leads nothing still receives their heartbeats.

**The digest.** Every `liveness.digestInterval` (1 second), each manager that
does not lead the status lane sends the leader one manager-plane RPC listing
the node ids it has heard from. The cost is `managers x 1 RPC/interval`,
independent of fleet size — at 1000 nodes across 3 managers, two RPCs a second
carrying ~333 ids each, against ~500 forwarded raft writes a second before.

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
**control-lane** column that the existing mark-lost composite already flips as
a replicated write, so there is no cross-lane read to create.

**Single-manager clusters and `single` topology get all of this for free.**
There is no peer and no digest path; liveness is process-local memory and the
RPC never engages.

### Peer-gap suppression

If a peer manager's digests stop arriving, its nodes look silent to the leader
even though that manager is happily receiving their beats. A
manager-to-manager problem must not present as fleet-wide node loss, so:

> When a peer manager's last digest is older than one digest interval, **the
> node ids that peer last claimed are not eligible to be marked lost** — until
> `liveness.peerDigestGrace` (30 seconds) expires, at which point the peer is
> considered genuinely gone and its nodes become eligible again (the workers
> dialing a dead manager are genuinely unreachable).

The leader raises `legion_liveness_peer_stale{peer}` for a peer that has
missed several digests. **That is the metric to alert on** for this failure
mode: without it, one quiet manager looks like its share of the fleet dying.

Peer-gap suppression composes with the mass-disruption circuit breaker rather
than replacing it: this catches the partial case (one manager's share of the
fleet), the breaker catches the total case.

---

## Reading the last-heartbeat time

**The node's status is exact everywhere.** `node_health.status` is still
replicated, so `legion node list`, `legion node status`, the UI status pill,
and `legion_nodes{status}` are byte-for-byte correct on every manager, leader
or not. This is the important half and it does not regress.

**The last-heartbeat *timestamp* is approximate off the owner.** Every
`liveness.checkpointInterval` (30 seconds) the status-lane leader writes one
batched status-lane commit stamping every active node's **actual last observed
beat time**. That is two commits a minute in place of roughly 500 commits a
second.

```console
$ legion node list
ID          POOL     STATUS   AVAILABILITY  LAST HEARTBEAT
node-0007   default  active   available     2026-08-03T09:14:21Z
node-0008   default  active   available     2026-08-03T09:13:58Z (checkpoint)
```

`NodeMsg.heartbeat_source` carries the distinction, `--json` emits it, and the
UI shows it as a tooltip on the relative time:

- `live` — this manager owns liveness and observed the beat. Current.
- `checkpoint` — from the replicated checkpoint, accurate to within
  `checkpointInterval`.
- empty — no beat observed, or checkpoints are disabled.

Two rules make the checkpoint safe, and neither is negotiable:

- **A checkpoint is never read back as liveness.** Not by the liveness tick,
  not on election, not ever. It is display state. The seed-at-`now` rule above
  exists precisely so that a 30-second-stale checkpoint can never become a
  loss decision.
- **It records the observed time, not the write time.** A downgraded or older
  manager reading the column sees a truthful (if coarse) value rather than one
  claiming the whole fleet beat in lockstep.

Setting `checkpointInterval: "0s"` turns checkpoints off entirely — zero
heartbeat-derived writes reach raft at all, and non-owning managers report
`heartbeat_source: ""` with an empty column. Offered because some operators
prefer an honestly blank field to an approximate one.

Sorting `legion node list` by last heartbeat still works; it sorts on whatever
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

Rejected at manager startup, with the offending values named:

- `failoverGrace` shorter than the lost window (a grace shorter than the
  window it protects is not a grace).
- `digestInterval` at or above `peerDigestGrace` (a peer must get several
  chances before it is considered quiet).
- `checkpointInterval` between `0s` and `5s` (anything faster re-creates the
  per-heartbeat commit load this design removes).

---

## Metrics

| Metric | Meaning |
| --- | --- |
| `legion_liveness_tracked_nodes` | Nodes in this manager's table. Equals `legion_nodes{status="active"}` on the owner; 0 elsewhere. |
| `legion_liveness_responsible_seconds` | Seconds since this manager became responsible. Resets at every acquisition. |
| `legion_liveness_digest_rpcs_total{outcome}` | `ok` / `not_leader` / `error` / `unimplemented`. |
| `legion_liveness_digest_nodes` | Node ids per digest. |
| `legion_liveness_peer_stale{peer}` | **The metric to alert on** for a quiet peer manager. |
| `legion_liveness_loss_suppressed_total{reason}` | `failover_grace` / `peer_digest_gap` / `mass_disruption`. |
| `legion_liveness_checkpoint_seconds` | Wall-clock of one checkpoint commit. |
| `legion_nodes_recovered_total{path}` | Lost nodes that came back: `heartbeat` / `reregistration`. Read beside `legion_nodes_marked_lost_total` for net loss. |

`legion_raft_pipeline_depth{lane="status"}` should sit at approximately zero
between checkpoints on a steady-state fleet. If it does not, heartbeats are
still committing — check `legion_liveness_digest_rpcs_total{outcome="unimplemented"}`.

---

## Upgrades and downgrades

**Workers are unaffected.** No worker-plane proto field, no behavior change,
no protocol-version bump. A worker built before this change and one built
after are indistinguishable to a manager on either side.

**Managers first**, as always ([Upgrades](upgrades.md)). During the roll:

- **New manager, old peers.** `ReportNodeLiveness` returns `Unimplemented`
  from an older peer. The sender falls back to the pre-digest behavior — one
  replicated write per heartbeat — and counts
  `legion_liveness_digest_rpcs_total{outcome="unimplemented"}`. Correct, just
  slower. This is the identical degradation `ForwardLaneWrite` shipped with,
  and the identical remedy: finish the roll.
- **Old manager leads the status lane, new followers.** The old leader never
  asks for digests and keeps applying replicated heartbeat writes; new
  followers detect it and forward per heartbeat. Resolves the moment
  leadership lands on an upgraded manager.
- **New manager leads, old followers.** Old followers forward heartbeats as
  raft writes, which the new leader applies as before *and* records in its
  liveness table. Both paths converge on the same table, so the mixed state is
  correct without a special case.

**No migration.** `node_health.last_heartbeat` keeps its column and its
meaning; it is simply written by the checkpoint instead of by every beat.

> **Downgrade all managers together.** During the downgrade, workers resume
> writing heartbeats through raft within one interval, and the downgraded
> binary's own 10-second startup grace covers the gap. A *partial* downgrade
> that leaves an old manager leading the status lane while a new one holds the
> checkpoint is the one unsafe combination; it resolves within one lost window
> and is bounded by the mass-disruption circuit breaker.

---

## Known limitations

- **No per-pool liveness reporting.** The tracked-node gauge and the freshness
  ratio are fleet-wide.
- **The last-heartbeat timestamp is approximate on any manager that does not
  own liveness**, by up to `checkpointInterval`. The status is not.
- **The digest carries ids, not per-node times.** Ids plus the digest's own
  timestamp decide liveness at `digestInterval` resolution; the checkpoint is
  therefore coarser for nodes that dial a follower.
- **Liveness is not gossiped.** Per-node cost is still linear in fleet size
  from the manager's side. Gossip/SWIM would flatten that curve and remains
  open; it would replace the *input* to the liveness table and leave the
  transition, grace, checkpoint, and read-path machinery intact.
- **Multi-region / federated liveness is out of scope.**

---

## See also

- [Heartbeat cadence and scale](heartbeat-cadence-and-scale.md)
- [Managers and workers](architecture-managers-workers.md)
- [Multi-raft](multi-raft.md) — lane leadership and divergent-leadership costs
- [Raft operations](raft-operations.md), [Upgrades](upgrades.md)
- [Metrics](metrics.md), [Agent readiness](agent-readiness.md)
