# Running Raft on Legion (Replicated Manager State)

This guide explains how to run Legion managers in **Raft persistence mode**.
In this mode, several managers form one cluster. Every store mutation is
committed by a quorum before it is acknowledged or visible, so losing any
minority of managers loses no committed write.

> Current scope: write replication, leader election, membership (join
> tokens and operator commands), non-leader write rejection with a leader
> hint, snapshots and log compaction, crash/restart recovery (log replay or
> snapshot install), and `legion state backup` / `--restore-from` are all
> enabled. For the engine-level reference, see `docs/multi-raft.md`.

## How replication works (operator's view)

- One manager is the **leader**, and only it accepts mutations. A mutation
  runs on the leader, and its row-level effects (a SQLite session
  changeset) are proposed through [openraft]. Once a quorum commits the
  entry, every manager (the leader included) applies the same bytes to its
  local state DB. Replicas apply effects rather than re-running code, so
  they cannot diverge.
- Nothing is visible anywhere, even on the leader, before quorum commit.
  Callers get their reply only after the local apply, so read-your-writes
  holds on the leader.
- Mutations sent to a **non-leader** are rejected with
  `FAILED_PRECONDITION`. The message includes the current leader's name and
  address, and the CLI shows this hint. Point clients (`LEGION_ADDR`) at the
  leader. Every manager serves reads from its local replica.
- **Scheduling is leader-only.** Every manager replicates eval-queue and
  scheduler state, but only the leader's eval workers and node status
  checker act on it. After a leadership change, the new leader's loops take
  over within a few seconds and resume the replicated queue where the old
  leader left off.
- The raft transport is a gRPC service (`RaftTransport`) that shares the
  manager listener. It is authenticated by a cluster secret minted at
  bootstrap and distributed only inside join tokens.

## Configure a cluster

See `configs/manager-raft.ncl` for a complete, commented 3-node example.
The minimal shape:

```nickel
{
  persistence = {
    type = "raft",
    raft = {
      nodeId = "mgr-a",                  # stable unique identity
      bootstrap = true,                  # FIRST manager only
      advertiseAddr = "10.0.0.1:4747",   # required behind wildcard binds
      peers = ["mgr-a=10.0.0.1:4747", "mgr-b=10.0.0.2:4747", "mgr-c=10.0.0.3:4747"],
      dataDir = ".legion/raft",          # raft log + cluster secret
      topology = "single",               # default; 'lanes' is experimental
      joinTokenTtlSeconds = 900,
    },
  },
}
```

- `nodeId`: the manager's identity in the cluster. It must be unique, and it
  is permanent once the manager joins (the raft member ID derives from it).
- `bootstrap`: set `true` on exactly one manager, once, to create the
  cluster. That manager elects itself as the sole voter. It is idempotent
  across restarts.
- `advertiseAddr`: the `host:port` that peers dial for this manager's
  transport.
- `peers`: a list used for information and name resolution
  (`name=host:port`). Actual membership comes from joins and operator
  commands.
- `topology`: `single` (one raft group carries all state, the recommended
  default) or `lanes` (separate control, scheduler, and status groups, for
  write loads that bottleneck one group's log). It is fixed at the first
  engine start for each data dir. See `docs/multi-raft.md` § "Topology:
  `single` vs `lanes`".
- `snapshotEntries`: the number of applied entries between whole-DB
  snapshots (default 10000). The raft log is compacted behind each
  snapshot.

For the full field-by-field reference (including `clusterSecret`,
`dataDir`, and `forceNewCluster`), see `docs/multi-raft.md`.

### Bootstrap and grow

```bash
# 1. First manager (bootstrap: true in its config)
legion start manager -c configs/manager-raft.ncl

# 2. Mint a join token on it
legion raft join-token
# token=lgnraft_...  expires_at=...

# 3. Start each additional manager with bootstrap: false and
#    joinToken: 'lgnraft_...' in its raft config.
```

A joining manager enrolls itself in the background. It dials the leader,
is added as a **learner on every raft group**, and catches up by log replay
(or by snapshot install, when the leader has compacted past what replay
could serve). Then it is promoted to **voter**. Progress appears in both
managers' logs and in `legion raft history`.

The token embeds the cluster secret, so treat it as a secret. Mint fresh
tokens as needed. They expire after `joinTokenTtlSeconds`.

Restarted members do not need a token. They recover from their raft log
and rejoin automatically.

### Upgrading an existing single-manager cluster

Set `persistence.type` to `raft` (with `bootstrap = true`) on the existing
manager. The first raft start **adopts the existing `store.sqlite` as the
initial replicated state** and builds a seed snapshot of it, so even late
joiners receive the full baseline through snapshot install. See
`docs/upgrades.md` ("Raft persistence upgrades") for the procedure.

## CLI operations

### Status

```bash
legion raft status
```

This reports the engine state: role, current leader, term, last log and
applied indexes, and voters and learners. Under `topology = "lanes"`, it
prints one line per raft group. Every lane also gets a quorum line:

```
voters 3 (quorum 2) — survives 1 failure(s)
```

A `⚠` and a plain-language advisory are appended when the voter count is
even, or below `persistence.raft.expectedVoters`. For example: "2 voters
need both alive to commit — this cluster survives zero failures. Use 1, 3,
or 5 voters; add one more manager or remove one." The `GetRaftStatus` reply
carries the same fields (`lanes[].voters/quorum/survives/warning`). See
docs/multi-raft.md, "How many managers?".

**Membership changes in flight.** A raft membership change happens in two
steps. First comes a *joint* configuration that holds both the old and the
new voter sets, then the uniform new one. A joint configuration needs a
majority of **each** set to commit or elect. So a lane in that window
reports the strictest set's `quorum` and `survives`. For example, growing
`{a,b}` to `{a,b,c}` reports that it survives **zero** failures, because
it still needs both `a` and `b`. The lane also shows the advisory
`membership change in flight: this lane is in a JOINT configuration …`.

Do not remove or restart a manager while a lane shows this advisory. If it
does not clear, re-issue the join. Membership changes are idempotent.

The top-level `voters` list shows managers that are **settled voters on
every lane**. A manager appears there only once every lane carries it in a
uniform membership. Under `topology = "lanes"`, this is stricter than any
single lane's list, so the headline never reports a fault-tolerant cluster
while one lane is still mid-join.

### History

```bash
legion raft history --limit 100
```

This lists transitions recorded from the engine's metrics stream
(elections, role changes, membership changes, and join-token issuance),
plus operator commands.

### Commands

```bash
legion raft command trigger-election
legion raft command add-learner --target mgr-d@10.0.0.4:4747
legion raft command add-voter   --target mgr-d
legion raft command remove-member --target mgr-d
```

These call the openraft membership APIs. Targets are `name@host:port`, or
a bare `name` that resolves from `raft.peers`.

Membership changes compute the **resulting** voter count. Legion warns
loudly and blocks rarely:

- An **even result** **proceeds**. This covers `add-voter` onto an odd
  cluster, `remove-member` from an odd cluster, and a join promotion
  through 2 voters. The advisory is logged at WARN on the manager, recorded
  as a `membership` event in `legion raft history`, and printed in the
  command's own output (`⚠ …`, carried in `IssueRaftCommandReply.warning`).
  Legion never withholds a join promotion to keep the voter count odd.
- `remove-member` is blocked in these cases, and each refusal message
  includes the fix:
  - The removal would leave **zero voters**.
  - The remaining voters could not form the resulting quorum **right now**.
    Legion checks this by actively probing each remaining voter's transport
    listener (a fresh dial with a 2s timeout). A membership change whose
    new voter set cannot acknowledge it can never commit, and it would
    leave the lane stuck in a joint configuration.
  - **Self-removal.** The manager currently leading every lane cannot
    remove itself. Committing a config that excludes itself deposes it
    mid-removal, which leaves the change half-applied across lanes. Use the
    two-step decommission below instead.
- Each lane's membership change has a 30s limit. If a remaining voter dies
  between the probe and the commit, the command returns an error with the
  recovery path instead of hanging. Restore a remaining voter, and the
  change then commits on its own. Re-running the command is safe.

**Decommissioning the current leader** takes two steps:

```bash
legion raft command trigger-election --target <another-manager>
```

Then, once `legion raft status` shows that manager leading every lane:

```bash
legion raft command remove-member --target <old-leader>
```

The current engine does **not support** `step-down` or `transfer-leader`
(openraft 0.9 has no leader-transfer API). Both return an error that says
so. To move leadership, run `trigger-election` against the manager you want
as the new leader.

### Join token

```bash
legion raft join-token
```

Any running member can issue a join token. The embedded leader address
always points at the current leader.

## UI operations

Open the Legion UI and go to **Cluster → Raft**. It shows the status card,
operator command buttons, join-token issuance, and event history, all
backed by the same engine state as the CLI.

## Operational notes

- **Quorum:** run an odd number of managers (1, 3, or 5). A majority must
  be up for writes. If quorum is lost, reads are still served from local
  replicas. Set `persistence.raft.expectedVoters`, and every surface
  (config load, `raft status`, membership commands, `/metrics`, `/readyz`)
  warns when the voter count is even or short. See docs/multi-raft.md, "How
  many managers?".
- **Restart without deadlock:** a restarted member may briefly log that no
  leader is known and continue without one. It catches up as soon as its
  listener is up and peers reconnect.
- **Observability:** `legion raft status` and `history`, the UI Raft page,
  and `/readyz` all report raft state. In raft mode, `/readyz` requires a
  known leader. While the voter count is even, it also includes the
  voter-parity advisory as a `note:` line without failing readiness.
  `/metrics` emits per-lane `legion_raft_*` series: voters, quorum, term,
  commit and applied index, apply lag, leadership, write latency, op counts,
  and pipeline depth. See `docs/metrics.md` for the full list.
- **Snapshots and compaction:** these are automatic. Legion takes a
  whole-DB snapshot every `raft.snapshotEntries` applied entries and purges
  the log behind it. Lagging followers and new joiners catch up by snapshot
  install. There is no manual compaction, and you don't need one.
- **Backup and restore:** use `legion state backup` (leader-only in raft
  mode) and `legion start manager --restore-from <backup>`. See
  `docs/upgrades.md`.
- **Data layout:** the replicated state DB lives at
  `<storePath>/store.sqlite`. The raft log, snapshots, and cluster secret
  live under `raft.dataDir` (per lane). Both must survive restarts
  together. See `forceNewCluster` in `docs/multi-raft.md` for what happens
  when they get out of sync.
- **Workers and failover:** each worker dials one fixed manager. Sometimes
  that manager cannot propose a worker's write, because it is a follower
  after a failover or does not lead the owning lane under
  `topology = "lanes"`. In that case it forwards the write to the leader
  over the authenticated raft transport and answers the worker after the
  forwarded write commits. Failure reports, health reports, and volume
  reports keep working without the worker redialing. See
  `docs/multi-raft.md`, "Non-leader write redirects and forwarding".
- **Heartbeats do not go through raft.** A steady-state heartbeat is one
  write into the memory of the manager that owns node status. A manager
  that does not own node status sends one digest RPC per second, regardless
  of fleet size. This keeps the highest-frequency write in the system off
  the log. As a result, fast storage for the raft store is a **performance
  preference rather than a requirement**. Network-attached disks (EBS gp3
  and equivalents) are a supported choice for the control plane. Local NVMe
  adds headroom on the paths that still commit. See
  `docs/node-liveness.md`.
- **Elections are quiet.** A newly elected status-lane leader inherits no
  liveness state. It seeds every active node at `now` and marks nothing
  lost for `liveness.failoverGrace` (60s). A failover drill on a live fleet
  should mark **zero** nodes lost. If it marks any, check
  `legion_liveness_loss_suppressed_total`.

### Recovering a replica that stopped a lane on an apply conflict

Replicas apply row changesets with a conflict-ABORT policy. Any conflict
means the replica's table has diverged from the leader's history. Instead
of diverging silently, the lane STOPS on that node and logs a
changeset-conflict error. Restarting the manager replays into the same
conflict. The recovery paths, best first:

1. **Snapshot the leader past the poisoned entry.** The node catches up by
   snapshot install (which replaces the diverged tables completely) once
   the leader's log is compacted beyond the entry it cannot apply. A
   snapshot happens automatically every `raft.snapshotEntries` applied
   writes. On a quiet cluster, generate write traffic, or temporarily lower
   `snapshotEntries` on the leader and restart it.
2. **Rebuild the member.** Remove it (`remove-member`), wipe its
   `raft.dataDir` AND `store.sqlite`, and re-join it with a fresh token. It
   comes back through snapshot install.

If the SAME conflict appears on a majority of replicas, stop and treat it
as a bug, since the leader and replicas disagree about history. Take a
`legion state backup` where possible and file the changeset-conflict log
lines.

## Not yet done

- Automatic forwarding of **client** writes from followers. Today, followers
  reject client writes with a leader hint. The worker plane and internal
  lane halves do forward (see above).
- `topology = "lanes"` is runnable and measured but stays opt-in. See
  `docs/multi-raft.md` ("Topology: `single` vs `lanes`").
- `step-down` / `transfer-leader` (openraft 0.9 has no leader-transfer API).
- Transport mTLS. A shared cluster secret secures the raft transport.

For the full list of limitations, see `docs/multi-raft.md` ("Limitations
and follow-ups").

[openraft]: https://github.com/databendlabs/openraft
