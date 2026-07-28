# Running Raft on Legion (Replicated Manager State)

This guide explains how to run Legion managers in **Raft persistence mode**:
a multi-manager cluster where every store mutation is committed by quorum
before it is acknowledged or visible, and the loss of any minority of
managers loses no committed write.

> Current scope: write replication, leader election, membership (join
> tokens + operator commands), non-leader write rejection with a leader
> hint, snapshots + log compaction, crash/restart recovery (log replay or
> snapshot install), and `legion state backup` / `--restore-from` are all
> **real and enabled**. Engine-level reference: `docs/multi-raft.md`.

## How replication works (operator's view)

- One manager is the **leader**; only it accepts mutations. A mutation
  executes on the leader, its exact row-level effects (a SQLite session
  changeset) are proposed through [openraft], and once a quorum commits the
  entry every manager — leader included — applies the same bytes to its
  local state DB. Replicas cannot diverge: they apply effects, not code.
- Nothing is visible anywhere (not even on the leader) before quorum
  commit. Callers get their reply only after the local apply, so
  read-your-writes holds on the leader.
- Mutations sent to a **non-leader** are rejected with
  `FAILED_PRECONDITION` and the current leader's name and address in the
  message; the CLI surfaces the hint. Point clients (`LEGION_ADDR`) at the
  leader. Reads are served by every manager from its local replica.
- **Scheduling is leader-only**: every manager replicates eval-queue and
  scheduler state, but only the leader's eval workers and node status
  checker act. On a leadership change the new leader's loops take over
  within a few seconds, resuming the replicated queue where the old leader
  left it.
- The raft transport is a gRPC service (`RaftTransport`) sharing the
  manager listener, authenticated by a cluster secret minted at bootstrap
  and distributed only inside join tokens.

## Configure a cluster

See `configs/manager-raft.jsonnet` for a complete commented 3-node example.
Minimal shape:

```jsonnet
{
  persistence: {
    type: 'raft',
    raft: {
      nodeId: 'mgr-a',                  // stable unique identity
      bootstrap: true,                  // FIRST manager only
      advertiseAddr: '10.0.0.1:4747',   // required behind wildcard binds
      peers: ['mgr-a=10.0.0.1:4747', 'mgr-b=10.0.0.2:4747', 'mgr-c=10.0.0.3:4747'],
      dataDir: '.legion/raft',          // raft log + cluster secret
      topology: 'single',               // default; 'lanes' is experimental
      joinTokenTtlSeconds: 900,
    },
  },
}
```

- `nodeId`: the manager's identity in the cluster. Unique, and permanent
  once joined (the raft member id derives from it).
- `bootstrap`: `true` on exactly one manager, once, to create the cluster
  (it elects itself as sole voter). Idempotent across restarts.
- `advertiseAddr`: `host:port` peers dial for this manager's transport.
- `peers`: informational/name-resolution list (`name=host:port`); real
  membership is driven by joins and operator commands.
- `topology`: `single` (one raft group carries all state — recommended).
  `lanes` (independent control/scheduler/status groups) is experimental
  and currently **refused at engine start** — per-lane table ownership is
  not yet disjoint, so it could diverge replicas (see `docs/multi-raft.md`).
- `snapshotEntries`: applied entries between whole-DB snapshots (default
  10000); the raft log is compacted behind each snapshot.

Full field-by-field reference (including `clusterSecret`, `dataDir`,
`forceNewCluster`): `docs/multi-raft.md`.

### Bootstrap and grow

```bash
# 1. First manager (bootstrap: true in its config)
legion start manager -c configs/manager-raft.jsonnet

# 2. Mint a join token on it
legion raft join-token
# token=lgnraft_...  expires_at=...

# 3. Start each additional manager with bootstrap: false and
#    joinToken: 'lgnraft_...' in its raft config.
```

A joining manager enrolls itself in the background: it dials the leader,
is added as a **learner on every raft group**, is caught up (log replay,
or snapshot install when the leader has compacted past what replay could
serve), and is promoted to **voter**. Progress appears in both managers' logs and in
`legion raft history`. The token embeds the cluster secret — treat it as a
secret, and mint fresh ones as needed (they expire after
`joinTokenTtlSeconds`).

Restarted members do not need a token: they recover from their raft log
and rejoin automatically.

### Upgrading an existing single-manager cluster

Flip `persistence.type` to `raft` (with `bootstrap: true`) on the existing
manager: the first raft start **adopts the existing `store.sqlite` as the
initial replicated state** and builds a seed snapshot of it, so even late
joiners receive the full baseline via snapshot install. See
`docs/upgrades.md` ("Raft persistence upgrades") for the procedure.

## CLI operations

### Status

```bash
legion raft status
```

Reports real engine state: role, current leader, term, last log/applied
indexes, voters/learners, and — under `topology: 'lanes'` — one line per
raft group. Every lane also gets a quorum line:

```
voters 3 (quorum 2) — survives 1 failure(s)
```

with a `⚠` + a plain-language advisory appended when the voter count is
even ("2 voters need both alive to commit — this cluster survives zero
failures. Use 1, 3, or 5 voters; add one more manager or remove one.") or
below `persistence.raft.expectedVoters`. The same fields ride the
`GetRaftStatus` reply (`lanes[].voters/quorum/survives/warning`). See
docs/multi-raft.md, "How many managers?".

### History

```bash
legion raft history --limit 100
```

Real transitions recorded from the engine's metrics stream (elections,
role changes, membership changes, join-token issuance) plus operator
commands.

### Commands

```bash
legion raft command trigger-election
legion raft command add-learner --target mgr-d@10.0.0.4:4747
legion raft command add-voter   --target mgr-d
legion raft command remove-member --target mgr-d
```

These drive the real openraft membership APIs. Targets are
`name@host:port`, or a bare `name` resolvable from `raft.peers`.

Membership changes compute the **resulting** voter count (plan JC-29:
warn loudly, block rarely):

- An **even result** (`add-voter` onto an odd cluster, `remove-member`
  from an odd cluster, a join promotion through 2 voters) **proceeds**,
  but the advisory is WARN-logged on the manager, recorded as a
  `membership` event in `legion raft history`, and printed on the
  command's own output (`⚠ …`, threaded through
  `IssueRaftCommandReply.warning`). Join promotion is never withheld to
  preserve parity.
- The **only hard block**: a `remove-member` that would leave **zero
  voters**, or a resulting quorum that the currently-known-alive members
  could not form (best-effort liveness from the leader's replication
  streams) — such a change could itself never commit and would wedge the
  cluster. The refusal message names the fix (add a voter first / remove
  the dead member instead).

`step-down` / `transfer-leader` are **not supported** by the current
engine (openraft 0.9 has no leader-transfer API); they return an error
saying so. To move leadership, run `trigger-election` while pointed at the
desired new leader.

### Join token

```bash
legion raft join-token
```

Issued by any running member; the embedded leader address always points at
the current leader.

## UI operations

Open the Legion UI and navigate to **Cluster → Raft** for the status card,
operator command buttons, join-token issuance, and event history — all
backed by the same real engine state as the CLI.

## Operational notes

- **Quorum:** run an odd number of managers (1, 3, or 5); a majority must
  be up for writes. With a quorum lost, reads still serve from local
  replicas. Set `persistence.raft.expectedVoters` and every surface
  (config load, `raft status`, membership commands, `/metrics`, `/readyz`)
  warns when the cluster drifts even or short — see docs/multi-raft.md,
  "How many managers?".
- **Restart deadlocks avoided:** a restarted member may briefly log that no
  leader is known and proceed leaderless; it catches up as soon as its
  listener is up and peers reconnect.
- **Observability:** raft state is served by `legion raft status` /
  `history`, the UI Raft page, and `/readyz` (which requires a known
  leader in raft mode, and carries the voter-parity advisory as a `note:`
  line — without breaking readiness — while the voter count is even).
  `/metrics` emits `legion_raft_voters{lane}` and
  `legion_raft_quorum{lane}`; the rest of the `legion_raft_*` prefix
  remains **reserved, not yet emitted** (see `docs/metrics.md`).
- **Snapshots/compaction:** automatic — a whole-DB snapshot every
  `raft.snapshotEntries` applied entries, log purged behind it. Laggard
  followers and fresh joiners catch up by snapshot install; no manual
  compaction exists or is needed.
- **Backup/restore:** `legion state backup` (leader-only in raft mode) and
  `legion start manager --restore-from <backup>` — see `docs/upgrades.md`.
- **Data layout:** the replicated state DB lives at
  `<storePath>/store.sqlite`; the raft log, snapshots, and cluster secret
  live under `raft.dataDir` (per lane). Both must survive restarts
  together (and see `forceNewCluster` in `docs/multi-raft.md` for what
  happens when they part ways).

## Not yet done

- Auto-forwarding writes from followers to the leader (today: reject +
  hint).
- `topology: 'lanes'` is refused at engine start until the node/liveness
  table split makes per-lane table ownership disjoint.
- `step-down` / `transfer-leader` (no leader-transfer API in openraft 0.9).
- Raft `/metrics` series; transport mTLS (today: shared cluster secret).

Full limitations list: `docs/multi-raft.md` ("Limitations and follow-ups").
Track progress in `internal_docs/plans/persistence-plan.md`.

[openraft]: https://github.com/databendlabs/openraft
