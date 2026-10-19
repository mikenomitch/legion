# Multi-Raft Persistence (Operator Guide)

`persistence.type = 'raft'` turns a set of Legion managers into a
quorum-replicated cluster. A majority of managers must commit every store
change before it is acknowledged or visible anywhere. Losing any minority of
managers loses no committed write.

This page is the engine-level reference. The task-oriented runbook is
[docs/raft-operations.md](raft-operations.md). Upgrade, backup, and restore
procedures are in [docs/upgrades.md](upgrades.md).

## What raft mode gives you

A write runs on the leader inside a savepoint that is then rolled back.
While it runs, a SQLite session records its row-level effects. This record is
a **changeset**: the inserts, updates, and deletes with their values and
rowids. The leader proposes those bytes as one [openraft] log entry. Only
after a quorum commits it does every manager, the leader included, apply the
same bytes to its local `store.sqlite`.

Replicas cannot diverge, because they apply the recorded effects instead of
re-running code. Nothing becomes visible, cached, or published as an event
before quorum commit, not even on the leader. The caller gets its reply only
after the local apply, so read-your-writes holds on the leader.

```text
client ──write RPC──▶ leader manager
                        │ 1. capture: run the SQL in a savepoint + session,
                        │    extract the changeset, ROLLBACK (DB untouched)
                        │ 2. propose changeset to the raft group
                        ├────── append ──────▶ follower B ──ack──┐
                        ├────── append ──────▶ follower C ──ack──┤
                        │ 3. quorum commit                 ◀─────┘
                        │ 4. every node applies the changeset + applied
                        │    index in ONE transaction, then projects
                        │    read-model cache / events / notifies from it
                        ◀─ 5. reply to client (after local apply)
```

Raft mode also gives you:

- Leader election and automatic failover.
- Token-based membership (join, learner, voter promotion).
- Operator commands with audit history.
- Snapshots and log compaction.
- Follower catch-up via log replay or snapshot install.
- Leader-only scheduling, with a follower taking over on failover.
- Rejection of writes sent to a non-leader, with a hint naming the leader.

## Configuration reference

All fields live under `persistence.raft` (camelCase in config files).
Defaults come from `RaftPersistenceConfig::with_defaults` in
`src/structs/config.rs`:

| Field | Default | Meaning |
| --- | --- | --- |
| `nodeId` | manager `host:port` id | Stable string identity of this manager in the cluster. The numeric raft member id is derived from it (`fnv1a64(nodeId)`), so it must be unique per manager and **permanent once joined**. |
| `bootstrap` | `false` | Set `true` only on the **first** manager of a new cluster. It initializes the raft group with itself as the sole voter. Safe across restarts (re-initialization is a no-op). |
| `peers` | `[]` | Peer managers as `"name=host:port"` entries. The address is the peer's manager gRPC listener, which also serves the raft transport. Used for information and for name resolution in operator commands. Actual membership comes from joins and membership commands. Bare `"name"` entries still parse. |
| `joinTokenTtlSeconds` | `900` | Lifetime of tokens minted by `legion raft join-token`. |
| `dataDir` | `<data dir>/raft` (`$LEGION_DATA_DIR` or `.legion`) | Root for raft engine state: per-lane log DBs (`<dataDir>/<lane>/log.sqlite`), snapshots (`<dataDir>/<lane>/snapshots/`), the cluster secret file (`<dataDir>/cluster_secret`), and the double-start advisory lock (`lock.sqlite`). |
| `topology` | `"single"` | `"single"`: one raft group replicates every table (recommended default). `"lanes"`: three independent control/scheduler/status groups, for clusters whose write load is heavy enough that one group's log becomes a bottleneck (see "Topology: `single` vs `lanes`" below). Fixed at first engine start per data dir. Switching requires backup → fresh cluster → restore. |
| `advertiseAddr` | manager `host:port` | `host:port` other managers dial for this manager's raft transport. Wildcard binds (`0.0.0.0`/`::`) are normalized to `127.0.0.1`, so set this explicitly for any multi-node deployment. |
| `clusterSecret` | minted at bootstrap | Shared secret that authenticates manager-plane raft RPCs. When absent, it is minted at bootstrap and saved (mode 0600) to `<dataDir>/cluster_secret`. Joiners receive it inside the join token. |
| `joinToken` | none | Token from `legion raft join-token` that a new manager uses to join an existing cluster. |
| `expectedVoters` | unset | The number of voting managers this cluster should have (1, 3, or 5). Only used for warnings: `legion raft status` warns when the observed voter count is below it, config load warns when it is even, and `0` is rejected at load. Membership itself always comes from joins and operator commands, never from this setting. See "How many managers?" below. |
| `snapshotEntries` | `10000` | Applied log entries between snapshots. Each snapshot is a whole-DB `VACUUM INTO` image. The log is purged behind it, keeping a `snapshotEntries / 2` tail (capped at 1000) for cheap follower catch-up. |
| `heartbeatIntervalMs` | `250` | How often a leader sends heartbeats (append-entries) to every follower. This sets the floor on how fast a dead leader can be noticed. openraft also uses it as the append-entries RPC **deadline**, so it bounds how long one replication batch may take, and pairs with the entries-per-batch cap. Lower values mean faster failure detection, more constant traffic, and less tolerance for a slow round trip. See "When to raise the election timings" below. |
| `electionTimeoutMinMs` | `600` | Lower bound of the randomized election timeout. A follower that has heard nothing for a duration drawn from `[min, max)` campaigns to replace the leader. This setting controls stability. It must sit well above `heartbeatIntervalMs` so a couple of slow or dropped heartbeats never look like a dead leader. If it is too close to the heartbeat interval, the cluster flaps: followers time out under transient load, campaign, split the vote, and the term climbs while no leader holds power long enough to commit. Config load **rejects** `electionTimeoutMinMs < 2 × heartbeatIntervalMs`. |
| `electionTimeoutMaxMs` | `1200` | Upper bound of the randomized election timeout. Must be strictly greater than `electionTimeoutMinMs` (config load rejects otherwise). The gap between min and max is what breaks up split votes, so keep it wide. This value is **also the follower leader-lease**: a follower will not grant its vote while it still believes the current leader's lease is live. A larger maximum adds stability under load, but slows every intentional leadership move (lane convergence, `trigger_lane_election`) by the same proportion. Prefer widening the window over raising both ends. The default keeps `max = 2 × min`. |
| `forceNewCluster` | `false` | Explicit override to re-stamp a state DB onto a NEW cluster identity. Without it, a manager whose derived cluster id does not match the id stamped into its state DB **refuses to start**. This protects against a wiped `dataDir` + `bootstrap = true` silently creating a second cluster over the old cluster's data (split-brain). |

The raft transport is a gRPC service (`RaftTransport`) that shares the
manager's existing listener. There is no separate raft port.

### When to raise the election timings

The defaults (250 ms heartbeat, 600–1200 ms election window) assume a
manager that gets scheduled promptly and reaches its peers over a LAN. They
are in the range of Consul's and etcd's defaults. A dead leader is replaced
in roughly one election timeout plus a round trip, about 1 s.

Raise all three together, keeping the ratios roughly the same, when the
environment can stall a heartbeat for longer than the window allows:

- **Loaded managers.** A manager competing for CPU with the rest of the box
  can be descheduled for hundreds of milliseconds. Its followers read that
  as a dead leader.
- **WAN or cross-region links.** The heartbeat interval is also the
  append-entries deadline, so it has to cover a full round trip plus the
  batch, not just the one-way latency.
- **Virtualized or otherwise noisy hosts.** Steal time and ballooning cause
  the multi-hundred-millisecond pauses that trigger spurious elections.
- **CI.** In-process multi-node harnesses run several raft cores on a
  couple of CPU cores in a debug build. The test suites under `tests/` use
  relaxed timings for this reason.

You are trying to avoid **flapping**. Followers time out against a leader
that is only slow, campaign, and the term climbs while no leader holds power
long enough to commit. The cost of raising the timings is slower failover in
proportion. Because `electionTimeoutMaxMs` is also the follower leader-lease,
intentional leadership moves slow down too. Widen the window instead of
raising both ends: raise `electionTimeoutMinMs` for stability, and keep
`electionTimeoutMaxMs` a small multiple of it.

Config load rejects combinations that are guaranteed to flap, and names the
offending values. `electionTimeoutMinMs` must be at least
`2 × heartbeatIntervalMs`, and `electionTimeoutMinMs < electionTimeoutMaxMs`.

## How many managers?

Run **1, 3, or 5** voting managers. A raft group commits when a majority
(quorum = `voters / 2 + 1`) of its voters is alive:

| Voters | Quorum | Survives | Verdict |
| --- | --- | --- | --- |
| 1 | 1 | 0 failures | fine for dev / single-node durability |
| 2 | 2 | 0 failures | ⚠ both must be alive to commit, so worse than 1 in practice |
| 3 | 2 | 1 failure | the recommended production minimum |
| 4 | 3 | 1 failure | ⚠ no more fault tolerance than 3, more machines to keep in quorum |
| 5 | 3 | 2 failures | for clusters that must survive two concurrent failures |

An **even** voter count adds a manager without adding fault tolerance. Two
voters are worse than one, because either machine failing stops all writes.
Legion warns about this, but never blocks it. The warning appears in these
places:

- **Config load.** An even `expectedVoters`, or a `peers` list describing an
  even cluster, logs the warning once at startup. `expectedVoters = 0` fails
  to load.
- **`legion raft status`.** Every lane prints
  `voters N (quorum Q) — survives F failure(s)`, with a `⚠` and the warning
  when N is even or below `expectedVoters`. The same numbers are in the
  `GetRaftStatus` reply (`lanes[].voters/quorum/survives/warning`).
- **Membership changes.** `add-voter`, `remove-member`, and join promotion
  compute the resulting voter count. An even result goes ahead, but is
  logged at WARN, recorded as a `membership` event in `legion raft history`,
  and shown in the command's own output. Join promotion is never held back
  to keep the count odd, because a visible warning is better than a hidden
  availability problem.
- **Metrics and probes.** The `legion_raft_*` series (docs/metrics.md)
  cover quorum shape (`voters`/`quorum`), per-lane progress (`term`,
  `commit_index`, `applied_index`, `apply_lag`, `is_leader`), and histograms
  that show where time goes (`capture_seconds` vs `propose_apply_seconds`,
  plus `ops_total`). `readyz` stays `200` but includes the warning as a
  `note:` line while the voter count is even.

The only hard block is a `remove-member` that would leave zero voters, or a
quorum the members known to be alive cannot form. That membership change
could never commit, and the cluster would get stuck. The refusal message
names the fix.

Set `expectedVoters` to the size you intend (say `3`). Then alerts on the
status warning or gauges tell you when a manager is down, still a learner,
or never joined.

## Bootstrap and join: a 3-node walkthrough

See `configs/manager-raft.ncl` for a complete commented example.

1. **Bootstrap the first manager** (`mgr-a`):

   ```nickel
   persistence = {
     type = "raft",
     raft = {
       nodeId = "mgr-a",
       bootstrap = true,
       advertiseAddr = "10.0.0.1:4747",
       peers = ["mgr-a=10.0.0.1:4747", "mgr-b=10.0.0.2:4747", "mgr-c=10.0.0.3:4747"],
     },
   },
   ```

   ```bash
   legion start manager -c mgr-a.ncl
   ```

   It elects itself leader of a single-voter cluster. If a pre-existing
   `store.sqlite` has data (an upgrade from `local_disk` or from the old
   simulated raft), that database **becomes the initial replicated state**.
   Bootstrap builds a seed snapshot of it, so later joiners receive the full
   baseline through snapshot install.

2. **Mint a join token on any running member:**

   ```bash
   legion raft join-token
   # token=lgnraft_...   expires_at=...
   ```

   The token is base64url JSON that carries the cluster id, the cluster
   secret, the current **leader's** advertise address, and an expiry.
   Treat it as a secret, because the embedded cluster secret is the
   credential.

3. **Start `mgr-b` and `mgr-c`** with `bootstrap = false` and
   `joinToken = "lgnraft_..."` in their raft config. Each joiner enrolls in
   the background. It dials the leader's `Join` RPC (retrying until the
   leader is reachable), is added as a **learner**, catches up its log (by
   replay or snapshot install), and is promoted to **voter**. Progress
   appears in both sides' logs and in `legion raft history`.

Restarted members never need a token. They recover from their raft log and
rejoin automatically. Permanent join rejections (expired token, wrong
cluster, malformed token) stop the enrollment loop with an error. Mint a
fresh token and restart the joiner.

Two safety checks run at engine start:

- **Double-start lock:** `<dataDir>/lock.sqlite` holds an exclusive OS file
  lock for the life of the process. A second manager process pointed at the
  same `dataDir` fails immediately instead of running a competing raft core
  over the same log.
- **Cluster stamp:** the state DB is stamped with its cluster id at first
  engine start, and every later start must match (see `forceNewCluster`
  above).

## Operator commands

```bash
legion raft status         # role, leader, term, commit/applied, voters/learners
legion raft history -n 100 # real transitions from the engine metrics stream
legion raft command <cmd> [--target <node>]
legion raft join-token
```

Supported commands (each calls the openraft API and is recorded in the audit
history with the actor who ran it):

- `trigger-election`: force an election on every lane.
- `add-learner --target name@host:port`: add a non-voting member.
- `add-voter --target name`: promote to voter (address resolved from
  `raft.peers` or given as `name@host:port`).
- `remove-member --target name`: demote and remove a member.
- `transfer-leader --target name`: make that voter the leader, from any
  manager. Built on a remote election (openraft 0.9 has no leader-transfer
  API): the target campaigns, re-asked once per election timeout until it
  leads or the deadline passes, which the reply reports as a warning.
- `step-down`: hand leadership to the most caught-up other voter. Runs on
  the leader; a follower forwards it there.

`legion raft status` prints per-lane lines (`lane=... role=... term=...`)
when more than one raft group is active, which means under the experimental
`lanes` topology. In `single` topology, the headline lines describe the one
group. The UI's **Cluster → Raft** page shows the headline status, the
history, and the operator commands, but not per-lane lines. The full data is
available through the `GetRaftStatus` / `ListRaftHistory` /
`IssueRaftCommand` / `GetRaftJoinToken` RPCs.

## Topology: `single` vs `lanes`

The engine supports multiple raft groups. Every store method that writes is
assigned to a lane:

- `control`: job, config, KV, node registration, ACL, and template writes.
- `scheduler`: placement, eval, rollout, instance, volume, snapshot, and
  boot-ticket writes. An instance is created, bound to its allocation,
  suspended, forked, and destroyed on this lane, which is why a keyed
  sandbox's whole lifecycle can ride a handful of scheduler-lane entries
  (and why an edge-boot ratification spends its ticket in the same entry
  that places the instance).
- `status`: heartbeat, liveness, allocation health, and task status writes.

A test checks that every method is assigned, and the transport routes groups
by `group_id`.

- **`single` (default, recommended):** one raft group, keyed `control`,
  carries every lane. This gives the durability guarantee with whole-DB
  snapshots, which are simple to reason about.
- **`lanes` (experimental):** three independent raft groups, each with its
  own log, apply loop, and replication pipeline. A burst of control writes
  cannot stall the scheduler's log. Correctness depends on
  **single-writer table ownership**. Every replicated table belongs to
  exactly one lane, and this is enforced at capture time in every topology:
  a write whose rows fall outside its lane's tables is rejected before
  proposal. A small set of insert-only tables with globally unique keys is
  the exception. Snapshots are per lane and cover only that lane's tables
  (below). Legion tries to keep lane leadership on one manager (see
  below).

### Lane leadership: co-located, best effort

Under `lanes`, Legion tries to keep **one manager leading all three lanes**.
This matters for operations that write to more than one lane. Registration
writes control identity and status liveness. Job submit writes control
tables, a status seed, and a scheduler-lane volume-claim sync. Each part is
proposed from the node the client is talking to. If different managers lead
different lanes, the part for a lane that node does not lead fails.

The **control lane is the anchor**. Whichever node leads it campaigns for the
lanes it does not lead. openraft 0.9 has no leader-transfer API, so a leader
can't be handed a lane. It can only win one in an election.

**So convergence is best effort, and not guaranteed.** A campaign is a race,
and raft's election safety can refuse it indefinitely. A node whose log for
that lane is behind cannot win, and should not. Legion guarantees the
following instead:

- **A lane always elects a leader.** Convergence never campaigns for a lane
  that has no leader. A leaderless lane is already holding an election on
  every voter's own randomized timer, under raft's normal rules (leader
  lease, and an extra delay for a node that has seen a longer log
  elsewhere). An outside campaign would bypass those rules and restart this
  node's candidacy at a higher term. openraft 0.9 has no pre-vote, so it
  would also push every other voter's term up. Two campaigners on a
  three-voter group with one member down never reach a quorum, so the term
  climbs and nobody wins. A working write path always matters more than
  co-location, so convergence stays out of the way.
- **Disruption is bounded.** Campaigns for a lane led elsewhere are per lane
  and spaced exponentially (2 → 16 election windows). After three
  consecutive failures they stop for a cooldown of 60 election windows. The
  cooldown doubles, up to 480, for a lane this node keeps failing to take.
  Any win resets it. A lane this node cannot take costs the cluster a few
  raft terms per cooldown instead of an unbounded climb, and raft's own
  elections get long uninterrupted stretches. Each stand-down is logged at
  `warn` with the lane and the cooldown.
- **A campaign is skipped when it cannot win.** Before campaigning, the node
  checks that the lane's log is not still streaming into it. A log that grew
  since the previous check means this node is behind whoever it replicates
  from. That is the only signal of log freshness openraft gives a
  non-leader. The real test is the election itself, which is what the
  backoff above tracks.

**What split leadership costs you.** It is safe, and the cluster degrades
instead of failing, but there are costs:

- **Worker liveness keeps working, through digests.** Workers dial one fixed
  manager. When that manager does not lead the status lane, it batches the
  node ids it has heard from. It sends the owning leader one
  `ReportNodeLiveness` RPC per `liveness.digestInterval` (1s) over the
  authenticated manager plane. The cost is `managers x 1 RPC/interval`,
  independent of fleet size, and no heartbeat becomes a replicated write.
  If those digests stop arriving, the leader does not mark that peer's nodes
  lost within `liveness.peerDigestGrace`, and raises
  `legion_liveness_peer_stale{peer}`. A problem between managers should not
  look like fleet-wide node loss. See [node-liveness.md](node-liveness.md).

  Health-check, allocation-health, and failure-report writes are still
  forwarded to the owning lane's leader (`ForwardLaneWrite` on the raft
  transport). The worker's RPC succeeds only after the forwarded write
  commits. Heartbeats fall back to the same path during a manager upgrade,
  while a peer that predates `ReportNodeLiveness` leads the status lane. The
  forwarding path also lets the status leader sync a lost node's
  availability to the control lane, so a lost node stops receiving
  placements. It also covers plain `single`-topology failover: workers
  pointed at a demoted ex-leader keep reporting through it.
- **The control leader accepts job submits**, and they complete
  asynchronously. The control part (the operator's intent) must commit on
  the node you submitted to. A manager that does not lead the control lane
  rejects the submit with the typed leader hint, and the API layer replays
  it to the control leader (see "Non-leader write redirects and
  forwarding"), so the caller sees the leader's answer. The owning lanes'
  leaders then complete the cross-lane parts: the `task_status` seed, the
  volume-claim rows, the rollout for an updated job, and the scheduling
  eval. Evals forward immediately. Per-lane reconcilers derive the rest from
  replicated state on the node-status-checker cadence (default 2s). Until
  they run, a just-submitted job can briefly show tasks without statuses,
  and an updated job's rollout can appear a tick late. Canary and manual
  gating still apply. The reconciler creates the same rollout the submit
  would have, and the eval-regeneration fallback that would bypass gating
  only runs after it.
- **Scheduling is slower.** Placement runs on the scheduler lane's leader.
  Work that would have been enqueued inline arrives by forwarding or on the
  reconcile cadence instead.
- **`legion state backup` is unavailable** until one node leads every lane
  again, because it needs a single authoritative image.
- Single-lane operations are unaffected. Each lane keeps serving under its
  own leader, and every replica serves reads.
- `/readyz` tolerates about 10s of continuous split leadership, then returns
  `503` with a per-lane body. Each manager measures this with its own probe,
  so a rolling restart gets the full grace period. A standing split needs an
  operator's attention, even though the write path keeps working.

If a cluster stays split, treat it like any leadership problem. Check
`legion raft status` per lane. If one lane's leader is unhealthy, restart it
so its lane re-elects. On loaded or WAN clusters, raising
`electionTimeoutMinMs` / `electionTimeoutMaxMs` also stretches the whole
convergence schedule, which is derived from `electionTimeoutMaxMs`.

### Table ownership is a cluster-wide agreement

Which lane owns a table is part of the on-disk and wire format. A lane's
snapshot is a dump of the tables that lane owns. The receiver checks the
dump's manifest against **its own** ownership map before replacing anything,
and refuses to overwrite a sibling lane's table. So **moving a table from one
lane to another cannot be done as a rolling upgrade.** A mixed-version
cluster cannot agree on what a lane's snapshot contains. The old-version node
rejects the new-version node's snapshot (and the reverse) with an error
naming the table.

Moving a table between lanes takes the same path as any other on-disk format
change. Back up (`legion state backup`), start a **fresh** cluster on the new
version, restore it with `legion start manager --restore-from <backup>`, then
rejoin the members. Adding a new table to a lane is fine. Only moving an
existing table between lanes breaks compatibility.

## Snapshots, compaction, and catch-up

In `single` topology:

- Every `snapshotEntries` (default 10 000) applied entries, the engine
  builds a snapshot. It is a `VACUUM INTO` image of the whole state DB,
  read on its own connection, so writes keep committing while it is built.
  The image is one read of committed state, so it is transactionally
  consistent **and contains its own applied-index marker** (the
  `raft_applied` table is inside the image). Metadata goes in a
  `.meta.json` sidecar. The two newest snapshots are kept.
- **Snapshots are held in memory.** The engine's snapshot API passes the
  image as one in-memory buffer, so building, serving, and installing a
  snapshot each hold a full copy of the image in the manager's memory (and
  a catching-up follower receives it in chunks into memory). Budget
  manager memory for at least two copies of `store.sqlite`. Install also
  blocks writes on the installing follower while it replaces its state DB.
- The raft log is purged behind each snapshot, keeping a tail
  (`snapshotEntries / 2`, capped at 1000). A slightly-behind follower
  catches up by cheap log replay. Anything older is served by snapshot
  install.
- **Install** replaces the follower's whole state DB. It is staged to a temp
  file, the applied marker is written in, and the file is renamed
  atomically. Every connection and the read model are reopened, and then a
  consistency check runs. A crash at any point leaves either the old or the
  new DB, never a partial one.
- **Bootstrap seed snapshot:** on a fresh bootstrap, the engine immediately
  snapshots the (possibly pre-raft) database and compacts the log to it.
  Any existing data then reaches joiners through snapshot install, instead
  of being silently missing from log replay.
- Crash and restart recovery: openraft replays the vote and log from the
  per-lane log store (SQLite, WAL + `synchronous=FULL`). The state machine
  resumes from the applied index, which is recorded **in the same
  transaction** as each applied changeset, so state and index cannot
  diverge.

In `lanes` topology, the policy, compaction, retention, and recovery are the
same **per lane**, but the snapshot's scope differs. A whole-DB image would
overwrite the sibling lanes' tables on install. So a lane's snapshot is a
logical dump of only the tables that lane owns, read in one read
transaction alongside the lane's applied marker. The control lane also
carries the shared insert-only tables. Install is one transaction that
replaces those tables' rows and writes the lane's applied marker. Tables
owned by other lanes are never touched, and the dump's manifest is checked
against this node's ownership map first. See "Table ownership is a
cluster-wide agreement" above for why moving a table between lanes requires
a fresh cluster.

## Backup and restore

- `legion state backup [--out <path>]` (RPC `Manager/GetStateBackup`, ACL
  `operator:write`) writes a consistent `VACUUM INTO` copy **on the
  manager host** (default `<storePath>/backups/legion-state-backup-<ts>.sqlite`).
  It reports the path, size in bytes, and the raft applied index it
  captures. It works in every persistence mode. In raft mode it runs only on
  the node that **leads every lane**. Any other node answers
  `FAILED_PRECONDITION` and names that lane's leader. During a standing lane
  split (convergence is best effort, see above), no node qualifies, so back
  up after leadership has converged.
- **What the backup contains.** A point-in-time image of that node's
  applied state: the whole database file, with all lanes' tables, as of one
  moment under an exclusive hold that blocks captures and applies. It is
  **not** a cluster-wide snapshot at a single raft index. Under `lanes` the
  three groups apply independently, so the image is at a different applied
  index per lane, and the reported `applied index` is the highest of them.
  That is fine for restore, which seeds a fresh cluster. Just don't read the
  number as "the cluster was at index N".
- `legion start manager --restore-from <backup> [--force-restore]` copies
  the backup into place as the state DB before the store and engine start.
  It clears raft bookkeeping, so the restored state becomes the **starting
  state of a fresh cluster**. Restore onto a fresh bootstrap node, then join
  the others. Never restore into a member of an existing cluster.

Full procedures (including rollback out of raft mode): `docs/upgrades.md`.

## Failure modes

An in-process 3-node test covers each of these, in
`tests/integration/raft_durability.rs` / `tests/integration/raft_snapshot_recovery.rs`:

| Scenario | Behavior |
| --- | --- |
| **Leader loss** | In-flight unacked writes fail with a typed error (retry them). Every acked write survives. A new leader is elected within the election timeout (600–1200 ms by default, see `electionTimeoutMinMs`/`electionTimeoutMaxMs`). Its eval workers and node status checker take over the replicated queues within about 1 s. Clients pointed at the dead leader get a connection error and retry against another manager; clients pointed at a live manager are forwarded to the new leader. Workers fail over to another manager after two unreachable heartbeats. |
| **Minority partition** | The majority side keeps electing and committing. Managers on the minority side cannot reach a quorum, so writes sent to them fail with `FAILED_PRECONDITION` (forwarding to a leader they cannot reach fails too), and they keep serving local reads. When the partition heals, the minority catches up and ends up byte-identical. |
| **Follower loss** | No client impact. The cluster commits on the remaining quorum. When the follower returns, it catches up by log replay, or by snapshot install if the leader has compacted past its position. |
| **Full-cluster restart** | Every node recovers from its raft log and state DB. A leader is re-elected, and the cluster resumes with all committed state. A restarted member that cannot yet hear the leader (its inbound transport shares the manager listener) starts without a leader, logs a warning, and catches up once it is serving. Startup never deadlocks on the election. |
| **Operator double-start** | The second process fails fast on the `dataDir` advisory lock. |
| **Wiped `dataDir`, kept state DB** | The cluster stamp blocks startup unless `forceNewCluster = true` (an intentional re-stamp). |
| **Standing lane split** (`lanes` topology, different managers keep leading different lanes) | Nothing is lost. Worker liveness and reports forward to the owning lane's leader. The control leader accepts submits, and lane reconcilers complete them within the checker cadence. Single-lane operations and reads are unaffected. Costs: slower scheduling, briefly lagging task statuses, claims, and rollouts, no `state backup`, and `/readyz` returns 503 after about 10s to alert the operator. Covered by `tests/integration/raft_lanes.rs` (standing-split and forwarding tests). |

## Non-leader write redirects and forwarding

**Client-plane** RPCs that write and are sent to a non-leader are rejected
internally with a typed not-the-leader error (`FAILED_PRECONDITION` naming
the leader and its raft advertise address) **before** any capture work runs,
and the receiving manager then replays the whole request to that leader and
returns the leader's reply (the leader-forward layer; see
[Writes on any manager](raft-operations.md#writes-on-any-manager)). Clients
can point `LEGION_ADDR` at any manager. Every manager serves reads from its
local replica. The same typed rejection, and the same forwarding, covers
leader-only operations that don't propose anything (`legion state backup`,
`raft command step-down`). The caller sees the rejection only when the
leader is unreachable from the manager it asked.

**Worker-plane** writes are forwarded per operation, below the RPC layer. A
manager may receive a heartbeat, health check, allocation-health
snapshot, failure report, terminal-exit report, or volume report that it
cannot propose, because
it does not lead the owning lane. This happens after a failover in `single`
topology, or during a lane split in `lanes`. The manager sends the operation
to the lane's leader over the authenticated raft transport
(`ForwardLaneWrite`, cluster-secret auth, 5s deadline). It answers the
worker only after the forwarded write commits.

Forwarded operations carry their full arguments. The receiver runs the
non-forwarding variant, so a stale hint costs one typed rejection and never
causes a forwarding loop. A forward that cannot complete returns the
original typed rejection. During a rolling upgrade from a release without
this RPC, a forward to an old manager fails cleanly, and the worker falls
back to the older reject-and-retry behavior.

A report whose writes span lanes goes out as one write per lane, in order.
A batch task's exit marks its allocation complete on the scheduler lane,
which also decides the task's status from every replica's row. The status
lane then records that status, and the dependency eval follows on the
scheduler lane. If a later write fails, the manager asks the worker to send
the report again (`HeartbeatReply.retry_terminated_allocations`). The
earlier writes are idempotent, so the repeat converges. In the `lanes`
topology the eval can run before the scheduler leader has applied the
status-lane write. It then keeps holding the dependents, and they wait for
the job's next evaluation (a resubmit, a node change, any other eval). The
`single` topology, where every lane rides one group, doesn't have this
race.

Scheduling is leader-only in the same way. Eval queue state replicates
everywhere, so a failover resumes the queue. Only the leader's loops act.
Follower loops stay idle until a leadership change flips their leadership
check.

## Performance

This is the raft-mode write-path baseline, measured by
`tests/integration/raft_perf_smoke.rs`
(`cargo test --test integration raft_perf_smoke:: --locked -- --ignored --nocapture`). It
runs a single-node bootstrap cluster (`topology = single`, self-quorum) and
sends `put_kv` through the full propose → capture → quorum-commit →
state-machine-apply pipeline. Debug build, 2026-07-23:

| Workload | Ops | p50 | p99 | Throughput |
| --- | --- | --- | --- | --- |
| Sequential `put_kv` round-trips | 1000 | 3.34 ms | 30.73 ms | 224.6 ops/s |
| Concurrent burst (8 threads × 100 ops) | 800 | 23.56 ms | 39.20 ms | 332.6 ops/s |

The smoke test asserts p99 < 250 ms. That bound is loose on purpose. It
catches order-of-magnitude regressions in the replication pipeline
(capture-mutex contention, per-apply read-model projection, log fsync
pileups), and is not a tuned benchmark target. Latencies include everything
the caller experiences: the capture savepoint and changeset extraction, the
sync→async bridge to `client_write`, local log append and fsync, and
changeset apply with read-model and event projection.

For comparison, the `local_disk` scheduler SLO gate
(`cargo test --test integration scheduler_benchmark_slo:: --locked -- --ignored --nocapture`)
at the same commit measured: steady-small 238.7 evals/s / p99 5.34 ms,
churn-medium 174.0 evals/s / p99 7.27 ms, high-fanout 135.0 evals/s /
p99 10.84 ms. All are within SLO. Raft mode does not change `local_disk`
behavior.

### Lane isolation A/B (lanes vs single, 2026-07-28)

Lanes exist so that saturating one lane does not slow down another. The
`lanes_vs_single_scheduler_isolation_ab` test in `tests/integration/raft_perf_smoke.rs`
measures this. It runs a 3-node in-process cluster (tonic `RaftTransport`
listeners over loopback) and samples scheduler-lane `place_alloc`
propose→apply latency twice per topology. The first phase is unloaded. In
the second, 2 threads saturate the control lane with job-submit bursts and 2
threads flood the status lane with heartbeats. Each phase measures 200
placements, in a debug build, excluding samples retried after a leadership
blip. These are three consecutive runs on an otherwise idle Apple Silicon
machine, 2026-07-28. The table shows the middle run:

| Topology | Phase | p50 | p99 | Placements/s | Cross-lane load committed (load window) |
| --- | --- | --- | --- | --- | --- |
| `single` | baseline | 1.17 ms | 1.38 ms | 855 | (none) |
| `single` | cross-lane load | 2.69 ms | 110.45 ms | 129 | 335 submits + 990 heartbeats (1.6 s window) |
| `lanes` | baseline | 1.13 ms | 1.41 ms | 875 | (none) |
| `lanes` | cross-lane load | 1.65 ms | 3.37 ms | 548 | 774 submits + 857 heartbeats (0.4 s window) |

Across the three runs, scheduler p99 under cross-lane load grew
**50–93× under `single` and 1.6–2.4× under `lanes`**. The load that pushes
the single-group scheduler tail to about 100–130 ms leaves the lanes
scheduler tail nearly flat (2.4–3.4 ms). Lanes also keeps 4–5× more
placement throughput under load (543–606/s vs 104–131/s).

Under `single`, every operation goes through one raft group's pipeline, so
placements queue behind the whole flood. Under `lanes`, the scheduler group
commits independently. The only shared stage left is the per-node capture
mutex. Captures are local SQLite work that takes under a millisecond, while
quorum commit is the main cost and runs in parallel. The loaded lane
benefits too: control-lane submit throughput was about 9–10× higher under
`lanes` (about 1900/s vs 200/s), because submits no longer queue behind
heartbeats and placements.

Caveats: this is a debug build on an in-process loopback cluster, so compare
the ratios, not the absolute numbers. Quorum commits over a real WAN behave
very differently. Unloaded baselines match between topologies within noise,
so lanes cost nothing when idle. The flat scheduler tail under cross-lane
load showed up in every run on an idle machine. Earlier runs on shared-CPU
CI-class machines showed the same split (5.4× vs 1.1×) with much noisier
absolute numbers. The benchmark needs a quiet machine. Nine raft cores share
the host, so outside CPU load inflates the baseline phases and can hide the
signal entirely. Treat a run taken alongside other work as invalid, not as a
counter-result.

## Limitations and follow-ups

In order of impact:

1. **`lanes` is opt-in, not the default.** It runs, it is tested (the
   `tests/integration/raft_lanes.rs` suite plus the standing-split and forwarding
   tests), and its isolation benefit is measured (above). `single` stays the
   recommended default until lanes has more production use. Choose `lanes`
   when a single group's log is clearly the bottleneck for your write load.
2. **Lane leadership convergence is best effort** (above). A lane whose
   current leader this node cannot beat stays where it is. The cost of an
   unconverged cluster is slower operations and delayed cross-lane updates,
   not lost writes: worker liveness forwards, and submits are accepted and
   reconciled. A firm co-location guarantee needs a leader-transfer API,
   which openraft 0.9 does not have.
3. **Transport auth is a shared cluster secret**, not mTLS. This matches the
   worker plane, which has the same known authentication gap. It will be
   upgraded along with worker mTLS. Forwarded lane writes use the same
   channel and the same auth.
4. **Client-plane forwarding replays the whole request** to the leader of
   the lane that rejected it, one hop, over plaintext manager-to-manager
   HTTP/2 like the raft transport. Under `lanes`, a composite whose first
   half committed on a lane this manager leads is replayed whole on the
   other lane's leader, the same converging shape as a client retry.
5. The `legion_raft_*` engine series are emitted (quorum shape, per-lane
   term/indexes/apply-lag/leadership, and the capture-vs-propose latency
   histograms). OTLP metrics push and the opt-in worker `/metrics` listener
   exist (docs/metrics.md). Snapshot timings and per-peer replication lag
   are not emitted. The UI's Raft page does not show per-lane quorum shape
   or voter-count warnings.
6. There is no raft autopilot.

openraft 0.9 has no leader-transfer API, so `step-down` and
`transfer-leader` are built on remote elections: the target is asked to
campaign (`RaftTransport.TriggerElection`) until it leads the control group,
while the manager handing leadership away stops campaigning. A move costs a
few terms, and it is best effort under load (the command warns when
leadership has not moved by its deadline). Decommissioning the current
all-lane leader still takes two steps, because `remove-member` refuses
self-removal: `transfer-leader` to another manager, then remove the old
leader from the new one. Legion does not enforce an odd number of managers,
so run 3 or 5. There is no pre-vote either: a restarted member stays quiet
until a leader reaches it (see [raft-operations.md](raft-operations.md)),
but a member cut off by a partition still campaigns, and its higher term
deposes the leader when the partition heals.

Two operational issues to know about:

- **A replica that hits an apply conflict stops that lane.** Legion detects
  divergence and aborts, instead of hiding it. The lane's raft core on that
  node crash-loops on the bad entry until the leader's log compaction
  passes it. The node then catches up by snapshot install, which with
  default settings can be thousands of writes later. If a lane on one node
  is down with a changeset-conflict error in its log, get the leader to
  snapshot past the bad entry, or rebuild the member. There is no command to
  take a snapshot on demand. Generate write traffic, or temporarily lower
  `snapshotEntries` on the leader and restart it. `legion raft command
  trigger-election` does not fix it. See
  [Recovering a replica that stopped a lane on an apply conflict](raft-operations.md#recovering-a-replica-that-stopped-a-lane-on-an-apply-conflict).
- **Scheduler-event ids are ordered per process, not by wall clock.** After
  a failover to a manager whose clock is behind, new event ids can sort
  below an existing `after_id` cursor until the clock catches up. Pollers
  can miss that window's events live. If gaps matter to you, re-read by
  cursor range (see docs/event-stream.md).

[openraft]: https://github.com/databendlabs/openraft
