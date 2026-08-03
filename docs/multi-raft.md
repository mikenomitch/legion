# Multi-Raft Persistence (Operator Guide)

`persistence.type = 'raft'` turns a set of Legion managers into a
quorum-replicated cluster: every store mutation is committed by a majority
of managers before it is acknowledged or visible anywhere, and losing any
minority of managers loses no committed write.

This is the engine-level reference. The task-oriented runbook is
[docs/raft-operations.md](raft-operations.md); upgrade/backup/restore
procedures are in [docs/upgrades.md](upgrades.md); the design history and
judgment-call log is
[docs/plans/durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md).

## What raft mode gives you

A mutation executes on the leader inside a rolled-back savepoint while a
SQLite session records its exact row-level effects (a **changeset** —
inserts/updates/deletes with values, rowids included); those bytes are
proposed as one [openraft] log entry, and only after a quorum commits it
does every manager — leader included — apply the same bytes to its local
`store.sqlite`. Replicas cannot diverge, because they apply *effects*, not
re-executed code, and nothing (not even on the leader) becomes visible,
cached, or event-published before quorum commit. The caller's reply
unblocks only after the local apply, so read-your-writes holds on the
leader.

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

Alongside write replication you get: leader election and automatic
failover, token-based membership (join/learner/voter promotion), real
operator commands and audit history, snapshots + log compaction, follower
catch-up via log replay or snapshot install, leader-only scheduling with
follower takeover, and non-leader write rejection with a leader hint.

## Configuration reference

All fields live under `persistence.raft` (camelCase in config files).
Defaults from `src/structs/config.rs` (`RaftPersistenceConfig::with_defaults`):

| Field | Default | Meaning |
| --- | --- | --- |
| `nodeId` | manager `host:port` id | Stable string identity of this manager in the cluster. The numeric raft member id is derived from it (`fnv1a64(nodeId)`), so it must be unique per manager and **permanent once joined**. |
| `bootstrap` | `false` | `true` on the **first** manager of a new cluster only: it initializes the raft group with itself as sole voter. Idempotent across restarts (re-initialization is a swallowed no-op). |
| `peers` | `[]` | Peer managers as `"name=host:port"` entries (the address is the peer's manager gRPC listener, which also serves the raft transport). Informational + name resolution for operator commands; real membership is driven by joins and membership commands. Bare `"name"` entries still parse. |
| `joinTokenTtlSeconds` | `900` | Lifetime of tokens minted by `legion raft join-token`. |
| `dataDir` | `.legion/raft` | Root for raft engine state: per-lane log DBs (`<dataDir>/<lane>/log.sqlite`), snapshots (`<dataDir>/<lane>/snapshots/`), the cluster secret file (`<dataDir>/cluster_secret`), and the double-start advisory lock (`lock.sqlite`). |
| `topology` | `"single"` | `"single"`: one raft group replicates every table (recommended default). `"lanes"`: three independent control/scheduler/status groups, for clusters whose write load is heavy enough that one group's log becomes a bottleneck (see "Topology: `single` vs `lanes`" below). Fixed at first engine start per data dir (JC-43): switching requires backup → fresh cluster → restore. |
| `advertiseAddr` | manager `host:port` | `host:port` other managers dial for this manager's raft transport. Wildcard binds (`0.0.0.0`/`::`) are normalized to `127.0.0.1` — set this explicitly for any real multi-node deployment. |
| `clusterSecret` | minted at bootstrap | Shared secret authenticating manager-plane raft RPCs. When absent it is minted at bootstrap and persisted (mode 0600) to `<dataDir>/cluster_secret`; joiners receive it inside the join token. |
| `joinToken` | none | Token from `legion raft join-token` used by a new manager to enroll into an existing cluster. |
| `expectedVoters` | unset | The number of voting managers this cluster is *supposed* to have (1, 3, or 5). Purely ergonomic: `legion raft status` warns when the observed voter count is below it, config load warns when it is even, and `0` is rejected at load. Membership itself is always driven by joins and operator commands, never by this knob. See "How many managers?" below. |
| `snapshotEntries` | `10000` | Applied log entries between snapshots. Each snapshot is a whole-DB `VACUUM INTO` image; the log is purged behind it keeping a `snapshotEntries / 2` tail (capped at 1000) for cheap follower catch-up. |
| `heartbeatIntervalMs` | `250` | How often a leader sends heartbeats (append-entries) to every follower. This is the floor on how fast a dead leader can be noticed, and — because openraft reuses it as the append-entries RPC **deadline** — it also bounds how long one replication batch may take, so it is a matched pair with the entries-per-batch cap. Lower = faster failure detection, more constant traffic, less tolerance for a slow round trip. See "When to raise the election timings" below. |
| `electionTimeoutMinMs` | `600` | Lower bound of the randomized election timeout: a follower that has heard nothing for a duration drawn from `[min, max)` campaigns to replace the leader. This is the **stability** knob — it must sit comfortably above `heartbeatIntervalMs` so a couple of merely slow or dropped heartbeats never look like a dead leader. Set it too close to the heartbeat interval and the cluster flaps: followers time out under transient load, campaign, split the vote, and the term climbs while no leader holds power long enough to commit. Config load **rejects** `electionTimeoutMinMs < 2 × heartbeatIntervalMs`. |
| `electionTimeoutMaxMs` | `1200` | Upper bound of the randomized election timeout; must be strictly greater than `electionTimeoutMinMs` (config load rejects otherwise). The randomized *width* between min and max is what breaks up split votes, so keep a real gap. This value is **also the follower leader-lease**: a follower will not grant its vote while it still believes the current leader's lease is live. That cuts both ways — a larger maximum buys stability under load but slows every *deliberate* leadership move (lane convergence, `trigger_lane_election`) by the same proportion. Prefer widening the window over inflating it; the default keeps `max = 2 × min`. |
| `forceNewCluster` | `false` | Explicit override to re-stamp a state DB onto a NEW cluster identity. Without it, a manager whose derived cluster id does not match the id stamped into its state DB **refuses to start** (protects against a wiped `dataDir` + `bootstrap: true` silently minting a second cluster over the old cluster's data — split-brain). |

The raft transport is a gRPC service (`RaftTransport`) multiplexed onto the
manager's existing listener — there is no separate raft port.

### When to raise the election timings

The defaults (250 ms heartbeat, 600–1200 ms election window) assume a
manager that gets scheduled promptly and reaches its peers over a LAN. They
are in the range of Consul's and etcd's defaults: a dead leader is replaced
in roughly one election timeout plus a round trip, ~1 s.

Raise all three together — keeping the ratios roughly intact — when the
environment can stall a heartbeat for longer than the window tolerates:

- **Loaded managers.** A manager competing for CPU with the rest of the box
  can be descheduled for hundreds of milliseconds; its followers read that
  as a dead leader.
- **WAN or cross-region links.** The heartbeat interval doubles as the
  append-entries deadline, so it has to cover a full round trip plus the
  batch, not just the one-way latency.
- **Virtualised or otherwise noisy hosts.** Steal time and ballooning
  produce exactly the multi-hundred-millisecond pauses that trigger
  spurious elections.
- **CI.** In-process multi-node harnesses run several raft cores on a
  couple of cores in a debug build; the test suites under `tests/` set
  relaxed timings for this reason.

The failure mode you are avoiding is **flapping**: followers time out
against a leader that is merely slow, campaign, and the term climbs while
no leader holds power long enough to commit. The cost of raising them is
proportionally slower failover — and, because `electionTimeoutMaxMs` is
also the follower leader-lease, proportionally slower *deliberate*
leadership moves. Widen the window rather than inflating it: raise
`electionTimeoutMinMs` for stability and keep `electionTimeoutMaxMs` a
small multiple of it.

Config load rejects combinations that would flap by construction, naming
the offending values: `electionTimeoutMinMs` must be at least
`2 × heartbeatIntervalMs`, and `electionTimeoutMinMs < electionTimeoutMaxMs`.

## How many managers?

Run **1, 3, or 5** voting managers. A raft group commits when a majority
(quorum = `voters / 2 + 1`) of its voters is alive, so:

| Voters | Quorum | Survives | Verdict |
| --- | --- | --- | --- |
| 1 | 1 | 0 failures | fine for dev / single-node durability |
| 2 | 2 | 0 failures | ⚠ both must be alive to commit — worse than 1 in practice |
| 3 | 2 | 1 failure | the recommended production minimum |
| 4 | 3 | 1 failure | ⚠ no more fault tolerance than 3, more machines to keep in quorum |
| 5 | 3 | 2 failures | for clusters that must survive two concurrent failures |

An **even** voter count adds a manager without adding fault tolerance — and
2 voters are strictly worse than 1, because now *either* machine failing
stops all writes. Legion therefore warns (never blocks, per plan JC-29) at
every surface where you would notice:

- **Config load** — an even `expectedVoters`, or a `peers` list describing
  an even cluster, logs the advisory once at startup; `expectedVoters: 0`
  refuses to load.
- **`legion raft status`** — every lane prints
  `voters N (quorum Q) — survives F failure(s)`, with a `⚠` + the advisory
  when N is even or below `expectedVoters`. The same numbers ride the
  `GetRaftStatus` reply (`lanes[].voters/quorum/survives/warning`).
- **Membership changes** — `add-voter`, `remove-member`, and join promotion
  compute the *resulting* voter count; an even result proceeds but is
  WARN-logged, recorded as a `membership` event in `legion raft history`,
  and returned on the command's own output. Join promotion is **never**
  withheld to preserve parity — a visible warning beats an invisible
  availability surprise.
- **Metrics / probes** — the `legion_raft_*` series (docs/metrics.md):
  quorum shape (`voters`/`quorum`), per-lane progress (`term`,
  `commit_index`, `applied_index`, `apply_lag`, `is_leader`), and the
  bottleneck-attribution histograms (`capture_seconds` vs
  `propose_apply_seconds`, plus `ops_total`); `readyz` stays `200`
  but includes the advisory as a `note:` line while parity is even.

The **only hard block** is a `remove-member` that would leave zero voters
or a quorum the currently-known-alive members cannot form (the membership
change itself could then never commit and the cluster would wedge); the
refusal message names the fix.

Set `expectedVoters` to the size you intend (say `3`) and alerting on the
status warning / gauges will tell you when a manager is down, still a
learner, or never joined.

## Bootstrap and join: a 3-node walkthrough

See `configs/manager-raft.jsonnet` for a complete commented example.

1. **Bootstrap the first manager** (`mgr-a`):

   ```jsonnet
   persistence: {
     type: 'raft',
     raft: {
       nodeId: 'mgr-a',
       bootstrap: true,
       advertiseAddr: '10.0.0.1:4747',
       peers: ['mgr-a=10.0.0.1:4747', 'mgr-b=10.0.0.2:4747', 'mgr-c=10.0.0.3:4747'],
     },
   },
   ```

   ```bash
   legion start manager -c mgr-a.jsonnet
   ```

   It elects itself leader of a single-voter cluster. If a pre-existing
   `store.sqlite` has data (an upgrade from `local_disk` or from the old
   simulated raft), that database **becomes the initial replicated state**,
   and bootstrap builds a seed snapshot of it so late joiners receive the
   full baseline via snapshot install.

2. **Mint a join token on any running member:**

   ```bash
   legion raft join-token
   # token=lgnraft_...   expires_at=...
   ```

   The token is base64url JSON carrying the cluster id, the cluster
   secret, the current **leader's** advertise address, and an expiry.
   Treat it as a secret — the embedded secret is the credential.

3. **Start `mgr-b` and `mgr-c`** with `bootstrap: false` and
   `joinToken: 'lgnraft_...'` in their raft config. Each joiner enrolls in
   the background: it dials the leader's `Join` RPC (retrying until the
   leader is reachable), is added as a **learner**, has its log caught up
   (by replay or snapshot install), and is promoted to **voter**. Progress
   appears in both sides' logs and in `legion raft history`.

Restarted members never need a token: they recover from their raft log and
rejoin automatically. Permanent join rejections (expired token, wrong
cluster, malformed token) stop the enrollment loop with a loud error —
mint a fresh token and restart the joiner.

Two safety interlocks at engine start:

- **Double-start lock:** `<dataDir>/lock.sqlite` holds an exclusive OS file
  lock for the process lifetime; a second manager process pointed at the
  same `dataDir` fails immediately instead of running a competing raft
  core over the same log.
- **Cluster stamp:** the state DB is stamped with its cluster id at first
  engine start and every later start must match (see `forceNewCluster`
  above).

## Operator commands

```bash
legion raft status         # role, leader, term, commit/applied, voters/learners
legion raft history -n 100 # real transitions from the engine metrics stream
legion raft command <cmd> [--target <node>]
legion raft join-token
```

Supported commands (all driving real openraft APIs, all recorded in the
audit history with actor attribution):

- `trigger-election` — force an election on every lane.
- `add-learner --target name@host:port` — add a non-voting member.
- `add-voter --target name` — promote to voter (address resolvable from
  `raft.peers` or given as `name@host:port`).
- `remove-member --target name` — demote and remove a member.
- `step-down` / `transfer-leader` — **not supported**: openraft 0.9 has no
  leader-transfer API; these return an honest error rather than a
  simulated success. Run `trigger-election` pointed at the desired new
  leader instead.

`legion raft status` prints per-lane lines (`lane=... role=... term=...`)
when more than one raft group is active — i.e. under the experimental
`lanes` topology; in `single` topology the headline lines are the one
group. The same data is in the UI under **Cluster → Raft** and via the
`GetRaftStatus` / `ListRaftHistory` / `IssueRaftCommand` /
`GetRaftJoinToken` RPCs.

## Topology: `single` vs `lanes`

The engine is built multi-group: every mutating store method is classified
into a lane (`control` = job/config/KV/instance writes, `scheduler` =
placement/eval/rollout writes, `status` = heartbeat/liveness writes), with
an exhaustiveness test, and the transport multiplexes groups by
`group_id`.

- **`single` (default, recommended):** one raft group — keyed `control` —
  carries every lane. This delivers the durability guarantee with
  whole-DB snapshots that are trivially sound.
- **`lanes` (experimental):** three independent raft groups — independent
  logs, apply loops, and replication pipelines, so a burst of control
  writes cannot stall the scheduler's log. Soundness rests on
  **single-writer table ownership**: every replicated table belongs to
  exactly one lane, enforced at capture time in every topology (a write
  whose rows leave its lane's ownership is rejected before proposal), plus
  a small commutative class of insert-only tables with globally unique
  keys. Snapshots are per-lane and table-scoped (below). Lane leadership
  is deliberately **co-located**, not spread — see below.

### Lane leadership: co-located, best effort

Under `lanes`, Legion tries to keep **one manager leading all three lanes**.
The reason is composite operations: registration writes control identity and
status liveness, job submit writes control tables, a status seed *and* a
scheduler-lane volume-claim sync. Each half is proposed from the node the
client is talking to, so under divergent leadership a composite half-fails on
whichever lane that node does not lead.

The mechanism: the **control lane is the anchor**, and whichever node leads it
campaigns for the lanes it does not lead. That is the only lever available —
openraft 0.9 has no leader-transfer API, so a leader cannot be *handed* a lane,
only *won*.

**Convergence is therefore best effort, not an invariant.** A campaign is a
race, and raft's election safety can refuse it forever — a node whose log for
that lane is behind cannot win, and does not deserve to. What Legion
guarantees instead:

- **A lane always elects.** Convergence never campaigns for a lane that has
  no leader. A leaderless lane is already holding an election on every
  voter's own randomized timer, run by raft's real policy (leader lease,
  and a deliberate extra delay for a node that has seen a longer log
  elsewhere); an external campaign bypasses all of it, restarts this node's
  own candidacy at a higher term, and — openraft 0.9 has no pre-vote — drags
  every other voter's term up with it. Two campaigners on a three-voter
  group with one member down never reach a quorum, so the term climbs and
  nobody wins. A dead write path always outranks co-location, so
  convergence stands aside.
- **Disruption is bounded.** Campaigns for a strayed lane are per-lane,
  spaced exponentially (2 → 16 election windows), and stop entirely after
  three consecutive failures for a cooldown of 60 election windows, doubling
  up to 480 for a lane this node keeps failing to take. Any win resets it.
  A lane this node cannot take therefore costs the cluster a handful of raft
  terms per cooldown rather than an unbounded climb, and raft's own
  elections get long uninterrupted stretches. The stand-down is logged at
  `warn` naming the lane and the cooldown.
- **A campaign is skipped when it cannot win.** Before campaigning, the node
  checks that the lane's log is not still streaming into it: a log that grew
  since the previous check means this node is behind whoever it replicates
  from. That is the only log-currency signal openraft exposes to a
  non-leader — the authoritative test is the election itself, which is what
  the backoff above scores.

**What divergent leadership costs you.** It is safe and it degrades rather
than dies, but it is not free:

- **Worker liveness keeps working, by digest.** Workers dial one fixed
  manager; when that manager does not lead the status lane it batches the
  node ids it has heard from and sends the owning leader ONE
  `ReportNodeLiveness` RPC per `liveness.digestInterval` (1s) over the
  authenticated manager plane. The cost is `managers x 1 RPC/interval`,
  independent of fleet size, and no heartbeat becomes a replicated write.
  If those digests stop arriving, the leader will not mark that peer's nodes
  lost inside `liveness.peerDigestGrace` — a manager-to-manager problem must
  not present as fleet-wide node loss — and raises
  `legion_liveness_peer_stale{peer}`. See
  [node-liveness.md](node-liveness.md).

  Health-check, allocation-health and failure-report writes are still
  FORWARDED to the owning lane's leader (`ForwardLaneWrite` on the raft
  transport), and the worker's RPC succeeds only after the forwarded write
  commits. Heartbeats fall back to that same path during a manager upgrade,
  while a peer that predates `ReportNodeLiveness` leads the status lane. The
  forwarding path also keeps a mark-lost's control-lane availability sync
  working from the status leader, so a lost node cannot keep receiving
  placements. (It equally covers plain `single`-topology failover: workers
  pointed at a demoted ex-leader keep reporting through it.)
- **Job submits are accepted by the control leader** and complete
  asynchronously. The control half — the operator's intent — must commit on
  the node you submitted to (a non-control-leader still rejects with the
  typed leader hint). The cross-lane projections (the `task_status` seed,
  the volume-claim rows, the rollout for an updated job, the scheduling
  eval) are then completed by the owning lanes' leaders: evals forward
  immediately, and per-lane reconcilers derive the rest from replicated
  state on the node-status-checker cadence (default 2s). Until they run,
  a just-submitted job can briefly show tasks without statuses, and an
  updated job's rollout can appear a tick late — with its canary/manual
  gating intact (the reconciler creates the same rollout the submit would
  have; the eval-regeneration fallback that would bypass gating runs only
  after it).
- **Scheduling latency degrades.** Placement runs on the scheduler lane's
  leader; work that would have been enqueued inline arrives by forwarding
  or by the reconcile cadence instead.
- **`legion state backup` is unavailable** until one node leads every lane
  again (it needs a single authoritative image).
- Single-lane operations are unaffected: a lane keeps serving under its own
  leader throughout, and reads are served by every replica.
- `/readyz` tolerates ~10s of continuously-observed divergence (measured by
  each manager's own probe, so a rolling restart gets the full grace) and
  then returns `503` with a per-lane body — a standing split is an abnormal
  leadership shape an operator should look at even though the write path
  keeps working.

If a cluster sits split, the fix is the same as for any leadership problem:
check `legion raft status` per lane, and if one lane's leader is unhealthy,
restart it so its lane re-elects. Raising `electionTimeoutMinMs` /
`electionTimeoutMaxMs` on loaded or WAN clusters also stretches the whole
convergence schedule, which is derived from `electionTimeoutMaxMs`.

### Table ownership is a cluster-wide agreement

Which lane owns a table is part of the on-disk and on-the-wire contract, not
an internal detail: a lane's snapshot is a dump of the tables that lane owns,
and the receiver validates the dump's manifest against **its own** ownership
map before replacing anything (refusing to clobber a sibling lane's table).
So **moving a table from one lane to another is not a rolling upgrade.** A
mixed-version cluster cannot agree on what a lane's snapshot contains: the
old-version node rejects the new-version node's snapshot (and vice versa)
with an error naming the table.

Reclassifying a table therefore takes the same path as any other on-disk
format change (JC-43): back up (`legion state backup`), start a **fresh**
cluster on the new version, restore it with
`legion start manager --restore-from <backup>`, then rejoin the members.
Adding a *new* table to a lane is fine — only moving an existing one between
lanes is a break.

## Snapshots, compaction, and catch-up

In `single` topology:

- Every `snapshotEntries` (default 10 000) applied entries the engine
  builds a snapshot: a `VACUUM INTO` image of the whole state DB, taken
  under an exclusive hold that excludes captures and applies, so the image
  is transactionally consistent **and contains its own applied-index
  marker** (the `raft_applied` table travels inside the image). Metadata
  lands in a `.meta.json` sidecar; the two newest snapshots are retained.
- The raft log is purged behind each snapshot, keeping a tail
  (`snapshotEntries / 2`, capped at 1000) so a slightly-behind follower
  catches up by cheap log replay; anything older is served by snapshot
  install.
- **Install** replaces the follower's state DB wholesale: staged to a temp
  file, applied marker written in, atomic rename, every connection and the
  read model reopened, then a post-install consistency check. A crash at
  any point leaves either the old or the new DB — never a torn one.
- **Bootstrap seed snapshot:** on a fresh bootstrap the engine immediately
  snapshots the (possibly pre-raft) database and compacts the log to it,
  so any adopted baseline reaches joiners via snapshot install rather than
  being silently missing from log replay.
- Crash/restart recovery: openraft replays vote/log from the per-lane log
  store (SQLite, WAL + `synchronous=FULL`); the state machine resumes from
  the applied index recorded **in the same transaction** as each applied
  changeset, so state and index cannot diverge.

In `lanes` topology the policy, compaction, retention, and recovery are
identical **per lane**, but the snapshot's scope is not: a whole-DB image
would clobber the sibling lanes' tables on install, so a lane's snapshot is
a logical dump of just the tables that lane owns (the control lane also
carries the commutative insert-only tables). Install is one transaction that
replaces those tables' rows and writes the lane's applied marker; tables
owned by other lanes are never touched, and the dump's manifest is validated
against this node's ownership map first — see "Table ownership is a
cluster-wide agreement" above for why that makes lane reclassification a
fresh-cluster operation.

## Backup and restore

- `legion state backup [--out <path>]` (RPC `Manager/GetStateBackup`, ACL
  `operator:write`) writes a consistent `VACUUM INTO` copy **on the
  manager host** (default `<storePath>/backups/legion-state-backup-<ts>.sqlite`)
  and reports path, bytes, and the raft applied index it captures. Works
  in every persistence mode; in raft mode it runs only on the node that
  **leads every lane** — anywhere else answers `FAILED_PRECONDITION` naming
  that lane's leader. Under a standing lane split (convergence is best
  effort, above) no node satisfies that, so back up after leadership has
  converged.
- **What the backup is, exactly.** A point-in-time image of *that node's*
  applied state — the whole database file, all lanes' tables, as of one
  wall-clock moment under an exclusive hold that excludes captures and
  applies. It is **not** a cluster-wide snapshot at a single raft index:
  under `lanes` the three groups apply independently, so the image sits at
  a different applied index per lane and the reported `applied index` is
  the highest across them. This is the right artifact for what restore
  does — seed a fresh cluster — but do not read the number as "the cluster
  was at index N".
- `legion start manager --restore-from <backup> [--force-restore]` copies
  the backup into place as the state DB before the store/engine starts,
  clearing raft bookkeeping so the restored state becomes the **adopted
  baseline of a fresh cluster**. Restore onto a fresh bootstrap node, then
  join the others — never into a member of an existing cluster.

Full procedures (including rollback out of raft mode): `docs/upgrades.md`.

## Failure modes

Each of these is exercised by an in-process 3-node test in
`tests/raft_durability.rs` / `tests/raft_snapshot_recovery.rs`:

| Scenario | Behavior |
| --- | --- |
| **Leader loss** | In-flight unacked writes fail with a typed error (retry them); every acked write survives. A new leader is elected within the election timeout (600–1200 ms by default, see `electionTimeoutMinMs`/`electionTimeoutMaxMs`); its eval workers and node status checker take over the replicated queues within ~1 s. Clients pointed at the dead leader redial per the leader hint. |
| **Minority partition** | The majority side keeps electing and committing. Managers on the minority side reject writes with `FAILED_PRECONDITION` + leader hint (they cannot reach a quorum) and keep serving local reads. On heal, the minority catches up and converges byte-identically. |
| **Follower loss** | No client impact; the cluster commits on the remaining quorum. On return the follower catches up by log replay, or by snapshot install if the leader has compacted past its position. |
| **Full-cluster restart** | Every node recovers from its raft log + state DB; a leader is re-elected and the cluster resumes with all committed state. A restarted member that cannot yet hear the leader (its inbound transport shares the manager listener) proceeds leaderless with a warning and catches up once serving — startup never deadlocks on the election. |
| **Operator double-start** | The second process fails fast on the `dataDir` advisory lock. |
| **Wiped `dataDir`, kept state DB** | Startup is refused by the cluster stamp unless `forceNewCluster: true` (deliberate re-stamp). |
| **Standing lane split** (`lanes` topology; different managers lead different lanes past convergence) | Nothing is lost: worker liveness and reports forward to the owning lane's leader, submits are accepted by the control leader and completed by lane reconcilers within the checker cadence, single-lane ops and reads are unaffected. Costs: scheduling latency, briefly-lagging task statuses/claims/rollouts, no `state backup`, `/readyz` 503 after ~10s as the operator signal. Exercised by `tests/raft_lanes.rs` (standing-split and forwarding tests). |

## Non-leader write redirects and forwarding

**Client-plane** mutating RPCs sent to a non-leader are rejected with
`FAILED_PRECONDITION` and a message naming the current leader (`retry
against leader mgr-a at 10.0.0.1:4747`); the CLI surfaces it. The
rejection happens **before** any capture work runs — point write clients
(`LEGION_ADDR`) at the leader. Reads are served by every manager from its
local replica. The same typed rejection covers leader-only non-proposal
operations (`legion state backup`).

**Worker-plane** writes are different: workers dial one fixed manager and
cannot chase leader hints, so their writes FORWARD instead of rejecting.
A manager that receives a heartbeat, health check, allocation-health
snapshot, failure report, or volume report it cannot propose (it does not
lead the owning lane — after a failover in `single` topology, or under a
lane split in `lanes`) sends the operation over the authenticated raft
transport (`ForwardLaneWrite`, cluster-secret auth, 5s deadline) to the
lane's leader and answers the worker only after the forwarded write
commits. Forwarded operations carry their full arguments, the receiver
executes the non-forwarding variant (a stale hint costs one typed
rejection, never a forwarding loop), and a forward that cannot complete
surfaces the original typed rejection. During a rolling upgrade from a
release without this RPC, a forward to an old manager fails cleanly and
behavior degrades to the old reject-and-retry.

Scheduling is leader-only by the same mechanism: eval queue state
replicates everywhere (so a failover resumes the queue), but only the
leader's loops act; follower loops idle until a leadership change flips
their leadership probe.

## Performance

Raft-mode write-path baseline, measured by `tests/raft_perf_smoke.rs`
(`cargo test --test raft_perf_smoke --locked -- --ignored --nocapture`):
a single-node bootstrap cluster (`topology = single`, self-quorum) running
`put_kv` through the full propose → capture → quorum-commit →
state-machine-apply pipeline, debug build, 2026-07-23:

| Workload | Ops | p50 | p99 | Throughput |
| --- | --- | --- | --- | --- |
| Sequential `put_kv` round-trips | 1000 | 3.34 ms | 30.73 ms | 224.6 ops/s |
| Concurrent burst (8 threads × 100 ops) | 800 | 23.56 ms | 39.20 ms | 332.6 ops/s |

The smoke asserts p99 < 250 ms — a deliberately generous bound that catches
order-of-magnitude regressions in the replication pipeline (capture-mutex
contention, per-apply read-model projection, log fsync pileups), not a tuned
benchmark target. Latencies include everything the caller experiences:
capture savepoint + changeset extraction, the sync→async bridge to
`client_write`, local log append/fsync, and changeset apply with read-model
and event projection.

For comparison, the `local_disk` scheduler SLO gate
(`cargo test --test scheduler_benchmark_slo --locked -- --ignored --nocapture`)
run at the same commit: steady-small 238.7 evals/s / p99 5.34 ms,
churn-medium 174.0 evals/s / p99 7.27 ms, high-fanout 135.0 evals/s /
p99 10.84 ms — all within SLO, unchanged by the raft work (`local_disk` is
byte-identical per plan §A2).

### Lane isolation A/B (lanes vs single, 2026-07-28)

The point of lanes is that saturating one lane must not drag down another.
Measured by `lanes_vs_single_scheduler_isolation_ab` in
`tests/raft_perf_smoke.rs`: a 3-node in-process cluster (real tonic
`RaftTransport` listeners over loopback), scheduler-lane `place_alloc`
propose→apply latency sampled twice per topology — unloaded, then while 2
threads saturate the control lane with job-submit bursts and 2 threads
flood the status lane with heartbeats (200 measured placements per phase,
debug build, retried-after-leadership-blip samples excluded). Three
consecutive runs on an otherwise idle Apple-Silicon machine, 2026-07-28;
the representative middle run:

| Topology | Phase | p50 | p99 | Placements/s | Cross-lane load committed (load window) |
| --- | --- | --- | --- | --- | --- |
| `single` | baseline | 1.17 ms | 1.38 ms | 855 | (none) |
| `single` | cross-lane load | 2.69 ms | 110.45 ms | 129 | 335 submits + 990 heartbeats (1.6 s window) |
| `lanes` | baseline | 1.13 ms | 1.41 ms | 875 | (none) |
| `lanes` | cross-lane load | 1.65 ms | 3.37 ms | 548 | 774 submits + 857 heartbeats (0.4 s window) |

Scheduler p99 degradation under cross-lane load, across the three runs:
**`single` 50–93×, `lanes` 1.6–2.4×**. The same cross-lane flood that
blows the single-group scheduler tail out to ~100–130 ms leaves the lanes
scheduler tail essentially flat (2.4–3.4 ms), and lanes retains 4–5× more
placement throughput under load (543–606/s vs 104–131/s). Under `single`,
every op funnels through one raft group's pipeline, so placements queue
behind the whole flood; under `lanes` the scheduler group commits
independently, and the only shared stage left is the per-node capture
mutex (captures are local SQLite work, ~sub-ms — quorum commit is the
dominant cost and it parallelizes). The isolation also shows up on the
loaded lane itself: control-lane submit throughput was ~9–10× higher
under `lanes` (~1900/s vs ~200/s) because submits no longer queue behind
heartbeats and placements.

Honest caveats: debug build, in-process loopback cluster — read the
ratios, not the absolute numbers (real WAN quorum commits dominate very
differently). Unloaded baselines are identical between topologies within
noise, so lanes costs nothing when idle. The isolation direction (flat
scheduler tail under cross-lane load) was consistent across every run
on an idle machine, and on earlier shared-CPU CI-class runs (which
showed the same split at 5.4× vs 1.1× with far noisier absolutes). The
benchmark itself needs a quiet machine to mean anything: nine raft cores
share the host, so external CPU load inflates the *baseline* phases and
can bury the signal entirely — treat a run taken alongside other work as
invalid rather than as a counter-result.

## Limitations and follow-ups

In priority order (plan §6, updated 2026-07-28):

1. **`lanes` is opt-in, not the default.** It is runnable, tested (the
   `tests/raft_lanes.rs` battery plus the standing-split and forwarding
   tests), and its isolation benefit is measured (above) — but `single`
   remains the recommended default until lanes has more production
   mileage. Choose `lanes` when a single group's log demonstrably
   bottlenecks your write load.
2. **Lane leadership convergence is best effort** (above): a lane whose
   incumbent this node cannot beat stays where it is. Since the
   standing-split resilience work (plan §8), the cost of an unconverged
   cluster is degraded latency and deferred projections, not lost writes —
   worker liveness forwards, submits are accepted and reconciled. A real
   co-location guarantee needs a leader-transfer API openraft 0.9 does not
   have.
3. **Transport auth is a shared cluster secret**, not mTLS (JC-7 stopgap,
   consistent with the worker plane's tracked authn gap). Upgrades with
   the worker-mTLS work. Forwarded lane writes ride the same channel and
   the same posture.
4. **Auto-forwarding covers the worker plane and internal lane halves
   only.** Client-plane RPCs still reject with the leader hint (above) —
   deliberate: clients can redial, and every client-facing op is anchored
   on the control leader.
5. The `legion_raft_*` engine series are emitted (quorum shape, per-lane
   term/indexes/apply-lag/leadership, and the capture-vs-propose latency
   histograms), OTLP metrics push and the opt-in worker `/metrics`
   listener exist (docs/metrics.md); snapshot timings and per-peer
   replication lag remain reserved, and a Raft health UI page is still
   open.
6. Raft autopilot — speculative, default-off.

Also: `step-down`/`transfer-leader` await an openraft with a
leader-transfer API — including for decommissioning the current all-lane
leader, which `remove-member` refuses as self-removal: move leadership
first (`trigger-election` on another manager), then remove the
now-follower from the new leader. Manager count is whatever you join (no
enforced odd-membership guard) — run 3 or 5.

Two operational sharp edges an operator should know:

- **A replica that hits an apply conflict stops that lane loudly** (the
  conflict-ABORT policy: divergence is detected, never papered over). The
  lane's raft core on that node crash-loops on the poisoned entry until
  the leader's log compaction passes it, after which the node catches up
  by snapshot install — with default settings that can be thousands of
  writes later. If a lane on one node is down with a changeset-conflict
  error in its log, the fast fix is a fresh snapshot on the leader
  (`legion raft command trigger-election` is NOT it — see
  docs/raft-operations.md) or restoring the node from backup.
- **Scheduler-event ids order per process, not per wall clock.** After a
  failover to a manager whose clock is behind, newly minted event ids can
  sort below an existing `after_id` cursor until the clock catches up —
  pollers can miss that window's events live and should re-read by cursor
  range if gap-sensitive (see docs/event-stream.md).

[openraft]: https://github.com/databendlabs/openraft
