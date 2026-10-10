# Upgrades

This page covers how to upgrade a Legion cluster safely: the order to upgrade
in, what the version handshake enforces, and how to see version skew across
the fleet.

## Upgrade order: managers first, then workers

Always upgrade in this order:

1. **Managers.** Upgrade every manager to the new release (rolling, one at a
   time in an HA deployment). In a raft cluster, upgrade the followers first
   and the leader last. See
   [Rolling upgrades across a schema change](#rolling-upgrades-across-a-schema-change).
2. **Workers.** Once all managers run the new release, upgrade workers at
   whatever pace suits the workloads (drain and replace, or restart in place.
   See `legion node drain`).

The protocol handshake (below) enforces this order. Managers stay compatible
with every worker protocol version up to their own. They cannot promise to
satisfy a contract newer than they know about, so a worker upgraded before
its managers is rejected at registration.

## Stopping a manager

Restart managers with SIGTERM (or Ctrl-C, which sends SIGINT). That's what
`systemctl stop` and Kubernetes send. The manager then stops in order:

1. It stops accepting connections and gives in-flight RPCs and open streams
   (event streams, worker connections) up to 10 seconds to finish.
2. It stops its background loops (eval workers, node checker, keyring and
   identity rotation, the in-process worker), so nothing writes to a store
   that is shutting down.
3. It stops persistence, with up to 15 seconds for this step. In `streaming`
   mode it runs one final replica sync and then releases the object-storage
   lease, so a standby that takes over finds every committed write. In
   `raft` mode it shuts each lane's raft core down cleanly.
4. It exits with status 0.

The whole sequence fits in Kubernetes' default 30-second termination grace
period. A second SIGTERM or SIGINT during the drain skips the rest and exits
immediately.

Raft leadership is not handed off before exit. openraft 0.9 has no
leadership-transfer API, so the remaining managers elect a new leader after
their election timeout, the same as after a crash. Writes to lanes the
stopped manager led pause for that window, so restart managers one at a
time and wait for `legion raft status` to show a leader before the next.

A manager whose gRPC listener fails to bind (the port is taken, or the TLS
certificate can't be loaded) or stops serving exits with a non-zero status
and `manager gRPC server on <addr> failed`. Run managers under a supervisor
that restarts them. The manager doesn't keep scheduling without an API,
because nothing could reach it and, in raft mode, it would have no raft
transport.

## The `protocol_version` contract

Workers and managers exchange versions during registration
(`WorkerRegister` in `proto/legion.proto`):

- The worker sends `build_version` (its Cargo package version, e.g.
  `"0.1.0"`) and `protocol_version` (the wire-contract version it speaks,
  `PROTOCOL_VERSION` in `src/constants.rs`).
- The manager replies with `manager_build_version` and
  `manager_protocol_version`.

`protocol_version` changes only for breaking wire-contract changes to
worker-plane RPCs (registration, heartbeats, health checks, work dispatch).
That means removed or renamed fields, changed field semantics, or new
required behavior an old peer cannot satisfy. Additive, backward-compatible
fields do not change it.

The manager decides whether to admit a worker at registration:

| Worker `protocol_version`     | Manager decision                                        |
| ----------------------------- | ------------------------------------------------------- |
| `0` (pre-versioning build)    | Accepted, with a warning log to upgrade this worker     |
| `1 ..= manager's version`     | Accepted                                                |
| `> manager's version`         | Rejected with `FAILED_PRECONDITION`                     |

A `0` appears because proto3 decodes a missing field as the zero value.
Workers built before the handshake existed do not send the field. They are
admitted so a rolling upgrade from the current release works, and the
warning reminds you to finish the rollout.

A rejection means the upgrade order was reversed: the worker was upgraded
before the manager. The error message says so and tells you to upgrade all
managers first, then re-join the worker. Heartbeats skip the handshake to
keep that hot path lean. The check runs once per registration.

## What each protocol version added

| Version | Added | Behavior with an older worker |
| --- | --- | --- |
| 2 | Workers report task-process exits (`terminated_allocations` on the heartbeat), so a batch task's `Success` comes from its exit code. See [Task Supervision](task-supervision.md) | The manager reads the node's advertised `protocolVersion` and keeps the pre-2 behavior for batch tasks placed there: the task is marked `Success` at placement, as before. Nothing stalls mid-rollout. New placements get exit-code-based status once the worker is upgraded. |
| 1 | The versioned handshake itself | Workers reporting `0` are pre-versioning builds and are accepted with a warning |

## Graceful stop and preemption under skew

Neither feature changes `protocol_version`, and both degrade instead of
failing. Know how they degrade on a mixed fleet:

- **An old worker ignores the stop sequence** and hard-kills as it always
  did. Its tasks get no signal, no grace period, and no
  `LEGION_STOP_NOTICE_PATH` file. Preemption still works, but victims are
  killed abruptly ([graceful-stop.md](graceful-stop.md)).
- **An old worker does not order stops before starts**, so a new manager can
  place a preemptor there while the victim is still running. This causes
  brief oversubscription. A container task can briefly hold memory the
  preemptor was placed against.

`enforce` is the default, so a new manager preempts as soon as it starts. To
avoid both problems, set `schedulerSettings.preemption.mode = "observe"` in
the manager config before you upgrade the managers. Switch it back to
`enforce` once every worker is upgraded
([preemption-and-eviction.md](preemption-and-eviction.md)). Preemption is not
gated on a minimum worker protocol version. A gate would make it silently
inactive on any fleet with one stale worker, which is worse than a documented
window.

Priority bands need nothing from the worker. They live entirely in the
manager's planner and eval queue, so worker skew does not affect them.

## Edge boot under skew

[Edge boot](edge-boot.md) is the one feature that **is** gated on a minimum
worker protocol version. `TaskAllocation.provisional` is a new optional
field, so an old worker would ignore it and run the payload as soon as it
arrives. With pre-commit dispatch, that means running it before the commit,
with no gate and no lease.

That is not a safe degradation, so the dispatcher blocks it. A node that
advertises `protocolVersion` below 2 silently falls back to post-commit
dispatch and does not get the speedup. Nothing stalls, and no allocation
runs ungated.

The other direction fails closed on its own. `RatifyClaim` is a new RPC, so
an old manager rejects it and the worker kills the claim when its lease
expires. The result is a wasted boot, never a duplicate.

Neither the `boot` ACL resource nor the `edgeBoot` config blocks need a
coordinated upgrade. Both are off by default, and a manager that does not
know them ignores them.

## Task-plugin ABI

Task runtimes loaded from `plugins/tasks/<name>/` are **not** covered by
`protocol_version`. They are dynamically linked into the worker and must be
rebuilt against the Legion version they run under. Each plugin declares the
vtable generation it was compiled against. A mismatch (or a missing
declaration) is refused at load with a message that tells you to rebuild.
See [task-runtime-plugins.md](task-runtime-plugins.md). Rebuild out-of-tree
task plugins as part of any worker upgrade.

## Build-version skew expectations

`build_version` is informational and never causes rejection. Only
`protocol_version` gates admission. What to expect:

- **Same minor version** (e.g. managers `0.4.2`, workers `0.4.1`): fine.
  This is the normal state mid-rollout.
- **Different minor or major versions**: works as long as the protocol
  version is compatible, but the worker logs a warning at registration when
  its build version differs from the manager's. If the fleet keeps warning
  after a rollout should have finished, finish the upgrade.

## Observing fleet versions

Registration stores each worker's versions in the node's dynamic
attributes, so you can see skew from the control plane:

```sh
legion node list -j | jq '.data[] | {id, dynamicAttrs}'
```

Each node carries `buildVersion` (the worker's build version, or
`"unknown"` for pre-versioning workers) and `protocolVersion` (`0` for
pre-versioning workers) in `dynamic_attrs`. The same attributes appear in
the node ledger and the UI's node views. Workers also log the manager's
build and protocol version after every successful registration.

## Manager-plane skew (raft clusters)

Managers in one raft cluster talk to each other over the `RaftTransport`
service. During a rolling manager upgrade, the plane tolerates skew:

- The lane-write forwarding RPC (`ForwardLaneWrite`, added 2026-07-28)
  degrades cleanly against an older manager. The forward fails with the
  older side's "unimplemented" status, and the caller returns the same
  typed leader-hint rejection it returned before forwarding existed.
  Upgrade all managers (the standard order above) and the window closes.
- The liveness digest RPC (`ReportNodeLiveness`, added 2026-08-03) degrades
  the same way, with the same fix. An older manager answers
  `Unimplemented`, and the sender counts
  `legion_liveness_digest_rpcs_total{outcome="unimplemented"}` and falls
  back to forwarding each heartbeat as a replicated write. That is correct
  but slower. It clears as soon as status-lane leadership moves to an
  upgraded manager.
  **Downgrade all managers together.** A partial downgrade that leaves an
  old manager leading the status lane while a new one holds the display
  checkpoint is the one unsafe combination. It resolves within one lost
  window. See [node-liveness.md](node-liveness.md).
- The root key fetch (`FetchRootKey`, see
  [kv-encryption.md](kv-encryption.md#multi-manager-raft-clusters)) asks the
  leader first, then the other members. An older manager answers
  `Unimplemented` and the caller moves on to the next member, or retries on
  its next keyring tick. It succeeds as soon as any upgraded member holds
  the key.
- Adaptive heartbeat cadence is on by default, and the leader relays its
  served interval to the other managers on the liveness digest reply. An
  older manager neither relays nor serves an interval, so workers attached
  to it keep their worker-type `heartbeatSeconds` until it is upgraded. The
  leader still widens their lost windows, so nobody is marked lost for it.
  A cluster that upgrades from a release where adaptive cadence was off
  sees no change up to 2,500 nodes. Set `heartbeat = { adaptive = false }`
  before upgrading to keep the fixed cadence at any size. See
  [heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md#upgrading-to-adaptive-by-default).
- Payloads inside the transport are versioned by `PROTOCOL_VERSION`.
  Payloads that cannot be decoded produce an error that names mixed
  manager protocol versions.
- Plan submissions carry ranked candidate lists as a format-2 row
  ([late-binding-plan-apply.md](late-binding-plan-apply.md)) that
  pre-candidate managers cannot parse. The standard upgrade order covers
  this. If you keep a mixed-version manager fleet longer than an upgrade
  window, set `schedulerSettings.plan_candidates = 0` until every peer is
  on the same version. That turns off format-2 emission entirely.
- The KV encryption keyring ([kv-encryption.md](kv-encryption.md)) adds a
  table and a column that a pre-keyring manager cannot apply. Unlike other
  schema additions, the leader would write to them on its own at startup.
  So the leader gates itself. Every transport reply carries the answering
  manager's schema version, and the keyring is not initialized (no
  encrypted row is written) until every voter runs a build that has it.
  `legion keyring status` reports `pending upgrade` and names the managers
  still to upgrade. KV values are stored as before until then. There is
  nothing to set or run. Finish the rolling upgrade.
  **Downgrading** past this release afterward leaves a database ahead of
  the binary, which the manager refuses to open. Run
  `legion keyring rewrap --plaintext` first, then restore a pre-upgrade
  backup.

### Rolling upgrades across a schema change

Raft entries are SQLite changesets: the exact rows a write changed, with
the columns the leader's schema has. A manager on an older release cannot
hold rows for a column or table its schema does not have yet. SQLite itself
does not fail on that. It skips the rows and reports success, so without a
guard an older manager would fall silently out of step with the cluster.
Legion refuses instead, in two places:

- **The leader refuses the write.** Every transport reply carries the
  answering manager's schema version (`x-legion-schema-version`). Before it
  proposes a write, a leader checks it against the schema of every member
  that reports an older version. A write that member could not apply fails
  on the leader, nothing is committed, and the error names the member:

  ```
  raft op put_kv needs a schema that manager mgr-c does not have yet (table
  `kv_entry` has 6 columns where the change has 7 on schema version 127;
  this leader runs schema version 129). The write was not applied anywhere.
  Finish the rolling manager upgrade (docs/upgrades.md), or move leadership
  to a manager on the older release until then
  ```

  Writes that only touch tables the release did not change go through. A
  member that runs a build from before the schema-version header counts as
  behind on every table, so a newer leader refuses every write until it is
  upgraded.
- **An older manager refuses the entry.** If an entry it cannot apply
  reaches it anyway (for example, it was down when the leader checked), the
  older manager stops that raft lane with an error that says to upgrade it.
  The entry is not applied, so after the upgrade and restart the lane
  resumes from it with nothing lost. A member stopped this way does not
  count toward quorum until it is upgraded.

An older leader's writes always apply on newer managers, because a newer
schema only adds columns and tables. That gives the safe order:

1. Upgrade the followers one at a time. Wait for each to rejoin and catch
   up (`legion raft status`) before the next.
2. Upgrade the leader last. Leadership moves to an upgraded manager, and by
   then every member runs the new schema.

If an upgraded manager takes leadership while an older one is still
running, writes that need the new schema fail with the error above until
you finish the upgrade. Nothing diverges in the meantime.

## Raft persistence upgrades

### Enabling raft on an existing manager (local_disk → raft)

On the **first** raft start (no raft data dir yet) with an existing,
non-empty `store.sqlite`, the existing database becomes the initial state
machine content. There is no import step or seed tooling. Set
`persistence.type = "raft"` with `raft.bootstrap = true` on the existing
manager and start it. It elects itself leader of a single-voter cluster,
and every later change is a replicated raft entry applied on top of the
adopted state. Jobs, allocations, KV, ACLs, and everything else the manager
had carry over unchanged.

The same applies when you upgrade from the old simulated raft backend. It
was leader-election bookkeeping over a local SQLite store, so there is no
replicated state to migrate, only the local DB, which bootstrap adopts.
Mixed clusters (simulated raft and real raft managers) are not supported.
Bring the cluster up fresh from the bootstrap node.

### Growing the cluster

Additional managers join with a token (`legion raft join-token`, see
`docs/raft-operations.md`). A joiner catches up from the leader through
**log replay** when the needed entries are still in the leader's log, and
through **snapshot install** otherwise. For a snapshot install, the leader
ships a whole-database snapshot image (a `VACUUM INTO` copy of its state
DB, which includes any adopted pre-raft baseline). The joiner atomically
swaps it in as its state DB, and replication resumes from the snapshot's
applied index. You never need to copy a DB by hand, at any history length.

In the default `single` topology, snapshots are taken every
`raft.snapshotEntries` applied entries (default 10 000), and the raft log
is compacted behind each snapshot. A tail of recent entries is kept so
slightly-behind followers can catch up by replay. Bootstrap also builds a
**seed snapshot** of the existing database and compacts the log up to it,
so a joiner always receives any adopted pre-raft baseline through snapshot
install instead of incomplete log replay. Under the `lanes` topology the
same policy applies **per lane**, with per-lane table-scoped snapshots. The
topology is fixed at first engine start per data dir. To switch (`single` ↔
`lanes`), take the backup → fresh cluster → restore path below (see
`docs/multi-raft.md`).

### Backup and restore

**Backup** (`legion state backup [--out <path>]`, or the
`Manager/GetStateBackup` RPC, `operator:write`): the manager writes a
consistent point-in-time copy of its state store to a **server-side** path
with `VACUUM INTO`. The default is a timestamped file under
`<storePath>/backups/`. The manager reports the path, size, and the raft
applied index the backup captures. Backups work in every persistence mode.
In raft mode, only the leader takes them, and a non-leader answers
`FAILED_PRECONDITION` with the leader's name. The backup is a plain SQLite
database, so copy it off-host with ordinary tools.

**KV values in a backup are encrypted, and the root key is not in it.**
The reply (and the CLI's last line) name the root key's provider and
location. `--restore-from` on a host without the key refuses to serve until
you copy the key into place. Keep the key with the backup. See
[KV Encryption at Rest](kv-encryption.md).

**Restore** (`legion start manager --restore-from <backup>`): before the
store (and any raft engine) starts, the backup is copied into place as
`<storePath>/store.sqlite`. By default this only happens when no state DB
exists yet. Pass `--force-restore` to overwrite an existing one. The
restored database's raft bookkeeping (`raft_applied` and the
`raft_cluster` identity stamp) is cleared, so:

- in `local_disk` mode, the manager starts from the backup's state.
- in raft mode, the restored state becomes the **adopted baseline of a
  fresh cluster**. Restore onto the bootstrap node (`raft.bootstrap = true`, a fresh `raft.dataDir`), start it, then join the other managers,
  which receive the restored state through snapshot install. Do not restore
  a backup into a member of an existing cluster. Seed a fresh cluster
  instead (the fresh-bootstrap rule above).

### Cluster identity guard (`forceNewCluster`)

At first raft engine start, the state DB is stamped with the cluster's
identity (derived from the cluster secret). A later start with a different
identity is **refused**. This usually happens when `raft.dataDir` was wiped
(cluster secret included) and `bootstrap = true` minted a fresh secret.
Bootstrapping a new cluster over an existing cluster's data would cause
split-brain with any surviving members. Restore the original
`raft.dataDir`/`clusterSecret`, or set
`persistence.raft.forceNewCluster = true` to re-stamp the DB once the old
cluster is fully retired. `--restore-from` clears the stamp along with
`raft_applied`, because a restore is an explicit "seed a fresh cluster"
action. The fresh-bootstrap restore flow above needs no override.

### Rollback

To leave raft mode, stop the managers and keep one node's `store.sqlite`
(any converged replica, or a `legion state backup` file). Set
`persistence.type = "local_disk"` on that node, and delete or ignore
`raft.dataDir`. Committed state is a plain SQLite database at every point
in time.
