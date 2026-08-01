# Upgrades

How to upgrade a Legion cluster safely: the order to upgrade in, what the
version handshake enforces, and how to observe version skew across the fleet.

## Upgrade order: managers first, then workers

Always upgrade in this order:

1. **Managers.** Upgrade every manager to the new release (rolling, one at a
   time in an HA deployment).
2. **Workers.** Once all managers run the new release, upgrade workers at
   whatever pace suits the workloads (drain + replace, or restart in place —
   see `legion node drain`).

This order is what the protocol handshake (below) enforces: managers are
built to stay compatible with all *older* worker protocol versions up to
their own, but they cannot guarantee they satisfy a contract *newer* than
they know about. A worker that is upgraded before its managers is rejected
at registration.

## The `protocol_version` contract

Workers and managers exchange versions during registration
(`WorkerRegister` in `proto/legion.proto`):

- The worker sends `build_version` (its Cargo package version, e.g.
  `"0.1.0"`) and `protocol_version` (the wire-contract version it speaks,
  `PROTOCOL_VERSION` in `src/constants.rs`).
- The manager replies with `manager_build_version` and
  `manager_protocol_version`.

`protocol_version` is bumped **only** for breaking wire-contract changes to
worker-plane RPCs (registration, heartbeats, health checks, work dispatch):
removed/renamed fields, changed field semantics, or new required behavior an
old peer cannot satisfy. Additive, backwards-compatible fields do not bump
it.

The manager's admission decision at registration:

| Worker `protocol_version`     | Manager decision                                        |
| ----------------------------- | ------------------------------------------------------- |
| `0` (pre-versioning build)    | Accepted, with a warning log — upgrade this worker      |
| `1 ..= manager's version`     | Accepted                                                |
| `> manager's version`         | Rejected with `FAILED_PRECONDITION`                     |

A `0` arises because proto3 decodes a missing field as the zero value:
workers built before the handshake existed simply do not send the field.
They are admitted so a rolling upgrade *from* the current release works —
the warning is the nudge to finish the rollout.

A rejection means the upgrade order was inverted: the worker was upgraded
before the manager. The error message says exactly that — upgrade all
managers first, then re-join the worker. Heartbeats are deliberately left
out of the handshake to keep that hot path lean; the check runs once per
registration.

## What each protocol version added

| Version | Added | Behavior with an older worker |
| --- | --- | --- |
| 2 | Workers report task-process exits (`terminated_allocations` on the heartbeat), which is what makes a batch task's `Success` come from an exit code — see [Task Supervision](task-supervision.md) | The manager sees the node's advertised `protocolVersion` and keeps the pre-2 behavior for batch tasks placed there: the task is marked `Success` at placement, as it always was. Nothing stalls mid-rollout; new placements get the honest behavior once the worker is upgraded. |
| 1 | The versioned handshake itself | Workers reporting `0` are pre-versioning builds and are accepted with a warning |

## Graceful stop and preemption under skew

Neither feature bumps `protocol_version`, and both degrade rather than fail —
but the degradation is worth knowing before you enable `enforce`:

- **An old worker ignores the stop ladder** and hard-kills as it always did.
  Its tasks get no signal, no grace, and no
  `LEGION_STOP_NOTICE_PATH` file. Preemption still works; victims are just
  killed abruptly ([graceful-stop.md](graceful-stop.md)).
- **An old worker does not order stops before starts**, so a new manager can
  place a preemptor there while the victim is still running. That is a
  transient oversubscription, not merely a graceless stop — a container task
  can briefly hold memory the preemptor was placed against.

Both are reasons to **upgrade workers before enabling
`schedulerSettings.preemption.mode: enforce`** on a mixed fleet, or to run
`observe` until the fleet is uniform
([preemption-and-eviction.md](preemption-and-eviction.md)). Preemption is
deliberately not gated on a minimum worker protocol version: a gate would make
it silently inert on any fleet with one stale worker, which is worse than a
documented window.

Priority bands need nothing from the worker at all — they live entirely in the
manager's planner and eval queue — so they are unaffected by worker skew.

## Task-plugin ABI

Task runtimes loaded from `plugins/tasks/<name>/` are **not** covered by
`protocol_version`; they are dynamically linked into the worker and must be
rebuilt against the Legion version they run under. Each declares the vtable
generation it was compiled against, and a mismatch (or a missing declaration)
is refused at load with a message naming the rebuild. See
[task-runtime-plugins.md](task-runtime-plugins.md). Rebuild out-of-tree task
plugins as part of any worker upgrade.

## Build-version skew expectations

`build_version` is informational and never causes rejection — only
`protocol_version` gates admission. Expectations:

- **Same minor version** (e.g. managers `0.4.2`, workers `0.4.1`): fine.
  This is the normal state mid-rollout.
- **Different minor/major versions**: works as long as the protocol version
  is compatible, but the worker logs a warning at registration when its
  build version differs from the manager's. Treat a fleet that warns for
  more than the duration of a rollout as a signal to finish the upgrade.

## Observing fleet versions

Registration stores each worker's versions into the node's dynamic
attributes, so skew is visible from the control plane:

```sh
legion node list -j | jq '.data[] | {id, dynamicAttrs}'
```

Each node carries `buildVersion` (the worker's build version, or
`"unknown"` for pre-versioning workers) and `protocolVersion` (`0` for
pre-versioning workers) in `dynamic_attrs`. The same attributes appear in
the node ledger and the UI's node views. Workers additionally log the
manager's build/protocol version after every successful registration.

## Manager-plane skew (raft clusters)

Managers in one raft cluster speak the `RaftTransport` service to each
other. During a rolling manager upgrade the plane tolerates skew:

- The lane-write forwarding RPC (`ForwardLaneWrite`, added 2026-07-28)
  degrades cleanly against an older manager: the forward fails with the
  older side's "unimplemented" status and the caller surfaces the same
  typed leader-hint rejection it would have produced before forwarding
  existed. Upgrade managers first (the standard order below) and the
  window closes.
- Payloads inside the transport are versioned by `PROTOCOL_VERSION`;
  undecodable payloads name mixed manager protocol versions in their
  error.
- Plan submissions carry ranked candidate lists as a format-2 row
  ([late-binding-plan-apply.md](late-binding-plan-apply.md)) that
  pre-candidate managers cannot parse. The standard same-version upgrade
  order covers this; for a mixed-version manager fleet held longer than
  an upgrade window, set `schedulerSettings.plan_candidates: 0` until
  every peer is level — it holds format-2 emission off entirely.

## Raft persistence upgrades

### Enabling raft on an existing manager (local_disk → raft)

On the **first** raft start — no raft data dir yet — with an existing,
non-empty `store.sqlite`, the existing database simply **becomes the
initial state machine content**. No import step, no seed tooling: set
`persistence.type: 'raft'` with `raft.bootstrap: true` on the existing
manager and start it. It elects itself leader of a single-voter cluster,
and every subsequent mutation is a replicated raft entry applied on top of
the adopted state. Jobs, allocations, KV, ACLs — everything the manager
had — carries over untouched.

The same applies when upgrading from the old simulated raft backend: it
was leader-election bookkeeping over a local SQLite store, so there is no
replicated state to migrate — only the local DB, which bootstrap adopts.
Mixed clusters (simulated raft + real raft managers) are not supported;
bring the cluster up fresh from the bootstrap node.

### Growing the cluster

Additional managers join with a token (`legion raft join-token`; see
`docs/raft-operations.md`). A joiner catches up from the leader via **log
replay** when the needed entries are still in the leader's log, and via
**snapshot install** otherwise: the leader ships a whole-database snapshot
image (a `VACUUM INTO` copy of its state DB, which includes any pre-raft
adopted baseline), the joiner atomically swaps it in as its state DB, and
replication resumes from the snapshot's applied index. No manual DB
copying is needed at any history length.

Snapshots are taken every `raft.snapshotEntries` applied entries (default
10 000) in the default `single` topology, and the raft log is compacted
behind each snapshot (a tail of recent entries is retained so
slightly-behind followers catch up by replay). Bootstrap additionally
builds a **seed snapshot** of the existing database and compacts the log
up to it, so a joiner always receives any pre-raft adopted baseline via
snapshot install rather than incomplete log replay. Under the `lanes` topology
the same policy applies **per lane**, with per-lane table-scoped
snapshots; the topology is fixed at first engine start per data dir, and
switching (`single` ↔ `lanes`) takes the backup → fresh cluster →
restore path below (see `docs/multi-raft.md`).

### Backup and restore

**Backup** (`legion state backup [--out <path>]`, or the
`Manager/GetStateBackup` RPC, `operator:write`): the manager writes a
consistent point-in-time copy of its state store to a **server-side**
path via `VACUUM INTO` (default: a timestamped file under
`<storePath>/backups/`) and reports the path, size, and the raft applied
index the backup captures. Works in every persistence mode; in raft mode
it is leader-only (a non-leader answers `FAILED_PRECONDITION` naming the
leader). The backup is a plain SQLite database — copy it off-host with
ordinary tooling.

**Restore** (`legion start manager --restore-from <backup>`): before the
store (and any raft engine) starts, the backup is copied into place as
`<storePath>/store.sqlite`. By default this only happens when no state DB
exists yet; pass `--force-restore` to overwrite an existing one. The
restored database's raft bookkeeping (`raft_applied` and the
`raft_cluster` identity stamp) is cleared, so:

- in `local_disk` mode the manager simply starts from the backup's state;
- in raft mode the restored state becomes the **adopted baseline of a
  fresh cluster** — restore onto the bootstrap node (`raft.bootstrap:
  true`, a fresh `raft.dataDir`), start it, then join the other managers,
  which receive the restored state via snapshot install. Do not restore a
  backup into an existing cluster's member: seed a fresh cluster instead
  (the fresh-bootstrap rule above).

### Cluster identity guard (`forceNewCluster`)

At first raft engine start the state DB is stamped with the cluster's
identity (derived from the cluster secret). A later start whose identity
does not match — typically a wiped `raft.dataDir` (cluster secret
included) plus `bootstrap: true` minting a fresh secret — is **refused**:
bootstrapping a new cluster over an existing cluster's data would
split-brain against any surviving members. Restore the original
`raft.dataDir`/`clusterSecret`, or set
`persistence.raft.forceNewCluster: true` to deliberately re-stamp the DB
once the old cluster is fully retired. (`--restore-from` clears the stamp
along with `raft_applied` — a restore *is* an explicit "seed a fresh
cluster" action — so the fresh-bootstrap restore flow above needs no
override.)

### Rollback

To leave raft mode, stop the managers, keep one node's `store.sqlite`
(any converged replica — or a `legion state backup` file), set
`persistence.type: 'local_disk'` on that node, and delete/ignore
`raft.dataDir`. Committed state is a plain SQLite database at every point
in time.
