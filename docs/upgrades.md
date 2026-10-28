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

## Restarting a worker in place

A worker restarted in place re-adopts the tasks it left running, so their
processes keep running through the upgrade (see
[Running a worker under a service manager](architecture-managers-workers.md#running-a-worker-under-a-service-manager)
for the settings that make that work). Their network connections do not
all survive it.

The node's datapath runs inside the worker process. That covers the
[`connect`](service-discovery.md#connecting-to-services-the-connect-block)
upstream listeners a task dials, and the ingress listeners, TLS termination
and [handlers](network-handlers.md) in front of its ports. When the worker
process exits, every connection passing through them is severed, and new
dials fail until the restarted worker re-registers and its first heartbeat
brings back the endpoint feed. A task's own listeners, and connections that
reach them without passing through the datapath, are unaffected.

So a task that keeps a long-lived connection through the mesh sees it drop
on every worker restart, even though the task itself never restarted:

- **Reconnect in the app.** Treat a closed or reset upstream connection as
  retryable, and re-dial with backoff. Most database pools and gRPC channels
  do this already. An app that opens one connection at startup and never
  re-dials stays broken until it is restarted or rescheduled, as a web app
  holding a single database connection through `connect` does.
- **Drain first when that is not acceptable.** `legion node drain <node>`
  moves the allocations to other nodes before you restart the worker, and
  `legion node uncordon <node>` returns the node afterward.

A worker keeps some state as JSON files in its state directory, and some of
them hold API messages as written by the binary that saved them. A newer
binary cannot yet read such a file when a field was added to the message
since, and it fails naming the field, for example `missing field
envelopeResources` reading `netd_state.json`. This is a known gap.

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

Stopping a manager does not hand off raft leadership: the remaining managers
elect a new leader after their election timeout, the same as after a crash,
and writes to lanes the stopped manager led pause for that window. To avoid
the pause when stopping the leader, run `legion raft command step-down` (or
`transfer-leader --target <manager>`) first; see
[raft-operations.md](raft-operations.md#moving-leadership). Restart managers
one at a time and wait for `legion raft status` to show a leader before the
next. A restarted manager rejoins as a follower without forcing an election.
Workers move to another manager while one is down
([Managers a worker can reach](architecture-managers-workers.md#managers-a-worker-can-reach)).

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

CI guards the wire contract. Every pull request runs
[`buf breaking`](https://buf.build/docs/breaking/) with the `WIRE` rules in
`proto/buf.yaml`, comparing `proto/legion.proto` against the base branch. It
fails on renumbered, retyped, or deleted fields (a removed field's number
must be `reserved`) and on removed RPCs. Renaming a field passes, since only
its number goes on the wire. For a deliberate break, bump `PROTOCOL_VERSION`
and label the PR `proto-breaking-ok`.

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
| 4 | One datapath fetch: `FetchDatapathMaterial` replaces `FetchNetPolicy`, `GetIngressCertificates`, and `GetUpstreamCredentials`. The mesh is `service.mesh`, and TLS that is not the mesh lives on `network.ports[].tls`, read off the allocation's ports. Allocations carry the task's group and service, so a mesh caller is named by its real group ([Network Handlers](network-handlers.md#the-mesh)) | A protocol-3 worker calls the three removed RPCs, which a protocol-4 manager no longer serves: it keeps the policy bodies, certificates, and credentials it already holds, a new allocation on it gets none (a gated one stays closed), and it does not terminate mesh or port TLS for allocations placed after the manager upgraded. Upgrade workers right after the managers |
| 3 | The authenticated worker plane: the worker serves `Worker` over mutual TLS with a node certificate it requested by CSR (the `IssueNodeCertificate` reply no longer carries a key), binds all interfaces, and advertises a reachable `https://` address ([Managers and Workers](architecture-managers-workers.md#the-worker-plane)). Join tokens are format v2 and pin the cluster CA | Managers never dial a worker in plaintext. A protocol-2 worker keeps registering, heartbeating, and running the work its heartbeat replies carry, but exec, actions, logs, and restart against it fail with an error naming its protocol version until it is upgraded. Push dispatch to it is skipped (the heartbeat delivers the work one interval later), and its mesh certificate requests are refused (no CSR), so mesh flows on that node stop until it is upgraded. Join tokens minted by an older manager (format v1) are refused; mint new ones. Upgrade workers promptly after the managers, and allow TCP 4848 from every manager (and edge router) to every worker first |
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

`TaskAllocation.task_speculative_start` carries a task's `speculativeStart`
block into the node's spec cache. While either side predates it, a node
reads every cached task as `off` and refuses its boot tickets with `task
<name> does not permit speculative starts`. That also fails closed, and it
ends once both sides are upgraded and the node is dispatched the task again.

Neither the `boot` ACL resource nor the `edgeBoot` config blocks need a
coordinated upgrade. Both are off by default, and a manager that does not
know them ignores them.

## Snapshots under skew

[Snapshots](suspend-and-resume.md) are gated on a worker **capability**, not
on the protocol version. A worker that predates them does not list
`checkpoint` in its registration, so the manager never sends it checkpoint
work, and `legion snapshot create` against one of its instances fails with
`FailedPrecondition` naming the worker's version. Upgrade the worker and the
same command works. An old manager rejects the snapshot RPCs as unknown, and
old workers ignore the new heartbeat fields.

Two upgrades change what a snapshot can do afterwards:

- **A VMM upgrade on a node** strands its memory snapshots: a memory snapshot
  resumes only on the VMM version it was taken on, and placement refuses a
  restore with `sandbox.vmm.<name>.version is ..., the checkpoint needs ...`.
  Take a fresh snapshot after the upgrade, or restart instances cold. Disk
  snapshots are unaffected.
- **Downgrading a manager** below the snapshot schema leaves `snapshot` rows
  and the `instance.restore_from` column it does not read. Let pending
  restores finish before downgrading.

## Suspend under skew

[Suspend](suspend-and-resume.md) rides the same `checkpoint` capability as
snapshots. `legion instance suspend` against an instance on an older worker
fails with `FailedPrecondition` naming the worker's version, and the manager
never sends such a worker suspend work or a resume's restore payload.

- **Resume or stop suspended instances before a manager downgrade.**
  `suspended` is a new instance state. A manager that predates it reads an
  instance state it does not know, and a suspended instance's checkpoint is
  invisible to it. Run `legion instance list <job>` per job, then `instance
  start` (resume) or `instance stop` (discard the checkpoint) every
  suspended instance first.
- **Drain suspended instances off a node before upgrading its VMM.** A memory
  checkpoint restores only on the VMM version it was taken on. After a VMM
  upgrade, a resume of an older checkpoint fails placement on the `compat`
  check, and `onRestoreFailure` decides: `cold` boots it fresh on its
  retained storage after `resumeTimeout`, `fail` leaves it suspended. Resume
  the node's suspended instances before the upgrade (and suspend them again
  after it if you want), or accept the cold boot.
- **The allocation table is rebuilt once** on the upgrade that adds suspend,
  so allocation ids are never reused afterwards. The rebuild copies every
  row and runs in the migration, before the manager serves requests.

## Cluster CA under skew

Heartbeat replies name the cluster CA's whole trust bundle
(`HeartbeatReply.cluster_ca_pins`, which replaced the single
`cluster_ca_pin`; see [The Cluster CA](cluster-ca.md#how-workers-learn)).
Between a worker and a manager on either side of that change, the field the
other side reads is empty, which a worker treats as "no news": it keeps its
certificate and re-issues on its own 8-hour timer instead of within one
heartbeat of a CA change. Nothing breaks, but finish the rollout before you
rotate the cluster CA, or a CA change reaches the older nodes only on that
timer.

Every manager in a raft cluster needs the same `clusterCa` block. A manager
upgraded with a different one (or without one, while the others use `file`
or `exec`) refuses to sign and says why in `legion net ca show`
([Every manager must agree](cluster-ca.md#every-manager-must-agree)).

## Durable checkpoints under skew

Publishing (`suspend.durability = "store"`, `snapshot create --publish`) is
gated on a second worker capability, `content-publish`, which a worker lists
only when its `contentStore` is configured and usable. A store suspend on a
node without it fails with `FailedPrecondition` naming the node, before
anything is captured.

- **Upgrade workers before you rely on cross-node resume.** A worker that
  predates published snapshots cannot fetch a part from the store before a
  restore: it fails the restore (`... is missing or corrupt in this node's
  content store`), and `onRestoreFailure` decides what happens. Placement
  does not yet know which workers can fetch, so a mixed fleet can place a
  published resume on an old worker.
- **Downgrading a manager** leaves the `snapshot.durability` and
  `published_at` columns it does not read, and it reads a `published`
  snapshot's state as unknown. Resume, or stop, instances with published
  checkpoints first.
- **Chunked parts and whole-object parts do not mix.** Workers from the
  release that publishes parts as chunks ([How published parts are
  stored](suspend-and-resume.md#how-published-parts-are-stored)) cannot open
  a part an older worker published as one object, and older workers cannot
  open a chunked part. Both fail the fetch with `failed authentication`, and
  `onRestoreFailure` decides what happens. Upgrade every worker that shares
  a `contentStore` together, and resume published instances on the node
  that took them (which needs no fetch) or let them boot cold.
- **A VMM upgrade strands published memory checkpoints too.** A published
  checkpoint resumes on any node with the VMM version and CPU it was taken
  on, so upgrading the whole fleet's VMM leaves it nowhere to run. Upgrade a
  subset first, or resume the instances before the last old node goes.

## Unnamed instance ids

Server-minted keyed ids changed shape and guarantee in the release that
added unnamed instances ([Id rules](stateful-workloads.md#id-rules)):

- **The never-recycled guarantee for server-minted ids is now
  probabilistic.** A minted id is `<task>-<26 base32>` (130 random bits) and
  `instance destroy` deletes its row instead of keeping a tombstone. Ids you
  choose keep their tombstones exactly as before.
- **Nothing existing is released.** The upgrade marks every existing
  instance row named, including tombstones of ids minted by an older
  release (`<task>-<16 alphanumerics>`), so they keep refusing re-creation.
  Only instances minted after the upgrade are unnamed.
- **A caller id may no longer end in `-` plus 26 characters of `[a-z2-7]`.**
  That shape is reserved for minted ids. Scripts that generate ids of that
  shape themselves get `InvalidArgument`; drop the id to have one minted,
  or change the generator.
- **Downgrading a manager** leaves the `instance.named` column it does not
  read, and an older manager tombstones every destroy again.

## Archived instances

The release that added [archived instances](suspend-and-resume.md#archiving-idle-instances)
(`suspend.archive`) changed one output shape and added one column:

- **`legion instance list -j` returns an object.** The data was a bare
  array of instances. It is now `{ "instances": [...], "nextIdlePageToken":
  "..." }`, so `--include-idle` can page. Scripts that read `.data[]` read
  `.data.instances[]`.
- **The `instance.manifest_url` column** is appended by the upgrade.
  Nothing is archived until a job sets `suspend.archive`.
- **Downgrading a manager** loses archived instances: an older manager does
  not read `manifest_url`, so a slim archived instance looks like a
  suspended instance whose checkpoint is gone (`onRestoreFailure` decides),
  and a `manifestOnly` instance, which has no row, is not found. Start the
  archived instances you need before a downgrade.

## Session mode

The release that added [session mode](session-mode.md) changed these
shapes:

- **Managers first, as always.** A worker advertises the `sessions`
  capability only once it runs a version with session support, and
  envelopes are placed only on workers that advertise it. A session job
  submitted while every worker is older places no envelope, and creates
  answer `FailedPrecondition` until a worker upgrades.
- **Heartbeats carry a session report**, and replies may ask for a resync
  or name sessions to stop. An older manager ignores the report, so a
  worker upgraded first runs sessions nobody indexes; upgrade managers
  first.
- **`legion instance list -j` adds `complete` and `asOf`**, and
  `instance create -j` adds `mode`, `durable`, `node`, and `state`. Existing
  fields are unchanged.
- **The `instance.promoted_from` column** is appended by the upgrade.
  Downgrading a manager loses it, and a promoted instance reads as an
  ordinary keyed instance.
- **Downgrading a worker** ends its running sessions: an older worker does
  not know them. Promote the sessions you need first.

## Namespaces

The release that added [namespaces](namespaces.md) changed these shapes. A
cluster that never creates a namespace keeps every job in `default` under
its bare name, so job ids, and every key derived from one, are unchanged for
it.

- **ACL policies must say their reach.** An allow rule over a namespaced
  resource (`job`, `exec`, `action`, `scale`, `boot`, `snapshot`,
  `namespace`) without `namespaces` is refused when the policy is written,
  and so is `namespaces` on a rule over only cluster-scoped resources.
  Policies stored before the upgrade are not rewritten: re-apply each one with
  `namespaces = ["default"]` (what they meant) or `["*"]` (every namespace,
  including future ones) before relying on them for namespaced jobs. Until
  then a stored allow rule that names namespaced resources without
  `namespaces` matches `default` jobs only, which is what it meant before:
  it never widens to a namespace created later. A `resources = ["*"]` rule
  and a deny rule without `namespaces` match every namespace.
- **The default workload-identity subject gains `{namespace}`**:
  `{region}:{namespace}:{job}:{group}:{task}` (`auto:default:api:web:server`).
  Trust policies that match the old `auto:api:web:server` must be updated.
  A configured `subTemplate` without `{namespace}` stops the manager from
  starting; add it.
- **Content-store paths spell the namespace out.** Archive manifests move
  from `manifests/<job>/…` to `manifests/<namespace>/<job>/…`, session
  manifests from `sessions/<job>/…` to `sessions/<namespace>/<job>/…`, and
  fence markers from `sessions/fence/` to `sessions/_fence/`. Objects written
  under the old paths are not found by the new release: resume the archived
  instances and store sessions you need before upgrading.
- **List RPCs default to `default`.** `ListJobs`, `ListServices`,
  `ListRollouts`, `ListVolumes` and `ListSnapshots` with an empty `namespaces`
  list only `default` (`["*"]` lists every readable namespace), unless a
  request names a job.
- **Service `allow` entries without a `/` match the service's own
  namespace**, and an absent `allow` admits only its own namespace.
- **Exec-key `jobs` patterns without a `/` match `default` only.**
- **Endpoint feeds carry the upstream key**, so an aliased service upstream
  (`{ name = "db", service = "postgres" }`) now receives endpoints; it did not
  before.
- **Downgrading a manager** past this release loses namespaces and namespace
  policies (their tables are not read), and jobs in other namespaces keep
  their qualified ids, which an older manager treats as plain names.

## Strict jobspec fields

The release that made [unknown jobspec
fields](jobs-tasks-and-scheduling.md#unknown-fields) errors changed this
behavior on purpose:

- **A spec carrying a stray or misspelled key now fails to submit.** Before,
  the key was accepted and silently ignored. Run `legion job validate` over
  your specs before upgrading the managers: each error names the key, its
  path, and the closest known key. Fix the spec, or pass
  `--allow-unknown-fields` to submit it as before while you do (keys on the
  job, a task, a volume, a port, or a service are kept as pending, the rest
  are dropped, and both are reported and recorded on the version).
- **Blocks that were already strict now report every key the same way.**
  `volumes`, `stateful`, `stateful.storage`, `stop`, `disruption`,
  `actions`, `network.ports[].tls`, `connect.upstreams`, `speculativeStart`,
  `snapshots`, `suspend`, and the session blocks used to fail on the first
  unknown key with serde's message; they now report every unknown key with
  its path and a suggestion, and the flag can drop (or, on a volume, keep)
  them.
- **Stored jobs are unaffected.** A stored config never carried an unknown
  key (the old parser dropped them before storing), so reading, re-rendering,
  and restarting existing jobs works unchanged.

Under skew:

- **A manager older than this release drops unknown keys silently**, as it
  always did, and ignores `allow_unknown_fields`. Nothing records what it
  dropped. Upgrade the managers before relying on the error.
- **The CLI's `legion job validate` checks locally**, with the parser
  compiled into the CLI binary. An older CLI does not catch unknown keys; a
  newer CLI catches them even against an older manager.
- **A newer CLI's `--allow-unknown-fields` against an older manager** is
  harmless: the manager ignores the field and drops the keys, as before.
- **Downgrading a manager** past this release ignores the task- and
  job-level `pendingExtensions` maps and the recorded warnings (nothing acted
  on them anyway), but an older manager cannot read a version whose volumes
  carry pending keys: its volume parser refuses unknown keys. Resubmit those
  jobs without `--allow-unknown-fields` before downgrading.

## Task-plugin ABI

Task runtimes loaded from `plugins/tasks/<name>/` are **not** covered by
`protocol_version`. They are dynamically linked into the worker and must be
rebuilt against the Legion version they run under. Each plugin declares the
vtable generation it was compiled against. A mismatch (or a missing
declaration) is refused at load with a message that tells you to rebuild.
See [task-runtime-plugins.md](task-runtime-plugins.md). Rebuild out-of-tree
task plugins as part of any worker upgrade.

Every dylib plugin kind (tasks, scheduling, rollouts, node checks, artifact
fetchers) also carries a build fingerprint: the `rustc` version, the `legion`
version, and the layout of the types it shares with the host. Any upgrade that
changes the `legion` version or the toolchain changes the fingerprint, so
rebuild every dylib plugin with the new binary, managers included (rollout
and scheduling plugins load there). A stale plugin is refused at load instead
of crashing the process. See
[plugin-system.md](plugin-system.md#build-fingerprint-required-for-dynamic-plugins).

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
