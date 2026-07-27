# Plan: Volume Plugin System

> **Status:** IMPLEMENTED (v1). User doc: [docs/volumes.md](../volumes.md).
> Guides: [Postgres](../guides/postgres.md), [Redis](../guides/redis.md).
>
> This plan is the follow-up that the
> [Stateful Workloads & Instance Identity plan](stateful-workloads-and-instance-identity-plan.md)
> deferred to: that plan defines the *binding* (instance → storage handle,
> reattach-on-reschedule); this plan defines the *drivers* that make a handle
> real, plus the mount pipeline, scheduling constraints, and operator surfaces.
>
> The design below is the **revised** plan. Where it departs from the original
> proposal the reasoning is recorded inline under **★ Judgement call** markers.

## Problem Statement

Stateful instances (slot or keyed) carry a stable `storage_handle` that must be
materialized into a real, mounted path before the task starts, on whatever node
the instance lands on. Before this work Legion had no storage drivers at all —
the handle was an opaque `local-dir/<random>` token and nothing provisioned,
mounted, or reclaimed anything.

Hard-coding one storage backend would be the wrong move: storage is the most
environment-specific part of any scheduler (a laptop wants a local directory, a
bare-metal fleet wants an NVMe mount point or ZFS, a cloud fleet wants EBS/PD
via CSI). The right shape is a **volume plugin system**: a small, stable
contract between Legion and drivers, in the spirit of CNI's exec-plugin model
and Nomad's host/CSI volumes.

## Goals

- **A minimal, versioned plugin contract** covering the whole lifecycle a v1
  scheduler actually needs: capability probing, mount, unmount, destroy.
- **Exec-based plugins as the extension ABI** (CNI-style): a plugin is any
  executable that reads a JSON request on stdin and writes a JSON response on
  stdout. Language-agnostic, crash-isolated, testable from a shell.
- **In-process built-ins for the zero-dependency path**: `local-dir`, `host`,
  `nfs`, and `tmpfs` ship inside the Legion binary, so a single-binary cluster
  has working stateful storage with nothing installed.
- **Capability-driven scheduling**: each plugin declares whether its volumes are
  node-local or reachable from any node; the scheduler pins node-local
  instances and filters nodes that lack the plugin.
- **Volumes for stateless tasks too**: mounting a pre-existing host path
  (models, certs, a shared cache) into *any* task is a first-class use case, not
  a stateful-only one.
- **End-user simplicity**: a stateful task says *what* it needs
  (`storage: { path: '/var/lib/db', plugin: 'local-dir' }`) and never sees mount
  mechanics. The default (`plugin` omitted) just works on a laptop.

## Non-Goals

- **Re-implementing CSI inside Legion.** We define a smaller native contract; a
  `csi-bridge` exec plugin can be written against it without Legion core ever
  speaking CSI.
- **Snapshots, cloning, expansion.** The contract reserves the operation names
  (`snapshot`, `restore`, `clone`, `expand`) so they can be added without an ABI
  break; no v1 plugin implements them. `clone` is named deliberately rather than
  left for later — see revision 2.
- **Cross-node data migration.** A node-local volume pins its instance; moving
  the bytes is the operator's job.
- **Volume quotas/enforcement beyond what the driver gives us.** `tmpfs` gets a
  real size limit because the kernel enforces it; `local-dir` records `size` as
  an attribute and does not enforce it.

---

## Background

### What the stateful plan already gave us

- `Instance.storage_handle: Option<String>` — an opaque ref owned by the
  instance, minted on first placement, reattached on every later placement.
- Retention knobs: `retainOnScaleIn`, `retainOnStop`; keyed destroy drops the
  handle, keyed stop retains it.
- The at-most-one guarantee (node-loss fencing), which is a *precondition* for
  safely reattaching single-writer storage.

This plan fills in what a `storage_handle` *is* — a `{plugin}/{volume_id}`
reference backed by a `volume` row — and the drivers that act on it.

### Prior art we mirror

| System | What we take |
| --- | --- |
| **CNI** | The exec-plugin ABI: JSON on stdin, op in `argv[1]`, JSON on stdout, versioned contract, discovery from a bin dir. Easy to write a plugin for in an afternoon. |
| **Nomad host volumes** | "Operator pre-declares, jobs claim by name" → our `host` built-in. Dynamic host volumes → our `local-dir` built-in. |
| **CSI** | Access modes, idempotency-as-a-rule, and the *idea* of a compatibility bridge rather than native vendor drivers. |
| **K8s StatefulSet** | `volumeClaimTemplates` semantics (one volume per replica, reattached by ordinal) — which Legion already had via instances. |
| **Legion's plugin system** ([docs/plugin-system.md](../plugin-system.md)) | Manifest validation (`plugin.json` with kind/name/api version) and the `plugins/<kind>/<name>/` layout. Volume plugins live at `plugins/volumes/<name>/`. |

---

## Revisions to the original proposal

The original plan proposed seven operations split across manager (controller)
and worker (node), with a `volumes` table whose transitions were manager-driven.
Building it surfaced three problems, and the design changed in response.

> ★ **Judgement call 1 — collapse seven operations into four.**
> The original op set was `capabilities / create / attach / mount / unmount /
> detach / destroy`, mirroring CSI. CSI needs `stage` vs `publish` because many
> pods share one staged volume per node, and `ControllerPublish` vs `NodeStage`
> because cloud disks attach out-of-band. **Legion v1 has neither**: an instance
> has at most one live allocation by construction (the fencing invariant), so
> there is never a second consumer to stage for. Six of the seven ops collapse
> to two useful ones.
>
> v1 contract: **`capabilities`, `mount`, `unmount`, `destroy`**. `create`,
> `attach`, `detach`, `snapshot`, `restore`, and `expand` are **reserved names**
> in the ABI: a plugin advertises them in `capabilities.ops` and Legion will
> dispatch them when the feature lands, but nothing calls them today, so no
> plugin has to implement them. Provisioning is lazy-on-first-mount, which is
> what `local-dir`, `host`, `nfs`, and `tmpfs` all want anyway (and matches
> open question 1's leaning).

> ★ **Judgement call 2 — every plugin operation runs on a worker; the manager
> only owns the state machine.**
> The original plan split ops between "manager (controller)" and "worker
> (node)". But the manager cannot `mkdir` on a remote node's disk, and for every
> v1 plugin `create` and `destroy` are node-side filesystem operations. Making
> the manager a plugin host would mean loading drivers into the control plane
> and giving it storage credentials, for no benefit.
>
> Instead: **workers own the plugin registry; the manager owns the `volume`
> table, pinning, and orchestration.** The manager needs plugin *metadata* for
> admission validation — it gets that from worker fingerprints, which advertise
> each installed plugin's capabilities. A plugin no node advertises is rejected
> at submit time with a clear message. `destroy` is dispatched to the owning
> worker over the heartbeat channel (a `volume_ops` list in the reply) and
> confirmed on the next heartbeat, so it is crash-safe and retried.
>
> Consequence: the control plane needs zero storage privileges, and a future
> controller-side plugin (EBS `CreateVolume`) is an additive capability flag
> (`controller: true`), not a redesign.

> ★ **Judgement call 3 — `path` semantics are kind-aware, and `path` is optional.**
> The original plan said `path` is "where the task sees it (mount target inside
> task fs)". That is exactly right for `container` (a bind mount into the
> rootfs) and *impossible* for `exec`, which runs a host process in the host
> mount namespace — there is no filesystem view to mount into, and creating a
> real mount at `/var/lib/db` on the host would be an unrequested, unscoped
> mutation of the operator's machine.
>
> Resolution:
> - **`container`**: `path` is required and becomes a real bind mount
>   (`{destination: path, type: bind, source: <host path>, options: [rbind, rw|ro]}`)
>   in the OCI spec.
> - **`exec`**: the task gets the **host path** via environment variables —
>   `LEGION_VOLUME_<NAME>` for every volume, plus `LEGION_STORAGE_DIR` for the
>   stateful one. `path` is accepted, reported in the UI/CLI as the *intended*
>   path, and not mounted. Declaring it is not an error (jobs move between kinds),
>   but the docs are explicit that exec tasks should read the env var.
>
> This is why the guides use `PGDATA=$LEGION_STORAGE_DIR` for exec and
> `/var/lib/postgresql/data` for container. Both read naturally.

> ★ **Judgement call 4 — two declaration sites, one pipeline.**
> `stateful.storage` (per-instance, lifecycle-managed) and `task.volumes[]`
> (claims of pre-existing/shared storage, any task) are different *lifecycles*,
> so they are different fields — the same split K8s makes between
> `volumeClaimTemplates` and `volumes`, and Nomad makes between per-alloc CSI
> volumes and host volumes. They compile to the same `VolumeMount` list on the
> wire and share one worker mount pipeline, so the cost is a few lines of
> admission code, and the win is that a stateless job can mount `/mnt/models`
> read-only without pretending to be stateful.

> ★ **Judgement call 5 — one instance volume in v1, but the schema takes N.**
> `stateful.storage` is a single object, not a list, because `Instance` has one
> `storage_handle` column and 95% of stateful workloads want exactly one data
> volume. The `volume` table carries a `name` column and is keyed by
> `(instance_id, name)`, so adding `stateful.volumes: [...]` later is purely
> additive — no migration of existing rows, no handle format change.

> ★ **Judgement call 6 — no manager-side `create` means no `pending` state.**
> Volumes are born `pending` at handle-mint time (placement), become `created`
> the first time a worker successfully mounts them (which is also when
> `node_id` is recorded for node-local plugins), and go `destroying → destroyed`
> on reclaim. A volume that never mounted is just a `pending` row and a name; no
> external resource leaks, so nothing needs a two-phase create.

---

## Proposed Design

### 1) The volume contract

Four operations. **Every operation is idempotent** — Legion retries on worker
restarts and partial failures, so "already done" is a success, not an error.

| Op | Purpose |
| --- | --- |
| `capabilities` | Report contract version, `nodeLocal`, supported access modes, required/optional params, extra ops, and whether the plugin is usable on *this* node right now (`available` + `detail`). |
| `mount` | Make the volume available on this node at a concrete host path for one allocation. Provisions lazily on first call. Returns `mountPath` (+ optional attrs recorded on the volume row). |
| `unmount` | Release the mount. The volume and its data survive. |
| `destroy` | Permanently delete the volume and its data. |

Reserved (named in the ABI, dispatched only once implemented):
`create`, `attach`, `detach`, `snapshot`, `restore`, `expand`.

### 2) The exec ABI (CNI-style)

A volume plugin is an executable at `plugins/volumes/<name>/<name>` (or any
directory on `LEGION_VOLUME_PLUGIN_DIR`), beside a `plugin.json` manifest:

```json
{ "kind": "volumes", "name": "zfs", "legionVolumeApiVersion": 1 }
```

Legion runs the binary with the operation as `argv[1]` and a JSON request on
stdin; the plugin writes a JSON response on stdout and exits 0. Non-zero exit
means failure, and Legion reads `{ "error": { "code", "message", "retryable" } }`
from stdout when present, falling back to stderr text.

```jsonc
// stdin for: zfs mount
{
  "apiVersion": 1,
  "op": "mount",
  "volume": {
    "id": "vol-9f2c…",
    "name": "data",
    "plugin": "zfs",
    "attrs": { "dataset": "tank/legion/vol-9f2c" },
    "readOnly": false
  },
  "params": { "size": "10G" },
  "instance": { "id": "db-default-2", "job": "db", "group": "default", "mode": "slot", "slot": 2 },
  "allocId": 8841,
  "node": { "id": "node-a1", "dataDir": "/var/lib/legion", "volumeDir": "/var/lib/legion/volumes" }
}
```

```jsonc
// stdout
{ "apiVersion": 1, "mountPath": "/tank/legion/vol-9f2c", "attrs": { "dataset": "tank/legion/vol-9f2c" } }
```

Why exec rather than the existing `cdylib` ABI: volume operations are
low-frequency (task start/stop), often need external CLI tools (`mount`, `zfs`,
cloud SDKs) and elevated privileges, and must not be able to take the worker
down on a crash. Process isolation buys all three and the spawn cost is
irrelevant at this rate. `cdylib` stays right for hot-path plugins (task kinds,
scheduling policies).

### 3) Built-in plugins (in-process, ship with the binary)

Built-ins implement the same `VolumePlugin` trait the exec dispatcher does, so
nothing downstream knows whether a plugin is in-process or exec'd.

| Plugin | Node-local | What it does | Params |
| --- | --- | --- | --- |
| **`local-dir`** *(default)* | yes | A directory under `<state dir>/volumes/<volume_id>`, created on first mount, removed on destroy. Zero config; this is what a bare `storage: { path: … }` uses. | `size` (recorded, not enforced), `mode` (dir permissions) |
| **`host`** | yes | Claims an operator-declared host path from the worker config (`hostVolumes: { models: { path: '/mnt/models', readOnly: true } }`). No provisioning; `destroy` is a no-op (Legion never deletes operator data). | `name` (required), `readOnly` |
| **`nfs`** | **no** | Mounts an NFS export per volume, with a per-volume subdirectory so replicas do not collide. The first non-node-local plugin: instances reschedule freely. | `server`, `export` (required), `subdir`, `options` |
| **`tmpfs`** | yes | A RAM-backed scratch mount sized by `size`, wiped on unmount. Data does **not** survive a restart — the plugin advertises `ephemeral: true` and Legion surfaces that in the UI/CLI so nobody mistakes it for durable storage. Linux only. | `size` (default `64M`), `mode` |

Plus a reference **exec** plugin at `plugins/volumes/example/` — a ~60-line
POSIX shell script implementing the full contract over a directory tree. Its job
is to prove the ABI end to end and to be the thing you copy when writing a real
one; `legion volume plugin test example` runs the conformance harness against it.

### 4) Job spec (user-facing)

Per-instance storage (stateful tasks) — the existing block grows three fields:

```jsonnet
stateful: {
  mode: 'slot',
  storage: {
    path: '/var/lib/db',       // container: bind destination. exec: informational
                               // (read $LEGION_STORAGE_DIR instead)
    plugin: 'local-dir',       // default when omitted
    params: { size: '10G' },   // passed to the plugin verbatim
    readOnly: false,
    retainOnScaleIn: true,     // unchanged
    retainOnStop: true,        // unchanged
  },
},
```

Shared/pre-existing storage (any task, stateful or not):

```jsonnet
volumes: [
  { name: 'models', plugin: 'host', params: { name: 'models' }, path: '/models', readOnly: true },
  { name: 'scratch', plugin: 'tmpfs', params: { size: '512M' }, path: '/scratch' },
],
```

Admission validation:

- the named plugin must be advertised by at least one node (else: *"no node in
  the cluster provides volume plugin `zfs`; install it under
  `plugins/volumes/zfs/` on a worker"*);
- required params must be present (the plugin declares them in `capabilities`);
- `path` must be absolute when set, and is **required for container tasks**;
- volume `name` must match `[A-Za-z0-9._-]{1,64}` and be unique within a task
  (it becomes `LEGION_VOLUME_<NAME>`);
- `readOnly: true` is rejected on `stateful.storage` (a per-instance volume
  nobody can write to is always a mistake);
- an ephemeral plugin (`tmpfs`) on `stateful.storage` is rejected — the instance
  contract promises data survives reschedules and tmpfs cannot.

### 5) Scheduler integration

- **Plugin availability.** Workers advertise
  `fingerprint.volumePlugins: { "<name>": { nodeLocal, ephemeral, available, … } }`.
  A task's required plugin set filters candidate nodes before scoring; when
  nothing matches the eval fails with *"no node provides volume plugin X"*
  rather than a generic no-capacity error.
- **Node-local pinning.** A volume row records `node_id` the first time it
  mounts. When an instance with a node-local volume needs a replacement
  allocation, the planner forces that placement onto the pinned node; if the
  node is gone the eval reports *"volume `vol-…` is node-local and pinned to
  node-a1; waiting for that node (or `legion volume delete --force`)"*.
- **Binder pairing.** The instance binder (the transaction that pairs a new
  allocation with an instance) prefers the free slot / keyed instance whose
  node-local volume is already pinned to the node the allocation landed on. This
  makes pinning self-correcting: even if plan order and bind order diverge, an
  allocation on node-a1 binds the instance whose data is on node-a1.

### 6) State & failure model

`volume` table:
`{ volume_id, name, plugin, job_id, instance_id, task_name, state, node_id,
   node_local, params_json, attrs_json, mount_path, read_only, created_at,
   updated_at, last_error }`
with `state ∈ { pending, created, destroying, destroyed }`.

- Handles are `"{plugin}/{volume_id}"` — the same shape the pre-driver code
  already minted (`local-dir/<rand>`), so existing rows keep parsing.
- The volume row is created in the **same transaction** that mints the handle,
  so a handle can never exist without a row.
- `destroy` is two-phase: mark `destroying`, dispatch to the worker, mark
  `destroyed` only on confirmation. A crash mid-destroy retries; nothing leaks.
- An orphan reaper marks volumes `destroying` when their owning instance is gone
  (keyed destroy, scale-in with `retainOnScaleIn: false`, job destroy) and
  re-dispatches `destroying` volumes on every tick until a worker confirms.
- `legion volume delete --force` marks a volume `destroyed` without plugin
  confirmation, for the node-is-never-coming-back case (open question 3).

---

## Implementation Phases

1. **Contract + built-ins + worker pipeline** — `VolumePlugin` trait,
   `local-dir`/`host`/`nfs`/`tmpfs`, mount before start / unmount on stop, env +
   OCI bind mounts. *Exit: a slot job gets a persistent dir that survives task
   restarts on the same node.*
2. **Manager state** — `volume` table, handle↔row invariant, dispatch of the
   mount list in `TaskAllocation`, orphan reaper, destroy-over-heartbeat.
3. **Scheduler** — plugin fingerprinting, node filtering, node-local pinning,
   binder pairing.
4. **Exec dispatcher + conformance harness** — manifest discovery, JSON ABI,
   timeouts, `legion volume plugin test`, reference shell plugin.
5. **Surfaces** — RPCs, `legion volume …`, MCP tools, UI index/detail/cross-links.
6. **Guides** — Postgres and Redis, with runnable jobspecs.
7. *(future)* CSI bridge, snapshots, multi-volume `stateful.volumes[]`.

## Testing Plan

- **Conformance harness** (shared by tests and `legion volume plugin test`):
  every plugin runs `capabilities → mount → write file → unmount → mount →
  read file back → unmount → destroy`, plus a double-call idempotency check on
  every op. Built-ins and exec plugins go through the identical harness.
- **Unit**: handle parse/format, manifest validation, exec ABI framing (bad
  JSON, non-zero exit, timeout), capability parsing, admission errors, mount
  spec → OCI mount translation, env var naming.
- **Integration**: reschedule on the same node reattaches the same `local-dir`
  volume with data intact; keyed destroy reaps the volume; `retainOnScaleIn`
  honored both ways; a node-local instance whose node is gone reports the pin
  diagnostic instead of placing elsewhere.
- **Regression**: stateless jobs and pre-volume stateful jobs (bare `path`)
  behave unchanged.

## Revision 2: block-mode volumes (from the sandbox/microVM work)

The `sandbox` task kind (OCI images booted inside microVMs) is the first
consumer that cannot use a filesystem volume at all: a guest kernel attaches a
virtio-blk device, not a host directory. Its plan
(`docs/plans/sandbox-snapshots-and-fast-start-plan.md` §0) is written as a
dependency on this one. Three of its asks were genuinely blocking; the rest are
recorded below as decided-but-deferred.

### Implemented

> ★ **Judgement call 11 — `volume_mode: filesystem | block`.**
> `mount` returning only a host path is right for `exec` (a process reads a
> directory) and useless for a microVM. Rather than let a VM kind grow a
> parallel storage mechanism, the contract now carries the same
> `Filesystem | Block` split Kubernetes has, for the same reason.
>
> - `capabilities` gains `volumeModes`; `mount` returns `devicePath` + `fs` for
>   block, `mountPath` for filesystem, and the worker rejects a driver that
>   answers in the wrong shape rather than passing a bad path downstream.
> - `stateful.storage.mode` / `volumes[].mode` declare it, defaulting to
>   filesystem so every existing jobspec is unchanged.
> - `TaskPlugin::supported_volume_modes()` declares the kind's side, and
>   admission intersects **kind ∩ plugin ∩ requested**, naming all three in the
>   error.
> - `local-dir` serves block mode as a sparse, formatted disk image — a plain
>   file, so no loop device and no root. `host` serves it as an operator-declared
>   `device`.
>
> **The plugin formats, never the consumer.** Driven by `params.fs`, on the
> first mount only. That keeps `mkfs` out of untrusted guests and makes "is this
> volume fresh?" a driver-side fact — `mount` returns `fresh: true` exactly once,
> which is the first-boot-initialize signal. `mount` never reformats an existing
> image; the conformance suite checks that explicitly, because getting it wrong
> silently destroys the volume you asked to reattach.

> ★ **Judgement call 12 — `clone` is reserved now, implemented later.**
> Added beside `snapshot`/`restore`/`expand`. Settling the *name* costs nothing
> and is what stops the sandbox snapshot work from forcing an ABI break;
> implementing reflink clone before there is a consumer to test it against would
> be speculative. `local-dir` via `FICLONE`/`clonefile` is the natural first
> implementation.

> ★ **Judgement call 13 — a driver may return a not-yet-populated device.**
> Lazy/fetch-on-read disks (NBD/FUSE over a CAS base, qcow2 backing chains,
> dm-thin over sparse) all fit behind `mount` with **no new contract surface** —
> but only if a plugin author knows they are allowed to. Now stated explicitly
> in the contract docs rather than left as a non-obvious inference.

Also landed: `volume_mode` and `fs` columns on the `volume` table (so a future
restore/clone knows the shape without re-probing), `volumeModes` and `ops` on
`ListVolumePlugins`, and `legion volume plugin test --mode block|filesystem`
with a block-specific cycle. Block persistence is verified by fingerprinting the
image across unmount/remount rather than mounting it — Legion hands out the
device and the *guest* owns the filesystem, and the harness deliberately needs no
privileges.

### Already in place

The proposal's second blocking item — "the resolved volume must reach the
worker" — was already built: `TaskAllocation.task_volumes` (field 15, the exact
number and name proposed) carries the resolved mount list, and `TaskRuntime`
exposes it to `prepare`/`start` as typed `RuntimeVolume`s. Block mode extended
that struct with `mode`, `fs`, and `fresh`; nothing else changed.

### Decided, not yet built

- **`clone` × node-locality.** Fanning 50 forks across a fleet from one
  node-local volume is impossible by definition. The answer when `clone` lands:
  support same-node clone natively (every fork pins to the origin node) and
  route cross-node fan-out through a snapshot blob (CAS → `restore` on the
  target), with `capabilities` reporting `cloneScope: node | cluster`.
- **`attach` concurrency.** v1 mounts after artifact staging and before
  `start`. The scheduler knows the target node earlier than that, so mounting
  concurrently with staging is a real start-latency win. Deliberately not
  specified as strictly serial anywhere, so it stays available.
- **`config.state` desugar.** When the sandbox kind merges, its
  `config.state: { path, size_mib }` should desugar into a `stateful.storage`
  claim with `mode: 'block'` — the same trick `container.image` uses to become
  an implicit artifact. Owning the desugar here means no user jobspec changes on
  the day volumes ship.
- **Retention has exactly one owner.** Once a kind's storage is a volume, the
  kind must stop making retention decisions: `retainOnScaleIn` / `retainOnStop`
  govern the volume, and the reaper acts on them. A kind that also retains
  "forever" on its own leaks disks on the node while the volume system believes
  it reclaimed them — a silent failure, so it is called out here.

### Deliberately unchanged

The exec ABI, `plugin.json` discovery, two-phase destroy, orphan reconciliation,
and the CSI bridge all survive contact with the sandbox work unchanged. Nothing
hypervisor-specific entered the contract: `block` is generic, and everything
about virtio, kernels, or memory snapshots stays in the task kind.

## Judgement calls made during implementation

Beyond the six design calls above, four decisions were made while building and
testing it:

7. **Reclaim is reconciled, not triggered.** Instrumenting each of the four
   handle-drop sites (keyed destroy, scale-in, `retainOnStop`, job destroy) to
   also mark a volume would mean any future code path that drops a handle
   silently leaks storage. Instead a background loop marks any volume no
   instance references. One rule, no way to forget it.

8. **A node that advertises no `volumePlugins` fingerprint is assumed to have
   the built-ins.** Excluding such nodes outright is technically correct but
   would strand every stateful job on an upgrade until the last worker
   restarted. The built-ins ship in the binary, so assuming them is both
   backward-compatible and true. Same leniency at admission: with no plugin
   metadata anywhere, the check is skipped rather than rejecting every job.

9. **A successful mount report may correct a stale `destroyed` tombstone, but
   never a `destroying` row.** Task volume claims have deterministic ids, so a
   claim re-declared after a job destroy lands on the same row; leaving it
   tombstoned would make a genuinely mounted volume invisible in
   `legion volume list` and immune to the reaper. A mount is proof the storage
   exists. `destroying` is excluded because a reclaim is deliberately in flight.

10. **Task volume claims are shared per `(job, group, task, name)`**, not per
    allocation. That is what "a claim on storage that already exists" means, and
    it is right for the dominant case (N replicas mounting one models
    directory read-only). Per-allocation scratch already exists as
    `$LEGION_TASK_DIR`; the `tmpfs` sharing caveat is documented rather than
    special-cased.

## Open Questions (block mode)

8. Should `clone` accept a target node/topology hint, or is cross-node fan-out
   always snapshot-mediated? (Leaning: snapshot-mediated, with same-node clone
   native — see "Decided, not yet built".)
9. In block mode, does `mount` stay per-allocation, or does the device stay
   attached across a stop/start on the same node to cut restart latency?
   Idempotency makes re-running mount safe either way; this is purely a latency
   call and needs a real workload to decide.

## Resolved Open Questions

1. **Lazy vs eager create for node-local plugins.** → **Lazy**, always. There is
   no `create` op in v1; the first `mount` provisions. This is what every v1
   plugin wants and it removes an entire failure mode (a created-but-never-
   mounted volume).
2. **Volume sharing across instances (many readers).** → `accessModes` is
   reported by `capabilities` and recorded on the volume, and anything other
   than single-writer is rejected at admission. The field exists so multi-reader
   can be turned on per plugin later without a spec change. The `host` plugin is
   the practical escape hatch today: N tasks may claim the same host volume
   read-only, because it has no per-instance lifecycle.
3. **Who destroys a node-local volume when its node is permanently gone?** →
   `legion volume delete --force`, which marks the row `destroyed` without
   plugin confirmation and unpins the instance. The un-forced path waits for the
   node, because silently forgetting a volume that still holds bytes is worse
   than an operator having to say so.
4. **Exec plugin privileges.** → Plugins run as the worker's user. Mount-based
   plugins (`nfs`, `tmpfs`) therefore need a worker running as root (or
   `CAP_SYS_ADMIN`); their `capabilities` probe reports `available: false` with
   a `detail` explaining why when it cannot mount, and admission surfaces that
   instead of failing at task start. A per-plugin `runAs` is deliberately not in
   v1: it invites a setuid-shaped hole for a problem operators can solve by
   running the worker correctly.
