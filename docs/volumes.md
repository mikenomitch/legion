# Volumes

A stateful task needs its data to still be there after the task moves. The
[stateful workloads](stateful-workloads.md) doc covers the *identity* half of
that — replica 2 always comes back as replica 2. This doc covers the *storage*
half: what replica 2's disk actually is, who creates it, and what happens to it
when the replica goes away.

> Design plan: [volume-plugin-system-plan.md](../internal_docs/plans/volume-plugin-system-plan.md).
> Guides: [Postgres](guides/postgres.md), [Redis](guides/redis.md).

## The short version

```jsonnet
{
  name: 'db',
  tasks: [{
    name: 'postgres',
    kind: 'exec',
    count: 3,
    stateful: {
      storage: { path: '/var/lib/postgresql/data' },   // that's it
    },
    config: { command: ['postgres', '-D', '$LEGION_STORAGE_DIR'] },
  }],
}
```

Three replicas (slot mode is the default), three independent data
directories, each reattached to its own replica across restarts, redeploys,
and reschedules. Nothing to install and
nothing to configure: the default `local-dir` driver ships in the binary.

## Concepts

### Volume vs storage handle

An **instance** owns an opaque `storage_handle` that looks like
`local-dir/9f2c…`. That is `{driver}/{volume_id}`: which driver owns the
storage, and which volume it is. The **volume** is the row behind that handle —
where it lives, what the driver recorded about it, and where it is in its
lifecycle.

| | Allocation | Instance | Volume |
| --- | --- | --- | --- |
| Lifetime | one placement | the workload's logical life | the data's life |
| Dies when | rescheduled | destroyed / scaled in | reclaimed |
| Identified by | churning integer | stable string id | `{driver}/{volume_id}` |

Volumes deliberately **outlive** their instances: when an instance is destroyed
its bytes still need reclaiming, so the volume row sticks around until a driver
confirms the storage is gone.

### Instance storage vs volume claims

Two declaration sites, because they are two different lifecycles:

| | `stateful.storage` | `volumes: [...]` |
| --- | --- | --- |
| One per | instance (replica) | task |
| Provisioned | on first placement | never — it already exists |
| Reclaimed | with the instance | only by `legion volume delete` |
| Shared | no, single writer | yes, every allocation of the task |
| Valid on stateless tasks | no | **yes** |

`stateful.storage` is the K8s `volumeClaimTemplates` analog: one volume per
replica, provisioned automatically. `volumes[]` is a claim on storage that
already exists — a models directory, an NFS export, a scratch tmpfs — and works
on any task, stateful or not.

```jsonnet
tasks: [{
  name: 'inference',
  kind: 'container',
  count: 8,
  // Stateless. Still mounts a volume.
  volumes: [
    { name: 'models', driver: 'host', params: { name: 'models' },
      path: '/models', readOnly: true },
  ],
  config: { image: 'ghcr.io/acme/inference:1.2', command: ['serve'] },
}]
```

### Node-local vs network

Every driver declares whether its volumes are **node-local**:

- **node-local** (`local-dir`, `host`, `tmpfs`) — the bytes exist on one
  machine. The instance holding one is **pinned** to that node: replacements can
  only be placed there. If the node is gone, the instance waits.
- **network** (`nfs`) — any node can reach the storage. The instance
  reschedules freely, and the volume follows.

This is the single most important property of a driver, because it decides
whether "my database comes back on the same machine" or "my database comes
back". `legion volume list` marks a pin with `*`:

```console
$ legion volume list
 VOLUME              PLUGIN     JOB  INSTANCE      STATE    NODE
 9f2c8a…             local-dir  db   db-default-0  created  node-a1*
 4471be…             nfs        db   db-default-1  created  node-b2

* node-local: the owning instance can only be replaced on that node.
```

### Filesystem mode vs block mode

Every volume is handed over one of two ways, declared with `mode`:

- **`filesystem`** (the default) — a host directory the task reads and writes.
  What `exec` and `container` tasks want.
- **`block`** — a raw block device or disk image the task *attaches*. A guest
  kernel consumes a virtio-blk device, not a host directory, so a microVM task
  kind cannot use a filesystem volume at all.

This is the same split Kubernetes makes with `volumeMode`, for the same reason.

```jsonnet
stateful: {
  mode: 'keyed',
  storage: {
    path: '/workspace',        // where the TASK sees it (the guest path)
    driver: 'local-dir',
    mode: 'block',
    params: { size: '4G', fs: 'ext4' },
  },
},
```

Three things have to agree, and admission checks all three at submit:

1. the **task kind's** supported modes (`exec`/`container` → filesystem);
2. the **driver's** supported modes (`legion volume drivers` shows them);
3. the mode the **job** asked for.

```
task db stateful.storage: task kind `exec` cannot consume a `block` volume (it
supports: filesystem). Filesystem volumes are directories a process reads; block
volumes are devices a guest attaches
```

**The driver formats, never the guest.** A block volume is created and
formatted with `params.fs` (default `ext4`) on its first mount, so `mkfs` never
runs inside an untrusted guest and "is this volume fresh?" is a driver-side
fact rather than a guess. `mount` returns `fresh: true` exactly once — on the
call that created the storage — which is the signal to run first-boot
initialization. **A remount never reformats**; the conformance suite has a
dedicated check for that, because getting it wrong silently destroys the volume
you asked to reattach.

A driver is also allowed to return a device that is **not yet fully populated**
— an NBD/FUSE device over a content-addressed base, a qcow2 backing chain,
dm-thin over a sparse file — so a consumer can start before the data finishes
syncing. Nothing in the contract requires `mount` to block until every byte is
local.

## How `path` works per task kind

`path` is where the *task* sees the volume, and the two task kinds see different
filesystems. Legion does not pretend otherwise:

| Kind | What `path` does | What the task should read |
| --- | --- | --- |
| `container` | A real bind mount at that destination inside the container. **Required.** | `path` — or `$LEGION_VOLUME_<NAME>`, which carries the same value |
| `exec` | Nothing. An exec task runs in the **host** mount namespace: there is no filesystem view to mount into, and creating a mount at `/var/lib/db` on the operator's machine is not something a jobspec should be able to do. `path` is recorded and shown in the UI/CLI as intent. | `$LEGION_VOLUME_<NAME>` (or `$LEGION_STORAGE_DIR`), which carries the real host path |

Every volume is exported as `LEGION_VOLUME_<NAME>` (name upper-cased,
non-alphanumerics folded to `_`). The instance volume from `stateful.storage` is
additionally exported as **`LEGION_STORAGE_DIR`**, which is what the guides use:

```jsonnet
// exec — read the env var
config: { command: ['redis-server', '--dir', '$LEGION_STORAGE_DIR'] }

// container — the path is real
stateful: { mode: 'slot', storage: { path: '/var/lib/postgresql/data' } }
config: { image: 'postgres:16', env: { PGDATA: '/var/lib/postgresql/data' } }
```

## Built-in drivers

Four drivers ship in the binary. `legion volume drivers` shows what a given
cluster actually has, including anything installed out of tree.

### `local-dir` (the default)

A directory under the worker's state dir, created on first mount, removed on
destroy. Node-local. Zero configuration — this is what a bare
`storage: { path: … }` uses.

```jsonnet
storage: { path: '/var/lib/db', params: { size: '10G', mode: '0700' } }
```

`size` is **not a quota** in filesystem mode — a directory has no enforcement,
and pretending otherwise would be worse than being honest. It *is* used for
placement: the scheduler checks a node's free disk can hold a task's node-local
volumes before placing there (network drivers like `nfs` are exempt — their
bytes live on the storage server). `mode` sets the directory permissions.

In **block mode** it serves a sparse disk image at
`<volume dir>/<volume_id>/disk.img`, formatted with `params.fs` (default
`ext4`) at `params.size` (default 1GiB). Here `size` *is* real — it is the
image's size — and the image is a plain file, so producing one needs no
privileges and no loop device. Block mode is advertised only on nodes with the
matching `mkfs` on `PATH`; elsewhere the driver reports filesystem mode only
and says why.

### `host`

Claims a path the operator declared in the worker config. No provisioning, and
`destroy` is deliberately a **no-op**: the data belongs to the operator, so a
scaled-in job must never delete `/mnt/models`.

```jsonnet
// worker config (configs/workers/<type>.jsonnet)
{
  workerType: 'default',
  hostVolumes: {
    models: { path: '/mnt/models', readOnly: true },
    pgdata: { path: '/mnt/nvme0/pg', create: true },
  },
}
```

```jsonnet
// jobspec
volumes: [{ name: 'models', driver: 'host', params: { name: 'models' },
            path: '/models', readOnly: true }],
```

For block mode, declare a `device` instead of a `path` — the bare-metal escape
hatch for handing a dedicated disk to a guest:

```jsonnet
hostVolumes: {
  scratch: { device: '/dev/mapper/legion-scratch', fs: 'ext4' },
}
```

`host` never formats a declared device: whatever is on the operator's disk is
what the consumer gets.

`readOnly` on the declaration is a hard boundary: a read-write claim is an
error, not a silent downgrade. `create` (default false) makes the worker create
a missing directory — off by default so a typo in a path cannot silently produce
an empty volume where real data was meant to be.

This is the escape hatch for "the bytes are already on the machine" and the way
to put a database on a specific NVMe.

### `nfs`

Mounts an NFS export and hands out a per-volume subdirectory inside it. The one
built-in that is **not** node-local, so instances holding an `nfs` volume
reschedule anywhere that can reach the server.

```jsonnet
storage: {
  path: '/var/lib/db',
  driver: 'nfs',
  params: { server: '10.0.0.5', export: '/exports/legion', options: 'vers=4,hard' },
}
```

| Param | | |
| --- | --- | --- |
| `server` | required | NFS server host or IP |
| `export` | required | Export path on the server |
| `subdir` | optional | Subdirectory within the export (default: the volume id) |
| `options` | optional | `mount -o` string (default `vers=4`, or `resvport` on macOS) |

Mounting needs privileges: run the worker as root, or the driver reports itself
unavailable and jobs claiming it are rejected at admission with the reason.

### `tmpfs`

RAM-backed scratch, sized by `size`, wiped on unmount. **Linux only** — macOS
has no tmpfs, and quietly substituting a disk-backed directory would turn "this
is in RAM and capped at 512M" into a lie.

```jsonnet
volumes: [{ name: 'scratch', driver: 'tmpfs', params: { size: '2G' },
            path: '/scratch' }],
```

`tmpfs` advertises itself as **ephemeral**, and admission rejects it for
`stateful.storage`: the instance contract promises data survives a reschedule
and a tmpfs cannot keep that promise. Declare it under `volumes` instead.

> A `tmpfs` claim is shared per `(job, group, task, name)` like any other claim,
> so two allocations of the same task on one node share it. For genuinely
> per-allocation scratch, use `$LEGION_TASK_DIR`, which is already private and
> cleaned up on stop.

## Lifecycle

```
              first placement                 first mount
instance ──────────────────────> pending ──────────────────> created
                                                                │
                       handle dropped (destroy / scale-in /     │
                       retainOnStop:false / job destroy)        │
                                                                v
                                             destroyed <──── destroying
                                          (driver confirmed)
```

- **Provisioning is lazy.** There is no separate create step: the first `mount`
  creates the storage. A volume that never mounted is a row and a name — nothing
  external leaks.
- **Reclaim is reconciled, not triggered.** A background loop marks any volume
  that no instance references as `destroying`. One rule covers every way a
  handle can be dropped (keyed destroy, scale-in without `retainOnScaleIn`,
  `retainOnStop: false` on job stop, a job destroy that cascades instance rows
  away), so no code path can forget to free storage.
- **Destroy is two-phase.** The manager marks `destroying` and re-sends the
  request to the owning worker on every heartbeat until a driver confirms. A
  worker that dies mid-destroy simply retries; nothing leaks and nothing is
  double-deleted.

Retention is governed by the same knobs stateful workloads already had —
`retainOnScaleIn` and `retainOnStop`, both defaulting to Retain. See
[Scale-In / Scale-Out Retention](stateful-workloads.md#scale-in--scale-out-retention).

## Operating volumes

```bash
# Everything, or filtered
legion volume list
legion volume list --job db --state created
legion volume list --node node-a1 --driver local-dir

# One volume: driver, params, metadata, node pin, last driver error
legion volume get 9f2c8a1b…

# What can this cluster do?
legion volume drivers

# Reclaim (DESTROYS DATA — the instance mints a fresh, empty volume next time).
# Refused while a live allocation runs against the volume: stop the workload
# first. The worker also refuses to reclaim storage under a live mount, so even
# --force cannot rm -rf a running database.
legion volume delete 9f2c8a1b…

# The node is never coming back
legion volume delete 9f2c8a1b… --force
```

Every command takes `-j`/`--json` and emits the standard envelope. The same
operations are exposed over MCP as `legion_volume_list`, `legion_volume_get`,
`legion_volume_drivers`, and `legion_volume_delete`.

### When a replica will not place

A node-local volume pins its instance. If that node is gone, the eval fails with
the reason rather than silently placing the replica on an empty disk elsewhere:

```
Failed to place task postgres: instance db-default-1 holds node-local storage on
node node-a1, which is unavailable. Waiting for that node to return; use
`legion volume delete --force <id>` to abandon the data if it never will
```

`legion instance get <id>` says the same thing from the instance's side:

```
detail: volume-pinned: volume 9f2c8a1b… (local-dir) is node-local and lives on
node node-a1 (status: lost), so this instance can only be placed there. …
```

Two ways out:

1. **Bring the node back.** The replacement places itself the moment it returns,
   with its data intact. This is usually what you want.
2. **Abandon the data**: `legion volume delete --force <id>` detaches the volume
   and tombstones it without driver confirmation. The instance mints a fresh,
   empty volume and places anywhere. Whatever bytes exist on the dead node stay
   there — which is why this is not the default.

Avoiding the choice entirely means not using node-local storage: an `nfs` volume
reschedules freely.

### Why is my job rejected?

Admission validates volume declarations against what the cluster actually
advertises, so storage problems surface at submit rather than at task start:

```
task postgres stateful.storage: no node in the cluster provides volume driver
`zfs`. Install it under plugins/volumes/zfs/ on a worker, or run
`legion volume drivers` to see what is available

task cache volume "scratch": volume driver `tmpfs` is installed on node-a1 but
unusable everywhere: tmpfs mounts are Linux-only

task db stateful.storage: volume driver `nfs` requires params: export
```

`legion volume drivers` shows a `NODES` column as *usable/installed* — a gap
between the two is exactly the thing to investigate.

## Writing a volume driver

A volume driver is any executable that reads a JSON request on stdin and writes
a JSON response on stdout, installed under `plugins/volumes/`. Legion runs it
with the operation as `argv[1]`. This
is CNI's model, chosen for the same reasons: language-agnostic, crash-isolated
from the worker, and testable from a shell.

Install one at `plugins/volumes/<name>/<name>` beside a manifest:

```json
{ "kind": "volumes", "name": "zfs", "legionVolumeApiVersion": 1 }
```

### The contract

Four operations. **Every one must be idempotent** — Legion retries on worker
restarts and partial failures, so "already done" is a success, not an error.

| Op | Purpose |
| --- | --- |
| `capabilities` | Report `nodeLocal`, `ephemeral`, access modes, required/optional params, and whether the driver is usable on **this** node right now |
| `mount` | Make the volume available at a host path, provisioning it if this is the first call. Return `mountPath` |
| `unmount` | Release the mount. The data survives |
| `destroy` | Delete the volume and its data |

`create`, `attach`, `detach`, `snapshot`, `restore`, `clone`, and `expand` are
**reserved names**: a driver may advertise them in `capabilities.ops`, and
Legion will dispatch them when those features land, without an ABI break.

`clone` — "make a new volume that starts as a copy-on-write copy of this one" —
is reserved deliberately rather than left for later: forking a workload needs
it, and the backends that can do it instantly (reflink/`FICLONE`, `zfs clone`,
dm-thin, EBS-from-snapshot) are the same ones that make forking viable at all.
Settling its name now is what keeps that work from forcing an ABI break.

### The request

```jsonc
// stdin for: zfs mount
{
  "apiVersion": 1,
  "op": "mount",
  "volume": {
    "id": "vol-9f2c…",
    "name": "data",
    "driver": "zfs",
    "attrs": { "dataset": "tank/legion/vol-9f2c" },  // what you recorded last time
    "readOnly": false
  },
  "params": { "size": "10G" },                        // straight from the jobspec
  "instance": { "id": "db-default-2", "job": "db", "group": "default",
                "mode": "slot", "slot": 2 },
  "allocId": 8841,
  "node": { "id": "node-a1", "dataDir": "/var/lib/legion",
            "volumeDir": "/var/lib/legion/volumes" }
}
```

```jsonc
// stdout
{ "apiVersion": 1, "mountPath": "/tank/legion/vol-9f2c",
  "attrs": { "dataset": "tank/legion/vol-9f2c" } }
```

`attrs` is yours: whatever you return is persisted on the volume row and handed
back on every later operation, so drivers stay stateless between calls.

Report failure by exiting non-zero. Legion prefers a structured error on stdout
over stderr text:

```json
{ "error": { "code": "ENOSPC", "message": "pool is full", "retryable": false } }
```

### Test it

`plugins/volumes/example/` is a working reference in ~60 lines of shell. Copy it,
then run the conformance suite — the same one Legion's own tests use:

```bash
legion volume driver test example
legion volume driver test local-dir --mode block -p size=1G
```

```
Volume driver conformance: example (filesystem mode)

  ✓ capabilities.apiVersion  v1
  ✓ capabilities             nodeLocal=true ephemeral=false accessModes=[single-writer]
  ✓ mount                    /var/lib/legion/volumes/example/conformance-z4u33fbM
  ✓ mount.idempotent         second mount returned the same path
  ✓ write                    …/.legion-conformance
  ✓ unmount                  released
  ✓ unmount.idempotent       second unmount succeeded
  ✓ persistence              data survived unmount/remount
  ✓ destroy                  removed
  ✓ destroy.idempotent       second destroy succeeded

PASS — example honors the volume contract.
```

It needs no manager and exits non-zero on failure, so it drops straight into CI.
Pass driver params with `-p key=value` (`legion volume driver test nfs -p
server=10.0.0.5 -p export=/exports/legion`).

### CSI

Legion core does not speak CSI, and won't. The mapping onto this contract is
small enough to live in one driver: `mount` → `NodeStageVolume` +
`NodePublishVolume`, `unmount` → the unpublish/unstage pair, `destroy` →
`DeleteVolume`. A `csi-bridge` exec driver is the intended path to the existing
driver ecosystem; nothing in the contract needs to change for it.

## Where volumes run

Workers own the drivers. The manager owns the state machine — which volumes
exist, their pins, their reclaim status — and never loads a driver or touches
storage. It learns what drivers exist entirely from the capabilities workers
publish in their fingerprints, which is also what `legion volume drivers` and
job admission read.

The practical consequence: the control plane needs no storage credentials and no
`mount` privileges, and adding a driver is a worker-side operation that needs no
manager restart.

## What v1 does not do

- **Snapshots, clones, expansion.** Reserved in the contract, unimplemented.
- **Multi-writer volumes.** `accessModes` is reported and recorded; anything
  other than single-writer is rejected for instance storage. Several tasks *can*
  share a `host` volume read-only, since it has no per-instance lifecycle.
- **Multiple volumes per instance.** `stateful.storage` is one volume. The
  schema is keyed by `(instance, name)`, so `stateful.volumes: [...]` is a purely
  additive change when it is needed.
- **Snapshots, clones, expansion.** Named in the contract and reserved in
  `capabilities.ops`, but no built-in implements them yet.
- **Moving data between nodes.** A node-local volume pins its instance; moving
  the bytes is the operator's job.
