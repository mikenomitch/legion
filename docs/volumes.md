# Volumes

A stateful task needs its data to still be there after the task moves. The
[stateful workloads](stateful-workloads.md) doc covers identity: replica 2
always comes back as replica 2. This doc covers storage: what replica 2's disk
is, who creates it, and what happens to it when the replica goes away.

> Guides: [Postgres](guides/postgres.md), [Redis](guides/redis.md).

## The short version

```nickel
{
  name = "db",
  tasks = [{
    name = "postgres",
    runtime = "exec",
    count = 3,
    stateful = {
      storage = { path = "/var/lib/postgresql/data" },   # that's it
    },
    config = { command = ["postgres", "-D", "$LEGION_STORAGE_DIR"] },
  }],
}
```

This gives you three replicas (slot mode is the default) with three independent
data directories. Each directory is reattached to its own replica across
restarts, redeploys, and reschedules. There is nothing to install or configure,
because the default `local-dir` driver ships in the binary.

## Concepts

### Volume vs storage handle

An **instance** owns an opaque `storage_handle` that looks like
`local-dir/9f2c…`. The format is `{driver}/{volume_id}`: which driver owns the
storage, and which volume it is. The **volume** is the record behind that
handle. It tracks where the storage lives, what the driver recorded about it,
and where it is in its lifecycle.

| | Allocation | Instance | Volume |
| --- | --- | --- | --- |
| Lifetime | one placement | the workload's logical life | the data's life |
| Dies when | rescheduled | destroyed / scaled in | reclaimed |
| Identified by | churning integer | stable string id | `{driver}/{volume_id}` |

Volumes outlive their instances. When an instance is destroyed, its bytes still
need reclaiming, so the volume record stays until a driver confirms the storage
is gone.

### Instance storage vs volume claims

There are two places to declare storage, because they have two different
lifecycles:

| | `stateful.storage` | `volumes = [...]` |
| --- | --- | --- |
| One per | instance (replica) | task |
| Provisioned | on first placement | never (it already exists) |
| Reclaimed | with the instance | only by `legion volume delete` |
| Shared | no, single writer | yes, every allocation of the task |
| Valid on stateless tasks | no | **yes** |

`stateful.storage` is the Kubernetes `volumeClaimTemplates` analog: one volume
per replica, provisioned automatically. `volumes[]` is a claim on storage that
already exists, such as a models directory, an NFS export, or a scratch tmpfs.
It works on any task, stateful or not.

```nickel
tasks = [{
  name = "inference",
  runtime = "container",
  count = 8,
  # Stateless. Still mounts a volume.
  volumes = [
    { name = "models", driver = "host", params = { name = "models" },
      path = "/models", readOnly = true },
  ],
  config = { image = "ghcr.io/acme/inference:1.2", command = ["serve"] },
}]
```

### Node-local vs network

Every driver declares whether its volumes are **node-local**:

- **Node-local** (`local-dir`, `host`, `tmpfs`): the bytes exist on one
  machine. The instance holding the volume is **pinned** to that node, and
  replacements can only be placed there. If the node is gone, the instance
  waits.
- **Network** (`nfs`): any node can reach the storage. The instance
  reschedules freely, and the volume follows.

This is the most important property of a driver. It decides whether your
database comes back on the same machine, or comes back anywhere.
`legion volume list` marks a pin with `*`:

```console
$ legion volume list
 VOLUME              PLUGIN     JOB  INSTANCE      STATE    NODE
 9f2c8a…             local-dir  db   db-default-0  created  node-a1*
 4471be…             nfs        db   db-default-1  created  node-b2

* node-local: the owning instance can only be replaced on that node.
```

### Filesystem mode vs block mode

A volume is handed to the task in one of two ways, set with `mode`:

- **`filesystem`** (the default): a host directory the task reads and writes.
  This is what `exec` and `container` tasks want.
- **`block`**: a raw block device or disk image the task attaches. A guest
  kernel consumes a virtio-blk device, not a host directory, so a
  [`sandbox`](task-runtime-sandbox.md) microVM cannot use a filesystem volume
  at all.

Kubernetes makes the same split with `volumeMode`, for the same reason.

```nickel
stateful = {
  mode = "keyed",
  storage = {
    path = "/workspace",        # where the TASK sees it (the guest path)
    driver = "local-dir",
    mode = "block",
    params = { size = "4G", fs = "ext4" },
  },
},
```

Three things have to agree, and admission checks all three at submit:

1. The **task runtime's** supported modes. `exec` and `container` support
   filesystem. `sandbox` supports both, split by backend: block on microVMs,
   filesystem in its dev modes.
2. The **driver's** supported modes (`legion volume drivers` shows them).
3. The mode the **job** asked for.

```
task db stateful.storage: task runtime `exec` cannot consume a `block` volume (it
supports: filesystem). Filesystem volumes are directories a process reads; block
volumes are devices a guest attaches
```

The driver formats block volumes, never the guest. A block volume is created
and formatted with `params.fs` (default `ext4`) on its first mount. That way
`mkfs` never runs inside an untrusted guest, and the driver knows for certain
whether a volume is fresh. `mount` returns `fresh = true` only once, on the call
that created the storage. Use that signal to run first-boot initialization.

A remount never reformats. The conformance suite checks this, because getting
it wrong silently destroys the volume you asked to reattach.

A driver may also return a device that is not yet fully populated, so a
consumer can start before the data finishes syncing. Examples include an
NBD/FUSE device over a content-addressed base, a qcow2 backing chain, or
dm-thin over a sparse file. The contract does not require `mount` to block
until every byte is local.

## How `path` works per task runtime

`path` is where the task sees the volume. Different task runtimes see different
filesystems, so `path` behaves differently in each:

| Runtime | What `path` does | What the task should read |
| --- | --- | --- |
| `container` | A real bind mount at that destination inside the container. **Required.** | `path`, or `$LEGION_VOLUME_<NAME>`, which carries the same value |
| `exec` | Nothing. An exec task runs in the **host** mount namespace, so there is no filesystem view to mount into. A jobspec should not be able to create a mount at `/var/lib/db` on the operator's machine. `path` is recorded and shown in the UI and CLI as intent. | `$LEGION_VOLUME_<NAME>` (or `$LEGION_STORAGE_DIR`), which carries the actual host path |
| `sandbox` | Block mode: the guest init mounts the attached virtio-blk device there. Dev modes: a bind mount, like `container`. If you omit `path` on a block volume, the volume is attach-only (the guest gets the raw device). | `path`, or `$LEGION_VOLUME_<NAME>`, which carries the guest path |

Every volume is exported as `LEGION_VOLUME_<NAME>` (name upper-cased,
non-alphanumerics folded to `_`). The instance volume from `stateful.storage` is
also exported as **`LEGION_STORAGE_DIR`**, which is what the guides use:

```nickel
# exec — read the env var (through a shell: exec argv is not expanded)
config = { command = ["sh", "-c", "exec redis-server --dir \"$LEGION_STORAGE_DIR\""] }

# container — the path is real
stateful = { mode = "slot", storage = { path = "/var/lib/postgresql/data" } }
config = { image = "postgres:16", env = { PGDATA = "/var/lib/postgresql/data" } }
```

## Built-in drivers

Four drivers ship in the binary. Run `legion volume drivers` to see what a
given cluster has, including anything installed out of tree.

### `local-dir` (the default)

A directory under the worker's state dir, created on first mount and removed on
destroy. It is node-local and needs no configuration. A bare
`storage = { path = … }` uses this driver.

```nickel
storage = { path = "/var/lib/db", params = { size = "10G", mode = "0700" } }
```

In filesystem mode, `size` is not a quota, because a directory has no
enforcement. The scheduler does use it for placement: it checks that a node's
free disk can hold a task's node-local volumes before placing there. Network
drivers like `nfs` are exempt, since their bytes live on the storage server.
An instance going back to the node that already holds its volume (a
replacement after a failure, a rollout's new version) is not charged its
`size` again, since that volume adds nothing to the node's disk. A node whose
free disk has dropped under the declared size can still take its own
instances back.
`mode` sets the directory permissions.

In **block mode**, the driver serves a sparse disk image at
`<volume dir>/<volume_id>/disk.img`, formatted with `params.fs` (default
`ext4`) at `params.size` (default 1GiB). Here `size` is enforced, because it is
the image's size. The image is a plain file, so creating one needs no
privileges and no loop device. Block mode is advertised only on nodes with the
matching `mkfs` on `PATH`. Elsewhere the driver reports filesystem mode only
and says why.

`local-dir` implements the optional `snapshot`, `restore`, `clone`, and
`snapshot-delete` ops in both modes. See
[Snapshot, restore, and clone](#snapshot-restore-and-clone).

### `host`

Claims a path the operator declared in the worker config. It does no
provisioning, and `destroy` is a **no-op**. The data belongs to the operator,
so a scaled-in job must never delete `/mnt/models`.

```nickel
# worker config (configs/workers/<type>.ncl)
{
  workerType = "default",
  hostVolumes = {
    models = { path = "/mnt/models", readOnly = true },
    pgdata = { path = "/mnt/nvme0/pg", create = true },
  },
}
```

```nickel
# jobspec
volumes = [{ name = "models", driver = "host", params = { name = "models" },
            path = "/models", readOnly = true }],
```

For block mode, declare a `device` instead of a `path`. Use this to hand a
dedicated disk to a guest on bare metal:

```nickel
hostVolumes = {
  scratch = { device = "/dev/mapper/legion-scratch", fs = "ext4" },
}
```

`host` never formats a declared device. The consumer gets whatever is on the
operator's disk.

`readOnly` on the declaration is a hard boundary. A read-write claim against it
is an error, not a silent downgrade. `create` (default false) makes the worker
create a missing directory. It is off by default so a typo in a path cannot
silently produce an empty volume where your data was supposed to be.

Use `host` when the bytes are already on the machine, or to put a database on
a specific NVMe drive.

### `nfs`

Mounts an NFS export and hands out a per-volume subdirectory inside it. This is
the only built-in driver that is **not** node-local, so instances holding an
`nfs` volume can reschedule to any node that can reach the server.

```nickel
storage = {
  path = "/var/lib/db",
  driver = "nfs",
  params = { server = "10.0.0.5", export = "/exports/legion", options = "vers=4,hard" },
}
```

| Param | | |
| --- | --- | --- |
| `server` | required | NFS server host or IP |
| `export` | required | Export path on the server |
| `subdir` | optional | Subdirectory within the export (default: the volume id) |
| `options` | optional | `mount -o` string (default `vers=4`, or `resvport` on macOS) |

Mounting needs privileges. Run the worker as root. Otherwise the driver reports
itself unavailable, and admission rejects jobs that claim it, with the reason.

### `tmpfs`

RAM-backed scratch space, sized by `size` (default `64M`) and wiped on unmount. It is **Linux
only**. macOS has no tmpfs, and substituting a disk-backed directory would
break the promise that the data is in RAM and capped at the size you set.

```nickel
volumes = [{ name = "scratch", driver = "tmpfs", params = { size = "2G" },
            path = "/scratch" }],
```

`tmpfs` advertises itself as **ephemeral**, so admission rejects it for
`stateful.storage`. Instance storage promises that data survives a reschedule,
and a tmpfs cannot keep that promise. Declare it under `volumes` instead.

> A `tmpfs` claim is shared per `(job, group, task, name)` like any other claim,
> so two allocations of the same task on one node share it. For per-allocation
> scratch, use `$LEGION_TASK_DIR`, which is already private and cleaned up on
> stop.

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

- **Provisioning is lazy.** There is no separate create step. The first `mount`
  creates the storage. A volume that never mounted is only a record and a name,
  so nothing external leaks.
- **Reclaim is reconciled, not triggered.** A background loop marks any volume
  that no instance references as `destroying`. One rule covers every way a
  handle can be dropped: keyed destroy, scale-in without `retainOnScaleIn`,
  `retainOnStop = false` on job stop, and a job destroy that removes instance
  records. No code path can forget to free storage.
- **Destroy is two-phase.** The manager marks the volume `destroying` and
  re-sends the request to the owning worker on every heartbeat until a driver
  confirms. A worker that dies mid-destroy retries. Nothing leaks and nothing
  is deleted twice.
- **A destroyed volume leaves a tombstone.** Once the driver confirms, the row
  stays as `destroyed` with no node, so you can still see where a job's
  storage went. `legion volume list` hides tombstones by default. Use `--all`
  or `--state destroyed` to see them.

Retention uses the same settings as other stateful workloads:
`retainOnScaleIn` and `retainOnStop`, both defaulting to Retain. See
[Scale-In / Scale-Out Retention](stateful-workloads.md#scale-in--scale-out-retention).

## Operating volumes

```bash
# Every live volume, or filtered
legion volume list
legion volume list --job db --state created
legion volume list --node node-a1 --driver local-dir

# Include destroyed tombstones (or ask for only those)
legion volume list --job db --all
legion volume list --state destroyed

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

Volume commands authorize under the `job` resource, scoped to the volume's
job. A volume no job owns can only be deleted with cluster-wide `job:write`
(a rule with no `names` or `namespaces` selector).

Every command takes `-j`/`--json` and emits the standard envelope. The same
operations are exposed over MCP as `legion_volume_list`, `legion_volume_get`,
`legion_volume_drivers`, and `legion_volume_delete`.

### When a replica will not place

A node-local volume pins its instance. If that node is gone, the eval fails
with a reason. Legion does not place the replica on an empty disk elsewhere:

```
Failed to place task postgres: instance db-default-1 holds node-local storage on
node node-a1, which is unavailable. Waiting for that node to return; use
`legion volume delete --force <id>` to abandon the data if it never will
```

`legion instance get <id>` reports the same thing from the instance's side:

```
detail: volume-pinned: volume 9f2c8a1b… (local-dir) is node-local and lives on
node node-a1 (status: lost), so this instance can only be placed there. …
```

You have two options:

1. **Bring the node back.** The replacement places as soon as the node
   returns, with its data intact. This is usually what you want.
2. **Abandon the data.** `legion volume delete --force <id>` detaches the
   volume and tombstones it without driver confirmation. The instance mints a
   fresh, empty volume and places anywhere. Any bytes on the dead node stay
   there, which is why this is not the default.

To avoid this choice, don't use node-local storage. An `nfs` volume
reschedules freely.

### Why is my job rejected?

Admission validates volume declarations against what the cluster advertises,
so storage problems show up at submit time instead of at task start:

```
task postgres stateful.storage: no node in the cluster provides volume driver
`zfs`. Install it under plugins/volumes/zfs/ on a worker, or run
`legion volume drivers` to see what is available

task cache volume "scratch": volume driver `tmpfs` is installed on node-a1 but
unusable everywhere: tmpfs mounts are Linux-only

task db stateful.storage: volume driver `nfs` requires params: export
```

`legion volume drivers` shows a `NODES` column as *usable/installed*. If the two
numbers differ, start investigating there.

## Common workflows

Each scenario below is a common storage setup. Each takes a few lines of spec
and at most one CLI command. Caveats are listed next to the workflow they apply
to.

### A database with zero storage decisions

```nickel
stateful = { storage = { path = "/var/lib/postgresql/data" } },
```

Every field has a default: `mode = "slot"`, `driver = "local-dir"`,
`storage.mode = "filesystem"`, and retention on. You get one replica and one
volume, and the data survives restarts and reschedules to the same node. This
fits dev clusters and single-node production. The
[Postgres guide](guides/postgres.md) builds on it.

### Remote volume: lose the node, replace the instance, reattach

```nickel
stateful = {
  storage = { path = "/data", driver = "nfs",
             params = { server = "10.0.0.5", export = "/exports/pg" } },
  onLost = { selfTerminateAfter = "90s", replaceAfter = "3m" },
},
```

When the node dies:

1. The partitioned worker self-fences at 90s.
2. The manager frees the allocation at 3m.
3. The scheduler places a replacement on **any** node advertising `nfs`. There
   is no pin, because the volume is not node-local.
4. `mount` reattaches the same export subdirectory. The volume record carries
   the params, and the subdirectory defaults to the volume id.

No operator action is needed. Total downtime is about `replaceAfter` plus
application recovery. For a step-by-step walkthrough, see
[the Postgres guide](guides/postgres.md#what-node-loss-looks-like-on-nfs-step-by-step).

To keep a human in the loop, set `replaceAfter = "never"`. The instance then
waits until someone runs `legion instance replace <id>`, and reattaches the
same way.

### Node-local volume: lose the node, wait for it

With `local-dir`, the bytes are on the node, so the same failure pins the
replacement instead of moving it. See
[When a replica will not place](#when-a-replica-will-not-place). Starting a
database on an empty disk looks like recovery but is data loss, so Legion
waits. You choose the failure mode per job at submit time: `nfs` moves freely,
and `local-dir` waits until the node returns or you abandon the data.

### Abandon a dead node and restore from backup

```bash
legion volume delete <id> --force   # tombstone the stranded volume
# the instance mints a fresh, empty volume and places on a healthy node
legion action run postgres sql --param query="$(cat backup.sql)"   # or psql < backup.sql
```

`--force` confirms that the node is never coming back. It abandons the volume
record without waiting for a driver that can no longer answer. One guard rail
still applies with `--force`: a worker never destroys storage under a live
mount. A mistyped id cannot reclaim a running database's disk.

### Fast local disk (NVMe) the operator provisioned

The worker config names the disks, and the job claims one by name:

```nickel
# worker config
hostVolumes = { nvme0 = { path = "/mnt/nvme0" } }
```

```nickel
stateful = { storage = { path = "/data", driver = "host", params = { name = "nvme0" } } },
```

`host` is node-local, so this pins like `local-dir`. That is the right
behavior for hardware you chose on purpose. A `device` entry instead of `path`
serves the same disk as a raw block device.

### Shared read-only model / artifact cache

```nickel
volumes = [{ name = "models", driver = "nfs", path = "/models", readOnly = true,
            params = { server = "10.0.0.5", export = "/exports/models" } }],
```

A `volumes` claim (as opposed to `stateful.storage`) is shared per task. Every
replica on every node mounts the same export, read-only, with no instance
identity and no pin. You can scale the fleet freely. With `driver = "host"`,
this instead means every node has the dataset pre-synced at a named path.

### Scratch space

```nickel
volumes = [{ name = "scratch", driver = "tmpfs", path = "/scratch",
            params = { size = "1G" } }],
```

`tmpfs` is RAM-backed, wiped on unmount, and Linux-only. It cannot back
`stateful.storage`, because it advertises itself as ephemeral and admission
enforces that. For private per-allocation scratch, `$LEGION_TASK_DIR` needs no
declaration at all.

### A sandbox (or tenant) per key, with pause and resume

```nickel
stateful = { mode = "keyed", storage = { path = "/home/user" } },
```

```bash
legion instance create sandbox --id user-42     # mint the key + its volume
legion instance stop  sandbox-user-42           # pause: allocation stops, volume retained
legion instance start sandbox-user-42           # resume: same key, same volume, any valid node
legion instance destroy sandbox-user-42         # tombstone the key; reaper reclaims the volume
legion volume list --job sandbox                # its volume is gone once the driver confirms
```

Stop and start act as suspend and resume for stateful sandboxes. The storage
handle stays with the id while nothing runs. Destroy drops the handle. The
reaper then marks the orphaned volume `destroying`, and the owning worker's
driver confirms the reclaim.

### Ordered rollout of a quorum system

```nickel
rollout = { slots = { order = "sequential" } },
```

Updates replace **allocations**, never instances. Slot N's replacement
re-mounts slot N's existing volume, so a rollout cannot touch data.
`sequential` updates one slot at a time, highest ordinal first, and each step
is health-gated (the StatefulSet `OrderedReady` analog). For a staged canary,
add `updateFrom = 2`. Slots 2 and up get the new config, and slots 0 and 1 keep
running the old one until you lower the floor.

### Scale in without losing data, scale back out and reattach

```bash
# count: 3 → 1: slots 1 and 2 stop; their volumes stay `created` (retention
# defaults to retain). count: 1 → 3 later: the same volumes reattach.
legion volume list --job db   # the retained rows are visible the whole time
```

Set `retainOnScaleIn = false` to get automatic reclaim instead of reattaching
on scale-out. Either way, the spec declares the behavior ahead of time.

### Changing the driver of a live job

If you resubmit with a different `driver`, only volumes created **after** the
change use it. That covers scale-out, new keys, and re-mints after `--force`.
Existing instances keep their existing volume, including its driver, params,
and data, because the storage handle records the driver it was created with.
Legion never migrates bytes on its own. To move data, back up, stop the
workload, `legion volume delete` the old volume, let the instance mint a new
one under the new driver, and restore.

### A block device for a VM or sandbox runtime

```nickel
stateful = { storage = { path = "/workspace", driver = "local-dir", mode = "block",
                       params = { size = "10G", fs = "ext4" } } },
```

The driver creates and formats a disk image and hands the task a device path
instead of a mount path. MicroVM runtimes attach this as a virtio disk. The
[`sandbox` runtime](task-runtime-sandbox.md) persists a workspace this way: the
device becomes `/dev/vdb` in the guest, and the generated init mounts it at
`path`. Admission cross-checks filesystem tasks against block volumes, so a
plain container asking for `mode = "block"` is rejected with the reason.

## Writing a volume driver

A volume driver is any executable that reads a JSON request on stdin and writes
a JSON response on stdout, installed under `plugins/volumes/`. Legion runs it
with the operation as `argv[1]`. This is the CNI model. It is
language-agnostic, isolated from worker crashes, and testable from a shell.

Install a driver at `plugins/volumes/<name>/<name>` beside a manifest:

```json
{ "kind": "volumes", "name": "zfs", "legionVolumeApiVersion": 1 }
```

### The contract

There are four operations, and **every one must be idempotent**. Legion retries
on worker restarts and partial failures, so "already done" counts as success.

| Op | Purpose |
| --- | --- |
| `capabilities` | Report `nodeLocal`, `ephemeral`, access modes, required/optional params, and whether the driver is usable on **this** node right now |
| `mount` | Make the volume available at a host path, provisioning it if this is the first call. Return `mountPath` |
| `unmount` | Release the mount. The data survives |
| `destroy` | Delete the volume and its data |

Four more operations are **optional**. A driver that implements one lists it
in `capabilities.ops`, and Legion only dispatches an optional op to a driver
that lists it. They must be idempotent too.

| Op | Purpose |
| --- | --- |
| `snapshot` | Capture the volume as the snapshot named by `snapshot.id`. Return the snapshot ref |
| `restore` | Replace the volume's contents with `snapshot`. The snapshot survives |
| `clone` | Create `volume` as a new copy of `snapshot` |
| `snapshot-delete` | Delete `snapshot` |

`create`, `attach`, `detach`, and `expand` are **reserved names**. A driver may
advertise them, and Legion will dispatch them when those features land, without
an ABI break.

### Snapshot, restore, and clone

A snapshot is a copy of a volume that is independent of it from then on:
destroying the volume keeps its snapshots, and restoring or cloning leaves the
snapshot in place. Checkpoints use these ops for their disk half
([task-runtime-sandbox.md](task-runtime-sandbox.md#checkpoints)).

The caller picks the snapshot id and passes it in the request's `snapshot`
field. That makes `snapshot` idempotent: a retry after a lost reply finds the
finished snapshot and returns it instead of taking a second one. `restore`,
`clone`, and `snapshot-delete` take the ref a previous `snapshot` returned:

```nickel
# stdin for: zfs restore (snapshot is what `zfs snapshot` printed)
{
  "apiVersion" = 1,
  "op" = "restore",
  "volume" = { "id" = "vol-9f2c…", "name" = "data", "driver" = "zfs", "attrs" = {}, "readOnly" = false },
  "snapshot" = {
    "id" = "snap-7f3a91",
    "volumeId" = "vol-9f2c…",
    "driver" = "zfs",
    "mode" = "filesystem",
    "attrs" = { "dataset" = "tank/legion/vol-9f2c@snap-7f3a91" },
    "size" = 1073741824
  },
  # params, instance, allocId, node as for mount
}
```

The caller stops writing to the volume before `snapshot`, and unmounts it
before `restore`. A snapshot of a volume under write is only crash-consistent.

`local-dir` keeps snapshots beside its volumes, at
`<volume dir>/../local-dir-snapshots/<snapshot_id>/`:

- **Filesystem mode** copies the directory tree. **Block mode** copies the disk
  image.
- The copy is a reflink (`FICLONE` on btrfs and XFS, `clonefile` on APFS)
  where the filesystem supports one, so it is instant and shares blocks until
  either side writes. Elsewhere it is a sparse copy, so the cost is the bytes
  actually written, not the volume's size. The snapshot ref records which one
  happened in `attrs.method` (`reflink` or `copy`).
- Every op builds under a temporary name and renames into place, so a crash
  never leaves a half-written snapshot, volume, or clone.
- `restore` swaps the whole volume: files created after the snapshot are gone
  afterward, not merged.
- `clone` into a volume id that already exists succeeds without copying,
  because only a finished clone can exist under that id.
- Snapshots live on the node that took them, like the volume.

### The request

```nickel
# stdin for: zfs mount
{
  "apiVersion" = 1,
  "op" = "mount",
  "volume" = {
    "id" = "vol-9f2c…",
    "name" = "data",
    "driver" = "zfs",
    "attrs" = { "dataset" = "tank/legion/vol-9f2c" },  # what you recorded last time
    "readOnly" = false
  },
  "params" = { "size" = "10G" },                        # straight from the jobspec
  "instance" = { "id" = "db-default-2", "job" = "db", "group" = "default",
                "mode" = "slot", "slot" = 2 },
  "allocId" = 8841,
  "node" = { "id" = "node-a1", "dataDir" = "/var/lib/legion",
            "volumeDir" = "/var/lib/legion/volumes" }
}
```

```nickel
# stdout
{ "apiVersion" = 1, "mountPath" = "/tank/legion/vol-9f2c",
  "attrs" = { "dataset" = "tank/legion/vol-9f2c" } }
```

`attrs` belongs to your driver. Whatever you return is stored on the volume
record and passed back on every later operation, so drivers can stay stateless
between calls.

Report failure by exiting non-zero. Legion prefers a structured error on stdout
over stderr text:

```json
{ "error": { "code": "ENOSPC", "message": "pool is full", "retryable": false } }
```

### Test it

`plugins/volumes/example/` is a working reference in about 60 lines of shell.
Copy it, then run the conformance suite. Legion's own tests use the same suite:

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
  - snapshot                 driver does not advertise `snapshot` in capabilities.ops
  ✓ destroy                  removed
  ✓ destroy.idempotent       second destroy succeeded

PASS — example honors the volume contract.
```

A driver that advertises `snapshot` also gets the snapshot checks: snapshot,
snapshot again to the same id, mutate the volume, `restore` twice and compare
with the snapshot-time content, `clone` twice into a new volume and compare,
and `snapshot-delete` twice. Each optional op the driver does not advertise is
reported as skipped.

The suite needs no manager and exits non-zero on failure, so you can run it in
CI. Pass driver params with `-p key=value` (`legion volume driver test nfs -p
server=10.0.0.5 -p export=/exports/legion`).

### CSI

Legion core does not speak CSI, and there are no plans for it to. The mapping
onto this contract is small enough to live in one driver: `mount` maps to
`NodeStageVolume` + `NodePublishVolume`, `unmount` to the unpublish/unstage
pair, and `destroy` to `DeleteVolume`. A `csi-bridge` exec driver is the
intended path to the existing CSI driver ecosystem, and it needs no changes to
the contract.

## Where volumes run

Workers own the drivers. The manager owns the state machine: which volumes
exist, their pins, and their reclaim status. It never loads a driver or touches
storage. It learns which drivers exist from the capabilities workers publish in
their fingerprints. `legion volume drivers` and job admission read the same
data.

As a result, the control plane needs no storage credentials and no `mount`
privileges. Adding a driver is a worker-side change that needs no manager
restart.

## What v1 does not do

- **Expansion.** `expand` is a reserved name in the contract, and no driver
  implements it.
- **Snapshots of anything but the instance volume.** `legion snapshot`
  ([suspend and resume](suspend-and-resume.md)) snapshots and restores an
  instance's `stateful.storage` volume with these ops. Shared `volumes = [...]`
  claims are never snapshotted, and nothing calls `clone` yet.
- **Multi-writer volumes.** `accessModes` is reported and recorded, but
  anything other than single-writer is rejected for instance storage. Several
  tasks *can* share a `host` volume read-only, since it has no per-instance
  lifecycle.
- **Multiple volumes per instance.** `stateful.storage` is one volume. The
  schema is keyed by `(instance, name)`, so adding `stateful.volumes = [...]`
  later would be a purely additive change.
- **Moving data between nodes.** A node-local volume pins its instance. Moving
  the bytes is up to the operator.
