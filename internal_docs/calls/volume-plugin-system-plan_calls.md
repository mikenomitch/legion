# Judgement Calls — Volume Plugin System

Companion to
[`plans/volume-plugin-system-plan.md`](../plans/volume-plugin-system-plan.md).
Decisions made while writing the plan and while implementing it, where the
request was ambiguous or a real alternative existed. Entries 2 and 13 were
recorded during the 2026-06-09 plan pass; entries 5, 6, 10, 11, 12, and 21
during the 2026-07-27 feedback pass (revision 3); the rest during the
implementation pass that shipped v1.

Entries are ranked by project impact, highest first.

## 1. Every plugin operation runs on a worker; the manager owns only the state machine

**Call.** Workers hold the plugin registry and run every driver call. The
manager owns the `volume` table, node pinning, and reclaim orchestration, but
loads no driver and never touches storage. It learns what plugins exist entirely
from the capabilities workers publish in their node fingerprints — which is what
job admission validates against and what `legion volume drivers` renders.
Reclaim is dispatched to the owning worker over the heartbeat channel and
confirmed on the next heartbeat.

**Alternatives.** The plan as written: split the ops into manager-side
"controller" calls (`create`, `destroy`) and worker-side "node" calls (`attach`,
`mount`), mirroring CSI's controller/node split. That is the industry-standard
shape and what someone arriving from CSI would build.

**Trade-off.** A genuinely controller-side driver — EBS `CreateVolume`, which
wants to run once from anywhere rather than on the node — has nowhere to live
today. Accepted: it becomes an additive `controller: true` capability flag when a
driver needs one, not a redesign. The immediate cost is that reclaim is
asynchronous and needs the two-phase `destroying → destroyed` machine with
heartbeat re-dispatch, where a synchronous manager-side `destroy` would have been
a function call.

**Why (simplicity, and a security boundary).** The manager cannot `mkdir` on a
remote node's disk, and for every v1 driver `create` and `destroy` *are*
node-side filesystem operations — so the controller half would have been an empty
abstraction. Drivers only on workers means the control plane needs no storage
credentials and no `mount` privileges, and installing a plugin is a worker-side
operation needing no manager restart.

## 2. Volume plugins use an exec ABI, not the existing `cdylib` ABI

Legion's existing plugins (task kinds, scheduling, node checks, rollouts) are
in-process `cdylib`s. The volume system deliberately diverges to CNI-style
external executables — JSON on stdin, operation in `argv[1]`, JSON on stdout —
because volume ops are low-frequency, frequently shell out to privileged tools,
and must not crash the worker.

**Trade-off.** Two plugin ABIs in one codebase, and a fork per operation. The
fork cost is irrelevant at task-start frequency; the second ABI is the real
price, paid because the failure domains genuinely differ.

v1 ships `local-dir`, `host`, `nfs`, and `tmpfs` as in-process built-ins behind
the same `VolumePlugin` trait, so the zero-dependency path needs no external
binaries and nothing downstream knows whether a driver is in-process or a
subprocess. (The plan originally named only `local-dir` and `host`.)

## 3. Four operations, not the seven CSI implies

**Call.** The v1 contract is `capabilities`, `mount`, `unmount`, `destroy` — all
required to be idempotent. `create`, `attach`, `detach`, `snapshot`, `restore`,
`clone`, and `expand` are **reserved names**: a plugin may advertise them in
`capabilities.ops`, and Legion will dispatch them when those features land,
without an ABI break.

**Alternatives.** The plan's seven-operation set, mirroring CSI's `CreateVolume`
/ `ControllerPublish` / `NodeStage` / `NodePublish` lifecycle. Every storage
engineer recognises it, and it is what a CSI bridge maps onto most directly.

**Trade-off.** A driver that genuinely wants a separate staging step — one device
staged per node, published into several consumers — has to do it inside `mount`.
Nothing in v1 needs that, and the reserved names make adding it additive rather
than breaking.

**Why (simplicity).** CSI needs stage-vs-publish because many pods share one
staged volume per node, and controller-publish because cloud disks attach
out-of-band. Legion has **at most one live allocation per instance by
construction** — that is the node-loss fencing invariant, not a coincidence — so
there is never a second consumer to stage for. Six of the seven operations
collapse into two useful ones: a plugin author writes four functions instead of
seven, and three of them are one-liners for a directory-backed driver.

## 4. `volume_mode: filesystem | block`

**Call.** Every volume is handed over one of two ways, declared as `mode` on
`stateful.storage` / `volumes[]` and defaulting to `filesystem`. `mount` returns
`mountPath` for filesystem and `devicePath` + `fs` for block. Task kinds declare
their side via `TaskPlugin::supported_volume_modes()`, and admission intersects
**task kind ∩ plugin ∩ requested mode**, naming all three when they disagree:

```
task db stateful.storage: task kind `exec` cannot consume a `block` volume (it
supports: filesystem). Filesystem volumes are directories a process reads; block
volumes are devices a guest attaches
```

**Alternatives.** (a) Leave the contract filesystem-only and let a microVM task
kind carry its own disk-image mechanism — which is what the sandbox branch was
doing, keyed off a scraped `LEGION_INSTANCE_ID`. (b) Always return a path and
make block consumers loop-mount it themselves, which needs privileges in the
consumer.

**Trade-off.** Every driver now has to answer "which modes do you serve", and
`local-dir` grew a `mkfs` dependency on the block path (advertised only where a
matching `mkfs` exists). Accepted because the alternative is worse: a parallel
storage mechanism inside one task kind gets none of the volume table, the
pinning, the reclaim, or the CLI.

**Why (extensibility).** A guest kernel consumes a block device, not a host
directory — a scheduler whose storage contract cannot express that has closed the
door on a whole class of workload. Kubernetes made the same split for the same
reason, which is also the argument that it is generic rather than
microVM-specific: nothing hypervisor-shaped entered the contract.

## 5. `volume delete` refuses while anything runs against the volume — at both layers

**Call.** `DeleteVolume` returns `FailedPrecondition`, naming the instance or
allocation that holds the volume, while any live workload mounts it. `--force`
overrides the *manager's* check — it abandons the row without driver
confirmation — but the second layer is independent: a worker refuses a
`destroy` request for a volume it currently has mounted, so even `--force`
cannot reclaim storage under a live mount. Deleting storage means first
expressing intent about the *workload* (`legion job stop`,
`legion instance stop/destroy`), then about the data.

**Alternatives.** (a) Cascade: have delete stop the workload itself, K8s-PVC
style, one command doing both. (b) Trust the operator: delete means delete,
immediately, like `rm`.

**Trade-off.** Reclaiming storage from a stuck-but-alive workload is two
commands instead of one. Accepted — the second command is the one that stops a
database.

**Why (safety, defence in depth).** The command names a volume, not a workload;
inferring "and kill whatever uses it" from a storage command is exactly the
class of surprise an orchestrator should not spring. Two independent layers
because the manager's view can be stale: even a `--force` issued against a
wedged cluster cannot `rm -rf` a directory a live database is writing, because
the worker holding the mount is the one asked to do it, and it declines.

## 6. Driver calls never run under the worker's task-manager lock

**Call.** `WorkerVolumeManager` is shared as an `Arc` with interior mutexes that
are never held across a driver call, and heartbeat-dispatched destroys run on a
blocking task *outside* the task-manager lock. Mount results and destroy
confirmations travel through a report queue drained by the next heartbeat.

**Alternative.** Run driver operations inline where they are triggered, under
the task-manager lock — the first implementation, and the natural one.

**Trade-off.** More architecture: a handle to clone, interior locking, a report
queue, and an in-use set that must be pruned per request window. Accepted.

**Why (a storage plugin must not be able to take down a node).** A driver is
arbitrary third-party code, and `nfs` operations in particular can hang on a
dead server for minutes. Held under the task-manager lock, that hang blocks
heartbeats; blocked heartbeats look like a dead node; the manager confirms the
loss and the worker self-fences — a wedged *volume op* escalating to the fencing
machinery killing healthy tasks. The lock split caps the blast radius of a bad
driver at the volume it was touching.

## 7. `path` means different things per task kind, and the docs say so

**Call.** For `container`, `path` is a real bind-mount destination and is
required. For `exec` there is no filesystem view to mount into, so `path` is
recorded and displayed as intent while the task reads the **host** path from
`$LEGION_VOLUME_<NAME>` (plus `$LEGION_STORAGE_DIR` for the instance volume).

```jsonnet
// exec — read the env var
config: { command: ['redis-server', '--dir', '$LEGION_STORAGE_DIR'] }

// container — the path is real
stateful: { mode: 'slot', storage: { path: '/var/lib/postgresql/data' } }
```

**Alternatives.** (a) Honor `path` for exec too, by creating a bind mount or a
symlink at that location on the host. (b) Reject `path` on exec tasks outright,
making the asymmetry a validation error rather than a documentation problem.

**Trade-off.** One field means two things, which is exactly what generates
confused bug reports. Mitigated by stating it plainly in `docs/volumes.md` with a
per-kind table, and by both guides using the form that matches their kind. (b)
was tempting but punishes the common case of moving a job between kinds.

**Why (simplicity, over purity).** An `exec` task runs in the host mount
namespace. Making `path` real there means Legion mutating `/var/lib/db` on the
operator's machine because a jobspec asked — an unrequested, unscoped change to a
host, needing privileges. An honest asymmetry beats a uniform surface that lies.

## 8. Two declaration sites: `stateful.storage` and `task.volumes[]`

**Call.** Per-instance storage stays on `stateful.storage` (one volume per
replica, provisioned and reclaimed with the instance). Claims on storage that
already exists get a separate `volumes: [...]` list, valid on **stateless tasks
too**. Both compile to the same mount list on the wire and share one worker
pipeline.

**Alternative.** One unified `volumes[]` array with a per-entry flag
distinguishing "provision me one per replica" from "mount this existing thing".
Fewer concepts on the page.

**Trade-off.** Two fields to learn instead of one, and a reader has to understand
why. Accepted because the flag version hides the difference that actually matters
— lifecycle — behind a boolean.

**Why (simplicity).** They are different lifecycles, not different options: one is
reclaimed by instance churn, the other never is; one is single-writer, the other
is shared. K8s splits them (`volumeClaimTemplates` vs `volumes`) and Nomad splits
them, for this reason. The win is that a stateless inference job can mount
`/mnt/models` read-only without pretending to be stateful.

## 9. Reclaim is reconciled, not triggered

**Call.** A background loop marks any volume that no instance references as
`destroying`. Nothing at the four handle-drop sites (keyed destroy, slot scale-in
without `retainOnScaleIn`, `retainOnStop: false` on job stop, job destroy)
explicitly frees storage.

**Alternative.** Instrument each drop site to mark its volume — the direct,
obvious implementation, with no polling delay.

**Trade-off.** Reclaim is eventual rather than immediate, bounded by the
node-status check interval, and a background loop scans a table that mostly has
nothing to do. Accepted for the correctness property it buys.

**Why (correctness).** With triggers, any *future* code path that drops a handle
without knowing about volumes silently leaks storage, and the failure is
invisible until a disk fills. With one reconciling rule — "no instance references
this" — there is no path to forget. The same rule also catches a volume orphaned
by a crash between the handle drop and the mark.

## 10. Node-local volume sizes count against node disk; remote ones don't

**Call.** Placement subtracts the declared `size` of every **node-local** volume
(`local-dir`, `host`, and any exec driver advertising `nodeLocal`) from the
node's fingerprinted free disk, accumulating across allocations planned in the
same eval. Network (`nfs`) and RAM-backed (`tmpfs`) volumes are exempt — their
bytes are not on the node's disk. The failure names the fix:

```
no node has enough free disk for its node-local volume(s). Shrink the volume
`size`, add disk, or use a network driver (nfs) whose capacity does not come
from the node
```

**Alternatives.** (a) Treat `size` as purely advisory and let mounts fail (or
disks fill) at runtime. (b) Go further and enforce `size` as a runtime quota via
filesystem projects/loop devices.

**Trade-off.** `size` is a placement claim, not a quota — a task can still write
past it, and a volume that declares no size is not counted. Both stated in
`docs/volumes.md` rather than implied.

**Why.** A 100G `nfs` volume on a node with a 1G root disk is *fine* and must
not be blocked; a 100G `local-dir` on a 10G disk is a guaranteed later failure
that placement had the information to prevent. Failing at eval with a named
reason beats failing at mount at 3am. Quotas are a driver feature to grow into
(`expand` is already reserved), not a scheduler feature to fake.

## 11. The config word is `driver`; "plugin" is only the packaging

**Call.** Everything a user selects or sees says **driver**: `storage: { driver:
'nfs' }`, `legion volume drivers`, `legion volume driver test`, the `DRIVER`
column, `VolumeInfo.driver`, fingerprints' `volumeDrivers`. "Plugin" survives
only where it names delivery: `plugins/volumes/<name>/`, `plugin.json`, and the
plugin-system doc. The rename landed pre-release with `deny_unknown_fields`, so
a stale `plugin:` key fails loudly at submit instead of being ignored.

**Alternatives.** (a) Keep "plugin" everywhere, matching the rest of Legion's
plugin system. (b) K8s vocabulary: `storageClass`/`provisioner`.

**Trade-off.** Two words for one subsystem, needing exactly one sentence of
definition ("a driver is what serves volumes; a plugin is how a driver is
delivered") — and a breaking rename, taken now precisely because nothing public
has shipped.

**Why.** `plugin: 'nfs'` in a jobspec collided head-on with task-kind plugins
(`kind: 'container'` *is* a plugin) and read as "which code do I load" rather
than "which system serves my bytes". Docker and Nomad both call this exact
concept a volume driver, so the word arrives pre-taught. The built-ins also
aren't plugins in any packaging sense — calling them drivers is simply accurate.

## 12. `stateful.mode` defaults to `'slot'`

**Call.** `mode` became optional, defaulting to `slot`, so the minimal stateful
block is `stateful: { storage: { path: ... } }`. Keyed stays an explicit opt-in.

**Alternative.** Keep `mode` required — the original shape, on the theory that
identity semantics are too important to default.

**Trade-off.** A default can be learned by omission: someone may run slot
workloads for months without knowing the word. Accepted; the docs spell the
default everywhere the field appears.

**Why.** Slot is the right answer for essentially every database-shaped
workload — K8s made the same shape *the* StatefulSet, not an option of it.
Keyed mode is the specialised one (you know when you need per-key instances),
and specialised modes are the ones that should cost a line of config. The
zero-decision path producing the safe, ordinary thing is the design goal of the
whole defaults story (`local-dir`, `filesystem`, retain-on-scale-in — and now
`slot`).

## 13. "CNI compatibility layer" in the request → CSI

The request asked for a volume plugin system "similar to CNI or Nomad's volume
plugins" with "a CNI compatibility layer (optional)". CNI is the *networking*
plugin standard; the storage equivalent is **CSI**. We interpreted the intent as
two separate things and did both:

- Adopt the **CNI-style exec-plugin model** (JSON over stdin/stdout, versioned
  contract, manifest + bin-dir discovery) as the native volume ABI.
- Leave a **CSI compatibility bridge** as the documented path to the existing
  driver ecosystem, mapping onto the v1 contract with no core changes.

Alternative considered: literally bridging to CNI plugins — rejected as a
category error (CNI plugins configure network interfaces, not storage).

## 14. One instance volume in v1, with a schema that takes N

**Call.** `stateful.storage` is a single object, not a list. The `volume` table
is nonetheless keyed by `(instance, name)`, so `stateful.volumes: [...]` is purely
additive later — no migration, no handle-format change.

**Alternative.** Ship the list now, since the storage layer already supports it.

**Trade-off.** A workload that genuinely wants two per-instance volumes (data and
WAL on separate devices — the classic database layout) has to wait. Accepted:
`Instance` has one `storage_handle` column, and plumbing N through the binder,
the retention knobs, and the CLI is real work with no current consumer.

**Why (simplicity).** 95% of stateful workloads want exactly one data volume, and
the singular form reads better for all of them.

## 15. Task volume claims are shared per `(job, group, task, name)`

**Call.** A `volumes[]` claim gets a deterministic id derived from those four
values, so every allocation of the task mounts the same volume, and a resubmit
lands on the same row rather than orphaning the old one.

**Alternative.** One volume per allocation, which is what "each task gets its own
scratch" would imply.

**Trade-off.** Two allocations of one task on one node share a `tmpfs` claim,
which surprises anyone expecting private scratch. Documented, with
`$LEGION_TASK_DIR` named as the actual answer for per-allocation scratch — it
already exists and is already cleaned up on stop.

**Why.** "A claim on storage that already exists" is the concept, and sharing is
the point of it: ten replicas mounting one read-only models directory is the
dominant case.

## 16. Provisioning is lazy: the first `mount` creates the storage

**Call.** There is no `create` operation in v1. A volume row is born `pending` and
becomes `created` the first time a worker mounts it.

**Alternative.** An explicit `create` at handle-mint time, as the plan's open
question 1 posed it, with a topology hint so the scheduler picks the node first.

**Trade-off.** The volume row exists before anything backs it, so `pending` means
"named but not yet real". Accepted because the alternative introduces a
created-but-never-mounted state that can leak an external resource.

**Why (simplicity).** Every v1 driver wants lazy anyway — `local-dir` cannot
create a directory before it knows the node, and `host` has nothing to create.
Removing the operation removes a failure mode rather than deferring one.

## 17. A node advertising no volume plugins is assumed to have the built-ins

**Call.** A node whose fingerprint carries no `volumeDrivers` key at all is
treated as providing `local-dir`, `host`, `nfs`, and `tmpfs`. Nodes that *do*
advertise are filtered precisely. Admission skips its check entirely when nothing
in the cluster advertises anything.

**Alternative.** Treat an unadvertised plugin as absent — the strictly correct
reading, and what the code did first.

**Trade-off.** A pre-upgrade worker can be sent a `host` claim it cannot serve,
failing at mount instead of at admission. Bounded and self-correcting: the node
advertises properly the moment it restarts on the new build.

**Why.** The strict version was caught by the test suite stranding every stateful
job until the last worker in a cluster was upgraded. The built-ins ship inside
the binary, so assuming them is not merely convenient — it is true of any worker
running Legion at all.

## 18. A successful mount may correct a stale `destroyed` tombstone, never a `destroying` one

**Call.** `record_volume_mounted` accepts any state except `destroying`. A worker
reporting a successful mount for a tombstoned volume resurrects the row.

**Alternative.** Only ever move `pending → created`, treating the volume row as
authoritative over the worker.

**Trade-off.** A volume force-deleted by an operator can come back if a worker
later re-mounts it — after a worker restart re-establishes mounts for
still-running allocations, say. That is arguably correct (the storage
demonstrably exists and is in use) but it is a surprise, so `--force` documents
that it leaves bytes behind.

**Why (self-healing).** Task volume claims have deterministic ids, so a claim
re-declared after a job destroy lands on the same row. Leaving it `destroyed`
made a genuinely mounted volume invisible in `legion volume list` and immune to
the reaper — found in live testing, not review. A mount is proof the storage
exists; `destroying` is excluded because a reclaim is deliberately in flight and
resurrecting the row would race the driver.

## 19. `clone` is reserved now and implemented later

**Call.** `clone` — "make a new volume that starts as a copy-on-write copy of
this one" — is named in the contract and reserved in `capabilities.ops` alongside
`snapshot`, `restore`, and `expand`. Nothing implements it.

**Alternative.** Implement it for `local-dir` via reflink (`FICLONE` /
`clonefile`) now, since the syscall is a few lines.

**Trade-off.** The forking workflow the sandbox work wants still has no
implementation. Accepted: settling the *name* is what prevents the ABI break, and
building the semantics without a consumer to test against means guessing at the
wrong shape — particularly around cross-node fan-out, where a node-local clone
can only ever produce forks pinned to the origin node.

## 20. A driver may return a device that is not yet fully populated

**Call.** `mount` is explicitly allowed to hand back a device backed by lazy
fetch — an NBD/FUSE device over a content-addressed base, a qcow2 backing chain,
dm-thin over a sparse file — so a consumer can start before the data finishes
syncing.

**Trade-off.** None to the contract; it is a permission, not a mechanism. The
cost is that a consumer cannot assume a returned device is fully local, which is
now stated rather than left to be discovered.

**Why (extensibility).** Fast-start disks need no new contract surface *if*
plugin authors know they may rely on this. An unstated property is one nobody
builds against.

## 21. Coexistence with real CNI is structural, not configured

**Call.** Nothing was changed to "support" hosts that also run CNI networking
plugins, because the isolation is already structural, and
`docs/plugin-system.md` now says so: Legion discovers volume plugins only under
its own `plugins/volumes/` tree (never `/opt/cni/bin` or `/etc/cni/net.d`), and
the ABIs differ on purpose — op as `argv[1]` + self-describing JSON vs CNI's
`CNI_COMMAND` env var — so neither kind of binary can be half-run as the other.

**Alternative.** Make the volume exec ABI literally CNI-wire-compatible so
existing plugin tooling could be reused.

**Why rejected.** Category error (see entry 13): CNI binaries configure network
interfaces, and wire-compat would *invite* dropping them into the volume path,
converting a loud `capabilities` failure into subtle half-operation. Reusing the
actual CNI ecosystem is the planned `datapath` plugin kind's job — a shim that
speaks real CNI to real CNI plugins.
