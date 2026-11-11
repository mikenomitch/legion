# Plan: Sandbox Snapshots, Fast Start, and Forking

> **Judgement calls:** [sandbox-snapshots-and-fast-start-plan_calls.md](../calls/sandbox-snapshots-and-fast-start-plan_calls.md)

> **Superseded in part (2026-10-09) by
> [`suspend-resume-and-oversubscription-plan.md`](suspend-resume-and-oversubscription-plan.md).**
> That plan wins on scope, phasing, the warm-pool and idle-handling stance
> (suspend and idle policy now live inside Legion), blob transport (the node
> content store and location index), and store configuration (per worker, not
> `snapshots.stores`). This document remains the reference for capture
> mechanics, the manifest and `compat` block, quiesce hooks, the fork
> identity-collision analysis, and lazy memory. Its open questions 4 and 5 are
> answered there. Its phase list below is no longer the execution order.

> **Status:** PLANNED — not implemented. Design for snapshot/restore, sub-second
> starts, and forking on the [`sandbox` task runtime](../../docs/task-runtime-sandbox.md).
>
> **This plan is a consumer of two existing plans, not a peer of them.**
> [Stateful workloads](stateful-workloads-and-instance-identity-plan.md) owns
> instance identity and the instance→storage binding;
> [the volume plugin system](volume-plugin-system-plan.md) owns what that
> binding *is* and how storage is provisioned, attached, and reclaimed. This
> plan adds the two things neither covers: **capturing a point in time** and
> **starting from one fast**. §0 records the convergence that made this
> possible — it has since SHIPPED (the sandbox runtime consumes block-mode
> volumes; its interim storage is gone).
>
> Also composes with:
> [artifact cache and pre-warming](artifact-cache-and-prewarm-plan.md) (blob
> transport, node cache, region locality) and
> [exec and actions](../../docs/exec-and-actions.md) (quiesce hooks).

## The Problem

The sandbox runtime boots an OCI image in a microVM. Every start is a cold start:
build a root disk, boot a kernel, run userspace init, run the command. For an
agent workspace or a per-request sandbox that is 1–10 seconds of dead time, and
the workload's in-memory state is lost every time it stops.

Three capabilities close that gap, and they share most of their machinery:

- **Snapshot / restore** — capture a sandbox (disk, or disk + memory) and bring
  it back exactly where it left off.
- **Fast start** — restore in tens of milliseconds instead of booting, including
  starting the VM *before* its state has finished arriving on the node.
- **Fork** — start N sandboxes from one snapshot, each diverging independently.

## Scenarios

**S1 — Checkpoint an agent workspace.** An agent sandbox has a warm language
server, a populated build cache, and an open REPL. Snapshot it on idle; restore
it 6 hours later with all of that intact.

**S2 — Sub-second cold start.** A request arrives for a sandbox that isn't
running. Restore from a base snapshot in ~50 ms rather than booting for 3 s.

**S3 — Fork a prepared environment.** One sandbox installs dependencies and
loads a dataset into RAM; snapshot it once, then fork 50 copies. Each sees the
prepared state and diverges from there.

**S4 — Restore in the right region.** A snapshot taken in `us-west` is restored
in `us-west` from a regional store, not pulled across an ocean.

**S5 — Roll back a bad change.** Snapshot before a risky operation; restore the
instance to that point on failure.

---

## 0. Reconciling With Stateful Workloads and Volumes

> **Status: SHIPPED.** This section was written when the sandbox runtime carried
> its own interim storage (`config.state`) and the volume system was still a
> plan. Both halves have since landed and converged: the volume system shipped
> with every extension this section demanded, and the sandbox runtime was
> reworked to consume volumes directly *before ever shipping in a release* —
> so `config.state` was **deleted outright** rather than kept as a desugaring
> shorthand (nothing existed to stay compatible with; the runtime now rejects the
> field with the migration spelled out). The section is kept as the design
> record; the user-facing result is [docs/volumes.md](../../docs/volumes.md)
> and [docs/task-runtime-sandbox.md](../../docs/task-runtime-sandbox.md).

### What the two plans own (both implemented)

[Stateful workloads](../../docs/stateful-workloads.md) gives every stateful
task an **instance** whose id survives reschedules, with at most one live
allocation at a time, and an opaque **`storage_handle`** minted on first
placement and reattached on every later placement.

[The volume plugin system](volume-plugin-system-plan.md) fills in what a
handle *is*: a `volumes` table with a state machine, the driver contract,
`local-dir`/`host`/`nfs`/`tmpfs` built-ins, exec drivers for everything else,
node-local pinning in the scheduler, orphan reconciliation, and two-phase
destroy. `snapshot` / `restore` / `clone` / `expand` are reserved contract
names, advertised via `capabilities.ops` but not yet dispatched — **they are
this plan's insertion point.**

### The mismatch that had to be solved: `mount_path` vs. block devices

A `mount` that returns a host **filesystem path** is exactly right for `exec`
(chdir into it) and `container` (bind mount it) — and useless for a microVM: a
guest kernel consumes a *block device* (virtio-blk), not a host directory.
The contract therefore had to express **shape**, mirroring Kubernetes'
`volumeMode: Filesystem | Block`, which exists for the same reason. (The
alternative bridge — virtio-fs, a `virtiofsd` per mount — keeps filesystem
semantics at the cost of a daemon per volume and a much fuzzier snapshot
story; it remains future work on the runtime, not the contract.)

### What shipped, item by item

Everything this section originally demanded of the volume plan landed:

- **(a) `volume_mode`** — in driver `capabilities` (`volumeModes`), the job
  spec (`storage.mode: 'block'`), and admission, which intersects task runtime ×
  driver × request and rejects mismatches at submit. `mount` returns
  `device_path` + `fs` + `fresh` in block mode; **the driver formats, never
  the guest**, and a remount never reformats (conformance-tested).
  `local-dir` serves a sparse ext4 image; `host` serves operator-declared
  devices. Task runtimes declare their side via
  `TaskPlugin::supported_volume_modes()` — `sandbox` returns
  `[Block, Filesystem]`, block for microVM backends, filesystem for its
  dev-mode backends (bind mounts).
- **(b) `clone` as a reserved op** — in the contract's op enum, advertisable
  in `capabilities.ops`. Dispatch is this plan's work (Part III needs it for
  forking; reflink/`zfs clone`/dm-thin/EBS-from-snapshot are the backends
  that make forking viable).
- **(c) the resolved volume reaches the worker** —
  `TaskAllocation.task_volumes` carries the resolved mount list; the worker
  mounts before `prepare` and hands runtimes typed `MountedVolume`s
  (mode, host device/dir path, guest path, fs, fresh, read-only) on
  `TaskRuntime.volumes`. This retired `config.state`.

### The converged model (as built)

```
stateful.storage          (core: instance-bound, retention policy)
        │
        ▼
   storage_handle  ──►  volumes table  ──►  volume driver
                                              │
                          volume_mode: block ─┴─ filesystem
                                   │                │
                        sandbox: virtio-blk    exec/container: bind mount
                        (guest init mounts     (host path / bind dest)
                         /dev/vdb at `path`)
```

### What this buys the snapshot design

Everything below gets simpler, because the disk half of a snapshot stops being
sandbox-specific:

- **Disk snapshot is a volume op**, so `exec` and `container` tasks get
  snapshot/restore of their state for free. A per-runtime disk-snapshot mechanism
  would have been the third storage mechanism in this system.
- **Lifecycle machinery already exists** — the `volumes` table, its state
  machine, two-phase destroy, orphan reconciliation, and node-local pinning.
  This plan adds a `snapshot` table and reuses the rest rather than restating it.
- **Retention has one owner.** `retainOnScaleIn` / `retainOnStop` govern the
  *volume*; snapshots get their own TTL because a snapshot is not the volume and
  is meant to outlive it.
- **Node affinity for restore is the volume plan's `node_local` pinning**, not a
  new concept — combined with the artifact cache's warm-node scoring for the
  blobs that must move.

---

## Design Principles

### 1. Snapshots are instance operations, not actions

An [action](../../docs/exec-and-actions.md#actions) is a server-rendered command
template executed **inside** the workload. Wrong shape for a snapshot:

- **It runs in the wrong place.** Snapshotting is the hypervisor pausing the VM
  and serializing memory and device state, or the storage backend cloning a
  volume. Nothing inside the guest can do either, and nothing inside the guest
  should be trusted to.
- **The result isn't an exit code.** It is a manifest plus content-addressed
  blobs that must be recorded, garbage-collected, and located again later.
- **The authorization differs in kind.** `action:write` says "may run
  `flush-cache`." A **memory snapshot is a complete copy of the sandbox's RAM** —
  every secret, token, and key the workload has touched. That must not be
  reachable through an action grant, and it is not `job:write` either.

Snapshots get their own ACL resource and RPCs scoped to the **instance** — the
identity the stateful plan already defines. Actions serve at the other end, as
**quiesce hooks** (§2.4).

### 2. A snapshot is (volume snapshot) + (VM memory snapshot)

Two halves with different owners, captured under one pause:

| Half | Owner | Applies to |
| --- | --- | --- |
| Volume snapshot | volume plugin (`snapshot` op) | every stateful runtime |
| Memory + device state | task runtime (VMM) | `sandbox` only |

`kind: "disk"` is the first half alone and works for `exec`/`container` too.
`kind: "full"` is both, taken with the VM paused across the pair so they are a
single consistent instant.

### 3. Snapshot blobs are artifacts

A memory image is a large immutable content-addressed blob. Legion already has a
pipeline for that: fetch → verify → cache → advertise → pre-warm → score
placement toward warm nodes. Snapshots ride it rather than growing a second
transport, which answers most of node caching and region locality for free.

Volume snapshots that live natively in a storage backend (a ZFS snapshot, an EBS
snapshot) are **not** streamed through the CAS — the manifest records a
backend-native reference and the plugin restores from it in place.

### 4. Full snapshots by default; diffs opt-in

Diff chains are smaller but turn deletion into refcounted GC over a DAG, and a
broken link is unrecoverable. Ship full-only; add `--diff` when a workload
measurably needs it.

### 5. Restore compatibility is a placement constraint

A memory snapshot resumes only on a machine whose CPU exposes the features the
guest saw, by the same VMM, at the same guest size. The manifest records those
facts and the scheduler turns them into constraints **before** placing —
composed with the volume plan's `node_local` pinning and the artifact cache's
warmth scoring, so a restore lands somewhere that can run it *and* already holds
the bytes.

---

## Part I — Snapshots

### 1. Interface

```bash
legion snapshot create user-42 --name pre-upgrade      # disk only (volume snapshot)
legion snapshot create user-42 --memory --name warm-base

legion snapshot list --job agent-sandboxes -j
legion snapshot get snap-7f3a -j

legion snapshot restore snap-7f3a --instance user-42   # in place
legion instance create agent-sandboxes --from-snapshot warm-base --id user-99

legion snapshot delete snap-7f3a
legion snapshot warm warm-base --constraint region=us-west
```

Per repo conventions each command takes `--json`/`-j` with the standard
envelope, lands in `catalog()` in `src/cmd/capabilities.rs`, gets an MCP tool
(mutating ones behind `--allow-write`), and accepts `--idempotency-key`.

RPCs: `CreateSnapshot`, `ListSnapshots`, `GetSnapshot`, `DeleteSnapshot`,
`RestoreInstance`, `WarmSnapshot`. New ACL resource `snapshot`, scoped by job
name. Because a memory snapshot is a RAM dump, `snapshot:write` should be
documented in `docs/auth-and-acls.md` as equivalent to read access to everything
that job handles.

### 2. Manifest

Note that the disk half is a **reference to a volume snapshot**, not an embedded
blob — that is the merge with the volume plan made concrete.

```jsonc
{
  "apiVersion": 1,
  "id": "snap-7f3a91",
  "name": "warm-base",
  "job_id": "agent-sandboxes",
  "task": "workspace",
  "origin_instance": "user-42",
  "kind": "full",                          // "disk" | "full"
  "created_at": "2026-07-26T09:14:03Z",
  "parent": null,

  // Disk half: whatever the volume plugin returned from its `snapshot` op.
  "volume_snapshot": {
    "volume_id": "vol-9f2c",
    "plugin": "local-dir",
    "mode": "block",
    "ref": { "kind": "cas", "digest": "sha256:ab…", "size": 4294967296 }
    // or  { "kind": "native", "attrs": { "dataset": "tank/legion/vol-9f2c@snap-7f3a91" } }
  },

  // Memory half: sandbox-only, always CAS blobs.
  "memory": [
    { "role": "memory",   "digest": "sha256:9c…", "size": 1073741824 },
    { "role": "vm-state", "digest": "sha256:41…", "size": 18244 },
    { "role": "vm-config","digest": "sha256:d1…", "size": 1102 }
  ],

  "compat": {
    "vmm": { "type": "firecracker", "version": "1.7.0" },
    "guest": { "vcpus": 2, "memory_mib": 1024 },
    "kernel_digest": "sha256:77…",
    "cpu": { "vendor": "GenuineIntel", "model": "6:106:6", "features": ["avx2","avx512f"] }
  }
}
```

State store — a `snapshot` table beside the volume plan's `volumes` table,
reusing its two-phase-delete discipline rather than restating it:

```sql
snapshot(snapshot_id PK, job_id, task_name, origin_instance_id, name,
         kind, state, manifest_json, total_bytes, parent_snapshot_id,
         created_at, expires_at)
snapshot_alias(job_id, name, snapshot_id, updated_at, PRIMARY KEY(job_id, name))
```

`state ∈ { capturing, ready, failed, deleting }`. `--name` is a repointable
per-job alias, which is what makes `--from-snapshot warm-base` stable while the
base is rebuilt weekly. Instances gain `restored_from_snapshot_id` for
provenance.

**Retention has one owner per object.** The *volume*'s lifetime is governed by
`retainOnScaleIn` / `retainOnStop` and the volume plan's orphan reconciler —
this plan does not touch it. *Snapshots* get `snapshots: { ttl: '7d', keep: 10 }`
plus explicit delete, and are deliberately **not** cascaded by instance destroy
(capture, destroy the sandbox, restore next week is the point). They *are*
cascaded by job destroy, matching instance history.

Deleting a snapshot calls the volume plugin's `snapshot`-delete for
`kind: "native"` refs and drops CAS blobs for `kind: "cas"` ones. A snapshot
referenced by a live `clone` (fork) is pinned until the clone is destroyed —
the one refcount this plan adds, and only for backends where a clone depends on
its source (ZFS, dm-thin; reflink clones are independent immediately).

### 3. Capture mechanism

**Disk half** — the volume plugin's `snapshot` op, promoted from the volume
plan's reserved list to implemented. Per backend:

| Backend | Mechanism | Cost |
| --- | --- | --- |
| `local-dir` (block) on btrfs/XFS | `FICLONE` reflink of the image | instant, COW |
| `local-dir` (block) elsewhere | sparse copy into the CAS | O(used bytes) |
| `zfs` | `zfs snapshot` | instant, native ref |
| `dm-thin` | thin snapshot | instant, native ref |
| cloud (`csi-bridge`) | provider snapshot API | provider-side |

**Memory half** — both VMMs implement pause → serialize → resume. The task runtime
grows:

```rust
fn supports_snapshot(&self) -> SnapshotSupport;
fn snapshot(&self, allocation, task, runtime, opts) -> R<MemoryParts>;
fn restore(&self, allocation, task, runtime, manifest) -> R<()>;
```

*cloud-hypervisor* works against the API socket the runtime already creates:

```
ch-remote --api-socket <sock> pause
ch-remote --api-socket <sock> snapshot file:///<out-dir>
ch-remote --api-socket <sock> resume
```

*Firecracker* needs one launch change. The runtime boots with `--no-api
--config-file` today, which leaves no control channel; snapshots require
`--api-sock <path>` **alongside** the config file, so the VM still boots from
JSON but stays controllable:

```
PATCH /vm             {"state": "Paused"}
PUT   /snapshot/create {"snapshot_type":"Full","snapshot_path":…,"mem_file_path":…}
PATCH /vm             {"state": "Resumed"}
```

Diff snapshots additionally need `track_dirty_pages: true` at boot — another
reason to keep them opt-in.

**Ordering for `kind: "full"`:** quiesce → pause VM → volume `snapshot` → VMM
snapshot → resume VM → unquiesce. Pausing across both halves is what makes disk
and memory a single instant.

> Verify flags against the VMM versions you deploy; both projects have moved
> their snapshot surfaces between releases. `compat.vmm.version` exists so a
> restore can refuse a mismatch rather than produce a corrupt guest.

### 4. Consistency: quiesce hooks

A volume snapshot of a live filesystem is crash-consistent — usually fine (ext4
replays its journal), occasionally not. This is where actions belong:

```jsonnet
actions: [
  { name: 'pre-snapshot',  command: ['/usr/sbin/fsfreeze', '-f', '/workspace'] },
  { name: 'post-snapshot', command: ['/usr/sbin/fsfreeze', '-u', '/workspace'] },
],
snapshots: { quiesce: { pre: 'pre-snapshot', post: 'post-snapshot', timeout: '10s' } },
```

The manager runs `pre` through the existing action path, captures, then *always*
runs `post` — including on failure, or the guest stays frozen. A failed `pre`
aborts by default (`onQuiesceFailure: 'abort' | 'proceed'`).

This matters far less for `kind: "full"`, where the VM is paused before either
half is captured. **Memory snapshots are the consistent option; disk-only
snapshots are the cheap one** — and disk-only is the one every runtime gets.

---

## Part II — Fast Start

Five techniques, biggest win first; they compose.

**1. Restore instead of boot (~10–100 ms).** Skips kernel boot, init, and
application warmup. Needs nothing beyond Part I; everything else stacks on it.

**2. Lazy memory — start before RAM arrives.** Firecracker accepts a
**userfaultfd** memory backend
(`mem_backend: { backend_type: "Uffd", backend_path: … }`): it sends the guest
memory mappings and a `userfaultfd` over a socket, and a handler serves pages on
demand. The VM resumes as soon as device state loads; pages stream in behind it.

The handler is a **separate process**, for the same reason the volume plan
exec's its plugins: perf-critical, unforgiving kernel interface, and a crash
must not take down the worker and every other allocation on the node.

```
legion-uffd-handler --socket /run/legion/uffd-<alloc>.sock \
                    --source file:///var/lib/legion/cache/blobs/sha256/9c… \
                    --prefetch-ahead 64MiB
```

Source order: local CAS (mmap, ~free) → peer node in region → regional object
store. Remote sourcing is what makes restoring a 4 GiB VM on a cold node take
milliseconds — the guest touches a small working set and the rest is never
fetched. Treat lazy memory as **Firecracker-first**; cloud-hypervisor restores
eagerly until its shared-memory story firms up.

**3. Lazy disk — a volume plugin concern**, which is the cleanest consequence of
the merge. "Produce a device usable immediately that fetches on read" has
backend-specific answers (qcow2 backing chain, NBD/FUSE lazy fetch, dm-thin over
a sparse device, overlay over a CAS base), and they all fit behind `mount`
returning a `device_path`. Recommended: implement `clone`-from-local-base via
reflink in `local-dir` and leave fetch-on-read to plugins; the common case is
"the base is already on this node," which pre-warming makes true.

**4. Pre-warming and region-local fetch** — directly the
[artifact cache plan](artifact-cache-and-prewarm-plan.md): snapshot blobs share
the node CAS (budget, LRU, TTL, pinning), workers advertise contents, the
scheduler scores warm nodes higher. `legion snapshot warm <snap> --constraint
region=us-west` pre-stages ahead of demand, authenticating with node identity,
and never blocks placement.

Region locality uses node attributes that already exist (`region: us-west`):

```jsonnet
snapshots: {
  stores: {
    'us-west':    { url: 's3://legion-snapshots-usw/', region: 'us-west-2' },
    'eu-central': { url: 's3://legion-snapshots-euc/', region: 'eu-central-1' },
  },
  defaultStore: 'us-west',
}
```

Fetch order **local CAS → regional store → origin-region store**. Cross-region
replication is explicitly not Legion's job: configure bucket replication or run
a replicator. A first cross-region fetch populates the local CAS, so the second
sandbox in that region is fast regardless.

**5. Warm pools — outside Legion.** Keeping N sandboxes pre-restored and idle is
policy, already expressible on the instance API (create keyed instances from a
base snapshot ahead of demand, hand out ids, destroy on release). Legion should
make `create --from-snapshot` fast enough that the pool can be small, not grow a
pooling scheduler.

---

## Part III — Forking

```bash
legion snapshot create user-42 --memory --name prepared
legion instance create agent-sandboxes --from-snapshot prepared --count 50
```

Three mechanisms, two of them nearly free:

- **Memory** — each fork maps the same blob copy-on-write (`MAP_PRIVATE`, or a
  shared read-only page source behind UFFD). 50 forks of a 1 GiB snapshot start
  at ~1 GiB total and grow only as they diverge.
- **Disk** — the volume plugin's new `clone` op per fork: reflink, `zfs clone`,
  thin snapshot. Instant when the base is local. Each fork gets its own
  `volume_id` in the `volumes` table, so the volume plan's existing lifecycle,
  node pinning, and reclamation apply unchanged.
- **Identity** — the hard part.

### The identity collision problem

Every fork resumes believing it is the sandbox that was snapshotted: same
hostname, same `LEGION_INSTANCE_ID` (baked into `/legion/env` at disk-build
time), same MAC, same random seed, same frozen clock, same open connections.
Fifty forks with one identity is precisely the invariant the
[stateful plan](../../docs/stateful-workloads.md) exists to protect.

**Host half — per-fork devices at load time.** Reserve a small (~1 MiB)
**identity disk** as an extra device *when the snapshot is taken*. At restore,
before resuming, point it at a freshly generated per-fork image and give the NIC
a fresh MAC. Firecracker: load with `resume_vm: false`, `PATCH /drives/identity`
and `PATCH /network-interfaces/eth0`, then resume. Reserving the slot at snapshot
time is what makes this work at all — you cannot hotplug into a restored
Firecracker VM, but you can repoint a device that was already there. (MMDS is an
alternative for pure metadata and rides in the snapshot.)

**Guest half — a resume hook.** The generated `/legion/init` is a shell script
that runs once at boot; a fork needs work done *after resume*, which init never
sees. That calls for a small static **`legion-guest-agent`** baked into the root
disk, which blocks on a resume signal and then:

1. re-reads identity (`LEGION_INSTANCE_ID`, hostname, ports) from the identity
   disk and re-exports it;
2. reseeds `/dev/urandom` — forks sharing an entropy pool generate colliding
   keys, UUIDs, and session ids;
3. resyncs the clock, frozen since the snapshot;
4. reconfigures the NIC for its new MAC/IP;
5. runs an optional user `onResume` hook so the app can reconnect pools.

Steps 2–3 are not niceties; both VMM projects call them out, and getting them
wrong surfaces weeks later as duplicate UUIDs. The guest agent is the one
genuinely new component this plan requires.

**Fork semantics in the instance model.** A fork is a **new keyed instance** —
new id, never recycled, its own volume, its own workload identity. It is not a
second live allocation of the snapshotted instance, so the
at-most-one-live-allocation invariant holds untouched. The origin instance may
be running, stopped, or long destroyed.

---

## What Lives Where

| Concern | Where | Why |
| --- | --- | --- |
| Instance identity, at-most-one-live-allocation, retention policy | **Stateful plan** (implemented) | Already the owner; this plan adds nothing. |
| Volume provisioning, attach/mount, node pinning, reclamation, `snapshot`/`clone` ops | **Volume plan** (+ this plan's extensions) | Backend-specific by nature; every runtime benefits, not just sandboxes. |
| `volume_mode: block`, `clone` op, volume ref in `TaskAllocation` | **Volume plan extension** (§0) | Prerequisite: without it a microVM cannot consume a volume at all. |
| Snapshot RPCs, ACL resource, `snapshot` table + aliases, TTL | **Legion core** | Identity, lifecycle, and authorization are core's job. |
| Manifest format + `compat` metadata | **Legion core** | The contract every other piece reads. |
| Memory/device capture + restore | **Legion core** (task runtime) | The runtime owns the VMM connection. |
| Blob transport, node cache, pre-warm, region routing | **Legion core**, reusing the artifact pipeline | Already planned; a second transport would be duplication. |
| Restore-compatibility placement constraints | **Legion core** (scheduler) | Only the scheduler can act before placement. |
| `legion-guest-agent` | **Legion-owned, separate binary** | Must run inside the guest; too coupled to the init contract to be third-party. |
| UFFD page-fault handler | **Separate process, Legion-shipped, replaceable** | Perf-critical and crash-prone; must not take the worker down. |
| Cross-region replication | **Outside** (object store / replicator) | Storage systems solve this well already. |
| Warm pools, fork fan-out policy | **Outside**, on the instance API | Policy; every team wants a different one. |

The through-line: Legion owns **identity, lifecycle, authorization, and
placement**. Storage mechanics live behind the volume contract. Anything
perf-critical, crash-prone, or a policy choice lives in a separate process or
outside the system.

---

## Implementation Phases

Phase 0 was the merge work, a prerequisite for everything else.

0. ~~**Converge storage.**~~ **DONE** — shipped across the volume system v1
   and the sandbox-runtime rework: `volume_mode` in the contract and job spec,
   `local-dir` block mode, the resolved volume list on `TaskAllocation`,
   `supported_volume_modes()` on `TaskPlugin`, and the sandbox runtime consuming
   volumes directly (`config.state` deleted before ever shipping — rejected
   at validation with the migration, no desugar needed).
   *Exit criterion holds:* a sandbox's `/workspace` is an ordinary Legion
   volume, visible in `legion volume list`, pinned by the scheduler, reclaimed
   by the volume reconciler — and `configs/jobs/sandbox-microvm.jsonnet`
   declares it once.
1. **Disk snapshots.** Volume `snapshot` op (promoted from reserved),
   `local-dir` reflink implementation, `snapshot` table + aliases, CRUD RPCs +
   CLI + MCP, ACL resource. Works for `exec` and `container` too.
2. **Restore in place.** `RestoreInstance`: stop allocation → volume `restore` →
   start. Provenance on the instance. Quiesce hooks wired to actions.
3. **Memory snapshots.** Firecracker `--api-sock`; pause/create/resume; `compat`
   in the manifest; restore-compatibility constraints in the scheduler.
4. **Fast start.** Regional snapshot stores, `legion snapshot warm`, cache
   advertisement and warm-node scoring for snapshot blobs. Measured cold-vs-warm
   numbers in `docs/testing-and-benchmarks.md`.
5. **Forking.** Volume `clone` op, identity disk, per-fork device override,
   `legion-guest-agent`, `instance create --from-snapshot --count N`.
6. **Lazy memory.** `legion-uffd-handler`, UFFD restore path, remote page
   sources.
7. **Later.** Diff snapshots, lazy-disk plugins, virtio-fs for filesystem-mode
   volumes in sandboxes, P2P blob transfer within a region.

Phases 0–3 are the useful product; 4–6 are the performance story.

## Testing Plan

- **Volume contract harness** (the volume plan already defines one) extends with
  `snapshot` → `restore` → `clone`, plus double-call idempotency, and runs
  against `local-dir` in both modes. No KVM needed.
- **Round-trip:** write a marker → snapshot → destroy instance → restore into a
  new instance → read the marker. Runs for `exec` as well as `sandbox`, which is
  the proof that the disk half is genuinely runtime-agnostic.
- **Block-mode consumption:** a sandbox boots with a `local-dir` block volume and
  sees it at `stateful.storage.path`; the same volume in filesystem mode is
  rejected at admission with an actionable error.
- **Fork divergence:** 3 forks from one snapshot, distinct data, distinct
  instance ids / MACs / hostnames, three distinct `volume_id`s.
- **Entropy and clock:** forks must not produce identical `/dev/urandom` reads or
  UUIDs — the regression test for the guest agent's subtlest job.
- **Compatibility refusal:** a `compat` mismatch fails *placement* with a
  diagnostic naming the mismatch, never a corrupt guest.
- **GC:** two-phase delete resumes after a simulated crash; a snapshot pinned by
  a live clone is not deletable; expired snapshots are collected.
- **Ignored end-to-end** (KVM + VMM, like `sandbox_microvm_boots`): capture and
  restore a real microVM with a restore-latency threshold.

## Open Questions

1. **Block vs. virtio-fs as the sandbox default.** Block is simpler and
   snapshots cleanly; virtio-fs keeps the data host-inspectable and shareable.
   Leaning block for v1, virtio-fs as a later `mode` — but a host operator who
   wants to `ls` a sandbox's workspace will feel the difference immediately.
2. **Who formats a block volume?** The plugin (`params: { fs: 'ext4' }`) or the
   guest on first boot? Leaning plugin, so the same volume works for a runtime that
   expects a filesystem — but that puts `mkfs` in the plugin's job description.
3. **CH restore-time device override.** Firecracker's `PATCH /drives` before
   resume is a documented fork path; cloud-hypervisor's equivalent needs
   verifying. If absent, forks are Firecracker-first and CH gets disk-only forks.
4. **Snapshot blobs vs. artifact cache budget.** A 4 GiB memory blob will
   dominate a cache sized for container layers; snapshots probably need their own
   budget class rather than competing under one LRU.
5. **Secrets in memory snapshots.** A full snapshot contains live
   workload-identity tokens that may still be valid at restore. Options: refuse
   memory snapshots for identity-declaring tasks, force refresh on resume via the
   guest agent, or encrypt blobs at rest. Refresh-on-resume is most likely right
   and should be settled before phase 3.
6. ~~**Does `config.state` survive as sugar, or get removed?**~~ Resolved:
   removed. The volume system landed before the sandbox runtime ever shipped in a
   release, so no jobspec existed to keep working — there is exactly one way
   to declare storage, and the runtime rejects the old field with the migration
   spelled out.
