# Node-wide sandbox base root disks

> **Judgement calls:** [rootfs-base-cache-plan_calls.md](../calls/rootfs-base-cache-plan_calls.md)

**Status: IN EXECUTION.** Phase 1 is built (the cache, the per-sandbox
injection, the `ensure_image_base` seam, and shared tag resolution for queued
pulls). Phase 2 (artifacts on the cached path) and Phase 3 (the `container`
runtime) are planned. Real-KVM boot of a cloned disk is not yet verified.

## Problem

Every microVM sandbox boots from a root disk the worker builds at start:
resolve the image, extract every layer into a tree, write Legion's guest
files and the sandbox's init and env into it, then `mkfs.ext4 -d` the tree
into a raw image (`src/task_plugins/sandbox.rs`, `start_microvm`). The only
reuse is an in-place restart of the *same* allocation, through the
`rootfs.stamp` fingerprint.

So N sandboxes of one image on a node extract and format the same image N
times. That sits on the critical path of every keyed create, every session
start, and every edge claim at the `stage` gate. Measured:

| Image | Build (extract + mkfs), per sandbox |
| --- | --- |
| `alpine:3.20` (real KVM rig, legion-04, c8i.xlarge) | ~80 ms extract + mkfs, plus ~110 ms image resolve and pull, of a 162 ms p50 create-to-VMM-spawn |
| `python:3.12-slim` (OrbStack arm64, 2 vCPU, btrfs, GNU tar + e2fsprogs) | 2.5 s |
| `rust:1.90-bookworm`, 1.6 GB unpacked (same host) | 28 s |

The cost is CPU as much as latency. legion-04's rig is CPU-bound at about 6
guests a second at concurrency 8, so every core-second spent re-extracting
an image is a core-second not booting guests. After the `quiet` guest
cmdline change, the root disk is the largest slice of a cold start even for
alpine, which is a best case.

The per-sandbox parts of the disk are tiny: init, env, cwd, hostname, and
resolv.conf. Everything else is the image.

## Prior art

- **containerd snapshotters / overlayfs.** Containers share image layers
  read-only and give each container a thin writable upper layer. That is
  the model for the `container` runtime (Phase 3). A microVM needs a block
  device, so it cannot take an overlay of host directories.
- **[firecracker-containerd](https://github.com/firecracker-microvm/firecracker-containerd)**
  and Kata Containers can use containerd's
  [devmapper snapshotter](https://github.com/containerd/containerd/blob/main/docs/snapshotters/devmapper.md):
  one thin snapshot per image layer, a thin clone per VM. Copy-on-write at
  the block layer, but it needs a thin pool provisioned on the host and root
  to drive it.
- **Nomad** caches downloaded artifacts and driver images per node but leaves
  rootfs construction to the driver.

We take the per-host base plus copy-on-write clone, using file reflinks
(`FICLONE`, `clonefile`) because they need no root, no pool, and no new
dependency, and fall back to a sparse copy where the filesystem cannot
reflink. We reject overlay-in-the-guest (an initramfs that overlays a
read-only base disk with a writable one) for this phase: it changes the
guest's device layout, needs `CONFIG_OVERLAY_FS` in every guest kernel, and
touches every checkpoint and restore path.

## Jobs to be done

**Platform builder running many sandboxes**
- When users open sandboxes of the same image in bursts, I want each one to
  start in tens of milliseconds, so the sandbox feels instant and my nodes
  spend their CPU on guests rather than on unpacking the same image again.
- When a new image shows up, I want the first sandbox to start no slower
  than it does today, so caching never makes the cold path worse.

**Cluster operator**
- When a node's disk fills, I want the image cache to stay inside a budget I
  can set, so cached images never crowd out volumes or checkpoints.
- When I pick a filesystem for the data dir, I want to know what it buys
  me, so I can choose XFS or btrfs when start latency matters.

**Security / platform admin**
- When sandboxes share cached image data, I want each sandbox's secrets and
  identity to stay in its own disk, so one tenant's env can never show up in
  another's guest.

**Prefetcher (another Legion component)**
- When I know an image is about to be used on a node, I want one call that
  makes the node ready for it, so I can warm nodes without building a second
  cache.

*Not in scope:* the job author. Nothing in the jobspec changes.

## Goals and non-goals

Goals:

- A sandbox start on a node that has already built its image's base is
  a copy and a few small writes.
- A miss is never slower than a start without the cache.
- Concurrent starts of one image build it once.
- Env, hostname, instance id, and init never enter a shared base.
- The cache is bounded by a byte budget with LRU eviction, and honors the
  content store's free-disk floor.
- One idempotent, concurrency-safe seam for prefetching.

Non-goals:

- Pre-warming triggers, pushes, or scheduler involvement (agent 3's
  prefetch calls the seam, it does not build its own cache).
- A warm pool of booted VMs.
- Cross-node sharing of bases. They are cheap to rebuild from cached layers
  and their bytes are not deterministic across `mkfs` runs.
- Changes to checkpoint formats.

## What exists today

- `src/task_plugins/sandbox.rs` `start_microvm`: resolves the layout
  (`oci_image::resolve_layout`), builds `GuestPayload`, fingerprints it
  (`build_stamp`), and on a miss extracts, installs artifacts, writes the
  payload into the tree (`microvm::install_guest_payload`), and runs
  `mkfs.ext4 -d`.
- `src/task_plugins/oci_image.rs` `pull_layout` already goes through the node
  content store: layers are hard-linked from it and a tag costs one `HEAD`.
  Concurrent pulls of one reference queue on `ImagePullClaim`, and each sends
  its own `HEAD` when its turn comes.
- `src/volumes/copy.rs` has reflink and sparse-copy helpers, used by
  checkpoints.
- Checkpoints copy `<scratch>/rootfs.img` with `clone_file` and restore by
  cloning the part back. Neither cares how the disk was made.
- `install_guest_payload` wrote `/etc/hostname`, `/etc/resolv.conf`, and
  everything under `/legion` on the host through the extracted tree with
  `std::fs::write`, which follows the image's symlinks. An image with
  `/etc/hostname -> /root/.ssh/authorized_keys` (absolute) made a root worker
  overwrite that host file. Layer extraction itself resolves paths inside the
  tree (`resolve_in_rootfs`), but these later writes did not.

## Design

### 1. The split

A root disk is the **base** (image layers, `/legion/`, the guest binary) plus
the **sandbox's own files** (`/legion/init`, `/legion/env`, `/legion/cwd`,
`/etc/hostname`, and `/etc/resolv.conf` when the guest has its own
resolvers). The base is built from the tree with `mkfs.ext4 -d`, as before.
The sandbox's own files are written into its copy with `debugfs -w`, which
edits the ext4 image in place with no mount and no root, and resolves every
path inside the image's own filesystem. Every start, cached or not, writes
them this way, so there is one code path and the host-symlink writes above
are gone.

The base build claims `/legion` (removing whatever the image put there
without following it) and removes the image's `/etc/hostname`, recording
whether `/etc` is a real directory and whether the image has a
`resolv.conf`. Those two facts travel with the base so the per-sandbox
writes know whether to replace a file.

### 2. The key

`sha256(format version, layer digests, guest binary fingerprint,
rootfs_headroom_mib)`. Layer digests are read from the manifest, so a staged
layout and a pulled one share a base. Headroom is in the key, not applied per
clone, because growing an ext4 image with `resize2fs` takes 1.7 s at the 1 KiB
block size `mkfs` picks for small disks (53 ms at 4 KiB, measured).

### 3. Filling on demand, never slower on a miss

A start that misses claims the key (single flight), builds at its own disk
path exactly as before, then offers the result to the cache:

- **Reflink filesystem:** the cache takes a reflink of the just-built disk
  before the sandbox's files are written. Instant, and the start is done.
- **No reflinks:** copying the disk would sit on the start's critical path,
  and the disk is about to be written. Instead the extracted tree moves into
  the cache's staging dir and a background thread formats it a second time.
  The start pays nothing extra.

Starts that arrive while a fill is in flight wait for it, then clone. After one
wait they never wait again: if the fill did not land (too big, disk low),
they build their own disk in parallel. A key whose base is larger than the
whole budget is remembered, so later starts skip the queue.

### 4. Bounds

- Budget: `LEGION_SANDBOX_ROOTFS_CACHE` (a size, a share, or `off`), default
  10% of the filesystem clamped to 1..20 GiB, counted in allocated blocks.
- LRU eviction on commit. Last use is the mtime of the entry's `base.json`,
  touched on every hit.
- No commit while free disk is below the content store's `minFreeDisk`.
- Entries are directories renamed into place from `tmp/`, which is cleared
  when the worker starts, so a crash never leaves a half-written base that
  looks valid.

### 5. The seam

```rust
rootfs_cache::ensure_image_base(image: &str, headroom_mib: Option<u32>) -> R<Ensured>
rootfs_cache::ensure_layout_base(layout: &Path, headroom_mib: Option<u32>) -> R<Ensured>
```

Idempotent: `Present` on a hit (and it refreshes the entry's last use),
`Built` after a fill, `Skipped(reason)`, or `Disabled`. It shares the single
flight with starts, so a prefetch and a start of the same image never build
twice.

### 6. Shared tag resolution

A burst of N starts of one tag used to send N `HEAD` requests one after
another, each behind `ImagePullClaim`. A pull now reuses a resolution of the
same reference and credential that was *sent* after the pull arrived. That
answer is at least as fresh as the one it would get itself, so tag semantics
do not change: a sequential start still sends its own `HEAD`. A burst pays
about two.

### Surface

Operator-facing only:

```bash
LEGION_SANDBOX_ROOTFS_CACHE=40GiB legion start worker --token-file /etc/legion/join-token
LEGION_SANDBOX_ROOTFS_CACHE=off   # every start builds its disk
```

```text
Sandbox root disk ready allocation_id=88213 image=python:3.12-slim root_disk=cached base (reflink) elapsed_ms=48
Sandbox root disk ready allocation_id=88214 image=rust:1.90 root_disk=built, caching the base in the background elapsed_ms=28115
```

A node without `debugfs` fails a microVM start with
`debugfs not found: install e2fsprogs (e2fsprogs-extra on Alpine)`.

No proto, jobspec, or CLI changes.

## Phases

**Phase 1 (this change).** The cache, the per-sandbox injection for every
start, the seam, shared tag resolution. Sandboxes with file, tree, or layer
artifacts build in full, as before.

**Phase 2: artifacts on the cached path.** Write artifacts into the clone
with debugfs (`mkdir`, `write`, `symlink` per entry), growing the clone with
`resize2fs` first when they do not fit the slack. Needs 4 KiB-block bases so
the grow is fast, and sizing that rounds every file up to a block. Also stop
copying the artifact that `config.kernel` names into the guest.

**Phase 3: the `container` runtime.** It extracts every image per allocation
too (`oci_bundle::build_bundle`). The fix there is different: a node-wide
extracted lower dir per image and an overlayfs mount per container. Needs its
own plan for the id-mapped (`userns`) case, where each container's owners
are shifted.

**Phase 4: visibility.** Report the cache in `legion node content`
(entries, bytes, budget) and add a hit/miss counter once the worker has a
metrics surface for start stages.

## Testing

- Unit (any OS): the key, single flight with concurrent starts, ensure and
  start sharing a fill, LRU eviction under budget, oversized bases, a failed
  build releasing waiters, recovery from a crash mid-fill, debugfs script
  quoting, payload file rendering, the base claiming `/legion` through
  hostile symlinks, shared tag resolution.
- With e2fsprogs (Linux CI, skipped on macOS): writing the payload into a
  formatted disk and reading it back with modes and owners, and the full
  `start_microvm` path building a disk, then a second allocation cloning it.
- Real KVM: boot a cloned disk on cloud-hypervisor and Firecracker,
  checkpoint and restore one. Open.

## Rollout

Default on. Legion has no installed base, and the cache is invisible except
for disk use, which the budget bounds. `debugfs` becomes a requirement for
microVM starts; it ships with `mkfs.ext4` everywhere except Alpine.

## Acceptance (Phase 1)

- A second sandbox of an image on a node logs `root_disk=cached base`.
- Eight concurrent starts of a new image extract it once.
- `/legion/env` in a cached base is absent (`debugfs -R "ls /legion"` on
  `rootfs-cache/<key>/rootfs.img` lists only `legion`).
- The cache never exceeds its budget.
