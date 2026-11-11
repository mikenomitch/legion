# Judgement calls: node-wide sandbox base root disks

Calls made building [rootfs-base-cache-plan.md](../plans/rootfs-base-cache-plan.md).
Ordered by project impact, highest first: the number is the rank.

## 1. Split the disk into a cached base and per-sandbox files written with debugfs

**Call.** A root disk is a base (image layers, `/legion/`, the guest binary),
cached per node, plus the sandbox's own files (`/legion/init`, `/legion/env`,
`/legion/cwd`, `/etc/hostname`, its `/etc/resolv.conf`), written into each
sandbox's copy with `debugfs -w` after the copy is made. A disk built for one
sandbox alone (artifacts, or the cache off) gets the same files in its tree
before `mkfs`, through a writer that replaces entries instead of following
them, so that path costs exactly what it did before.

**Alternatives.** (a) Two disks: a shared read-only base and a small
per-sandbox disk, joined by an overlayfs mount in an early guest init. No
per-start write into the root disk at all. (b) Keep writing the per-sandbox
files into the tree and cache nothing but extracted trees, so each start
still runs `mkfs` over a reflinked tree. (c) Bake env into the base and key
the cache on it.

**Trade-off.** debugfs becomes a hard dependency for microVM starts. It ships
in e2fsprogs with `mkfs.ext4` on Debian, Ubuntu, RHEL, Fedora, Arch, and
Homebrew, but Alpine splits it into `e2fsprogs-extra`. debugfs also reports
command failures only on stderr with exit status 0, so the worker treats any
stderr line but the version banner as a failure. A file name with a line
break cannot be written (none of the per-sandbox names have one).

**Why (performance, then simplicity).** (a) changes the guest's device
layout, needs `CONFIG_OVERLAY_FS` in every guest kernel, and touches every
checkpoint and restore path, none of which can be verified without KVM. (b)
keeps `mkfs` (most of the cost) on every start. (c) puts secrets into a
shared, long-lived file and makes the cache useless, since env differs per
sandbox. Writing a few small files with debugfs costs about 16 ms (measured,
process spawn included). Both writers take the same list of files
(`guest_payload_files`), so what goes into a sandbox's disk is decided in one
place. The change also fixes a real hole: the old code wrote `/etc/hostname`,
`/etc/resolv.conf`, and `/legion/*` on the host through the extracted tree
with `std::fs::write`, which follows an image's absolute symlinks out of the
tree.

## 2. A miss builds at the sandbox's path, then the cache copies the result

**Call.** A start that misses builds its own disk exactly as before. On a
reflink filesystem the cache then takes a reflink of that disk (before the
sandbox's files go in). Without reflinks, the extracted tree moves into the
cache's staging dir and a background thread formats it a second time.

**Alternatives.** (a) Build into the cache first and clone to the sandbox.
Instant with reflinks, but a full byte copy on the critical path without
them (0.23 s for python:3.12-slim, 2.8 s for the 1.6 GB rust image,
measured). (b) Copy the built disk into the cache before writing the
sandbox's files: the same copy, still on the critical path. (c) Fill only on
reflink filesystems.

**Trade-off.** The disk a miss builds is the base, without the sandbox's
files, so the miss writes them with debugfs afterwards: one debugfs pass
(about 16 ms) more than a start with the cache off, once per image per node.
Writing them into the tree first would make that disk unusable as the base.
On ext4 a miss also costs a second `mkfs` of the image in the background,
and the extracted tree stays on disk until it finishes. Starts of that image
that arrive meanwhile wait for the background format, which for the first
few is longer than an uncontended private build would be.

**Why (performance).** The user-facing promise is that a miss is never
slower than a start without the cache. (a) and (b) break it on the most
common Linux filesystem. (c) leaves ext4 nodes, where a hit (a sparse copy)
is still 10x cheaper than a build, without the win.

## 3. The base is keyed on its inputs, not content-addressed in the node content store

**Call.** The cache is its own directory (`<state dir>/rootfs-cache/`), keyed
by a hash of the layer digests, the guest binary fingerprint, and
`rootfs_headroom_mib`, with its own budget (`LEGION_SANDBOX_ROOTFS_CACHE`).

**Alternatives.** A third class in the node content store
(`content.budgets.rootfs`), which already has budgets, LRU, TTL, pins, and
`minFreeDisk`, plus a table mapping the input key to the content digest.

**Trade-off.** A second eviction mechanism on the node and a second budget
knob. `legion node content` does not show the bases (Phase 4).

**Why (performance).** The content store names every blob by the sha256 of
its bytes and hashes on every put: 2.1 s for the python base and 10.8 s for
the rust one (measured, `sha256sum`), on the miss path, for a digest nothing
else can use, since `mkfs` output differs run to run. The store also
advertises every entry to the manager's locality index, where these digests
would be noise. The cache still honors the store's `minFreeDisk`, so there is
one free-disk floor per node.

## 4. Headroom is in the key; nothing is resized per start

**Call.** `rootfs_headroom_mib` is part of the base key. A task with
non-default headroom gets its own base.

**Alternatives.** Build a minimal base and grow each clone to its task's
headroom with `resize2fs`.

**Trade-off.** Two jobs on the same image with different headroom keep two
bases.

**Why (performance).** `resize2fs` growing a small base by 256 MiB took 1.7 s
at the 1 KiB block size `mkfs` picks for disks under 512 MiB (53 ms at 4 KiB,
measured). Forcing 4 KiB blocks changes the guest's filesystem for every
small image and the sizing math with it, for a case (non-default headroom)
that is rare. Phase 2 revisits this for artifacts.

## 5. Sandboxes with artifacts build in full

**Call.** A sandbox with any file, tree, or layer artifact skips the cache
and builds its disk in full, as before. That includes a kernel shipped as an
artifact, which the root disk build copies into the guest too.

**Alternatives.** Write artifacts into the clone with debugfs, growing it
with `resize2fs` when they do not fit.

**Trade-off.** The documented "ship the kernel as an artifact" pattern does
not benefit. The public doc says so and points at installing the kernel on
the node instead.

**Why (simplicity).** Tree artifacts need a recursive debugfs writer with
quoting for arbitrary names, and growing a clone is slow at 1 KiB blocks
(call 4). Both belong in a phase that can measure them. Phase 1 serves the
common sandbox shape: an image, a command, and env.

## 6. Waiters wait once, then build on their own

**Call.** Starts that find a fill in flight for their key wait for it. If it
did not land, they build their own disk without trying to fill again. A key
whose base exceeds the whole budget is remembered and never waited on.

**Alternatives.** Elect the next waiter as the filler and retry.

**Trade-off.** A fill that failed for a transient reason (an I/O error, low
disk) is not retried until the next start that finds no flight.

**Why (performance).** Retrying the fill makes every waiter queue behind a
build that is likely to fail the same way, turning a burst into a serial
chain of full builds.

## 7. Queued pulls share a tag resolution only if it was sent after they arrived

**Call.** A pull reuses a `HEAD` for the same reference and credential whose
request was sent after the pull arrived. Otherwise it sends its own.

**Alternatives.** (a) A short TTL on tag resolutions (say 5 s), which would
also skip the `HEAD` for back-to-back sequential starts. (b) Share any
resolution that finished while the pull waited.

**Trade-off.** A burst still pays about two round trips (the first pull's,
and the first waiter's, whose wait began before the first `HEAD` was sent),
and sequential starts still pay one each: about 110 ms per session on
legion-04's rig.

**Why (simplicity).** Both alternatives change what a tag means: a start
could run a digest older than the one the registry would have given it. With
"sent after I arrived" the answer is never staler than the pull's own, so no
one has to reason about it. Operators who want no `HEAD` at all can pin the
image (`artifacts.containerImageDesugar` with `resolve = "pin"`).
