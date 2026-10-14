# Plan: Lazy Memory Restore

> **Judgement calls:** [`calls/lazy-memory-restore-plan_calls.md`](../calls/lazy-memory-restore-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-13). Phase 1 is built: restores
trust the node content store's digest instead of rehashing every part,
cloud-hypervisor restores memory on demand where it can, and
`config.memory_restore = "lazy" | "eager"` picks per task (default `lazy`).
Not verified on KVM yet (§9). Deliberately deferred and named: starting a
published restore while its memory still streams from the bucket (Phase 2),
working-set prefetch (Phase 3), and page sharing between cloud-hypervisor
forks (Phase 4, waits on a cloud-hypervisor release).

**Relationship to existing plans.** This executes the "lazy memory" item of
[suspend-resume-and-oversubscription-plan.md](suspend-resume-and-oversubscription-plan.md)
Phase 8 and the old
[sandbox-snapshots-and-fast-start-plan.md](sandbox-snapshots-and-fast-start-plan.md)
Part II.2. It does not change the checkpoint publishing format, checkpoint
delivery, or capture.

---

## 1. Problem statement

A resume, a snapshot restore, a fork, and a create from a snapshot all
restore a sandbox from a memory checkpoint. Today the time that takes grows
with the guest's RAM, on both VMMs, even when the checkpoint is already on
the node:

1. **Every part is rehashed before the VMM starts.**
   `materialize_parts` (`src/worker/checkpoints.rs`) called
   `ContentStore::verify` on each part, which reads and SHA-256s the whole
   blob: guest memory and the root disk. The store had already hashed those
   bytes when they came in.
2. **cloud-hypervisor copies all of memory before the guest runs.** The
   restore launches `cloud-hypervisor --restore source_url=…`, whose default
   `copy` mode reads the whole `memory-ranges` file into guest RAM before
   `vm.resume` can return. cloud-hypervisor is what `hypervisor = "auto"`
   picks first.
3. **Both happen under the worker's task-manager lock.** The restore runs
   inside `start_registered_allocation` (`src/worker/task_manager.rs`), so
   heartbeats and every other start and stop on the node wait for it.
4. **A slow copy failed outright.** cloud-hypervisor serves its API socket
   before `--restore` finishes, and Legion's `vm.resume` waited with the 10 s
   control timeout, so a copy longer than that failed the restore and
   `onRestoreFailure` booted the instance cold.

Firecracker was already lazy past step 1: its `File` memory backend maps the
memory file `MAP_PRIVATE` and the kernel loads pages on first touch.

What that costs, for a 4 GiB guest with a 1 GiB root disk. Rows marked
*measured* ran on this branch's dev machine (Apple M-series, macOS, a 2 GiB
random file in the page cache, `ContentDigest::of_file`'s loop with the
same `sha2` 0.10.9 build Legion uses). The machine was shared with other
builds (load average about 80 on 10 cores), so the hash is reported in CPU
time, which is what an idle core would take. The rest are estimates or
published numbers.

| Step | Arithmetic | Time |
| --- | --- | --- |
| Rehash on aarch64 (*measured*) | Legion builds `sha2` without its `asm` feature, so aarch64 (Graviton, Ampere, Apple) hashes in software: 2.0 CPU-s per GiB | ~10 s for 5 GiB |
| Rehash on x86_64 with SHA-NI (*estimate*) | `sha2` uses SHA-NI there, about 1.5 to 2 GB/s per core | ~3 s for 5 GiB |
| Rehash on a busy node (*measured*) | The same hash took 10 to 16 s of wall time per GiB under the load above | far worse |
| cloud-hypervisor copy, page cache warm (*measured* as a read into a fresh buffer) | 1.5 to 1.9 GiB/s | ~2.5 s for 4 GiB |
| cloud-hypervisor copy, from disk (*published*) | 2 GB in 7,140 ms on GCE n2-standard-8 ([#7800](https://github.com/cloud-hypervisor/cloud-hypervisor/pull/7800#issuecomment-4014058925)) | ~14 s for 4 GiB |
| cloud-hypervisor on demand (*published*) | 2 GB in 83 ms, RSS 7 MB after restore (same comment) | ~0.1 s |

So a 4 GiB cloud-hypervisor resume spent roughly 5 to 25 s before the guest
ran, and failed past the 10 s resume timeout when the copy came from a slow
disk. A Firecracker resume spent 3 to 10 s, all of it in the rehash. Wake on
request ([wake-on-request-plan.md](wake-on-request-plan.md)) holds the
client's request for that whole time.

One more restore step grows with size and stays outside this plan: the root
disk is cloned into the new allocation (`clone_file`), which is a reflink on
btrfs, XFS, and APFS and a full sparse copy on ext4.

## 2. Prior art

- **Firecracker** ([snapshot-support.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md)):
  the `File` backend creates "a MAP_PRIVATE mapping of the memory file,
  resulting in runtime on-demand loading of memory pages". The `Uffd`
  backend hands the guest memory and a userfaultfd to an external process
  that serves page faults. **Take:** `File` as is. **Defer:** `Uffd` to
  Phase 2, where the page source is remote.
- **cloud-hypervisor** v52.0 (2026-05-14) added
  `memory_restore_mode=ondemand` on `--restore`
  ([#7800](https://github.com/cloud-hypervisor/cloud-hypervisor/pull/7800)):
  a handler thread inside the VMM serves faults from the snapshot file with
  `UFFDIO_COPY`, and the restore fails rather than falling back when it
  cannot create a userfaultfd. v53.0 (2026-07-12) prefaults the remaining
  pages in the background after the guest resumes
  ([#8150](https://github.com/cloud-hypervisor/cloud-hypervisor/pull/8150)),
  so memory is fully resident "within a few seconds". A third mode,
  `copyonwrite` (a private file mapping, so forks share pages), is on
  `main` and not in a release yet
  ([docs](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/snapshot_restore.md)).
  **Take:** `ondemand` on v52+. **Defer:** `copyonwrite` to Phase 4.
- **CRIU** [lazy migration](https://criu.org/Lazy_migration): restores a
  process with its memory registered on a userfaultfd and pages served by a
  `lazy-pages` daemon, locally or from the source host. The same shape as
  Phase 2.
- **REAP** ([Ustiugov et al., ASPLOS '21](https://arxiv.org/abs/2101.09355)):
  a function restored from a Firecracker snapshot ran 95% slower on average
  than a memory-resident one, because its working set came in one page
  fault at a time. Recording the working set and prefetching it cut
  cold-start delay 3.7x. This is the honest cost of lazy restore, and
  Phase 3.
- **containerd's content store** verifies a blob's digest when it is
  committed and serves reads without rehashing. Legion's own OCI layer
  placement does the same: `ImageBlobCache::place`
  (`src/artifacts/fetchers/oci.rs`) hardlinks a layer out of the node
  content store after `get`, and only small JSON manifests are rehashed on
  read. **Take:** the same rule for checkpoint parts.
- Nomad and Kubernetes have no memory restore to compare with.

## 3. Jobs to be done

**Job author (an agent platform)**
- When a sleeping sandbox gets a request, I want it to answer about as fast
  with 8 GiB of RAM as with 512 MiB, so I can size sandboxes for the work
  and not for wake latency.
- When a run forks ten copies of a big sandbox, I want each copy to start
  without reading the whole parent first, so the fan-out finishes in
  seconds.

**Job author (a memory-hungry workload)**
- When my sandbox touches most of its memory right after it wakes (a large
  heap, an in-memory index), I want to load all of it up front, so the
  first minute after a resume runs at full speed.

**Cluster operator**
- When a node restores a large sandbox, I want the rest of the node to keep
  moving, so one resume never delays heartbeats or other starts.
- When a node cannot restore lazily, I want the log to say why, so I can fix
  the node instead of chasing slow resumes.

**On-call responder**
- When resumes get slow, I want each restore's log line to name the mode it
  used, so I can tell a slow disk from a node that fell back to copying.

*Not in scope:* the autonomous agent persona has no new surface. The field
is ordinary task config, set like any other.

**Scenarios.**
1. A 4 GiB coding sandbox suspended for an hour gets a request through the
   gateway. The resume loads device state, the guest runs, and pages arrive
   as the request touches them.
2. A team running an in-memory search index sets `memory_restore =
   "eager"`. Its resumes take seconds, as before, and its first queries are
   not slowed by page faults.
3. A node runs cloud-hypervisor v51. Its restores copy memory, and each one
   logs that on-demand restore needs v52.

## 4. Goals and non-goals

**Goals**
- On a node that holds the checkpoint, the time before the guest runs does
  not grow with guest memory, on both VMMs, by default.
- One task-level knob with a safe default, meaning the same thing on both
  VMMs.
- No new process, no new format, no new RPC.

**Non-goals**
- Starting a restore before a published checkpoint's parts are on the node
  (Phase 2).
- Changing how checkpoints are captured, published, or delivered.
- Page sharing between cloud-hypervisor forks (Phase 4).
- Diff snapshots and warm pools.

## 5. What exists today

- `materialize_parts` (`src/worker/checkpoints.rs`) links each part out of
  the node content store into `<alloc>/.restore`, after
  `ContentStore::verify` rehashed it.
- `restore_microvm` (`src/task_plugins/sandbox_checkpoint.rs`) checks part
  sizes, clones the root disk, writes the identity disk, and launches the
  VMM:
  - Firecracker: `--api-sock`, then `PUT /snapshot/load` with
    `mem_backend: { backend_type: "File" }` on a hardlink of the memory
    part in the allocation's scratch dir (the mapping lives as long as the
    VM), then a resume.
  - cloud-hypervisor: `--restore source_url=file://<alloc>/.restore`, then
    `vm.resume` with `CONTROL_TIMEOUT` (10 s).
- `restore_from_snapshot` (`src/worker/task_manager.rs`) removes
  `<alloc>/.restore` once the runtime returns.
- Content store blobs are hashed on commit (`hash_and_commit`, or the
  digest check of a fetch) and made read-only (0444). Nothing writes
  through a hardlink to one: Firecracker's mapping is private, both
  cloud-hypervisor modes only read, and the `config.json` rewrite replaces
  the file rather than editing it.
- Published parts are sealed in independent 1 MiB AES-256-GCM chunks
  (`src/checkpoint_seal.rs`), so chunk *i* can be fetched and opened on its
  own. Phase 2 relies on that.

## 6. Design

### 6.1 Restores trust the content store's digest

`materialize_parts` checks each part with `ContentStore::get` (present, at
its recorded size) and hardlinks it, without rehashing. A blob changed
behind the store's back at the same size restores as is. A missing or
truncated one still fails the restore, by name:

```text
snapshot snap-5b10e2c7a9d04f31 part memory (sha256:9c…) is missing or truncated in this node's content store
```

### 6.2 `config.memory_restore`

```nickel
config = {
  image = "ghcr.io/acme/search:1.4",
  command = ["/srv/search"],
  # This workload touches most of its memory right after it wakes.
  memory_restore = "eager",
},
```

| | `lazy` (default) | `eager` |
| --- | --- | --- |
| Firecracker | `File` backend: the guest runs at once, pages load from the file on first touch | The worker reads the memory file through the page cache, then the same load, so first touches are cache hits |
| cloud-hypervisor v52+ with a userfaultfd | `memory_restore_mode=ondemand`: the guest runs at once, pages load on first touch (v53 also fills the rest in the background) | `copy`: all of memory is copied in before the guest runs |
| cloud-hypervisor before v52, or no userfaultfd | Falls back to `copy`, and logs why | `copy` |

Validation rejects anything else at submit:

```text
sandbox task memory_restore must be `lazy` or `eager`, got `fast`
Hint: `lazy` (the default) resumes a restored guest before its memory is read, and `eager` reads all of it first (docs/task-runtime-sandbox.md, "How memory comes back")
```

### 6.3 Deciding at restore time

The worker decides per restore (`plan_memory_restore`):

- The VMM version comes from the checkpoint's `compat.vmm.version`.
  Placement already guarantees the restoring node runs that version.
- userfaultfd comes from a probe in the worker
  (`userfaultfd_available`): open `/dev/userfaultfd`, else call
  `userfaultfd(2)` and close the fd. That is the order cloud-hypervisor
  tries, and the VMM runs with the worker's credentials.
- A fallback logs at `info`, and the restore's own log line carries
  `memory_restore`:

```text
INFO Restoring sandbox memory eagerly: lazy restore is not available here allocation_id=412 snapshot_id=snap-5b10e2c7a9d04f31 reason="cloud-hypervisor 51.2.0 restores memory only by copying it (on-demand restore needs v52.0 or later)"
INFO Restored sandbox from memory checkpoint allocation_id=412 snapshot_id=snap-5b10e2c7a9d04f31 vmm=cloud-hypervisor memory_restore=eager restore_ms=6120
```

### 6.4 The first resume after `--restore` waits for the restore

`CloudHypervisorApi::resume_restored` sends `vm.resume` with the snapshot
timeout (300 s) instead of the 10 s control timeout, because the request
queues behind the restore in cloud-hypervisor's control loop.

### 6.5 What a lazily restored guest depends on

- **Firecracker:** the hardlinked memory file in the allocation's scratch
  dir, for the VM's whole life (unchanged).
- **cloud-hypervisor:** the open file descriptor its handler thread holds.
  Removing `<alloc>/.restore` or evicting the content store blob does not
  break the guest. The descriptor closes when the background prefault
  finishes (v53, seconds) or when the VM exits (v52).
- In both cases the bytes stay on disk while held, even after the content
  store evicts its entry, so the store's budget undercounts them for that
  time.
- An I/O error reading the file after the resume: Firecracker cannot fault
  the page in and its VMM exits. cloud-hypervisor poisons the unrestored
  memory on Linux 6.6 and later (the guest sees memory errors) and exits
  on older kernels. Either way the allocation is lost and its restart
  policy applies, as with an eager restore whose disk fails mid-copy.

### 6.6 Checkpoints during the background fill

cloud-hypervisor v53 refuses `vm.snapshot` with "VM on-demand memory
restore is still in progress" until its background fill completes
(`vm_snapshot` checks `restoring()`). A capture (suspend, snapshot, fork)
in that window pauses the VM, retries the snapshot every 200 ms for up to
60 s (`snapshot_cloud_hypervisor`), and then fails as any capture does,
resuming the guest. v52 has no such check: its snapshot reads the guest's
untouched pages through the fault handler, which works and is slower.

### 6.7 Memory accounting

Legion's cloud-hypervisor guests use private anonymous RAM, which
cloud-hypervisor snapshots densely, so a copy restore makes every page
resident at once. An on-demand restore makes pages resident as they are
touched (v52) or over the next seconds (v53's background prefault), so it
is never above the copy. Firecracker's clean pages stay in the page cache,
shared between forks, as before.

## 7. Struct and config changes

- `SandboxTaskConfig.memory_restore: Option<String>`, validated to `lazy` or
  `eager`, in the runtime's JSON schema.
- `RestoreTarget.memory_restore` and `RestoreTarget.userfaultfd`.
- `configs/jobs/maximal.ncl` shows `memory_restore`.
- No proto, manifest, store, or persistence change.

## 8. Phases

**Phase 1: lazy restore of a checkpoint the node holds (built).** §6 in
full. Works for every local restore, and for a published one once its parts
are fetched.

**Phase 2: start a published restore while its memory streams in.** Today
`fetch_published_parts` downloads every part before the start. Serve the
memory part to the VMM from a page source that reads ranges of the sealed
object (1 MiB chunks open independently), with a background fill behind
it: Firecracker's `Uffd` backend with a Legion page server in its own
process, and cloud-hypervisor's socket page source
(`SocketUffdMemorySource`, v53) or its offload daemon. This couples a
running guest to the bucket until the fill completes, so it needs its own
review: a fetch failure mid-life kills the guest, and the fill competes
with other traffic. The default stays "fetch, then restore" until measured.

**Phase 3: working-set prefetch.** Record which pages a guest touches in
its first seconds after a restore (the userfaultfd fault log, or
`mincore` on Firecracker's mapping) into the next checkpoint's manifest,
and load those first. REAP's result says this is where the post-resume
cost of lazy restore goes away.

**Phase 4: cloud-hypervisor `copyonwrite`.** When a release ships it, use it
for `lazy` on cloud-hypervisor, so forks of one checkpoint share clean
pages the way Firecracker forks do. Closes the "cloud-hypervisor forks
share no memory pages" TODO.

## 9. Testing plan

**Phase 1, no KVM (in CI):**
- `plan_memory_restore` across both VMMs, both asks, versions either side
  of v52, unknown versions, and with and without a userfaultfd.
- A cloud-hypervisor restore launched through a stand-in VMM that records
  its argv: `memory_restore_mode=ondemand` exactly when the plan says so.
- Firecracker `eager` reads the whole memory file ahead.
- A cloud-hypervisor capture retries while the VMM answers "on-demand
  memory restore is still in progress", fails by name past its wait, and
  returns any other error at once.
- `materialize_parts` links parts without rehashing and refuses a
  truncated one.
- Config validation of `memory_restore`, and its default.
- The userfaultfd probe, compiled and run on Linux (aarch64, kernel 7.0,
  in a container): `false` under Docker's default seccomp profile, `false`
  with seccomp unconfined but no `CAP_SYS_PTRACE`
  (`vm.unprivileged_userfaultfd = 0`), `true` with `CAP_SYS_PTRACE`. So a
  worker in a default container falls back to the copy restore instead of
  failing.

**Phase 1, on KVM (not run yet).** Numbers from these runs go in two
places: the §1 table here, and the "Restore time" paragraph of
docs/task-runtime-sandbox.md, "How memory comes back", which is the one
public place that states restore times.

- `restore_ms` for 1, 4, and 8 GiB guests, `lazy` and `eager`, on
  Firecracker and cloud-hypervisor v51, v52, and v53, from a warm and a
  dropped page cache (`echo 3 > /proc/sys/vm/drop_caches`).
- Time to first response through the gateway for a suspended sandbox with
  `suspend.wake`.
- A touch-all-memory workload after the resume (`stress-ng --vm 1
  --vm-bytes 90% --vm-keep`, timed), `lazy` against `eager`, to size the
  post-resume cost.
- `vm.unprivileged_userfaultfd = 0` with the worker as a non-root user:
  the fallback log, and a working copy restore.
- Heartbeat gaps on the restoring node during an 8 GiB resume.

## 10. Rollout

Default on for every sandbox, with no migration: existing checkpoints
restore lazily, because nothing in their format changes. What changes for
an existing cluster: cloud-hypervisor restores on demand on v52+, and a
corrupted content store blob of the right size is no longer caught at
restore. Nodes on cloud-hypervisor before v52 behave as before, minus the
rehash.

## 11. Open questions

1. **Should `lazy` stay the default once KVM numbers exist?** *Lean:* yes,
   unless the touch-all benchmark shows on-demand total time far above the
   copy on v53, where the background prefault should close the gap.
2. **Should anything rescan content store blobs?** Rehashing on restore was
   the only place snapshot parts were ever re-verified. *Lean:* a
   low-priority background scrub of the whole store, for every class, is
   the right home, not the restore path.

## 12. Acceptance criteria

**Phase 1**
- A restore never reads a part's bytes before the VMM starts, except
  Firecracker `eager`'s read-ahead.
- A `lazy` cloud-hypervisor restore on v52+ with a userfaultfd launches with
  `memory_restore_mode=ondemand`, and otherwise launches the copy restore
  and logs why.
- A cloud-hypervisor copy restore longer than 10 s completes.
- On KVM: `restore_ms` for a 4 GiB guest under 300 ms on both VMMs with
  `lazy`, from a warm page cache.
