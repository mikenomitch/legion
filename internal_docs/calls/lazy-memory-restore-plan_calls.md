# Judgement calls: Lazy Memory Restore

Calls made while building [lazy-memory-restore-plan.md](../plans/lazy-memory-restore-plan.md).
Entries are ranked by project impact, highest first, and the number is the rank.

## 1. Lazy is the default, chosen per task with `config.memory_restore`

```nickel
config = { image = "ghcr.io/acme/search:1.4", command = ["/srv/search"], memory_restore = "eager" },
```

Every sandbox restores lazily unless its task says `eager`. On
cloud-hypervisor that turns on `memory_restore_mode=ondemand`, which used to
be opt-in inside cloud-hypervisor itself.

- **Alternatives.** (a) Default `eager`, so no existing restore changes
  behavior, and offer `lazy` to those who ask. (b) A worker-level setting
  (`sandbox.memoryRestore`), since the node knows whether it can do it.
  (c) No knob: always lazy where the VMM can.
- **Trade-off.** A workload that touches most of its memory right after a
  resume gets slower overall by default: REAP measured restored Firecracker
  functions running 95% slower on average while their working set faulted
  in one page at a time, and cloud-hypervisor's userfaultfd faults cost more
  than Firecracker's kernel-handled ones. That team has to know to set
  `eager`. A per-task field is also one more thing in the sandbox config.
- **Why.** The restores Legion exists to make fast are resumes woken by a
  request and forks of an agent's sandbox, which touch a small part of
  memory before they answer. Firecracker has always restored this way (its
  `File` backend has no eager mode), so `lazy` is already the default for
  half the VMMs, and cloud-hypervisor v53 fills the rest of memory in the
  background seconds later. The memory access pattern after a wake is a
  property of the workload, not the node, so the job author is the one who
  can choose (b loses). A knob is worth its line because the slow case is
  real and measurable, and one field fixes it (c loses). Performance won
  over keeping existing behavior, which AGENTS.md says not to optimize for.

## 2. A restore trusts the content store's commit-time digest instead of rehashing

`materialize_parts` checks each part with `ContentStore::get` (present, at
its recorded size) instead of `ContentStore::verify` (a full SHA-256).

- **Alternatives.** (a) Keep rehashing on every restore. (b) Rehash in the
  background after the guest resumes, and quarantine the blob if it is
  corrupt. (c) Rehash only blobs this worker process has not hashed itself
  since it started.
- **Trade-off.** A part changed on the node's disk after it was committed,
  at the same size, restores as is, into a running guest. Nothing else
  rescans snapshot parts now (plan open question 2).
- **Why.** Every blob is hashed on its way into the store (a capture's
  import, a fetch's digest check), committed by rename, and made read-only,
  and nothing writes through the hardlinks a restore makes. The rehash only
  guarded against corruption at rest, at a cost of reading all of guest
  memory and the root disk before the guest could start, under the
  task-manager lock: for 4 GiB plus a 1 GiB root disk, about 3 s on x86_64
  with SHA-NI and 10 s on aarch64, where Legion hashes in software. It also
  made every lazy VMM mode pointless. Legion already places OCI layers from the
  same store without rehashing, as containerd does. (b) would read the whole
  file during the window when the guest's own page faults need the disk,
  and could only report corruption after the guest had consumed it. (c)
  helps nothing, since the common case (suspend, then resume) hashes the
  blob moments before the restore anyway. Performance won over a defense
  the rest of the store does not have.

## 3. A lazy ask the node cannot honor falls back to eager, with a logged reason

On cloud-hypervisor before v52, or a node without a userfaultfd, `lazy`
restores by copying and logs:

```text
INFO Restoring sandbox memory eagerly: lazy restore is not available here ... reason="cloud-hypervisor 51.2.0 restores memory only by copying it (on-demand restore needs v52.0 or later)"
```

- **Alternatives.** (a) Pass `ondemand` anyway and let the restore fail,
  which is cloud-hypervisor's own strict semantics. (b) Refuse at
  placement, through a node attribute such as `sandbox.vmm.lazy_restore`.
- **Trade-off.** A task that asked for `lazy` can get a slow restore and
  only the worker log says so. A cluster with mixed nodes restores at mixed
  speeds.
- **Why.** `memory_restore` is a performance preference, not a correctness
  requirement, and the default. A failed restore runs `onRestoreFailure`,
  which by default boots the instance cold and throws away its memory:
  strictly worse than a slow restore. A placement constraint would make a
  default-on preference strand checkpoints on nodes that can still restore
  them (simplicity).

## 4. A failed on-demand restore is not retried with a copy (port onto #43)

The fallback in entry 3 is decided before the VMM launches, from the
checkpoint's VMM version and the userfaultfd probe. If cloud-hypervisor
still refuses an on-demand restore after that, the restore fails and
`onRestoreFailure` applies, as for any failed restore.

- **Alternatives.** Reap the VMM and relaunch once with the copy restore,
  logging the first error.
- **Trade-off.** A node where the probe says yes but cloud-hypervisor
  cannot restore on demand (a wrapper or a seccomp profile stricter than
  the worker's, a kernel without the userfaultfd features it needs) loses
  the restore, and by default boots the instance cold, until the operator
  sets `memory_restore = "eager"` or fixes the node.
- **Why.** A retry is only safe when the guest never ran, and the restore
  cannot always tell: a `vm.resume` that times out may have resumed the
  guest, which could then have written to its root disk and volumes before
  the second launch rewinds its memory over them. cloud-hypervisor
  documents the cases where `ondemand` fails (no userfaultfd, `prefault`,
  VFIO, vDPA, vfio-user, and vhost-user devices), and Legion's sandboxes
  hit only the first, which the probe covers. The KVM checks (plan §9)
  include the no-userfaultfd fallback. If they turn up a failure the probe
  misses, a retry gated on "the VMM exited before the resume was sent" is
  the follow-up (simplicity over a speculative recovery path).

## 5. Firecracker `eager` reads the memory file through the page cache first

- **Alternatives.** (a) Ignore `eager` on Firecracker and document that it
  always restores lazily. (b) Use Firecracker's `Uffd` backend with a
  handler that prefaults every page before the resume.
- **Trade-off.** It costs a full read of the memory file, and fills the
  page cache with it. The guest still takes minor page faults afterwards,
  so `eager` on Firecracker is "nothing waits on the disk", not "every page
  mapped".
- **Why.** The field means the same thing on both VMMs ("read all of it
  before the guest runs"), so a jobspec with `hypervisor = "auto"` behaves
  the way its author expects wherever it lands (simplicity). (b) is a new
  process for the eager case only. (a) would make the knob a no-op on half
  the VMMs without telling anyone.

## 6. The node probes for a userfaultfd itself, and takes the VMM version from the checkpoint

- **Alternatives.** (a) Advertise lazy-restore support as a node attribute
  at registration. (b) Run `cloud-hypervisor --version` on every restore.
- **Trade-off.** The probe runs in the worker, not the VMM. If something
  restricts the VMM more than the worker (a wrapper, a stricter seccomp
  profile), the probe says yes and the restore fails. The checkpoint's
  version is the capturing node's.
- **Why.** The probe tries what cloud-hypervisor tries, in the same order,
  as the same user, and costs one `open` or syscall per restore, so a
  sysctl change takes effect without a worker restart. Placement already
  requires the restoring node's VMM version to equal the checkpoint's, so
  the recorded version is this node's, without spawning a process (b). No
  node attribute, because nothing should place on it (entry 3).

## 7. The first resume after a cloud-hypervisor `--restore` waits up to five minutes

`CloudHypervisorApi::resume_restored` uses the 300 s snapshot timeout
instead of the 10 s control timeout.

- **Alternatives.** (a) Keep 10 s. (b) Poll `vm.info` until the VM
  reports it is paused and restored, then resume with the short timeout.
- **Trade-off.** A wedged cloud-hypervisor now holds a restore (and the
  task-manager lock) for up to five minutes before it is reaped, instead of
  ten seconds.
- **Why.** cloud-hypervisor serves its API socket before the restore
  finishes and queues the resume behind it, so the 10 s timeout failed every
  copy restore longer than that: about 3 GiB on the disk in
  cloud-hypervisor's own measurement (2 GB in 7.1 s). Each of those booted
  the instance cold. Dropping the rehash (entry 2) made it likelier, because
  the copy no longer reads from a page cache the rehash had just warmed. (b)
  adds a polling loop to wait for the same thing. Lazy restores, the
  default, return in well under a second either way.

## 8. A capture during cloud-hypervisor's background fill waits for it, paused

cloud-hypervisor v53 refuses `vm.snapshot` while an on-demand restore is
still loading memory. Legion's capture retries for up to 60 s with the VM
paused.

- **Alternatives.** (a) Fail the capture and let the caller retry (an idle
  suspend comes back on the next sweep, a user re-runs `snapshot create`).
  (b) Wait before pausing, by probing until the refusal stops.
- **Trade-off.** A live snapshot or fork taken in the seconds after a lazy
  resume pauses the guest for the rest of the fill, not the usual sub-second
  capture. A wedged fill holds the capture for a minute.
- **Why.** Forking right after a wake is exactly what an agent platform
  does, and (a) turns the new default into a failed fork. cloud-hypervisor
  reports "restoring" only as a refusal of the snapshot itself, so (b)
  would mean sending snapshots to an unpaused VM to see how they fail, and
  the fill finishes at the same moment either way. Most captures here are
  suspends of an idle guest, where pausing early costs nothing.

## 9. Phase 1 uses each VMM's own mechanism, with no Legion page-fault handler

- **Alternatives.** The old plan's `legion-uffd-handler`: Firecracker's
  `Uffd` backend, with a Legion process serving pages from the content
  store, and later from peers and the bucket.
- **Trade-off.** No streaming restore from the bucket and no working-set
  prefetch yet. A published checkpoint still downloads whole before it
  restores (Phase 2).
- **Why.** For a checkpoint already on the node, Firecracker's `File`
  backend is lazy with the kernel as the page server, which no userspace
  handler beats, and cloud-hypervisor's handler lives in the VMM process.
  A handler process is a new moving part on a security boundary, and it
  only pays for itself when the page source is remote, which is where Phase
  2 adds it (simplicity first, performance where it is measured).
