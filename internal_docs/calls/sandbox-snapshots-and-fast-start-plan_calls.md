# Judgement calls: sandbox snapshots (capture mechanics)

Calls about the memory-checkpoint mechanics this plan remains the reference
for ([plan](../plans/sandbox-snapshots-and-fast-start-plan.md)): device
links, the identity disk, and what a restore rewrites. Scope, phasing, and
blob transport are the
[suspend plan's](suspend-resume-and-oversubscription-plan_calls.md).

**Entries are ranked by project impact, highest first; an entry's number is
its rank.** Anything here a user needs to know is also stated in
[the sandbox runtime doc](../../docs/task-runtime-sandbox.md#where-a-memory-checkpoint-can-be-restored)
or [suspend and resume](../../docs/suspend-and-resume.md#limitations).

## 1. Device links live at `<data dir>/vmdev/<key>/`, not under the worker-id-keyed state dir, and the launch records the link dir in the checkpoint

*Found by the 2026-10-12 AWS spot check: a `durability = "store"` sandbox
suspended on `sc-wkr-2` failed to resume on `sc-wkr-1` with
`VM Restore failed: DeviceManager(Disk(Os { code: 2, kind: NotFound }))`
and cold-booted. Same-node resumes worked.*

**The call.** Both VMMs record each disk's host path in the saved state and
reopen exactly that path. The links were under
`<data dir>/workers/<worker-id>/allocs/.vmdev/<key>/`, and the worker id
differs on every node, so a published checkpoint named paths the restoring
node never creates. The link root moves to the node-level
`<data dir>/vmdev/` (`TaskInstance::device_link_root`, set by worker core),
which is identical on nodes that share a data dir. The launch also records
the absolute link dir in the checkpoint's runtime state (`linkDir`), so a
restore knows where the saved state points.

**Alternatives.**
- Recreate the recorded link paths verbatim on the restoring node. This is
  what a VMM wants, and it needs no new field on `TaskInstance`. But it
  would create `<data dir>/workers/<other-worker-id>/` on the restoring
  node, and the worker picks its id by listing that directory: its next
  restart fails with `Multiple worker IDs found`. It would also let a
  manifest choose where a root worker creates directories.
- Keep the per-worker root and rewrite paths on restore for both VMMs.
  Firecracker's `vm-state` is an opaque binary with no drive override on
  `PUT /snapshot/load` (only `network_overrides` and `vsock_override`), so
  this only works for cloud-hypervisor.
- A fixed host path such as `/run/legion/vmdev`. Identical everywhere, even
  across data dirs, but unwritable for a non-root dev worker, and outside
  everything `LEGION_DATA_DIR` promises to relocate.

**Trade-off.** Workers on one host that share a data dir share the link root.
`LINK_LOCK` serializes repoints within one process only, so two such workers
restoring forks of the same checkpoint at the same instant could race. This
is a dev-only layout (the data-dir doc tells you to isolate). Firecracker
still needs the same data dir on both nodes (entry 2).

**Why.** Simplicity: the default install (`LEGION_DATA_DIR=/var/lib/legion`
everywhere) now moves checkpoints between nodes on both VMMs with no new
knob, and the data dir is already the thing operators keep uniform.

## 2. cloud-hypervisor's `config.json` disks are rewritten to this node's links on every restore. Firecracker on another data dir is refused by name

**The call.** Every cloud-hypervisor restore already rewrites its own copy
of `config.json` for the vsock socket and a fork's TAP. It now also points
every `disks[].path` named `<link_key>/<name>` at this node's link of that
name, so a cloud-hypervisor checkpoint restores on any node whatever its
data dir. A disk outside the links that does not exist here is refused
before the VMM runs (`restore needs disk <path>: ...`) instead of
cloud-hypervisor's `Cannot open disk path`. A Firecracker restore whose
recorded `linkDir` differs from this node's fails before anything is
spawned or linked, so `onRestoreFailure` decides:

```text
checkpoint snap-… reopens its disks under /var/lib/legion/vmdev/sbx-…, but this node keeps device links under /opt/legion/vmdev/sbx-…: Firecracker restores a guest's disks only at the paths it captured, so the capturing and restoring nodes need the same data directory (LEGION_DATA_DIR)
```

**Alternatives.**
- Rely on entry 1 alone for cloud-hypervisor too. Less code, but it leaves
  a restore across data dirs failing with the VMM's opaque error.
- Filter placement on the data dir, so a Firecracker checkpoint only lands
  where it can restore. That puts a node attribute and a scheduler
  constraint on a misconfiguration that a uniform install never hits.

**Trade-off.** A Firecracker checkpoint published across nodes with
different data dirs still places there and then cold-boots (or fails, per
`onRestoreFailure`), with the reason in the event, not at placement.

**Why.** Simplicity: one rewrite pass that was already there, and a named
refusal in place of a VMM stack trace. The documented limitation is in
`docs/suspend-and-resume.md` (Limitations).
