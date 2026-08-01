# Task Runtime: sandbox

`sandbox` is a built-in task runtime implemented in `src/task_plugins/sandbox.rs`
(with the microVM mechanics in `src/task_plugins/microvm.rs`).

## Purpose

Run an OCI image inside a **microVM** instead of a container. The workload gets
its own kernel, so escaping it means escaping a hypervisor rather than a
syscall filter — the isolation level you want for untrusted code, agent
workspaces, CI runners, and multi-tenant exec sandboxes.

The user-facing shape is deliberately close to
[`container`](task-runtime-container.md) (`image`, `command`, `env`) plus what a VM
needs and a container does not: guest size and a kernel. Persistent state is
not a runtime-specific field — it is a **block-mode volume**
([volumes](volumes.md)), because a guest kernel attaches a block device rather
than reading a host directory.

```jsonnet
{
    name: 'workspace',
    runtime: 'sandbox',
    group: 'sandboxes',
    stateful: {
        mode: 'keyed',
        // The persistent half of the sandbox: a formatted disk image served
        // by the local-dir driver, bound to the instance, attached to the VM
        // as /dev/vdb, and mounted at `path` by the generated guest init.
        storage: { path: '/workspace', mode: 'block', params: { size: '4G' } },
    },
    config: {
        image: 'alpine:latest',
        command: ['/bin/sh', '-c', 'cd /workspace && ./agent.sh'],
        vcpus: 2,
        memory_mib: 1024,
    },
}
```

## Runtime Selection

`config.hypervisor` (or the worker's `LEGION_SANDBOX_HYPERVISOR`) picks the
backend; the default is `auto`:

| Value | Backend | Isolation |
| --- | --- | --- |
| `auto` (default) | Linux+KVM: `cloud-hypervisor`, else `firecracker`. macOS: Apple `container`. | VM |
| `cloud-hypervisor` | cloud-hypervisor (Linux + `/dev/kvm`) | microVM |
| `firecracker` | Firecracker (Linux + `/dev/kvm`) | microVM |
| `dev` | macOS: Apple `container`. Linux: an OCI runtime (`youki` / `LEGION_OCI_RUNTIME`). | VM on macOS, **namespaces only** on Linux |

Binaries are found on `PATH`, overridable with `LEGION_CLOUD_HYPERVISOR_BIN` /
`LEGION_FIRECRACKER_BIN`. When no monitor is available the error names what to
install and points at `dev`; it never silently downgrades isolation.

Each worker logs the backend it resolved to when it loads the runtime at startup
(`Sandbox task runtime ready backend=cloud-hypervisor microvm=true`, or a warning
naming what is missing). Backend availability deliberately does **not** ride on
the runtime's node-readiness entry: readiness values are ANDed into node health,
which gates rollouts, and every worker loads this runtime whether or not it will
ever run a sandbox — so reporting "no hypervisor here" would fail deploys on
unrelated nodes. A task that cannot boot on its node fails at start with an
actionable error instead.

### macOS (dev mode)

cloud-hypervisor and Firecracker are Linux+KVM only, so on macOS the runtime runs
on Apple `container`, which gives each container its own lightweight VM
(Virtualization.framework). The isolation boundary is still a VM, but Apple
owns the kernel and disks, so the Legion-specific machinery does not apply:

- `kernel`, `kernel_args`, `rootfs_headroom_mib`, and `network` are ignored;
- `vcpus` / `memory_mib` are forwarded only when the jobspec sets them
  explicitly (otherwise the backend picks);
- volumes must be **filesystem** mode (bind-mounted, like the container runtime);
  a block volume fails at start — there is no VMM of Legion's to attach it to;
- exec **is** available here (`container exec`), unlike the microVM path.

A `warn!` is logged on every dev-mode start so it is never mistaken for the
real thing. File/tree artifacts are not copied into a dev-mode sandbox either
(only the image is consumed) — that copy happens while Legion builds the root
disk, which dev mode does not do.

### Linux dev mode

Explicitly asking for `hypervisor: 'dev'` on a Linux node without KVM (CI
containers, VMs without nested virt) runs the image through an OCI runtime
bundle — namespaces, not a VM. It is never selected automatically, exists so
the runtime is exercisable on machines that cannot boot a VM, and is not a
security boundary for untrusted code.

## How a microVM Boots

A microVM boots a kernel, not a container runtime: nothing mounts `/proc`,
exports env, attaches state, or shuts the machine down when the workload is
done. The runtime supplies exactly that layer.

1. **Get the image's OCI layout** — staged through the artifact pipeline when
   `artifacts.containerImageDesugar` is on, else a layout pulled by an earlier
   start of this allocation, else an anonymous pull through the shared `oci://`
   fetcher (`src/task_plugins/oci_image.rs`, shared with `container`).
2. **Build the root disk.** Extract the layers (same code the container runtime's
   bundle path uses), copy in staged artifacts, write the generated
   `/legion/init` and `/legion/env`, then format a raw **ext4** image with
   `mkfs.ext4 -d` — which populates a filesystem image straight from a
   directory, so no loop device, no mount, and no root are needed.
3. **Attach the volumes.** The worker mounted the task's block-mode volumes
   before `start` ([volumes](volumes.md)); each one's device or disk image
   becomes an extra virtio-blk device after the root disk, in declaration
   order: `/dev/vdb`, `/dev/vdc`, ….
4. **Boot.** cloud-hypervisor gets argv; Firecracker gets a `--no-api
   --config-file` JSON config. Both are single-process VMMs, so:
   - the guest serial console is the VMM's stdout, which lands in the
     allocation's `stdout.log` like any other task's output;
   - killing the child kills the VM — no daemon to chase (contrast the
     container runtime, which must tear down by name).

The generated `/legion/init` is plain POSIX shell (so it runs on whatever
`/bin/sh` the image ships) and: mounts `/proc`, `/sys`, `/dev`, `/dev/pts`,
`/dev/shm`, `/tmp`; brings up loopback; mounts each volume disk at its declared
`path` (with the filesystem the driver reported, read-only when claimed
read-only; a volume with no `path` is attached but left unmounted — raw device
access); sources `/legion/env`; `cd`s to the working directory; runs the
command; echoes `legion: sandbox command exited with status N`; and powers the
machine off so the VMM exits. The kernel cmdline carries `panic=1` as a
backstop, so even a wedged guest terminates instead of hanging the allocation.

**Rebuilds are fingerprinted.** The root disk is rebuilt only when the image,
command, env, working dir, volume mounts, or artifacts change — otherwise a
crash-looping sandbox would re-extract and re-format a multi-hundred-MiB image
on every restart. The fingerprint hashes env values (they can be secrets); they
are never logged or stored.

## Stateful Sandboxes

Sandboxes are usually stateful — a dev workspace, a database, an agent's home
directory. The runtime owns **none** of the storage machinery: persistence is the
[volume system](volumes.md), consumed in its **block** mode.

```jsonnet
stateful: {
    mode: 'keyed',
    storage: { path: '/workspace', mode: 'block', params: { size: '4G' } },
},
```

What each layer does:

1. The core binds a volume to the **instance** — provisioned on first
   placement, reattached on every later allocation for that instance, and
   reclaimed by the manager when the handle is dropped (instance destroy,
   scale-in without `retainOnScaleIn`, `legion volume delete`).
2. The **driver** serves the shape. `local-dir` in block mode produces a
   sparse ext4 disk image, formats it on first mount (`fresh: true` exactly
   once), and never reformats a reattach — the conformance suite pins that.
3. The **worker** mounts before start and hands this runtime the device path.
4. **This runtime** attaches it as `/dev/vdb` and the generated guest init mounts
   it at `path`. That's the runtime's entire storage job.

```bash
legion instance create agent-sandboxes --id user-42   # boots a microVM
# ... the sandbox writes to /workspace ...
# allocation dies (crash, drain, config update) → replacement reattaches
legion instance get user-42 -j                        # new allocId, same instanceId
# ... /workspace still has the data; only the root disk was rebuilt ...
legion instance stop user-42                          # paused, volume retained
legion instance start user-42                         # same data, new VM
legion instance destroy user-42                       # volume reclaimed by the manager
```

Design notes that matter:

- **The root disk is disposable, the volume is not.** Everything outside the
  volume mount lives in the ephemeral root disk (sized by
  `rootfs_headroom_mib`) and is discarded when the allocation ends. This is
  the property that makes "rebuild the sandbox, keep the workspace" cheap.
- **Retention and reclamation belong to the volume system.** `retainOnScaleIn`
  / `retainOnStop` govern the volume like any other stateful task's; the
  runtime's `destroy` touches only allocation scratch. There is no
  sandbox-specific disk lifecycle to operate.
- **Node pinning is automatic.** `local-dir` volumes are node-local, so the
  scheduler pins the instance to the node holding its data and refuses to
  silently restart it elsewhere on an empty disk — with
  `legion volume delete --force` as the explicit abandon-the-data escape
  ([volumes.md](volumes.md#when-a-replica-will-not-place)).
- **Single writer is guaranteed by the core.** A filesystem image tolerates
  exactly one writer, and Legion's at-most-one-live-allocation-per-instance
  invariant (worker self-fencing + the manager's loss-confirmation gate, and
  the admission-time rejection of `createFirst` rollouts on stateful tasks) is
  exactly that guarantee. Nothing in this runtime relaxes it.
- **`LEGION_STORAGE_DIR` / `LEGION_VOLUME_<NAME>`** are exported inside the
  guest with the *guest* mount path, so a payload finds its persistent
  directory without hardcoding it. (`exec` tasks get host paths in these vars;
  kinds with their own filesystem view get in-task paths.)
- **Mode pairing per backend:** microVM backends take `mode: 'block'` only
  (until virtio-fs lands); dev-mode backends bind-mount `filesystem` volumes
  only. A mismatch fails at start naming the volume and both ways out. In
  practice placement already steers this: a node that cannot serve block-mode
  `local-dir` (no `mkfs.ext4`) never advertises it.
- Task-level `volumes: [...]` claims work too — e.g. a read-only `host` block
  device shared across sandboxes on a bare-metal node. A claim with no `path`
  is attached as a raw, unmounted guest device.

## Config Schema

Required:

- `image`: image reference string.
- `command`: non-empty string array.

Optional:

| Field | Default | Meaning |
| --- | --- | --- |
| `env` | `{}` | Environment exported inside the sandbox (merged over the image's env, task wins). |
| `cwd` | image's `WorkingDir` | Working directory in the guest. |
| `vcpus` | `1` | Guest vCPUs. |
| `memory_mib` | `512` | Guest RAM (minimum 64). |
| `rootfs_headroom_mib` | `256` | Writable space added to the ephemeral root disk beyond the image contents. |
| `hypervisor` | `auto` | `auto`, `cloud-hypervisor`, `firecracker`, `dev`. |
| `kernel` | `LEGION_SANDBOX_KERNEL`, then `/var/lib/legion/kernels/vmlinux` | Uncompressed guest kernel. `${artifact.<name>}` works here. |
| `kernel_args` | none | Appended to Legion's kernel cmdline. |
| `network` | `{ mode: 'none' }` | `{ mode: 'tap', tap: 'tap0', mac: … }` attaches the guest to a **pre-created** host TAP device. |

Persistent state is **not** part of the runtime config: declare
`stateful.storage` (or `volumes[]`) on the task with `mode: 'block'` — see
[Stateful Sandboxes](#stateful-sandboxes). A leftover `config.state` block
from the pre-volume design is rejected at validation with the migration.

### Guest kernel

microVMs need an uncompressed kernel image (`vmlinux`, not `bzImage`). Point
`LEGION_SANDBOX_KERNEL` at one on each worker, install it at
`/var/lib/legion/kernels/vmlinux`, or ship it per-job as an artifact:

```jsonnet
artifacts: [{ name: 'kernel', source: 'https://…/vmlinux' }],
config: { kernel: '${artifact.kernel}', /* … */ },
```

A missing kernel fails at start with an error naming all three options.

## Networking

Guest networking needs a host TAP device, which needs `CAP_NET_ADMIN` and a
host addressing plan — so Legion does not create one. The default is
`mode: 'none'`: the guest has loopback only, which is the right default for
compute/agent sandboxes. With `mode: 'tap'` the operator (or a future network
plugin) pre-creates the device and Legion attaches it. `LEGION_PORT_*` /
`LEGION_ADDR_*` are still exported into the guest, but nothing maps host ports
into the VM yet.

That single, operator-owned interface is also what makes `sandbox` the one
runtime whose [edge boot](edge-boot.md) `boot` gate is a **real** network
boundary. A guest started before its placement is durable boots with the TAP
device attached but administratively **down** — Firecracker cannot hotplug a
NIC, so the gate has to be on the link rather than on the attachment — and
ratification is the only thing that raises it. With the default
`mode: 'none'` there is nothing to gate, which is strictly stronger. Legion
changes link state only; it installs no per-interface filter, so an operator
who wants belt-and-braces isolation should add one.

## Artifacts Support

`sandbox` reports `supports_artifacts()`, and — unlike `container` — file and
tree artifacts genuinely reach the workload:

- **Image desugaring:** `config.image` desugars into an implicit `oci://`
  artifact when the cluster sets `artifacts.containerImageDesugar`, so sandbox
  images get cluster auth matchers, digest pinning, and per-artifact status
  rows. With the flag off the runtime pulls anonymously itself.
- **File / tree / layer artifacts** are copied **into the guest root disk** at
  `/legion/artifacts/<name>` while it is being built. `LEGION_ARTIFACT_<NAME>`
  and any `${artifact.<name>}` path already substituted into the command are
  rewritten from the host staging path to the in-guest path, so a jobspec reads
  the same as it would for `exec`.

This is possible because the runtime builds its own root filesystem; a container's
isolated filesystem cannot see the host task dir at all.

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ❌ (baked at disk-build time; a change needs a restart) |
| Identity env / file delivery | ✅ / ❌ |
| Signal delivery | ❌ (needs a guest agent) |
| Memory / CPU limits, exclusive cores | ❌ — the runtime has not adopted the enforcement interface; `resources` size the guest and are otherwise scheduling-only. It is the designed-for next adopter (`RuntimeApplied`, pinning the VMM's vCPU threads) — see [docs/resource-enforcement.md](resource-enforcement.md) |
| Artifacts | ✅ image, file, tree, layers |
| Exec / interactive PTY | ❌ on the microVM path (needs a vsock guest agent); ✅ / ✅ in macOS dev mode |
| Volumes | ✅ `block` on microVM backends, `filesystem` on dev backends |

Sandbox exec is never host-level (`kind_is_host_level("sandbox") == false`), so
an `exec:write` rule without `host` suffices — see
[exec-and-actions.md](exec-and-actions.md).

## Trying It Locally

```bash
# Linux with KVM. Install cloud-hypervisor (or firecracker) and e2fsprogs,
# then point the worker at an uncompressed kernel:
export LEGION_SANDBOX_KERNEL=/path/to/vmlinux

cargo run -- start manager -c configs/manager.jsonnet   # terminal 1
cargo run -- join <TOKEN>                               # terminal 2

# terminal 3 — keyed sandboxes: submitting places nothing
cargo run -- job run configs/jobs/sandbox-microvm.jsonnet
cargo run -- instance create sandbox-vm --id user-42
cargo run -- job logs sandbox-vm            # guest console (serial) output
cargo run -- instance destroy user-42
```

On macOS the same job runs in dev mode with no kernel setup (Apple `container`
must be installed). To exercise the runtime on a Linux machine without KVM, set
`hypervisor: 'dev'` on the task.

The end-to-end boot test is opt-in:

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux \
  cargo test --lib sandbox_microvm_boots -- --ignored --nocapture
```

## Snapshots, Fast Start, and Forking

Snapshot/restore (disk and memory), sub-second restores, and forking one
prepared sandbox into many are designed in
[sandbox-snapshots-and-fast-start-plan.md](../internal_docs/plans/sandbox-snapshots-and-fast-start-plan.md)
— not implemented yet. The short version:

- Snapshots are **instance-scoped operations** with their own ACL resource, not
  [actions](exec-and-actions.md#actions): capturing memory dumps the sandbox's
  entire RAM, which is a different privilege from running a declared command
  inside it. Actions do serve as **quiesce hooks** (`fsfreeze`, DB checkpoint)
  around a capture.
- Snapshot blobs are content-addressed and ride the
  [artifact cache and pre-warm](../internal_docs/plans/artifact-cache-and-prewarm-plan.md)
  pipeline, so node caching, region-local fetch, and warm-node placement scoring
  come for free.
- Disk snapshot/clone is a volume driver op (reserved in the
  [volume contract](volumes.md)); memory/device capture is the task runtime's,
  since it owns the VMM connection.
- Forking needs per-fork identity (instance id, hostname, MAC, entropy, clock)
  rewritten after resume — the one piece that requires a small in-guest agent.

Note for that work: Firecracker is launched today with `--no-api`, which leaves
no control channel. Snapshot support means passing `--api-sock` alongside
`--config-file`. cloud-hypervisor already gets an API socket and is
snapshot-capable as-is.

## Current Scope and Limitations

- **No exec into a running microVM.** That needs a vsock guest agent; the runtime
  reports the capability honestly rather than failing at session open.
- **No signal delivery** into the guest (same reason), so an env-delivered
  identity must use `changeMode: restart`.
- **No virtio-fs.** A microVM sandbox consumes block volumes only; a
  `filesystem` volume on a microVM backend fails at start. Filesystem-mode
  support via virtio-fs (a `virtiofsd` per mount) is future work.
- **`local-dir` volumes are node-local** — the instance pins to the node
  holding its data ([volumes.md](volumes.md#node-local-vs-network)). Network
  block storage would need a driver that serves block mode from a remote
  backend.
- **The payload runs as root in the guest**; the image's `User` is not applied.
  The isolation boundary is the VM, not the guest uid.
- **`/bin/sh` must exist in the image** (the generated init is a shell script),
  so scratch/distroless images do not work yet.
- The guest's exit status is reported on the console, not as the VMM's exit
  code — restart policies see the VMM exit, and health checks work as usual.
- zstd-compressed image layers are rejected (gzip and uncompressed supported),
  inherited from the shared layer extraction.
