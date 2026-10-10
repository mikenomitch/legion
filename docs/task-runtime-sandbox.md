# Task Runtime: sandbox

`sandbox` is a built-in task runtime implemented in `src/task_plugins/sandbox.rs`
(with the microVM mechanics in `src/task_plugins/microvm.rs`).

## Purpose

Run an OCI image inside a microVM instead of a container. The workload gets
its own kernel, so escaping it means escaping a hypervisor instead of a
syscall filter. Use it for untrusted code, agent workspaces, CI runners, and
multi-tenant exec sandboxes.

The job config is close to [`container`](task-runtime-container.md) (`image`,
`command`, `env`). It adds what a VM needs and a container does not: guest size
and a kernel. Persistent state is not a runtime-specific field. It is a
block-mode volume ([volumes](volumes.md)), because a guest kernel attaches a
block device and cannot read a host directory.

```nickel
{
    name = "workspace",
    runtime = "sandbox",
    group = "sandboxes",
    stateful = {
        mode = "keyed",
        # The persistent half of the sandbox: a formatted disk image served
        # by the local-dir driver, bound to the instance, attached to the VM
        # as /dev/vdb, and mounted at `path` by the generated guest init.
        storage = { path = "/workspace", mode = "block", params = { size = "4G" } },
    },
    config = {
        image = "alpine:latest",
        command = ["/bin/sh", "-c", "cd /workspace && ./agent.sh"],
        vcpus = 2,
        memory_mib = 1024,
    },
}
```

## Runtime Selection

`config.hypervisor` (or the worker's `LEGION_SANDBOX_HYPERVISOR`) picks the
backend. The default is `auto`:

| Value | Backend | Isolation |
| --- | --- | --- |
| `auto` (default) | Linux+KVM: `cloud-hypervisor`, else `firecracker`. macOS: Apple `container`. | VM |
| `cloud-hypervisor` | cloud-hypervisor (Linux + `/dev/kvm`) | microVM |
| `firecracker` | Firecracker (Linux + `/dev/kvm`) | microVM |
| `dev` | macOS: Apple `container`. Linux: an OCI runtime (`youki` / `LEGION_OCI_RUNTIME`). | VM on macOS, **namespaces only** on Linux |

Legion finds the binaries on `PATH`. Override them with
`LEGION_CLOUD_HYPERVISOR_BIN` or `LEGION_FIRECRACKER_BIN`. When no monitor is
available, the error names what to install and points at `dev`. Legion never
silently downgrades isolation.

Each worker logs the backend it picked when it loads the runtime at startup
(`Sandbox task runtime ready backend=cloud-hypervisor microvm=true`, or a warning
naming what is missing). Backend availability is not part of the runtime's
node-readiness entry. Readiness values are ANDed into node health, which gates
rollouts, and every worker loads this runtime even if it never runs a sandbox.
Reporting "no hypervisor here" would fail deploys on unrelated nodes. Instead, a
task that cannot boot on its node fails at start with an actionable error.

### macOS (dev mode)

cloud-hypervisor and Firecracker run only on Linux with KVM. On macOS the
runtime uses Apple `container`, which gives each container its own lightweight
VM (Virtualization.framework). The isolation boundary is still a VM. Apple owns
the kernel and disks, though, so the Legion-specific machinery does not apply:

- `kernel`, `kernel_args`, `rootfs_headroom_mib`, and `network` are ignored.
- `vcpus` and `memory_mib` are forwarded only when the jobspec sets them.
  Otherwise the backend picks.
- Volumes must use filesystem mode (bind-mounted, like the container runtime).
  A block volume fails at start, because Legion has no VMM to attach it to.
- Exec is available here (`container exec`), unlike the microVM path.

Every dev-mode start logs a warning, so you won't mistake it for a microVM.
File and tree artifacts are not copied into a dev-mode sandbox either. Only the
image is used. That copy happens while Legion builds the root disk, which dev
mode does not do.

### Linux dev mode

Setting `hypervisor = "dev"` on a Linux node without KVM (CI containers, VMs
without nested virtualization) runs the image through an OCI runtime bundle.
That isolates with namespaces only, with no VM. Legion never selects it
automatically. It exists so you can exercise the runtime on machines that
cannot boot a VM. Do not treat it as a security boundary for untrusted code.

## How a microVM Boots

A microVM boots a kernel, with no container runtime around it. Nothing mounts
`/proc`, exports env, attaches state, or shuts the machine down when the
workload is done. The sandbox runtime supplies that layer.

1. **Get the image's OCI layout.** Legion uses the layout staged through the
   artifact pipeline when `artifacts.containerImageDesugar` is on. Otherwise it
   reuses a layout pulled by an earlier start of this allocation, or pulls
   anonymously through the `oci://` fetcher it shares with `container`
   (`src/task_plugins/oci_image.rs`).
2. **Build the root disk.** Extract the layers (with the same code as the
   container runtime's bundle path), copy in staged artifacts, and write the
   generated `/legion/init` and `/legion/env`. Then format a raw ext4 image with
   `mkfs.ext4 -d`. This builds a filesystem image straight from a directory, so
   it needs no loop device, no mount, and no root.
3. **Attach the volumes.** The worker mounts the task's block-mode volumes
   before `start` ([volumes](volumes.md)). Each one's device or disk image
   becomes an extra virtio-blk device after the root disk, in declaration
   order: `/dev/vdb`, `/dev/vdc`, ….
4. **Boot.** cloud-hypervisor gets argv. Firecracker gets a `--no-api
   --config-file` JSON config. Both are single-process VMMs, so:
   - The guest serial console is the VMM's stdout. It lands in the
     allocation's `stdout.log` like any other task's output.
   - Killing the child kills the VM. There is no daemon to chase (unlike the
     container runtime, which must tear down by name).

The generated `/legion/init` is plain POSIX shell, so it runs on whatever
`/bin/sh` the image ships. In order, it:

- mounts `/proc`, `/sys`, `/dev`, `/dev/pts`, `/dev/shm`, and `/tmp`
- brings up loopback
- mounts each volume disk at its declared `path`, with the filesystem the
  driver reported (read-only when claimed read-only). A volume with no `path`
  is attached but left unmounted for raw device access.
- sources `/legion/env` and `cd`s to the working directory
- runs the command and echoes `legion: sandbox command exited with status N`
- powers the machine off so the VMM exits

The kernel cmdline carries `panic=1` as a backstop, so even a wedged guest
terminates instead of hanging the allocation.

**Rebuilds are fingerprinted.** Legion rebuilds the root disk only when the
image, command, env, working dir, volume mounts, or artifacts change. Without
this, a crash-looping sandbox would re-extract and re-format a
multi-hundred-MiB image on every restart. The fingerprint hashes env values,
since they can be secrets. The values are never logged or stored.

## Stateful Sandboxes

Sandboxes are usually stateful: a dev workspace, a database, an agent's home
directory. The runtime owns none of the storage machinery. Persistence comes
from the [volume system](volumes.md), used in its block mode.

```nickel
stateful = {
    mode = "keyed",
    storage = { path = "/workspace", mode = "block", params = { size = "4G" } },
},
```

What each layer does:

1. The core binds a volume to the instance. The volume is provisioned on first
   placement and reattached on every later allocation for that instance. The
   manager reclaims it when the handle is dropped (instance destroy, scale-in
   without `retainOnScaleIn`, `legion volume delete`).
2. The driver serves the volume. `local-dir` in block mode produces a sparse
   ext4 disk image and formats it on first mount (`fresh = true` once). It never
   reformats on reattach, and the conformance suite tests that.
3. The worker mounts the volume before start and hands this runtime the device
   path.
4. The sandbox runtime attaches it as `/dev/vdb`, and the generated guest init
   mounts it at `path`. That is all the runtime does for storage.

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

Design notes:

- **The root disk is disposable. The volume is not.** Everything outside the
  volume mount lives in the ephemeral root disk (sized by
  `rootfs_headroom_mib`) and is discarded when the allocation ends. This makes
  "rebuild the sandbox, keep the workspace" cheap.
- **The volume system handles retention and reclamation.** `retainOnScaleIn`
  and `retainOnStop` govern the volume like any other stateful task's. The
  runtime's `destroy` touches only allocation scratch. There is no
  sandbox-specific disk lifecycle to operate.
- **Node pinning is automatic.** `local-dir` volumes are node-local. The
  scheduler pins the instance to the node holding its data and won't silently
  restart it elsewhere on an empty disk. To abandon the data, run
  `legion volume delete --force`
  ([volumes.md](volumes.md#when-a-replica-will-not-place)).
- **The core guarantees a single writer.** A filesystem image tolerates only
  one writer. Legion allows at most one live allocation per instance, enforced
  by worker self-fencing, the manager's loss-confirmation gate, and rejecting
  `createFirst` rollouts on stateful tasks at admission. Nothing in this
  runtime relaxes that.
- **`LEGION_STORAGE_DIR` and `LEGION_VOLUME_<NAME>`** are exported inside the
  guest with the guest mount path, so a payload finds its persistent directory
  without hardcoding it. (`exec` tasks get host paths in these vars. Runtimes
  with their own filesystem view get in-task paths.)
- **Each backend takes one volume mode.** microVM backends take
  `mode = "block"` only (until virtio-fs support exists). Dev-mode backends
  bind-mount `filesystem` volumes only. A mismatch fails at start, naming the
  volume and both fixes. In practice placement already handles this: a node
  that cannot serve block-mode `local-dir` (no `mkfs.ext4`) never advertises it.
- Task-level `volumes = [...]` claims work too, such as a read-only `host` block
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

Persistent state is not part of the runtime config. Declare
`stateful.storage` (or `volumes[]`) on the task with `mode = "block"`. See
[Stateful Sandboxes](#stateful-sandboxes). Validation rejects an old
`config.state` block and tells you how to migrate it.

### Guest kernel

microVMs need an uncompressed kernel image (`vmlinux`, not `bzImage`). Point
`LEGION_SANDBOX_KERNEL` at one on each worker, install it at
`/var/lib/legion/kernels/vmlinux`, or ship it per-job as an artifact:

```nickel
artifacts = [{ name = "kernel", source = "https://example.com/vmlinux" }],
config = { kernel = "${artifact.kernel}", image = "alpine:latest" },
```

A missing kernel fails at start. The error names all three options: the task's
`kernel` field, `LEGION_SANDBOX_KERNEL` on the worker, or a kernel installed at
`/var/lib/legion/kernels/vmlinux`.

## Networking

Guest networking needs a host TAP device. That needs `CAP_NET_ADMIN` and a
host addressing plan, so Legion does not create one. The default is
`mode = "none"`, which gives the guest loopback only. That suits compute and
agent sandboxes. With `mode = "tap"`, the operator (or a future network
plugin) pre-creates the device and Legion attaches it. `LEGION_PORT_*` and
`LEGION_ADDR_*` are still exported into the guest, but nothing maps host ports
into the VM yet.

**Gated networking.** Set the task's `network.mode = "gated"` (the block next
to `service` and `connect`, not `config.network`). The guest then boots with
no NIC at all. Its only way out is a vsock to the node datapath, where policy,
handlers, and identity judge every connection. The worker binds the host end
before boot. The generated init starts `legion guest-agent` (the Legion binary,
copied into the rootfs) to relay the guest's loopback proxy and resolver ports
over the vsock. See [network-handlers.md](network-handlers.md#sandboxes) for
the mechanics, what is enforced, and current status. It is unit-tested over
Unix sockets but not yet boot-tested on a VMM in CI.

This single, operator-owned interface also makes `sandbox` the only runtime
whose [edge boot](edge-boot.md) `boot` gate is a true network boundary. A
guest started before its placement is durable boots with the TAP device
attached but administratively down. Firecracker cannot hotplug a NIC, so the
gate has to be on the link instead of on the attachment. Only ratification
brings the link up. With the default `mode = "none"` there is nothing to gate,
which is stronger still. Legion changes link state only and installs no
per-interface filter. If you want extra isolation, add one yourself.

## Artifacts Support

`sandbox` reports `supports_artifacts()`. Unlike `container`, file and tree
artifacts reach the workload:

- **Image desugaring:** `config.image` desugars into an implicit `oci://`
  artifact when the cluster sets `artifacts.containerImageDesugar`, so sandbox
  images get cluster auth matchers, digest pinning, and per-artifact status
  rows. With the flag off the runtime pulls anonymously itself.
- **File / tree / layer artifacts** are copied **into the guest root disk** at
  `/legion/artifacts/<name>` while it is being built. `LEGION_ARTIFACT_<NAME>`
  and any `${artifact.<name>}` path already substituted into the command are
  rewritten from the host staging path to the in-guest path, so a jobspec reads
  the same as it would for `exec`.

This works because the runtime builds its own root filesystem. A container's
isolated filesystem cannot see the host task dir at all.

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ❌ (baked in at disk-build time, so a change needs a restart) |
| Identity env / file delivery | ✅ / ❌ |
| Signal delivery | ❌ (needs a guest agent) |
| Memory / CPU limits, exclusive cores | ❌ The runtime has not adopted the enforcement interface. `resources` size the guest and are otherwise used only for scheduling. See [docs/resource-enforcement.md](resource-enforcement.md). |
| Artifacts | ✅ image, file, tree, layers |
| Exec / interactive PTY | ❌ on the microVM path (needs a vsock guest agent). ✅ / ✅ in macOS dev mode. |
| Volumes | ✅ `block` on microVM backends, `filesystem` on dev backends |

Sandbox exec is never host-level (`kind_is_host_level("sandbox") == false`), so
an `exec:write` rule without `host` is enough. See
[exec-and-actions.md](exec-and-actions.md).

## Trying It Locally

```bash
# Linux with KVM. Install cloud-hypervisor (or firecracker) and e2fsprogs,
# then point the worker at an uncompressed kernel:
export LEGION_SANDBOX_KERNEL=/path/to/vmlinux

cargo run -- start manager -c configs/manager.ncl   # terminal 1
cargo run -- join <TOKEN>                               # terminal 2

# terminal 3 — keyed sandboxes: submitting places nothing
cargo run -- job run configs/jobs/sandbox-microvm.ncl
cargo run -- instance create sandbox-vm --id user-42
cargo run -- job logs sandbox-vm            # guest console (serial) output
cargo run -- instance destroy user-42
```

On macOS the same job runs in dev mode with no kernel setup (Apple `container`
must be installed). To exercise the runtime on a Linux machine without KVM, set
`hypervisor = "dev"` on the task.

The end-to-end boot test is opt-in:

```bash
LEGION_SANDBOX_KERNEL=/path/to/vmlinux \
  cargo test --lib sandbox_microvm_boots -- --ignored --nocapture
```

## Snapshots, Fast Start, and Forking

The sandbox runtime does not support snapshots, snapshot restore, or forking
one sandbox into many. Every sandbox boots from scratch.

## Current Scope and Limitations

- **No exec into a running microVM.** That needs a vsock guest agent. The
  runtime reports the capability as unsupported up front instead of failing
  when a session opens.
- **No signal delivery** into the guest, for the same reason. An env-delivered
  identity must use `changeMode = "restart"`.
- **No virtio-fs.** A microVM sandbox uses block volumes only. A `filesystem`
  volume on a microVM backend fails at start. Filesystem-mode support via
  virtio-fs (a `virtiofsd` per mount) is future work.
- **`local-dir` volumes are node-local.** The instance pins to the node holding
  its data ([volumes.md](volumes.md#node-local-vs-network)). Network block
  storage would need a driver that serves block mode from a remote backend.
- **The payload runs as root in the guest.** The image's `User` is not applied.
  The isolation boundary is the VM, not the guest uid.
- **`/bin/sh` must exist in the image**, because the generated init is a shell
  script. Scratch and distroless images do not work yet.
- The guest's exit status is reported on the console, not as the VMM's exit
  code. Restart policies see the VMM exit, and health checks work as usual.
- zstd-compressed image layers are rejected. Gzip and uncompressed layers work.
  This comes from the shared layer extraction code.
