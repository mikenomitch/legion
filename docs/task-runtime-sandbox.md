# Task Runtime: sandbox

`sandbox` is a built-in task runtime implemented in `src/task_plugins/sandbox.rs`
(with the microVM mechanics in `src/task_plugins/microvm.rs`).

## Purpose

Run an OCI image inside a microVM instead of a container. The workload gets
its own kernel, so escaping it means escaping a hypervisor instead of a
syscall filter. Use it for untrusted code, agent workspaces, CI runners, and
multi-tenant exec sandboxes. For running many of them (what each create,
suspend, and resume costs, and how to measure it), see
[Sandboxes at scale](sandboxes-at-scale.md).

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
    },
    # The guest boots with 1024 MiB (memoryMax) and the scheduler reserves
    # 512 MiB for it. See "Guest size" below.
    resources = { memory = 512, memoryMax = 1024 },
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
| `dev` | macOS: Apple `container`. Linux: an OCI runtime (`runc`, or `crun` via `LEGION_OCI_RUNTIME`). | VM on macOS, **namespaces only** on Linux |

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
- `vcpus` is forwarded only when the jobspec sets it. Otherwise the backend
  picks. Memory follows the same [guest size](#guest-size) rule as a
  microVM.
- Apple `container` refuses a VM under **200 MiB**, a higher floor than a
  microVM's 64 MiB. A sandbox whose constraints pin it to macOS
  (`constraints = { os = "macos" }`) and sizes its guest below 200 MiB is
  rejected at submit. Any other sandbox that lands on a macOS worker that
  small fails at start with a message naming the floor and the field to
  raise, instead of the runtime's error appearing only in the task's stderr.
- The command follows the image directly, with no `--` separator: Apple
  `container` would run a `--` as the executable ("failed to find target
  executable --"). `container exec` into the sandbox has none either.
- Volumes must use filesystem mode (bind-mounted, like the container runtime).
  A block volume fails at start, because Legion has no VMM to attach it to.
- Exec is available here through `container exec`. A microVM takes exec
  through its guest agent instead ([Exec](#exec)).

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
It runs on the same OCI runtime as the `container` runtime: runc by default,
or crun through `LEGION_OCI_RUNTIME`. Any other runtime fails the start with
the error described in [Installing: crun
instead](installation.md#crun-instead).

The bundle is the one the `container` runtime builds, with its defaults:
image layers extracted with their owners and modes, Docker's default
capabilities without `NET_RAW`, `RLIMIT_NOFILE` at 1048576, and the task's log
files handed to the image's user so it can reopen `/dev/stdout`. A sandbox has
no `security` or `ulimits` fields; those defaults are fixed. See
[docs/task-runtime-container.md](task-runtime-container.md#linux-image--bundle).

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
4. **Boot.** cloud-hypervisor gets argv, plus an `--api-socket`. Firecracker
   gets a `--config-file` JSON config and `--no-api`, unless the allocation is
   launched for memory checkpoints (see [Checkpoints](#checkpoints)) or boots
   with a balloon (see [Guest memory under pressure](#guest-memory-under-pressure)),
   in which case it gets `--api-sock` instead. Both are single-process VMMs, so:
   - The guest serial console is the VMM's stdout. It lands in the
     allocation's `stdout.log` like any other task's output.
   - Killing the child kills the VM. There is no daemon to chase (unlike the
     container runtime, which must tear down by name). That is also why the
     worker stops a microVM from inside rather than by killing it (see
     [Stopping a sandbox](#stopping-a-sandbox)).

The generated `/legion/init` is plain POSIX shell, so it runs on whatever
`/bin/sh` the image ships. In order, it:

- mounts `/proc`, `/sys`, `/dev`, `/dev/pts`, `/dev/shm`, and `/tmp`
- brings up loopback
- mounts each volume disk at its declared `path`, with the filesystem the
  driver reported (read-only when claimed read-only). A volume with no `path`
  is attached but left unmounted for raw device access.
- starts the [guest agent](#the-guest-agent) when the guest needs it: the
  gated datapath's relay, and the resume handler for a guest launched for
  memory checkpoints or with identity token files
- sources `/legion/env` and `cd`s to the working directory
- runs the command and echoes `legion: sandbox command exited with status N`.
  While it runs, init answers a [stop request](#stopping-a-sandbox).
- flushes the disks and unmounts the volumes. It first kills whatever still
  holds them open: strays the command left, exec sessions, and the agent.
- powers the machine off so the VMM exits

The kernel cmdline carries `panic=1` as a backstop, so even a wedged guest
terminates instead of hanging the allocation.

**Rebuilds are fingerprinted.** Legion rebuilds the root disk only when the
image, command, env, working dir, volume mounts, artifacts, or guest agent
binary change. Without this, a crash-looping sandbox would re-extract and
re-format a multi-hundred-MiB image on every restart. The fingerprint hashes
env values, since they can be secrets. The values are never logged or stored.

### The guest agent

Every microVM runs a small Legion program next to the workload, copied into
the root disk as `/legion/legion` and started by init (`guest-agent`):

- **The exec listener**, on every microVM. It takes `legion exec` and action
  sessions on guest vsock port 1100 ([Exec](#exec)).
- **The gated datapath's relay**, on a gated sandbox. It relays the guest's
  loopback proxy and resolver ports to the node over vsock, relays the
  node's inbound connections on each advertised port (`--ingress-port
  <port>` on its command line) to the workload's loopback port, and relays
  each upstream's loopback port and VIP (`--upstream <port>=<vip>:<port>`)
  to the node's listener for it
  ([network-handlers.md](network-handlers.md#sandboxes)). On a TAP or
  `none` sandbox the agent runs `--no-relay`: no loopback listeners, and the
  node binds nothing the guest could dial, so the guest still cannot reach
  the host.
- **The resume handler**, on a sandbox launched for memory checkpoints or
  with identity token files. It refreshes the guest's identity, RNG, clock,
  token files, and a TAP guest's address after a restore and runs `onResume`
  ([Identity after a restore](#identity-after-a-restore)). On a gated
  sandbox the relay runs it in the same process, so a restore also closes
  the relays the capture left dead.

The program is `legion-guest`, a statically linked build of just these
jobs (under 1 MB), which runs on any image with `/bin/sh`: glibc, musl
(`alpine:latest`), and busybox alike. Ship it with the worker:

| Where the worker looks | |
| --- | --- |
| `LEGION_GUEST_BINARY` in the worker's environment | Used when set. A path that does not exist fails the sandbox's start with that reason |
| `legion-guest` in the same directory as the worker binary | The normal install |
| neither | The worker copies **itself**, and warns at start: `No static legion-guest binary beside the worker` |

The fallback works only where the image's libc can run the worker binary (a
glibc image close enough to the worker's build host). On a musl image it
cannot, so init prints `legion: guest agent missing ...` or the binary fails
to start. The guest then takes no exec sessions, a gated guest has no
network at all (fail-closed), and a restored guest keeps its old identity. Installing `legion-guest` fixes both;
the root disk rebuilds on the next start, because the binary is part of its
fingerprint.

Build it for each architecture the workers run:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build -p legion-guest --profile dist --target x86_64-unknown-linux-musl
# target/x86_64-unknown-linux-musl/dist/legion-guest
```

### Exec

`legion exec`, `legion action run`, the MCP exec tools, and the UI terminal
reach a microVM the same way they reach a container. The worker connects to
the VMM's vsock host socket (`<alloc>/.sandbox/vsock.sock`), sends
`CONNECT 1100`, and the guest agent runs the command:

```console
$ legion exec --instance user-42 -it -- /bin/bash
root@legion-sbx-88213:/workspace# exit 3
$ echo $?
3
```

- The command runs as root in the guest, in the task's environment
  (`/legion/env`, and `/legion/resume.env` after a restore) and its working
  directory, under a pty with `-t` or with separate stdout and stderr
  without. The exit status is the process's own.
- Closing the session, or the client vanishing, kills the session's whole
  process group in the guest.
- A memory capture resets open sessions. One that was open ends with
  `session ended: the guest closed it without an exit status (a suspend or a
  restore resets open exec sessions)`. After a resume the instance is a new
  allocation, so `--instance` finds it.
- An open session is the idle policy's `exec` signal: a sandbox someone is
  working in is never suspended as idle
  ([suspend-and-resume.md](suspend-and-resume.md#what-each-signal-sees-on-a-sandbox)).
- The worker treats the agent as untrusted plumbing: frames are capped at 1
  MiB, anything that is not a frame ends the session, and a guest runs at most
  64 sessions at once.

When the agent is not there (a guest restored from a checkpoint captured
before exec support, or an image the agent binary cannot run on), the session
fails by name:

```text
Error: sandbox exec needs the guest agent, which is not listening in this guest (the VMM closed the connection on CONNECT)
Hint: the agent starts with the guest; a sandbox resumed from a checkpoint captured before exec support has none until its next cold boot, and an image the agent cannot run on (no static legion-guest on this worker) cannot take exec sessions
```

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

## Stopping a sandbox

A write inside the guest lands in the guest kernel's page cache first. The
guest writes it back to the volume disk on its own schedule, and Linux can
wait about 30 seconds. Killing the VMM before then loses the write, even
though the workload saw it succeed. So the worker never stops a running
microVM by killing it.

When an allocation stops (`instance stop`, `instance destroy`, `job stop`, a
drain, a rollout, a node-pressure eviction), the worker:

1. Asks the guest to stop. The guest agent runs `kill -s <signal> 1` with
   the task's `stop.signal` (default `SIGTERM`), signalling the guest's init.
2. Init flushes the guest's disks (`sync`) right away, then passes the signal
   to the command.
3. Init waits for the command to exit, kills whatever is left, unmounts the
   volumes, and powers off.
4. The worker holds the allocation, and its volumes, until the VMM exits.
   Only then does it unmount the volume on the host.

If the VMM is still running at `killAfter` (default `30s`), the worker runs
`sync` in the guest once more, for up to 10 seconds, then kills the VMM.

```nickel
stop = { signal = "SIGTERM", killAfter = "60s" },
```

The console (`stdout.log`) shows the stop:

```text
legion: stop requested (SIGTERM); flushing disks and signalling the sandbox command
legion: sandbox command exited with status 143
```

**What is guaranteed:** every write the workload finished before the stop
request reaches the volume, whether or not the workload handles the signal,
as long as the guest agent answers. A write made after the request is safe
if the workload exits within `killAfter`, or if the final flush reaches it.

The same flush covers the paths that end a VMM without asking first:

- **An in-place restart and a disk suspend** stop the VMM directly. The
  worker runs `sync` in the guest, for up to 10 seconds, before it kills the
  VMM, so the restarted guest or the volume snapshot has every earlier
  write.
- **A memory checkpoint** runs `sync` before it pauses the guest. The capture
  is consistent without it, because the page cache is in the memory image.
  The flush also puts every earlier write on the volume itself, which is
  what a cold boot of the instance sees if the memory is never restored.

When the stop does not go to plan:

- **No guest agent** (an image the agent binary cannot run on, or a guest
  restored from a checkpoint captured before exec support): the request
  fails within 3 seconds, and the worker logs `Could not deliver a stop
  request into the sandbox guest; stopping it hard` and kills the VMM at
  once. The final flush fails the same way (`Could not flush the sandbox
  guest's disks before stopping its VMM`), so writes the guest had not
  written back are lost. Install `legion-guest` beside the worker
  ([The guest agent](#the-guest-agent)).
- **A command that ignores the signal** keeps the guest up until
  `killAfter`. Writes from before the request are already on the volume.
  Later ones depend on the final flush.
- **A wedged guest** (a hung kernel, an agent that does not answer) costs at
  most 3 seconds for the request and 10 for the final flush. Then the VMM is
  killed, and writes still in the guest's page cache are lost.
- **A guest restored from a memory checkpoint taken by an older worker**
  runs an init with no stop handler, so its kernel drops the request. The
  guest runs until `killAfter`, then gets the final flush and the kill.
- **`SIGINT` and `SIGQUIT` reach the command ignored.** Init runs the command
  as a background job, so it can answer the request while the command runs,
  and a POSIX shell starts background jobs with those two signals ignored. A
  program that installs its own handler still receives them. A shell script
  cannot. Use `SIGTERM`, `SIGHUP`, `SIGUSR1`, or `SIGUSR2`.
- **A volume init cannot unmount**, even after every other process is gone,
  is remounted read-only. If that fails too, the console shows `legion:
  could not unmount volume at <path>; its data was synced`, and the next
  boot replays the filesystem journal.

The dev-mode backends run no guest, so they are torn down at once, with no
grace period.

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
| `memory_mib` | derived from `resources` | Guest RAM (minimum 64). Leave it unset: the manager fills it at submit from `resources.memoryMax`, else `resources.memory`, else 512 ([Guest size](#guest-size)). An explicit value still wins in this release, with a submit warning when it disagrees. |
| `rootfs_headroom_mib` | `256` | Writable space added to the ephemeral root disk beyond the image contents. |
| `hypervisor` | `auto` | `auto`, `cloud-hypervisor`, `firecracker`, `dev`. |
| `kernel` | `LEGION_SANDBOX_KERNEL`, then `/var/lib/legion/kernels/vmlinux` | Uncompressed guest kernel. `${artifact.<name>}` works here. |
| `kernel_args` | none | Appended to Legion's kernel cmdline. |
| `network` | `{ mode: 'none' }` | `{ mode: 'tap', bridge: 'br0' }` gives the guest a NIC on a TAP device Legion creates per instance on that host bridge, with an address leased from the bridge's pool. See [Networking](#networking). |

Persistent state is not part of the runtime config. Declare
`stateful.storage` (or `volumes[]`) on the task with `mode = "block"`. See
[Stateful Sandboxes](#stateful-sandboxes). Validation rejects an old
`config.state` block and tells you how to migrate it.

### Guest size

The guest's RAM follows the task's `resources`, so what the scheduler
reserves and what the VM boots with can't drift apart:

- **`resources.memoryMax` when set.** The guest can use up to that much, the
  scheduler reserves `resources.memory`, and on a worker with resource
  enforcement the cgroup protects the reservation with `memory.low`. A
  sandbox with `memory = 512, memoryMax = 4096` boots a 4 GiB guest and
  reserves 512 MiB.
- **Else `resources.memory`.** A sandbox task that declares no `memory`
  gets the runtime's own default of 512 MiB, the size sandboxes have always
  booted with. That default wins over the manager's
  `taskResourceDefaults.memory`, which is tuned for host processes and would
  otherwise resize every guest. The scheduler reserves the same 512 MiB the
  guest boots with.

The size is fixed when the job is submitted: the manager writes it into the
stored config as `memory_mib`, and the worker boots exactly that. A job
stored by an older manager (no `memory_mib`) keeps booting at 512 MiB.
Resubmitting such a job pins the size and its reservation (now 512 MiB
instead of the old 256 MiB default), so the resubmit shows as a new version.

With enforcement on, the allocation's cgroup ceiling is the guest size plus
the worker's `sandbox.vmmOverheadMiB` (default 64). See
[resource-enforcement.md](resource-enforcement.md#sandbox-tasks).

The guest's vCPU count is `config.vcpus` (default 1), independent of
`resources.cpus`, `cpu`, and `cores`, which only govern the VMM's threads on
the host.

**Upgrading from `memory_mib`.** Before this rule, `config.memory_mib` sized
the guest (default 512) and nothing tied it to `resources.memory`. An
explicit `memory_mib` keeps working in this release. When it disagrees with
`resources`, `job run` warns:

```console
warning: task workspace sets config.memory_mib = 1024, but its resources size the guest at
512 MiB (resources.memoryMax, else resources.memory). This release still boots 1024 MiB, and
a future release rejects the mismatch. Drop memory_mib and set resources.memoryMax = 1024 to
burst to it, or resources.memory = 1024 to reserve it. See docs/task-runtime-sandbox.md
```

A `memory_mib` with no `resources.memory` is compared with the 512 MiB
default, so `memory_mib = 1024` alone also warns. A derived guest under
64 MiB fails at submit.

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

cloud-hypervisor publishes guest kernels that work here on its
[linux releases page](https://github.com/cloud-hypervisor/linux/releases)
(`vmlinux-x86_64`). [Installing Legion and Its Runtimes](installation.md#3-install-a-guest-kernel)
has the commands, along with installing cloud-hypervisor and checking
`/dev/kvm`.

Images reach the root disk through the same node
[image cache](task-runtime-container.md#image-cache) the `container` runtime
uses, so a second sandbox of one image on a node downloads nothing.

## Networking

A sandbox's network is one of three shapes:

| Shape | How | What the guest gets |
| --- | --- | --- |
| Loopback only | The default (`config.network` unset, or `{ mode = "none" }`) | No NIC. Suits compute sandboxes. Nothing can reach its ports, so submit refuses `network.ports` |
| Gated | The task's `network.mode = "gated"` (see [Gated networking](#gated-networking) below) | No NIC. A vsock to the node datapath, where policy, handlers, and identity judge every connection |
| TAP | `config.network = { mode = "tap", bridge = "br0" }` | A NIC on a host bridge, with an address Legion leases it |

Gated is the shape for untrusted code: the guest has no NIC, and every
connection is judged by policy. TAP is the escape hatch for raw networking:
L2 adjacency on the bridge, protocols the egress proxy cannot carry (UDP,
ICMP, SSH without a proxy command), and bulk throughput. Egress from a TAP
guest is whatever the bridge and its NAT allow, so enforce it there.

```nickel
config = {
  image = "ghcr.io/acme/agent-base:2026-10",
  command = ["/usr/local/bin/harness"],
  network = { mode = "tap", bridge = "br0" },
},
```

**TAP devices.** For a TAP sandbox the node creates a persistent TAP for each
instance, enslaves it to `bridge`, brings it up, and deletes it when the
allocation is destroyed. The device is named `lgf` plus 12 hex characters of
the instance id's SHA-256 (the allocation id for a task without instances),
which fits the kernel's 15-character limit. It is the same name a memory fork
of that instance gets, so one instance always asks for one device:

- A resume recreates the device under the name its checkpoint recorded, on
  the same bridge (see [Networking](suspend-and-resume.md#networking)).
- A fork gets its own `lgf` device on the bridge (see
  [Networking for forks](suspend-and-resume.md#networking-for-forks)).
- The guest's MAC is a locally administered address derived from the
  allocation id (`02:4c:...`), so guests on one bridge never collide.

Any number of TAP sandboxes fit on a node, and each one can take memory
checkpoints. Creating TAPs needs `CAP_NET_ADMIN` and the `ip` tool on the
worker. A device of that name that already exists and is free (left by a
worker crash) is reused and re-attached to `bridge`. One that another process
holds fails the boot by name: `sandbox needs TAP device lgf...: it exists and
another process holds it, so this instance already runs a guest on this
node`.

`config.network.tap` and `config.network.mac` no longer exist. Submit names
the replacement:

```text
Error: jobspec failed validation: task box config invalid: config.network.tap is gone:
Legion creates a TAP per instance on network.bridge and leases the guest an address from
the bridge's pool
Hint: Set bridge = "<the bridge tap0 was on>", and declare sandbox.bridges.<bridge> in the
worker config (docs/task-runtime-sandbox.md, "Addressing")
```

### Addressing

Every TAP guest gets its IPv4 address from a pool the worker config declares
for its bridge. Legion applies it at boot and leases a fresh one on every
restore and fork, so the address an image baked in, an address the captured
guest held, and the origin's address in a fork are all replaced. No image
needs network configuration of its own.

```nickel
# worker config
sandbox = {
  bridges = {
    br0 = {
      subnet = "10.88.0.0/16",   # the bridge's network: the guest's netmask
      gateway = "10.88.0.1",     # the guest's default route
      range = "10.88.128.0/17",  # what Legion leases (default: the whole subnet)
      dns = ["10.88.0.1"],       # the guest's resolv.conf (default: the gateway)
    },
  },
},
```

| Field | Default | Meaning |
| --- | --- | --- |
| `subnet` | required | The bridge's IPv4 network, `a.b.c.d/n` (`/30` or wider). The guest's netmask comes from it |
| `gateway` | none | The guest's default route. Must be inside `subnet`. Never leased |
| `range` | `subnet` | The CIDR Legion leases from. Must be inside `subnet`. The subnet's network and broadcast addresses, the gateway, and the resolvers are never leased |
| `dns` | the gateway | Resolvers written to the guest's `/etc/resolv.conf` |

The manager checks every worker type's pools when it loads the config, so a
malformed CIDR, a range outside its subnet, or a gateway outside its subnet
fails there by name (`sandbox.bridges.br0.range 10.99.0.0/24 is outside
subnet 10.88.0.0/16`).

- **At boot** the kernel command line carries the lease as kernel IP
  autoconfiguration, ahead of the task's own `kernel_args`:
  `ip=10.88.128.7::10.88.0.1:255.255.0.0:legion-sbx-42:eth0:off`. The guest
  kernel needs `CONFIG_IP_PNP=y` (Firecracker's and cloud-hypervisor's
  reference guest kernels have it). The root disk's `/etc/resolv.conf` is
  written from `dns`, replacing the image's file (or symlink).
- **After a restore or fork** the identity disk carries the new lease, and
  the guest's resume handler flushes `eth0`'s addresses, applies the new one
  and its default route through netlink, and rewrites `/etc/resolv.conf`.
  The image needs no `ip` tool. This needs the resume handler, which every
  sandbox launched for memory checkpoints runs.
- **The lease** is a file in the allocation's scratch dir (`guest-addr.json`).
  The set of leases on a node is the set of those files, so a worker restart
  rebuilds it from disk, a restarted allocation keeps its address, and the
  lease is released when the allocation is destroyed. A resume on the same
  node asks for the address its checkpoint recorded first, and gets it unless
  another guest holds it. A fork never asks for its origin's.
- **The address is visible**: `LEGION_GUEST_ADDR` in the guest's env (and in
  `/legion/resume.env` after a restore), and an `address:` line in `legion
  instance get` (`InstanceInfo.guest_address` and `guest_bridge` in JSON):

  ```text
  address: 10.88.128.7 (br0)
  ```

- **Placement.** A worker advertises the bridges it declares as the node
  attribute `sandbox.bridges` (`["br0"]`), and a TAP sandbox gets an implicit
  constraint at submit, `{ key = "sandbox.bridges", operator = "contains",
  value = "br0" }`, which `job get` shows. A TAP sandbox never places on a
  node that cannot address its guest. With no such node, placement fails
  like any unsatisfiable constraint.
- **Exhaustion fails the start by name**: `bridge br0's sandbox range
  10.88.128.0/17 is full (32766 addresses leased on node w-03)`. So does a
  node that runs the task without a pool for its bridge (`sandbox needs bridge
  br0's address pool, and this worker's config declares no
  sandbox.bridges.br0`), which only a node reporting stale attributes reaches.

Pools are node-local. When `br0` on each node is its own L2 segment behind
host NAT, the common setup, every node can use the same range. When one L2
segment spans nodes, give each node a disjoint `range`. When the bridge also
runs a DHCP server for other hosts, keep its pool outside `range`. The bridge
itself, its gateway address, and any NAT behind it are the operator's: Legion
only attaches TAPs to it and leases addresses on it.

**Ports.** A TAP guest's advertised ports are served at the address the
catalog advertises. The node datapath owns `<node address>:<host port>` and,
for each connection its ingress chain allows (every connection, without a
chain), dials `<guest address>:$LEGION_PORT_<label>`, reading the lease per
connection so a restore's new address applies at once. The guest binds
`$LEGION_PORT_<label>` on its bridge address or on every address
(`0.0.0.0`); a loopback-only bind is unreachable. See
[Ingress into a sandbox](network-handlers.md#ingress-into-a-sandbox).
`LEGION_ADDR_*` names the node's address, which is what consumers dial.

### Idle detection and the boot gate

A TAP-wired guest's traffic never crosses the node
datapath, so the idle policy's `network` signal reads the TAP's counters
instead: bytes the guest sends (`/sys/class/net/<tap>/statistics/rx_bytes`)
count as activity. See
[what each signal sees on a sandbox](suspend-and-resume.md#what-each-signal-sees-on-a-sandbox).

A TAP guest's single, operator-owned interface also makes `sandbox` the only
runtime whose [edge boot](edge-boot.md) `boot` gate is a true network
boundary. A guest started before its placement is durable boots with the TAP
device attached but administratively down. Firecracker cannot hotplug a NIC,
so the gate has to be on the link instead of on the attachment. Only
ratification brings the link up. A loopback-only guest has nothing to gate,
which is stronger still, and a gated guest's gate is its datapath listeners.
Legion changes link state only and installs no per-interface filter. If you
want extra isolation, add one yourself.

### Gated networking

Set the task's `network.mode = "gated"` (the block next
to `service` and `connect`, not `config.network`). The guest then boots with
no NIC at all. Its only way out is a vsock to the node datapath, where policy,
handlers, and identity judge every connection. The worker binds the host end
before boot. The generated init starts the guest agent (`/legion/legion
guest-agent`, see [The guest agent](#the-guest-agent)) to relay the guest's
loopback proxy and resolver ports over the vsock. See [network-handlers.md](network-handlers.md#sandboxes) for
the mechanics, what is enforced, and current status. It is unit-tested over
Unix sockets but not yet boot-tested on a VMM in CI.

A gated guest that no policy covers denies every destination
(`gated-default`, see
[Default deny](network-handlers.md#default-deny)), so open what the guest
needs in `network.egress.allow`. Every gated sandbox gets a resolver, and the
guest's proxy variables come in both cases plus `NODE_USE_ENV_PROXY=1`.
Submit refuses a gated sandbox that also sets `config.network`.

A gated sandbox serves the ports it declares in `network.ports`. The node
datapath owns each advertised port on the node and, for every connection
the ingress chain allows, dials into the guest through the VMM's vsock
socket (`CONNECT <port>`). The guest agent listens on that vsock port and
relays to the workload's `127.0.0.1:$LEGION_PORT_<label>`, so the workload
binds the port as on any runtime, on loopback or every address. Health
checks, the catalog, and consumers all use the advertised port. See
[Ingress into a sandbox](network-handlers.md#ingress-into-a-sandbox).

A gated sandbox reaches its `connect.upstreams` as a container does:
`$LEGION_UPSTREAM_<KEY>` and `<key>.svc.legion` both work in the guest. The
guest agent listens on each upstream's loopback port and VIP and relays to
the node's upstream listener over the vsock, so HTTP mode, policy, and
handlers apply as on any runtime. Submit refuses an upstream `localPort` of
1080 or 53, and an advertised port the guest would bind on 1080 or 1100,
because the agent holds those. A TAP-wired or loopback-only sandbox cannot reach its
upstreams. See [network-handlers.md](network-handlers.md#sandboxes).

A gated sandbox takes memory checkpoints like any other: suspend, resume,
snapshots, forks, and publish. Every restore moves the guest's vsock onto the
restoring allocation's own socket, so the restored guest is judged by the new
allocation's identity and current policy, and forks never share a door. On
Firecracker this needs 1.16.1 or later. A memory checkpoint closes the
guest's open connections. See
[suspend-and-resume.md](suspend-and-resume.md#gated-sandboxes).

**Upstreams and credentials.** Because a gated sandbox's upstreams run
through the node's listener,
[credential injection](network-handlers.md#credential-injection) applies to
them: the node adds the API key (from KV, or a workload-identity token) on
the way to the origin, so the key is never in the guest and never in a
memory checkpoint, fork, or published snapshot. A TAP-wired or
loopback-only sandbox cannot reach its upstreams, so it cannot use
injection. HTTPS through the gated proxy is a `CONNECT` tunnel, and no
credential is injected into it.

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
| Identity env / file delivery | ✅ / ✅ (under `/legion/identity`, refreshed with no restart) |
| Signal delivery | ❌ (needs a guest agent) |
| Memory / CPU limits, exclusive cores | ✅ with `resourceEnforcement.enabled` on Linux. The VMM runs in the allocation's worker cgroup, with `memory.max` = the [guest size](#guest-size) + the worker's `sandbox.vmmOverheadMiB` (default 64) and `memory.low` = `resources.memory`. `cpu` and `cores` govern the VMM's threads. See [resource-enforcement.md](resource-enforcement.md#sandbox-tasks). |
| Measured usage | ✅ with enforcement on: memory, CPU, and idle time in `legion node usage` and `instance list` ([resource-enforcement.md](resource-enforcement.md#measuring-usage)). |
| Artifacts | ✅ image, file, tree, layers |
| Exec / interactive PTY | ✅ / ✅ on the microVM path, through the guest agent over vsock ([Exec](#exec)). ✅ / ✅ in macOS dev mode. ❌ in Linux dev mode. |
| Volumes | ✅ `block` on microVM backends, `filesystem` on dev backends |

Sandbox exec is never host-level (`kind_is_host_level("sandbox") == false`), so
an `exec:write` rule without `host` is enough. See
[exec-and-actions.md](exec-and-actions.md).

## Trying It Locally

See [Installing Legion and Its Runtimes](installation.md#linux-the-sandbox-runtime-microvms)
for the host setup: `/dev/kvm`, cloud-hypervisor, e2fsprogs, and a guest
kernel. On AWS, the C7i, M7i, and R7i families and the 8i generation support
nested virtualization, so a microVM worker does not need a `.metal` instance.

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

## Checkpoints

A **checkpoint** captures a running allocation so it can be restored later,
in the same or a new allocation. The sandbox runtime supports two kinds:

| Backend | Checkpoint | What is captured |
| --- | --- | --- |
| `cloud-hypervisor`, `firecracker` | `memory` | Guest memory, VMM device state, the root disk, and the volumes |
| Apple `container`, Linux `dev` | `disk` | The volumes only. Restore boots the task fresh on them. |

The other runtimes (`exec`, `container`) report a `disk` checkpoint when the
task has a volume whose driver supports the volume `snapshot` op, and none
otherwise. The volume half of every checkpoint is the volume driver's
[`snapshot` op](volumes.md#snapshot-restore-and-clone).

`legion instance suspend` and `legion snapshot create --memory` take a memory
checkpoint, and `legion instance start` and `legion snapshot restore` restore
one (see [suspend and resume](suspend-and-resume.md)). This section describes what the
runtime does when the worker asks it to, and the node attributes a memory
checkpoint is matched against.

### Launching a sandbox for memory checkpoints

A microVM records the host path of every disk in its saved state and reopens
those exact paths on restore. A restored sandbox is a new allocation with a new
directory, so a sandbox that may be memory-checkpointed boots differently:

- **Stable device links.** The VMM sees its disks through symlinks under
  `<data dir>/vmdev/<key>/` (`rootfs.img`, `vol0.img`, …, `identity.img`).
  A restore repoints the links at the new allocation's files before the VMM
  opens them. The links live in the node's data dir rather than the worker's
  state dir, because the state dir's path includes the worker id, which
  differs on every node a published checkpoint can resume on.
- **An identity disk.** A 1 MiB raw disk is attached after the volumes as
  drive `identity`, read-only. Its guest device is on the kernel cmdline as
  `legion.identity=/dev/vdX`. The host rewrites it on every restore with the
  new allocation id, instance id, hostname, a fresh workload-identity token
  when the task has one, and fresh entropy. Between restores it rewrites the
  same disk in place whenever a workload-identity token is refreshed. A
  restored Firecracker VM cannot hotplug a disk, but it can repoint one it
  already has, which is why the slot is reserved at boot. A sandbox that is
  not launched for checkpoints still gets this disk (attached directly, with
  no device links) when it declares file-delivered identities.
- **Firecracker gets an API socket.** It boots from `--config-file` as usual,
  plus `--api-sock <alloc>/.sandbox/vmm.sock`. A sandbox that is not launched
  for checkpoints (and has no balloon) keeps `--no-api` and has no control
  channel at all.
- **A balloon device**, the memory pressure response's lever for guest RAM
  (see [Guest memory under pressure](#guest-memory-under-pressure)).

A sandbox launches this way when its task declares a
[`suspend` block](suspend-and-resume.md#the-suspend-block) (without
`checkpoint = "disk"`) or a
[`snapshots` block](suspend-and-resume.md#the-snapshots-block) (without
`memory = false`), and its backend is a real VMM. Such a sandbox also gets the
worker's own binary at `/legion/legion`, and its init starts
`legion guest-resume` in the background (see
[Identity after a restore](#identity-after-a-restore)). A sandbox launched without
these cannot be memory-checkpointed. The error says so and names the fix.

### Capture and restore

Capture pauses the VM, has the VMM write its state (Firecracker
`PUT /snapshot/create`, cloud-hypervisor `vm.snapshot`), copies the root disk
beside it, and then either resumes the VM or kills the paused VMM. For a live
snapshot the worker keeps the VM paused while it snapshots the instance
volume, then resumes it, so memory and disk are one instant. The guest never
runs past the captured instant. The root disk copy is a reflink on
btrfs, XFS, and APFS, and a sparse copy elsewhere. When any step fails after
the pause, the VM is resumed before the error is returned, so a failed
checkpoint leaves the sandbox running.

Restore launches a fresh VMM, Firecracker with only `--api-sock` (then
`PUT /snapshot/load`, a `PATCH /drives/identity`, and a resume),
cloud-hypervisor with `--restore source_url=file://…` (then a resume). The
restored VMM joins the allocation's cgroup like a booted one. The restored
guest keeps its memory, open files, and processes.

Each part of a memory checkpoint is recorded with a `sha256` digest and size.
Restore checks every part's size before launching the VMM.

A suspend (`legion instance suspend`) captures the same way, without copying
the instance volume: the volume stays in place on the node, which is what the
resume restores onto. The guest stays paused from the capture until the
worker has stored its memory in the node content store, and only then is the
VMM killed. If storing fails, the guest is resumed and keeps running.

### Identity after a restore

The restored guest wakes up mid-instruction as a new allocation. Its init
started `legion guest-resume` at first boot, which polls the identity disk
(dropping the guest's block cache first, so it never reads the captured
copy). When the disk names a new allocation, the handler reseeds the RNG
from the disk's fresh entropy (`RNDADDENTROPY`), steps the clock to the
host's time, sets the hostname (and the NIC's MAC when the restore assigns
one), re-addresses a TAP guest's `eth0` with its new lease over netlink
([Addressing](#addressing)), writes `/legion/identity.json`,
`/legion/resume.env` (with `LEGION_GUEST_ADDR` on a TAP guest), and one file
per env-delivered token under `/legion/identity/`, and finally runs
`suspend.onResume`. Each step is logged to the console, and a failed step
does not stop the others. VMGenID, where the VMM exposes it, reseeds the
kernel RNG on its own as well. See [suspend and
resume](suspend-and-resume.md#identity-after-a-resume).

When the disk names the same allocation with new tokens, that is a token
refresh rather than a restore: the handler rewrites the token files and
`/legion/resume.env` only. See
[workload identity on sandboxes](workload-identity.md#sandboxes).

### Where a memory checkpoint can be restored

A memory checkpoint only resumes on the same VMM at the same version, on the
same CPU, with the same guest size. Workers report these node attributes,
which you can also use in constraints:

| Attribute | Example | Meaning |
| --- | --- | --- |
| `sandbox.vmm` | `cloud-hypervisor` | The VMM `hypervisor = "auto"` resolves to on this node |
| `sandbox.vmm.version` | `51.2.0` | Its version, normalized to `MAJOR.MINOR.PATCH` (cloud-hypervisor's `v51.2` banner reports `51.2.0`) |
| `sandbox.vmm.<name>.version` | `sandbox.vmm.firecracker.version = 1.7.0` | One per installed VMM |
| `cpu.vendor` | `GenuineIntel` | CPU vendor (an implementer code such as `0x41` on ARM) |
| `cpu.model` | `6:106:6` | `family:model:stepping` on x86, `implementer:part:variant:revision` on ARM |
| `cpu.features` | `3f9a0c21d4e5b6a7` | A digest of the sorted CPU flag list |

A checkpoint records the values it was taken under, and a node must match all
of them exactly. VMM versions compare by release: a leading `v`, a missing
patch number, and a build suffix (`-dirty`, a git describe) are ignored, so
`v51.2` and `51.2.0` match while `51.2.0` and `51.3.0` do not. A version that
does not parse as `MAJOR.MINOR[.PATCH]` must match as written. Restoring into a task whose `vcpus` or `memory_mib` changed is
refused with an error naming both shapes.

A `local` checkpoint restores only on the node that took it. A published one
(`suspend.durability = "store"`, or `snapshot create --publish`) restores on
any node whose attributes match, and that node fetches the memory, VMM state,
and root disk parts from the content store first. The guest's root disk is
one of those parts, so a sandbox with no `stateful.storage` moves whole. See
[Durability](suspend-and-resume.md#durability-surviving-the-node).

The saved state names each disk by its device-link path on the node that
took the checkpoint. On cloud-hypervisor the restore rewrites its own copy
of `config.json` to name the restoring node's links, so a published
checkpoint restores on any node. Firecracker's saved state cannot be
rewritten, so the restoring node must keep its links at the same paths,
which means the same data dir (`LEGION_DATA_DIR`) on both nodes. A
Firecracker restore onto a node with a different data dir is refused before
the VMM starts, and `onRestoreFailure` decides what happens next:

```text
checkpoint snap-dfaf943bf48d859c reopens its disks under /var/lib/legion/vmdev/sbx-…, but this node keeps device links under /opt/legion/vmdev/sbx-…: Firecracker restores a guest's disks only at the paths it captured, so the capturing and restoring nodes need the same data directory (LEGION_DATA_DIR)
```

## Guest memory under pressure

To the host, a guest's RAM is anonymous memory of the VMM process, which the
kernel cannot reclaim without swap. So a sandbox that can run above its
reservation boots with a virtio-balloon device: one whose `resources.memoryMax`
is above `memory`, and every one launched for memory checkpoints.

- **Firecracker** gets `"balloon": { "amount_mib": 0, "deflate_on_oom": true }`
  in its config file, and an API socket to drive it.
- **cloud-hypervisor** gets `--balloon size=0,deflate_on_oom=on,free_page_reporting=on`.
  With free page reporting, a guest kernel built with `CONFIG_PAGE_REPORTING`
  hands freed pages back to the host as it frees them.

The balloon starts empty, so the guest sees all of its memory. When the node
runs short, the [`reclaim` pressure step](resource-enforcement.md#sandboxes-and-the-balloon)
inflates it by the bytes the guest uses above its reservation (never further),
and deflates it once the pressure clears. `deflate_on_oom` lets the guest take
memory back before its own OOM killer would act. The balloon's bounds are
recorded beside the VMM socket at boot (`balloon.json`), and again after a
memory restore, since the device rides the snapshot.

## Current Scope and Limitations

- **Exec on a real VMM is verified by unit tests only** in Legion's CI, which
  has no KVM: the agent's listener and the worker's session run against each
  other over Unix sockets. The KVM checks are listed in the sandbox-exec
  plan.
- **The only signal delivered into the guest is the stop signal**
  ([Stopping a sandbox](#stopping-a-sandbox)). There is no way to signal the
  workload for anything else, so an env-delivered identity must use
  `changeMode = "restart"`.
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
- **TAP guests are IPv4 only, and need `CONFIG_IP_PNP=y`** in the guest
  kernel for their boot address. A kernel without it ignores the `ip=`
  argument and the guest boots with an unconfigured `eth0` (the console
  shows no `IP-Config` line). A restored guest is re-addressed by the resume
  handler either way.
- **Memory checkpoints of a gated sandbox need Firecracker 1.16.1 or later**
  (or cloud-hypervisor). On an older Firecracker, a memory suspend, snapshot,
  or fork is refused before capture with the node's version and the fix.
- **A memory checkpoint closes a gated guest's open connections.** On
  Firecracker that includes a running origin's, for a live snapshot or a
  fork. Retry interrupted calls. See
  [suspend-and-resume.md](suspend-and-resume.md#gated-sandboxes).
- **Only a gated sandbox reaches its upstreams.** On a TAP-wired or
  loopback-only sandbox, `LEGION_UPSTREAM_<KEY>` names the guest's own
  loopback, where nothing listens, so every dial is refused. Use
  `network.mode = "gated"` for a sandbox with `connect.upstreams`.
- **A gated sandbox's socket path must fit a Unix socket.** The datapath
  listens on `<worker state>/allocs/<id>/.sandbox/vsock.sock_1080`, and a
  path over 107 bytes fails the start or restore with `gated sandbox socket
  path ... is N bytes, over the 107-byte Unix socket limit: shorten the
  worker's state path`.
- **Running processes keep their old environment after a restore.** The
  resume handler refreshes the hostname, RNG, clock, and the identity files,
  but a process's env is fixed at exec. Read the new token from
  `/legion/identity/<ENV>` or `/legion/resume.env`, or restart the process
  from `suspend.onResume`.
- **A worker without `legion-guest` copies itself into the guest.** That
  binary needs the image's glibc, so on a musl image a gated guest has no
  network and a restore keeps its old identity. See
  [The guest agent](#the-guest-agent).
- **A TAP device is restored by name.** Both VMMs reopen the TAP device
  name recorded in the saved state. Before a restore the worker checks that
  the task still names the same device, uses it when it exists and is free,
  and creates it when it is missing (removing it again on destroy). A busy
  device, a different one, or a failed create fails the restore with
  `restore needs TAP device <name>: ...`, never a guest without its network.
  A [fork](suspend-and-resume.md#networking-for-forks), or an instance
  created from a snapshot, is the exception: it cannot share the origin's
  TAP, so it gets its own (`lgf...`), on the origin TAP's bridge. On
  Firecracker that needs version 1.12 or later.
- **Restore holds a node-wide lock** for the few milliseconds between
  repointing device links and the VMM opening them, so concurrent restores
  that share a checkpoint are serialized.
- zstd-compressed image layers are rejected. Gzip and uncompressed layers work.
  This comes from the shared layer extraction code.
