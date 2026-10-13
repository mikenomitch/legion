# Installing Legion and Its Runtimes

Legion is one binary. The manager, the worker, and the CLI are all `legion`.
What a worker can *run* depends on what else is installed on its host: the
`exec` runtime needs nothing, but the `container` and `sandbox` runtimes drive
an OCI runtime or a virtual machine monitor that Legion does not ship.

This page covers both: installing Legion, then installing the host pieces each
runtime uses, on Linux and on macOS.

| Runtime | Linux host needs | macOS host needs |
| --- | --- | --- |
| `exec` | nothing | nothing |
| `container` | an OCI runtime: `runc` 1.2+ (default), or `crun` | Apple `container` |
| `sandbox` (microVM) | `/dev/kvm`, `cloud-hypervisor` (or `firecracker`), a guest kernel, `e2fsprogs` | Apple `container` (dev mode) |
| `sandbox` with `hypervisor = "dev"` | an OCI runtime, as for `container` | Apple `container` |

A worker that lacks a runtime's pieces still joins and runs everything else.
A task that needs them fails at start with an error naming what to install.

## Legion itself

Build from [crates.io](https://crates.io/crates/legion-rs). You need
[Rust](https://rustup.rs) and `protoc`:

```bash
# Debian / Ubuntu
sudo apt-get install -y protobuf-compiler
# macOS
brew install protobuf

cargo install legion-rs
```

Workers that run `container` or `sandbox` tasks on Linux run as root: they
create cgroups, network namespaces, and TAP devices, and they extract image
layers with their original owners (see
[docs/task-runtime-container.md](task-runtime-container.md#linux-image--bundle)).

## Linux: the `container` runtime

Linux containers run under a low-level OCI runtime. Legion builds the bundle
(root filesystem plus `config.json`) itself, so the runtime is the only
dependency. There is no Docker or containerd involved.

### runc (the default)

Every Linux container runs in its own user namespace by default
([`security.userNamespace`](task-runtime-container.md#security)), and its
task directory and volumes are idmapped mounts. That needs **runc 1.2 or
newer**. Distribution packages can be older (check `runc --version`), and
the worker refuses a user-namespaced start on an older runc, naming the fix.
[runc](https://github.com/opencontainers/runc) publishes static release
binaries. Pick the current version from its
[releases page](https://github.com/opencontainers/runc/releases):

```bash
VERSION=1.5.2          # see the releases page for the latest; 1.2.0 at least
ARCH=$(dpkg --print-architecture 2>/dev/null || uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
curl -fsSL -o /tmp/runc \
  "https://github.com/opencontainers/runc/releases/download/v${VERSION}/runc.${ARCH}"
sudo install -m 0755 /tmp/runc /usr/local/sbin/runc
runc --version
```

The worker finds `runc` on its `PATH`. If you run the worker under systemd,
check that `/usr/local/sbin` is on the unit's `PATH`, or install the binary to
`/usr/sbin`.

### crun instead

Legion supports one other OCI runtime, [crun](https://github.com/containers/crun).
Point the worker at it with `LEGION_OCI_RUNTIME`, in the worker's environment
(for example in its systemd unit):

```bash
LEGION_OCI_RUNTIME=crun legion join <token>
```

crun implements idmapped mounts, so it runs the defaults. Legion's privilege
floor has been verified on runc only, so check a crun node with a test job
before you rely on it.

The worker refuses any other runtime. It identifies the runtime by the first
word of its `--version` output, so a renamed runc binary still works, but
another OCI runtime fails every container start with:

```text
OCI runtime `/opt/bin/my-runtime` is not runc or crun (its `--version` reports `my-runtime version 2.0`); Legion runs containers with runc 1.2 or newer (the default) or crun. Install one, and point LEGION_OCI_RUNTIME at it or unset LEGION_OCI_RUNTIME to use runc from PATH
```

The sandbox runtime's Linux dev mode (`hypervisor = "dev"`) uses the same
runtime, the same `LEGION_OCI_RUNTIME`, and the same check.

### Checking it

A node whose runtime is an installed runc or crun publishes a
`container: pass` readiness check, visible in `legion node info <node>`.
Without one, the key is absent and container tasks on that node fail at start
with:

```text
OCI runtime `runc` is not installed on this node (install runc 1.2+ or crun, or set LEGION_OCI_RUNTIME to one that is)
```

`container: pass` does not check runc's version. A runc older than 1.2 still
passes, and its user-namespaced starts fail with:

```text
OCI runtime `runc` is runc 1.1.12-0ubuntu3, which predates idmapped mounts; security.userNamespace (on by default) needs them for the task directory and volumes. Install runc 1.2 or newer (the default) or crun, and point LEGION_OCI_RUNTIME at it, or set security.userNamespace = false on the task
```

The worker's data directory must be traversable by other users all the way
down (`/var/lib/legion` is, at the default `0755`). A user-namespaced
container's root is an unprivileged host id, and the runtime reaches the
container's root filesystem by path as that id. The worker sets the modes of
the directories it creates itself. A data directory under a `0700` parent
such as `/root` fails container starts with an error naming that parent.

Container images are pulled by the worker itself, over HTTPS, from the
registry in the image reference. Workers need outbound access to the
registries your jobs use, and they cache layers locally (see
[Image cache](task-runtime-container.md#image-cache)).

## Linux: the `sandbox` runtime (microVMs)

A sandbox boots each task in its own microVM. That needs three things on the
worker: hardware virtualization through `/dev/kvm`, a virtual machine
monitor, and an uncompressed guest kernel. Root disks are built with
`mkfs.ext4`, so the worker also needs `e2fsprogs`.

### 1. Check for KVM

```bash
ls -l /dev/kvm                          # must exist
grep -cE 'vmx|svm' /proc/cpuinfo        # > 0: the CPU exposes virtualization
lsmod | grep kvm                        # kvm plus kvm_intel or kvm_amd
```

If `/dev/kvm` is missing on a machine whose CPU supports virtualization, load
the module (`sudo modprobe kvm_intel`, or `kvm_amd`). If the CPU flags are
missing, the host does not expose virtualization to you. That is usual on
cloud VMs, which need either a bare-metal instance type or nested
virtualization.

**AWS.** EC2 offers
[nested virtualization](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html)
on the C7i, M7i, and R7i families and the 8i generation, so microVMs run on
those regular (non-metal) instances once it is enabled for the instance. The
AWS guide covers how to turn it on. Other families need a `.metal` instance
type for `/dev/kvm`.

**Without KVM** (CI runners, VMs without nested virtualization), a sandbox can
still run with `hypervisor = "dev"`, which runs the image through the OCI
runtime above. That isolates with namespaces only, so it is for development,
not untrusted code (see
[docs/task-runtime-sandbox.md](task-runtime-sandbox.md#linux-dev-mode)).

### 2. Install cloud-hypervisor

`hypervisor = "auto"` (the default) prefers
[cloud-hypervisor](https://github.com/cloud-hypervisor/cloud-hypervisor) when
it is installed. It publishes a static binary with each release:

```bash
CH_VERSION=v48.0       # see https://github.com/cloud-hypervisor/cloud-hypervisor/releases
# x86_64:
curl -fsSL -o /tmp/cloud-hypervisor \
  "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CH_VERSION}/cloud-hypervisor-static"
# aarch64: the asset is cloud-hypervisor-static-aarch64
sudo install -m 0755 /tmp/cloud-hypervisor /usr/local/bin/cloud-hypervisor
cloud-hypervisor --version
```

The worker finds `cloud-hypervisor` on its `PATH`, or at
`LEGION_CLOUD_HYPERVISOR_BIN`. Firecracker works too (`firecracker` on the
`PATH`, or `LEGION_FIRECRACKER_BIN`); `auto` falls back to it when
cloud-hypervisor is absent.

Install the filesystem tools the root-disk build uses:

```bash
sudo apt-get install -y e2fsprogs       # mkfs.ext4; or set LEGION_MKFS_EXT4
```

### 3. Install a guest kernel

A microVM boots an uncompressed kernel image (`vmlinux`, not `bzImage`).
cloud-hypervisor maintains a guest kernel and publishes builds on its
[linux releases page](https://github.com/cloud-hypervisor/linux/releases).
Each release carries a `vmlinux-x86_64` asset (and an arm64 image). Put it
where the worker looks by default:

```bash
KERNEL_TAG=ch-release-v6.12.8-20250114  # see the releases page for the latest tag
sudo mkdir -p /var/lib/legion/kernels
sudo curl -fsSL -o /var/lib/legion/kernels/vmlinux \
  "https://github.com/cloud-hypervisor/linux/releases/download/${KERNEL_TAG}/vmlinux-x86_64"
```

Two other ways to supply it, in the order the worker checks them:

- per job, with the task's `config.kernel` (an artifact works:
  `kernel = "${artifact.kernel}"`);
- per worker, with `LEGION_SANDBOX_KERNEL=/path/to/vmlinux`.

`/var/lib/legion/kernels/vmlinux` is the fallback. A sandbox start with none of
them fails with an error that names all three. Memory reclaim from a running
guest (free page reporting) needs a kernel built with `CONFIG_PAGE_REPORTING`;
see [docs/task-runtime-sandbox.md](task-runtime-sandbox.md#guest-memory-under-pressure).

### 4. Check it

When the worker starts, it logs which backend sandboxes will use:

```text
Sandbox task runtime ready backend=cloud-hypervisor microvm=true
```

or a warning naming what is missing. Then run the example job:

```bash
legion job run configs/jobs/sandbox-microvm.ncl
legion instance create sandbox-vm --id try-1
legion job logs sandbox-vm          # the guest's serial console
legion instance destroy try-1
```

## macOS

Both image runtimes use Apple's
[`container`](https://github.com/apple/container), which runs each container
in its own lightweight VM. It needs Apple silicon.

```bash
brew install container
container system start          # or: brew services start container
container system status
```

- `container` tasks run on it directly.
- `sandbox` tasks run on it in dev mode: still a VM, but Apple owns the kernel
  and disks, so `kernel` and the microVM-specific fields are ignored.
- Apple `container` will not boot a VM with less than **200 MiB** of memory.
  Legion rejects such a task at submit when its constraints pin it to macOS
  (`constraints = { os = "macos" }`), and otherwise fails the start on a macOS
  worker with a message naming the floor. Size macOS tasks at 200 MiB or more.

If the service is not running, container tasks fail at start with
`the Apple container service is not running on this node (start it with
container system start)`.

## See also

- [docs/task-runtime-container.md](task-runtime-container.md): how images
  become bundles, capabilities, limits, and the image cache.
- [docs/task-runtime-sandbox.md](task-runtime-sandbox.md): backends, guest
  sizing, and the microVM boot.
- [docs/data-directory.md](data-directory.md): where a worker keeps its state.
