# Task Runtime: container

`container` is a built-in task runtime implemented in `src/task_plugins/container.rs`.

## Purpose

Run containerized workloads through the host's container runtime binaries.

## Runtime Selection

Legion picks the runtime by operating system. There is no Docker dependency.

- **macOS: Apple `container`.** A high-level runtime that pulls and runs images
  by reference, with a docker-compatible CLI. Install it with
  `brew install container` and start its service with `container system start`
  (or run it as a login service with `brew services start container`). Each
  container gets its own lightweight Linux VM. Legion launches it with
  `container run --rm --name legion-<alloc> --progress none [--memory …]
  [--cpus …] [--publish …] [--env K=V …] <image> <command…>`.
- **Linux: a low-level OCI runtime.** Legion uses `youki` by default. Any
  OCI-spec runtime (`runc`, `crun`) works through the `LEGION_OCI_RUNTIME` env
  override, since they share the bundle interface. These runtimes can't pull
  images or read image references. They run a *bundle* (an extracted `rootfs/`
  plus a `config.json` runtime spec), which Legion builds from the image's OCI
  layout (see [Linux: image → bundle](#linux-image--bundle)).
- **Other operating systems:** the runtime is rejected as unsupported.

Before every launch, the runtime checks its OCI backend. If the binary is
missing, or the `container` service isn't running on macOS, the allocation
fails with a message that says how to fix it. Nodes publish a `container: pass`
readiness check when the runtime is usable. The key is omitted when it isn't,
since a node without a container runtime is still healthy for every other
runtime.

### Linux: image → bundle

A low-level OCI runtime can't fetch an image, so Legion fetches and converts it
(`src/task_plugins/oci_bundle.rs`):

1. **Get the image's OCI layout.** If the image was staged through the
   artifact pipeline (`artifacts.containerImageDesugar`), Legion uses that
   layout. Otherwise it pulls the image anonymously through the shared `oci://`
   fetcher. A layout pulled by an earlier start of the same allocation is
   reused, so in-place restarts don't refetch from the registry.
2. **Build a bundle.** Extract each image layer into `rootfs/`, honoring
   overlayfs whiteouts. Gzip and uncompressed layers work. Zstd layers are
   rejected with an explicit error. Then generate an OCI runtime `config.json`
   with:
   - the task `command`/`env` merged over the image config's env (the task
     wins, and `PATH` gets a default if it's missing),
   - the image's working dir and user. Named users and groups resolve against
     the extracted rootfs's `/etc/passwd` and `/etc/group`, and `HOME` comes
     from the passwd entry when the env doesn't set it. Names that can't be
     resolved fall back to root with a warning.
   - the allocation's network namespace when it runs gated (the default; see
     [Networking](#networking-gated-by-default-on-linux)), with a generated
     `resolv.conf` pointing at its resolver. With `network.mode = "host"`, the
     host's network, with the host's `/etc/resolv.conf` and `/etc/hosts`
     bind-mounted read-only so DNS works (or a generated `resolv.conf` when
     the allocation has its own resolver),
   - a writable root filesystem.
3. **Run it:** `<runtime> run --bundle <dir> legion-<alloc>`.

The image and bundle scratch space lives under the allocation's private
directory and is removed on destroy.

## Config Schema

Required:

- `image`: image reference string.
- `command`: non-empty string array.

Optional:

- `env`: map of string env vars.
- `files`: files to write into the task directory before the container
  starts (`[{ path, mode, content }]`), readable inside at
  `/legion/task/<path>`. This is the shape `kvInjections.files` renders into.
  See [The task directory](#the-task-directory-legiontask).

The task-level `stop` block also applies here. It sits next to `resources`,
not inside the runtime config. See [Graceful stop](#graceful-stop).

Put the command directly after the image reference, flags included. Don't add
a `--` separator. Apple's argument parser reads `--` as the target executable,
and the container fails to start with "failed to find target executable --".

## Resources and Ports

The `container` runtime isolates its workload, unlike `exec`. So the task's
declared `resources` and assigned ports are applied to the runtime, instead of
only being used for scheduling. Both travel in the allocation payload
(`TaskAllocation.task_resources` / `.network`).

- **`resources.memory` (MiB) → `--memory`, `resources.cores` → `--cpus`
  (macOS).** Apple `container` boots a VM per container. A guest with no size
  set gets the runtime's own default (1 GiB / 4 CPUs), no matter what the
  scheduler reserved on the node. Only *declared* values are applied.
  Scheduler defaults aren't folded in, so a job that declares nothing keeps the
  runtime's defaults. The guest reports one more CPU than requested (its own
  housekeeping core) and rounds memory up.
  On macOS, `resources.cpu` (MHz) has no runtime equivalent and is used only
  for scheduling. `cores` sets the guest size rather than reserving exclusive
  cores, because Apple `container` has no CPU weight or pinning to map them
  onto.
- **Assigned ports → `--publish <host>:<container>` (macOS).** The container
  has its own IP inside a VM, so a host port only reaches it when published.
  That applies to service consumers and to the worker's own health checks. A
  port that declares `to` publishes to that container-side port. A port
  without `to` publishes host port to the same host port, which matches the
  `LEGION_PORT_<NAME>` value the task receives.
- **Linux: declared resources become kernel limits** on a worker running with
  `resourceEnforcement.enabled`. You get hard memory limits, exclusive pinned
  cores, and proportional CPU weight, applied through the generated OCI spec's
  `linux.resources` and `cgroupsPath`. The container runtime is a
  `RuntimeApplied` consumer of the node-wide enforcement subsystem, so a core
  it reserves is also off-limits to `exec` tasks on the same node. See
  [docs/resource-enforcement.md](resource-enforcement.md) for the model,
  requirements, and failure modes.

  Two validation rules apply on both Linux and macOS: `cores` must be a
  **whole number** (a cpuset can't hold a fraction of a CPU), and a task
  declares `cores` **or** `cpu`, never both.
- **Linux: ports arrive through the datapath.** In the default `gated` mode
  the datapath owns each assigned host port and forwards it into the
  container's namespace, to `to` when the port declares it and to the same
  port number otherwise, so `{ name = "pg", to = 5432 }` reaches a server
  listening on 5432. In `host` mode, assigned ports reach the container
  directly and `to` is not applied
  ([Network Handlers](network-handlers.md#gated-mode)).

## Runtime Behavior

- `prepare`: logging only.
- `start`:
  - parse `ContainerTaskConfig`, then detect and check the runtime,
  - remove any container still holding this allocation's name (a worker crash
    or a killed `--rm` run can leave one behind, and `run --name` fails if the
    name is taken),
  - macOS: build the Apple `container run` args. Linux: get the OCI layout,
    build a bundle, and `run --bundle`.
  - spawn the child process. Stdout and stderr are captured per allocation
    when the worker sets a log dir. The runtime's own fetch progress is
    suppressed so it doesn't get mixed into the task's stderr.
  - track the child and its PID.
- `stop`: tear the container down by name **first**, gracefully (see
  [Graceful stop](#graceful-stop)). Killing the foreground `run` process alone
  doesn't stop the container. Apple `container` is daemon-backed, and a Linux
  OCI runtime reparents the container's init process when its supervisor is
  killed. After that, the child handle and PID are reaped.
- `destroy` delegates to `stop`.

## Graceful stop

Stopping a container sends a signal, waits, and only then kills it. You can
set both the signal and the wait per task, next to `resources`:

```nickel
task = {
  name = "db",
  runtime = "container",
  config = { image = "postgres:16", command = ["postgres"] },
  stop = { signal = "SIGINT", killAfter = "90s" },   # defaults: SIGTERM, 30s
}
```

The block's options, defaults, and 5-minute cap are covered in
[Graceful stop](graceful-stop.md#the-ladder). Two things are specific to this
runtime: where the signal goes, and what happens when the jobspec doesn't set
one.

- **The signal goes to the container's init process**, not to the CLI that
  launched it. On both platforms the container keeps running when the
  foreground process gets a signal, so signaling the tracked host process
  would reach nothing.
- **On Linux, the default chain matches Docker:** the jobspec's `stop.signal`,
  then the image's own `STOPSIGNAL`, then `SIGTERM`. So leaving out the `stop`
  block isn't the same as declaring `SIGTERM`. Only the first case checks the
  image. If the image declares a signal Legion doesn't support, Legion ignores
  it instead of failing the stop. A container that can't be stopped is worse
  than one that gets a `SIGTERM` the image didn't ask for.
- **Apple `container` applies the image's stop signal itself**, so on macOS
  the chain is the jobspec's signal or `SIGTERM`. The runtime handles the whole
  stop sequence. It may send SIGKILL on its own schedule if `killAfter` is
  longer than its `--time` flag accepts.

How each platform does it:

- **Linux:** `<runtime> kill legion-<alloc> <signal>` ("not running" is
  ignored, since a finished batch container is already gone). Then Legion
  polls `<runtime> state` until the container is stopped or gone, up to the
  grace period. Finally it always runs `<runtime> delete --force`, which
  cleans up a stopped container and kills a hung one.
- **macOS:** `container stop --signal <signal> --time <secs> legion-<alloc>`,
  which runs the same sequence inside the daemon.

A container that exits within the grace period is never force-killed. One
that ignores the signal is killed at the deadline. Running `stop` on a
container that already exited doesn't return an error.

## Signals

The runtime sends signals to the container's **init process** through the
runtime CLI (`container kill --signal` on macOS, `<runtime> kill` on Linux).
It doesn't signal the tracked PID. That PID is the runtime supervisor, and
killing it would leave the workload running without ever getting the signal.

This lets a container task declare an identity with `changeMode = "signal"`.
When the token rotates, the worker rewrites the token file inside the mounted
task directory and signals the container. The workload then re-reads the file
without a restart. See [docs/workload-identity.md](workload-identity.md).

## The task directory (`/legion/task`)

Every container task gets the allocation's host task directory bind-mounted
read-write at **`/legion/task`**. Everything the worker stages for the task is
readable there: file and tree artifacts, KV-rendered files, and identity
tokens.

Legion mounts the whole directory, never individual files. The worker
replaces rendered files atomically by renaming them. A rename shows up through
a directory bind mount, but a per-file bind mount silently keeps the old file
(a well-known Docker pitfall, where the container holds on to the stale
inode). Because of this, a token rotation, or a KV file declared with
`onChange = "update"` (see
[Live file updates](kv-store.md#live-file-updates)), reaches a running
container without a restart or any extra projection machinery.

The mount is added *before* declared `volumes[]`, so a job can intentionally
shadow it with a volume at the same destination.

**Env vars are rewritten to the in-container paths**, following the same
convention volumes already use:

| Variable | Value inside the container |
| --- | --- |
| `LEGION_TASK_DIR` | `/legion/task` |
| `LEGION_ARTIFACT_<NAME>` | the staged path, rebased onto `/legion/task` |
| `LEGION_VOLUME_<NAME>`, `LEGION_STORAGE_DIR` | the declared mount destination |
| `LEGION_ALLOC_DIR` | **not set**. The alloc dir isn't mounted, so there is no in-container path to point it at |

`${artifact.<name>}` interpolation in the runtime config uses the same
rewritten paths.

**File destinations must resolve under `/legion/task`.** Nothing else on the
host is reachable from inside the container. A destination outside the mount
is rejected at submit, instead of being staged somewhere the workload can never
read. Relative paths resolve against the mount:

```nickel
kvInjections = { files = [
  { path = "config/app.toml", file = { template = "..." } },  # -> /legion/task/config/app.toml
] },
identities = [{
  name = "api",
  file = { path = "/legion/task/identity/token" },           # absolute form, in range
  changeMode = "signal", changeSignal = "SIGHUP",
}],
```

A path outside the mount fails at submit, and the message tells you how to fix
it:

```
task api identity "api": file.path "/var/run/legion/token" is outside the
container task directory; use a path under /legion/task (or a relative path),
or switch to env delivery
```

This is the one place a jobspec can't be copied unchanged between the `exec`
and `container` runtimes. The exec runtime runs on the host's filesystem, so
its host-absolute destinations stay valid and aren't rewritten.

Only the allocation's own private task directory is exposed. The log directory
isn't mounted, because captured stdout and stderr can contain secrets and stay
owner-restricted on the host. `noNewPrivileges`, masked paths, and the
capability set are unchanged.

## Artifacts Support

`container` reports `supports_artifacts()`, but it only consumes images
directly (see [docs/artifacts.md](artifacts.md)):

- **Image desugaring (behind a cluster flag):** when the manager config sets
  `artifacts.containerImageDesugar = true`, the `image` field is converted at
  admission into an implicit `oci://` artifact named `image`. Docker Hub
  shorthand expands, so `alpine:latest` becomes
  `oci://docker.io/library/alpine:latest`. The worker pulls it through the
  shared artifact pipeline (cluster auth matchers, digest pinning, status
  rows), and the plugin's `consume_artifact` imports the staged OCI layout
  into the runtime.
  - macOS (Apple `container`): Legion tars the layout and imports it with
    `container image load`. The run then uses the artifact's **pinned digest
    reference** instead of the jobspec's tag. That guarantees the container
    runs the exact image the pipeline resolved, with no registry pull. (An
    allocation restored after a worker restart has no staged record and falls
    back to the jobspec's reference.)
  - Linux (OCI runtime): the layout is converted to a runtime bundle (see
    [Linux: image → bundle](#linux-image--bundle)).
  - Container images are Linux guests on every host, so the `oci://` fetcher
    resolves multi-platform indexes against `linux/<arch>` instead of the
    host platform. On macOS the host platform (`darwin/arm64`) wouldn't match
    anything in an image index.
  - The flag defaults to **off**. Legion has no artifact cache, so with the
    flag on, every allocation would re-download its image, a step backward
    for runtimes that cache images themselves. With it off,
    macOS lets Apple `container` pull the image itself. Linux falls back to an
    anonymous pull through the shared `oci://` fetcher, so the OCI runtime
    (which can't pull) still gets its rootfs. Private-registry auth and digest
    pinning require the artifact pipeline (flag on).
- **File and tree artifacts work.** They're staged into the host task
  directory, which is mounted at `/legion/task`. Both `LEGION_ARTIFACT_<NAME>`
  and `${artifact.<name>}` hold the in-container path. See
  [The task directory](#the-task-directory-legiontask).

## Networking: gated by default on Linux

On a Linux node a `container` task runs **gated** unless it says otherwise:
`network.mode = "auto"`, the default, resolves to `gated` at placement. The
worker creates an empty network namespace for the allocation before it
starts, and the container joins it by path through an OCI `network`
namespace entry. The namespace holds loopback and the datapath's listeners,
nothing else:

| Inside the namespace | What it is |
| --- | --- |
| `127.0.0.1:1080` | the egress proxy, exported as `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` (and lower case) and `LEGION_EGRESS_PROXY` |
| `127.0.0.53:53` | the allocation's resolver, in `/etc/resolv.conf` and `LEGION_DNS`. A task with upstreams gets its VIP block's `.1` instead |
| `LEGION_UPSTREAM_<KEY>`, `<key>.svc.legion` | upstreams and their VIPs |
| each declared port | the port the task listens on: `to` when the port sets it, else the host port. The datapath owns the host port on the node and forwards to it |

**Egress is denied unless something allows it.** A gated container that no
task block and no policy object covers is refused every destination with the
`gated-default` source. Opening a host is one line:

```nickel
network = { egress = { allow = ["api.stripe.com:443"] } },
```

Observing everything and blocking nothing is also one line, and the
[host guard](network-handlers.md#the-host-guard) still applies under it:

```nickel
network = { egress = { policy = "allow", accessLog = true } },
```

`legion job validate`, `legion job run`, and `legion job plan` warn when a
container on a Linux node would run gated with nothing allowed:

```text
warning: task "web" is gated (the default for containers on Linux) and allows no
egress. Outbound connections will be refused with `gated-default`.
hint: add hosts to network.egress.allow, or set network.mode = "host"
```

A tool that ignores the proxy variables has no route out, so its only way
off the namespace is a connection the datapath judges. Every destination the
proxy dials is checked against the host guard first: the node itself
(loopback and its own addresses), link-local (the cloud metadata service),
and other allocations' listeners are refused whatever the job's policy says,
and a hostname is judged by the address it resolves to.

**Inbound.** Every declared port gets an ingress listener on its host port,
with or without an ingress chain, so the service is reachable from other
nodes, from service consumers, and from its own health checks. Without a
chain the listener allows every peer. The task sees the datapath as its peer,
not the original client.

**What gated costs a container.** No raw UDP (DNS goes through the resolver),
no ICMP, IPv4 only inside the namespace, and no route for tools that ignore
the proxy variables. That includes Java's default HTTP client and most
database drivers. Reach a database outside the cluster through an external
upstream (`connect.upstreams` with `to = "tcp://db.example.com:5432"`), which
needs no proxy support. Each start also pays for the namespace and its
listeners, a few milliseconds (see [Testing](#testing) for the numbers).

**Opting out.** `network.mode = "host"` shares the host's network, as every
container did before gated became the default. Declared ports are host
ports, and egress policy applies only to traffic through upstreams and the
resolver. A host-mode container can reach the node, other allocations'
listeners, and the cloud metadata service, so keep it for trusted images.

On macOS, `auto` stays `host`: Apple `container` already runs each container
in its own VM on its own network, and macOS has no network namespaces. The
full model, the fail-closed rules, and the limits are in
[Network Handlers](network-handlers.md#gated-mode).

## Security

On Linux, every container gets a privilege floor on top of its namespaces
and its three capabilities (`CAP_AUDIT_WRITE`, `CAP_KILL`,
`CAP_NET_BIND_SERVICE`, with `noNewPrivileges`). It is on by default:

```nickel
config = {
  image = "postgres:17",
  command = ["postgres"],
  security = {
    userNamespace = true,   # false runs container root as host root
    seccomp = "default",    # "unconfined" removes the filter
  },
},
```

- **A user namespace per allocation.** Container ids `0-65535` map to a
  65,536-id range on the host that belongs to this allocation alone, so
  container root is an unprivileged host id no other allocation or host user
  shares. The worker owns the pool (host ids from `1048576`, 4,096 ranges
  per node; don't hand those ids to anything else) and records each
  allocation's range in its private directory, so an in-place restart keeps
  it. Image layers are extracted with their owners already shifted into the
  range. The task directory and every volume are **idmapped mounts**: on
  disk they keep host ownership, inside they belong to container root, so a
  volume reattached to an allocation with a different range keeps working,
  and files the worker rewrites later (KV files, templates) stay readable.
  Inside a user namespace `/sys` is a read-only bind of the host's, not a
  fresh `sysfs`, because a gated or host network namespace does not belong
  to the container's user namespace.
- **A default seccomp profile.** It refuses, with `EPERM`, the syscalls
  Docker's default profile refuses: `mount` and the new mount API,
  `unshare`, `setns`, and `clone` with a namespace flag, kernel modules and
  `kexec`, `bpf`, `perf_event_open`, `userfaultfd`, the kernel keyring,
  clock and time setting, `reboot`, swap, `acct`, `quotactl`, `pivot_root`,
  cross-process memory access, handle-based opens, NUMA policy, and legacy
  syscalls. `clone3` answers `ENOSYS`, so libc falls back to `clone`, whose
  flags the filter can see. Docker's profile is an allowlist; Legion's is a
  denylist over the same calls, so a syscall a future kernel adds is allowed
  until the list names it.
- **A cgroup namespace**, so the container sees its own cgroup as the root
  and not the host's tree.

**Opting out** of either is per task. `userNamespace = false` runs container
root as host root, and `seccomp = "unconfined"` removes the filter. Each
makes the task host-level, and `job validate`, `job run`, and `job plan`
say so:

```text
warning: task "db" sets security.userNamespace = false: on Linux nodes its root is host
root, so a container escape owns the node. The task is host-level (it needs the host
grant on job:write)
```

**Requirements and failure modes.**

- Idmapped mounts need Linux 5.12 or later and a filesystem that supports
  them (ext4, xfs, btrfs; tmpfs from 6.3). Without them, a start with a task
  directory or volume fails with the runtime's error naming the mount; set
  `userNamespace = false` on that node's jobs, or move them to a newer
  kernel.
- An image whose files are owned by ids above `65535` cannot be extracted
  into a range; the start fails naming the file.
- More than 4,096 containers with a user namespace on one node at once fail
  to start with `no free user namespace range on this node`.
- macOS needs none of this: Apple `container` runs each container in its own
  VM with its own kernel, and `security` is ignored there.
- Verified with `runc` 1.5 (`oci_runtime_security_*` tests below). `youki`
  and `crun` take the same OCI fields, but the floor has not been run on them
  in this repository.

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ✅ (destinations must resolve under `/legion/task`) |
| Identity env / file delivery | ✅ / ✅ (file destinations under `/legion/task`) |
| Signal delivery | ✅ to the container init, both platforms |
| Graceful stop (`stop { signal, killAfter }`) | ✅ both platforms |
| Memory / CPU limits | ✅ macOS (guest sizing). ✅ Linux with `resourceEnforcement.enabled` (cgroup v2). See [docs/resource-enforcement.md](resource-enforcement.md) |
| Exclusive cores (`resources.cores`) | ✅ Linux (cpuset pinning). macOS sizes the guest instead |
| Port publishing | ✅ macOS (`--publish`). Linux: ingress listeners forward each host port into the gated namespace (`to`, or the same port); not applied in `host` mode |
| Volumes (`volumes[]`, `stateful.storage`) | ✅ bind-mounted at the declared `path`. See [docs/volumes.md](volumes.md) |
| Artifacts | ✅ image (staged via `containerImageDesugar`, or pulled by the runtime) and file/tree (readable under `/legion/task`) |
| Exec / interactive PTY | ✅ / ✅ on both Apple `container` and the Linux OCI runtimes (sandboxed). See [docs/exec-and-actions.md](exec-and-actions.md) |

Container exec runs inside the container's namespaces, so it is sandboxed
(`host = false`). An `exec:write` rule without `host` is enough.

## Examples

- [`configs/jobs/mac-container.ncl`](../configs/jobs/mac-container.ncl):
  batch task with env and declared resources that size the guest.
- [`configs/jobs/mac-container-service.ncl`](../configs/jobs/mac-container-service.ncl):
  long-running service with dynamic port publishing, health checks, service
  discovery, exec, and declared actions.

Both are written for Apple `container` and constrained to macOS. The service
example also relies on publishing a dynamic host port to nginx's port 80
inside the container, which Linux doesn't do. Apart from the `os` constraint,
the batch jobspec is portable.

## Testing

Unit tests cover launch argv construction (no `--` separator, progress
suppression, resource mapping, port publishing, pinned-reference launch), the
exec and signal argv formats per runtime, the stop-signal default chain, the
generated `linux.resources`/`cgroupsPath`, the `/legion/task` mount ordering,
env rewriting, and the runtime preflight check. End-to-end tests are marked
`#[ignore]` because they need a runtime and network access. The limit tests
also need cgroup v2 and root.

```bash
# macOS, with Apple `container` installed and started
cargo test --lib apple_container -- --ignored --nocapture

# Linux, with an OCI runtime installed
LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime -- --ignored --nocapture
```

**What gated costs at start.** Two ignored benchmarks measure it on a Linux
node, as root:

```bash
# the namespace and its listeners alone, 200 runs
cargo test --lib gated_start_overhead -- --ignored --nocapture
# a whole container start, host vs gated, 20 runs each
LEGION_OCI_RUNTIME=runc cargo test --lib container_start_latency -- --ignored --nocapture
# the privilege floor: container root's host id, seccomp refusing a new user
# namespace, the cgroup namespace, and a volume across two id ranges
LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime_security -- --ignored --nocapture
```

On a 4-core Linux 6.18 VM with `runc` and an unoptimized build, the
namespace and listeners added p50 1.0 ms and p99 3.9 ms (teardown p50
0.1 ms). A whole `alpine` container start that runs `/bin/true` took p50
354 ms and p99 414 ms on host networking, and p50 360 ms and p99 506 ms
gated. With the [privilege floor](#security) on as well (user namespace,
seccomp, cgroup namespace), a rerun measured p50 396 ms and p99 497 ms on
host networking and p50 376 ms and p99 482 ms gated: at 20 runs, the floor's
cost is inside the run-to-run noise.

## Current Scope and Limitations

- The runtime integration is simple: it shells out to runtime binaries.
- The root filesystem is writable. The only host paths mounted in are the
  allocation's own task directory at `/legion/task` and any volumes the job
  declares ([docs/volumes.md](volumes.md)). The alloc directory and the log
  directory aren't mounted.
- File-delivery destinations must resolve under `/legion/task`. Per-file bind
  mounts and K8s-style projected volumes were rejected as designs because they
  break atomic refresh. They aren't planned.
- Linux containers are gated by default, with the costs listed under
  [Networking](#networking-gated-by-default-on-linux): no raw UDP or ICMP,
  IPv4 only, and no route for tools that ignore the proxy variables.
  `network.mode = "host"` opts out.
- Linux resource enforcement requires the cgroup v2 unified hierarchy, a
  worker that can create the `legion/` subtree at the cgroup root, and
  `resourceEnforcement.enabled` in the worker config. Without it, `resources`
  are used only for scheduling on Linux.
- On macOS, `resources.cpu` (MHz) is used only for scheduling, and `cores`
  sets the guest size instead of reserving exclusive cores. Apple `container`
  has no CPU weight or pinning equivalent.
- There is no `legion alloc signal` command. Signals are sent only by declared
  policy (`changeSignal` on refresh, `stop.signal` on teardown), not on demand.
- Private-registry image auth and digest pinning require the artifact pipeline
  (`containerImageDesugar`). The runtime's own fallback pull is anonymous.
- Zstd-compressed image layers are rejected. Gzip and uncompressed layers are
  supported.
- There is no artifact cache, so a Linux pull outside the pipeline fetches
  once per allocation. In-place restarts reuse the pulled layout, and moves
  refetch.
- Health checks still run through the worker health monitor.
