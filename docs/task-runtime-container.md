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
   reused, so in-place restarts don't refetch from the registry. Either way,
   layers the node already holds come from the
   [image cache](#image-cache), not the registry.
2. **Build a bundle.** Extract each image layer into `rootfs/`, honoring
   overlayfs whiteouts. Gzip and uncompressed layers work. Zstd layers are
   rejected with an explicit error. Each file keeps what the image recorded
   for it, as Docker's extraction does:
   - its **owner** (uid and gid), when the worker runs as root. An image that
     runs as a non-root `USER` depends on this: `RUN chown nginx
     /var/cache/nginx` is how such an image lets its own user write there. A
     worker that is not root can't chown, so its extracted files all belong
     to the worker's user.
   - its **mode**, including setuid, setgid, and sticky bits.
   - its **extended attributes**, best effort and only as root. This carries
     file capabilities (`security.capability`, e.g. `setcap
     cap_net_bind_service+ep` on a binary). `security.selinux` and
     `trusted.*` are skipped, and an attribute the host filesystem refuses is
     skipped with a debug log.

   Then generate an OCI runtime `config.json` with:
   - the task `command`/`env` merged over the image config's env (the task
     wins, and `PATH` gets a default if it's missing),
   - the image's working dir and user. Named users and groups resolve against
     the extracted rootfs's `/etc/passwd` and `/etc/group`, and `HOME` comes
     from the passwd entry when the env doesn't set it. Names that can't be
     resolved fall back to root with a warning.
   - host networking by default, with the host's `/etc/resolv.conf` and
     `/etc/hosts` bind-mounted read-only so DNS works. When the allocation has
     its own resolver, the bundle mounts a generated `resolv.conf` pointing at
     it instead. With `network.mode = "gated"`, the bundle joins the
     allocation's network namespace (see
     [Networking: `host` and `gated`](#networking-host-and-gated)),
   - a writable root filesystem,
   - Docker's default capabilities without `NET_RAW`, adjustable with
     [`security.capabilities`](#capabilities-security),
   - `RLIMIT_NOFILE` at 1048576, adjustable with
     [`ulimits`](#resource-limits-ulimits),
   - `noNewPrivileges`.
3. **Run it:** `<runtime> run --bundle <dir> legion-<alloc>`, with the task's
   log files as its stdout and stderr (see
   [Stdout and stderr](#stdout-and-stderr)).

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
- `security.capabilities`: `{ add = [...], drop = [...] }` adjustments to the
  default Linux capability set. See
  [Capabilities](#capabilities-security).
- `ulimits`: per-process resource limits (`setrlimit`), such as
  `{ nofile = 65536 }`. See [Resource limits](#resource-limits-ulimits).

`security` and `ulimits` apply on Linux. Apple `container` runs each container
in its own VM with the runtime's defaults, so on macOS they are accepted and
ignored (the start logs that once). Both are validated at submit: an unknown
capability, an unknown limit name, or `soft` above `hard` fails `job run` and
`job validate`.

The task-level `stop` block also applies here. It sits next to `resources`,
not inside the runtime config. See [Graceful stop](#graceful-stop).

Put the command directly after the image reference, flags included. Don't add
a `--` separator. Apple's argument parser reads `--` as the target executable,
and the container fails to start with "failed to find target executable --".

On Linux, `command` is the container's whole argv: the image's `ENTRYPOINT`
and `CMD` are not used. Apple `container` follows Docker's rule instead, where
`command` replaces `CMD` and the image's `ENTRYPOINT` still runs. To run an
image's entrypoint on both, name it in `command`. For the official Postgres
image, `command = ["docker-entrypoint.sh", "postgres"]` works everywhere: the
entrypoint passes a command it does not recognize straight through.

## Capabilities (`security`)

A Linux container's process gets Docker's default capability set, less
`CAP_NET_RAW`:

```text
AUDIT_WRITE  CHOWN  DAC_OVERRIDE  FOWNER  FSETID  KILL  MKNOD
NET_BIND_SERVICE  SETFCAP  SETGID  SETPCAP  SETUID  SYS_CHROOT
```

Stock images assume these. The official nginx image's master process
`chown`s its cache directories and runs workers as the `nginx` user, and the
Postgres entrypoint `chown`s its data directory and drops to `postgres` with
`su-exec` (or `gosu`). Without `CHOWN`, `SETUID`, and `SETGID` they fail with
`Operation not permitted`.

`CAP_NET_RAW` is withheld because a Legion container shares the node's network
namespace unless it is `gated`. Raw sockets there could sniff and spoof traffic
on the node's own interfaces, not just the container's. Nomad's Docker driver
leaves it out of its default for the same reason.

The set is granted as the bounding, effective, and permitted sets. Nothing is
inheritable or ambient, so a process running as a non-root user keeps no
capabilities across `execve`, exactly as under Docker.

Adjust it with `security.capabilities`, which has the semantics of
`docker run --cap-add` / `--cap-drop`:

```nickel
config = {
  image = "alpine:3.20",
  command = ["ping", "-c", "3", "1.1.1.1"],
  security = {
    capabilities = {
      add = ["NET_RAW"],    # on top of the defaults
      drop = ["MKNOD"],     # removed from the defaults
    },
  },
},
```

- Names are case-insensitive, with or without the `CAP_` prefix
  (`net_raw`, `NET_RAW`, and `CAP_NET_RAW` are the same).
- `drop = ["ALL"]` starts from nothing, so the container gets exactly `add`.
  That is the least-privilege form: `drop = ["ALL"], add = ["NET_BIND_SERVICE"]`.
- `add = ["ALL"]` grants every capability except the ones `drop` names.
- A name in both `add` and `drop` ends up granted, as in Docker. `ALL` in
  both lists is rejected.
- An unknown name fails at submit:
  `unknown capability "NET_RAWW" in security.capabilities.add: use a Linux
  capability name such as NET_ADMIN (the CAP_ prefix is optional), or ALL`.

Without a user namespace, container root is host root with these
capabilities. Adding `SYS_ADMIN`, `NET_ADMIN`, `SYS_PTRACE`, `SYS_MODULE`, or
`ALL` gives the container enough to act on the host, so add them only for
workloads you trust as much as an `exec` task.

**`noNewPrivileges` stays on.** It stops a setuid binary or a file
capability from *raising* privileges at `execve`. It does not stop a root
process from dropping to another user with `setuid(2)`/`setgid(2)`, which is
all `gosu` and `su-exec` do, so root entrypoints that step down work. What it
does block: an image whose `USER` is non-root and that relies on `sudo`, or on
a file capability (`setcap cap_net_bind_service+ep`) to bind a low port, does
not get that privilege. Bind a port above 1024, or run the process as root and
let it drop privileges itself.

## Resource limits (`ulimits`)

The container's process gets `RLIMIT_NOFILE` soft and hard at 1048576. That is
the value Docker containers ran with for years and the one CRI-O sets. The OCI
runtimes' own default is a hard 1024, which Redis warns about at start and a
busy proxy exhausts. On a host whose `fs.nr_open` is lower, the default drops
to it, and on a worker that is not root, to the worker's own hard limit (raising
a hard limit needs root).

Set limits per task with `ulimits`, keyed by the lowercase name `docker run
--ulimit` uses (`as`, `core`, `cpu`, `data`, `fsize`, `locks`, `memlock`,
`msgqueue`, `nice`, `nofile`, `nproc`, `rss`, `rtprio`, `rttime`,
`sigpending`, `stack`):

```nickel
config = {
  image = "redis:7-alpine",
  command = ["redis-server"],
  ulimits = {
    nofile = 65536,                       # soft and hard
    nproc = { soft = 4096, hard = 8192 },
    memlock = "unlimited",
  },
},
```

A declared limit replaces the default of the same name; the others keep
theirs. `soft` may not exceed `hard`. A hard limit above the node's
`fs.nr_open` makes the OCI runtime fail the start, with its own error in the
task's stderr.

## Stdout and stderr

The container's stdout and stderr are the allocation's `stdout.log` and
`stderr.log`, opened by the worker and handed to the runtime. Writing to them
through the inherited descriptors always works. Many images also *reopen*
them by path: nginx symlinks `/var/log/nginx/access.log` to `/dev/stdout`, and
`/dev/stdout` is `/proc/self/fd/1`. A reopen checks the file's own
permissions, so the worker hands both files to the container's user (the
image's `USER`, resolved against its rootfs) at every start. That is what
Docker does with the pipes it gives a container. The log directory itself
stays owner-only on the host (`0700`, the worker's user); a reopen through
`/proc/self/fd` does not traverse it, so the container reaches its own two
files and nothing else.

Two consequences to know about:

- A process that switches to a *different* user inside the container (`su`,
  `gosu`, `su-exec`) and then reopens `/dev/stdout` by path gets `EACCES`, as
  it would under Docker. Entry points that step down keep writing through the
  descriptors they inherited, which works. In a script, write `>&2`, not
  `> /dev/stderr`.
- Reopening with truncation (`echo x > /dev/stdout` in a shell, which opens
  with `O_TRUNC`) truncates the captured log, because it is a file rather than
  a pipe. Append (`>>`) or write to the descriptor (`>&1`) instead.

A worker that is not root cannot chown, so there the files stay the worker's,
and only an image running as the worker's own user can reopen them.

## Image cache

Each worker keeps the images it pulls in its
[node content store](artifacts.md#the-node-content-store)
(`<worker state dir>/content/`), one blob per manifest, config, and layer,
named by its digest. Both pull paths use it: the anonymous pull a `container`
or `sandbox` task makes on its own, and an image staged through the artifact
pipeline with `containerImageDesugar`.

- **A digest reference the node holds** (`alpine@sha256:…`) starts with no
  registry request at all.
- **A tag** (`nginx:1.27-alpine`) costs one `HEAD` request per start, to learn
  which digest the tag names now. A `HEAD` downloads nothing, and Docker Hub
  does not count it toward the anonymous pull limit. If the digest is one the
  node holds, nothing else is fetched. If the tag moved, the new manifest and
  only the layers the node doesn't hold yet are downloaded, so a rebuilt image
  that shares its base layers costs its changed layers.
- **When the registry doesn't answer** (rate limited with `429`, down, or the
  node is offline), a tag starts from the digest it last resolved to on this
  node, with a warning in the worker log: `Registry did not answer for this
  tag; using the digest it last resolved to on this node`. A tag the node has
  never resolved still fails with the registry's error.
- **Concurrent starts of one image share a pull.** A second allocation of the
  same reference waits for the first and then reads its layers from the cache.

Layers are hard-linked from the cache into each allocation's image layout, so
a cached image costs its disk once per node. Each allocation still extracts
its own root filesystem.

**Eviction.** Images share the content store's `artifacts` budget (by default
10% of the disk, between 1 GiB and 20 GiB), with LRU eviction when a new blob
would exceed it, and a TTL: an entry unused for `content.ttl` (default `72h`)
is removed by the store's GC, which runs every 10 minutes. An image in use by
a running allocation is safe from eviction, because its layout holds hard
links to the blobs. Configure both in the worker's `content` block (see
[docs/artifacts.md](artifacts.md#the-node-content-store)).
`content.enabled = false` turns the cache off, and every start pulls in full.

**Forcing a fresh pull.** A moved tag is picked up on the next start without
doing anything. To throw away everything a node has cached, stop the worker,
delete `<worker state dir>/content/`, and start it again (always safe; the
next start of each image pulls it in full). A cached manifest is rehashed
each time it is read, and one that no longer matches its digest is dropped and
fetched again. Layers are checked for their recorded size only; a layer
damaged in place fails extraction, and deleting the cache directory clears it.

On macOS without `containerImageDesugar`, Apple `container` pulls the image
itself and keeps it in its own image store (`container image ls`), so this
cache does not apply there.

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
  Apple `container` refuses a VM smaller than **200 MiB**. A task whose
  constraints pin it to macOS (`constraints = { os = "macos" }`) and sizes its
  guest below that (`resources.memoryMax`, else `resources.memory`) is
  rejected at submit. Any other task that lands on a macOS worker with less
  fails at start, with the reason in the allocation's events instead of only
  in the task's stderr:

  ```text
  task web sizes its macOS VM at 128 MiB (resources.memoryMax, else resources.memory), below the 200 MiB Apple `container` requires; raise it to at least 200, or constrain the task to Linux nodes
  ```

  Linux has no such floor: there `memory` is a cgroup limit.
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
- **Linux port mapping is not applied.** In the default `host` mode, assigned
  ports reach the container directly. In `gated` mode, inbound traffic only
  arrives through the datapath's ingress listeners
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
  config = { image = "postgres:16", command = ["docker-entrypoint.sh", "postgres"] },
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
capability set don't depend on the mount.

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
  - The flag defaults to **off**. With it off, macOS lets Apple `container`
    pull the image itself. Linux falls back to an anonymous pull through the
    shared `oci://` fetcher, so the OCI runtime (which can't pull) still gets
    its rootfs. Both Linux paths go through the node's
    [image cache](#image-cache). Private-registry auth and digest pinning
    require the artifact pipeline (flag on).
- **File and tree artifacts work.** They're staged into the host task
  directory, which is mounted at `/legion/task`. Both `LEGION_ARTIFACT_<NAME>`
  and `${artifact.<name>}` hold the in-container path. See
  [The task directory](#the-task-directory-legiontask).

## Networking: `host` and `gated`

By default a `container` task shares the host's network (`network.mode =
"host"`). Declared ports are host ports, and `connect.upstreams` are loopback
ports owned by the node datapath. Egress policy applies to traffic through
those upstreams and to names resolved through the allocation's resolver
(`LEGION_DNS`). In this mode, enforcement depends on the workload using them.

With `network.mode = "gated"` (Linux), the worker creates an empty network
namespace for the allocation before it starts. The container joins it by path
through an OCI `network` namespace entry. The namespace holds only loopback and
the datapath's listeners: upstream ports, VIPs, the resolver, and an egress
proxy at `127.0.0.1:1080` exported as `HTTP_PROXY` / `HTTPS_PROXY` /
`ALL_PROXY`. A tool that ignores the proxy has no route out, so it can't
bypass egress policy. The full model, the fail-closed rules, and the limits
(no raw UDP, no ICMP, no inbound to undeclared ports, Linux only) are in
[Network Handlers](network-handlers.md#gated-mode).

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
| Port publishing | ✅ macOS. n/a on Linux (host networking, or ingress listeners in `gated` mode) |
| Volumes (`volumes[]`, `stateful.storage`) | ✅ bind-mounted at the declared `path`. See [docs/volumes.md](volumes.md) |
| Artifacts | ✅ image (staged via `containerImageDesugar`, or pulled by the runtime) and file/tree (readable under `/legion/task`) |
| Exec / interactive PTY | ✅ / ✅ on both Apple `container` and the Linux OCI runtimes (sandboxed). See [docs/exec-and-actions.md](exec-and-actions.md) |
| Capability adjustments (`security.capabilities`) | ✅ Linux. Ignored on macOS (VM boundary, runtime defaults) |
| Resource limits (`ulimits`) | ✅ Linux. Ignored on macOS |
| Image layer cache | ✅ Linux, per node, in the content store. macOS: Apple `container`'s own image store |

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
default and adjusted capability sets and rlimits, layer extraction with owners,
setuid bits, and extended attributes, handing the log files to the image's
user, the macOS memory floor, the
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

Three of the Linux tests run stock images as they ship (nginx, the Postgres
entrypoint, and an image whose `USER` is `nobody`) and assert the
capabilities, ownership, `/dev/stdout`, and `nofile` behavior above. Where
anonymous pulls are rate-limited, point `LEGION_OCI_TEST_LAYOUT_DIR` at a
directory holding pre-fetched OCI layouts named `nginx`, `postgres`, and
`exporter`.

The image cache has unit tests against an in-process fake registry that
count requests: a repeated tag costs one `HEAD`, a held digest costs nothing,
a registry answering `503` still starts a cached tag, and a moved tag pulls
only its changed layers.

## Current Scope and Limitations

- The runtime integration is simple: it shells out to runtime binaries.
- The root filesystem is writable. The only host paths mounted in are the
  allocation's own task directory at `/legion/task` and any volumes the job
  declares ([docs/volumes.md](volumes.md)). The alloc directory and the log
  directory aren't mounted.
- File-delivery destinations must resolve under `/legion/task`. Per-file bind
  mounts and K8s-style projected volumes were rejected as designs because they
  break atomic refresh. They aren't planned.
- Linux containers share host networking by default. A private network
  namespace is available only through `network.mode = "gated"`, which has its
  own limits ([Network Handlers](network-handlers.md#gated-mode)). Port
  mapping is not implemented.
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
- Images are cached per node, by layer ([Image cache](#image-cache)). The
  extracted root filesystem is not: every new allocation extracts its image's
  layers again, which costs CPU and disk time on a large image but no
  network.
- On Linux, `command` replaces the image's `ENTRYPOINT` and `CMD`; on macOS
  only `CMD`. Name the entrypoint in `command` for a jobspec that runs the
  same on both.
- There is no user namespace yet, so container root is host root, limited
  by the capability set, `noNewPrivileges`, and the mount namespace. Adding
  privileged capabilities removes most of that limit (see
  [Capabilities](#capabilities-security)). There is no seccomp profile.
- Only the image's `USER` can reopen `/dev/stdout` and `/dev/stderr` by path;
  a process that switched to another user inside the container gets `EACCES`
  (see [Stdout and stderr](#stdout-and-stderr)).
- Supplementary groups from the image's `/etc/group` are not applied to the
  process; it runs with the resolved uid and gid only.
- Health checks still run through the worker health monitor.
