# Task Runtime: container

`container` is a built-in task runtime implemented in `src/task_plugins/container.rs`.

## Purpose

Run containerized workloads via host container runtime binaries.

## Runtime Selection

The runtime is chosen by operating system — no Docker dependency:

- **macOS: Apple `container`.** A high-level runtime that pulls and runs images
  by reference (docker-compatible CLI surface). Install it with
  `brew install container` and start its service with `container system start`
  (it also runs as a login service via `brew services start container`). Each
  container gets its own lightweight Linux VM. Launch is
  `container run --rm --name legion-<alloc> --progress none [--memory …]
  [--cpus …] [--publish …] [--env K=V …] <image> <command…>`.
- **Linux: a low-level OCI runtime.** `youki` by default; any OCI-spec runtime
  (`runc`, `crun`) works via the `LEGION_OCI_RUNTIME` env override, since they
  share the bundle interface. These runtimes neither pull nor understand image
  references — they run a *bundle* (an extracted `rootfs/` + a `config.json`
  runtime spec), which Legion builds from the image's OCI layout (see
  [Linux: image → bundle](#linux-image--bundle)).
- **Other OS:** the runtime is rejected as unsupported.

Before every launch the runtime preflights its OCI backend and fails the allocation
with an actionable message when the binary is missing or (macOS) the
`container` service is not running. Nodes publish a `container: pass` readiness
check when the runtime is usable; the key is **omitted** when it is not, since
a node without a container runtime is still healthy for every other runtime.

### Linux: image → bundle

A low-level OCI runtime cannot fetch an image, so the runtime sources and converts
it (`src/task_plugins/oci_bundle.rs`):

1. **Obtain the image's OCI layout.** If the image was staged through the
   artifact pipeline (`artifacts.containerImageDesugar`), that layout is used;
   otherwise the runtime pulls it anonymously through the shared `oci://` fetcher.
   A layout pulled by an earlier start of the same allocation is reused, so
   in-place restarts don't refetch from the registry.
2. **Build a bundle.** Extract each image layer into `rootfs/` (honoring
   overlayfs whiteouts; gzip or uncompressed layers — zstd is rejected with an
   explicit error), and synthesize an OCI runtime `config.json`:
   - the task `command`/`env` merged over the image config's env (task wins;
     `PATH` is defaulted if absent),
   - the image's working dir and user — named users/groups resolve against the
     extracted rootfs's `/etc/passwd` / `/etc/group` (with `HOME` set from the
     passwd entry when the env doesn't set it); unresolvable names fall back to
     root with a warning,
   - host networking (no private network namespace) with the host's
     `/etc/resolv.conf` and `/etc/hosts` bind-mounted read-only so DNS works,
   - a writable root filesystem.
3. **Run it:** `<runtime> run --bundle <dir> legion-<alloc>`.

The image/bundle scratch lives under the allocation's private directory and is
removed on destroy.

## Config Schema

Required:

- `image`: image reference string.
- `command`: non-empty string array.

Optional:

- `env`: map of string env vars.
- `files`: files to materialize in the task directory before the container
  starts (`[{ path, mode, content }]`), readable inside at
  `/legion/task/<path>`. This is the shape `kvInjections.files` renders into
  — see [The task directory](#the-task-directory-legiontask).

The task-level `stop` block (a sibling of `resources`, not runtime config)
also applies here — see [Graceful stop](#graceful-stop).

The command follows the image reference directly — flags and all. There is no
`--` terminator: Apple's argument parser reads `--` as the target executable
and the container fails to start with "failed to find target executable --".

## Resources and Ports

Unlike the `exec` runtime, `container` isolates its workload, so the task's
declared `resources` and assigned ports are applied to the runtime rather than
being scheduling bookkeeping alone. Both ride the allocation payload
(`TaskAllocation.task_resources` / `.network`).

- **`resources.memory` (MiB) → `--memory`, `resources.cores` → `--cpus`
  (macOS).** Apple `container` boots a VM per container; an unsized guest gets
  the runtime's own default (1 GiB / 4 CPUs) regardless of what the scheduler
  reserved on the node. Only *declared* values are applied — scheduler
  defaults are deliberately not folded in, so a job that declares nothing keeps
  the runtime's defaults. The guest reports one CPU more than requested (its
  own housekeeping core) and rounds memory up.
  On macOS `resources.cpu` (MHz) has no runtime analogue and stays
  scheduling-only, and `cores` means guest size rather than exclusivity —
  Apple `container` has no weight or pinning to map them onto.
- **Assigned ports → `--publish <host>:<container>` (macOS).** The container
  has its own IP inside a VM, so a host port only reaches it when published —
  by service consumers and by the worker's own health checks alike. A port
  declaring `to` publishes to that container-side port; one without publishes
  host-port to host-port, matching the `LEGION_PORT_<NAME>` value the task is
  handed.
- **Linux: declared resources become kernel limits** on a worker running with
  `resourceEnforcement.enabled` — hard memory, exclusive pinned cores, and
  proportional CPU weight, applied through the generated OCI spec's
  `linux.resources` and `cgroupsPath`. The container runtime is a
  `RuntimeApplied` consumer of the node-wide enforcement subsystem, so a core
  it reserves is off-limits to `exec` tasks on the same node too. The model,
  the requirements, and the failure modes are documented in
  [docs/resource-enforcement.md](resource-enforcement.md).

  Two validation rules come with it, on Linux and macOS alike: `cores` must be
  a **whole number** (a cpuset cannot hold a fraction of a CPU), and a task
  declares `cores` **or** `cpu`, never both.
- **Linux port mapping is not applied.** The bundle uses host networking, so
  assigned ports reach the container without mapping; private network
  namespaces are the networking plan's territory.

## Runtime Behavior

- `prepare`: logging only.
- `start`:
  - parse `ContainerTaskConfig`, detect and preflight the runtime,
  - remove any container still holding this allocation's name (a worker crash
    or a killed `--rm` run leaves one behind, and `run --name` fails on a
    taken name),
  - macOS: build Apple `container run` args; Linux: obtain the OCI layout,
    build a bundle, and `run --bundle`,
  - spawn child (stdout/stderr captured per allocation when the worker sets a
    log dir; the runtime's own fetch progress is suppressed so it cannot be
    interleaved into the task's stderr),
  - track child and PID.
- `stop`: tear the container down by name **first**, gracefully — see
  [Graceful stop](#graceful-stop). Killing the foreground `run` alone does not
  stop the container: Apple `container` is daemon-backed, and a Linux OCI
  runtime reparents the container init when its supervisor is killed. Then the
  child handle / PID is reaped.
- `destroy` delegates to `stop`.

## Graceful stop

Stopping a container delivers a signal, waits, and only then kills. Both
halves are declarable per task, next to `resources`:

```jsonnet
task: {
  name: 'db',
  runtime: 'container',
  config: { image: 'postgres:16', command: ['postgres'] },
  stop: { signal: 'SIGINT', killAfter: '90s' },   // defaults: SIGTERM, 30s
}
```

The block itself — its vocabulary, its defaults, and the 5-minute cap — is
documented in [Graceful stop](graceful-stop.md#the-ladder). What is specific
to this runtime is **where the signal is delivered** and **what happens when
the jobspec is silent**:

- **The signal goes to the container's init**, not to the launching CLI.
  Both platforms keep the container alive when the foreground process is
  signalled, so signalling the tracked host process would notify nobody.
- **The Linux default chain is docker's** — the jobspec's `stop.signal`, then
  the image's own `STOPSIGNAL`, then `SIGTERM`. This is why an *undeclared*
  `stop` block is a distinct state from one that declares `SIGTERM`: only the
  silent case consults the image. An image declaring a signal outside
  Legion's vocabulary is ignored rather than failing the stop — an
  unstoppable container is worse than a `SIGTERM` the image did not ask for.
- **Apple `container` applies the image's stop signal itself**, so the macOS
  chain is jobspec-or-`SIGTERM`, and the runtime owns the ladder end to end:
  it may SIGKILL on its own schedule if `killAfter` is longer than its
  `--time` accepts.

The mechanism per platform:

- **Linux:** `<runtime> kill legion-<alloc> <signal>` ("not running" is
  ignored — a finished batch container is already gone), then poll
  `<runtime> state` until the container is stopped or absent, up to the grace
  period, then `<runtime> delete --force` unconditionally: the reaper for a
  stopped container and the killer for a hung one.
- **macOS:** `container stop --signal <signal> --time <secs> legion-<alloc>`,
  which runs the same ladder inside the daemon.

A container that exits inside the window is never force-killed; one that
ignores the signal is gone at the deadline. `stop` against an already-exited
container stays error-free.

## Signals

The runtime delivers signals to the container's **init process**, through the
runtime CLI (`container kill --signal` on macOS, `<runtime> kill` on Linux) —
*not* to the tracked pid, which is the runtime supervisor and whose death
would leave the workload running unsignaled.

That is what lets a container task declare an identity with
`changeMode: 'signal'`: on token rotation the worker rewrites the token file
inside the mounted task directory and signals the container, and the workload
re-reads it with no restart. See
[docs/workload-identity.md](workload-identity.md).

## The task directory (`/legion/task`)

Every container task gets the allocation's host task directory bind-mounted
read-write at **`/legion/task`**. Everything the worker stages for the task is
readable there: file and tree artifacts, KV-rendered files, and identity
tokens.

The *directory* is mounted, never individual files. That is the whole point:
the worker replaces rendered files atomically by rename, which stays visible
through a directory bind mount and silently breaks through a per-file one (the
well-known docker pitfall, where the container keeps the stale inode). A KV
re-render or a token rotation therefore reaches a running container with no
restart and no projection machinery.

The mount is added *before* declared `volumes[]`, so a job can deliberately
shadow it with a volume at the same destination.

**Env is translated to the in-container view**, extending the convention
volumes already set:

| Variable | Value inside the container |
| --- | --- |
| `LEGION_TASK_DIR` | `/legion/task` |
| `LEGION_ARTIFACT_<NAME>` | the staged path, rebased onto `/legion/task` |
| `LEGION_VOLUME_<NAME>`, `LEGION_STORAGE_DIR` | the declared mount destination |
| `LEGION_ALLOC_DIR` | **not set** — the alloc dir is not mounted, and pointing env at an invisible path is the bug this exists to fix |

`${artifact.<name>}` interpolation in the runtime config uses the same
translated paths.

**File destinations must resolve under `/legion/task`.** Nothing else is
reachable from inside the container, so a destination outside the mount is
rejected at submit rather than staged where the workload could never read it.
Relative paths resolve against the mount:

```jsonnet
kvInjections: { files: [
  { path: 'config/app.toml', file: { template: '...' } },  // -> /legion/task/config/app.toml
] },
identities: [{
  name: 'api',
  file: { path: '/legion/task/identity/token' },           // absolute form, in range
  changeMode: 'signal', changeSignal: 'SIGHUP',
}],
```

An out-of-range path fails at submit with the fix in the message:

```
task api identity "api": file.path "/var/run/legion/token" is outside the
container task directory; use a path under /legion/task (or a relative path),
or switch to env delivery
```

This is the one place a jobspec is **not** verbatim-portable between the
`exec` and `container` runtimes: the exec runtime's filesystem *is* the
host's, so its host-absolute destinations stay valid and are not translated.

Only the allocation's own private task directory is exposed. The log directory
is not mounted (captured stdout/stderr can carry secrets and stays
owner-restricted on the host), and `noNewPrivileges`, masked paths, and the
capability set are unchanged.

## Artifacts Support

`container` reports `supports_artifacts()`, with consumption scoped to
images (see [docs/artifacts.md](artifacts.md)):

- **Image desugaring (behind a cluster flag):** when the manager config sets
  `artifacts.containerImageDesugar: true`, the `image` field is desugared at
  admission into an implicit `oci://` artifact named `image` (Docker Hub
  shorthand expands, e.g. `alpine:latest` ->
  `oci://docker.io/library/alpine:latest`). The worker pulls it through the
  shared artifact pipeline — cluster auth matchers, digest pinning, status
  rows — and the plugin's `consume_artifact` imports the staged OCI layout
  into the runtime.
  - macOS (Apple `container`): the layout is tarred and imported via
    `container image load`, and the subsequent run uses the artifact's
    **pinned digest reference** rather than the jobspec's tag — so the
    container provably runs the image the pipeline resolved, with no registry
    pull. (An allocation restored after a worker restart has no staged record
    and falls back to the jobspec's reference.)
  - Linux (OCI runtime): the layout is converted to a runtime bundle (see
    [Linux: image → bundle](#linux-image--bundle)).
  - Container images are Linux guests on every host, so the `oci://` fetcher
    resolves multi-platform indexes against `linux/<arch>` rather than the
    host platform — on macOS the host platform (`darwin/arm64`) matches
    nothing in an image index.
  - The flag defaults to **off** until the artifact cache lands. With it off,
    macOS lets Apple `container` pull the image itself, while Linux falls back
    to an anonymous pull through the shared `oci://` fetcher (so the OCI runtime,
    which cannot pull, still gets its rootfs). Private-registry auth and digest
    pinning require the artifact pipeline (flag on).
- **File/tree artifacts work:** they stage into the host task directory, which
  is mounted at `/legion/task`, and both `LEGION_ARTIFACT_<NAME>` and
  `${artifact.<name>}` carry the in-container path. See
  [The task directory](#the-task-directory-legiontask).

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ✅ — destinations must resolve under `/legion/task` |
| Identity env / file delivery | ✅ / ✅ — file destinations under `/legion/task` |
| Signal delivery | ✅ — to the container init, both platforms |
| Graceful stop (`stop { signal, killAfter }`) | ✅ both platforms |
| Memory / CPU limits | ✅ macOS (guest sizing); ✅ Linux with `resourceEnforcement.enabled` (cgroup v2) — see [docs/resource-enforcement.md](resource-enforcement.md) |
| Exclusive cores (`resources.cores`) | ✅ Linux (cpuset pinning); macOS sizes the guest instead |
| Port publishing | ✅ macOS; n/a on Linux (host networking) |
| Volumes (`volumes[]`, `stateful.storage`) | ✅ bind-mounted at the declared `path` — see [docs/volumes.md](volumes.md) |
| Artifacts | ✅ image (staged via `containerImageDesugar`, or pulled by the runtime) and file/tree (readable under `/legion/task`) |
| Exec / interactive PTY | ✅ / ✅ on both Apple `container` and the Linux OCI runtimes (sandboxed) — see [docs/exec-and-actions.md](exec-and-actions.md) |

Container exec runs inside the container's namespaces, so it is sandboxed
(`host: false`): an `exec:write` rule without `host` suffices.

## Examples

- [`configs/jobs/mac-container.jsonnet`](../configs/jobs/mac-container.jsonnet)
  — batch task: env, declared resources sizing the guest.
- [`configs/jobs/mac-container-service.jsonnet`](../configs/jobs/mac-container-service.jsonnet)
  — long-running service: dynamic port publishing, health checks, service
  discovery, exec, and declared actions.

Both are macOS-constrained because the Linux path needs an OCI runtime
installed; the jobspecs themselves are portable apart from the `os` constraint.

## Testing

Unit tests cover launch-argv construction (no `--` separator, progress
suppression, resource mapping, port publishing, pinned-reference launch), the
exec and signal argv dialects per runtime, the stop-signal default chain, the
generated `linux.resources`/`cgroupsPath`, the `/legion/task` mount ordering,
env translation, and the runtime preflight. End-to-end tests are `#[ignore]`d
because they need a runtime and network (and, for the limit tests, cgroup v2
and root):

```bash
# macOS, with Apple `container` installed and started
cargo test --lib apple_container -- --ignored --nocapture

# Linux, with an OCI runtime installed
LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime -- --ignored --nocapture
```

## Current Scope and Limitations

- Runtime integration is intentionally simple (shelling out to runtime binaries).
- The root filesystem is writable. The host paths mounted in are the
  allocation's own task directory at `/legion/task` and whatever the job
  declares as volumes ([docs/volumes.md](volumes.md)) — the alloc directory
  and the log directory are not mounted.
- File-delivery destinations must resolve under `/legion/task`; per-file bind
  mounts and K8s-style projected volumes are rejected designs, not pending
  work (they break refresh atomicity).
- Linux containers share host networking (with host DNS files bind-mounted
  read-only); private network namespaces and port mapping are not implemented.
- Linux resource enforcement requires the cgroup v2 unified hierarchy, a
  worker that can create the `legion/` subtree at the cgroup root, and
  `resourceEnforcement.enabled` in the worker config. Without it, `resources`
  stay scheduling-only on Linux, exactly as before.
- On macOS, `resources.cpu` (MHz) remains scheduling-only and `cores` means
  guest size rather than exclusivity — Apple `container` has no weight or
  pinning analogue.
- There is no `legion alloc signal` verb: signals are delivered by declared
  policy (`changeSignal` on refresh, `stop.signal` on teardown), not on demand.
- Private-registry image auth and digest pinning require the artifact pipeline
  (`containerImageDesugar`); the runtime's own fallback pull is anonymous.
- zstd-compressed image layers are rejected (gzip and uncompressed supported).
- No artifact cache yet, so an off-pipeline Linux pull fetches once per
  allocation (in-place restarts reuse the pulled layout; moves refetch).
- Health checks still run through worker health monitor.
