# Task Kind: container

`container` is a built-in task kind implemented in `src/task_plugins/container.rs`.

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
- **Other OS:** the kind is rejected as unsupported.

Before every launch the kind preflights its runtime and fails the allocation
with an actionable message when the binary is missing or (macOS) the
`container` service is not running. Nodes publish a `container: pass` readiness
check when the runtime is usable; the key is **omitted** when it is not, since
a node without a container runtime is still healthy for every other kind.

### Linux: image → bundle

A low-level OCI runtime cannot fetch an image, so the kind sources and converts
it (`src/task_plugins/oci_bundle.rs`):

1. **Obtain the image's OCI layout.** If the image was staged through the
   artifact pipeline (`artifacts.containerImageDesugar`), that layout is used;
   otherwise the kind pulls it anonymously through the shared `oci://` fetcher.
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

The command follows the image reference directly — flags and all. There is no
`--` terminator: Apple's argument parser reads `--` as the target executable
and the container fails to start with "failed to find target executable --".

## Resources and Ports

Unlike the `exec` kind, `container` isolates its workload, so the task's
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
  `resources.cpu` (MHz) has no runtime analogue and is scheduling-only.
- **Assigned ports → `--publish <host>:<container>` (macOS).** The container
  has its own IP inside a VM, so a host port only reaches it when published —
  by service consumers and by the worker's own health checks alike. A port
  declaring `to` publishes to that container-side port; one without publishes
  host-port to host-port, matching the `LEGION_PORT_<NAME>` value the task is
  handed.
- **Linux resource limits and port mapping are not applied yet.** The bundle
  uses host networking (so ports need no mapping) and sets no cgroup limits.

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
- `stop`: tear the container down by name **first** —
  `container stop legion-<alloc>` (macOS) / `<runtime> delete --force
  legion-<alloc>` (Linux). Killing the foreground `run` alone does not stop the
  container: Apple `container` is daemon-backed, and a Linux OCI runtime
  reparents the container init when its supervisor is killed. Then the child
  handle / PID is reaped.
- `destroy` delegates to `stop`.

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
- **File/tree artifacts and env referencing are deferred:** explicitly
  declared artifacts stage to the *host* task directory, and the kind does not
  mount that directory into the container, so its isolated filesystem cannot
  see them; worker-injected env (`LEGION_TASK_DIR`, `LEGION_ARTIFACT_*`) is
  not applied either. Declared `volumes[]` are mounted (see
  [docs/volumes.md](volumes.md)) — the task directory simply is not one of
  them yet. Same reason KV and identity *file* delivery are unsupported.

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ❌ (task dir is not mounted into the container) |
| Identity env / file delivery | ✅ / ❌ |
| Signal delivery | ❌ |
| Memory / CPU limits | ✅ macOS; ❌ Linux (no cgroup config yet) |
| Port publishing | ✅ macOS; n/a on Linux (host networking) |
| Volumes (`volumes[]`, `stateful.storage`) | ✅ bind-mounted at the declared `path` — see [docs/volumes.md](volumes.md) |
| Artifacts | ✅ image (staged via `containerImageDesugar`, or pulled by the kind); file/tree + env deferred |
| Exec / interactive PTY | ✅ / ✅ on Apple `container` (sandboxed); ❌ on the Linux OCI runtimes (deferred) — see [docs/exec-and-actions.md](exec-and-actions.md) |

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
suppression, resource mapping, port publishing, pinned-reference launch) and
the runtime preflight. End-to-end tests are `#[ignore]`d because they need a
runtime and network:

```bash
# macOS, with Apple `container` installed and started
cargo test --lib apple_container -- --ignored --nocapture

# Linux, with an OCI runtime installed
LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime -- --ignored --nocapture
```

## Current Scope and Limitations

- Runtime integration is intentionally simple (shelling out to runtime binaries).
- The root filesystem is writable, and the only host paths mounted in are the
  ones the job declares as volumes ([docs/volumes.md](volumes.md)). Legion does
  not mount the allocation's own task directory, which is what still blocks
  KV/identity *file* delivery and file/tree artifacts.
- Linux containers share host networking (with host DNS files bind-mounted
  read-only) and get no cgroup resource limits.
- Private-registry image auth and digest pinning require the artifact pipeline
  (`containerImageDesugar`); the kind's own fallback pull is anonymous.
- zstd-compressed image layers are rejected (gzip and uncompressed supported).
- No artifact cache yet, so an off-pipeline Linux pull fetches once per
  allocation (in-place restarts reuse the pulled layout; moves refetch).
- Health checks still run through worker health monitor.
