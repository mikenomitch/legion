# Task Runtime: container

`container` is a built-in task runtime implemented in `src/task_plugins/container.rs`.

## Purpose

Run containerized workloads via host container runtime binaries.

## Runtime Selection

The runtime is chosen by operating system — no Docker dependency:

- **macOS: Apple `container`.** A high-level runtime that pulls and runs images
  by reference (docker-compatible CLI surface). Launch is
  `container run --rm --name legion-<alloc> [--env K=V …] <image> -- <command>`
  (the `--` lets a flag-bearing command pass through).
- **Linux: a low-level OCI runtime.** `youki` by default; any OCI-spec runtime
  (`runc`, `crun`) works via the `LEGION_OCI_RUNTIME` env override, since they
  share the bundle interface. These runtimes neither pull nor understand image
  references — they run a *bundle* (an extracted `rootfs/` + a `config.json`
  runtime spec), which Legion builds from the image's OCI layout (see
  [Linux: image → bundle](#linux-image--bundle)).
- **Other OS:** the runtime is rejected as unsupported.

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

## Runtime Behavior

- `prepare`: logging only.
- `start`:
  - parse `ContainerTaskConfig`, detect runtime,
  - macOS: build Apple `container run` args; Linux: obtain the OCI layout,
    build a bundle, and `run --bundle`,
  - spawn child (stdout/stderr captured per allocation when the worker sets a
    log dir),
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
    `container image load`, so the subsequent run needs no registry pull.
  - Linux (OCI runtime): the layout is converted to a runtime bundle (see
    [Linux: image → bundle](#linux-image--bundle)).
  - The flag defaults to **off** until the artifact cache lands. With it off,
    macOS lets Apple `container` pull the image itself, while Linux falls back
    to an anonymous pull through the shared `oci://` fetcher (so the OCI runtime,
    which cannot pull, still gets its rootfs). Private-registry auth and digest
    pinning require the artifact pipeline (flag on).
- **File/tree artifacts and env referencing are deferred:** explicitly
  declared artifacts stage to the *host* task directory, which the
  container's isolated filesystem cannot see, and worker-injected env
  (`LEGION_TASK_DIR`, `LEGION_ARTIFACT_*`) is not applied — both wait on
  volume plumbing, the same limitation as the runtime's KV/identity file
  injection.

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ❌ (needs volume plumbing) |
| Identity env / file delivery | ✅ / ❌ |
| Signal delivery | ❌ |
| Artifacts | ✅ image (staged via `containerImageDesugar`, or pulled by the runtime); file/tree + env deferred |
| Exec / interactive PTY | ✅ / ✅ on Apple `container` (sandboxed); ❌ on the Linux OCI runtimes (deferred) — see [docs/exec-and-actions.md](exec-and-actions.md) |

Container exec runs inside the container's namespaces, so it is sandboxed
(`host: false`): an `exec:write` rule without `host` suffices.

## Current Scope and Limitations

- Runtime integration is intentionally simple (shelling out to runtime binaries).
- No advanced network/volume orchestration yet: Linux containers share host
  networking (with host DNS files bind-mounted read-only), and the root
  filesystem is writable (no volume mounts).
- Private-registry image auth and digest pinning require the artifact pipeline
  (`containerImageDesugar`); the runtime's own fallback pull is anonymous.
- zstd-compressed image layers are rejected (gzip and uncompressed supported).
- No artifact cache yet, so an off-pipeline Linux pull fetches once per
  allocation (in-place restarts reuse the pulled layout; moves refetch).
- Health checks still run through worker health monitor.

