# Task Kind: container

`container` is a built-in task kind implemented in `src/task_plugins/container.rs`.

## Purpose

Run containerized workloads via host container runtime binaries.

## Runtime Selection

The worker auto-detects the container runtime, so the kind "just works" on a
Mac (Docker Desktop) and on Linux without per-job configuration. Resolution
order:

1. **`LEGION_CONTAINER_RUNTIME` env override** (`docker` | `container` |
   `youki`) — pins a node's runtime regardless of detection.
2. **First installed runtime on `PATH`**, in a per-OS preference order:
   - macOS: Apple `container`, then `docker`.
   - Linux: `docker`, then `youki`.
3. **OS default** when nothing is installed, so the eventual spawn failure
   still names the expected binary.

Supported runtimes:

- **Docker** (`docker`): the portable default for local dev and Kamal-style
  workflows; available on macOS (Docker Desktop) and Linux.
- **Apple `container`** (`container`): the Apple container toolchain on macOS.
- **`youki`** (`youki`): low-level OCI runtime on Linux.

Other operating systems reject the kind as unsupported.

Docker and Apple `container` share a docker-compatible `run` surface; `youki`
takes `--image`. Only Apple `container` and `youki` need a `--` terminator
before the command (Docker rejects a literal `--`).

## Config Schema

Required:

- `image`: image reference string.
- `command`: non-empty string array.

Optional:

- `env`: map of string env vars.

## Runtime Behavior

- `prepare`: logging only.
- `start`:
  - parse `ContainerTaskConfig`,
  - detect runtime,
  - build runtime-specific launch args,
  - spawn child (stdout/stderr captured per allocation when the worker sets a
    log dir),
  - track child and PID.
- `stop`:
  - for daemon-backed runtimes (Docker, Apple `container`), tear the container
    down by name first (`docker rm -f legion-<alloc>` / `container stop
    legion-<alloc>`) — killing the foreground `run` CLI only detaches it,
    leaving the container running; `youki` runs the container init as a direct
    child so no by-name teardown is needed,
  - kill child handle if present,
  - else kill by PID lookup.
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
  - Docker (with the containerd image store) and Apple `container`: the layout
    is tarred and imported via `docker load` / `container image load`.
  - youki: not yet supported — consuming a Legion-managed image fails
    with an explicit error (OCI layout -> runtime bundle conversion pending).
    Keep the flag off for youki container workloads.
  - The flag defaults to **off** until the artifact cache lands; with it off,
    container behavior is unchanged (the runtime pulls images itself).
- **File/tree artifacts and env referencing are deferred:** explicitly
  declared artifacts stage to the *host* task directory, which the
  container's isolated filesystem cannot see, and worker-injected env
  (`LEGION_TASK_DIR`, `LEGION_ARTIFACT_*`) is not applied — both wait on
  volume plumbing, the same limitation as the kind's KV/identity file
  injection.

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ❌ (needs volume plumbing) |
| Identity env / file delivery | ✅ / ❌ |
| Signal delivery | ❌ |
| Artifacts | ✅ image only (`containerImageDesugar`, default off); file/tree + env deferred |

## Current Scope and Limitations

- Runtime integration is intentionally simple (shelling out to runtime binaries).
- No advanced network/volume orchestration in plugin itself yet; image
  pull auth exists only through the artifact pipeline above.
- Health checks still run through worker health monitor.

