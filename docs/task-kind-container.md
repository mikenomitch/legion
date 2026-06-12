# Task Kind: container

`container` is a built-in task kind implemented in `src/task_plugins/container.rs`.

## Purpose

Run containerized workloads via host container runtime binaries.

## Runtime Selection

- macOS: uses `container` binary (Apple container toolchain).
- Linux: uses `youki` binary.
- Other OS: kind is rejected as unsupported.

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
  - spawn child,
  - track child and PID.
- `stop`:
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
  - macOS: the layout is tarred and imported via `container image load`.
  - Linux (youki): not yet supported — consuming a Legion-managed image fails
    with an explicit error (OCI layout -> runtime bundle conversion pending).
    Keep the flag off for Linux container workloads.
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

