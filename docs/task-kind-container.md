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

## Current Scope and Limitations

- Runtime integration is intentionally simple (shelling out to runtime binaries).
- No advanced image/auth/network/volume orchestration in plugin itself yet.
- Health checks still run through worker health monitor.

