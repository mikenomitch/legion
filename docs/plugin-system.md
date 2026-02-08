# Plugin System

This document describes the plugin system that exists today and notes plugin categories that are planned but not implemented.

## How Plugin Loading Works

Legion resolves plugin roots from `LEGION_PLUGIN_DIR` (default: `plugins/` under repo root).

Dynamic plugin loading pattern:

- Plugins are Rust `cdylib` libraries.
- Exported symbol must be `get`.
- Legion loads plugin `.so/.dylib/.dll` with `libloading`.

Common directory pattern:

- `plugins/<kind>/<name>/target/release/lib<name>.<ext>`

## Plugin Types Implemented Today

## 1) Task Kind Plugins

Purpose: run workload tasks on workers.

- Built-in implementations in-process:
  - `exec` (`src/task_plugins/exec.rs`)
  - `container` (`src/task_plugins/container.rs`)
- Dynamically loaded examples/shipped plugins:
  - `noop` (`plugins/tasks/noop/`)

Entry trait: `TaskPlugin` in `src/task_plugins/mod.rs`.

## 2) Scheduling Policy Plugins

Purpose: select node placement strategy.

- Built-in policies:
  - `bin_pack`
  - `spread`
- Dynamic example plugin:
  - `random` (`plugins/scheduling/random/`)

Entry trait: `SchedulingPolicy` in `src/manager/scheduler/policy.rs`.

## 3) Node Check Plugins

Purpose: augment worker fingerprint and node health checks.

- Built-in checkers/fingerprinters:
  - arch
  - os
  - kitchen-sink fingerprint
  - disk health checks
- Dynamic example plugin:
  - `example` (`plugins/node_checks/example/`)

Entry trait: `NodeChecker` in `src/worker/node_check.rs`.

## 4) Rollout Strategy Plugins

Purpose: transform/resolve rollout config before rollout execution.

- Built-in strategy:
  - `default`
- Dynamic example plugin:
  - `slow_start` (`plugins/rollouts/slow_start/`)

Entry trait: `RolloutStrategyPlugin` in `src/manager/rollout_strategy.rs`.

## Build Behavior

`build.rs` behavior:

- Protobuf always compiles.
- Plugins build only when `LEGION_BUILD_PLUGINS=1`.
- Plugin builds are skipped if `LEGION_SKIP_PLUGIN_BUILD` is set.

This allows normal core builds without paying plugin build cost.

## Planned Plugin Categories (Not Implemented Yet)

Based on `docs/TASK-LIST.md`, `README.md`, and `docs/differentiators.md`, these are intended but not present as first-class plugin frameworks today:

- Networking/CNI plugins.
- Volume plugins.
- Artifact acquisition plugins (image/file/etc lifecycle).
- Command/API endpoint extension plugins.
- Logging/observability pipeline plugins.
- Eval hooks / scheduler reaction hooks.
- UI extension plugins.
- Job submit hook plugins.

Some similarly named functionality exists in non-plugin form today (for example, resource defaults/validation in `ResourcePolicy`).

