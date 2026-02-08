# Task Kind Plugins

This document explains how task-kind plugins integrate with workers.

## Plugin Contract

Task kinds implement `TaskPlugin` (`src/task_plugins/mod.rs`).

Lifecycle hooks:

- `schema()` / `validate_config()`
- `setup()`
- `readiness()`
- `prepare()`
- `start()`
- `stop()`
- `destroy()`

Runtime state container:

- `TaskRuntime` with:
  - arbitrary plugin `state` JSON
  - optional child process handle
  - optional PID

## Loading and Resolution

`TaskPluginRegistry` behavior:

- Caches plugins per `kind`.
- Loads built-ins for `exec` and `container` directly.
- Loads other kinds dynamically from plugin dir.

Dynamic plugin requirements:

- `cdylib`
- exported symbol `get() -> Box<dyn TaskPlugin>`
- location under `plugins/tasks/<kind>/...`

## Worker Integration

Worker task manager flow:

1. Load built-ins at startup (`exec`, `container`, optional `noop`).
2. On allocation start:
   - resolve plugin by `task.kind`
   - run `prepare` then `start`
3. On reconciliation stop/failure:
   - run `stop` then `destroy`
4. Persist allocation/task/PID state to local worker file.

Health checks and restart policies are managed by worker health monitor logic outside the plugin, but plugins control the actual runtime start/stop behavior.

## Built-in and Bundled Kinds

See per-kind docs:

- `docs/task-kind-exec.md`
- `docs/task-kind-container.md`
- `docs/task-kind-noop.md`

