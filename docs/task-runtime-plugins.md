# Task Runtime Plugins

This document explains how task-runtime plugins integrate with workers.

## Selecting a runtime

Every task names the runtime that runs it. The runtime is the pluggable
component responsible for starting, supervising, signalling, and reaping the
workload:

```jsonnet
tasks: {
  web: {
    runtime: 'container',
    config: { image: 'nginx:1.27' },
  },
  migrate: {
    runtime: 'exec',
    lifecycle: 'prestart',
    config: { command: ['./migrate.sh'] },
  },
}
```

`runtime` is required on every task. Its value is resolved against the worker's
task-plugin registry, so it is either a built-in (`exec`, `container`, `noop`)
or the directory name of a plugin under `plugins/tasks/<runtime>/`. The `config`
object is runtime-specific and is validated against that runtime's schema —
`legion job schema` emits the live per-runtime schemas under `taskRuntimes`.

> **Renamed from `kind`.** This field used to be called `kind`. There is no
> alias: a jobspec that still says `kind` is rejected at submit with an error
> naming the replacement. Editing existing jobspecs is a mechanical
> `kind:` → `runtime:` substitution on task blocks. Health checks keep their own
> `kind` field (`http`, `tcp`, `script`) — that is a different axis and was not
> renamed.

## Plugin Contract

Task runtimes implement `TaskPlugin` (`src/task_plugins/mod.rs`).

Lifecycle hooks:

- `schema()` / `validate_config()`
- `setup()`
- `readiness()`
- `prepare()`
- `start()`
- `stop()`
- `destroy()`

Per-task state container, passed `&mut` to every hook:

- `TaskInstance` with:
  - arbitrary plugin `state` JSON
  - optional child process handle
  - optional PID
  - allocation and task directories, injected env, staged artifacts, log dir

`TaskInstance` was previously named `TaskInstance`. It was renamed when the task
axis took the word `runtime`, so that a hook signature no longer has two
different "runtimes" in it. Out-of-tree plugins must rename the type at their
call sites; the fields and semantics are unchanged.

## Loading and Resolution

`TaskPluginRegistry` behavior:

- Caches plugins per `runtime`.
- Loads built-ins for `exec` and `container` directly.
- Loads other runtimes dynamically from plugin dir.

Dynamic plugin requirements:

- `cdylib`
- exported symbol `get() -> Box<dyn TaskPlugin>`
- location under `plugins/tasks/<runtime>/...`

## Worker Integration

Worker task manager flow:

1. Load built-ins at startup (`exec`, `container`, optional `noop`).
2. On allocation start:
   - resolve plugin by `task.runtime`
   - run `prepare` then `start`
3. On reconciliation stop:
   - run `stop` then `destroy`
4. On failure handling:
   - worker records failure and calls plugin `destroy` for cleanup
5. Persist allocation/task/PID state to local worker file.

Health checks and restart policies are managed by worker health monitor logic outside the plugin, but plugins control the actual runtime start/stop behavior.

## Built-in and Bundled Runtimes

See per-runtime docs:

- `docs/task-runtime-exec.md`
- `docs/task-runtime-container.md`
- `docs/task-runtime-noop.md`
