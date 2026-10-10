# Task Runtime: noop

`noop` is a bundled dynamic task plugin in `plugins/tasks/noop/`.

## Purpose

`noop` is a minimal task runtime. Use it to test scheduler and task-manager integration without launching real workloads.

## Plugin Type

- Dynamic plugin (`cdylib`), not a hard-coded built-in.
- Loaded through the normal task plugin path resolution.
- The worker tries to load it at startup if it is present.

## Behavior

- `schema`: accepts any object.
- `validate_config`: always succeeds.
- `prepare/start/stop/destroy`: log actions.
- `start`: copies the task config JSON into the runtime `state`.

## Artifacts Support

None. `noop` does not report `supports_artifacts()`. A `noop` task that
declares `artifacts` is rejected with an actionable error at
`legion job validate` and at admission (see
[docs/artifacts.md](artifacts.md)).

## Stop and resource fields

`noop` tracks no process and confines nothing. A `noop` task accepts the
task-level `stop` block, but the block has no effect. There is nothing to
signal, so the runtime declines the notice and tears the allocation down
immediately instead of waiting out the stop window. Its
`resource_enforcement()` is `Unsupported`, so `resources` only affect
scheduling and the worker touches no cgroup for it (see
[docs/resource-enforcement.md](resource-enforcement.md)).

## Usage Notes

- Useful in tests and lightweight demo jobs.
- It does not spawn a real process, so health checks that depend on external endpoints or process state may not make sense with this runtime.
