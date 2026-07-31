# Task Runtime: noop

`noop` is a bundled dynamic task plugin in `plugins/tasks/noop/`.

## Purpose

Provide a minimal task runtime for testing scheduler/task-manager integration without launching real workloads.

## Plugin Type

- Dynamic plugin (`cdylib`), not hard-coded as a built-in branch.
- Loaded via normal task plugin path resolution.
- Worker startup attempts to load it if present.

## Behavior

- `schema`: accepts any object.
- `validate_config`: always succeeds.
- `prepare/start/stop/destroy`: log actions.
- `start`: copies task config JSON into runtime `state`.

## Artifacts Support

None. `noop` does not report `supports_artifacts()`, so a `noop` task that
declares `artifacts` is rejected with an actionable error at
`legion job validate` and at admission (see
[docs/artifacts.md](artifacts.md)).

## Stop and resource fields

`noop` tracks no process and confines nothing, so the task-level `stopSignal`
and `stopGracePeriod` fields are **accepted and inert** on a `noop` task —
there is nothing to signal and nothing to wait for. Likewise its
`resource_enforcement()` is `Unsupported`, so `resources` stay scheduling-only
and the worker touches no cgroup for it (see
[docs/resource-enforcement.md](resource-enforcement.md)).

## Usage Notes

- Useful in tests and lightweight demo jobs.
- Because it does not spawn a real process, health checks that depend on external endpoints/process state may not make sense with this runtime.

