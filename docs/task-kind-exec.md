# Task Kind: exec

`exec` is a built-in task kind implemented in `src/task_plugins/exec.rs`.

## Purpose

Run a host process directly using `std::process::Command`.

## Config Schema

Required:

- `command`: array of strings, first element is program, remainder are args.

Optional:

- `env`: map of string env vars.
- `cwd`: working directory.
- `user` (unix): run as given user (resolved to uid/gid).
- `oom_adj` / `oom_score_adj` (linux): one of these, range `-1000..1000`.
- `cgroup` (linux): cgroup path.
- `landlock` (linux):
  - `read_only_paths`
  - `read_write_paths`

## Runtime Behavior

- `prepare`: logging only.
- `start`:
  - parse config,
  - configure process options,
  - spawn child,
  - store child and PID in runtime.
- `stop`:
  - kill tracked child handle when available,
  - otherwise try kill by PID lookup.
- `destroy` delegates to `stop`.

Spawn helper sets a new session (`setsid`) on Unix so task processes survive worker restarts and are signal-isolated from worker process handling.

## Notes

- Landlock features are Linux-only and conditional.
- Health checks are external (worker health monitor), not part of exec plugin logic.

