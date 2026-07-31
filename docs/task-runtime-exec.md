# Task Runtime: exec

`exec` is a built-in task runtime implemented in `src/task_plugins/exec.rs`.

## Purpose

Run a host process directly using `std::process::Command`.

## Config Schema

Required:

- `command`: array of strings, first element is program, remainder are args.

Optional:

- `env`: map of string env vars.
- `cwd`: working directory. Defaults to the allocation's private task
  directory when unset (see Artifacts Support below).
- `user` (unix): run as given user (resolved to uid/gid). At start the
  allocation directory tree is chowned to this user (without following
  symlinks): the worker creates alloc/task dirs owner-only as the isolation
  boundary between allocations on a node, so the tree must be handed over
  for the dropped-privilege process to use its own cwd and artifacts.
  Chowning to a different user requires the worker to run with the
  privileges to do so (as does setting uid/gid at all).
- `oom_adj` / `oom_score_adj` (linux): one of these, range `-1000..1000`.
- `cgroup` (linux): cgroup path.
- `landlock` (linux):
  - `read_only_paths`
  - `read_write_paths`
- `hardening` (linux): `noNewPrivileges`, `privateTmp`,
  `capabilities: { drop: [...] }`, `umask`, `nice` — see
  [Task Supervision](task-supervision.md#process-hardening).

## Runtime Behavior

- `validate_config` runs the *typed* parse at submit, not just the JSON schema,
  so a bad `umask`, an unknown capability, or both `oom_adj` and `oom_score_adj`
  fail in `legion job validate` rather than on some node at start.
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

## Artifacts Support

`exec` reports `supports_artifacts()` and gets the full artifact surface
(see [docs/artifacts.md](artifacts.md)):

- File/Tree artifacts stage into the per-allocation task directory
  (`allocs/<alloc_id>/<task_name>/` under the worker state dir) before
  `prepare`/`start`.
- `${artifact.<name>}` interpolation expands in all string config fields
  (command, env values, cwd, ...).
- Worker-injected env is applied at start: `LEGION_TASK_DIR`,
  `LEGION_ALLOC_DIR`, and `LEGION_ARTIFACT_<NAME>` (explicit task `env`
  wins on collision).
- `cwd` defaults to the task directory when unset.
- When `landlock` is configured, the task directory is appended to
  `read_write_paths` automatically so staged artifacts stay readable.

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ✅ |
| Identity env / file delivery | ✅ / ✅ |
| Signal delivery | ✅ |
| Exit reporting / restart policy | ✅ (see [Task Supervision](task-supervision.md)) |
| Artifacts | ✅ (File/Tree + interpolation + env) |
| Exec / interactive PTY | ✅ / ✅ — **host-level** (see [docs/exec-and-actions.md](exec-and-actions.md)) |

Exec runs a sibling host process with the task's env and working directory.
Because it is a process on the host (even under landlock), it carries the
`host` ACL flag: exec into an `exec` task requires an `exec:write` rule with
`host: true`.

## Notes

- Landlock features are Linux-only and conditional.
- Health checks are external (worker health monitor), not part of exec plugin logic.
- **Process exit** is observed by the worker's task supervisor, which reaps the
  child and applies the task's `restart` policy — see
  [Task Supervision](task-supervision.md).
- **Resource limits.** `config.cgroup` *joins* an existing cgroup. For Legion to
  create one and write `resources` into it as real limits, enable
  `resourceEnforcement` on the worker
  ([Task Supervision](task-supervision.md#resource-enforcement-cgroups)); an
  explicit `cgroup` opts the task out.

