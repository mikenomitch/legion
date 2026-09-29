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
  `capabilities = { drop = [...] }`, `umask`, `nice` — see
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
- The **stop notice** is the trait default: the task's `stop.signal` is
  delivered to the process *group* (tasks are spawned into their own session,
  so a shell-wrapped task's children are reached too), and the call returns
  immediately. The worker holds the allocation until it exits or its
  `stop.killAfter` window closes — see [Graceful stop](graceful-stop.md).
- `stop` is the **hard** step the worker's reaper calls at that point: kill
  the tracked child handle, or failing that by PID lookup. A process that
  exited inside its window is already gone and is never force-killed.
- Under worker-owned enforcement, that step additionally writes
  `cgroup.kill`, which reaps double-forked descendants that reparented away
  from the tracked pid — strictly after the reap, so a task that drained
  cleanly is never SIGKILLed for the privilege.
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

## Networking: `host` and `gated`

By default a `exec` task shares the host's network (`network.mode =
"host"`): declared ports are host ports, `connect.upstreams` are loopback
ports the node datapath owns, and egress policy applies to traffic that goes
through those upstreams and to names resolved through the allocation's
resolver (`LEGION_DNS`) — cooperatively.

With `network.mode = "gated"` (Linux) the worker creates an empty network
namespace for the allocation before it starts and the process enters it with `setns` before exec (ahead of the user switch and landlock). The namespace
holds loopback and the datapath's listeners only — upstream ports, VIPs,
the resolver, and an egress proxy at `127.0.0.1:1080` exported as
`HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` — so egress policy is
unbypassable: a tool that ignores the proxy has no route. The full model,
the fail-closed rules, and the limits (no raw UDP, no ICMP, no inbound to
undeclared ports, Linux only) are in
[Network Handlers](network-handlers.md#gated-mode).

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ✅ |
| Identity env / file delivery | ✅ / ✅ |
| Signal delivery | ✅ |
| Graceful stop (`stop { signal, killAfter }`) | ✅ |
| Memory / CPU limits, exclusive cores | ✅ Linux with `resourceEnforcement.enabled` — see [docs/resource-enforcement.md](resource-enforcement.md) |
| Exit reporting / restart policy | ✅ (see [Task Supervision](task-supervision.md)) |
| Artifacts | ✅ (File/Tree + interpolation + env) |
| Exec / interactive PTY | ✅ / ✅ — **host-level** (see [docs/exec-and-actions.md](exec-and-actions.md)) |

Exec runs a sibling host process with the task's env and working directory.
Because it is a process on the host (even under landlock), it carries the
`host` ACL flag: exec into an `exec` task requires an `exec:write` rule with
`host = true`.

## Notes

- Landlock features are Linux-only and conditional.
- Health checks are external (worker health monitor), not part of exec plugin logic.
- **Process exit** is observed by the worker's task supervisor, which reaps the
  child and applies the task's `restart` policy — see
  [Task Supervision](task-supervision.md).
- **Resource limits.** With `resourceEnforcement.enabled` on the worker, the
  `exec` runtime is a **governed** runtime: worker core creates this
  allocation's cgroup inside the node-wide `legion/` hierarchy, writes the
  task's `resources` into it as real limits (`memory.max`, `cpu.weight`,
  `cpuset.cpus`), and the plugin spawns the process into it. That puts exec
  tasks in the same hierarchy as container tasks, so a core reserved by either
  is exclusive against both. See
  [docs/resource-enforcement.md](resource-enforcement.md).

  `config.cgroup` still *joins* an existing cgroup and **wins**, opting the
  task out of the Legion hierarchy — it keeps meaning what it always meant.
  Because Legion cannot promise exclusivity inside a cgroup it does not
  manage, declaring `cores` together with an explicit `cgroup` is rejected at
  submit. `user`, `landlock`, and `oom_score_adj` compose with the
  worker-owned cgroup unchanged (the join is one more privileged `pre_exec`
  step, ordered before the switch to the task user).

