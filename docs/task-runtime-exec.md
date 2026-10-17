# Task Runtime: exec

`exec` is a built-in task runtime that runs a process directly on the host.
Use it when your workload is a binary or script that doesn't need a container
image. It is implemented in `src/task_plugins/exec.rs`.

## Purpose

Run a host process directly using `std::process::Command`.

## Trust

An `exec` task is a process on the host, with host paths, host users, and
host binaries. Without `user` it runs as root on the node: it can read the
worker's credentials (its node token, its node key) and, on a node that also
runs a manager, the cluster's. Run untrusted code in `container` or
`sandbox`, never `exec`.

- Submitting a job with an `exec` task needs the `host` grant on
  `job:write` ([Host-level jobs](auth-and-acls.md#host-level-jobs)).
- `user` drops to that user's uid and gid, and drops every supplementary
  group first (`setgroups`), so the task does not inherit the worker's
  groups.
- `network.mode = "gated"` refuses to run as root, because a root process
  with `CAP_SYS_ADMIN` can `setns` out of its namespace:

  ```text
  Error: task "worker" sets network.mode = "gated" but runs as root. A root exec task can
  leave its network namespace, so gated would not hold.
  Hint: set user = "nobody" (or another unprivileged user), or use runtime = "container"
  ```

- The [kernel host guard](network-handlers.md#the-host-guard-in-the-kernel)
  keeps a host-mode task off Legion's ports and the metadata service, but a
  root task can delete its table. It holds for tasks with a `user`.

## Config Schema

Required:

- `command`: array of strings, first element is program, remainder are args.

Optional:

- `env`: map of string env vars. The task does **not** inherit the
  worker's environment, which may hold a `LEGION_TOKEN` or cloud
  credentials. It starts from `PATH` (a standard one when the worker has
  none), `LANG`, `LANGUAGE`, `TZ`, and every `LC_*` from the worker, plus
  `HOME`, `USER`, `LOGNAME`, and `SHELL` for the user it runs as (the
  worker's own without a `user`). Legion's allocation env and this map are
  layered on top. A variable the task needs from the host (`http_proxy`,
  `SSL_CERT_FILE`) goes in `env`. An exec session into the task starts from
  the same allowlist.
- `cwd`: working directory. Defaults to the allocation's private task
  directory when unset (see Artifacts Support below).
- `user` (unix): run as the given user (resolved to uid/gid). At start, the
  worker chowns the allocation directory tree to this user, without following
  symlinks. The worker creates alloc and task directories as owner-only to
  isolate allocations on a node from each other. The tree has to be handed
  over so the lower-privilege process can use its own cwd and artifacts.
  The worker needs the privileges to chown to another user, and to set
  uid/gid at all.
- `oom_adj` / `oom_score_adj` (linux): one of these, range `-1000..1000`.
- `cgroup` (linux): cgroup path.
- `landlock` (linux):
  - `read_only_paths`
  - `read_write_paths`
- `hardening` (linux): `noNewPrivileges`, `privateTmp`,
  `capabilities = { drop = [...] }`, `umask`, `nice`. See
  [Task Supervision](task-supervision.md#process-hardening).

## Runtime Behavior

- `validate_config` runs the full typed parse at submit, in addition to the
  JSON schema check. A bad `umask`, an unknown capability, or setting both
  `oom_adj` and `oom_score_adj` fails in `legion job validate` instead of on
  a node at start.
- `prepare`: logging only.
- `start`:
  - parse config,
  - configure process options,
  - spawn child,
  - store child and PID in runtime.
- The stop notice uses the trait default. The task's `stop.signal` goes to
  the process group, and the call returns immediately. Tasks are spawned into
  their own session, so the children of a shell-wrapped task get the signal
  too. The worker holds the allocation until the process exits or its
  `stop.killAfter` window closes. See [Graceful stop](graceful-stop.md).
- `stop` is the hard step the worker's reaper calls when that window closes.
  It SIGKILLs the task's process group, then kills the tracked child handle, or
  falls back to a PID lookup. Processes that left the group with their own
  `setsid` escape this step; worker-owned enforcement's `cgroup.kill` (below)
  catches them. A process
  that exited inside its window is already gone and is never force-killed.
- Under worker-owned enforcement, that step also writes `cgroup.kill`. This
  reaps double-forked descendants that reparented away from the tracked PID.
  It runs only after the reap, so a task that drained cleanly is never sent
  SIGKILL.
- `destroy` delegates to `stop`.

On Unix, the spawn helper starts a new session (`setsid`). Task processes
survive worker restarts, and signals sent to the worker don't reach them.

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

By default an `exec` task shares the host's network (`network.mode =
"host"`). Declared ports are host ports, and `connect.upstreams` are loopback
ports owned by the node datapath. Egress policy applies to traffic through
those upstreams and to names resolved through the allocation's resolver
(`LEGION_DNS`). In this mode, enforcement depends on the task cooperating.
On a Linux node running the
[kernel host guard](network-handlers.md#the-host-guard-in-the-kernel), the
task still cannot reach the node's Legion ports, the cloud metadata service,
or other allocations' listeners: its sockets are matched by its cgroup leaf
and dropped with `EPERM`. A task running as root can remove that table, so
the guard holds for tasks with a `user` only.

With `network.mode = "gated"` (Linux), the worker creates an empty network
namespace for the allocation before it starts. The process enters it with
`setns` before exec, ahead of the user switch and landlock. The namespace
holds only loopback and the datapath's listeners: upstream ports, VIPs, the
resolver, and an egress proxy at `127.0.0.1:1080` exported as `HTTP_PROXY` /
`HTTPS_PROXY` / `ALL_PROXY`. A tool that ignores the proxy has no route out,
so its only way off the namespace is a connection the datapath judges, and
every destination is checked against the
[host guard](network-handlers.md#the-host-guard) before policy: the node
itself, link-local (the cloud metadata service), and other allocations'
listeners are refused whatever the job's policy says. A task running as root
can leave the namespace with `setns`, so `gated` contains non-root tasks
only. The full model, the fail-closed rules, and the limits (no raw UDP, no
ICMP, no inbound to undeclared ports, Linux only) are in
[Network Handlers](network-handlers.md#gated-mode).

## Capability Summary

| Capability | Supported |
| --- | --- |
| KV env injection | ✅ |
| KV file injection | ✅ |
| Identity env / file delivery | ✅ / ✅ |
| Signal delivery | ✅ |
| Graceful stop (`stop { signal, killAfter }`) | ✅ |
| Memory / CPU limits, exclusive cores | ✅ Linux with `resourceEnforcement.enabled` (see [docs/resource-enforcement.md](resource-enforcement.md)) |
| Exit reporting / restart policy | ✅ (see [Task Supervision](task-supervision.md)) |
| Artifacts | ✅ (File/Tree + interpolation + env) |
| Exec / interactive PTY | ✅ / ✅, host-level (see [docs/exec-and-actions.md](exec-and-actions.md)) |

Exec runs a sibling host process with the task's env and working directory.
Because that is a process on the host (even under landlock), it carries the
`host` ACL flag. To exec into an `exec` task, you need an `exec:write` rule
with `host = true`.

## Notes

- Landlock features are Linux-only and conditional.
- Health checks run in the worker's health monitor, outside the exec plugin.
- Process exit: the worker's task supervisor reaps the child and applies the
  task's `restart` policy. See [Task Supervision](task-supervision.md).
- Resource limits: with `resourceEnforcement.enabled` on the worker, `exec`
  is a governed runtime. The worker creates the allocation's cgroup inside
  the node-wide `legion/` hierarchy and writes the task's `resources` into it
  as limits (`memory.max`, `cpu.weight`, `cpuset.cpus`). The plugin then
  spawns the process into it. Exec and container tasks share this hierarchy,
  so a core reserved by either is exclusive against both. See
  [docs/resource-enforcement.md](resource-enforcement.md).

  Setting `config.cgroup` still joins an existing cgroup and takes priority,
  which opts the task out of the Legion hierarchy. Legion can't promise
  exclusive cores inside a cgroup it doesn't manage, so declaring `cores`
  together with an explicit `cgroup` is rejected at submit. `user`,
  `landlock`, and `oom_score_adj` work with the worker-owned cgroup as before.
  The join is one more privileged `pre_exec` step, run before the switch to
  the task user.

