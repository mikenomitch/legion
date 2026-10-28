# Task Supervision: Exits, Restarts, Limits, and Hardening

This page covers what Legion does when a task's process exits or misbehaves
(health checks are covered elsewhere). It explains how Legion observes an exit,
when it restarts the task, and when a batch task counts as finished. It also
covers how declared resources become kernel limits and how to lock down a host
process.

- [Watching the process](#watching-the-process)
- [The `restart` block](#the-restart-block)
- [Batch tasks and real completion](#batch-tasks-and-real-completion)
- [Resource enforcement (cgroups)](#resource-enforcement-cgroups)
- [Graceful stop](#graceful-stop)
- [Process hardening](#process-hardening)
- [Diagnostics](#diagnostics)
- [Limitations](#limitations)

---

## Watching the process

Every worker runs a task supervisor. Once a second, it polls each running
allocation's process, reaps it when it has exited, and records how it exited:
an exit code, or the signal that killed it.

Whether a task is supervised depends on its runtime:

| Runtime | Supervised | The tracked process |
| --- | --- | --- |
| `exec` | ✅ | the task process itself |
| `container` | ✅ | the container runtime child |
| `sandbox` | ✅ | the VMM (its exit is the guest's exit) |
| `noop` | ❌ | no process exists |
| third-party plugins | ❌ unless the plugin opts in | — |

A runtime that tracks no process can't report an exit. See
[Batch tasks](#batch-tasks-and-real-completion) for what that means for
completion.

Supervision is separate from health checks ([Jobs, Tasks, and
Scheduling](jobs-tasks-and-scheduling.md#health-checks-restarting-and-rescheduling)),
which probe a task that is still running. You don't need a health check to
notice that a task died. The two paths share one restart budget, so a flapping
task gets `onUnhealthy.restartCount` local restarts in total across both.

## The `restart` block

```nickel
{
  name = "api",
  runtime = "exec",
  lifecycle = "service",
  config = { command = ["/usr/local/bin/api"] },

  restart = {
    # always | onFailure | never
    policy = "always",
    # Wait before relaunching. Defaults to onUnhealthy.restartDelay.
    delay = "5s",
    # Exit codes counted as success. Default [0].
    successExitCodes = [0],
  },

  # The shared budget: how many local restarts, over what window, before the
  # allocation is failed and the manager moves it elsewhere.
  onUnhealthy = {
    restartCount = 3, restartPeriod = "30m", restartDelay = "30s",
    moveCount = 3, movePeriod = "30m", moveDelay = "30s",
  },
}
```

Defaults when you omit the `restart` block, or declare one without a
`policy` (for example only `delay`):

| Lifecycle | Default policy | Why |
| --- | --- | --- |
| `service`, `sidecar` | `always` | a service that exits is not doing its job, whatever its exit code |
| `batch` | `onFailure` | a batch task is supposed to exit 0 |

What happens on an exit:

| Situation | Outcome |
| --- | --- |
| policy wants a restart, budget available | wait `delay`, relaunch in place |
| policy wants a restart, budget exhausted | allocation fails → manager reschedules it after a backoff that doubles per consecutive failure, capped at 30 minutes (`move_*` budget, see [Manager-side move logic](jobs-tasks-and-scheduling.md#manager-side-move-logic)) → job fails when `moveCount` is set and spent |
| policy does not want a restart, exit was a success | allocation is complete |
| policy does not want a restart, exit was not a success | allocation fails, as above |

While a replacement waits out its backoff, nothing places it, whichever eval
runs for the job. Each deferral emits an `allocation.reschedule_deferred`
event. `legion instance get` (for a stateful instance) and `legion job info`
say which attempt failed, why, and when the next one is. `legion job run
<spec> --force-reschedule` retries every waiting replacement now, and
`legion instance start <id>` retries one stateful instance. The backoff
doubles from `moveDelay` up to 30 minutes and starts over once a
replacement survives `movePeriod`. Raise `moveDelay` to back off harder,
and set `moveCount` to fail the job instead of retrying forever.

A process killed by a signal has no exit code and never counts as a success.
With `policy = "never"`, a `SIGKILL` is a failure.

Restarts happen in place on the same node. The allocation keeps its id, its
directory, its staged artifacts, and its volumes. The work moves to another
node only after the local budget is spent.

The worker learns each task's `lifecycle` and `restart` block from the
manager's dispatch (`TaskAllocation.task_lifecycle` and `task_restart` in
`proto/legion.proto`), so the policy above is the one the jobspec declared.

An exit that is not a success carries the last three lines the process wrote
to stderr (at most 400 characters, from the current run only) into the
allocation's status message and, when it fails the allocation, into the
failure reason and the `allocation.failed` event:

```text
exited with status 1; last stderr: Error: failed to pull image: 429 Too Many Requests
```

Worker logs record the exit status only, never task output.

Health checks stand aside while the process is down: between an exit and the
supervisor's relaunch, failed checks are not counted, so an allocation whose
process keeps dying fails with its exit status and stderr rather than with
`tcp check failed: Connection refused`. After a relaunch the checks start
over, boot grace included.

## Batch tasks and real completion

A batch task's status comes from its process. Until the worker reports an
exit, the task is `Running`. A successful exit makes it `Success`, and a failing
one makes it `Failed`.

This means dependency conditions reflect what the task actually did:

```nickel
{ task = "migrate", condition = "success" }   # migrate exited 0
{ task = "migrate", condition = "complete" }  # migrate finished, either way
```

Know two things before you upgrade:

- A batch task that never exits blocks its dependents in later scheduling
  passes. Earlier releases didn't block them. If a "batch" task is really a
  daemon, set `lifecycle = "service"`.
- Runtimes that can't report exits (`noop`, and third-party plugins that don't
  opt in) report their batch tasks as complete at start. Earlier releases
  assumed this for every runtime. The worker now says so in the allocation's
  status message.

The worker reports the exit on its next heartbeat, and keeps reporting it
until a manager records it. That heartbeat can land on any manager. When the
manager can't record the exit yet (it hasn't applied the placement, or the
write can't reach the raft leader), it asks the worker to send the report
again on the next beat. A report from a node that doesn't own the allocation
is ignored. The worker stops re-sending once it no longer holds the
allocation. If a `success` dependent stays held after its dependency exited,
look in the manager logs for `asking the worker to report it again`.

Failure reports (a task that crashed, failed a health check, or failed
staging) travel the same way. When the manager can't read the allocation or
record the failure (the write can't reach the raft leader, say), it asks the
worker to send the report again (`HeartbeatReply.retry_failed_allocations`),
and the allocation is moved once the report lands. A report for an
allocation the manager has no row for, or has already removed, is not asked
for again. Before this, such a report was logged and lost, and the
allocation stayed placed on a node that had already torn it down.

A completed allocation keeps its row and its slot, and Legion never dispatches
it to a worker again. It no longer counts against the node's resources or
holds its ports, so other work can use them. Batch tasks with a `schedule` still re-run on their
schedule.

Version skew: exit reporting arrived with protocol version 2. You upgrade
managers before workers ([Upgrades](upgrades.md)). In between, a manager may
place batch tasks on older workers that never report an exit. Those tasks get
`Success` at placement, as in earlier releases. The manager decides this per
node from the worker's advertised protocol version. Once you upgrade the
workers, new placements report real exits.

## Resource enforcement (cgroups)

By default, a task's `resources` only affect scheduling. They decide where the
task fits, but they don't limit what it consumes. To turn them into kernel
limits, enable enforcement per worker:

```nickel
# configs/workers/<type>.ncl
{
  workerType = "default",
  resourceEnforcement = {
    enabled = true,
    cgroupParent = "legion",   # hierarchy root under /sys/fs/cgroup
  },
}
```

With this on, a Linux worker partitions the node into `legion/share` and
`legion/reserve` cgroups. It places every governed task (both `exec` and
`container`) into that hierarchy:

| Job spec | Kernel meaning |
| --- | --- |
| `resources.memory` (MiB) | a hard `memory.max`, with swap pinned equal to it |
| `resources.cores` (whole CPUs) | exclusive pinned CPUs (`cpuset.cpus`), removed from every other governed task's set |
| `resources.cpu` (MHz, alias `cpus`) | proportional `cpu.weight` under contention |

Enforcement is on by default, so declare `resources.memory` as what the task
really needs: a task that uses more is OOM-killed. Set
`resourceEnforcement.enabled = false` on a worker type to treat `resources` as
scheduling estimates only. A node that can't enforce (no cgroup v2, no write
access to its cgroup parent, no usable CPU fingerprint) logs a warning at
startup and runs tasks unconfined. Legion won't refuse to run a workload
because it can't enforce limits.

An explicit `config.cgroup` on an `exec` task opts out. It tells Legion to join
a cgroup you already created and limited, so Legion doesn't manage one
alongside it.

[Resource Enforcement](resource-enforcement.md) documents the full model: the
share/reserve partition, the core allocator, the validation rules (`cores` must
be whole, and you set either `cores` or `cpu`), OOM attribution, `cgroup.kill`
teardown, the failure modes, and the interface a task runtime adopts.

## Graceful stop

To stop a task, Legion sends a signal and waits before it kills the process.
Configure this per task in the `stop` block:

```nickel
task = {
  name = "db",
  runtime = "exec",
  config = { command = ["/usr/bin/postgres"] },
  stop = { signal = "SIGINT", killAfter = "90s" },   # defaults: SIGTERM, 30s
}
```

Stopping happens in two phases, which keeps the grace wait off the heartbeat
path. First, `reconcile_allocations` sends the signal and returns. Then the task
supervisor's tick (the loop described on this page) reaps whatever has exited
or timed out. A task that Legion stopped on purpose doesn't count against the
`restart` policy, because it didn't crash.

[Graceful stop](graceful-stop.md) covers the signal ladder, the per-runtime
support table, the stop notice file, and the stop causes. The Linux `container`
runtime's default signal chain (jobspec → the image's own `STOPSIGNAL` →
`SIGTERM`) is in
[Task Runtime: container](task-runtime-container.md#graceful-stop).

## Process hardening

You can lock down an `exec` task further. These settings apply on Linux.
Other platforms parse and validate them, then ignore them with a warning.

```nickel
config = {
  command = ["/usr/local/bin/api"],
  user = "app",
  hardening = {
    noNewPrivileges = true,            # prctl(PR_SET_NO_NEW_PRIVS)
    privateTmp = true,                 # own mount namespace + tmpfs /tmp
    capabilities = { drop = ["all"] },  # bounding-set drops; names or "all"
    umask = "0027",
    nice = 5,
  },
}
```

- `capabilities.drop` takes capability names with or without the `CAP_` prefix
  (`CAP_NET_RAW`, `net_raw`), case-insensitively, or the single entry `all`.
  Unknown names are rejected at submit by `legion job validate`.
- `privateTmp` and capability drops need privileges the task must not keep,
  so they run before Legion switches to `user`. The order is: join cgroup →
  private `/tmp` → drop capabilities → switch user → `no_new_privs` → `umask`
  and `nice` → Landlock → `exec`.
- A worker without `CAP_SYS_ADMIN` or `CAP_SETPCAP` can't do the privileged
  steps. In that case the task fails to start instead of running with less
  confinement than you asked for.
- Landlock ([Task Runtime: exec](task-runtime-exec.md)) is applied last and
  composes with all of the above.

## Diagnostics

- `legion job info <job>` and the UI show the allocation's last status message:
  `exited with status 2`, `killed by signal 9`, `exited with status 1; last
  stderr: ...; restarting`.
- `legion job logs <job> --follow` shows the task's output across restarts.
  The allocation directory and its logs survive an in-place restart.
- The event stream ([Event Stream](event-stream.md)) carries
  `allocation.terminated` (with `success`, the message, and the task/job ids)
  for terminal outcomes, and the existing `allocation.failed` for failures that
  go on to be rescheduled.
- Worker logs record every restart decision with the exit status and the delay.

## Limitations

- No socket activation, readiness notification, or watchdog. Legion probes
  readiness with `boot` and `health` checks. A slow starter needs a boot check
  and a grace window instead of an `sd_notify`-style "I am ready" signal. This
  isn't planned.
- No PID, IPC, or UTS namespaces and no seccomp filters for `exec`.
  `privateTmp` gives a mount namespace only. A PID namespace would need a
  double-fork, which would change how the worker tracks the task's pid.
- Exit status is unknown after a worker restart. An allocation re-adopted by
  pid has no parent to reap it. When it later exits, the worker only sees that
  the process is gone. That never counts as a success, so a `never`-policy task
  in this state ends as a failure.
- Re-adoption checks identity, not just liveness. The worker persists each
  task's pid together with the process start time, and after a restart it
  re-adopts a pid only when the live process with that pid started at the
  recorded time. After a host reboot the old pids belong to unrelated
  processes, so those allocations are dropped instead of adopted. The next
  heartbeat's desired set still lists them, so the worker starts them fresh. State written
  by an older worker has no start time, so its first restart after the upgrade
  falls back to a liveness check.
- A restart resets only the process. Health-check state and the restart budget
  are per allocation, not per process.
- The stderr lines a failed exit carries are the task's own output, copied
  into its status message and failure event. A task that prints a secret to
  stderr as it dies shows it to whoever can read the job's events, the same
  readers who can already see it with `legion job logs`. Runtimes that capture
  no logs (`noop`, third-party plugins without capture) carry none.
- Enforcement is per worker, not per pool or per job. A cluster with mixed
  enforcement is a set of workers configured differently. The scheduler doesn't
  prefer an enforcing node for a task that sets limits.
- `resources.disk` isn't enforced. Only memory and CPU are.
