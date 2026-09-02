# Task Supervision: Exits, Restarts, Limits, and Hardening

What Legion does when a task's **process** — as opposed to its health check —
misbehaves: how an exit is observed, when the task is restarted, when a batch
task is actually finished, how declared resources become kernel limits, and how
a host process can be locked down.

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

Every worker runs a **task supervisor**: once a second it polls each running
allocation's process, reaps it when it has exited, and records how it exited —
an exit code, or the signal that killed it.

Which runtimes participate is a property of the runtime, not the job:

| Runtime | Supervised | The tracked process |
| --- | --- | --- |
| `exec` | ✅ | the task process itself |
| `container` | ✅ | the container runtime child |
| `sandbox` | ✅ | the VMM (its exit is the guest's exit) |
| `noop` | ❌ | no process exists |
| third-party plugins | ❌ unless the plugin opts in | — |

A runtime that tracks no process cannot report an exit, and Legion does not
pretend otherwise: see [Batch tasks](#batch-tasks-and-real-completion) for what
that means for completion.

This is separate from **health checks** ([Jobs, Tasks, and
Scheduling](jobs-tasks-and-scheduling.md#health-checks-restarting-and-rescheduling)),
which probe a task that is still running. A task no longer needs a health check
just to notice that it died — but the two paths share one restart budget, so a
flapping task gets `on_unhealthy.restart_count` local restarts in total, not
that many per path.

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
    # Wait before relaunching. Defaults to on_unhealthy.restart_delay.
    delay = "5s",
    # Exit codes counted as success. Default [0].
    successExitCodes = [0],
  },

  # The shared budget: how many local restarts, over what window, before the
  # allocation is failed and the manager moves it elsewhere.
  on_unhealthy = {
    restart_count = 3, restart_period = "30m", restart_delay = "30s",
    move_count = 3, move_period = "30m", move_delay = "30s",
  },
}
```

**Defaults, when you write no `restart` block at all:**

| Lifecycle | Default policy | Why |
| --- | --- | --- |
| `service`, `sidecar` | `always` | a service that exits is not doing its job, whatever its exit code |
| `batch` | `onFailure` | exiting 0 is the *point* of a batch task |

What happens on an exit:

| Situation | Outcome |
| --- | --- |
| policy wants a restart, budget available | wait `delay`, relaunch in place |
| policy wants a restart, budget exhausted | allocation **fails** → manager reschedules it (`move_*` budget) → job fails when that is spent too |
| policy does not want a restart, exit was a success | allocation is **complete** |
| policy does not want a restart, exit was not a success | allocation **fails**, as above |

A process killed by a signal has no exit code and is never a success — so
`policy = "never"` plus a `SIGKILL` is a failure, not a completion.

Restarting is in place, on the same node, keeping the allocation id, its
directory, its staged artifacts, and its volumes. Only when the local budget is
spent does the work move.

## Batch tasks and real completion

A batch task's status now comes from the process, not from the plan. Before a
worker reports an exit the task is `Running`; a successful exit makes it
`Success`, a failing one `Failed`.

This is what makes dependencies mean what they say:

```nickel
{ task = "migrate", condition = "success" }   # migrate exited 0
{ task = "migrate", condition = "complete" }  # migrate finished, either way
```

Two consequences worth knowing before you upgrade:

- **A batch task that never exits now blocks its dependents** in later
  scheduling passes, where it previously did not. If a "batch" task is really a
  daemon, it should be `lifecycle = "service"`.
- **Runtimes that cannot report exits** (`noop`, third-party plugins that do not
  opt in) report their batch tasks as complete *at start*, which is exactly the
  optimistic answer Legion used to assume everywhere. The difference is that
  the worker now says so explicitly, and you can see it in the allocation's
  status message.

A completed allocation keeps its row and its slot; it is never re-dispatched to
a worker. Batch tasks with a `schedule` still re-run on their schedule.

**Version skew.** Exit reporting arrived with protocol version 2. Managers are
upgraded before workers ([Upgrades](upgrades.md)), so during the window a
manager places batch tasks on workers that will never report an exit; those
tasks keep the old optimistic `Success` at placement, decided per node from the
worker's advertised protocol version. Once the workers are upgraded, new
placements get the honest behavior.

## Resource enforcement (cgroups)

By default a task's `resources` are *scheduling* arithmetic: they decide where
the task fits, not what it may consume. Turn them into real limits per worker:

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
`legion/reserve` cgroups and places **every governed task** — `exec` *and*
`container` — into that hierarchy:

| Job spec | Kernel meaning |
| --- | --- |
| `resources.memory` (MiB) | a hard `memory.max`, with swap pinned equal to it |
| `resources.cores` (whole CPUs) | exclusive pinned CPUs (`cpuset.cpus`), removed from every other governed task's set |
| `resources.cpu` (MHz, alias `cpus`) | proportional `cpu.weight` under contention |

It is **off by default** on purpose: many jobs' `resources` were written as
scheduling estimates, and turning an estimate into `memory.max` on upgrade day
would OOM-kill workloads that were running fine. A node that opts in but
cannot enforce (no cgroup v2, no usable CPU fingerprint) warns at startup and
runs tasks unconfined — enforcement must not be the reason a workload will not
run.

An explicit `config.cgroup` on an `exec` task still opts out: it means "join
the cgroup I already made and limited", and Legion will not manage one
alongside it.

**The full model — the share/reserve partition, the core allocator, the
validation rules (`cores` must be whole; `cores` xor `cpu`), OOM attribution,
`cgroup.kill` teardown, the failure modes, and the interface a task runtime
adopts — is documented in
[Resource Enforcement](resource-enforcement.md).**

## Graceful stop

Stopping a task delivers a signal and waits before it kills, declared per task
in the `stop` block:

```nickel
task = {
  name = "db",
  runtime = "exec",
  config = { command = ["/usr/bin/postgres"] },
  stop = { signal = "SIGINT", killAfter = "90s" },   # defaults: SIGTERM, 30s
}
```

Stopping is two-phase, and that split is what keeps the grace wait off the
heartbeat path: `reconcile_allocations` signals and returns under the
task-manager lock, and the **task supervisor's tick** — the same loop this
page describes — reaps whatever has exited or timed out. A task Legion
deliberately stopped does not feed the `restart` policy: it did not crash.

**The ladder, the per-runtime support table, the stop notice file, and the
stop causes are documented in [Graceful stop](graceful-stop.md).** The Linux
`container` runtime's signal default chain (jobspec → the image's own
`STOPSIGNAL` → `SIGTERM`) is in
[Task Runtime: container](task-runtime-container.md#graceful-stop).

## Process hardening

An `exec` task can restrict itself further (Linux; parsed and validated
everywhere, ignored with a warning off Linux):

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
- `privateTmp` and capability drops need privileges the task must not keep, so
  they run **before** Legion switches to `user` — the order is: join cgroup →
  private `/tmp` → drop capabilities → switch user → `no_new_privs` → `umask`
  and `nice` → Landlock → `exec`. A worker without `CAP_SYS_ADMIN` /
  `CAP_SETPCAP` cannot do the privileged steps, and the task fails to start
  rather than starting less confined than you asked for.
- Landlock ([Task Runtime: exec](task-runtime-exec.md)) is applied last and
  composes with all of the above.

## Diagnostics

- `legion job info <job>` and the UI show the allocation's last status message:
  `exited with status 2`, `killed by signal 9`, `exited with status 1;
  restarting`.
- `legion job logs <job> --follow` still shows the task's own output across
  restarts; the allocation directory and its logs survive an in-place restart.
- The event stream ([Event Stream](event-stream.md)) carries
  `allocation.terminated` (with `success`, the message, and the task/job ids)
  for terminal outcomes, and the existing `allocation.failed` for failures that
  go on to be rescheduled.
- Worker logs record every restart decision with the exit status and the delay.

## Limitations

Named honestly, because the gaps are real:

- **No socket activation, no readiness notification, no watchdog.** Readiness is
  probed with `boot`/`health` checks; a slow starter needs a boot check and a
  grace window rather than an `sd_notify`-style "I am ready". Possible future
  work, not planned.
- **No PID/IPC/UTS namespaces and no seccomp filters** for `exec`. `privateTmp`
  gives a mount namespace only; a PID namespace requires a double-fork that
  would change how the worker tracks the task's pid.
- **Exit status is unknown after a worker restart.** An allocation re-adopted by
  pid has no parent to reap it, so when it later exits the worker sees only
  "the process is gone" — never a success, so a `never`-policy task in that
  state terminates as a failure rather than a completion.
- **A restart resets nothing but the process.** Health-check state and the
  restart budget are per allocation, not per process generation.
- **Enforcement is per worker, not per pool or per job.** A cluster with mixed
  enforcement is a fleet of workers configured differently, and nothing in the
  scheduler currently prefers an enforcing node for a task that wants limits.
- **`resources.disk` is not enforced**, only memory and CPU.
