# Host Process Supervision Plan

> **Judgement calls:** [../calls/host-process-supervision-plan_calls.md](../calls/host-process-supervision-plan_calls.md)

**Status: IN EXECUTION — phases 1–3 shipped (2026-07-30).** Process reaping and
exit-status semantics, worker-owned cgroups, and the `exec` hardening block are
in `main`; the public page is
[`docs/task-supervision.md`](../../docs/task-supervision.md). Phase 4
(activation and readiness) is written down as *possible*, not committed. PID 1 /
init duties are an explicit non-goal.

**Deliberately deferred**, and now tracked in [`../TODO.md`](../TODO.md):

- PID/IPC/UTS namespaces and seccomp for `exec` (a PID namespace needs a
  double-fork, which changes the pid the worker tracks — see
  [calls entry 10](../calls/host-process-supervision-plan_calls.md)).
- Privileged runtime tests for the cgroup and hardening paths. CI runs the
  derivation, parsing, and validation tests plus the unconfined fallback; the
  paths that need root, cgroup v2 delegation, `CAP_SYS_ADMIN`, and `CAP_SETPCAP`
  are exercised by hand against a Linux worker container
  ([`../development.md`](../development.md)), not in CI.
- `resources.disk` enforcement, and scheduler preference for enforcing nodes.

Background and the gap analysis that produced this plan:
[../systemd-and-chef-analysis.md](../systemd-and-chef-analysis.md) §§2–5.

## 1. Problem statement

Legion never notices a task process exiting.

- Nothing calls `waitpid`/`try_wait` on a task child while it runs. The only
  `try_wait` in the tree belongs to interactive exec sessions
  (`src/worker/exec.rs:211`).
- A task with no `checks` is marked healthy once and never re-examined:
  `spawn_health_monitor` short-circuits with `healthy (no checks configured)`
  (`src/worker/task_health.rs:54`).
- `reconcile_allocations` compares desired-vs-tracked **id sets**
  (`src/worker/task_manager.rs:523`); a dead process still has an `instances`
  entry, so the worker believes it is running, and `is_allocation_active` is
  literally `instances.contains_key` (`:612`).
- A batch task's `Success` is derived from its **lifecycle at planning time** —
  `task_status_for_lifecycle`: `TaskLifecycle::Batch => TaskStatusState::Success`
  (`src/storage/store.rs:6455`). Exit codes never enter the picture, so
  `dependsOn: { condition: "success" }` means *"was placed"*.
- Exited children are never reaped, so a long-lived worker accumulates zombies
  for every task that dies.

What a user has to do today: attach a health check to every task, even one whose
only failure mode is "the process died", and accept that a batch task that exits
1 still unblocks its dependents and still reports success.

Two adjacent gaps make host processes second-class in ways containers are not:

- `resources` is scheduling arithmetic only. `exec`'s `cgroup` field *joins* an
  already-existing cgroup v2 path (`src/task_plugins/exec.rs:472-481`); Legion
  never creates a cgroup and never writes a limit. Capacity is an honour system
  on `exec` nodes.
- Isolation for `exec` is `user` + `oom_score_adj` + Landlock paths. No
  `no_new_privs`, no capability bounding, no private `/tmp`, no `umask`/`nice`.

## 2. Prior art

**systemd** is the reference for the local half. `Restart=` (`no`, `on-success`,
`on-failure`, `on-abnormal`, `on-abort`, `always`), `RestartSec=`,
`SuccessExitStatus=`, `StartLimitIntervalSec=`/`StartLimitBurst=` for the "stop
flapping" budget, and `Type=` for what counts as started. Resource control is
cgroup v2 with delegation (`MemoryMax=`, `CPUQuota=`, `IOWeight=`); sandboxing is
`NoNewPrivileges=`, `CapabilityBoundingSet=`, `PrivateTmp=`, `ProtectSystem=`,
seccomp filters. We take the restart vocabulary, the failure budget, and the
cgroup/hardening field names; we reject `Type=`/socket activation for now
(phase 4) and everything init-shaped.

**Nomad** is the reference for the distributed half. Its `restart` block
(`attempts`, `interval`, `delay`, `mode = "delay"|"fail"`) is per-task-group and
local; when it is exhausted the allocation fails and `reschedule` moves it to
another node. Legion already has that two-tier shape in `on_unhealthy`
(`restart_*` then `move_*`), and Nomad's lesson is that both tiers must share
*one* budget or a flapping task gets double the restarts it was promised. Nomad
also reports real exit codes per task state, which is what makes its batch
dispatch usable.

**Kubernetes** gives `restartPolicy: Always|OnFailure|Never` at pod level, and
its `resources.limits` are cgroup limits, not scheduling hints — the distinction
Legion is missing. K8s also shows the failure mode we want to avoid: `limits`
defaulting on for everyone turns a bad memory estimate into an OOM kill in
production, which is why enforcement here is opt-in per worker.

## 3. Jobs to be done

**Job author / app developer**
- When my service crashes at 3am, I want it started again automatically without
  me having to invent a health check that proves it is alive, so a segfault is a
  blip instead of an outage.
- When I run a one-off task that fails, I want everything waiting on it to
  *stop*, so a broken migration does not roll out on top of a database that was
  never migrated.
- When I say a task gets 512 MiB, I want that to be true on every runtime, so I
  can reason about a node the same way whether I ship a binary or a container.

**Cluster operator**
- When one badly behaved binary starts eating the box, I want the kernel to stop
  it, so its neighbours survive.
- When I upgrade, I want the new supervision behavior to arrive without
  rewriting every existing jobspec, and without a surprise OOM wave.

**Security / platform admin**
- When I let a team run host processes, I want to strip their privileges to what
  they actually need, so a compromised binary cannot escalate.

**On-call responder**
- When a task keeps dying, I want to see *how* it died — exit code or signal,
  how many times, and whether the restart budget is spent — so I know whether to
  roll back or page the owner.

### User scenarios

1. A Go service is deployed with `runtime: "exec"` and no checks. It panics. The
   worker notices within a second, restarts it after `restart.delay`, and after
   the third crash inside `restart_period` gives up locally and lets the manager
   reschedule it elsewhere. `legion job info` shows `exited with status 2`.
2. A nightly batch job exits 1. The task is marked failed, `job wait` returns
   failure, dependents with `condition: "success"` are not planned, and the
   event stream carries `allocation.terminated` with the exit code.
3. An operator turns on `resourceEnforcement` in the worker config. A task
   declaring 512 MiB now runs in `…/legion/alloc-<id>` with `memory.max=512Mi`
   and `cpu.max` derived from `resources.cpus`.

## 4. Goals and non-goals

**Goals**
- Observe every task process exit on the worker; reap it; record status.
- A `restart` policy in the jobspec, evaluated from exit status, sharing one
  budget with the existing health-check restart path.
- Real terminal states for batch tasks, driven by exit codes, propagated to
  task status and DAG conditions.
- Worker-owned cgroup v2 for `exec`, applying `resources` as real limits, opt-in
  per worker.
- An `exec` `hardening` block: `noNewPrivileges`, capability bounding-set drop,
  `privateTmp`, `umask`, `nice`.

**Non-goals**
- PID 1 / init: early-boot mounts, initrd, udev, logind, ordered shutdown.
- Socket activation, `sd_notify`, `WatchdogSec` (phase 4, uncommitted).
- seccomp filters, PID/IPC/UTS namespaces (deferred; the PID namespace needs a
  double-fork that would change how the worker tracks pids).
- Boot integration (`legion install`), standalone/manager-less mode.
- cgroup accounting **feedback** into the scheduler (using observed usage to
  size future placements).

## 5. Background — what exists today

- `TaskInstance` carries `child: Option<Child>` and `pid: Option<u32>`
  (`src/task_plugins/mod.rs`). `exec`, `container`, and `sandbox` all set both
  (`exec.rs:416`, `container.rs:518`, `sandbox.rs:971`); `noop` sets neither.
- `reap_task_process` (`src/task_plugins/mod.rs:117`) kills by child handle or
  by pid — the shared "one host process we spawned" teardown.
- `WorkerTaskManager` owns `instances`, `allocations`, `task_configs`,
  `allocation_health`, `failed`, and persists a `PersistedAllocation` list;
  `restore_allocations` (`:983`) re-adopts live pids and drops dead ones.
- `record_failure` (`:405`) destroys the instance, unmounts volumes, cleans the
  alloc dir, and queues a `FailedAllocationReport` for the heartbeat.
- The health monitor owns a **local** `restart_history` and calls
  `can_restart` against `on_unhealthy.restart_count/restart_period`
  (`src/worker/task_health.rs:262`), then `restart_allocation`.
- Heartbeat carries `allocation_ids`, `failed_allocations`, `allocation_health`,
  `volume_reports`; the manager's handler is `heartbeat`
  (`src/manager/server.rs:4548`) and `build_desired_allocations` decides what the
  worker should be running.
- `update_task_statuses_for_allocations_apply` (`src/storage/store.rs:6491`)
  stamps task status from lifecycle at placement.
- Node registration records `protocolVersion` in the node's dynamic attrs
  (`src/manager/server.rs:4715`); `PROTOCOL_VERSION` lives in
  `src/constants.rs`. Upgrade order is managers first, then workers
  (`docs/upgrades.md`).

## 6. Proposed design

### 6.1 The `restart` block (new task field)

```jsonnet
{
  name: "api",
  runtime: "exec",
  lifecycle: "service",
  restart: {
    // always | onFailure | never
    // Default: "always" for service/sidecar, "onFailure" for batch.
    policy: "always",
    // Wait before relaunching. Defaults to on_unhealthy.restart_delay.
    delay: "5s",
    // Exit codes treated as success. Default [0].
    successExitCodes: [0],
  },
  // Unchanged: the budget both restart paths share.
  on_unhealthy: { restart_count: 3, restart_period: "30m", restart_delay: "30s",
                  move_count: 3, move_period: "30m", move_delay: "30s" },
}
```

Exit → decision table:

| Situation | Outcome |
| --- | --- |
| `policy: never` | terminal: success reported if the code is in `successExitCodes`, failure otherwise |
| `policy: onFailure`, success code | terminal success |
| restart wanted, budget available | sleep `delay`, relaunch in place, health reset to `restarting` |
| restart wanted, budget exhausted | `record_failure` → existing move/reschedule tier → job failed when the move budget is spent |

The budget is `on_unhealthy.restart_count` within `restart_period` and is
**shared** with the health-check path: restart history moves from the health
monitor's local `Vec<Instant>` into `WorkerTaskManager`, keyed by allocation.

### 6.2 Exit observation

A supervisor loop in the worker daemon (`src/worker/task_supervisor.rs`) ticks
once a second and asks the task manager for exits. Per tracked instance:

- `child.try_wait()` when the worker owns the handle — this also reaps.
- else `pid_is_running(pid)` for allocations re-adopted after a worker restart
  (status unknown; reported as an unknown-status exit).
- Runtimes that track neither are skipped. `TaskPlugin::supervises_process()`
  (default `false`, overridden `true` by `exec`/`container`/`sandbox`) says which
  runtimes participate, so a third-party plugin cannot be starved by a poller
  that will never see an exit.

Terminated allocations stay in the worker's maps, flagged terminal: they keep
appearing in `allocation_ids` (so the manager does not re-dispatch them into the
same node) and are never polled or relaunched again.

### 6.3 Terminal reporting to the manager

New heartbeat field and message:

```proto
// HeartbeatRequest
repeated TerminatedAllocation terminated_allocations = 6;

message TerminatedAllocation {
  int64 allocation_id = 1;   // allocation that reached a terminal state
  bool success = 2;          // exit code in successExitCodes
  optional int32 exit_code = 3;
  optional int32 signal = 4;
  string message = 5;        // "exited with status 1", "killed by signal 9"
}
```

Failures keep flowing through `failed_allocations` — the existing move/reschedule
machinery already does the right thing with them. `terminated_allocations` carries
only *successful terminal* outcomes plus the unsupervised-runtime case.

Manager handling (`handle_terminated_allocations`): mark the allocation row
`complete`, set the task's status to `Success`, and emit an
`allocation.terminated` event. The row is **not** deleted — deleting it would
leave the desired count unmet and re-run the batch task immediately.

### 6.4 Honest batch status, without breaking version skew

`task_status_for_lifecycle` stops claiming `Success` at placement. Batch tasks
start `Running` and become `Success` only when a worker reports a successful
termination.

Skew rule: managers upgrade first, so a new manager will place batch tasks on
old workers that never report terminations. The placement path therefore checks
the node's advertised `protocolVersion` (already in dynamic attrs) and keeps the
legacy optimistic `Success` for nodes below the new protocol version.
`PROTOCOL_VERSION` goes 1 → 2.

For a runtime that cannot be supervised (`noop`, third-party plugins), the
worker reports a successful termination immediately after a successful start of
a **batch** task — preserving today's semantics, but as an explicit statement
rather than an assumption.

### 6.5 Worker-owned cgroups (`exec`, Linux)

Worker config gains:

```jsonnet
{
  workerType: "linux",
  resourceEnforcement: {
    enabled: true,          // default false
    cgroupParent: "legion", // path under /sys/fs/cgroup
  },
}
```

When enabled, `exec` creates `/sys/fs/cgroup/<parent>/alloc-<id>-<task>/`, writes
`memory.max` from `resources.memory` (MiB) and `cpu.max` from `resources.cpus`
(`<quota> 100000`), moves the child in, and removes the directory on stop. An
explicit `config.cgroup` still means "join this cgroup, do not manage one" — the
escape hatch wins. Missing cgroup v2, missing permissions, or a parent without
`+memory +cpu` in `cgroup.subtree_control` logs a warning and starts the task
unconfined: enforcement must never be the reason a workload will not run.

The worker passes the setting to the in-process plugin through
`LEGION_RESOURCE_ENFORCEMENT` / `LEGION_CGROUP_PARENT`, the same channel
`sandbox` already uses for `LEGION_SANDBOX_KERNEL`.

### 6.6 `exec` hardening block (Linux)

```jsonnet
config: {
  command: ["/usr/local/bin/api"],
  user: "app",
  hardening: {
    noNewPrivileges: true,
    privateTmp: true,                       // mount namespace + private tmpfs /tmp
    capabilities: { drop: ["all"] },        // or ["CAP_NET_RAW", …]
    umask: "0027",
    nice: 5,
  },
},
```

All of it is applied in the child between fork and exec, in an order that
respects privilege: cgroup join → mount namespace + private `/tmp` → capability
bounding-set drops → `setgid`/`setuid` → `no_new_privs` → `umask`/`nice` →
`exec`. Because the sequence must own the privilege drop, the `user` switch moves
into the same pre-exec path when hardening or managed cgroups are configured.
Non-Linux platforms validate the block and warn that it is ignored.

## 7. Proto, struct, config, and persistence changes

- `proto/legion.proto`: `TerminatedAllocation` message; `HeartbeatRequest.terminated_allocations`.
- `src/constants.rs`: `PROTOCOL_VERSION` 1 → 2.
- `src/structs/job.rs`: `TaskConfig.restart: Option<TaskRestartPolicy>` with
  `policy`, `delay`, `success_exit_codes`; defaults resolved per lifecycle.
- `src/structs/config.rs`: worker `resourceEnforcement { enabled, cgroupParent }`.
- `src/task_plugins/exec.rs`: `hardening` config block; managed cgroup lifecycle.
- `src/task_plugins/mod.rs`: `TaskPlugin::supervises_process()`.
- `src/worker/task_supervisor.rs`: new module.
- `src/worker/task_manager.rs`: terminal map, shared restart budget, exit polling.
- `src/storage/store.rs`: terminal allocation status, protocol-aware batch status.
- No SQL migration: `allocation.status` already exists and is written today.
- `configs/jobs/maximal.jsonnet`: `restart` block and `hardening` block.

## 8. Implementation phases

**Phase 1 — reaping and exit semantics.** Supervisor loop, `supervises_process`,
`restart` block, shared budget, terminal reporting, honest batch status with the
protocol-version fallback. Independently valuable: it fixes silent dead
allocations and lying batch success with no new configuration required.

**Phase 2 — worker-owned cgroups.** Opt-in `resourceEnforcement`, managed cgroup
create/apply/join/remove for `exec`. Independently valuable: `resources` becomes
enforcement on `exec` nodes.

**Phase 3 — `exec` hardening.** The `hardening` block and the privilege-ordered
pre-exec sequence. Independently valuable: host processes get the isolation
floor containers already have.

**Phase 4 — activation and readiness (possible, not committed).** An
`sd_notify`-compatible readiness socket so a slow starter can say "ready"
instead of being probed; then `WatchdogSec`-style liveness pings; then socket
activation via pre-bound fds tied to `network.ports`. Each is separable. The
open question is whether readiness-by-notification earns its keep next to boot
checks, which already work — hence: possible, not planned.

## 9. Testing plan

Phase 1
- Unit: exit decision table (policy × exit status × budget) as a pure function.
- Unit: `poll_task_exits` sees a spawned `true` exit and reports its status; skips
  instances with no child and no pid; skips non-supervising runtimes.
- Unit: shared restart budget refuses the (N+1)th restart inside the period
  regardless of which path asks.
- Unit: `task_status_for_lifecycle` legacy fallback by node protocol version.
- Integration: batch task exiting non-zero fails the task and leaves
  `condition: "success"` dependents unplanned; exiting zero marks it Success.
- Regression: a task with health checks still restarts on check failure, and the
  two paths do not double-spend the budget.

Phase 2
- Unit: cgroup path/limit derivation from `resources` (`memory.max`, `cpu.max`).
- Unit: explicit `config.cgroup` wins over managed enforcement.
- Runtime tests need Linux + cgroup v2 + delegation, so they are manual against a
  worker container; CI exercises the derivation and the unconfined fallback.

Phase 3
- Unit: hardening config parse/validate, including bad umask and unknown
  capability names, through `legion job validate`'s code path.
- Unit: capability name → bounding-set constant table, and `all` expansion.
- Runtime tests need root (`CAP_SYS_ADMIN`, `CAP_SETPCAP`), so they are manual;
  on other platforms the plugin warns and runs.

## 10. Rollout and backward compatibility

- **Jobspecs unchanged** keep working. The new default (`restart.policy:
  "always"` for services) turns "silently dead" into "restarted", which is the
  behavior users already believed they had.
- **Batch DAGs**: `condition: "complete"`/`"success"` now mean what they say.
  A batch task that never exits (a bug in the job) will now block its dependents
  where it previously did not — called out in the docs.
- **Version skew**: managers first, as always. New manager + old worker keeps the
  legacy optimistic batch status via the node's `protocolVersion`. Old manager +
  new worker is already rejected by the handshake.
- **Enforcement is opt-in.** No cluster gets new OOM kills by upgrading;
  `resourceEnforcement.enabled` is the switch, and it is per worker.
- **Hardening is opt-in**, per task, and ignored with a warning off Linux.

## 11. Open questions

- Should `restart.policy` also accept `onSuccess`/`onAbnormal` (systemd's fuller
  matrix)? *Lean: no* — three values cover the real cases and the fourth is a
  `successExitCodes` list away.
- Should the move tier count restarts too, so a task that flaps across nodes
  fails faster? *Lean: later, with data* — the move budget already bounds it.
- Should `resourceEnforcement` become default-on once the fleet has run with it?
  *Lean: yes, after one release of evidence*, and only for tasks that declare
  `resources` explicitly.

## 12. Acceptance criteria

- **Phase 1**: killing a task process (no checks configured) results in a restart
  within ~1s + `delay`; the third crash inside the period fails the allocation and
  the manager reschedules it; a batch task exiting 1 reports failed and blocks
  `success` dependents; no zombie processes accumulate on the worker.
- **Phase 2**: with `resourceEnforcement.enabled`, an `exec` task declaring 64
  MiB is killed by the kernel when it allocates 256 MiB, and its cgroup directory
  is gone after the allocation stops.
- **Phase 3**: a task with `capabilities: { drop: ["all"] }` cannot use a
  privileged capability; `noNewPrivileges` blocks setuid escalation;
  `privateTmp` gives the task a `/tmp` no other task can see.
