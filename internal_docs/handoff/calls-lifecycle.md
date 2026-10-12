# Judgement calls: lifecycle track (spot-check P0-2, P0-3, P1-1, P1-4, P1-9a, P2-7a, P2-8)

Entries are ranked by project impact, highest first; the number is the rank.

## 1. Lifecycle and restart ride the dispatch as two discrete fields, not a serialized TaskConfig

The worker learns a task's lifecycle and restart policy from two new
`TaskAllocation` fields: `task_lifecycle` (`"service" | "batch" | "sidecar"`,
always set) and `task_restart` (the declared `restart` block as JSON, empty when
undeclared). `task_from_allocation` folds them into the TaskConfig the
supervisor already reads, so the existing supervisor, `decide()` and the
manager's `terminated_allocations` path work unchanged.

**Alternative.** Serialize the whole task config onto the payload (the
spot-check report's second suggestion), so no task-level block can ever again
be silently missing on the worker.

**Trade-off.** Every future task-level block the worker needs still has to be
added by hand (this is the third time: `task_stop`, `task_suspend`, now these).
A whole-config field would have closed the class of bug, at the cost of
shipping manager-only fields (constraints, `dependsOn`, `schedule`, rollout,
affinities) to every node on every heartbeat, duplicating `task_config` /
`task_checks` / ..., and making the worker's view of a task two competing
sources.

**Why.** Simplicity and performance: the payload stays "what the worker acts
on", the per-heartbeat desired set does not grow by the jobspec, and the
convention the message already documents (empty = undeclared, byte-identical
for jobs that do not use a block) still holds. The proto comment on
`task_stop` already warns that a block not on the message does not exist on the
worker; the new comments say the same and name the consequence.

Surface: `proto/legion.proto` `TaskAllocation.task_lifecycle = 26`,
`task_restart = 27`.

## 2. A `restart` block without `policy` keeps the lifecycle's default policy

`TaskRestartConfig.policy` became `Option<RestartPolicy>`. A block that only
tunes `delay` or `successExitCodes` now keeps `onFailure` on a batch task;
before, the field defaulted to `always`, so

```nickel
{ name = "fetch", lifecycle = "batch", restart = { delay = "5s" } }
```

quietly became a batch task that restarts on success, exactly the P0-2
symptom, once lifecycle and restart started reaching the worker.

**Alternative.** Keep "a declared block means `always` unless it says
otherwise" and document it.

**Trade-off.** A behavior change for anyone who relied on the old reading (a
service task is unaffected: its default is `always` either way). Stored configs
deserialize unchanged; only the effective policy of a policy-less block on a
batch task moves. No shim, per AGENTS.md.

**Why.** Simplicity/ease: the struct comment, the docs table and
`maximal.ncl` all said "defaults per lifecycle"; the old serde default
contradicted them, and fixing P0-2 would have made the contradiction bite.

## 3. A failed exit carries the task's last stderr lines into its status, failure reason and event

`TaskExit` gained `stderr_tail`: for an exit the task does not count as success,
the last 3 non-empty stderr lines (at most 400 characters, from the current run
only: the worker records the `stderr.log` length at each launch). It shows up
as `exited with status 1; last stderr: Error: ... 429 Too Many Requests` in the
allocation status, the `allocation.failed` reason and the event. A health check
that fails an allocation whose process is still running appends the same tail
(`health check failed: tcp check failed: Connection refused; last stderr: ...`).
Worker log lines keep `describe()` (status only).

**Alternatives.** (a) Point at the logs: `exited with status 1; see legion job
logs`. (b) Kubernetes' `terminationMessagePolicy: FallbackToLogsOnError`, an
opt-in per container.

**Trade-off.** Task output is copied into events, so a task that prints a secret
to stderr as it dies exposes it to event readers (the same people who can read
its logs). Bounded and only on failure, but not zero. Documented as a
limitation in docs/task-supervision.md.

**Why.** Ease: the spot check's whole complaint was that the cause existed only
in task stderr. Opt-in would leave the default experience unchanged, which is
the failure the finding describes; a pointer still sends every user on the
same hunt. Keeping it out of worker tracing follows the "never trace task
output/secrets" rule.

## 4. Health checks stand aside while the process is down instead of racing the supervisor

The health monitor now asks the task manager whether the allocation's process
has exited (`process_exit`) before counting a result. While it is down, failed
checks are ignored; after the supervisor relaunches it, the monitor resets its
failure windows and boot grace, as it does after its own restart.

**Alternative.** Let both paths keep racing and make whichever fails the
allocation merge in the other's facts (exit status into a health failure, check
results into an exit failure).

**Trade-off.** There is still a window of up to one supervisor tick (1s)
between the process dying and `poll_task_exits` noticing, during which a check
can count; with default thresholds (3 failures after a 1s boot grace) it rarely
matters. A check that would have caught a hung-but-alive process is unaffected.

**Why.** Simplicity: one owner per fact. The supervisor owns exits and already
applies the restart policy with the shared budget; two paths restarting the
same dead process also double-spent that budget.

## 5. A gated start creates its own namespace rather than failing or waiting for a heartbeat

Root cause of P1-4: the manager pushes new allocations straight to the worker
(`Worker.PostWork`), and that path never ran `apply_net_capture`, which only
the heartbeat calls. Whether the namespace existed at start depended on whether
a heartbeat happened to land before staging finished. Now
`task_plugins::ensure_gated_namespace` (used by the `exec` and `container`
starts) creates it when missing; `netd::netns::create` is serialized with a
process-wide lock so the heartbeat and a start cannot stack two bind mounts;
`record_failure` releases a namespace for a failed gated allocation, so one
created by a pushed start that failed before any heartbeat listed it does not
leak.

**Alternatives.** (a) Treat "not ready" as a retryable start: hold the
allocation and retry on the next heartbeat. (b) Have PostWork call the
heartbeat's capture setup.

**Trade-off.** The datapath listeners inside the namespace are still bound by
the next heartbeat, so a pushed gated task has loopback only for up to one
heartbeat interval (documented in docs/network-handlers.md). (a) would have
avoided that window but added a new "deferred start" state to the launch
machinery, and (b) would have put datapath setup on an RPC handler owned by
another track.

**Why.** Simplicity: creation is idempotent and cheap, the start is where the
namespace is needed, and the failure mode goes from "allocation failed and
rescheduled (and P1-3 then strands it)" to a short window without egress.

## 6. The staging thread enters the caller's tokio runtime; the monitor refuses to start without one

`spawn_staging_thread`, `spawn_provisional_staging_thread` and edge boot's
`provisional-launch` thread capture `Handle::try_current()` where they are
spawned (always inside the worker runtime: a `spawn_blocking` closure or an RPC
handler) and enter it after staging, which gives them the same context a
`spawn_blocking` closure has. `spawn_health_monitor` itself now uses
`Handle::try_current()`: without a runtime it logs an error and sets the
allocation's status to `health checks could not start (worker bug: no async
runtime)` instead of panicking its thread.

**Alternative.** Hand the "start the monitor" step back to the runtime through a
channel, or run staging on `spawn_blocking`.

**Trade-off.** Entering a runtime on an OS thread is implicit context a reader
has to know about; the helper pair (`current_runtime` / `enter_runtime`) and its
doc comment carry that. `spawn_blocking` for staging was rejected because a
slow pull would pin a blocking-pool thread for minutes.

**Why.** Smallest change that makes every tokio call reachable from those
threads correct (the audit found the same bug in the edge-boot launch thread),
plus a guard so the next missed call site is visible instead of silent.

## 7. The resource governor takes its configuration explicitly

`WorkerTaskManager` now starts with a disabled governor and the daemon calls
`configure_resource_enforcement(&config)` (via the new
`NodeResourceGovernor::from_settings(enabled, cgroup_parent)`) before restoring
or starting anything. The env is still exported for the task runtimes that read
it. The Linux "Resource enforcement enabled" line is now logged only by the
governor, once it has checked the node.

**Alternative.** Move `apply_resource_enforcement_env` above the task manager's
construction and keep `from_env()` in the constructor.

**Trade-off.** Two sources of the same setting remain (explicit for worker
core, env for runtimes); a runtime and the governor could in principle
disagree. They are written from the same config in the same function.

**Why.** An ordering dependency is what broke this; the reorder would have
kept it. Passing the config makes the dependency a call the compiler sees.

## 8. The default plugin root is found next to the binary, and the build checkout only if it exists

`plugin_root_dir()` without `LEGION_PLUGIN_DIR` now tries
`<exe dir>/plugins`, `<exe dir>/../lib/legion/plugins`, then the build
checkout's `plugins/`, taking the first that exists (falling back to
`<exe dir>/plugins`). `validate_manifest` no longer logs its own error at
`ERROR` (`#[instrument(err)]` removed); callers that need a plugin still fail
with the path in the error.

**Alternatives.** The worker's data directory; or keep the build path and only
downgrade the log.

**Trade-off.** The bundled layout is a convention we now support but do not
ship a package for. A binary whose directory happens to contain an unrelated
`plugins/` would look there.

**Why.** Ease: an installed binary must not carry its build machine's path, and
the two layouts cover a tarball and an FHS install without a new knob; `cargo
run`/`cargo test` keep working because the checkout exists for them.

## 9. Exec sessions into `exec` tasks get the task's env, and stay in the host network namespace

`ExecTaskPlugin::exec` now layers `allocation_env` (`LEGION_ALLOC_ID`,
`LEGION_PORT_*`, `LEGION_ADDR_*`, upstreams) under the worker-injected and task
env, matching `start`. Raw exec and actions share this path, so both get it.
An exec session for a `network.mode = "gated"` exec task still runs in the
host namespace.

**Alternative.** Also `setns` exec sessions into the gated namespace.

**Trade-off.** Inside a gated exec task, `legion exec -- curl $LEGION_UPSTREAM_DB`
reaches the host, not the namespace's listeners. Noted in
docs/exec-and-actions.md and as a TODO.

**Why.** Scope: P2-8 is the env; namespace entry for exec sessions needs
`ExecOptions` to carry a namespace and the PTY spawner to honor it, which is a
change to a shared runtime interface.

## 10. A ratified `stage`-gated claim gets its health monitor

Found in the P0-3 audit: `edge::apply_verdict` discarded
`Ratified.start_health_monitor`, so a `stage`-gated provisional start that was
ratified never got a health monitor and was never reported healthy. It now
spawns one after releasing the task-manager lock.

**Alternative.** Leave it to the edge-boot owner as a separate item.

**Trade-off.** Touches edge boot, which the spot check did not cover, with no
end-to-end test of its own here (the existing ratify unit tests assert the
field is set).

**Why.** It is the same "health monitor never starts" bug class this track
owns, and a one-line consumer of a field that existed for exactly this.
