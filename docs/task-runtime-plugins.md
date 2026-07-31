# Task Runtime Plugins

This document explains how task-runtime plugins integrate with workers.

## Selecting a runtime

Every task names the runtime that runs it. The runtime is the pluggable
component responsible for starting, supervising, signalling, and reaping the
workload:

```jsonnet
tasks: {
  web: {
    runtime: 'container',
    config: { image: 'nginx:1.27' },
  },
  migrate: {
    runtime: 'exec',
    lifecycle: 'prestart',
    config: { command: ['./migrate.sh'] },
  },
}
```

`runtime` is required on every task. Its value is resolved against the worker's
task-plugin registry, so it is either a built-in (`exec`, `container`, `noop`)
or the directory name of a plugin under `plugins/tasks/<runtime>/`. The `config`
object is runtime-specific and is validated against that runtime's schema —
`legion job schema` emits the live per-runtime schemas under `taskRuntimes`.

> **Renamed from `kind`.** This field used to be called `kind`. There is no
> alias: a jobspec that still says `kind` is rejected at submit with an error
> naming the replacement. Editing existing jobspecs is a mechanical
> `kind:` → `runtime:` substitution on task blocks. Health checks keep their own
> `kind` field (`http`, `tcp`, `script`) — that is a different axis and was not
> renamed.

## Plugin Contract

Task runtimes implement `TaskPlugin` (`src/task_plugins/mod.rs`).

Lifecycle hooks:

- `schema()` / `validate_config()`
- `setup()`
- `readiness()`
- `prepare()`
- `start()`
- `stop()` / `stop_with()`
- `destroy()`

The worker calls `stop_with(allocation, task, instance, ctx)`, where `ctx` is
a `StopContext` carrying the [stop cause](graceful-stop.md), the signal to
deliver, and the deadline after which the allocation is killed.

`stop_with` is the **notice**, not the kill. Deliver `ctx.signal` and return
promptly, reporting whether a notice actually went out:

- `Ok(true)` — a notice was delivered and the process is still running. The
  worker holds the allocation in its terminating set and calls `destroy()`
  when the process exits or the deadline passes, whichever comes first.
- `Ok(false)` — this runtime could not signal (no tracked process, no
  graceful path). The worker tears the allocation down immediately, exactly
  as it did before graceful stop existed.

**Never block for the grace window.** `stop_with` runs under the worker's
task-manager mutex, which the heartbeat, the self-fence watchdog, health
reporting and log maintenance all need; holding it for 30 seconds would let
the manager declare the node lost and reschedule everything on it.

The default implementation signals the tracked host process group, which is
right for any runtime whose model is "one process we spawned". Override it
when your runtime has its own shutdown protocol (the `container` runtime
signals the container's init; the `sandbox` runtime declines the notice
outright, because there is no in-guest agent to relay it to).

`StopContext` is `#[non_exhaustive]`, so it can gain fields without breaking
call sites.

Signals:

- `signal(allocation, task, instance, signal)` — deliver a Unix signal by
  name, outside the stop ladder (a workload-identity `changeMode: signal`
  refresh, for instance). The **default signals the tracked pid**, which is
  correct wherever the tracked process *is* the workload. Isolating runtimes
  override it to reach inside: the `container` runtime shells out to
  `<runtime> kill` / `container kill --signal`, because its tracked pid is the
  runtime CLI supervisor and signaling that would leave the workload running
  unsignaled. Pair the override with `supports_signal() -> true`, which is
  what admission gates `changeMode: signal` identities on.


### Plugin ABI version (required)

`plugins/tasks/` is loaded through a bare `extern "C" fn() -> Box<dyn
TaskPlugin>`, which carries no type information: a dylib built against an
older trait layout would load happily and then call the wrong vtable slots.
Every task plugin must therefore declare which ABI generation it was built
against, beside its `get` constructor:

```rust
legion::declare_task_plugin_abi!();

#[no_mangle]
pub extern "C" fn get() -> Box<dyn TaskPlugin> { Box::new(MyPlugin) }
```

A plugin that declares nothing, or a mismatched generation, is **refused at
load** with a message naming the rebuild — rather than silently corrupting.
The current generation is `2` (graceful stop: `stop_with` returns `R<bool>`
and `StopContext` carries a signal and a deadline). Generation `1` was the
trait as of stop causes.

Capability declarations (each has a default, so an existing plugin keeps
compiling):

- `supports_kv_env_injection()`, `supports_kv_file_injection()`,
  `supports_identity_env()`, `supports_identity_file()`, `supports_signal()`,
  `supports_artifacts()`, `supported_volume_modes()`, `exec_support()`
- `file_delivery_root()` — **default `None`**, meaning the task shares the
  host filesystem, so a declared file destination (`kvInjections.files[].path`,
  identity `file.path`) is already a real host path. Return `Some(root)` when
  your runtime mounts the task directory somewhere else inside an isolated
  filesystem (the `container` runtime returns `/legion/task`); admission then
  requires every declared destination to resolve under `root`, and the worker
  translates them onto the host task directory for you.
- `resource_enforcement()` — how the runtime participates in the node's CPU and
  memory accounting. See below.
- `supervises_process()` — **default `false`.** Return `true` when the task's
  execution is a host process tracked in `TaskInstance.child`/`pid`. The
  worker's task supervisor then polls it for exit, reaps it, and applies the
  task's `restart` policy (see [Task Supervision](task-supervision.md)). Leave
  it `false` when your runtime tracks no process: a `batch` task of such a
  runtime is reported complete at start instead of waiting forever for an exit
  that cannot be observed.

Per-task state container, passed `&mut` to every hook:

- `TaskInstance` with:
  - arbitrary plugin `state` JSON
  - optional child process handle
  - optional PID
  - allocation and task directories, injected env, staged artifacts, log dir
  - `volumes` — the worker-mounted volumes for this allocation (see
    [docs/volumes.md](volumes.md)): a directory to bind in filesystem mode, a
    device or disk image to attach in block mode. Persistent data belongs
    here, never under `alloc_dir` — runtimes consume volumes, they do not
    invent storage
  - `resource_assignment` — the node's CPU/memory decision for this
    allocation, filled in before `prepare()` (see below). `None` when nothing
    is governed

`TaskInstance` was previously named `TaskRuntime`. It was renamed when the task
axis took the word `runtime`, so that a hook signature no longer has two
different "runtimes" in it. Out-of-tree plugins must rename the type at their
call sites; the fields and semantics are unchanged.

## Resource enforcement

CPU and memory enforcement is a **worker-core subsystem** — CPU
fingerprinting, the exclusive-core allocator, the share/reserve cgroup
partition, persistence, restart recovery, release on teardown — and runtimes
consume it through one defaulted trait method. Worker core owns everything
hard, so an adopting runtime never touches accounting. The user-facing model
(what `memory`, `cores`, and `cpu` mean, the requirements, the failure modes)
is in [docs/resource-enforcement.md](resource-enforcement.md).

### The three modes

```rust
fn resource_enforcement(&self) -> ResourceEnforcement {
    ResourceEnforcement::Unsupported   // the default
}
```

- **`Unsupported`** — not governed. Worker core computes no assignment and
  touches no cgroup for your tasks, and their `resources` stay
  scheduling-only. This is the default, which is why a plugin compiled before
  this method existed behaves exactly as it does today.
- **`WorkerCgroup`** — for a runtime whose task **is a host process you
  spawn**. Worker core creates the cgroup, writes its limits, and you spawn
  into it.
- **`RuntimeApplied`** — for a runtime that **confines its own workload**: the
  `container` runtime writes the assignment into the generated OCI spec; a
  microVM runtime would pin vCPU threads and size the guest. Worker core
  computes and accounts, but touches no cgroup file for that task.

The question that decides between the last two: *does my task run as a host
process the worker can spawn into a cgroup?*

### What you receive

```rust
pub struct ResourceAssignment {
    pub slice: Slice,                    // Share | Reserve
    /// This task's cgroup, relative to /sys/fs/cgroup —
    /// `legion/share/legion-<alloc>` or `legion/reserve/legion-<alloc>`.
    pub cgroup_path: PathBuf,
    /// Exclusively assigned logical CPUs (Reserve only).
    pub cores: Option<Vec<u32>>,
    /// Proportional weight as an OCI `cpu.shares` value (shares == declared MHz).
    pub cpu_weight: Option<u64>,
    /// Hard memory ceiling from declared `memory` (MiB).
    pub memory_limit_bytes: Option<u64>,
}
```

It arrives on `instance.resource_assignment` before `prepare()`, at the same
lifecycle point volumes are mounted — a task must never observe its limits
arriving after it started. Every field is `Option` because enforcement is
**declared-only**: a job that declared nothing is governed (it lives in the
hierarchy, so another task's reserved cores are off-limits to it) but carries
no limits of its own.

### Adopting `WorkerCgroup`

Two lines:

```rust
fn resource_enforcement(&self) -> ResourceEnforcement {
    ResourceEnforcement::WorkerCgroup
}

// …and at spawn, before the process execs:
join_assignment_cgroup(&mut cmd, instance)?;
```

`join_assignment_cgroup` writes the pid into the target cgroup from a
`pre_exec` hook, so there is no window in which the process runs outside its
limits. It is a **no-op when nothing is governed** (enforcement off, a
non-Linux worker, an `Unsupported` runtime), so call it unconditionally. A
runtime that needs the join at a specific point in its own privileged
sequence — as `exec` does, before dropping to the task user — can read
`instance.resource_assignment` and place the write itself.

What worker core promises to do on your behalf:

- fingerprint the node's usable CPUs and allocate exclusive cores;
- create and maintain both slice cgroups and their cpuset masks;
- create your task's cgroup and write its limits (`WorkerCgroup` only);
- persist the assignment and rebuild the allocator on worker restart;
- release the reservation and remove the cgroup on stop, destroy, or failure;
- read `memory.events` on exit and annotate an OOM-killed failure with the
  limit that fired.

### Adopting `RuntimeApplied`

Return the mode and apply `instance.resource_assignment` your own way. The
`container` runtime maps it into the OCI spec — `cgroupsPath` from
`cgroup_path`, `linux.resources.memory.limit`/`.swap` from
`memory_limit_bytes`, `cpu.cpus` from `cores`, `cpu.shares` from `cpu_weight`
— and the OCI runtime creates the cgroup at that path, which places it inside
the worker's hierarchy so the share mask governs it.

Worker core will not touch a cgroup file for your task, so anything the
assignment describes and you do not apply simply is not enforced.

## Loading and Resolution

`TaskPluginRegistry` behavior:

- Caches plugins per `runtime`.
- Loads built-ins for `exec`, `container`, and `sandbox` directly.
- Loads other runtimes dynamically from plugin dir.

Dynamic plugin requirements:

- `cdylib`
- exported symbol `get() -> Box<dyn TaskPlugin>`
- location under `plugins/tasks/<runtime>/...`

## Worker Integration

Worker task manager flow:

1. Load built-ins at startup (`exec`, `container`, `sandbox`, optional `noop`).
2. On allocation start:
   - resolve plugin by `task.runtime`
   - run `prepare` then `start`
3. On reconciliation stop:
   - run `stop` then `destroy`
4. On failure handling:
   - worker records failure and calls plugin `destroy` for cleanup
5. Persist allocation/task/PID state to local worker file.

Health checks and restart policies are managed outside the plugin — the worker's
health monitor for check failures and its task supervisor for process exits (see
[Task Supervision](task-supervision.md)) — but plugins control the actual runtime
start/stop behavior.

## Built-in and Bundled Runtimes

See per-runtime docs:

- `docs/task-runtime-exec.md`
- `docs/task-runtime-container.md`
- `docs/task-runtime-sandbox.md`
- `docs/task-runtime-noop.md`
