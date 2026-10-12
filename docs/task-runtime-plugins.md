# Task Runtime Plugins

This page explains how task-runtime plugins integrate with workers. Read it if
you are choosing a runtime for a task or writing your own runtime plugin.

## Selecting a runtime

Every task names the runtime that runs it. The runtime is the pluggable
component that starts, supervises, signals, and reaps the workload:

```nickel
tasks = {
  web = {
    runtime = "container",
    config = { image = "nginx:1.27" },
  },
  migrate = {
    runtime = "exec",
    lifecycle = "prestart",
    config = { command = ["./migrate.sh"] },
  },
}
```

`runtime` is required on every task. The worker resolves it against its
task-plugin registry. The value is either a built-in (`exec`, `container`,
`sandbox`) or the directory name of a dynamic plugin under
`plugins/tasks/<runtime>/` (such as the bundled `noop`). The
`config` object is runtime-specific and is validated against that runtime's
schema. `legion job schema` emits the live per-runtime schemas under
`taskRuntimes`.

> **Renamed from `kind`.** This field used to be called `kind`. There is no
> alias. A jobspec that still says `kind` is rejected at submit with an error
> naming the replacement. To update existing jobspecs, replace `kind =` with
> `runtime =` on task blocks. Health checks keep their own `kind` field
> (`http`, `tcp`, `script`). That field means something else and was not
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

`stop_with` sends the stop notice. It does not kill the task. Deliver
`ctx.signal` and return promptly, reporting whether a notice went out:

- `Ok(true)`: a notice was delivered and the process is still running. The
  worker holds the allocation in its terminating set. It calls `destroy()`
  when the process exits or the deadline passes, whichever comes first.
- `Ok(false)`: this runtime could not signal (no tracked process, no
  graceful path). The worker tears the allocation down immediately.

**Never block for the grace window.** `stop_with` runs under the worker's
task-manager mutex. The heartbeat, the self-fence watchdog, health reporting,
and log maintenance all need that mutex. Holding it for 30 seconds could let
the manager declare the node lost and reschedule everything on it.

`prepare()` and `start()` are different. For allocations the manager
dispatches, the worker calls them on a launch thread with the task-manager
mutex released, so a slow image pull or volume attach does not stall
heartbeats. Volume mounts and `consume_artifact()` run on that thread too.
The allocation is registered first, as a pid-less placeholder like one that
is still staging artifacts, so the worker never launches it twice. If the
manager stops it, or it is self-fenced, before `start()` returns, the launch
is cancelled. The worker calls `destroy()` on whatever `start()` produced
once it returns. Two consequences for plugin authors:

- `prepare()` and `start()` may run at the same time as calls for other
  allocations, including `stop_with()` and `destroy()`. Keep shared plugin
  state behind your own synchronization.
- A launch that is cancelled is destroyed without a stop notice or grace
  window. It never reached the running set.

Provisional starts ([edge boot](edge-boot.md)) and `stop_with()` /
`destroy()` still run under the mutex. A runtime whose `destroy()` shells out
should keep it short.

The default implementation signals the tracked host process group. That works
for any runtime that spawns one process. Override it when your runtime has its
own shutdown protocol. For example, the `container` runtime signals the
container's init. The `sandbox` runtime declines the notice, because there is
no in-guest agent to relay it to.

`StopContext` is `#[non_exhaustive]`, so it can gain fields without breaking
call sites.

Signals:

- `signal(allocation, task, instance, signal)`: deliver a Unix signal by
  name, outside the stop ladder. A workload-identity `changeMode = "signal"`
  refresh uses this, for example. The default signals the tracked pid, which
  works when the tracked process is the workload. Isolating runtimes override
  it to reach inside. The `container` runtime shells out to `<runtime> kill` /
  `container kill --signal`, because its tracked pid is the runtime CLI
  supervisor. Signaling that pid would leave the workload running and
  unsignaled. Pair the override with `supports_signal() -> true`. Admission
  checks that method before it accepts `changeMode = "signal"` identities.


### Plugin ABI version (required)

Plugins in `plugins/tasks/` are loaded through a bare `extern "C" fn() ->
Box<dyn TaskPlugin>`, which carries no type information. A dylib built against
an older trait layout would load without error and then call the wrong vtable
slots. So every task plugin must declare which ABI generation it was built
against, next to its `get` constructor:

```rust
legion::declare_task_plugin_abi!();

#[no_mangle]
pub extern "C" fn get() -> Box<dyn TaskPlugin> { Box::new(MyPlugin) }
```

The macro also exports the plugin's build fingerprint (the `rustc` version,
the `legion` version, and shared type layouts), and the worker refuses a
plugin built differently from itself. See
[plugin-system.md](plugin-system.md#build-fingerprint-required-for-dynamic-plugins).

The worker refuses to load a plugin that declares nothing or declares a
mismatched generation. The error message tells you to rebuild. The current
generation is `2` (graceful stop: `stop_with` returns `R<bool>` and
`StopContext` carries a signal and a deadline). Generation `1` was the trait as
of stop causes.

Capability declarations (each has a default, so an existing plugin keeps
compiling):

- `supports_kv_env_injection()`, `supports_kv_file_injection()`,
  `supports_identity_env()`, `supports_identity_file()`, `supports_signal()`,
  `supports_artifacts()`, `supported_volume_modes()`, `exec_support()`
- `file_delivery_root()`: default `None`, meaning the task shares the host
  filesystem. A declared file destination (`kvInjections.files[].path`,
  identity `file.path`) is then already a host path. Return `Some(root)` when
  your runtime mounts the task directory somewhere else inside an isolated
  filesystem (the `container` runtime returns `/legion/task`). Admission then
  requires every declared destination to resolve under `root`, and the worker
  translates them onto the host task directory for you.
- `resource_enforcement()`: how the runtime takes part in the node's CPU and
  memory accounting. See below.
- `supervises_process()`: default `false`. Return `true` when the task runs
  as a host process tracked in `TaskInstance.child`/`pid`. The worker's task
  supervisor then polls it for exit, reaps it, and applies the task's
  `restart` policy (see [Task Supervision](task-supervision.md)). Leave it
  `false` when your runtime tracks no process. A `batch` task on such a
  runtime is reported complete at start, instead of waiting forever for an
  exit that can't be observed.

Per-task state container, passed `&mut` to every hook:

- `TaskInstance` with:
  - arbitrary plugin `state` JSON
  - optional child process handle
  - optional PID
  - allocation and task directories, injected env, staged artifacts, log dir
  - `volumes`: the worker-mounted volumes for this allocation (see
    [docs/volumes.md](volumes.md)). In filesystem mode this is a directory to
    bind. In block mode it is a device or disk image to attach. Persistent
    data belongs here, never under `alloc_dir`. Runtimes use volumes and
    should not create their own storage
  - `resource_assignment`: the node's CPU/memory decision for this
    allocation, filled in before `prepare()` (see below). `None` when nothing
    is governed

`TaskInstance` was previously named `TaskRuntime`. It was renamed when the task
field took the name `runtime`, so a hook signature no longer has two different
"runtimes" in it. Out-of-tree plugins must rename the type at their call sites.
The fields and semantics are unchanged.

## Resource enforcement

CPU and memory enforcement is part of the worker core. The worker core handles
CPU fingerprinting, the exclusive-core allocator, the share/reserve cgroup
partition, persistence, restart recovery, and release on teardown. Runtimes
use it through one defaulted trait method, so a runtime never touches
accounting itself. The user-facing model (what `memory`, `cores`, and `cpu`
mean, the requirements, the failure modes) is in
[docs/resource-enforcement.md](resource-enforcement.md).

### The three modes

```rust
fn resource_enforcement(&self) -> ResourceEnforcement {
    ResourceEnforcement::Unsupported   // the default
}
```

- **`Unsupported`**: not governed. Worker core computes no assignment and
  touches no cgroup for your tasks, and their `resources` only affect
  scheduling. This is the default, so a plugin compiled before this method
  existed behaves as it did before.
- **`WorkerCgroup`**: for a runtime whose task is a host process you spawn.
  Worker core creates the cgroup and writes its limits, and you spawn into it.
- **`RuntimeApplied`**: for a runtime that confines its own workload. The
  `container` runtime writes the assignment into the generated OCI spec.
  Worker core computes and accounts, but touches no cgroup file for that task.

The built-in `sandbox` runtime uses `WorkerCgroup`: its VMM is a host process,
so it joins the cgroup like `exec` does.

If your process holds more than the workload (a VMM holds guest RAM plus its
own memory), override the ceiling worker core writes:

```rust
fn enforced_memory_limit_bytes(&self, task: &TaskConfig) -> Option<u64> {
    None // the default: `resources.memoryMax`, else `resources.memory`, is the ceiling
}
```

`sandbox` returns the guest size plus the worker's `sandbox.vmmOverheadMiB`.

If your runtime's own config also sizes the workload, say so at submit:

```rust
fn check_resources(&self, task: &TaskConfig) -> R<Vec<String>> {
    Ok(Vec::new()) // the default: nothing to add
}
```

An `Err` rejects the job, and each returned string is a warning on the
`job run` reply. `sandbox` uses it to warn when `config.memory_mib`
disagrees with the guest size `resources` imply.
Every governed task, whatever its mode, is also measured and reported as
usage ([resource-enforcement.md](resource-enforcement.md#measuring-usage)).

To choose between the last two, ask whether your task runs as a host process
the worker can spawn into a cgroup.

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
    /// Hard memory ceiling: declared `memoryMax`, else `memory` (MiB).
    pub memory_limit_bytes: Option<u64>,
    /// Protected memory (`memory.low`): the declared `memory` reservation.
    pub memory_low_bytes: Option<u64>,
}
```

It arrives on `instance.resource_assignment` before `prepare()`, at the same
lifecycle point where volumes are mounted. A task never sees its limits arrive
after it started. Every field is `Option` because enforcement applies only to
what the job declared. A job that declared nothing is still governed: it lives
in the hierarchy, so another task's reserved cores are off-limits to it. It
just has no limits of its own.

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
`pre_exec` hook, so the process never runs outside its limits. It does nothing
when nothing is governed (enforcement off, a non-Linux worker, an
`Unsupported` runtime), so call it unconditionally. Some runtimes need the
join at a specific point in their own privileged sequence. For example,
`exec` joins before dropping to the task user. Such a runtime can read
`instance.resource_assignment` and do the write itself.

Worker core does the following for you:

- Fingerprints the node's usable CPUs and allocates exclusive cores.
- Creates and maintains both slice cgroups and their cpuset masks.
- Creates your task's cgroup and writes its limits (`WorkerCgroup` only).
- Persists the assignment and rebuilds the allocator on worker restart.
- Releases the reservation and removes the cgroup on stop, destroy, or failure.
- Reads `memory.events` on exit and annotates an OOM-killed failure with the
  limit that fired.

### Adopting `RuntimeApplied`

Return the mode and apply `instance.resource_assignment` your own way. The
`container` runtime maps it into the OCI spec:

- `cgroupsPath` from `cgroup_path`
- `linux.resources.memory.limit`/`.swap` from `memory_limit_bytes`
- `cpu.cpus` from `cores`
- `cpu.shares` from `cpu_weight`

The OCI runtime creates the cgroup at that path. That places it inside the
worker's hierarchy, so the share mask governs it.

Worker core does not touch a cgroup file for your task. Anything in the
assignment that you do not apply is not enforced.

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

1. Load built-ins at startup (`exec`, `container`, `sandbox`), and the
   bundled `noop` dylib if it is present.
2. On allocation start:
   - resolve plugin by `task.runtime`
   - run `prepare` then `start`
3. On reconciliation stop:
   - write the stop notice, then call `stop_with`
   - if it returns `Ok(true)`, call `destroy` when the process exits or the
     deadline passes
   - if it returns `Ok(false)` or fails, call `destroy` immediately
4. On failure handling:
   - worker records failure and calls plugin `destroy` for cleanup
5. Persist allocation/task/PID state to local worker file.

Health checks and restart policies are managed outside the plugin. The worker's
health monitor handles check failures, and its task supervisor handles process
exits (see [Task Supervision](task-supervision.md)). Plugins control the
runtime's start and stop behavior.

## Built-in and Bundled Runtimes

See per-runtime docs:

- `docs/task-runtime-exec.md`
- `docs/task-runtime-container.md`
- `docs/task-runtime-sandbox.md`
- `docs/task-runtime-noop.md`
