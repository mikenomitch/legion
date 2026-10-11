use crate::manager::rollout_strategy::validate_rollout_strategy_name;
use crate::plugin_runtime::{plugin_paths, validate_manifest};
use crate::prelude::*;
use crate::structs::{
    Allocation, InstanceMode, JobConfig, RolloutReplacement, StopCause, TaskConfig,
    TaskDependencyCondition, TaskLifecycle,
};
use crate::util::plugin_root_dir;
use jsonschema::JSONSchema;
use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command};

pub mod checkpoint;
mod container;
mod exec;
pub mod exec_isolation;
mod exec_session;
pub mod guest_resume;
mod microvm;
mod oci_bundle;
mod oci_image;
pub mod resource_enforcement;
mod sandbox;
mod sandbox_checkpoint;
pub use sandbox::{sandbox_usage_source, vsock_socket_base_for, SandboxUsageSource};
pub use sandbox_checkpoint::{
    firecracker_needs_api_socket, sandbox_checkpoint_support, sandbox_vmm_attrs, IDENTITY_DRIVE_ID,
};
mod schema_docs;
mod vmm_api;

pub use checkpoint::{AfterCheckpoint, CheckpointDir, CheckpointQuery, CheckpointSupport};
pub use container::ContainerTaskPlugin;
pub use exec::ExecTaskPlugin;
pub use exec_session::{spawn_pipe, spawn_pty, ExecOptions, ExecOutputStreams, ExecSession};
pub use resource_enforcement::{
    join_assignment_cgroup, ResourceAssignment, ResourceEnforcement, Slice, CGROUP_ROOT,
};
pub use sandbox::SandboxTaskPlugin;
pub use schema_docs::{
    builtin_task_plugin_schemas, render_builtin_task_plugin_schemas_markdown,
    validate_builtin_task_plugin_schemas,
};

/// Generation of the `TaskPlugin` vtable that a dynamically loaded plugin
/// must have been compiled against.
///
/// `plugins/tasks/` is a public extension point loaded through a bare
/// `extern "C" fn() -> Box<dyn TaskPlugin>`, which carries no type
/// information: a dylib built against an older trait layout would load
/// happily and then call the wrong slots. Bumping this constant — and the
/// [`declare_task_plugin_abi!`] symbol every plugin exports — turns that
/// silent corruption into a load-time error naming the fix.
///
/// - `1` — the trait as of Phase 1a (`stop_with(..) -> R<()>`), which no
///   plugin ever needed to override because the cause was informational.
/// - `2` — graceful stop (Phase 1b): `stop_with` returns `R<bool>` ("a notice
///   was delivered; the process is still running") and [`StopContext`] gained
///   `signal`, `deadline` and `grace`.
/// - `3` — checkpoints (suspend plan Phase 3): `checkpoint_support`,
///   `checkpoint`, and `restore` joined the trait, and [`TaskInstance`]
///   gained `checkpoint_launch`.
pub const TASK_PLUGIN_ABI_VERSION: u32 = 3;

/// Symbol name the loader looks for. A plugin declares it with
/// [`declare_task_plugin_abi!`].
const TASK_PLUGIN_ABI_SYMBOL: &[u8] = b"legion_task_plugin_abi_version";

/// Export the task-plugin ABI generation this plugin was compiled against.
/// Every dylib under `plugins/tasks/<name>/` must invoke this once, next to
/// its `get` constructor.
///
/// ```ignore
/// legion::declare_task_plugin_abi!();
///
/// #[no_mangle]
/// pub extern "C" fn get() -> Box<dyn TaskPlugin> { Box::new(MyPlugin) }
/// ```
#[macro_export]
macro_rules! declare_task_plugin_abi {
    () => {
        #[no_mangle]
        pub extern "C" fn legion_task_plugin_abi_version() -> u32 {
            $crate::task_plugins::TASK_PLUGIN_ABI_VERSION
        }

        $crate::declare_plugin_abi!();
    };
}

pub fn spawn_task_process(cmd: &mut Command) -> R<Child> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        // Run task processes in their own session so they survive worker restarts
        // and to keep worker signal handling independent from task signal handling.
        unsafe {
            cmd.pre_exec(|| {
                unsafe extern "C" {
                    fn setsid() -> i32;
                }
                if setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    cmd.spawn().context("Failed to spawn task process")
}

/// Legion-provided environment for a task: allocation identity, any assigned
/// ports (`LEGION_PORT_*`, `LEGION_ADDR_*`, ...) and any declared upstreams
/// (`LEGION_UPSTREAM_*`, `LEGION_UPSTREAM_PORT_*`). Task plugins apply these
/// before user-declared env so user values win on key conflicts.
pub fn allocation_env(allocation: &Allocation, task: &TaskConfig) -> Vec<(String, String)> {
    let mut pairs = vec![
        ("LEGION_ALLOC_ID".to_string(), allocation.id.to_string()),
        ("LEGION_JOB_ID".to_string(), allocation.job_id.clone()),
        ("LEGION_TASK_NAME".to_string(), task.name.clone()),
    ];
    if let Some(network) = allocation.network.as_ref() {
        pairs.extend(network.env_pairs());
    }
    pairs
}

/// File name for a captured stdout stream within an allocation log directory.
pub const STDOUT_LOG_FILE: &str = "stdout.log";
/// File name for a captured stderr stream within an allocation log directory.
pub const STDERR_LOG_FILE: &str = "stderr.log";

/// Redirect a task process's stdout/stderr to per-allocation capture files.
///
/// When `instance.log_dir` is set (the worker threads an allocation log
/// directory through the instance), the directory is created and the command's
/// stdout/stderr are pointed at append-mode `stdout.log` / `stderr.log` files.
/// When no log directory is configured this is a no-op and the child inherits
/// the worker's standard streams, preserving prior behaviour.
pub fn configure_task_log_capture(cmd: &mut Command, instance: &TaskInstance) -> R<()> {
    let Some(log_dir) = instance.log_dir.as_ref() else {
        return Ok(());
    };
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("failed to create log dir {}", log_dir.display()))?;
    // Captured stdout/stderr can carry secrets; keep the allocation's log
    // directory unreadable to other tasks on the node. The capture files are
    // opened by the worker before spawn, so the task itself never needs to
    // enter this directory.
    crate::util::restrict_dir_to_owner(log_dir)?;

    let stdout = open_log_file(&log_dir.join(STDOUT_LOG_FILE))?;
    let stderr = open_log_file(&log_dir.join(STDERR_LOG_FILE))?;
    cmd.stdout(stdout);
    cmd.stderr(stderr);
    Ok(())
}

fn open_log_file(path: &std::path::Path) -> R<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open log file {}", path.display()))
}

/// Why — and how — a task is being stopped, handed to
/// [`TaskPlugin::stop_with`].
///
/// `stop_with` is the **notice**, not the kill: a runtime delivers `signal`
/// and returns promptly. The worker keeps the allocation in a terminating set
/// until the process actually exits, or until `deadline` passes and
/// [`TaskPlugin::destroy`] takes it out hard. A runtime must never block for
/// the grace window itself — `stop_with` runs under the task-manager mutex
/// that the heartbeat, the self-fence watchdog and health reporting all need
/// (see `internal_docs/plans/preemption-and-eviction-plan.md` § Phase 1b).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StopContext {
    /// Why the platform is stopping this allocation.
    pub cause: StopCause,
    /// The notice signal to deliver (`stop.signal`, default `SIGTERM`).
    pub signal: String,
    /// When the grace window closes. After this instant the worker's reaper
    /// hard-destroys whatever is left.
    pub deadline: std::time::Instant,
    /// How long the grace window is, from the moment the signal is sent.
    /// Carried alongside `deadline` because it is the value a runtime hands
    /// to a stop-with-timeout API (`runc kill` + wait, a container CLI's
    /// `--time`), where an absolute instant is not expressible.
    pub grace: std::time::Duration,
}

impl Default for StopContext {
    fn default() -> Self {
        Self::new(StopCause::default())
    }
}

impl StopContext {
    /// A context with the default ladder (`SIGTERM`, 30s), for callers that
    /// have no task config to hand — test helpers and the legacy `stop()`
    /// bridge.
    pub fn new(cause: StopCause) -> Self {
        Self::with_ladder(cause, "SIGTERM", std::time::Duration::from_secs(30))
    }

    /// A context carrying an explicit ladder, as the worker builds it from
    /// the task's `stop` block.
    pub fn with_ladder(cause: StopCause, signal: &str, grace: std::time::Duration) -> Self {
        Self {
            cause,
            signal: signal.to_string(),
            deadline: std::time::Instant::now() + grace,
            grace,
        }
    }

    /// Whether the grace window has closed.
    pub fn expired(&self) -> bool {
        std::time::Instant::now() >= self.deadline
    }
}

/// The portable signal numbers behind [`crate::structs::STOP_SIGNALS`].
/// Kept here rather than pulled from `libc` so the mapping is identical on
/// every platform the worker builds for, including the non-Unix builds where
/// signalling degrades to a hard kill.
pub(crate) fn stop_signal_number(signal: &str) -> Option<i32> {
    match signal {
        "SIGHUP" => Some(1),
        "SIGINT" => Some(2),
        "SIGQUIT" => Some(3),
        "SIGTERM" => Some(15),
        "SIGUSR1" => Some(10),
        "SIGUSR2" => Some(12),
        _ => None,
    }
}

/// Deliver a stop notice signal to a task's tracked process, without waiting.
///
/// Returns `true` when a signal was actually delivered. `false` means the
/// caller should fall back to a hard stop: there is no process to signal, the
/// platform has no signals, or the signal name is not one Legion delivers.
///
/// Unlike [`reap_task_process`] this leaves `instance.child` / `instance.pid`
/// in place — the process is still running, the worker's supervisor still
/// polls it for exit, and the reaper still needs the handle to kill it at the
/// deadline.
pub(crate) fn signal_task_process(
    allocation_id: i64,
    task_name: &str,
    instance: &TaskInstance,
    runtime: &str,
    signal: &str,
) -> bool {
    let Some(pid) = instance
        .child
        .as_ref()
        .map(|child| child.id())
        .or(instance.pid)
    else {
        return false;
    };
    let Some(number) = stop_signal_number(signal) else {
        warn!(
            allocation_id,
            task_name, runtime, signal, "Unknown stop signal; falling back to a hard stop"
        );
        return false;
    };

    #[cfg(unix)]
    {
        // Tasks are spawned into their own session (`setsid` in
        // `spawn_task_process`), so signalling the process GROUP reaches a
        // shell-wrapped task's children too — the common case where the
        // tracked pid is `sh -c` and the real workload is its child. A
        // negative pid is the POSIX spelling for "the group".
        let delivered = unsafe { libc::kill(-(pid as i32), number) } == 0
            || unsafe { libc::kill(pid as i32, number) } == 0;
        if delivered {
            info!(
                allocation_id,
                task_name, runtime, pid, signal, "Sent stop signal to task process"
            );
        } else {
            // ESRCH — already gone. That is a successful stop, and the
            // reaper will notice the exit on its next tick.
            debug!(
                allocation_id,
                task_name, runtime, pid, signal, "Task process was already gone when signalled"
            );
        }
        // Either way there is nothing left to hard-kill right now.
        true
    }
    #[cfg(not(unix))]
    {
        let _ = (allocation_id, task_name, runtime, number, pid);
        false
    }
}

/// Stops a task's tracked process: the child handle when this worker spawned
/// it, else the recorded pid (a worker restart restores a pid, not a `Child`).
///
/// Shared by every runtime whose execution model is "one host process we
/// spawned" — which, for the microVM runtimes, is the VMM itself: killing it
/// takes the guest with it.
pub(crate) fn reap_task_process(
    allocation_id: i64,
    task_name: &str,
    instance: &mut TaskInstance,
    runtime: &str,
) {
    // Kill the whole process group first, matching the notice signal in
    // `signal_task_process`. A shell-wrapped task (`sh -c server`) is tracked
    // by the shell's pid, so SIGKILLing only that pid would orphan the server
    // that ignored its SIGTERM, still holding its ports and volume.
    #[cfg(unix)]
    if let Some(pid) = instance
        .child
        .as_ref()
        .map(|child| child.id())
        .or(instance.pid)
    {
        // Every runtime that reaps here spawned its task with
        // `spawn_task_process`, which makes the task a group leader
        // (`setsid`). ESRCH (no such group) is fine: the group is gone, or
        // this pid never led one, and the per-pid kill below still runs.
        let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
    }
    if let Some(mut child) = instance.child.take() {
        info!(allocation_id, task_name, runtime, "Stopping task process");
        let _ = child.kill();
        let _ = child.wait();
        instance.pid = None;
    } else if let Some(pid) = instance.pid.take() {
        info!(
            allocation_id,
            task_name, runtime, pid, "Stopping task process by pid"
        );
        let pid = sysinfo::Pid::from_u32(pid);
        let mut system = sysinfo::System::new();
        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
        if let Some(process) = system.process(pid) {
            process.kill();
        }
    }
}

pub(crate) fn derived_schema<T: schemars::JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T))
        .expect("derived task plugin schema should serialize")
}

#[derive(Default)]
pub struct TaskInstance {
    pub state: Value,
    pub child: Option<Child>,
    pub pid: Option<u32>,
    /// Private per-allocation directory (`allocs/<alloc_id>/`), created by
    /// the worker before `prepare()` and removed on destroy.
    pub alloc_dir: Option<std::path::PathBuf>,
    /// Private task directory (`allocs/<alloc_id>/<task_name>/`). Artifacts
    /// stage here; the exec runtime's `cwd` defaults here.
    pub task_dir: Option<std::path::PathBuf>,
    /// Env vars injected by the worker core (`LEGION_ALLOC_DIR`,
    /// `LEGION_TASK_DIR`, `LEGION_ARTIFACT_<NAME>`, `LEGION_VOLUME_<NAME>`).
    /// Runtimes that run host processes apply these at start; runtimes with isolated
    /// filesystems apply the subset that means something inside the sandbox
    /// (the container and sandbox runtimes apply [`Self::volume_env`], whose
    /// values are already in-task destinations).
    pub extra_env: std::collections::BTreeMap<String, String>,
    /// Typed staged artifacts (artifact system plan §8), attached before
    /// `prepare()` so plugins consume payloads instead of guessing paths.
    pub staged_artifacts: Vec<crate::artifacts::StagedArtifact>,
    /// Directory where the task's captured stdout/stderr are written, when log
    /// capture is enabled by the worker. `None` leaves the child attached to
    /// the worker's standard streams.
    pub log_dir: Option<PathBuf>,
    /// Volumes the worker mounted for this allocation before `prepare()` (see
    /// docs/volumes.md). Runtimes with an isolated filesystem turn these into
    /// real mounts; host-process runtimes just read `LEGION_VOLUME_*` from
    /// [`Self::extra_env`], which the worker already filled in.
    pub volumes: Vec<MountedVolume>,
    /// What worker core decided for this allocation's CPU and memory (see
    /// [`ResourceAssignment`]), filled in before `prepare()` for every
    /// runtime whose [`TaskPlugin::resource_enforcement`] is not
    /// `Unsupported`.
    ///
    /// `None` means nothing is governed — enforcement off, a non-Linux
    /// worker, or an unported runtime — and a plugin that ignores the field
    /// entirely behaves exactly as it does today.
    pub resource_assignment: Option<ResourceAssignment>,
    /// Launch this allocation so it can be memory-checkpointed later.
    ///
    /// Set by worker core before `start` when the task's runtime reports
    /// [`CheckpointSupport::Memory`] and the job enables suspend or
    /// snapshots. A sandbox on Firecracker then boots with an API socket,
    /// and every memory-checkpointable sandbox reserves an identity disk
    /// (see [`sandbox_checkpoint`]). `false`, the default, launches exactly
    /// as before checkpoints existed.
    pub checkpoint_launch: bool,
}

impl TaskInstance {
    /// The `LEGION_VOLUME_<NAME>` / `LEGION_STORAGE_DIR` vars the worker
    /// injected. For runtimes with an isolated filesystem view the worker
    /// resolved these to in-task destinations (bind target, guest mount
    /// path), so — unlike `LEGION_ALLOC_DIR` and friends, which name host
    /// paths the task cannot see — they are safe to forward inside.
    pub fn volume_env(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.extra_env
            .iter()
            .filter(|(key, _)| key.starts_with("LEGION_VOLUME_") || *key == "LEGION_STORAGE_DIR")
            .map(|(key, value)| (key.clone(), value.clone()))
    }
}

/// One mounted volume, as a task runtime sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountedVolume {
    /// Declared volume name (`data`, `models`, …).
    pub name: String,
    /// How the driver handed it over: a directory or a block device.
    pub mode: crate::volumes::VolumeMode,
    /// Where it lives on this host. In filesystem mode the bind source; in
    /// block mode the device (or raw disk image) to attach.
    pub host_path: PathBuf,
    /// Where the task should see it — the bind destination for a container, the
    /// guest path for a microVM. `None` when the job declared no path, which
    /// only makes sense for runtimes without a filesystem view.
    pub path: Option<String>,
    /// Block mode: the filesystem the device carries, so a consumer knows what
    /// to mount inside a guest without probing.
    pub fs: Option<String>,
    /// True when this mount created the storage rather than reattaching it —
    /// the signal to run first-boot initialization.
    pub fresh: bool,
    pub read_only: bool,
}

/// Whether exec into a runtime means host-level (unsandboxed) access, used by
/// the manager to pick the `host` ACL flag *before* it proxies to the worker.
///
/// This is a static classification (it must not depend on the worker's OS or
/// on what that worker detects locally, which the manager cannot see). The
/// worker still checks
/// real availability via [`TaskPlugin::exec_support`] and errors if a runtime
/// cannot actually exec. Unknown/dynamic runtimes are treated as host-level so
/// they require the stronger `exec:write` + `host` grant by default.
pub fn runtime_is_host_level(runtime: &str) -> bool {
    match runtime {
        "container" => false,
        // A sandbox is a VM (or, in dev mode, a container): exec'ing into one
        // is never host access.
        "sandbox" => false,
        "exec" => true,
        _ => true,
    }
}

/// What a task runtime can do for `legion exec` / actions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecSupport {
    /// One-shot + streamed exec (piped stdio).
    pub exec: bool,
    /// Interactive PTY sessions (`--tty`).
    pub pty: bool,
    /// True when exec'ing means unsandboxed host access (drives the ACL
    /// `host` distinction, like Nomad's `alloc-node-exec`).
    pub host_level: bool,
}

pub trait TaskPlugin: Send + Sync {
    fn name(&self) -> &'static str;

    fn schema(&self) -> Value {
        json!({"type": "object"})
    }

    fn validate_config(&self, _config: &Value) -> R<()> {
        let schema = self.schema();
        validate_schema(&schema, _config)
    }

    fn setup(&self) -> R<()> {
        Ok(())
    }

    fn readiness(&self) -> Map<String, Value> {
        Map::new()
    }

    fn supports_kv_env_injection(&self) -> bool {
        false
    }

    fn supports_kv_file_injection(&self) -> bool {
        false
    }

    fn supports_live_restart(&self) -> bool {
        true
    }

    /// Whether this runtime can deliver a workload-identity token as an env var.
    fn supports_identity_env(&self) -> bool {
        false
    }

    /// Whether this runtime can deliver a workload-identity token as a file.
    fn supports_identity_file(&self) -> bool {
        false
    }

    /// Where this runtime's tasks see their task directory, when that is not
    /// the host path.
    ///
    /// `None` (the default) means the task shares the host filesystem, so a
    /// file-delivery destination is a host-absolute path and anything goes —
    /// the `exec` runtime's position. `Some(root)` means the worker mounts
    /// the task directory at `root` inside an isolated filesystem, and every
    /// declared destination (`kvInjections.files[].path`, identity
    /// `file.path`) must resolve under it, because nothing else is reachable
    /// from in there. Relative destinations resolve against `root`.
    ///
    /// Admission enforces the rule; the worker re-checks before joining a
    /// declared path onto the host task directory (the same two-layer stance
    /// identity naming already takes for traversal).
    fn file_delivery_root(&self) -> Option<&'static str> {
        None
    }

    /// The subdirectory of the host task directory that
    /// [`TaskPlugin::file_delivery_root`] maps to, when it is not the task
    /// directory itself. A sandbox projects only its identity files into the
    /// guest, so they get a directory of their own on the host.
    fn file_delivery_host_subdir(&self) -> Option<&'static str> {
        None
    }

    /// The worker just rewrote this allocation's workload-identity tokens on
    /// the host (a refresh). A runtime whose workload cannot read the host
    /// files directly, such as a microVM guest, copies them in here, with no
    /// restart. The default does nothing: the host file is what the task
    /// reads.
    fn identity_tokens_refreshed(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        Ok(())
    }

    /// Whether this runtime can deliver a Unix signal to the running task (needed
    /// for an identity with `changeMode: signal`).
    fn supports_signal(&self) -> bool {
        false
    }

    /// Deliver a Unix signal (by name, e.g. `"SIGHUP"`) to the running task.
    ///
    /// The default signals the tracked pid, which is correct wherever the
    /// tracked process *is* the workload — the `exec` runtime, and any plugin
    /// that spawns its task directly. Isolating runtimes override it to reach
    /// inside: for a container the tracked pid is the runtime CLI supervisor,
    /// and signaling that kills the supervisor while the container runs on.
    ///
    /// Called by the worker for `changeMode: signal` identity refreshes and by
    /// graceful stop. A runtime that returns `false` from
    /// [`TaskPlugin::supports_signal`] is never asked (admission rejects the
    /// specs that would ask), so the default is a safety net, not a contract.
    fn signal(
        &self,
        allocation: &Allocation,
        _task: &TaskConfig,
        instance: &mut TaskInstance,
        signal: &str,
    ) -> R<()> {
        let pid = instance.pid.with_context(|| {
            format!("allocation {} has no tracked pid to signal", allocation.id)
        })?;
        crate::signals::send_signal(pid, signal)
    }

    /// Whether the task's execution is a host process this worker tracks —
    /// `TaskInstance.child` (a process we spawned) or `TaskInstance.pid` (one
    /// we re-adopted after a worker restart). True means the worker's task
    /// supervisor polls it for exit, reaps it, and applies the task's
    /// `restart` policy (see `internal_docs/plans/host-process-supervision-plan.md`).
    ///
    /// Default `false`: a runtime that tracks no process would otherwise be
    /// polled by a supervisor that can never observe its exit, and a `batch`
    /// task of that runtime would never reach a terminal state. Runtimes whose
    /// model is "one host process we spawned" (`exec`, `container`, `sandbox`)
    /// override this to `true`.
    fn supervises_process(&self) -> bool {
        false
    }

    /// Capability gate for the `artifacts` task field, mirroring
    /// `supports_kv_env_injection` and friends. Admission rejects
    /// artifact-bearing specs for runtimes that return false. Runtimes that return
    /// true get File/Tree payloads with zero further code: the worker core
    /// stages them into the task dir and exposes `${artifact.<name>}` /
    /// `LEGION_ARTIFACT_<NAME>`.
    /// Volume modes this runtime can consume (see docs/volumes.md). Most
    /// runtimes run a process against a directory; a microVM runtime attaches
    /// a block device instead and cannot use a path at all.
    ///
    /// Admission intersects this with the plugin's supported modes and the
    /// job's requested mode, so a mismatch is a submit-time error naming all
    /// three rather than a task that starts and immediately fails.
    fn supported_volume_modes(&self) -> &'static [crate::volumes::VolumeMode] {
        &[crate::volumes::VolumeMode::Filesystem]
    }

    fn supports_artifacts(&self) -> bool {
        false
    }

    /// How this runtime participates in worker-enforced resource placement
    /// (see [`ResourceEnforcement`] and `docs/resource-enforcement.md`).
    ///
    /// The default is `Unsupported`: worker core computes no assignment and
    /// touches no cgroup, so `resources` stay scheduling-only — exactly the
    /// behavior every runtime had before this method existed, which is what
    /// keeps third-party dylib plugins working untouched.
    ///
    /// Opting in is one line for a runtime whose task is a host process
    /// (`WorkerCgroup` + [`join_assignment_cgroup`] at spawn) and one field
    /// read for a runtime that confines its own workload (`RuntimeApplied`,
    /// which the `container` runtime writes into the generated OCI spec).
    fn resource_enforcement(&self) -> ResourceEnforcement {
        ResourceEnforcement::Unsupported
    }

    /// The memory ceiling, in bytes, worker core should write for this task
    /// instead of its declared `resources.memory`. `None` (the default) means
    /// the declared amount is the ceiling.
    ///
    /// For runtimes whose process holds more than the workload: the
    /// `sandbox` runtime's cgroup holds the VMM, so its ceiling is the guest
    /// size plus the worker's `sandbox.vmmOverheadMiB`. Only consulted when
    /// [`Self::resource_enforcement`] is not `Unsupported`.
    fn enforced_memory_limit_bytes(&self, task: &TaskConfig) -> Option<u64> {
        let _ = task;
        None
    }

    /// Runtime-specific normalization of a task at submit, applied before the
    /// manager folds in `taskResourceDefaults`, so anything this sets wins
    /// over a cluster default. The default does nothing.
    ///
    /// Only built-in runtimes are consulted ([`normalize_runtime_task`]):
    /// normalization runs inside the submit transaction, which must not
    /// load a dylib. The `sandbox` runtime uses it to default `memory` to
    /// its own 512 MiB and pin its guest size.
    fn normalize_task(&self, task: &mut TaskConfig) {
        let _ = task;
    }

    /// Runtime-specific judgement of a task's `resources`, at submit: an
    /// `Err` rejects the job, and each returned string is a submit warning
    /// (appended to the `job run` reply). The default has nothing to say.
    ///
    /// For runtimes whose own config also sizes the workload: the `sandbox`
    /// runtime warns when an explicit `memory_mib` disagrees with the guest
    /// size `resources` imply.
    fn check_resources(&self, task: &TaskConfig) -> R<Vec<String>> {
        let _ = task;
        Ok(Vec::new())
    }

    /// What this runtime can checkpoint for a task (suspend plan §6.5).
    ///
    /// The default is the honest answer for any runtime that runs a host
    /// process: [`CheckpointSupport::Disk`] when the task has a volume whose
    /// driver advertises `snapshot`, else [`CheckpointSupport::None`]. A disk
    /// checkpoint is the volume half alone, and the volume is worker core's,
    /// not the runtime's, so a runtime gets it without writing any code.
    /// Runtimes that can capture memory (the sandbox on a real VMM) override.
    fn checkpoint_support(&self, query: &CheckpointQuery<'_>) -> CheckpointSupport {
        if query.snapshot_volume {
            CheckpointSupport::Disk
        } else {
            CheckpointSupport::None
        }
    }

    /// Capture the running allocation's memory half into `out`, then either
    /// keep it running ([`AfterCheckpoint::Resume`]) or leave it stopped
    /// ([`AfterCheckpoint::Stop`]).
    ///
    /// Worker core drives the whole capture and owns the disk half:
    ///
    /// 1. call `checkpoint` with `then`;
    /// 2. snapshot each mounted volume with the driver's `snapshot` op while
    ///    the task is quiet (a memory runtime keeps the guest paused across
    ///    this step only for [`AfterCheckpoint::Stop`]; a live checkpoint's
    ///    disk half is crash-consistent);
    /// 3. write the [`SnapshotManifest`](crate::structs::checkpoint::SnapshotManifest)
    ///    from the returned parts and the volume refs.
    ///
    /// The default is a disk checkpoint: there is no memory to capture, so it
    /// stops the task when asked to and returns no parts. Only called for a
    /// task whose [`checkpoint_support`](Self::checkpoint_support) is not
    /// `None`.
    fn checkpoint(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        out: &CheckpointDir,
        then: AfterCheckpoint,
    ) -> R<crate::structs::checkpoint::MemoryParts> {
        let _ = out;
        if then == AfterCheckpoint::Stop {
            self.stop(allocation, task, instance)?;
        }
        Ok(crate::structs::checkpoint::MemoryParts::default())
    }

    /// Resume a task that [`checkpoint`](Self::checkpoint) left paused
    /// ([`AfterCheckpoint::Pause`]). Worker core calls it after the disk half,
    /// on success and on failure alike, so a paused task never stays paused.
    /// The default does nothing: a runtime that cannot pause never left
    /// anything paused.
    fn resume_after_checkpoint(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
    ) -> R<()> {
        let _ = (allocation, task, instance);
        Ok(())
    }

    /// Ask a running guest to give back up to `bytes` of memory, for the
    /// memory pressure response's `reclaim` step
    /// (docs/resource-enforcement.md, "Memory pressure"). Returns the bytes
    /// handed back, or `None` when this runtime has no such lever and the
    /// worker should use `memory.reclaim` on the allocation's cgroup instead.
    ///
    /// The default is `None`: a host process's memory is the cgroup's to
    /// reclaim. The sandbox overrides it with its balloon, because guest RAM
    /// is anonymous VMM memory the host kernel cannot reclaim without swap.
    fn reclaim_guest_memory(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &TaskInstance,
        bytes: u64,
    ) -> R<Option<u64>> {
        let _ = (allocation, task, instance, bytes);
        Ok(None)
    }

    /// Undo [`reclaim_guest_memory`](Self::reclaim_guest_memory) once the
    /// pressure episode is over (deflate the balloon). The default has
    /// nothing to undo.
    fn release_guest_memory(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &TaskInstance,
    ) -> R<()> {
        let _ = (allocation, task, instance);
        Ok(())
    }

    /// Start the allocation from a checkpoint instead of booting it. Called
    /// in place of `start`, after `prepare` and after worker core restored or
    /// cloned the volumes the manifest names.
    ///
    /// `source` holds the manifest's memory parts under their recorded file
    /// names. `identity` is who the restored allocation is now (its id, its
    /// instance, a fresh token).
    ///
    /// The default handles a disk checkpoint: the volumes already hold the
    /// captured state, so restoring is a fresh `start` on them. A memory
    /// manifest is refused, because this runtime wrote none.
    fn restore(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        manifest: &crate::structs::checkpoint::SnapshotManifest,
        source: &CheckpointDir,
        identity: &crate::structs::checkpoint::ResumeIdentity,
    ) -> R<()> {
        let _ = (source, identity);
        if manifest.kind == crate::structs::checkpoint::SnapshotKind::Memory {
            bail!(
                "task runtime `{}` cannot restore memory checkpoint {} (written by `{}`)",
                self.name(),
                manifest.id,
                manifest.runtime
            );
        }
        self.start(allocation, task, instance)
    }

    /// Desugars runtime-config fields into artifacts (artifact system plan §8).
    /// The manager calls this at admission; this is how `container.image`
    /// joins the shared pipeline without changing its user-facing spec.
    fn implicit_artifacts(&self, task_config: &Value) -> R<Vec<crate::structs::ArtifactSpec>> {
        let _ = task_config;
        Ok(Vec::new())
    }

    /// Consumes non-file payloads during prepare (OCI images, ORAS layers).
    /// The default handles nothing because File/Tree need no consumption.
    fn consume_artifact(
        &self,
        staged: &crate::artifacts::StagedArtifact,
        instance: &mut TaskInstance,
    ) -> R<()> {
        let _ = (staged, instance);
        Ok(())
    }

    /// What this runtime supports for exec / actions. Defaults to none, so a
    /// runtime that has not opted in fails fast and honestly (S4 in the plan).
    fn exec_support(&self) -> ExecSupport {
        ExecSupport::default()
    }

    /// Open a live exec session inside (or alongside) the running allocation.
    /// The default rejects; runtimes that report `exec_support().exec` override.
    fn exec(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
        _opts: ExecOptions,
    ) -> R<Box<dyn ExecSession>> {
        bail!("task runtime `{}` does not support exec", self.name())
    }

    fn prepare(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        Ok(())
    }

    fn start(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        Ok(())
    }

    fn stop(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        Ok(())
    }

    /// Begin stopping an allocation, told *why* and *how*.
    ///
    /// This is the form the worker calls, and it is the **notice**, not the
    /// kill: deliver `ctx.signal` and return promptly. Returning `Ok(true)`
    /// means "a notice was delivered, the process is still running" — the
    /// worker then holds the allocation in its terminating set until the
    /// process exits or `ctx.deadline` passes, and calls
    /// [`TaskPlugin::destroy`] at whichever comes first. `Ok(false)` means
    /// the runtime could not signal (no tracked process, no signal support)
    /// and the worker should tear down immediately, exactly as it did before
    /// graceful stop existed.
    ///
    /// **Never block for the grace window here.** `stop_with` runs under the
    /// worker's task-manager mutex, which the heartbeat, the self-fence
    /// watchdog, health reporting and log maintenance all need; holding it
    /// for 30s would let the manager declare the node lost and reschedule
    /// everything on it.
    ///
    /// The default delivers the signal through the shared host-process path,
    /// which is right for every runtime whose execution model is "one process
    /// we spawned". A runtime with its own shutdown protocol (containers,
    /// microVMs) overrides. An out-of-tree plugin compiled before this method
    /// existed keeps its old behavior: it inherits this default, which falls
    /// back to `Ok(false)` -> immediate `stop()` + `destroy()` when it tracks
    /// no host process.
    fn stop_with(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        ctx: &StopContext,
    ) -> R<bool> {
        if !self.supervises_process() {
            return Ok(false);
        }
        Ok(signal_task_process(
            allocation.id,
            &task.name,
            instance,
            self.name(),
            &ctx.signal,
        ))
    }

    fn destroy(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
    ) -> R<()> {
        Ok(())
    }

    /// How strong this runtime's network gate is for a provisional start held
    /// at gate `boot` (docs/edge-boot.md).
    ///
    /// The default is [`NetworkGate::Advisory`]: the worker still withholds
    /// service registration and the node-local upstream forwarders (both fall
    /// out of the allocation not being in the store yet), but nothing stops
    /// the workload dialing out through the host's own stack. That is the
    /// honest answer for host-process runtimes, and it is stated as a
    /// limitation in the public doc rather than papered over.
    fn network_gate(&self) -> NetworkGate {
        NetworkGate::Advisory
    }

    /// [`TaskPlugin::network_gate`] for one allocation: runtimes whose gate
    /// strength depends on the allocation's `network.mode` (a gated
    /// namespace is a real boundary; host networking is not) answer here.
    /// Defaults to the runtime-wide answer.
    fn network_gate_for(&self, _allocation: &Allocation) -> NetworkGate {
        self.network_gate()
    }

    /// Open or shut this allocation's network.
    ///
    /// Called with `open: false` immediately before `start` when a
    /// provisional allocation is held at gate `boot`, and with `open: true`
    /// when it is ratified. Runtimes reporting [`NetworkGate::Advisory`] may
    /// leave this as the no-op default.
    fn set_network_gate(
        &self,
        _allocation: &Allocation,
        _task: &TaskConfig,
        _instance: &mut TaskInstance,
        _open: bool,
    ) -> R<()> {
        Ok(())
    }
}

/// Whether an allocation runs in a gated namespace (`network.mode = "gated"`,
/// docs/network-handlers.md) — the shared answer for host runtimes' gate
/// strength and namespace entry.
pub fn allocation_is_gated(allocation: &Allocation) -> bool {
    allocation
        .network
        .as_ref()
        .is_some_and(|network| network.is_gated())
}

/// The edge-boot gate for a gated allocation is "policy withheld": a marker
/// the datapath checks per connection (`netd::netns::set_gate`). Host
/// runtimes call this from `set_network_gate`; it is a no-op for allocations
/// that are not gated.
pub fn set_gated_namespace_gate(allocation: &Allocation, open: bool) -> R<()> {
    if !allocation_is_gated(allocation) {
        return Ok(());
    }
    crate::worker::netd::netns::set_gate(allocation.id, !open)
}

/// How much a runtime's `boot`-gate network hold is actually worth.
///
/// This asymmetry is deliberate and load-bearing for the security story:
/// untrusted code belongs in a microVM, which is the same advice
/// `docs/task-runtime-sandbox.md` already gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkGate {
    /// A real boundary: the workload physically cannot reach the network
    /// before ratification (a microVM whose only interface is held down, or
    /// one with no interface at all).
    Enforced,
    /// Legion withholds what it controls — service registration and the
    /// node-local forwarders — but the workload shares the host's network
    /// stack and can still reach the outside world.
    Advisory,
}

pub struct LoadedTaskPlugin {
    pub plugin: Box<dyn TaskPlugin>,
    _lib: Option<Library>,
}

impl LoadedTaskPlugin {
    fn new(plugin: Box<dyn TaskPlugin>, lib: Option<Library>) -> Self {
        Self { plugin, _lib: lib }
    }

    pub fn task_plugin(&self) -> &dyn TaskPlugin {
        self.plugin.as_ref()
    }
}

#[derive(Default)]
pub struct TaskPluginRegistry {
    // `Arc` so a caller can hold a plugin (and, through it, the dylib that
    // backs it) while it works without the registry's owner locked.
    plugins: HashMap<String, Arc<LoadedTaskPlugin>>,
}

impl TaskPluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_plugin(&mut self, runtime: &str, run_setup: bool) -> R<()> {
        if self.plugins.contains_key(runtime) {
            return Ok(());
        }

        let plugin = load_task_plugin(runtime)?;
        if run_setup {
            plugin.plugin.setup()?;
        }
        self.plugins.insert(runtime.to_string(), Arc::new(plugin));
        Ok(())
    }

    pub fn plugin(&self, runtime: &str) -> Option<&dyn TaskPlugin> {
        self.plugins.get(runtime).map(|entry| entry.plugin.as_ref())
    }

    /// A shared handle to a loaded plugin, for slow runtime calls (`prepare`,
    /// `start`) made after the registry's owner has been unlocked.
    pub fn plugin_handle(&self, runtime: &str) -> Option<Arc<LoadedTaskPlugin>> {
        self.plugins.get(runtime).cloned()
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        let mut checks = Map::new();
        for plugin in self.plugins.values() {
            checks.extend(plugin.plugin.readiness());
        }
        checks
    }

    pub fn validate_task_config(&mut self, task: &TaskConfig) -> R<()> {
        self.load_plugin(&task.runtime, false)?;
        let plugin = self
            .plugin(&task.runtime)
            .context("task plugin loaded but missing")?;
        plugin
            .validate_config(&task.config)
            .with_context(|| format!("task {} config invalid", task.name))?;
        Ok(())
    }
}

#[instrument(err, skip_all)]
pub fn validate_job_config(config: &JobConfig) -> R<()> {
    let mut registry = TaskPluginRegistry::new();
    for task in &config.tasks {
        registry.validate_task_config(task)?;
        validate_memory_max(task)?;
        task.validate_checks()?;
        task.validate_kv_injections()?;
        task.validate_network()?;
        task.validate_connect()?;
        task.validate_stateful()?;
        task.validate_volumes()?;
        task.validate_identities()?;
        task.validate_artifacts()?;
        task.validate_actions()?;
        task.validate_stop()?;
        task.validate_disruption()?;
        task.validate_speculative_start()
            .map_err(|err| anyhow!(err))?;
        if let Some(snapshots) = task.snapshots.as_ref() {
            snapshots
                .resolve()
                .map_err(|err| anyhow!("task {}: {err}", task.name))?;
        }
        crate::structs::suspend::validate_suspend(task).map_err(|err| anyhow!(err))?;
        // One predicate decides "asks for memory checkpoints", so submit-time
        // refusals and the worker's launch decision can never disagree.
        crate::structs::snapshot::validate_memory_checkpoint_network(task)
            .map_err(|err| anyhow!(err))?;
        if !task.artifacts.is_empty() {
            registry.load_plugin(&task.runtime, false)?;
            let plugin = registry
                .plugin(&task.runtime)
                .context("task plugin loaded but missing")?;
            if !plugin.supports_artifacts() {
                bail!(
                    "Task {} runtime {} does not support artifacts",
                    task.name,
                    task.runtime
                );
            }
        }
        if !task.identities.is_empty() {
            // The job/group/task names become the OIDC `sub` components, so
            // constrain their charset to keep distinct workloads distinct (see
            // validate_subject_field). Only enforced for identity-declaring
            // tasks so other jobs keep their naming freedom.
            use crate::structs::workload_identity::validate_subject_field;
            validate_subject_field("job name", &config.name)?;
            validate_subject_field(&format!("task {} group", task.name), &task.group)?;
            validate_subject_field(&format!("task {} name", task.name), &task.name)?;

            registry.load_plugin(&task.runtime, false)?;
            let plugin = registry
                .plugin(&task.runtime)
                .context("task plugin loaded but missing")?;
            for identity in &task.identities {
                if identity.delivers_env() && !plugin.supports_identity_env() {
                    bail!(
                        "Task {} runtime {} does not support identity env delivery (identity {})",
                        task.name,
                        task.runtime,
                        identity.name
                    );
                }
                if identity.delivers_file() && !plugin.supports_identity_file() {
                    bail!(
                        "Task {} runtime {} does not support identity file delivery (identity {})",
                        task.name,
                        task.runtime,
                        identity.name
                    );
                }
                if identity.change_mode == crate::structs::workload_identity::ChangeMode::Signal
                    && !plugin.supports_signal()
                {
                    bail!(
                        "Task {} runtime {} does not support signaling required by identity {} \
                         changeMode signal",
                        task.name,
                        task.runtime,
                        identity.name
                    );
                }
            }
        }
        // Resource rules that only bite on a runtime the worker can actually
        // confine. Checked at admission so a spec that would be silently
        // meaningless (or unimplementable) fails at `job validate` instead of
        // at the node.
        {
            registry.load_plugin(&task.runtime, false)?;
            let plugin = registry
                .plugin(&task.runtime)
                .context("task plugin loaded but missing")?;
            if plugin.resource_enforcement() != ResourceEnforcement::Unsupported {
                validate_governed_resources(task)?;
            }
            if let Some(root) = plugin.file_delivery_root() {
                validate_file_delivery_paths(task, root)?;
            }
        }
        if task.schedule.is_some() && task.lifecycle != TaskLifecycle::Batch {
            bail!(
                "Task {} can only set schedule when lifecycle is batch",
                task.name
            );
        }
        if let Some(injections) = task.kv_injections.as_ref() {
            registry.load_plugin(&task.runtime, false)?;
            let plugin = registry
                .plugin(&task.runtime)
                .context("task plugin loaded but missing")?;
            if !injections.env.is_empty() && !plugin.supports_kv_env_injection() {
                bail!(
                    "Task {} runtime {} does not support kv env injection",
                    task.name,
                    task.runtime
                );
            }
            if !injections.files.is_empty() && !plugin.supports_kv_file_injection() {
                bail!(
                    "Task {} runtime {} does not support kv file injection",
                    task.name,
                    task.runtime
                );
            }
            for file in &injections.files {
                if file.change_mode == Some(crate::structs::workload_identity::ChangeMode::Signal)
                    && !plugin.supports_signal()
                {
                    bail!(
                        "Task {} runtime {} does not support signaling required by file {} \
                         changeMode signal",
                        task.name,
                        task.runtime,
                        file.path
                    );
                }
            }
        }
    }
    // Slot instance ids are derived from (job, group, slot), so two slot-mode
    // stateful tasks in one group would collide on instance identity.
    let mut slot_stateful_by_group: HashMap<&str, &str> = HashMap::new();
    for task in &config.tasks {
        if task
            .stateful
            .as_ref()
            .is_some_and(|stateful| stateful.mode == InstanceMode::Slot)
        {
            if let Some(existing) =
                slot_stateful_by_group.insert(task.group.as_str(), task.name.as_str())
            {
                bail!(
                    "Group {} declares multiple slot-mode stateful tasks ({} and {}); slot \
                     instance ids are derived from (job, group, slot) and would collide",
                    task.group,
                    existing,
                    task.name
                );
            }
        }
    }
    // A `createFirst` (replace-before-stop) rollout runs the incoming and
    // outgoing allocations concurrently. For a stateful task that means two
    // live allocations sharing one instance id, storage handle, and workload
    // identity at once — violating the "at most one live allocation per
    // instance" invariant (docs/stateful-workloads.md) with no fencing to make
    // it safe (internal_docs/stateful-vs-nomad-k8s.md). Reject the combo at admission:
    // stateful groups must roll with the default stop-then-start strategy until
    // fencing exists.
    for task in &config.tasks {
        if !task.is_stateful() {
            continue;
        }
        let replacement = config
            .rollout_config_for_group(&task.group)
            .and_then(|rollout| rollout.replacement);
        if replacement == Some(RolloutReplacement::CreateFirst) {
            bail!(
                "task {} (group {}) is stateful and cannot use a createFirst \
                 (replace-before-stop) rollout: it would run two live allocations on the same \
                 instance id and storage handle at once, breaking the at-most-one-live-allocation \
                 invariant. Use the default stop-first replacement for stateful groups.",
                task.name,
                task.group
            );
        }
    }
    // Ordinal-aware rollout options (`rollout.slots`: `order: sequential`,
    // `updateFrom`) only have meaning for a fixed pool of ordinal slots, i.e.
    // a group whose stateful task is slot mode. Reject the block anywhere
    // else (stateless groups, keyed groups) with a pointer at the per-group
    // scoping so mixed jobs can still use it.
    let job_groups = config
        .tasks
        .iter()
        .map(|task| task.group.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for group in job_groups {
        let Some(slots) = config
            .rollout_config_for_group(group)
            .and_then(|rollout| rollout.slots)
        else {
            continue;
        };
        let slot_task = config.tasks.iter().find(|task| {
            task.group == group
                && task
                    .stateful
                    .as_ref()
                    .is_some_and(|stateful| stateful.mode == InstanceMode::Slot)
        });
        let Some(slot_task) = slot_task else {
            bail!(
                "rollout slots options (order/updateFrom) are only valid for groups with a \
                 slot-mode stateful task, and group {} has none. Scope them via \
                 rolloutByGroup.{} on a slot-mode group instead.",
                group,
                group
            );
        };
        if let Some(update_from) = slots.update_from {
            // K8s allows partition >= replicas to mean "update nothing", but
            // a Legion rollout that completes instantly while silently
            // pinning every slot to the old config is almost always a typo.
            // Reject it and point at the intent-revealing alternatives.
            if update_from >= slot_task.count.num {
                bail!(
                    "rollout slots.updateFrom ({}) must be less than task {}'s count ({}): a \
                     value >= count would update nothing. Omit updateFrom to update every slot, \
                     or lower it to stage the update at a higher slot.",
                    update_from,
                    slot_task.name,
                    slot_task.count.num
                );
            }
        }
    }
    config.validate_rollout()?;
    if let Some(rollout) = config.rollout.as_ref() {
        if let Some(strategy) = rollout.strategy.as_ref() {
            validate_rollout_strategy_name(strategy).with_context(|| "invalid rollout.strategy")?;
        }
    }
    for (group, rollout) in &config.rollout_by_group {
        if let Some(strategy) = rollout.strategy.as_ref() {
            validate_rollout_strategy_name(strategy)
                .with_context(|| format!("invalid rolloutByGroup.{}.strategy", group))?;
        }
    }
    validate_task_dependencies(config)?;
    Ok(())
}

/// The permission bits for a KV-injected file: its declared octal `mode`, or
/// `0644` (what the old plain write produced under the usual umask) when
/// unset or unparseable.
pub fn injected_file_mode(mode: Option<&str>) -> u32 {
    mode.and_then(|mode| crate::structs::workload_identity::parse_file_mode(mode).ok())
        .unwrap_or(0o644)
}

/// Resolve a declared file-delivery destination against an isolating
/// runtime's task-dir mount, returning the path *relative to the task
/// directory* — which is exactly what the worker joins onto the host task dir.
///
/// Accepts a relative path (resolved against `root`) or an absolute path
/// inside `root`. Rejects anything else, including `..` traversal that would
/// climb out, so the check is the same at admission and on the worker.
pub fn resolve_task_dir_destination(root: &str, declared: &str) -> Result<PathBuf, String> {
    let declared = declared.trim();
    if declared.is_empty() {
        return Err("path cannot be empty".to_string());
    }
    let path = std::path::Path::new(declared);
    // Containers see their task directory at /legion/task; a sandbox guest
    // receives its token files under /legion/identity.
    let what = if root == sandbox::GUEST_IDENTITY_ROOT {
        "sandbox identity directory"
    } else {
        "container task directory"
    };
    let relative = if path.is_absolute() {
        path.strip_prefix(root).map_err(|_| {
            format!(
                "{declared:?} is outside the {what}; use a path under {root} \
                 (or a relative path), or switch to env delivery"
            )
        })?
    } else {
        path
    };

    // Normalize, refusing any component that could climb out. `.` is dropped;
    // `..` is a hard error rather than something to resolve, so the message
    // stays about the destination the author wrote.
    let mut normalized = PathBuf::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(part) => normalized.push(part),
            std::path::Component::CurDir => {}
            std::path::Component::RootDir if normalized.as_os_str().is_empty() => {}
            _ => {
                return Err(format!(
                    "{declared:?} escapes the {what}; use a path under {root}"
                ))
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(format!(
            "{declared:?} names the task directory itself, not a file inside it"
        ));
    }
    Ok(normalized)
}

/// Every file-delivery destination on a task whose runtime mounts its task
/// directory must land inside that mount — the one place a jobspec is not
/// verbatim-portable between the `exec` and `container` runtimes.
fn validate_file_delivery_paths(task: &TaskConfig, root: &str) -> R<()> {
    if let Some(injections) = task.kv_injections.as_ref() {
        for file in &injections.files {
            resolve_task_dir_destination(root, &file.path)
                .map_err(|err| anyhow!("task {} kvInjections.files path {err}", task.name))?;
        }
    }
    for identity in &task.identities {
        let Some(file) = identity.file.as_ref() else {
            continue;
        };
        resolve_task_dir_destination(root, &file.path).map_err(|err| {
            anyhow!(
                "task {} identity {:?}: file.path {err}",
                task.name,
                identity.name
            )
        })?;
    }
    Ok(())
}

/// `resources.memoryMax` against the task's declared `memory`, and the
/// runtime's own [`TaskPlugin::check_resources`] verdict. Declared values
/// only: the manager re-checks against `taskResourceDefaults` when it
/// normalizes the job (`ResourcePolicy::normalize_task_resources`).
fn validate_memory_max(task: &TaskConfig) -> R<()> {
    if let Some(resources) = task.resources.as_object() {
        crate::resources::validate_memory_max(resources)
            .with_context(|| format!("task {}", task.name))?;
    }
    let mut registry = TaskPluginRegistry::new();
    if registry.load_plugin(&task.runtime, false).is_ok() {
        if let Some(plugin) = registry.plugin(&task.runtime) {
            plugin.check_resources(task)?;
        }
    }
    Ok(())
}

/// Apply a built-in runtime's [`TaskPlugin::normalize_task`] to one task.
/// Third-party runtimes are left alone: their tasks keep the cluster's
/// `taskResourceDefaults`, as they always have.
pub fn normalize_runtime_task(task: &mut TaskConfig) {
    if matches!(task.runtime.as_str(), "exec" | "container" | "sandbox") {
        if let Ok(loaded) = load_task_plugin(&task.runtime) {
            loaded.plugin.normalize_task(task);
        }
    }
}

/// A `cpu` (or its alias `cpus`) under 10 MHz is almost always a CPU count
/// written in the wrong unit: `cpus = 1` reads as one CPU but reserves 1 MHz.
/// Accepted, since it is valid, but called out with the likely intent.
fn tiny_cpu_warning(task: &TaskConfig) -> Option<String> {
    let (key, value) = ["cpus", "cpu"].into_iter().find_map(|key| {
        task.resources
            .get(key)
            .and_then(serde_json::Value::as_f64)
            .map(|value| (key, value))
    })?;
    (value > 0.0 && value < 10.0).then(|| {
        format!(
            "task {} sets resources.{key} = {value}, which is {value} MHz, not {value} CPU(s): \
             `cpu` and its alias `cpus` are in MHz. Did you mean cpu = {}? (For whole \
             exclusive CPUs use `cores`; a sandbox's guest CPU count is `config.vcpus`.)",
            task.name,
            (value * 1000.0).round() as u64
        )
    })
}

/// Advisory lines for a job's `resources`, shown on `job run`: a `memoryMax`
/// the runtime cannot enforce, and whatever each runtime's
/// [`TaskPlugin::check_resources`] has to say. Never fails: a runtime that
/// will not load is a validation error reported elsewhere.
pub fn resource_submit_warnings(config: &JobConfig) -> Vec<String> {
    let mut registry = TaskPluginRegistry::new();
    let mut warnings = Vec::new();
    for task in &config.tasks {
        if let Some(line) = tiny_cpu_warning(task) {
            warnings.push(line);
        }
        if registry.load_plugin(&task.runtime, false).is_err() {
            continue;
        }
        let Some(plugin) = registry.plugin(&task.runtime) else {
            continue;
        };
        if task
            .resources
            .get(crate::resources::MEMORY_MAX_KEY)
            .is_some()
            && plugin.resource_enforcement() == ResourceEnforcement::Unsupported
        {
            warnings.push(format!(
                "task {} sets resources.memoryMax, but the {} runtime does not enforce \
                 resources: the scheduler still reserves resources.memory, and nothing caps the \
                 task at memoryMax. See docs/resource-enforcement.md",
                task.name, task.runtime
            ));
        }
        if let Ok(lines) = plugin.check_resources(task) {
            warnings.extend(lines);
        }
    }
    warnings
}

/// The `resources` rules that apply to a runtime the worker can confine
/// (`container`, `exec`) — Nomad's, because the semantics are Nomad's.
///
/// Both rules reject specs that were previously accepted-and-ignored, which
/// is deliberate: `cores: 1.5` was silently rounded on macOS and dropped on
/// Linux, and `cores` + `cpu` together asked for two different CPU contracts
/// on one task. Every message names the fix.
fn validate_governed_resources(task: &TaskConfig) -> R<()> {
    let cores = crate::worker::resources::declared_cores(&task.resources);
    let cpu_mhz = crate::worker::resources::cpu_weight_shares(&task.resources);

    if let Some(cores) = cores {
        if cores.fract() != 0.0 {
            bail!(
                "task {} declares resources.cores {cores}, which must be a whole number on the \
                 {} runtime: `cores` reserves exclusive CPUs and a cpuset cannot hold a fraction \
                 of one. Round it, or declare `cpu` in MHz (for example `cpu = 500`, about half \
                 of a 1 GHz core) for a proportional share instead",
                task.name,
                task.runtime,
            );
        }
        if cpu_mhz.is_some() {
            bail!(
                "task {} declares both resources.cores and resources.cpu: they are two different \
                 CPU contracts (exclusive whole CPUs vs. a proportional share of the shared \
                 pool) and cannot both hold. Declare `cores` for latency-sensitive work, or \
                 `cpu` (MHz) for everything else",
                task.name,
            );
        }
    }

    // An explicit `config.cgroup` on an exec task opts it out of Legion's
    // hierarchy, so Legion cannot promise exclusivity inside it.
    if cores.is_some() {
        let named_cgroup = task
            .config
            .get("cgroup")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(cgroup) = named_cgroup {
            bail!(
                "task {} declares resources.cores together with config.cgroup ({cgroup:?}): an \
                 explicit cgroup is one Legion does not manage, so it cannot fence other tasks \
                 off the cores you reserved. Drop `cores` (use `cpu` MHz instead), or drop \
                 `cgroup` and let the worker place the task",
                task.name,
            );
        }
    }
    Ok(())
}

fn validate_task_dependencies(config: &JobConfig) -> R<()> {
    let mut task_lookup: HashMap<String, &TaskConfig> = HashMap::new();
    for task in &config.tasks {
        if task_lookup.contains_key(&task.name) {
            bail!("Duplicate task name {}", task.name);
        }
        task_lookup.insert(task.name.clone(), task);
    }

    let mut group_per_key: HashMap<String, Option<String>> = HashMap::new();
    let mut group_has_primary: HashMap<String, bool> = HashMap::new();
    for task in &config.tasks {
        let per_key = task.count.per.clone();
        match group_per_key.get(&task.group) {
            Some(existing) if existing.as_deref() != per_key.as_deref() => {
                bail!(
                    "Group {} mixes count.per values ({:?} vs {:?})",
                    task.group,
                    existing,
                    per_key
                );
            }
            None => {
                group_per_key.insert(task.group.clone(), per_key);
            }
            _ => {}
        }
        if task.lifecycle != TaskLifecycle::Sidecar {
            group_has_primary.insert(task.group.clone(), true);
        }
    }

    for task in &config.tasks {
        if task.lifecycle == TaskLifecycle::Sidecar {
            let has_primary = group_has_primary.get(&task.group).copied().unwrap_or(false);
            if !has_primary {
                bail!(
                    "Sidecar task {} has no primary task in group {}",
                    task.name,
                    task.group
                );
            }
        }
        for dependency in &task.depends_on {
            let target = task_lookup.get(&dependency.task).with_context(|| {
                format!(
                    "Task {} depends on unknown task {}",
                    task.name, dependency.task
                )
            })?;
            if task.lifecycle == TaskLifecycle::Sidecar && target.group != task.group {
                bail!(
                    "Sidecar task {} depends on task {} outside group {}",
                    task.name,
                    dependency.task,
                    task.group
                );
            }
            if task.lifecycle == TaskLifecycle::Service
                && matches!(
                    dependency.condition,
                    TaskDependencyCondition::Complete | TaskDependencyCondition::Success
                )
                && target.lifecycle == TaskLifecycle::Service
            {
                bail!(
                    "Service task {} cannot depend on {} completion or success",
                    task.name,
                    dependency.task
                );
            }
        }
    }

    #[derive(Clone, Copy)]
    enum VisitState {
        Visiting,
        Visited,
    }

    fn visit(
        name: &str,
        task_lookup: &HashMap<String, &TaskConfig>,
        visited: &mut HashMap<String, VisitState>,
    ) -> R<()> {
        if let Some(state) = visited.get(name) {
            if matches!(state, VisitState::Visiting) {
                bail!("Task dependencies contain a cycle at {}", name);
            }
            return Ok(());
        }

        visited.insert(name.to_string(), VisitState::Visiting);
        if let Some(task) = task_lookup.get(name) {
            for dependency in &task.depends_on {
                visit(&dependency.task, task_lookup, visited)?;
            }
        }
        visited.insert(name.to_string(), VisitState::Visited);
        Ok(())
    }

    let mut visited = HashMap::new();
    for name in task_lookup.keys() {
        visit(name, &task_lookup, &mut visited)?;
    }

    Ok(())
}

fn load_task_plugin(name: &str) -> R<LoadedTaskPlugin> {
    match name {
        "exec" => Ok(LoadedTaskPlugin::new(Box::new(ExecTaskPlugin), None)),
        "container" => Ok(LoadedTaskPlugin::new(Box::new(ContainerTaskPlugin), None)),
        "sandbox" => Ok(LoadedTaskPlugin::new(Box::new(SandboxTaskPlugin), None)),
        other => {
            let plugin_root = plugin_root_dir();
            let paths = plugin_paths("tasks", other, &plugin_root);
            validate_manifest("tasks", other, &paths.root)
                .with_context(|| format!("invalid task plugin manifest for {}", other))?;
            unsafe {
                let lib = crate::plugin_runtime::open_plugin_library("tasks", other, &paths.dylib)?;
                check_task_plugin_abi(&lib, other)?;
                let plugin = {
                    let func: Symbol<unsafe extern "C" fn() -> Box<dyn TaskPlugin>> =
                        lib.get(b"get")?;
                    func()
                };
                Ok(LoadedTaskPlugin::new(plugin, Some(lib)))
            }
        }
    }
}

/// Refuse a dylib whose `TaskPlugin` vtable predates this build.
///
/// A missing symbol is treated as a mismatch, not as "assume compatible":
/// every plugin that predates [`TASK_PLUGIN_ABI_VERSION`] also predates the
/// trait changes the constant exists to guard, so the two cases need the same
/// answer. Both produce an error that names the rebuild.
unsafe fn check_task_plugin_abi(lib: &Library, name: &str) -> R<()> {
    let declared: Option<u32> = lib
        .get::<unsafe extern "C" fn() -> u32>(TASK_PLUGIN_ABI_SYMBOL)
        .ok()
        .map(|symbol| symbol());
    match declared {
        Some(version) if version == TASK_PLUGIN_ABI_VERSION => Ok(()),
        Some(version) => bail!(
            "task plugin {name} was built against task-plugin ABI {version}, but this \
             manager/worker speaks ABI {TASK_PLUGIN_ABI_VERSION}. Rebuild the plugin against \
             this Legion version (see docs/task-runtime-plugins.md)"
        ),
        None => bail!(
            "task plugin {name} declares no task-plugin ABI version, so it predates ABI \
             {TASK_PLUGIN_ABI_VERSION} and its TaskPlugin vtable cannot be trusted. Add \
             `legion::declare_task_plugin_abi!();` beside the plugin's `get` constructor and \
             rebuild (see docs/task-runtime-plugins.md)"
        ),
    }
}

#[cfg(test)]
fn task_plugin_name_to_path(name: &str) -> String {
    let plugin_root = plugin_root_dir();
    plugin_paths("tasks", name, &plugin_root)
        .dylib
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn validate_schema(schema: &Value, config: &Value) -> R<()> {
    let schema = Box::leak(Box::new(schema.clone()));
    let compiled = JSONSchema::compile(schema).context("Invalid task config schema")?;
    if let Err(errors) = compiled.validate(config) {
        let details = errors
            .map(|err| err.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("Task config failed schema validation: {details}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    fn alloc() -> Allocation {
        Allocation {
            id: 1,
            status: "running".into(),
            resources: json!({}),
            node_id: "node-1".into(),
            job_config_id: "job-1".into(),
            task_config_id: 1,
            job_id: "job".into(),
            instance_id: None,
            network: None,
        }
    }

    fn exec_task() -> TaskConfig {
        TaskConfig {
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("exec-task", "exec")
        }
    }

    fn container_task() -> TaskConfig {
        TaskConfig {
            config: json!({
                "image": "alpine",
                "command": ["echo", "hello"],
                "env": { "MODE": "test" }
            }),
            ..TaskConfig::new("container-task", "container")
        }
    }

    #[cfg(unix)]
    #[test]
    fn log_capture_dir_is_owner_only() -> R<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::TempDir::new()?;
        let log_dir = temp_dir.path().join("logs").join("1");
        let instance = TaskInstance {
            log_dir: Some(log_dir.clone()),
            ..TaskInstance::default()
        };

        configure_task_log_capture(&mut Command::new("true"), &instance)?;

        let mode = std::fs::metadata(&log_dir)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "log dir must be owner-only, got {mode:o}");
        Ok(())
    }

    /// A plugin written before `stop_with` existed: it implements only
    /// `stop`, exactly as every out-of-tree plugin compiled against the old
    /// trait does.
    #[derive(Default)]
    struct LegacyStopPlugin {
        stopped: std::sync::atomic::AtomicBool,
    }

    impl TaskPlugin for LegacyStopPlugin {
        fn name(&self) -> &'static str {
            "legacy-stop"
        }

        fn stop(
            &self,
            _allocation: &Allocation,
            _task: &TaskConfig,
            _instance: &mut TaskInstance,
        ) -> R<()> {
            self.stopped
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn a_plugin_that_supervises_no_process_declines_the_stop_notice() -> R<()> {
        // Phase 1b changed what `stop_with` means: it is now "deliver the
        // notice and return", reporting whether a notice was actually
        // delivered. A runtime that tracks no host process cannot deliver
        // one, and says so — which is the signal the worker uses to tear the
        // allocation down immediately, exactly as it did before grace
        // existed. `stop()` is still what stops it; the difference is that
        // the worker now calls it via `destroy` rather than via `stop_with`.
        let plugin = LegacyStopPlugin::default();
        let mut instance = TaskInstance::default();
        let ctx = StopContext::new(crate::structs::StopCause::Preempted);

        let signalled = plugin.stop_with(
            &alloc(),
            &TaskConfig::new("legacy-task", "legacy-stop"),
            &mut instance,
            &ctx,
        )?;

        assert!(
            !signalled,
            "a runtime with no tracked process must report that no notice was delivered"
        );
        Ok(())
    }

    #[test]
    fn the_stop_context_carries_the_whole_ladder() {
        let ctx = StopContext::with_ladder(
            crate::structs::StopCause::Preempted,
            "SIGQUIT",
            std::time::Duration::from_secs(45),
        );
        assert_eq!(ctx.signal, "SIGQUIT");
        assert_eq!(ctx.grace, std::time::Duration::from_secs(45));
        assert!(!ctx.expired(), "a fresh 45s window is not expired");

        // The zero-grace case is the degenerate ladder a `killAfter: 0s`
        // would produce: the deadline is already here, so the reaper takes it
        // on its next tick rather than waiting.
        let immediate = StopContext::with_ladder(
            crate::structs::StopCause::Stop,
            "SIGTERM",
            std::time::Duration::ZERO,
        );
        assert!(immediate.expired());
    }

    #[test]
    fn stop_signals_map_to_their_portable_numbers() {
        // The mapping is hard-coded rather than pulled from libc so it is
        // identical on every platform the worker builds for.
        assert_eq!(stop_signal_number("SIGTERM"), Some(15));
        assert_eq!(stop_signal_number("SIGINT"), Some(2));
        assert_eq!(stop_signal_number("SIGQUIT"), Some(3));
        assert_eq!(stop_signal_number("SIGHUP"), Some(1));
        assert_eq!(stop_signal_number("SIGUSR1"), Some(10));
        assert_eq!(stop_signal_number("SIGUSR2"), Some(12));
        // SIGKILL is deliberately absent: it is the END of the ladder, not a
        // notice, and letting a task ask for it as `stop.signal` would mean
        // opting into no grace at all while looking like it had some.
        assert_eq!(stop_signal_number("SIGKILL"), None);
        // Every signal the jobspec accepts must be deliverable.
        for signal in crate::structs::STOP_SIGNALS {
            assert!(
                stop_signal_number(signal).is_some(),
                "{signal} is offered in the jobspec but cannot be delivered"
            );
        }
    }

    #[test]
    fn task_registry_loads_exec_plugin() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        registry.validate_task_config(&exec_task())?;
        assert!(registry.plugin("exec").is_some());
        Ok(())
    }

    #[test]
    fn task_registry_loads_container_plugin() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        registry.validate_task_config(&container_task())?;
        assert!(registry.plugin("container").is_some());
        Ok(())
    }

    fn sandbox_task() -> TaskConfig {
        // No storage in the runtime config on purpose: a sandbox persists via
        // `stateful.storage` (a block-mode volume), not a runtime-private
        // field. See docs/task-runtime-sandbox.md.
        TaskConfig {
            config: json!({
                "image": "alpine",
                "command": ["/bin/sh", "-c", "sleep 1"],
            }),
            ..TaskConfig::new("sandbox-task", "sandbox")
        }
    }

    #[test]
    fn task_registry_loads_sandbox_plugin() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        registry.validate_task_config(&sandbox_task())?;
        assert!(registry.plugin("sandbox").is_some());
        Ok(())
    }

    #[test]
    fn sandbox_exec_is_never_host_level() {
        // A sandbox is a VM (or a container in dev mode), so exec into it must
        // not demand the stronger `host` grant.
        assert!(!runtime_is_host_level("sandbox"));
        assert!(!runtime_is_host_level("container"));
        assert!(runtime_is_host_level("exec"));
    }

    #[test]
    fn exec_plugin_runs_command() -> R<()> {
        let mut registry = TaskPluginRegistry::new();
        registry.load_plugin("exec", true)?;
        let plugin = registry.plugin("exec").context("missing exec plugin")?;
        let mut instance = TaskInstance::default();
        let task = exec_task();
        plugin.prepare(&alloc(), &task, &mut instance)?;
        plugin.start(&alloc(), &task, &mut instance)?;
        assert!(instance.child.is_some());
        plugin.stop(&alloc(), &task, &mut instance)?;
        Ok(())
    }

    /// The Phase 3 disk round trip on the `exec` runtime, no KVM: the task
    /// writes a marker to its volume, the allocation is disk-checkpointed
    /// (stopped, volume snapshotted), the marker is mutated, the snapshot is
    /// restored, and the restarted task sees the original marker.
    #[test]
    fn exec_disk_checkpoint_round_trip() -> R<()> {
        use crate::structs::checkpoint::{SnapshotKind, SnapshotManifest, VolumeSnapshotRef};
        use crate::volumes::builtins::LocalDirDriver;
        use crate::volumes::{
            VolumeDriver, VolumeMode, VolumeNodeRef, VolumeOp, VolumeRef, VolumeRequest,
        };

        let tmp = tempfile::tempdir()?;
        let driver = LocalDirDriver::new(tmp.path().join("volumes"));
        let volume = |op| {
            VolumeRequest::new(
                op,
                VolumeRef {
                    id: "vol-exec".into(),
                    name: "data".into(),
                    driver: "local-dir".into(),
                    attrs: Default::default(),
                    read_only: false,
                },
                VolumeNodeRef {
                    id: "node-1".into(),
                    data_dir: tmp.path().to_path_buf(),
                    volume_dir: tmp.path().join("volumes"),
                },
            )
        };
        let dir = driver
            .mount(&volume(VolumeOp::Mount))?
            .mount_path
            .expect("dir");

        let mut registry = TaskPluginRegistry::new();
        registry.load_plugin("exec", true)?;
        let plugin = registry.plugin("exec").context("missing exec plugin")?;
        let task = TaskConfig {
            config: json!({"command": ["/bin/sh", "-c",
                "cd \"$LEGION_VOLUME_DATA\" && if [ -f marker ]; then cat marker > seen; else echo v1 > marker; fi"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        let support = plugin.checkpoint_support(
            &CheckpointQuery::new(&task.config)
                .with_snapshot_volume(driver.capabilities().supports(VolumeOp::Snapshot)),
        );
        assert_eq!(support, CheckpointSupport::Disk);
        assert_eq!(
            plugin.checkpoint_support(&CheckpointQuery::new(&task.config)),
            CheckpointSupport::None,
            "no snapshot-capable volume, no checkpoint"
        );

        let run = |instance: &mut TaskInstance| -> R<()> {
            let mut child = instance.child.take().context("task process")?;
            assert!(child.wait()?.success());
            instance.pid = None;
            Ok(())
        };
        let mut instance = TaskInstance::default();
        instance
            .extra_env
            .insert("LEGION_VOLUME_DATA".into(), dir.display().to_string());
        plugin.prepare(&alloc(), &task, &mut instance)?;
        plugin.start(&alloc(), &task, &mut instance)?;
        run(&mut instance)?;
        assert_eq!(std::fs::read_to_string(dir.join("marker"))?, "v1\n");

        // Capture: the runtime stops the task and writes no memory, then the
        // worker snapshots the quiet volume.
        let out = CheckpointDir::create(tmp.path().join("ckpt"))?;
        let parts =
            plugin.checkpoint(&alloc(), &task, &mut instance, &out, AfterCheckpoint::Stop)?;
        assert!(parts.is_empty(), "a disk checkpoint has no memory parts");
        let snapshot = driver.snapshot(&volume(VolumeOp::Snapshot).with_snapshot(
            VolumeSnapshotRef {
                id: "snap-exec".into(),
                volume_id: "vol-exec".into(),
                driver: "local-dir".into(),
                mode: VolumeMode::Filesystem,
                attrs: Default::default(),
                size: 0,
            },
        ))?;

        std::fs::write(dir.join("marker"), "v2\n")?;
        driver.restore(&volume(VolumeOp::Restore).with_snapshot(snapshot.clone()))?;

        let manifest = SnapshotManifest {
            api_version: crate::structs::checkpoint::MANIFEST_API_VERSION,
            id: "snap-exec".into(),
            job_id: "job".into(),
            task: "exec-task".into(),
            origin_instance: None,
            origin_alloc_id: 1,
            runtime: "exec".into(),
            kind: SnapshotKind::Disk,
            created_at: "2026-10-10T00:00:00Z".into(),
            volumes: vec![snapshot],
            memory: parts.parts,
            compat: parts.compat,
            runtime_state: Value::Null,
        };
        let identity = crate::structs::checkpoint::ResumeIdentity::default();
        let mut restored = TaskInstance::default();
        restored
            .extra_env
            .insert("LEGION_VOLUME_DATA".into(), dir.display().to_string());
        plugin.prepare(&alloc(), &task, &mut restored)?;
        plugin.restore(&alloc(), &task, &mut restored, &manifest, &out, &identity)?;
        run(&mut restored)?;
        assert_eq!(
            std::fs::read_to_string(dir.join("seen"))?,
            "v1\n",
            "the restarted task saw the snapshot-time marker"
        );

        // A memory manifest is not something `exec` can restore.
        let mut memory = manifest;
        memory.kind = SnapshotKind::Memory;
        let err = plugin
            .restore(
                &alloc(),
                &task,
                &mut TaskInstance::default(),
                &memory,
                &out,
                &identity,
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("cannot restore memory checkpoint"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn noop_plugin_loads_when_built() -> R<()> {
        if std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_err() {
            let path = task_plugin_name_to_path("noop");
            if Path::new(&path).exists() {
                let mut registry = TaskPluginRegistry::new();
                registry.load_plugin("noop", true)?;
                let plugin = registry.plugin("noop").context("missing noop plugin")?;
                let readiness = plugin.readiness();
                if !readiness.is_empty() {
                    assert!(readiness.contains_key("noop"));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn service_task_cannot_depend_on_service_completion() {
        let config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "invalid".into(),
            tasks: vec![
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    config: json!({"command": ["true"]}),
                    ..TaskConfig::new("api", "exec")
                },
                TaskConfig {
                    lifecycle: TaskLifecycle::Service,
                    depends_on: vec![crate::structs::TaskDependency {
                        task: "api".into(),
                        condition: TaskDependencyCondition::Complete,
                    }],
                    config: json!({"command": ["true"]}),
                    ..TaskConfig::new("worker", "exec")
                },
            ],
            version: None,
        };

        let err = validate_job_config(&config).unwrap_err();
        assert!(err.to_string().contains("cannot depend"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn spawned_task_process_runs_in_own_session() -> R<()> {
        let mut cmd = std::process::Command::new("sleep");
        cmd.arg("1");
        let mut child = spawn_task_process(&mut cmd)?;

        let stat = std::fs::read_to_string(format!("/proc/{}/stat", child.id()))?;
        let after_comm = stat
            .rsplit_once(") ")
            .context("unexpected /proc stat format")?
            .1;
        let fields = after_comm.split_whitespace().collect::<Vec<_>>();
        let pgrp: u32 = fields
            .get(2)
            .context("missing pgrp in /proc stat")?
            .parse()
            .context("invalid pgrp in /proc stat")?;
        let session: u32 = fields
            .get(3)
            .context("missing session in /proc stat")?
            .parse()
            .context("invalid session in /proc stat")?;

        assert_eq!(pgrp, child.id());
        assert_eq!(session, child.id());

        let _ = child.kill();
        let _ = child.wait();
        Ok(())
    }

    #[test]
    fn non_batch_task_cannot_define_schedule() {
        let config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "invalid".into(),
            tasks: vec![TaskConfig {
                lifecycle: TaskLifecycle::Service,
                schedule: Some(crate::structs::TaskSchedule::Every(
                    std::time::Duration::from_secs(60),
                )),
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };

        let err = validate_job_config(&config).unwrap_err();
        assert!(err.to_string().contains("can only set schedule"));
    }

    #[test]
    fn sidecar_requires_primary_in_group() {
        let config = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "invalid".into(),
            tasks: vec![TaskConfig {
                lifecycle: TaskLifecycle::Sidecar,
                group: "helpers".into(),
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("logger", "exec")
            }],
            version: None,
        };

        let err = validate_job_config(&config).unwrap_err();
        assert!(err.to_string().contains("Sidecar task"));
    }

    #[test]
    fn validates_default_rollout_strategy() -> R<()> {
        let config = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                strategy: Some("default".into()),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "valid-rollout-strategy".into(),
            tasks: vec![TaskConfig {
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };

        validate_job_config(&config)?;
        Ok(())
    }

    #[test]
    fn rejects_unknown_rollout_strategy() {
        let config = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                strategy: Some("does-not-exist".into()),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "invalid-rollout-strategy".into(),
            tasks: vec![TaskConfig {
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };

        let err = validate_job_config(&config).expect_err("unknown strategy should fail");
        assert!(err.to_string().contains("invalid rollout.strategy"));
    }

    fn stateful_slot_task() -> TaskConfig {
        TaskConfig {
            count: crate::structs::TaskCount::new(2),
            stateful: Some(crate::structs::StatefulConfig {
                mode: InstanceMode::Slot,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
            }),
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("db", "exec")
        }
    }

    #[test]
    fn rejects_create_first_rollout_on_stateful_task() {
        let config = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                replacement: Some(RolloutReplacement::CreateFirst),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "stateful-createfirst".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };

        let err = validate_job_config(&config)
            .expect_err("createFirst over a stateful task must be rejected");
        assert!(
            err.to_string().contains("createFirst"),
            "error should name the createFirst combo: {err}"
        );
    }

    #[test]
    fn allows_stop_first_rollout_on_stateful_task() -> R<()> {
        // Default (stop-first) replacement on a stateful task is fine.
        let config = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                replacement: Some(RolloutReplacement::DestroyFirst),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "stateful-stopfirst".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };
        validate_job_config(&config)?;

        // No rollout block at all is also fine.
        let no_rollout = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "stateful-norollout".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };
        validate_job_config(&no_rollout)?;
        Ok(())
    }

    fn sequential_slots(update_from: Option<u32>) -> crate::structs::RolloutSlotsConfig {
        crate::structs::RolloutSlotsConfig {
            order: Some(crate::structs::RolloutSlotOrder::Sequential),
            update_from,
        }
    }

    #[test]
    fn accepts_sequential_slots_rollout_on_slot_group() -> R<()> {
        let config = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(Some(1))),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "stateful-sequential".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };
        validate_job_config(&config)
    }

    #[test]
    fn rejects_slots_rollout_on_group_without_slot_task() {
        // Stateless group.
        let config = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(None)),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "stateless-sequential".into(),
            tasks: vec![TaskConfig {
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            }],
            version: None,
        };
        let err = validate_job_config(&config).expect_err("slots on a stateless group");
        assert!(
            err.to_string().contains("slot-mode stateful task"),
            "error should explain the slot-mode requirement: {err}"
        );

        // Keyed-mode stateful group is also not a slot pool.
        let keyed = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(None)),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "keyed-sequential".into(),
            tasks: vec![TaskConfig {
                stateful: Some(crate::structs::StatefulConfig {
                    mode: InstanceMode::Keyed,
                    require_id: false,
                    initial_instances: None,
                    on_lost: None,
                    storage: None,
                }),
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("sandbox", "exec")
            }],
            version: None,
        };
        let err = validate_job_config(&keyed).expect_err("slots on a keyed group");
        assert!(err.to_string().contains("slot-mode stateful task"));
    }

    #[test]
    fn rejects_update_from_at_or_above_slot_count() {
        // stateful_slot_task() has count 2; updateFrom 2 would update nothing.
        let config = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(Some(2))),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "updatefrom-too-high".into(),
            tasks: vec![stateful_slot_task()],
            version: None,
        };
        let err = validate_job_config(&config).expect_err("updateFrom >= count");
        assert!(
            err.to_string().contains("would update nothing"),
            "error should explain the no-op and suggest omission: {err}"
        );
    }

    #[test]
    fn rejects_global_slots_when_any_group_lacks_slot_task() {
        // Global slots apply to every group; the stateless `web` group makes
        // the spec invalid, with a pointer at rolloutByGroup scoping.
        let config = JobConfig {
            priority: Default::default(),
            rollout: Some(crate::structs::RolloutConfig {
                slots: Some(sequential_slots(None)),
                ..Default::default()
            }),
            rollout_by_group: Default::default(),
            name: "mixed-groups".into(),
            tasks: vec![
                stateful_slot_task(),
                TaskConfig {
                    group: "web".into(),
                    config: json!({"command": ["true"]}),
                    ..TaskConfig::new("api", "exec")
                },
            ],
            version: None,
        };
        let err = validate_job_config(&config).expect_err("global slots over a stateless group");
        assert!(
            err.to_string().contains("rolloutByGroup"),
            "error should point at per-group scoping: {err}"
        );

        // Scoping the slots block to the slot group fixes it.
        let scoped = JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: std::collections::BTreeMap::from([(
                "default".to_string(),
                crate::structs::RolloutConfig {
                    slots: Some(sequential_slots(None)),
                    ..Default::default()
                },
            )]),
            name: "mixed-groups-scoped".into(),
            tasks: vec![
                stateful_slot_task(),
                TaskConfig {
                    group: "web".into(),
                    config: json!({"command": ["true"]}),
                    ..TaskConfig::new("api", "exec")
                },
            ],
            version: None,
        };
        validate_job_config(&scoped).expect("group-scoped slots block is valid");
    }

    /// The path rule, at the level admission and the worker both call.
    #[test]
    fn file_destinations_resolve_against_the_task_dir_mount() {
        let root = "/legion/task";
        // Relative resolves against the mount.
        assert_eq!(
            resolve_task_dir_destination(root, "config/app.toml").expect("relative is in range"),
            Path::new("config/app.toml")
        );
        // Absolute-in-range keeps its remainder.
        assert_eq!(
            resolve_task_dir_destination(root, "/legion/task/identity/token")
                .expect("absolute in range"),
            Path::new("identity/token")
        );
        // `./` noise is normalized away.
        assert_eq!(
            resolve_task_dir_destination(root, "./a/./b").expect("normalizes"),
            Path::new("a/b")
        );

        // Out of range names the fix and the alternative.
        let err = resolve_task_dir_destination(root, "/var/run/legion/token")
            .expect_err("outside the mount");
        assert!(err.contains("/legion/task"), "{err}");
        assert!(err.contains("env delivery"), "{err}");

        // Traversal cannot climb out, however it is spelled.
        for escape in ["../escape", "a/../../escape", "/legion/task/../escape"] {
            assert!(
                resolve_task_dir_destination(root, escape).is_err(),
                "{escape} must be refused"
            );
        }
        // The mount point itself is a directory, not a destination.
        assert!(resolve_task_dir_destination(root, "/legion/task").is_err());
        assert!(resolve_task_dir_destination(root, "").is_err());
    }

    /// The declared roots per built-in runtime — the contract validation and
    /// the worker's translation both key off.
    #[test]
    fn the_builtin_runtimes_declare_their_file_delivery_roots() {
        assert_eq!(
            ContainerTaskPlugin.file_delivery_root(),
            Some("/legion/task")
        );
        // The exec runtime's filesystem is the host's: nothing to translate.
        assert_eq!(ExecTaskPlugin.file_delivery_root(), None);
        assert_eq!(LegacyStopPlugin::default().file_delivery_root(), None);
    }

    // --- resource enforcement: the interface contract, and the validation
    // rules that come with it ---

    /// The three modes, asserted generically over the trait, so the next
    /// adopter — in-tree or dylib — inherits this suite instead of writing
    /// one. Each mode is a promise about what worker core does on the
    /// plugin's behalf.
    #[test]
    fn the_enforcement_contract_holds_for_all_three_modes() {
        struct ModePlugin(ResourceEnforcement);
        impl TaskPlugin for ModePlugin {
            fn name(&self) -> &'static str {
                "mode-plugin"
            }
            fn resource_enforcement(&self) -> ResourceEnforcement {
                self.0
            }
        }

        let mut governor = crate::worker::resources::NodeResourceGovernor::disabled();
        let task = TaskConfig {
            resources: json!({ "memory": 64 }),
            ..TaskConfig::new("api", "mode-plugin")
        };

        for mode in [
            ResourceEnforcement::Unsupported,
            ResourceEnforcement::WorkerCgroup,
            ResourceEnforcement::RuntimeApplied,
        ] {
            let plugin = ModePlugin(mode);
            assert_eq!(plugin.resource_enforcement(), mode);
            // A disabled governor (the default worker) never assigns,
            // whatever the mode — the opt-in promise.
            assert!(governor
                .assign(1, &task, plugin.resource_enforcement(), None)
                .expect("disabled governor never fails")
                .is_none());
        }

        // And a plugin that never mentions enforcement is `Unsupported`,
        // which is what keeps unported dylib plugins behaving as today.
        assert_eq!(
            LegacyStopPlugin::default().resource_enforcement(),
            ResourceEnforcement::Unsupported
        );

        // `join_assignment_cgroup` is a no-op without an assignment, so a
        // `WorkerCgroup` plugin can call it unconditionally.
        let mut cmd = Command::new("true");
        crate::task_plugins::join_assignment_cgroup(&mut cmd, &TaskInstance::default())
            .expect("no assignment is not an error");
    }

    /// The built-ins' declared modes are part of the contract: exclusivity is
    /// only node-wide because `container` and `exec` share one hierarchy.
    #[test]
    fn the_builtin_runtimes_declare_their_modes() {
        assert_eq!(
            ContainerTaskPlugin.resource_enforcement(),
            ResourceEnforcement::RuntimeApplied
        );
        assert_eq!(
            ExecTaskPlugin.resource_enforcement(),
            ResourceEnforcement::WorkerCgroup
        );
        // The VMM process is the sandbox's host process.
        assert_eq!(
            SandboxTaskPlugin.resource_enforcement(),
            ResourceEnforcement::WorkerCgroup
        );
    }

    /// `cpus = 1` reads as one CPU but means 1 MHz: submit warns, naming the
    /// likely intent. Ordinary MHz values and `cores` are left alone.
    #[test]
    fn a_tiny_cpu_value_warns_that_cpu_is_mhz() {
        let config = |resources: serde_json::Value| {
            job_with(TaskConfig {
                resources,
                config: json!({"command": ["true"]}),
                ..TaskConfig::new("api", "exec")
            })
        };
        let warnings = resource_submit_warnings(&config(json!({ "cpus": 1 })));
        assert!(
            warnings
                .iter()
                .any(|line| line.contains("resources.cpus = 1") && line.contains("cpu = 1000")),
            "{warnings:?}"
        );
        let warnings = resource_submit_warnings(&config(json!({ "cpu": 0.5 })));
        assert!(
            warnings.iter().any(|line| line.contains("cpu = 500")),
            "{warnings:?}"
        );
        for quiet in [json!({ "cpu": 500 }), json!({ "cores": 1 }), json!({})] {
            let warnings = resource_submit_warnings(&config(quiet.clone()));
            assert!(
                warnings.iter().all(|line| !line.contains("MHz, not")),
                "{quiet}: {warnings:?}"
            );
        }
    }

    /// A cpuset cannot hold half a CPU. This was silently rounded on macOS
    /// and dropped on Linux before, so the rejection is a tightening — the
    /// message names both fixes.
    #[test]
    fn governed_runtimes_reject_fractional_cores() {
        let task = TaskConfig {
            resources: json!({ "cores": 1.5 }),
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        let err = validate_job_config(&job_with(task)).expect_err("half a core is not a cpuset");
        let message = err.to_string();
        assert!(message.contains("whole number"), "{message}");
        assert!(message.contains("cpu = 500"), "{message}");
        assert!(
            !message.contains("cpus = 0.5"),
            "cpus is MHz, not cores: {message}"
        );

        // Whole numbers are fine, on both governed runtimes.
        for (runtime, config) in [
            ("container", json!({"image": "alpine", "command": ["x"]})),
            ("exec", json!({"command": ["true"]})),
        ] {
            validate_job_config(&job_with(TaskConfig {
                resources: json!({ "cores": 2 }),
                config,
                ..TaskConfig::new("api", runtime)
            }))
            .expect("whole cores are valid");
        }
    }

    /// `cores` and `cpu` are two different CPU contracts; asking for both has
    /// no coherent meaning (Nomad's rule).
    #[test]
    fn governed_runtimes_reject_cores_and_cpu_together() {
        let task = TaskConfig {
            resources: json!({ "cores": 2, "cpu": 500 }),
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        let err = validate_job_config(&job_with(task)).expect_err("two CPU contracts");
        let message = err.to_string();
        assert!(message.contains("both"), "{message}");
        assert!(message.contains("cores"), "{message}");
    }

    /// Legion cannot promise exclusivity inside a cgroup it does not manage.
    #[test]
    fn exec_rejects_cores_with_an_explicit_cgroup() {
        let task = TaskConfig {
            resources: json!({ "cores": 1 }),
            config: json!({"command": ["true"], "cgroup": "workloads/demo"}),
            ..TaskConfig::new("api", "exec")
        };
        let err = validate_job_config(&job_with(task)).expect_err("cannot fence a foreign cgroup");
        let message = err.to_string();
        assert!(message.contains("config.cgroup"), "{message}");
        assert!(message.contains("cores"), "{message}");

        // An explicit cgroup on its own is still fine — it means what it has
        // always meant, and back-compat is the whole point.
        validate_job_config(&job_with(TaskConfig {
            resources: json!({ "memory": 64 }),
            config: json!({"command": ["true"], "cgroup": "workloads/demo"}),
            ..TaskConfig::new("api", "exec")
        }))
        .expect("an explicit cgroup alone stays valid");
    }

    /// `sandbox` became governed (its VMM runs in the worker cgroup), so it
    /// now gets the same admission rules as `exec` and `container`. It was
    /// the last built-in runtime that was not: ungoverned runtimes (`noop`,
    /// third-party plugins) still skip these rules, which
    /// `the_enforcement_contract_holds_for_all_three_modes` covers.
    #[test]
    fn sandbox_tasks_get_governed_resource_rules() {
        let err = validate_job_config(&job_with(TaskConfig {
            resources: json!({ "cores": 1.5 }),
            config: json!({"image": "alpine", "command": ["/bin/sh", "-c", "sleep 1"]}),
            ..TaskConfig::new("box", "sandbox")
        }))
        .expect_err("a fractional cpuset is rejected on a governed runtime");
        assert!(err.to_string().contains("whole number"), "{err}");
    }

    #[test]
    fn builtin_plugin_schemas_have_descriptions_and_defaults() -> R<()> {
        validate_builtin_task_plugin_schemas()?;
        Ok(())
    }

    // --- workload identity capability gating ---

    use crate::structs::workload_identity::{ChangeMode, WorkloadIdentity, WorkloadIdentityFile};

    fn job_with(task: TaskConfig) -> JobConfig {
        JobConfig {
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            name: "identity-job".into(),
            tasks: vec![task],
            version: None,
        }
    }

    fn file_identity(name: &str) -> WorkloadIdentity {
        WorkloadIdentity {
            name: name.into(),
            audience: vec!["sts.amazonaws.com".into()],
            file: Some(WorkloadIdentityFile {
                path: "/var/run/legion/identity/token".into(),
                mode: Some("0400".into()),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn exec_task_accepts_file_identity() -> R<()> {
        let task = TaskConfig {
            identities: vec![file_identity("aws")],
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("api", "exec")
        };
        validate_job_config(&job_with(task))
    }

    /// Identity file delivery works in containers now that the task dir is
    /// mounted — at a destination under the mount. The FHS-conventional path
    /// the exec runtime uses is not reachable from inside, and the error says
    /// so with the fix.
    #[test]
    fn container_task_scopes_identity_files_to_the_task_dir() -> R<()> {
        let err = validate_job_config(&job_with(TaskConfig {
            identities: vec![file_identity("aws")],
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        }))
        .expect_err("/var/run/legion/... is outside the mount");
        let message = err.to_string();
        assert!(message.contains("/legion/task"), "names the fix: {message}");
        assert!(
            message.contains("outside the container task directory"),
            "{message}"
        );

        // The absolute in-range form, and the relative form, both work.
        for path in ["/legion/task/identity/token", "identity/token"] {
            validate_job_config(&job_with(TaskConfig {
                identities: vec![WorkloadIdentity {
                    name: "aws".into(),
                    audience: vec!["sts.amazonaws.com".into()],
                    file: Some(WorkloadIdentityFile {
                        path: path.into(),
                        mode: Some("0400".into()),
                    }),
                    ..Default::default()
                }],
                config: json!({"image": "alpine", "command": ["echo", "hi"]}),
                ..TaskConfig::new("api", "container")
            }))
            .unwrap_or_else(|err| panic!("{path} should be accepted: {err}"));
        }
        Ok(())
    }

    /// The exec runtime is untouched: its filesystem *is* the host's, so a
    /// host-absolute destination stays valid. This is the one place a jobspec
    /// is not verbatim-portable between the two runtimes.
    #[test]
    fn exec_task_keeps_host_absolute_file_destinations() -> R<()> {
        validate_job_config(&job_with(TaskConfig {
            identities: vec![file_identity("aws")],
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("api", "exec")
        }))
    }

    #[test]
    fn container_task_accepts_env_identity_with_restart() -> R<()> {
        let identity = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Restart,
            ..Default::default()
        };
        let task = TaskConfig {
            identities: vec![identity],
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        validate_job_config(&job_with(task))
    }

    /// The container runtime signals the container's init through its runtime
    /// CLI, so a token can be refreshed with a signal instead of a full
    /// container restart. (This test asserted the opposite before the signal
    /// seam landed.)
    #[test]
    fn container_task_accepts_signal_change_mode() -> R<()> {
        let identity = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Signal,
            change_signal: Some("SIGHUP".into()),
            ..Default::default()
        };
        let task = TaskConfig {
            identities: vec![identity],
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        validate_job_config(&job_with(task))
    }

    /// A runtime with nothing to signal still rejects the mode at submit,
    /// rather than accepting a refresh contract it cannot honor.
    #[test]
    fn sandbox_task_still_rejects_signal_change_mode() {
        let identity = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Signal,
            change_signal: Some("SIGHUP".into()),
            ..Default::default()
        };
        let task = TaskConfig {
            identities: vec![identity],
            config: json!({"image": "alpine", "command": ["/bin/sh", "-c", "sleep 1"]}),
            ..TaskConfig::new("box", "sandbox")
        };
        let err = validate_job_config(&job_with(task)).expect_err("sandbox cannot signal");
        assert!(err.to_string().contains("signaling"), "{err}");
    }

    /// The trait default is the pre-existing worker behavior: signal the
    /// tracked pid. A plugin that never overrides `signal` keeps it, and a
    /// plugin with no tracked process gets an error naming the allocation
    /// rather than a silent no-op.
    #[test]
    fn default_signal_targets_the_tracked_pid() -> R<()> {
        let plugin = LegacyStopPlugin::default();
        let task = TaskConfig::new("legacy-task", "legacy-stop");

        let mut empty = TaskInstance::default();
        let err = plugin
            .signal(&alloc(), &task, &mut empty, "SIGHUP")
            .expect_err("no pid to signal");
        assert!(err.to_string().contains("no tracked pid"), "{err}");

        // Our own pid, with a signal that is a no-op for a process that does
        // not handle it: proves the default reaches kill(2) successfully.
        let mut instance = TaskInstance {
            pid: Some(std::process::id()),
            ..TaskInstance::default()
        };
        plugin.signal(&alloc(), &task, &mut instance, "SIGWINCH")?;
        Ok(())
    }

    #[test]
    fn sandbox_task_scopes_identity_files_to_the_guest_identity_dir() -> R<()> {
        let config = json!({"image": "alpine", "command": ["/bin/sh", "-c", "sleep 1"]});

        // Env delivery is baked into the guest env file at boot, so it works.
        let env_identity = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            change_mode: ChangeMode::Restart,
            ..Default::default()
        };
        validate_job_config(&job_with(TaskConfig {
            identities: vec![env_identity],
            config: config.clone(),
            ..TaskConfig::new("box", "sandbox")
        }))?;

        // File delivery reaches the guest under /legion/identity (the
        // identity disk on a microVM, a bind mount in dev mode). A host path
        // is refused with the fix.
        let err = validate_job_config(&job_with(TaskConfig {
            identities: vec![file_identity("aws")],
            config: config.clone(),
            ..TaskConfig::new("box", "sandbox")
        }))
        .expect_err("/var/run/... is not in the guest");
        let message = format!("{err:#}");
        assert!(
            message.contains("outside the sandbox identity directory")
                && message.contains("/legion/identity"),
            "{message}"
        );
        for path in ["/legion/identity/aws", "aws"] {
            validate_job_config(&job_with(TaskConfig {
                identities: vec![WorkloadIdentity {
                    file: Some(WorkloadIdentityFile {
                        path: path.into(),
                        mode: None,
                    }),
                    ..file_identity("aws")
                }],
                config: config.clone(),
                ..TaskConfig::new("box", "sandbox")
            }))
            .unwrap_or_else(|err| panic!("{path} should be accepted: {err:#}"));
        }
        Ok(())
    }

    /// Env with `changeMode = "noop"` (the default) is accepted on a
    /// memory-checkpointable sandbox, which keeps the token fresh at
    /// `/legion/identity/<ENV>`, and refused on one without an identity disk.
    #[test]
    fn env_noop_identities_need_a_sandbox_that_refreshes_them() -> R<()> {
        let identity = WorkloadIdentity {
            name: "vault".into(),
            audience: vec!["vault.internal".into()],
            env: Some("VAULT_TOKEN".into()),
            ..Default::default()
        };
        let mut checkpointable: TaskConfig = serde_json::from_value(json!({
            "name": "box",
            "runtime": "sandbox",
            "config": {"image": "alpine", "command": ["sleep", "1"]},
            "stateful": {"mode": "keyed"},
            "suspend": {"checkpoint": "memory"},
        }))?;
        checkpointable.identities = vec![identity.clone()];
        validate_job_config(&job_with(checkpointable))?;

        let err = validate_job_config(&job_with(TaskConfig {
            identities: vec![identity],
            config: json!({"image": "alpine", "command": ["sleep", "1"]}),
            ..TaskConfig::new("box", "sandbox")
        }))
        .expect_err("no identity disk to refresh it");
        assert!(format!("{err:#}").contains("can never"), "{err:#}");
        Ok(())
    }

    #[test]
    fn duplicate_identity_names_rejected_in_job_validation() {
        let task = TaskConfig {
            identities: vec![file_identity("aws"), file_identity("aws")],
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("api", "exec")
        };
        let err = validate_job_config(&job_with(task)).expect_err("duplicate identity name");
        assert!(err.to_string().contains("duplicate identity"), "{err}");
    }

    // --- artifact capability gating (artifact system plan §8) ---

    use crate::structs::ArtifactSpec;

    #[test]
    fn exec_task_accepts_artifacts_with_valid_references() -> R<()> {
        let task = TaskConfig {
            artifacts: vec![
                ArtifactSpec::from_source("https://releases.example.com/tool.tar.gz").named("tool"),
            ],
            config: json!({"command": ["${artifact.tool}/run", "--once"]}),
            ..TaskConfig::new("batch", "exec")
        };
        validate_job_config(&job_with(task))
    }

    #[test]
    fn container_task_accepts_artifacts() -> R<()> {
        let task = TaskConfig {
            artifacts: vec![ArtifactSpec::from_source("oci://ghcr.io/acme/extra:v1")],
            config: json!({"image": "alpine", "command": ["echo", "hi"]}),
            ..TaskConfig::new("api", "container")
        };
        validate_job_config(&job_with(task))
    }

    #[test]
    fn unknown_artifact_interpolation_fails_validation() {
        let task = TaskConfig {
            artifacts: vec![
                ArtifactSpec::from_source("https://releases.example.com/tool.tar.gz").named("tool"),
            ],
            config: json!({"command": ["${artifact.tool}/run", "--w", "${artifact.modle}/w"]}),
            ..TaskConfig::new("batch", "exec")
        };
        let err = validate_job_config(&job_with(task)).expect_err("typo'd reference");
        assert!(err.to_string().contains("artifact.modle"), "{err}");
    }

    #[test]
    fn noop_runtime_rejects_artifacts_when_plugin_available() {
        // The noop plugin is a dylib; gate the assertion on its presence like
        // the other noop tests.
        if std::env::var("LEGION_SKIP_PLUGIN_BUILD").is_ok() {
            return;
        }
        let path = task_plugin_name_to_path("noop");
        if !Path::new(&path).exists() {
            return;
        }
        let task = TaskConfig {
            artifacts: vec![ArtifactSpec::from_source("https://example.com/x.tar.gz")],
            config: json!({}),
            ..TaskConfig::new("noop-task", "noop")
        };
        let err = validate_job_config(&job_with(task)).expect_err("noop has no artifacts");
        assert!(
            err.to_string().contains("does not support artifacts"),
            "{err}"
        );
    }

    #[test]
    fn sandbox_task_accepts_artifacts() -> R<()> {
        // Unlike a container, a sandbox builds its own root filesystem, so
        // file/tree artifacts can actually be copied into the guest.
        let task = TaskConfig {
            artifacts: vec![
                ArtifactSpec::from_source("https://releases.example.com/tool.tar.gz").named("tool"),
            ],
            config: json!({
                "image": "alpine",
                "command": ["${artifact.tool}/run", "--once"],
            }),
            ..TaskConfig::new("box", "sandbox")
        };
        validate_job_config(&job_with(task))
    }

    #[test]
    fn container_implicit_artifacts_desugar_image() -> R<()> {
        let plugin = ContainerTaskPlugin;
        assert!(plugin.supports_artifacts());
        let implicit = plugin
            .implicit_artifacts(&json!({"image": "ghcr.io/acme/api:v42", "command": ["x"]}))?;
        assert_eq!(implicit.len(), 1);
        assert_eq!(implicit[0].source, "oci://ghcr.io/acme/api:v42");
        assert_eq!(implicit[0].name.as_deref(), Some("image"));

        // Docker Hub shorthand expands to a fully-qualified reference.
        let implicit = plugin.implicit_artifacts(&json!({"image": "alpine:latest"}))?;
        assert_eq!(implicit[0].source, "oci://docker.io/library/alpine:latest");
        Ok(())
    }
}
