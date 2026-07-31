use crate::artifacts::{ArtifactStager, ArtifactStatusBoard, CredentialSource, StagedArtifact};
use crate::prelude::*;
use crate::structs::{
    substitute_artifact_tokens, Allocation, AllocationHealth, StopCause, TaskAllocation,
    TaskConfig, TaskOnUnhealthyConfig, VolumeMount, VolumeReport, WorkerArtifactFetchConfigFull,
};
use crate::task_plugins::{
    ResourceAssignment, ResourceEnforcement, StopContext, TaskInstance, TaskPluginRegistry,
};
use crate::worker::log_sink::{default_sinks, LogResource, LogSink};
use crate::worker::logs::{read_allocation_logs, CapturedStream};
use crate::worker::resources::NodeResourceGovernor;
use crate::worker::state::{pid_is_running, PersistedAllocation, WorkerStateStore};
use crate::worker::task_health::{spawn_health_monitor, HealthMonitorHandle};
use crate::worker::volumes::{volume_env, WorkerVolumeManager};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct FailedAllocationReport {
    pub allocation_id: i64,
    pub reason: String,
    /// Final per-artifact staging states (JSON) when the failure was an
    /// artifact staging failure; rides the heartbeat failure report so the
    /// manager can emit per-artifact `artifact.failed` events.
    pub artifact_states: Option<String>,
}

/// A terminal (non-failure) allocation outcome the worker owes the manager:
/// the task's process exited and the task's `restart` policy says this is the
/// end of it. Failures travel the older [`FailedAllocationReport`] path
/// instead, because the manager already reschedules those.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminatedAllocationReport {
    pub allocation_id: i64,
    /// Exit status was in the task's `successExitCodes`.
    pub success: bool,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    /// Human-readable, e.g. `exited with status 0`.
    pub message: String,
}

/// One observed task-process exit, as seen by the worker's supervisor poll.
/// `exit_code`/`signal` are both `None` when the worker only knew the pid (an
/// allocation re-adopted after a worker restart): the process is gone, but its
/// status died with the parent that could have reaped it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskExit {
    pub allocation_id: i64,
    pub task_name: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    /// The declared memory limit, in bytes, when the kernel OOM-killed
    /// something in this task's cgroup before it exited.
    ///
    /// Read from `memory.events` at exit for governed allocations, because
    /// exit code 137 on its own sends the job author hunting through dmesg —
    /// this is what lets [`Self::describe`] name the limit that fired.
    pub oom_killed_at_bytes: Option<u64>,
}

impl TaskExit {
    /// Human-readable rendering used in health status, failure reasons, and
    /// the terminal report.
    pub fn describe(&self) -> String {
        let status = match (self.exit_code, self.signal) {
            (Some(code), _) => format!("exited with status {code}"),
            (None, Some(signal)) => format!("killed by signal {signal}"),
            (None, None) => "process is gone (exit status unknown)".to_string(),
        };
        match self.oom_killed_at_bytes {
            Some(bytes) => format!(
                "{status}; task exceeded its {} MiB memory limit and was OOM-killed",
                bytes / (1024 * 1024)
            ),
            None => status,
        }
    }
}

/// One allocation inside its grace window: signalled, not yet reaped.
///
/// Everything the teardown needs is moved in here, because the allocation is
/// already out of the live maps by the time it lands.
pub struct TerminatingAllocation {
    allocation: Allocation,
    task: TaskConfig,
    instance: TaskInstance,
    /// Why it is stopping, for the reap log line.
    cause: StopCause,
    /// When the grace window closes and the reaper hard-destroys it.
    deadline: std::time::Instant,
    /// Whether teardown removes the allocation directory (false for an
    /// in-place re-dispatch that will reuse the staged artifacts).
    remove_dirs: bool,
}

/// Why [`WorkerTaskManager::reap_terminating`] finished an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapOutcome {
    /// The task exited inside its grace window — the good path.
    Exited,
    /// The grace window closed with the process still running, so it was
    /// SIGKILLed.
    Killed,
}

/// A staging job handed to a dedicated thread: everything needed to fetch a
/// task's artifacts without holding the task-manager lock.
pub struct StagingJob {
    pub allocation: Allocation,
    pub task: TaskConfig,
    pub task_dir: PathBuf,
    stager: Arc<ArtifactStager>,
    board: ArtifactStatusBoard,
}

/// What `begin_task_allocation` did with a new allocation.
pub enum BeginOutcome {
    /// No artifacts: the task was prepared and started synchronously.
    Started(Box<(Allocation, TaskConfig)>),
    /// Artifacts declared: the allocation is registered as
    /// `staging artifacts` and the caller must run the staging job on a
    /// blocking thread (see [`launch_task_allocation`]).
    Staging(Box<StagingJob>),
}

pub struct WorkerTaskManager {
    registry: TaskPluginRegistry,
    instances: HashMap<i64, TaskInstance>,
    allocations: HashMap<i64, Allocation>,
    task_configs: HashMap<i64, TaskConfig>,
    health_monitors: HashMap<i64, HealthMonitorHandle>,
    allocation_health: HashMap<i64, AllocationHealth>,
    failed: Vec<FailedAllocationReport>,
    /// Terminal (non-failure) outcomes owed to the manager on the next
    /// heartbeat.
    terminated_reports: Vec<TerminatedAllocationReport>,
    /// Allocations whose process exited and will not be relaunched here. They
    /// stay in `instances`/`allocations` so the heartbeat keeps reporting them
    /// as this node's (the manager must not re-dispatch a finished allocation
    /// onto us), but the supervisor never polls them again.
    terminated: HashMap<i64, TaskExit>,
    /// Local restart attempts per allocation, shared by *both* restart paths:
    /// health-check failures and process exits. One budget, so a flapping task
    /// gets `on_unhealthy.restart_count` restarts in total rather than that
    /// many per path.
    restart_history: HashMap<i64, Vec<std::time::Instant>>,
    // Per-allocation self-fence windows (stateful tasks only; see
    // `TaskAllocation.self_terminate_after_seconds`). The watchdog kills any
    // allocation whose window is exceeded by the time since the last
    // successful manager heartbeat.
    self_fence_windows: HashMap<i64, std::time::Duration>,
    /// Why the manager is stopping each allocation, from the most recent
    /// heartbeat reply (see `HeartbeatReply.stop_notices`).
    ///
    /// Advisory: an allocation removed from the desired set with no entry
    /// here stops with [`StopCause::Unknown`] rather than waiting for one.
    /// An entry is consumed by the stop it describes; everything else is
    /// bounded by [`Self::set_stop_causes`] replacing the map wholesale on
    /// each heartbeat, so this worker holds exactly what the manager
    /// currently knows and needs no expiry logic of its own.
    stop_causes: HashMap<i64, StopCause>,
    /// Who displaced each preempted allocation, from the same heartbeat feed
    /// as [`Self::stop_causes`]. Written into the stop notice so a task can
    /// tell "the platform wanted my capacity for job X" from "my operator
    /// stopped me", which is the bidirectional link Kueue had to retrofit.
    /// Only ever populated for [`StopCause::Preempted`].
    preemptors: HashMap<i64, crate::structs::StopNoticePreemptor>,
    /// Allocations that have been signalled and are inside their grace
    /// window. They are out of `instances`/`allocations` already (the manager
    /// no longer wants them, and the heartbeat must not report them as this
    /// node's running work), but their process is still alive and their
    /// storage is still mounted.
    ///
    /// This set is the load-bearing piece of graceful stop. `stop()` cannot
    /// simply wait for a deadline: `reconcile_allocations` runs under this
    /// struct's mutex inside the async heartbeat loop, so a blocking stop
    /// would hold the lock heartbeats, the self-fence watchdog and health
    /// reporting all need — at a 2s cadence a 30s grace would let the manager
    /// declare the node lost and reschedule everything on it. So the stop
    /// *signals and returns*, and [`Self::reap_terminating`] finishes the job
    /// from the supervisor loop.
    terminating: HashMap<i64, TerminatingAllocation>,
    state_store: WorkerStateStore,
    /// Root of per-allocation directories: `<state dir>/allocs/<alloc_id>/<task>/`.
    alloc_root: PathBuf,
    artifact_status: ArtifactStatusBoard,
    stager: Arc<ArtifactStager>,
    log_sinks: Vec<Box<dyn LogSink>>,
    /// Volume drivers plus the mounts held for each allocation. Mounting
    /// happens here, between `prepare_instance` and `start`, so a task can never
    /// start before its storage is real (see docs/volumes.md).
    ///
    /// Behind an `Arc` so the daemon can clone a handle out from under the
    /// task-manager lock and run slow driver operations (destroys against a
    /// hung NFS server, say) without chaining that lock — which the heartbeat
    /// path needs — to a driver's fate.
    volumes: Arc<WorkerVolumeManager>,
    /// Resolved mount lists per allocation, kept so a worker restart can
    /// re-mount exactly what the manager last dispatched.
    volume_specs: HashMap<i64, Vec<VolumeMount>>,
    /// The node's CPU/memory accounting and cgroup hierarchy (see
    /// `src/worker/resources.rs`). Disabled unless the worker was started
    /// with `resourceEnforcement.enabled`, in which case it governs every
    /// runtime that opted into the enforcement interface.
    resources: NodeResourceGovernor,
}

impl Default for WorkerTaskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerTaskManager {
    pub fn new() -> Self {
        Self::with_state_store(WorkerStateStore::new(WorkerStateStore::default_path()))
    }

    pub fn with_state_store(state_store: WorkerStateStore) -> Self {
        let alloc_root = Self::alloc_root_for(&state_store);
        // Volume data lives beside the alloc dirs (`<state dir>/volumes/`), not
        // inside them: alloc dirs are wiped on stop, and persistent storage
        // that vanished with its allocation would defeat the whole point.
        let alloc_root_parent = alloc_root
            .parent()
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| alloc_root.clone());
        Self {
            registry: TaskPluginRegistry::new(),
            instances: HashMap::new(),
            allocations: HashMap::new(),
            task_configs: HashMap::new(),
            health_monitors: HashMap::new(),
            allocation_health: HashMap::new(),
            failed: Vec::new(),
            terminated_reports: Vec::new(),
            terminated: HashMap::new(),
            restart_history: HashMap::new(),
            self_fence_windows: HashMap::new(),
            stop_causes: HashMap::new(),
            preemptors: HashMap::new(),
            terminating: HashMap::new(),
            state_store,
            alloc_root,
            artifact_status: ArtifactStatusBoard::new(),
            stager: Arc::new(ArtifactStager::new(
                WorkerArtifactFetchConfigFull::default(),
                Box::new(crate::artifacts::AnonymousCredentials),
            )),
            log_sinks: default_sinks(),
            volumes: Arc::new(WorkerVolumeManager::new(
                String::new(),
                crate::volumes::VolumeRuntimeConfig {
                    volume_root: alloc_root_parent.join("volumes"),
                    data_dir: alloc_root_parent,
                    host_volumes: HashMap::new(),
                    plugin_dir: crate::util::plugin_root_dir(),
                },
            )),
            volume_specs: HashMap::new(),
            resources: NodeResourceGovernor::from_env(),
        }
    }

    /// Build the [`LogResource`] describing an allocation's captured logs.
    fn log_resource(&self, allocation: &Allocation, task: &TaskConfig) -> LogResource {
        LogResource {
            allocation_id: allocation.id,
            job_id: allocation.job_id.clone(),
            node_id: allocation.node_id.clone(),
            task_name: task.name.clone(),
            task_runtime: task.runtime.clone(),
            log_dir: self.state_store.allocation_log_dir(allocation.id),
        }
    }

    /// Notify every log sink that an allocation has started (writes manifests,
    /// etc.). Failures are logged, never fatal to task startup.
    fn notify_log_sinks_started(&self, allocation: &Allocation, task: &TaskConfig) {
        let resource = self.log_resource(allocation, task);
        for sink in &self.log_sinks {
            if let Err(err) = sink.on_start(&resource) {
                warn!(
                    allocation_id = allocation.id,
                    sink = sink.name(),
                    error = ?err,
                    "Log sink on_start failed"
                );
            }
        }
    }

    /// Run rotation/retention (and any other sink upkeep) across all active
    /// allocations. Invoked periodically by the worker maintenance loop.
    pub fn maintain_logs(&self) {
        for (allocation_id, allocation) in &self.allocations {
            let Some(task) = self.task_configs.get(allocation_id) else {
                continue;
            };
            let resource = self.log_resource(allocation, task);
            for sink in &self.log_sinks {
                if let Err(err) = sink.maintain(&resource) {
                    warn!(
                        allocation_id = *allocation_id,
                        sink = sink.name(),
                        error = ?err,
                        "Log sink maintenance failed"
                    );
                }
            }
        }
    }

    /// Task directories live beside the worker state file:
    /// `.legion/workers/<id>/allocs/`.
    fn alloc_root_for(state_store: &WorkerStateStore) -> PathBuf {
        let root = state_store
            .path()
            .parent()
            .map(|parent| parent.join("allocs"))
            .unwrap_or_else(|| PathBuf::from("allocs"));
        // Task dirs become env vars, interpolated `${artifact.*}` paths, and
        // the exec cwd — all of which break if they are relative (a command
        // resolved against a cwd that is itself the task dir). Absolutize
        // once here.
        if root.is_absolute() {
            root
        } else {
            std::env::current_dir()
                .map(|cwd| cwd.join(&root))
                .unwrap_or(root)
        }
    }

    /// Wire up the volume drivers once the worker knows its node id and the
    /// operator's `hostVolumes` declarations (called after pre-registration,
    /// before any allocation can arrive).
    pub fn configure_volumes(
        &mut self,
        node_id: &str,
        host_volumes: HashMap<String, crate::volumes::HostVolumeConfig>,
    ) {
        let data_dir = self
            .alloc_root
            .parent()
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| self.alloc_root.clone());
        self.volumes = Arc::new(WorkerVolumeManager::new(
            node_id,
            crate::volumes::VolumeRuntimeConfig {
                volume_root: data_dir.join("volumes"),
                data_dir,
                host_volumes,
                plugin_dir: crate::util::plugin_root_dir(),
            },
        ));
        info!(
            node_id,
            drivers = ?self.volumes.driver_names(),
            "Loaded volume drivers"
        );
    }

    /// Volume plugin capabilities for this node's fingerprint. The manager
    /// learns what storage exists in the cluster only from this.
    pub fn volume_fingerprint(&self) -> Value {
        self.volumes.fingerprint()
    }

    /// Volume operation outcomes for the next heartbeat.
    pub fn drain_volume_reports(&mut self) -> Vec<VolumeReport> {
        self.volumes.drain_reports()
    }

    /// Put drained reports back after a failed heartbeat. A lost destroy
    /// confirmation would leave the manager re-dispatching the same reclaim
    /// forever, so reports are never dropped on a dead connection.
    pub fn requeue_volume_reports(&mut self, reports: Vec<VolumeReport>) {
        self.volumes.requeue_reports(reports);
    }

    /// A handle to the volume manager for work that must NOT run under the
    /// task-manager lock — reclaim dispatch, whose driver calls can stall on a
    /// dead storage backend. Cheap to clone; see the field docs.
    pub fn volumes_handle(&self) -> Arc<WorkerVolumeManager> {
        self.volumes.clone()
    }

    /// Applies the worker's artifact fetch settings and credential source
    /// (called once after pre-registration delivers the worker config).
    pub fn configure_artifacts(
        &mut self,
        config: WorkerArtifactFetchConfigFull,
        credentials: Box<dyn CredentialSource>,
    ) {
        self.stager = Arc::new(ArtifactStager::new(config, credentials));
    }

    pub fn load_builtin_plugins(&mut self) {
        if let Err(err) = self.registry.load_plugin("exec", true) {
            error!(?err, "Failed to load exec task plugin");
        }
        if let Err(err) = self.registry.load_plugin("container", true) {
            error!(?err, "Failed to load container task plugin");
        }
        if let Err(err) = self.registry.load_plugin("sandbox", true) {
            error!(?err, "Failed to load sandbox task plugin");
        }
        if let Err(err) = self.registry.load_plugin("noop", true) {
            debug!(?err, "Noop task plugin not available");
        }
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        self.registry.readiness_checks()
    }

    pub fn allocations_snapshot(&self) -> Vec<i64> {
        let mut allocations = self.instances.keys().copied().collect::<Vec<_>>();
        allocations.sort_unstable();
        allocations
    }

    /// Whether this worker currently runs the allocation (used by identity
    /// refresh loops to stop once the allocation is gone).
    pub fn has_allocation(&self, allocation_id: i64) -> bool {
        self.instances.contains_key(&allocation_id)
    }

    /// The tracked process id for an allocation, if any.
    pub fn allocation_pid(&self, allocation_id: i64) -> Option<u32> {
        self.instances
            .get(&allocation_id)
            .and_then(|instance| instance.pid)
    }

    /// Read a single env value from the stored task config (test/diagnostic
    /// helper; values may be secrets — never log them).
    pub fn task_env(&self, allocation_id: i64, key: &str) -> Option<String> {
        self.task_configs
            .get(&allocation_id)?
            .config
            .get("env")?
            .get(key)?
            .as_str()
            .map(str::to_string)
    }

    /// Update one env var in the *stored* task config so any subsequent
    /// (re)start launches with the new value. The live process env cannot
    /// change — that is exactly why env-delivered identities pair with
    /// `changeMode: signal`/`restart` (see src/worker/identity.rs).
    /// `value` may be a secret: it is never logged here.
    pub fn update_task_env(&mut self, allocation_id: i64, key: &str, value: &str) -> R<()> {
        let task = self
            .task_configs
            .get_mut(&allocation_id)
            .context("Allocation not found for env update")?;
        let config = task
            .config
            .as_object_mut()
            .context("Task config is not an object; cannot update env")?;
        let env = config
            .entry("env")
            .or_insert_with(|| Value::Object(Default::default()));
        let env = env
            .as_object_mut()
            .context("Task config env is not an object")?;
        env.insert(key.to_string(), Value::String(value.to_string()));
        self.persist_state();
        Ok(())
    }

    /// Deliver a Unix signal (e.g. "SIGHUP") to the allocation — the
    /// `changeMode: signal` pathway for refreshed identity tokens.
    ///
    /// Routed through the task runtime, not `kill(2)`: for an isolating
    /// runtime the tracked pid is a supervisor, and only the plugin knows how
    /// to reach the workload (see [`crate::task_plugins::TaskPlugin::signal`]).
    /// The default trait impl is exactly the old behavior, so host-process
    /// runtimes are unaffected.
    pub fn signal_allocation(&mut self, allocation_id: i64, signal: &str) -> R<()> {
        let allocation = self
            .allocations
            .get(&allocation_id)
            .cloned()
            .context("Allocation not found for signal")?;
        let task = self
            .task_configs
            .get(&allocation_id)
            .cloned()
            .context("Task config missing for signal")?;
        let plugin = self
            .registry
            .plugin(&task.runtime)
            .with_context(|| format!("Task plugin {} not loaded for signal", task.runtime))?;
        let instance = self
            .instances
            .get_mut(&allocation_id)
            .context("Task instance missing for signal")?;
        plugin
            .signal(&allocation, &task, instance, signal)
            .with_context(|| format!("Failed to deliver {signal} to allocation {allocation_id}"))?;
        info!(
            allocation_id,
            signal,
            task_runtime = %task.runtime,
            "Signaled allocation"
        );
        Ok(())
    }

    pub fn allocation_health_snapshot(&self) -> Vec<AllocationHealth> {
        let mut entries = self.allocation_health.values().cloned().collect::<Vec<_>>();
        for entry in &mut entries {
            entry.artifact_states = self.artifact_status.snapshot_json(entry.allocation_id);
        }
        entries.sort_by_key(|entry| entry.allocation_id);
        entries
    }

    /// Poll every supervised, non-terminal allocation for process exit.
    ///
    /// This is also where task children are **reaped**: `try_wait` on the
    /// handle we own collects the exit status and stops the zombie from
    /// outliving the task. Allocations we only know by pid (re-adopted after a
    /// worker restart) are checked with a liveness probe instead — their status
    /// is unknowable, so they are reported as an exit with no code.
    pub fn poll_task_exits(&mut self) -> Vec<TaskExit> {
        let mut exits = Vec::new();
        let candidates = self
            .instances
            .keys()
            .copied()
            .filter(|allocation_id| !self.terminated.contains_key(allocation_id))
            // An allocation we are deliberately draining is not a task that
            // crashed: its exit must not feed the restart policy.
            .filter(|allocation_id| !self.terminating.contains_key(allocation_id))
            .collect::<Vec<_>>();

        for allocation_id in candidates {
            let Some(task) = self.task_configs.get(&allocation_id) else {
                continue;
            };
            let task_name = task.name.clone();
            let supervised = self
                .registry
                .plugin(&task.runtime)
                .is_some_and(|plugin| plugin.supervises_process());
            if !supervised {
                continue;
            }
            let Some(instance) = self.instances.get_mut(&allocation_id) else {
                continue;
            };

            let exit = if let Some(child) = instance.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        instance.pid = None;
                        instance.child = None;
                        Some(exit_from_status(&status))
                    }
                    Ok(None) => None,
                    Err(err) => {
                        warn!(
                            allocation_id,
                            task_name = %task_name,
                            error = ?err,
                            "Failed to poll task process; treating it as still running"
                        );
                        None
                    }
                }
            } else if let Some(pid) = instance.pid {
                if pid_is_running(pid) {
                    None
                } else {
                    instance.pid = None;
                    Some((None, None))
                }
            } else {
                // Nothing started yet (staging artifacts), or a runtime that
                // reports no process. Neither is an exit.
                None
            };

            if let Some((exit_code, signal)) = exit {
                // Read the cgroup's OOM counter *before* teardown removes it.
                // Best-effort and cheap: one small read, and only for
                // governed allocations that carry a memory limit.
                let oom_killed_at_bytes =
                    instance
                        .resource_assignment
                        .as_ref()
                        .and_then(|assignment| {
                            let limit = assignment.memory_limit_bytes?;
                            (crate::worker::resources::oom_kill_count(assignment) > 0)
                                .then_some(limit)
                        });
                exits.push(TaskExit {
                    allocation_id,
                    task_name,
                    exit_code,
                    signal,
                    oom_killed_at_bytes,
                });
            }
        }

        for exit in &exits {
            self.terminated.insert(exit.allocation_id, exit.clone());
        }
        exits
    }

    /// The task config for a live allocation, if this worker still holds it.
    pub fn task_config(&self, allocation_id: i64) -> Option<&TaskConfig> {
        self.task_configs.get(&allocation_id)
    }

    /// Clear an allocation's terminal flag so the supervisor watches it again.
    /// Called when a restart relaunches the process.
    pub fn clear_termination(&mut self, allocation_id: i64) {
        self.terminated.remove(&allocation_id);
    }

    /// Whether an allocation may be restarted locally right now, against the
    /// shared budget (`count` restarts within `period`). Prunes attempts that
    /// have aged out of the window.
    pub fn can_restart_allocation(
        &mut self,
        allocation_id: i64,
        count: u32,
        period: std::time::Duration,
    ) -> bool {
        if count == 0 {
            return false;
        }
        let now = std::time::Instant::now();
        let history = self.restart_history.entry(allocation_id).or_default();
        history.retain(|attempt| now.duration_since(*attempt) <= period);
        (history.len() as u32) < count
    }

    /// Record a restart attempt against the shared budget.
    pub fn record_restart_attempt(&mut self, allocation_id: i64) {
        self.restart_history
            .entry(allocation_id)
            .or_default()
            .push(std::time::Instant::now());
    }

    /// Queue a terminal (non-failure) outcome for the next heartbeat and mark
    /// the allocation done locally.
    pub fn record_termination(&mut self, allocation_id: i64, exit: &TaskExit, success: bool) {
        if let Some(monitor) = self.health_monitors.remove(&allocation_id) {
            monitor.shutdown();
        }
        self.terminated.insert(allocation_id, exit.clone());
        self.set_allocation_health(allocation_id, success, exit.describe());
        self.terminated_reports.push(TerminatedAllocationReport {
            allocation_id,
            success,
            exit_code: exit.exit_code,
            signal: exit.signal,
            message: exit.describe(),
        });
        self.persist_state();
    }

    /// Report a batch task on an unsupervisable runtime as complete at start.
    /// See the call site in `start_prepared_allocation`.
    fn record_unsupervised_completion(&mut self, allocation_id: i64, runtime: &str) {
        let message =
            format!("runtime `{runtime}` reports no exit status; treated as complete at start");
        self.terminated.insert(
            allocation_id,
            TaskExit {
                allocation_id,
                task_name: String::new(),
                exit_code: None,
                signal: None,
                oom_killed_at_bytes: None,
            },
        );
        self.set_allocation_health(allocation_id, true, message.clone());
        self.terminated_reports.push(TerminatedAllocationReport {
            allocation_id,
            success: true,
            exit_code: None,
            signal: None,
            message,
        });
    }

    pub fn drain_terminations(&mut self) -> Vec<TerminatedAllocationReport> {
        std::mem::take(&mut self.terminated_reports)
    }

    /// Put drained terminal reports back after a failed heartbeat, so a dead
    /// connection cannot swallow a batch task's completion.
    pub fn requeue_terminations(&mut self, reports: Vec<TerminatedAllocationReport>) {
        self.terminated_reports.extend(reports);
    }

    pub fn drain_failures(&mut self) -> Vec<FailedAllocationReport> {
        std::mem::take(&mut self.failed)
    }

    fn alloc_dir(&self, allocation_id: i64) -> PathBuf {
        self.alloc_root.join(allocation_id.to_string())
    }

    /// Where an allocation's declared file destinations land on the host,
    /// for a runtime that mounts its task directory somewhere else.
    ///
    /// `Some((in_task_root, host_task_dir))` for such a runtime (the
    /// `container` runtime's `/legion/task`), `None` for one whose filesystem
    /// *is* the host's (`exec`), where a declared absolute path is already
    /// the real path. Used by identity delivery, which writes token files
    /// before the task starts and must put them where the workload will
    /// actually look.
    pub fn file_delivery_base(
        &mut self,
        allocation_id: i64,
        task_name: &str,
        runtime: &str,
    ) -> Option<(&'static str, PathBuf)> {
        if self.registry.load_plugin(runtime, false).is_err() {
            return None;
        }
        let root = self.registry.plugin(runtime)?.file_delivery_root()?;
        Some((root, self.alloc_dir(allocation_id).join(task_name)))
    }

    fn cleanup_alloc_dir(&self, allocation_id: i64) {
        let dir = self.alloc_dir(allocation_id);
        if dir.exists() {
            if let Err(err) = std::fs::remove_dir_all(&dir) {
                warn!(
                    allocation_id,
                    dir = %dir.display(),
                    error = ?err,
                    "Failed to remove allocation directory"
                );
            }
        }
    }

    /// Creates the allocation + task directories and returns a task instance
    /// pre-wired with dirs and the `LEGION_ALLOC_DIR`/`LEGION_TASK_DIR` env.
    fn prepare_instance(&self, allocation: &Allocation, task: &TaskConfig) -> R<TaskInstance> {
        let alloc_dir = self.alloc_dir(allocation.id);
        let task_dir = alloc_dir.join(&task.name);
        std::fs::create_dir_all(&task_dir)
            .with_context(|| format!("create task directory {}", task_dir.display()))?;
        // Owner-only: the alloc dir is the isolation boundary between
        // allocations sharing a node — other tasks must not be able to reach
        // this allocation's artifacts, injected secrets, or output.
        crate::util::restrict_dir_to_owner(&alloc_dir)?;
        crate::util::restrict_dir_to_owner(&task_dir)?;
        let mut instance = TaskInstance::default();
        instance
            .extra_env
            .insert("LEGION_ALLOC_DIR".into(), alloc_dir.display().to_string());
        instance
            .extra_env
            .insert("LEGION_TASK_DIR".into(), task_dir.display().to_string());
        // Exported at start, written at stop: a task that wants to react
        // programmatically to being stopped reads this path from its own
        // environment and needs no other coordination.
        instance.extra_env.insert(
            crate::structs::STOP_NOTICE_PATH_ENV.into(),
            task_dir
                .join(crate::structs::STOP_NOTICE_FILE)
                .display()
                .to_string(),
        );
        instance.alloc_dir = Some(alloc_dir);
        instance.task_dir = Some(task_dir);
        Ok(instance)
    }

    pub fn record_failure(&mut self, allocation_id: i64, reason: impl Into<String>) {
        if let Some(monitor) = self.health_monitors.remove(&allocation_id) {
            monitor.shutdown();
        }
        if let (Some(allocation), Some(task)) = (
            self.allocations.get(&allocation_id),
            self.task_configs.get(&allocation_id),
        ) {
            if let Some(plugin) = self.registry.plugin(&task.runtime) {
                if let Some(instance) = self.instances.get_mut(&allocation_id) {
                    if let Err(err) = plugin.destroy(allocation, task, instance) {
                        warn!(
                            allocation_id,
                            task_name = %task.name,
                            error = ?err,
                            "Failed to destroy allocation instance"
                        );
                    }
                }
            }
        }
        self.volumes.unmount_for_allocation(allocation_id);
        let assignment = self
            .instances
            .get(&allocation_id)
            .and_then(|instance| instance.resource_assignment.clone());
        self.resources.release(allocation_id, assignment.as_ref());
        self.volume_specs.remove(&allocation_id);
        self.instances.remove(&allocation_id);
        self.allocations.remove(&allocation_id);
        self.task_configs.remove(&allocation_id);
        self.allocation_health.remove(&allocation_id);
        self.self_fence_windows.remove(&allocation_id);
        self.terminated.remove(&allocation_id);
        self.terminating.remove(&allocation_id);
        self.restart_history.remove(&allocation_id);
        // A task that failed on its own was never stopped by the platform,
        // so any pending notice for it describes a stop that will not happen.
        self.stop_causes.remove(&allocation_id);
        // Capture the final board snapshot before clearing it: the failure
        // report is the last (and race-free) carrier of per-artifact failure
        // detail to the manager's event stream.
        let artifact_states = self.artifact_status.snapshot_json(allocation_id);
        self.artifact_status.remove(allocation_id);
        self.cleanup_alloc_dir(allocation_id);
        self.failed.push(FailedAllocationReport {
            allocation_id,
            reason: reason.into(),
            artifact_states,
        });
        self.persist_state();
    }

    /// Allocations whose self-fence window is exceeded by
    /// `since_last_contact` (time since the last successful manager
    /// heartbeat), with their windows. Pure decision logic — separated from
    /// [`Self::self_fence_expired_allocations`] so it is unit-testable
    /// without clocks or sleeps. Allocations without a window (stateless)
    /// are never candidates.
    pub fn fence_candidates(
        &self,
        since_last_contact: std::time::Duration,
    ) -> Vec<(i64, std::time::Duration)> {
        let mut candidates = self
            .self_fence_windows
            .iter()
            .filter(|(_, window)| since_last_contact > **window)
            .map(|(allocation_id, window)| (*allocation_id, *window))
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|(allocation_id, _)| *allocation_id);
        candidates
    }

    /// Self-fence every stateful allocation whose window has been exceeded:
    /// kill it locally (the same destroy path failed allocations take) and
    /// record it as failed, so the post-reconnect heartbeat reports it
    /// failed/stopped rather than running. This is the worker half of the
    /// at-most-one guarantee (see docs/stateful-workloads.md "Node loss and
    /// fencing"). Returns the fenced allocation ids.
    pub fn self_fence_expired_allocations(
        &mut self,
        since_last_contact: std::time::Duration,
    ) -> Vec<i64> {
        let candidates = self.fence_candidates(since_last_contact);
        // Whole seconds only: sub-second noise helps nobody in fence logs.
        let lost_for = std::time::Duration::from_secs(since_last_contact.as_secs());
        let mut fenced = Vec::with_capacity(candidates.len());
        for (allocation_id, window) in candidates {
            warn!(
                allocation_id,
                window_secs = window.as_secs(),
                lost_contact_secs = lost_for.as_secs(),
                "self-fencing allocation {allocation_id}: lost manager contact for {}",
                humantime::format_duration(lost_for)
            );
            self.record_failure(
                allocation_id,
                format!(
                    "self-fenced: worker lost manager contact for {} \
                     (> onLost.selfTerminateAfter {})",
                    humantime::format_duration(lost_for),
                    humantime::format_duration(window)
                ),
            );
            fenced.push(allocation_id);
        }
        fenced
    }

    pub fn set_allocation_health(
        &mut self,
        allocation_id: i64,
        healthy: bool,
        status: impl Into<String>,
    ) {
        self.allocation_health.insert(
            allocation_id,
            AllocationHealth {
                allocation_id,
                healthy,
                status: status.into(),
                artifact_states: None,
            },
        );
    }

    /// Stops allocations the manager no longer wants and returns the new
    /// allocations the caller should hand to [`launch_task_allocation`]
    /// (staging-aware launch must happen without holding this lock).
    ///
    /// **Stops order before starts.** Removals are applied first, and while
    /// anything on this node is still inside its grace window no new
    /// allocation is launched. That is what makes plan-carried eviction safe
    /// physically: the manager's plan is atomic *logically* (victims and
    /// preemptor commit in one transaction), and this serializes the physical
    /// handover, so the node is never running victim and preemptor
    /// concurrently past the grace window. The cost — a new allocation
    /// landing on a node with slow-stopping neighbours inherits their grace
    /// as start latency — is bounded by the `stop.killAfter` cap and is
    /// recorded as a judgement call.
    ///
    /// Held-back allocations are not lost: they stay in the manager's desired
    /// set, so the next heartbeat's reconcile returns them again.
    pub fn reconcile_allocations(&mut self, allocations: &[TaskAllocation]) -> Vec<TaskAllocation> {
        let desired_ids = allocations
            .iter()
            .map(|allocation| allocation.allocation_id)
            .collect::<std::collections::HashSet<_>>();
        let existing_ids = self.instances.keys().copied().collect::<Vec<_>>();
        for allocation_id in existing_ids {
            if desired_ids.contains(&allocation_id) {
                continue;
            }
            if let Err(err) = self.stop_allocation(allocation_id) {
                warn!(
                    allocation_id,
                    error = ?err,
                    "Failed to stop undesired allocation"
                );
            }
        }

        let pending: Vec<TaskAllocation> = allocations
            .iter()
            .filter(|allocation| !self.instances.contains_key(&allocation.allocation_id))
            .filter(|allocation| !self.terminating.contains_key(&allocation.allocation_id))
            .cloned()
            .collect();

        if !pending.is_empty() && self.has_terminating() {
            debug!(
                held = pending.len(),
                terminating = self.terminating.len(),
                "Holding new allocations until in-grace allocations exit"
            );
            return Vec::new();
        }
        pending
    }

    /// Mirror the manager's current stop-notice set (`HeartbeatReply
    /// .stop_notices`) into this worker.
    ///
    /// Replaces the map wholesale rather than merging, which is what keeps it
    /// bounded without any worker-side expiry: the manager re-sends every
    /// live notice on every heartbeat and prunes its own by age, so the
    /// worker's copy is simply whatever the manager currently knows. It also
    /// avoids the obvious merge bug — a notice arriving *before* the
    /// allocation actually leaves the desired set (the read model refreshes
    /// just after the removal commits) must survive until the stop, and a
    /// notice the manager has retracted must not linger to mislabel a later,
    /// unrelated stop of the same allocation.
    pub fn set_stop_causes(&mut self, causes: HashMap<i64, StopCause>) {
        self.stop_causes = causes;
    }

    /// Mirror the manager's preemptor links, on the same replace-wholesale
    /// terms as [`Self::set_stop_causes`].
    pub fn set_stop_preemptors(
        &mut self,
        preemptors: HashMap<i64, crate::structs::StopNoticePreemptor>,
    ) {
        self.preemptors = preemptors;
    }

    /// The cause recorded for an allocation, if the manager sent one.
    #[cfg(test)]
    pub(crate) fn stop_cause_for(&self, allocation_id: i64) -> Option<StopCause> {
        self.stop_causes.get(&allocation_id).copied()
    }

    fn stop_allocation(&mut self, allocation_id: i64) -> R<()> {
        self.stop_allocation_inner(allocation_id, true)
    }

    /// Begins stopping an allocation and forgets it as *running* work.
    ///
    /// Returns without waiting. When the task's runtime accepted a stop
    /// notice, the allocation moves into [`Self::terminating`] with its
    /// deadline and [`Self::reap_terminating`] finishes it; otherwise the
    /// teardown happens inline, exactly as it did before graceful stop.
    ///
    /// `remove_dirs: false` keeps the task directory in place so an immediate
    /// relaunch of the same allocation (in-place re-dispatch) can reuse
    /// already-staged artifacts via the staging manifest.
    fn stop_allocation_inner(&mut self, allocation_id: i64, remove_dirs: bool) -> R<()> {
        if let Some(monitor) = self.health_monitors.remove(&allocation_id) {
            monitor.shutdown();
        }

        // Consume the notice: a cause describes one stop, and the allocation
        // leaves the live set by the end of this function.
        let cause = self
            .stop_causes
            .remove(&allocation_id)
            .unwrap_or(StopCause::Unknown);

        // Drop the live-set bookkeeping first, so nothing downstream (a
        // heartbeat racing the reap, the supervisor's exit poll) still counts
        // this allocation as running while it drains.
        let allocation = self.allocations.remove(&allocation_id);
        let task = self.task_configs.remove(&allocation_id);
        let instance = self.instances.remove(&allocation_id);
        self.allocation_health.remove(&allocation_id);
        self.self_fence_windows.remove(&allocation_id);
        self.terminated.remove(&allocation_id);
        self.restart_history.remove(&allocation_id);
        self.artifact_status.remove(allocation_id);

        let (Some(allocation), Some(task), Some(mut instance)) = (allocation, task, instance)
        else {
            // Nothing was ever started for this id; there is no process to
            // wait on, so complete the teardown now. No assignment to release
            // — `release` still clears any reservation booked against the id.
            self.finish_teardown(allocation_id, remove_dirs, None);
            return Ok(());
        };

        let stop = task.stop_config();
        let ctx = StopContext::with_ladder(cause, &stop.signal, stop.kill_after);
        // The notice lands BEFORE the signal, always: a task that wakes on
        // SIGTERM and immediately reads the file must find it there.
        self.write_stop_notice(&allocation, &task, &instance, &ctx);

        info!(
            allocation_id,
            task_name = %task.name,
            cause = %cause,
            signal = %ctx.signal,
            grace_secs = ctx.grace.as_secs(),
            "Stopping allocation"
        );

        let signalled = match self.registry.plugin(&task.runtime) {
            Some(plugin) => plugin
                .stop_with(&allocation, &task, &mut instance, &ctx)
                .unwrap_or_else(|err| {
                    warn!(
                        allocation_id,
                        task_name = %task.name,
                        cause = %cause,
                        error = ?err,
                        "Failed to signal allocation instance; stopping it hard"
                    );
                    false
                }),
            None => false,
        };

        if signalled {
            self.terminating.insert(
                allocation_id,
                TerminatingAllocation {
                    allocation,
                    task,
                    instance,
                    cause,
                    deadline: ctx.deadline,
                    remove_dirs,
                },
            );
            self.persist_state();
            return Ok(());
        }

        // No notice was delivered (a runtime with no graceful path, no
        // tracked process, or a failed signal): tear down immediately.
        self.destroy_and_finish(
            allocation_id,
            &allocation,
            &task,
            &mut instance,
            remove_dirs,
        );
        Ok(())
    }

    /// Write the task's stop notice into its task directory, best-effort.
    ///
    /// Failures are logged and swallowed on purpose: the notice is an
    /// advisory convenience, and a full disk (or a task dir a hardened task
    /// user made unwritable) must never turn a stop into a stuck allocation.
    fn write_stop_notice(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &TaskInstance,
        ctx: &StopContext,
    ) {
        let Some(path) = instance
            .extra_env
            .get(crate::structs::STOP_NOTICE_PATH_ENV)
            .map(PathBuf::from)
        else {
            return;
        };
        let deadline = time::OffsetDateTime::now_utc()
            + time::Duration::try_from(ctx.grace).unwrap_or(time::Duration::ZERO);
        let notice = crate::structs::TaskStopNotice {
            cause: ctx.cause,
            deadline: deadline
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            signal: ctx.signal.clone(),
            grace_seconds: ctx.grace.as_secs(),
            preemptor: self.preemptors.get(&allocation.id).cloned(),
        };
        let write = serde_json::to_vec_pretty(&notice)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(&path, bytes));
        if let Err(err) = write {
            warn!(
                allocation_id = allocation.id,
                task_name = %task.name,
                path = %path.display(),
                error = ?err,
                "Failed to write stop notice; stopping anyway"
            );
        }
    }

    /// Run the runtime's `destroy`, then release storage and directories.
    fn destroy_and_finish(
        &mut self,
        allocation_id: i64,
        allocation: &Allocation,
        task: &TaskConfig,
        instance: &mut TaskInstance,
        remove_dirs: bool,
    ) {
        if let Some(plugin) = self.registry.plugin(&task.runtime) {
            if let Err(err) = plugin.destroy(allocation, task, instance) {
                warn!(
                    allocation_id,
                    task_name = %task.name,
                    error = ?err,
                    "Failed to destroy allocation instance"
                );
            }
        }
        let assignment = instance.resource_assignment.clone();
        self.finish_teardown(allocation_id, remove_dirs, assignment.as_ref());
    }

    /// The last teardown step, shared by the immediate and the reaped paths.
    fn finish_teardown(
        &mut self,
        allocation_id: i64,
        remove_dirs: bool,
        assignment: Option<&ResourceAssignment>,
    ) {
        // Release storage after the process is gone, never before: unmounting
        // under a live task is how you corrupt a database. Grace is exactly
        // what decoupled "stop returned" from "process is gone", so this must
        // stay behind the reap rather than beside the signal. Best-effort —
        // the data survives an unmount by contract, and a stuck driver must
        // not block teardown.
        self.volumes.unmount_for_allocation(allocation_id);

        // Return this allocation's reserved cores to the share slice and drop
        // its cgroup — for exactly the same reason, and so strictly behind
        // the reap too: a cgroup still holding a live process will not be
        // removed, and cores handed back early could be reserved by an
        // incoming task while the outgoing one is still on them.
        self.resources.release(allocation_id, assignment);

        self.preemptors.remove(&allocation_id);
        if remove_dirs {
            self.volume_specs.remove(&allocation_id);
            self.cleanup_alloc_dir(allocation_id);
        }
        self.persist_state();
    }

    /// Finish every terminating allocation whose process has exited, and hard
    /// kill every one whose grace window has closed. Driven by the worker's
    /// task supervisor (once a second), never by the heartbeat path.
    ///
    /// Returns what happened to each, newest ids last, for logging and tests.
    pub fn reap_terminating(&mut self) -> Vec<(i64, ReapOutcome)> {
        if self.terminating.is_empty() {
            return Vec::new();
        }
        let now = std::time::Instant::now();
        let mut ready: Vec<(i64, ReapOutcome)> = Vec::new();
        for (allocation_id, entry) in self.terminating.iter_mut() {
            if !Self::instance_process_alive(&mut entry.instance) {
                ready.push((*allocation_id, ReapOutcome::Exited));
            } else if now >= entry.deadline {
                ready.push((*allocation_id, ReapOutcome::Killed));
            }
        }
        ready.sort_unstable_by_key(|(allocation_id, _)| *allocation_id);

        for (allocation_id, outcome) in &ready {
            let Some(mut entry) = self.terminating.remove(allocation_id) else {
                continue;
            };
            match outcome {
                ReapOutcome::Exited => info!(
                    allocation_id = *allocation_id,
                    task_name = %entry.task.name,
                    cause = %entry.cause,
                    "Allocation exited within its grace period"
                ),
                ReapOutcome::Killed => warn!(
                    allocation_id = *allocation_id,
                    task_name = %entry.task.name,
                    cause = %entry.cause,
                    "Allocation ignored its stop signal; killing at the grace deadline"
                ),
            }
            // `destroy` is the hard stop either way. After a clean exit it is
            // pure teardown (image scratch, cgroup, container row); after a
            // timeout it is also the SIGKILL.
            let allocation = entry.allocation.clone();
            let task = entry.task.clone();
            let remove_dirs = entry.remove_dirs;
            self.destroy_and_finish(
                *allocation_id,
                &allocation,
                &task,
                &mut entry.instance,
                remove_dirs,
            );
        }
        ready
    }

    /// Whether a terminating instance's process is still running. Reaps the
    /// child handle when it has exited so the pid is not left claimed.
    fn instance_process_alive(instance: &mut TaskInstance) -> bool {
        if let Some(child) = instance.child.as_mut() {
            return match child.try_wait() {
                Ok(Some(_status)) => {
                    instance.child = None;
                    instance.pid = None;
                    false
                }
                Ok(None) => true,
                // An un-waitable child is not evidence of a live process; the
                // deadline still bounds the wait either way.
                Err(_) => false,
            };
        }
        match instance.pid {
            Some(pid) => pid_is_running(pid),
            None => false,
        }
    }

    /// Whether any allocation is still inside its grace window. The worker
    /// holds new launches while this is true (see
    /// [`Self::reconcile_allocations`]).
    pub fn has_terminating(&self) -> bool {
        !self.terminating.is_empty()
    }

    /// Allocation ids currently draining, for tests and diagnostics.
    pub fn terminating_ids(&self) -> Vec<i64> {
        let mut ids = self.terminating.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Whether an allocation is still wanted (used by staging threads to
    /// detect cancellation while they were fetching).
    pub fn is_allocation_active(&self, allocation_id: i64) -> bool {
        self.instances.contains_key(&allocation_id)
    }

    /// Open a live exec session against a local allocation. Resolves the task
    /// runtime plugin, checks its declared exec support, and spawns the
    /// session. The caller (worker exec RPC) drives the returned session.
    pub fn open_exec_session(
        &mut self,
        allocation_id: i64,
        task_name: Option<&str>,
        opts: crate::task_plugins::ExecOptions,
    ) -> R<Box<dyn crate::task_plugins::ExecSession>> {
        let allocation = self
            .allocations
            .get(&allocation_id)
            .cloned()
            .ok_or_else(|| anyhow!("allocation {allocation_id} is not active on this worker"))?;
        let task = self
            .task_configs
            .get(&allocation_id)
            .cloned()
            .ok_or_else(|| anyhow!("no task config for allocation {allocation_id}"))?;
        if let Some(name) = task_name {
            if !name.is_empty() && name != task.name {
                bail!(
                    "allocation {allocation_id} runs task `{}`, not `{name}`",
                    task.name
                );
            }
        }

        let plugin = self
            .registry
            .plugin(&task.runtime)
            .ok_or_else(|| anyhow!("no task plugin loaded for runtime `{}`", task.runtime))?;
        let support = plugin.exec_support();
        if !support.exec {
            bail!("task runtime `{}` does not support exec", task.runtime);
        }
        if opts.tty && !support.pty {
            bail!(
                "task runtime `{}` does not support interactive (tty) exec",
                task.runtime
            );
        }

        let instance = self
            .instances
            .get_mut(&allocation_id)
            .ok_or_else(|| anyhow!("no instance for allocation {allocation_id}"))?;
        plugin.exec(&allocation, &task, instance, opts)
    }

    /// Synchronous run for tasks without artifacts (and the post-staging
    /// path). Creates dirs, runs `prepare` + `start`, registers the
    /// allocation.
    #[instrument(err, skip_all)]
    pub fn run_allocation(&mut self, allocation: &Allocation, task: &TaskConfig) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            task_runtime = %task.runtime,
            "Received task allocation"
        );

        self.registry
            .load_plugin(&task.runtime, true)
            .with_context(|| format!("Failed to load task plugin {}", task.runtime))?;
        let instance = self.prepare_instance(allocation, task)?;
        self.instances.insert(allocation.id, instance);
        self.start_prepared_allocation(allocation, task)
    }

    /// Runs `consume_artifact` hooks + volume mounts + `prepare` + `start`
    /// against the already-registered instance, then records the allocation as
    /// started.
    fn start_prepared_allocation(&mut self, allocation: &Allocation, task: &TaskConfig) -> R<()> {
        // Storage first: a task must never observe a missing or empty data
        // directory because its volume was still being mounted. A failure here
        // fails the allocation, which is the honest outcome — the manager
        // reschedules and the operator sees the driver's error.
        self.mount_allocation_volumes(allocation)?;

        // Resource placement next, and before `prepare()`: a task must never
        // observe its CPU/memory limits arriving after it started. Runtimes
        // that did not opt in get `None` and are untouched.
        let enforcement = self
            .registry
            .plugin(&task.runtime)
            .map(|plugin| plugin.resource_enforcement())
            .unwrap_or(ResourceEnforcement::Unsupported);
        let assignment = self
            .resources
            .assign(allocation.id, task, enforcement)
            .with_context(|| {
                format!(
                    "failed to place task {} within this node's resource limits",
                    task.name
                )
            })?;

        let plugin = self
            .registry
            .plugin(&task.runtime)
            .context("Task plugin not loaded")?;
        let log_dir = self.state_store.allocation_log_dir(allocation.id);
        let instance = self
            .instances
            .get_mut(&allocation.id)
            .context("Task instance missing")?;
        instance.log_dir = Some(log_dir);
        instance.resource_assignment = assignment;

        let staged = instance.staged_artifacts.clone();
        for artifact in &staged {
            plugin
                .consume_artifact(artifact, instance)
                .with_context(|| {
                    format!(
                        "consume artifact {} for task {}",
                        artifact.name.as_deref().unwrap_or("<unnamed>"),
                        task.name
                    )
                })?;
        }

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Preparing task allocation"
        );
        plugin.prepare(allocation, task, instance)?;

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Starting task allocation"
        );
        plugin.start(allocation, task, instance)?;

        let supervised = plugin.supervises_process();

        self.allocations.insert(allocation.id, allocation.clone());
        self.task_configs.insert(allocation.id, task.clone());
        self.set_allocation_health(allocation.id, false, "starting");
        self.notify_log_sinks_started(allocation, task);
        self.persist_state();

        // A batch task on a runtime that tracks no process can never report
        // an exit, and the manager is now waiting for one before it will call
        // the task successful. Say so explicitly at start — the same
        // optimistic answer the manager used to assume, but stated by the
        // worker that knows it cannot do better.
        if !supervised && matches!(task.lifecycle, crate::structs::TaskLifecycle::Batch) {
            self.record_unsupervised_completion(allocation.id, &task.runtime);
        }

        Ok(())
    }

    /// Entry point for a new allocation from the manager. Tasks without
    /// artifacts start synchronously; artifact-bearing tasks are registered
    /// as `staging artifacts` and return a [`StagingJob`] the caller runs on
    /// a blocking thread (never under this lock — fetches can take minutes
    /// and heartbeats must keep flowing).
    pub fn begin_task_allocation(&mut self, allocation: &TaskAllocation) -> R<BeginOutcome> {
        let (alloc, task) = task_from_allocation(allocation)?;
        // The manager resolved these against the volume table, so they carry
        // concrete ids and the driver attrs from previous mounts. Recorded
        // before any start path so both the direct and post-staging routes
        // mount the same list.
        self.volume_specs
            .insert(alloc.id, parse_volume_mounts(allocation)?);
        // A re-dispatch of an id we already run is an in-place update (for
        // example a config change reusing the allocation). The previous
        // instance is normally gone already: `reconcile_allocations` holds a
        // re-dispatch back while anything is still terminating. Reaching here
        // with the id present means that did not happen, so tear it down now
        // rather than launching over a live process. `remove_dirs: false`
        // keeps the staged artifacts for the relaunch.
        if self.instances.contains_key(&alloc.id) {
            warn!(
                allocation_id = alloc.id,
                task_name = %task.name,
                "Re-dispatched allocation was still present; forcing teardown"
            );
            if let Err(err) = self.stop_allocation_inner(alloc.id, false) {
                warn!(
                    allocation_id = alloc.id,
                    error = ?err,
                    "Failed to stop previous instance of re-dispatched allocation"
                );
            }
        }
        if task.artifacts.is_empty() {
            self.run_allocation(&alloc, &task)?;
            // Arm the self-fence watchdog for stateful allocations (0 = no
            // fencing; see the TaskAllocation proto comments). Registered
            // after a successful start and persisted so it survives worker
            // restarts.
            if allocation.self_terminate_after_seconds > 0 {
                self.set_self_fence_window(
                    alloc.id,
                    std::time::Duration::from_secs(allocation.self_terminate_after_seconds),
                );
            }
            return Ok(BeginOutcome::Started(Box::new((alloc, task))));
        }

        info!(
            allocation_id = alloc.id,
            task_name = %task.name,
            task_runtime = %task.runtime,
            artifacts = task.artifacts.len(),
            "Received task allocation with artifacts"
        );
        self.registry
            .load_plugin(&task.runtime, true)
            .with_context(|| format!("Failed to load task plugin {}", task.runtime))?;
        // Defense in depth: admission already gates on supports_artifacts.
        let plugin = self
            .registry
            .plugin(&task.runtime)
            .context("Task plugin not loaded")?;
        if !plugin.supports_artifacts() {
            bail!(
                "task runtime {} does not support artifacts on this worker",
                task.runtime
            );
        }

        // Cleanliness is the stager's job now: it reuses the task dir when a
        // valid staging manifest matches the artifact list, and clears it
        // before any fresh fetch (covering partial content left by a worker
        // that died mid-staging).
        let instance = self.prepare_instance(&alloc, &task)?;
        let task_dir = instance
            .task_dir
            .clone()
            .context("task directory missing after prepare")?;
        self.instances.insert(alloc.id, instance);
        self.allocations.insert(alloc.id, alloc.clone());
        self.task_configs.insert(alloc.id, task.clone());
        self.artifact_status.init(alloc.id, &task.artifacts);
        self.set_allocation_health(alloc.id, false, "staging artifacts");
        // Arm the self-fence watchdog as soon as the allocation is
        // registered: staging can take minutes and the at-most-one fencing
        // guarantee must hold from the moment the manager could plan a
        // replacement (0 = no fencing; see the TaskAllocation proto
        // comments).
        if allocation.self_terminate_after_seconds > 0 {
            self.set_self_fence_window(
                alloc.id,
                std::time::Duration::from_secs(allocation.self_terminate_after_seconds),
            );
        }

        Ok(BeginOutcome::Staging(Box::new(StagingJob {
            allocation: alloc,
            task,
            task_dir,
            stager: self.stager.clone(),
            board: self.artifact_status.clone(),
        })))
    }

    /// Completes an allocation whose artifacts finished staging: interpolates
    /// `${artifact.<name>}` into the runtime config, exposes
    /// `LEGION_ARTIFACT_<NAME>` env vars, and runs consume/prepare/start.
    pub fn start_staged_allocation(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        staged: Vec<StagedArtifact>,
    ) -> R<()> {
        // A runtime that mounts the task dir sees staged artifacts at the
        // in-task path, not the host one — the same rule volume env already
        // follows. `${artifact.<name>}` interpolation must agree with
        // `LEGION_ARTIFACT_<NAME>`, so both are translated from one place.
        let in_task_root = self
            .registry
            .plugin(&task.runtime)
            .and_then(|plugin| plugin.file_delivery_root());
        let mut artifact_paths: HashMap<String, String> = HashMap::new();
        let instance = self
            .instances
            .get_mut(&allocation.id)
            .context("Task instance missing for staged allocation")?;
        let task_dir = instance.task_dir.clone();
        for artifact in &staged {
            if let Some(name) = artifact.name.as_deref() {
                let host_path = artifact.reference_path();
                let env_name = format!("LEGION_ARTIFACT_{}", name.to_uppercase().replace('-', "_"));
                // The env var keeps the host path here; the runtime plugin
                // translates it at start (it is the one that knows its own
                // mount). Interpolation cannot be deferred that way — the
                // rendered config is what the plugin parses — so it is
                // translated now.
                instance
                    .extra_env
                    .insert(env_name, host_path.display().to_string());
                let in_task_path = match (in_task_root, task_dir.as_ref()) {
                    (Some(root), Some(task_dir)) => host_path
                        .strip_prefix(task_dir)
                        .ok()
                        .map(|relative| PathBuf::from(root).join(relative).display().to_string())
                        .unwrap_or_else(|| host_path.display().to_string()),
                    _ => host_path.display().to_string(),
                };
                artifact_paths.insert(name.to_string(), in_task_path);
            }
        }
        instance.staged_artifacts = staged;

        // Worker-side interpolation is dumb substitution over the declared
        // names; admission already rejected unknown references.
        let mut started_task = task.clone();
        substitute_artifact_tokens(&mut started_task.config, &artifact_paths);

        self.start_prepared_allocation(allocation, &started_task)
    }

    /// Mount the allocation's volumes and expose them to the task.
    ///
    /// Two env views, because the two task kinds see different filesystems
    /// (see docs/volumes.md, "How `path` works per task kind"):
    /// - `exec` runs in the host mount namespace, so `LEGION_VOLUME_<NAME>`
    ///   carries the **host** path and there is nothing to mount into;
    /// - `container` gets a real bind mount at the declared `path`, so its env
    ///   carries the **destination** path — the host path would be meaningless
    ///   inside the container.
    fn mount_allocation_volumes(&mut self, allocation: &Allocation) -> R<()> {
        let Some(specs) = self.volume_specs.get(&allocation.id).cloned() else {
            return Ok(());
        };
        if specs.is_empty() {
            return Ok(());
        }
        // Re-mounting an allocation we already hold mounts for (an in-place
        // re-dispatch) would leak the old ones.
        self.volumes.unmount_for_allocation(allocation.id);

        let task = self.task_configs.get(&allocation.id).cloned();
        let instance = task
            .as_ref()
            .and_then(|task| crate::worker::volumes::instance_ref_from_task(allocation, task));
        let mounted = self
            .volumes
            .mount_for_allocation(allocation, instance.as_ref(), &specs)?;

        // Runtimes with an isolated filesystem view get volume env values that
        // are in-task destinations (bind target / guest mount path), because
        // the host path is unreachable from inside. Host-process runtimes
        // (exec) get the real host path.
        let in_task_view = task
            .map(|task| matches!(task.runtime.as_str(), "container" | "sandbox"))
            .unwrap_or(false);
        if let Some(instance) = self.instances.get_mut(&allocation.id) {
            for (key, value) in volume_env(&mounted, in_task_view) {
                instance.extra_env.insert(key, value);
            }
            instance.volumes = mounted
                .iter()
                .map(|mount| crate::task_plugins::MountedVolume {
                    name: mount.name.clone(),
                    mode: mount.mode,
                    host_path: mount.host_path.clone(),
                    path: mount.path.clone(),
                    fs: mount.fs.clone(),
                    fresh: mount.fresh,
                    read_only: mount.read_only,
                })
                .collect();
        }
        Ok(())
    }

    /// Arm (or re-arm) the self-fence window for a running allocation and
    /// persist it.
    pub fn set_self_fence_window(&mut self, allocation_id: i64, window: std::time::Duration) {
        self.self_fence_windows.insert(allocation_id, window);
        self.persist_state();
    }

    pub fn restart_allocation(&mut self, allocation_id: i64) -> R<()> {
        let allocation = self
            .allocations
            .get(&allocation_id)
            .context("Allocation not found for restart")?;
        let task = self
            .task_configs
            .get(&allocation_id)
            .context("Task config missing for restart")?;
        let instance = self
            .instances
            .get_mut(&allocation_id)
            .context("Task instance missing for restart")?;
        let plugin = self
            .registry
            .plugin(&task.runtime)
            .context("Task plugin missing for restart")?;

        plugin.stop(allocation, task, instance)?;
        plugin.start(allocation, task, instance)?;
        // A relaunched process is a live process again: drop the terminal flag
        // so the supervisor resumes watching this allocation.
        self.terminated.remove(&allocation_id);
        self.set_allocation_health(allocation_id, false, "restarting");
        self.persist_state();
        Ok(())
    }

    /// Read captured stdout/stderr for an allocation hosted on this worker.
    pub fn read_allocation_logs(
        &self,
        allocation_id: i64,
        stream: &str,
        offset: i64,
        max_bytes: i64,
        tail: bool,
    ) -> R<Vec<CapturedStream>> {
        let log_dir = self.state_store.allocation_log_dir(allocation_id);
        read_allocation_logs(&log_dir, stream, offset, max_bytes, tail)
    }

    pub fn register_health_monitor(&mut self, allocation_id: i64, handle: HealthMonitorHandle) {
        self.health_monitors.insert(allocation_id, handle);
    }

    pub fn restore_allocations(&mut self) -> R<Vec<(Allocation, TaskConfig)>> {
        let persisted = self.state_store.load()?;
        let mut active = Vec::new();
        let mut restored = Vec::new();

        for record in persisted {
            let Some(pid) = record.pid else {
                warn!(
                    allocation_id = record.allocation.id,
                    task_name = %record.task.name,
                    "Skipping persisted allocation without pid"
                );
                continue;
            };
            if !pid_is_running(pid) {
                warn!(
                    allocation_id = record.allocation.id,
                    task_name = %record.task.name,
                    pid,
                    "Skipping persisted allocation with dead pid"
                );
                continue;
            }

            let alloc_dir = self.alloc_dir(record.allocation.id);
            let task_dir = alloc_dir.join(&record.task.name);
            let mut instance = TaskInstance {
                pid: Some(pid),
                log_dir: Some(self.state_store.allocation_log_dir(record.allocation.id)),
                ..TaskInstance::default()
            };
            instance
                .extra_env
                .insert("LEGION_ALLOC_DIR".into(), alloc_dir.display().to_string());
            instance
                .extra_env
                .insert("LEGION_TASK_DIR".into(), task_dir.display().to_string());
            instance.alloc_dir = Some(alloc_dir);
            instance.task_dir = Some(task_dir);
            // Re-adopt the CPU/memory assignment this worker handed out
            // before it restarted, so the allocator does not hand a running
            // task's exclusive cores to somebody else.
            if let Some(assignment) = record.resource_assignment.as_ref() {
                self.resources.restore(record.allocation.id, assignment);
                instance.resource_assignment = Some(assignment.clone());
            }
            self.instances.insert(record.allocation.id, instance);
            self.allocations
                .insert(record.allocation.id, record.allocation.clone());
            self.task_configs
                .insert(record.allocation.id, record.task.clone());
            self.volume_specs
                .insert(record.allocation.id, record.volumes.clone());
            // The task survived the worker restart and is still writing to its
            // volume, so re-establish the mount (idempotent by contract) and
            // re-record the ownership this process lost. A failure is logged,
            // not fatal: the process is already running, and the manager sees
            // the failed mount report and can act on it.
            if let Err(err) = self.mount_allocation_volumes(&record.allocation) {
                warn!(
                    allocation_id = record.allocation.id,
                    error = ?err,
                    "Failed to re-establish volume mounts for a restored allocation"
                );
            }
            if let Some(window_secs) = record.self_terminate_after_secs {
                self.self_fence_windows.insert(
                    record.allocation.id,
                    std::time::Duration::from_secs(window_secs),
                );
            }
            self.set_allocation_health(record.allocation.id, false, "restored");
            active.push(record.clone());
            restored.push((record.allocation, record.task));
        }

        // One mask write for the whole restored set, after every
        // reservation is back in the allocator's view.
        self.resources.resync_masks();
        self.persist_state_with(&active);

        Ok(restored)
    }

    fn persist_state(&self) {
        let mut records = Vec::new();
        for (allocation_id, allocation) in &self.allocations {
            let Some(task) = self.task_configs.get(allocation_id) else {
                continue;
            };
            let pid = self
                .instances
                .get(allocation_id)
                .and_then(|instance| instance.pid);
            if pid.is_none() {
                // Allocations mid-staging have no pid yet by design; anything
                // else without a pid is worth a warning.
                if self.artifact_status.snapshot(*allocation_id).is_none() {
                    warn!(
                        allocation_id = allocation_id,
                        task_name = %task.name,
                        "Skipping persistence for allocation without pid"
                    );
                }
                continue;
            }
            records.push(PersistedAllocation {
                allocation: allocation.clone(),
                task: task.clone(),
                pid,
                resource_assignment: self
                    .instances
                    .get(allocation_id)
                    .and_then(|instance| instance.resource_assignment.clone()),
                self_terminate_after_secs: self
                    .self_fence_windows
                    .get(allocation_id)
                    .map(std::time::Duration::as_secs),
                volumes: self
                    .volume_specs
                    .get(allocation_id)
                    .cloned()
                    .unwrap_or_default(),
            });
        }

        self.persist_state_with(&records);
    }

    fn persist_state_with(&self, records: &[PersistedAllocation]) {
        if let Err(err) = self.state_store.save(records) {
            warn!(error = ?err, "Failed to persist worker allocation state");
        }
    }
}

/// Split a finished child's status into `(exit_code, signal)`. Unix reports a
/// signal death as `code: None`, which is exactly the distinction the restart
/// policy and the operator both want to see.
fn exit_from_status(status: &std::process::ExitStatus) -> (Option<i32>, Option<i32>) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        (status.code(), status.signal())
    }
    #[cfg(not(unix))]
    {
        (status.code(), None)
    }
}

/// Launches one allocation end to end: synchronous start for artifact-less
/// tasks, a dedicated staging thread for artifact-bearing ones. Failures land
/// in `record_failure` (heartbeat -> manager `on_unhealthy` move logic), the
/// same path as any other allocation failure. Must be called WITHOUT holding
/// the task-manager lock.
pub fn launch_task_allocation(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    allocation: &TaskAllocation,
) -> R<()> {
    let outcome = {
        let mut guard = manager.lock().expect("task manager");
        match guard.begin_task_allocation(allocation) {
            Ok(outcome) => outcome,
            Err(err) => {
                guard.record_failure(allocation.allocation_id, err.to_string());
                return Err(err);
            }
        }
    };

    match outcome {
        BeginOutcome::Started(started) => {
            let (alloc, task) = *started;
            spawn_health_monitor(manager.clone(), alloc, task);
            Ok(())
        }
        BeginOutcome::Staging(job) => {
            spawn_staging_thread(manager.clone(), *job);
            Ok(())
        }
    }
}

fn spawn_staging_thread(manager: Arc<Mutex<WorkerTaskManager>>, job: StagingJob) {
    let allocation_id = job.allocation.id;
    let manager_for_thread = manager.clone();
    let spawned = std::thread::Builder::new()
        .name(format!("artifact-staging-{allocation_id}"))
        .spawn(move || {
            let manager = manager_for_thread;
            let result = job.stager.stage_task_artifacts(
                allocation_id,
                &job.task_dir,
                &job.task.artifacts,
                &job.board,
            );

            let started = {
                let mut guard = manager.lock().expect("task manager");
                if !guard.is_allocation_active(allocation_id) {
                    // Stopped while staging; stop_allocation already cleaned
                    // the directories and status rows.
                    debug!(allocation_id, "Allocation removed while staging artifacts");
                    return;
                }
                match result {
                    Ok(staged) => {
                        match guard.start_staged_allocation(&job.allocation, &job.task, staged) {
                            Ok(()) => true,
                            Err(err) => {
                                guard.record_failure(
                                    allocation_id,
                                    format!("start after artifact staging failed: {err:#}"),
                                );
                                false
                            }
                        }
                    }
                    Err(err) => {
                        guard.record_failure(
                            allocation_id,
                            format!("artifact staging failed: {err:#}"),
                        );
                        false
                    }
                }
            };
            if started {
                spawn_health_monitor(manager, job.allocation, job.task);
            }
        });
    if let Err(err) = spawned {
        error!(allocation_id, error = ?err, "Failed to spawn artifact staging thread");
        manager
            .lock()
            .expect("task manager")
            .record_failure(allocation_id, format!("spawn staging thread: {err}"));
    }
}

/// The resolved volume mounts carried by a dispatch. Empty string means the
/// task declares no storage, so nothing is mounted at all.
fn parse_volume_mounts(allocation: &TaskAllocation) -> R<Vec<VolumeMount>> {
    if allocation.task_volumes.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&allocation.task_volumes).context("Invalid task volumes JSON")
}

fn task_from_allocation(allocation: &TaskAllocation) -> R<(Allocation, TaskConfig)> {
    let task_config: serde_json::Value =
        serde_json::from_str(&allocation.task_config).context("Invalid task configuration JSON")?;
    let checks = if allocation.task_checks.trim().is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(&allocation.task_checks).context("Invalid task health checks JSON")?
    };
    let on_unhealthy = if allocation.task_on_unhealthy.trim().is_empty() {
        TaskOnUnhealthyConfig::default()
    } else {
        serde_json::from_str(&allocation.task_on_unhealthy)
            .context("Invalid task on_unhealthy JSON")?
    };
    // Identity specs ride along so refresh loops (and post-restart restores)
    // know each identity's delivery and change mode.
    let identities = crate::worker::identity::parse_identities(&allocation.task_identities)
        .context("Invalid task identities JSON")?;
    let artifacts = if allocation.task_artifacts.trim().is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(&allocation.task_artifacts).context("Invalid task artifacts JSON")?
    };

    let network = if allocation.network.trim().is_empty() {
        None
    } else {
        Some(serde_json::from_str(&allocation.network).context("Invalid allocation network JSON")?)
    };
    // Declared upstreams. The loopback ports they were assigned arrive on
    // `allocation.network.upstreams`; this is the declaration side (service
    // names and balancing strategies) the forwarder syncs against.
    let connect = if allocation.task_connect.trim().is_empty() {
        None
    } else {
        Some(serde_json::from_str(&allocation.task_connect).context("Invalid task connect JSON")?)
    };
    // Declared resources, used by isolating runtimes to size the workload.
    // Empty when the jobspec declared none, so runtimes keep their defaults.
    let resources = if allocation.task_resources.trim().is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str(&allocation.task_resources).context("Invalid task resources JSON")?
    };

    // The declared stop ladder. Empty means the task declared none, which is
    // not the same as declaring the default: the container runtime consults
    // the image's own `STOPSIGNAL` only when the jobspec was silent.
    let stop = if allocation.task_stop.trim().is_empty() {
        None
    } else {
        Some(serde_json::from_str(&allocation.task_stop).context("Invalid task stop JSON")?)
    };

    let task = TaskConfig {
        config: task_config,
        checks,
        on_unhealthy,
        identities,
        artifacts,
        connect,
        resources,
        stop,
        ..TaskConfig::new(&allocation.task_name, &allocation.task_runtime)
    };
    let alloc = Allocation {
        id: allocation.allocation_id,
        status: "running".into(),
        resources: serde_json::json!({}),
        node_id: allocation.node_id.clone(),
        job_config_id: allocation.job_config_id.clone(),
        task_config_id: allocation.task_config_id,
        job_id: allocation.job_id.clone(),
        network,
        // The worker never needs the instance binding: instance env vars are
        // already injected into task_config by the manager at dispatch time.
        instance_id: None,
    };

    Ok((alloc, task))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::{ArtifactSpec, TaskCheck, TaskCheckKind, TaskCheckType};
    use crate::worker::task_health::spawn_health_monitor;
    use serde_json::json;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    fn alloc() -> Allocation {
        Allocation {
            id: 5,
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

    fn manager_in(temp_dir: &TempDir) -> WorkerTaskManager {
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);
        manager.load_builtin_plugins();
        manager
    }

    /// The supervisor's eyes: a process that has exited must be seen exactly
    /// once, with its status, and reaped in the process (no zombie left).
    #[test]
    fn poll_task_exits_reports_a_finished_process_once() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({"command": ["sh", "-c", "exit 3"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;

        let mut exits = Vec::new();
        for _ in 0..100 {
            exits = manager.poll_task_exits();
            if !exits.is_empty() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        assert_eq!(exits.len(), 1, "expected exactly one exit report");
        assert_eq!(exits[0].allocation_id, alloc().id);
        assert_eq!(exits[0].exit_code, Some(3));
        assert_eq!(exits[0].describe(), "exited with status 3");
        // Terminal allocations are reported once, never re-reported.
        assert!(manager.poll_task_exits().is_empty());
        Ok(())
    }

    /// A running process is not an exit, and a runtime that tracks no process
    /// is never polled at all.
    #[test]
    fn poll_task_exits_ignores_live_and_unsupervised_allocations() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({"command": ["sleep", "30"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        assert!(manager.poll_task_exits().is_empty());

        // Same allocation, but the registry has no plugin claiming to
        // supervise a process for this runtime.
        manager.task_configs.insert(
            alloc().id,
            TaskConfig {
                ..TaskConfig::new("ghost", "not-a-real-runtime")
            },
        );
        assert!(manager.poll_task_exits().is_empty());

        manager.stop_allocation(alloc().id)?;
        Ok(())
    }

    /// One budget, both paths: the exit supervisor and the health monitor
    /// spend the same allowance.
    #[test]
    fn restart_budget_is_shared_and_time_windowed() {
        let temp_dir = TempDir::new().expect("temp dir");
        let mut manager = manager_in(&temp_dir);
        let period = Duration::from_secs(60);

        assert!(manager.can_restart_allocation(1, 2, period));
        manager.record_restart_attempt(1);
        assert!(manager.can_restart_allocation(1, 2, period));
        manager.record_restart_attempt(1);
        assert!(!manager.can_restart_allocation(1, 2, period));

        // A budget of zero never permits a restart, and attempts age out.
        assert!(!manager.can_restart_allocation(2, 0, period));
        assert!(manager.can_restart_allocation(1, 2, Duration::from_millis(0)));
    }

    /// A batch task on a runtime that cannot report exits gets its completion
    /// stated at start, so the manager never waits forever for a report that
    /// cannot come.
    #[test]
    fn unsupervised_batch_tasks_report_completion_at_start() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let mut task = TaskConfig {
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        task.lifecycle = crate::structs::TaskLifecycle::Batch;

        // exec supervises its process, so nothing is reported at start.
        manager.run_allocation(&alloc(), &task)?;
        assert!(manager.drain_terminations().is_empty());

        // The unsupervised path, exercised directly: the runtime name is in
        // the message so an operator can see why the answer is optimistic.
        manager.record_unsupervised_completion(alloc().id, "noop");
        let reports = manager.drain_terminations();
        assert_eq!(reports.len(), 1);
        assert!(reports[0].success);
        assert!(reports[0].message.contains("noop"));
        assert!(manager.drain_terminations().is_empty());
        Ok(())
    }

    #[test]
    fn worker_task_manager_runs_exec_plugin() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("exec-task", "exec")
        };

        manager.run_allocation(&alloc(), &task)?;
        Ok(())
    }

    #[test]
    fn run_allocation_creates_task_dir_and_env() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("exec-task", "exec")
        };

        manager.run_allocation(&alloc(), &task)?;

        let task_dir = temp_dir.path().join("allocs").join("5").join("exec-task");
        assert!(task_dir.is_dir(), "{} missing", task_dir.display());

        let instance = manager.instances.get(&5).expect("instance");
        assert_eq!(instance.task_dir.as_deref(), Some(task_dir.as_path()));
        assert_eq!(
            instance
                .extra_env
                .get("LEGION_TASK_DIR")
                .map(String::as_str),
            Some(task_dir.to_str().unwrap())
        );
        assert!(instance.extra_env.contains_key("LEGION_ALLOC_DIR"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn alloc_and_task_dirs_are_owner_only() -> R<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("exec-task", "exec")
        };

        manager.run_allocation(&alloc(), &task)?;

        // The alloc dir is the isolation boundary between tasks on the node:
        // anything looser than 0700 exposes artifacts/secrets/output to
        // every other local process.
        let alloc_dir = temp_dir.path().join("allocs").join("5");
        for dir in [alloc_dir.clone(), alloc_dir.join("exec-task")] {
            let mode = std::fs::metadata(&dir)?.permissions().mode() & 0o777;
            assert_eq!(
                mode,
                0o700,
                "{} must be owner-only, got {mode:o}",
                dir.display()
            );
        }
        Ok(())
    }

    #[test]
    fn task_dirs_are_absolute_even_for_relative_state_paths() {
        // Relative task dirs would break interpolated paths and env vars the
        // moment exec's cwd defaults to the task dir.
        let state_store = WorkerStateStore::new(PathBuf::from(".test-relative/worker_state.json"));
        let manager = WorkerTaskManager::with_state_store(state_store);
        assert!(manager.alloc_root.is_absolute());
    }

    #[test]
    fn stop_allocation_removes_alloc_dir() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({"command": ["true"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;

        let alloc_dir = temp_dir.path().join("allocs").join("5");
        assert!(alloc_dir.is_dir());

        // Graceful stop is two-phase. The reconcile signals and returns, so
        // the allocation is out of the running set but its directory (and,
        // crucially, its volumes) are still held.
        let _ = manager.reconcile_allocations(&[]);
        assert_eq!(
            manager.terminating_ids(),
            vec![5],
            "a signalled allocation waits in the terminating set"
        );
        assert!(
            alloc_dir.exists(),
            "teardown must wait for the process to exit — unmounting under a live task \
             is how a database gets corrupted"
        );

        // The reaper finishes it. `true` exits immediately, so this is the
        // exited path, not the deadline path.
        let reaped = loop {
            let reaped = manager.reap_terminating();
            if !reaped.is_empty() {
                break reaped;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        assert_eq!(reaped, vec![(5, ReapOutcome::Exited)]);
        assert!(!alloc_dir.exists(), "alloc dir must be cleaned once reaped");
        assert!(manager.terminating_ids().is_empty());
        Ok(())
    }

    #[test]
    fn a_task_that_ignores_its_stop_signal_is_killed_at_the_deadline() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        // Traps SIGTERM and keeps running: the case `killAfter` exists for.
        let task = TaskConfig {
            config: json!({"command": ["sh", "-c", "trap '' TERM; sleep 30"]}),
            stop: Some(crate::structs::StopConfig {
                signal: "SIGTERM".into(),
                kill_after: std::time::Duration::ZERO,
            }),
            ..TaskConfig::new("stubborn", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;

        let _ = manager.reconcile_allocations(&[]);
        assert_eq!(manager.terminating_ids(), vec![5]);

        // A zero grace window is already expired, so the very first reap
        // takes it — no sleeping, no flakiness.
        let reaped = manager.reap_terminating();
        assert_eq!(
            reaped,
            vec![(5, ReapOutcome::Killed)],
            "a task still running at the deadline is killed, not waited on forever"
        );
        assert!(manager.terminating_ids().is_empty());
        Ok(())
    }

    #[test]
    fn stops_order_before_starts_on_a_node_with_draining_work() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({"command": ["sh", "-c", "trap '' TERM; sleep 30"]}),
            // Zero grace so the reap below is deterministic; the ordering
            // rule under test is the same at any window length.
            stop: Some(crate::structs::StopConfig {
                signal: "SIGTERM".into(),
                kill_after: std::time::Duration::ZERO,
            }),
            ..TaskConfig::new("victim", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;

        // The manager's next desired set drops allocation 5 and adds 6 — the
        // shape a preemption plan produces, since victim and preemptor commit
        // together and arrive in one heartbeat diff.
        let preemptor = TaskAllocation {
            allocation_id: 6,
            ..Default::default()
        };
        let launch = manager.reconcile_allocations(&[preemptor.clone()]);
        assert!(
            launch.is_empty(),
            "the preemptor must not start while the victim is still running"
        );
        assert_eq!(manager.terminating_ids(), vec![5]);

        // Once the victim is gone the next heartbeat's reconcile releases it.
        // (The manager keeps re-offering the allocation, so nothing is lost.)
        manager.reap_terminating();
        assert!(manager.terminating_ids().is_empty());
        let launch = manager.reconcile_allocations(&[preemptor]);
        assert_eq!(
            launch.iter().map(|a| a.allocation_id).collect::<Vec<_>>(),
            vec![6],
            "the held-back allocation is offered again once the node is clear"
        );
        Ok(())
    }

    #[test]
    fn every_task_gets_a_stop_notice_path_it_can_read() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({"command": ["sh", "-c", "sleep 30"]}),
            ..TaskConfig::new("noticed", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;

        let notice_path = manager
            .instances
            .get(&5)
            .and_then(|instance| instance.extra_env.get(crate::structs::STOP_NOTICE_PATH_ENV))
            .cloned()
            .expect("every task is told where its stop notice will be");
        assert!(
            !std::path::Path::new(&notice_path).exists(),
            "the notice is written at stop, not at start"
        );

        manager.set_stop_causes(HashMap::from([(5, StopCause::Preempted)]));
        manager.set_stop_preemptors(HashMap::from([(
            5,
            crate::structs::StopNoticePreemptor {
                job: "api".into(),
                band: "high".into(),
            },
        )]));
        let _ = manager.reconcile_allocations(&[]);

        let notice: crate::structs::TaskStopNotice =
            serde_json::from_slice(&std::fs::read(&notice_path)?)?;
        assert_eq!(notice.cause, StopCause::Preempted);
        assert_eq!(notice.signal, "SIGTERM");
        assert_eq!(notice.grace_seconds, 30, "the built-in default ladder");
        assert_eq!(
            notice.preemptor.as_ref().map(|p| p.job.as_str()),
            Some("api"),
            "a preempted task learns who took its capacity"
        );
        manager.reap_terminating();
        Ok(())
    }

    #[test]
    fn captures_and_reads_exec_task_logs() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);
        manager.load_builtin_plugins();

        let task = TaskConfig {
            config: json!({"command": ["sh", "-c", "echo captured-stdout"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        let allocation = alloc();
        manager.run_allocation(&allocation, &task)?;

        // Poll for the captured output rather than sleeping a fixed duration,
        // which races on slow/loaded CI and varying filesystems.
        let mut streams = Vec::new();
        for _ in 0..100 {
            streams = manager.read_allocation_logs(allocation.id, "stdout", 0, 0, false)?;
            if streams
                .first()
                .is_some_and(|s| s.content.contains("captured-stdout"))
            {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream, "stdout");
        assert!(
            streams[0].content.contains("captured-stdout"),
            "unexpected stdout: {:?}",
            streams[0].content
        );
        Ok(())
    }

    #[test]
    fn reconcile_removes_undesired_allocations() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        let allocation = alloc();
        manager
            .instances
            .insert(allocation.id, TaskInstance::default());
        manager
            .allocations
            .insert(allocation.id, allocation.clone());
        manager
            .task_configs
            .insert(allocation.id, TaskConfig::new("noop-task", "noop"));
        manager.set_allocation_health(allocation.id, true, "healthy");

        assert!(manager.allocations.contains_key(&allocation.id));
        assert!(manager.allocation_health.contains_key(&allocation.id));
        let _ = manager.reconcile_allocations(&[]);
        assert!(!manager.allocations.contains_key(&allocation.id));
        assert!(!manager.instances.contains_key(&allocation.id));
        assert!(!manager.allocation_health.contains_key(&allocation.id));

        Ok(())
    }

    /// Put an allocation into the maps `reconcile_allocations` reads, so that
    /// reconciling to an empty desired set stops it.
    fn seed_running_allocation(manager: &mut WorkerTaskManager, allocation: &Allocation) {
        manager
            .instances
            .insert(allocation.id, TaskInstance::default());
        manager
            .allocations
            .insert(allocation.id, allocation.clone());
        manager
            .task_configs
            .insert(allocation.id, TaskConfig::new("noop-task", "noop"));
    }

    #[test]
    fn stop_cause_is_consumed_by_the_stop_it_describes() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        let allocation = alloc();
        seed_running_allocation(&mut manager, &allocation);
        manager.set_stop_causes(HashMap::from([(allocation.id, StopCause::Drain)]));
        assert_eq!(
            manager.stop_cause_for(allocation.id),
            Some(StopCause::Drain)
        );

        let _ = manager.reconcile_allocations(&[]);

        // The notice described one stop, and that stop has happened.
        assert_eq!(
            manager.stop_cause_for(allocation.id),
            None,
            "a consumed notice must not linger to label a later stop"
        );
        Ok(())
    }

    #[test]
    fn a_stop_without_a_notice_still_stops() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        let allocation = alloc();
        seed_running_allocation(&mut manager, &allocation);

        // No `set_stop_causes` call at all — an old manager, or a notice that
        // has not arrived yet. The allocation must still go away: causes are
        // advisory, and a stop is never gated on one.
        let _ = manager.reconcile_allocations(&[]);
        assert!(!manager.allocations.contains_key(&allocation.id));
        assert!(!manager.instances.contains_key(&allocation.id));
        Ok(())
    }

    /// The regression guard for the heartbeat stall: reconcile must return
    /// immediately even when the grace window is long. Every other stop test
    /// uses a zero window, so none of them would catch a blocking wait
    /// creeping back into `stop_with` — this one uses five minutes.
    #[test]
    fn reconcile_never_waits_out_a_grace_period() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            // Ignores SIGTERM, so only the deadline could ever end it.
            config: json!({
                "command": ["/bin/sh", "-c", "trap '' TERM; while :; do sleep 0.1; done"],
            }),
            stop: Some(crate::structs::StopConfig {
                signal: "SIGTERM".into(),
                kill_after: Duration::from_secs(300),
            }),
            ..TaskConfig::new("stubborn", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        thread::sleep(Duration::from_millis(300));

        let started = std::time::Instant::now();
        let _ = manager.reconcile_allocations(&[]);
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "reconcile must return immediately, took {elapsed:?}"
        );
        assert_eq!(
            manager.terminating_ids(),
            vec![alloc().id],
            "the allocation is draining, not gone"
        );

        // Not reapable yet either: the deadline is five minutes out and the
        // process is ignoring the signal.
        assert!(manager.reap_terminating().is_empty());
        assert_eq!(manager.terminating_ids(), vec![alloc().id]);
        Ok(())
    }

    /// The other half: a task that exits on its signal is reaped by the very
    /// next supervisor tick rather than waiting out its window.
    #[test]
    fn a_drained_task_is_reaped_as_soon_as_it_exits() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);
        let task = TaskConfig {
            config: json!({
                "command": ["/bin/sh", "-c", "trap 'exit 0' TERM; while :; do sleep 0.1; done"],
            }),
            stop: Some(crate::structs::StopConfig {
                signal: "SIGTERM".into(),
                kill_after: Duration::from_secs(300),
            }),
            ..TaskConfig::new("drain", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        thread::sleep(Duration::from_millis(300));

        let _ = manager.reconcile_allocations(&[]);
        // The window is five minutes, so anything reaped here was reaped for
        // exiting, not for timing out.
        let mut outcome = None;
        for _ in 0..100 {
            if let Some(reaped) = manager.reap_terminating().first().copied() {
                outcome = Some(reaped);
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            outcome,
            Some((alloc().id, ReapOutcome::Exited)),
            "a task that honored its stop signal must be reaped on exit, not at the deadline"
        );
        Ok(())
    }

    /// A runtime that cannot deliver a stop notice declines it, and the
    /// worker tears the allocation down immediately rather than holding it
    /// for a window nothing can use.
    #[test]
    fn a_runtime_that_cannot_signal_is_torn_down_without_a_drain() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);

        let allocation = alloc();
        manager
            .instances
            .insert(allocation.id, TaskInstance::default());
        manager
            .allocations
            .insert(allocation.id, allocation.clone());
        manager.task_configs.insert(
            allocation.id,
            TaskConfig {
                // `sandbox` declines the notice: reaching inside a guest
                // needs an in-VM agent it does not have.
                stop: Some(crate::structs::StopConfig {
                    signal: "SIGTERM".into(),
                    kill_after: Duration::from_secs(300),
                }),
                config: json!({"image": "alpine", "command": ["/bin/sh", "-c", "sleep 1"]}),
                ..TaskConfig::new("box", "sandbox")
            },
        );
        manager.registry.load_plugin("sandbox", false)?;

        let _ = manager.reconcile_allocations(&[]);
        assert!(
            manager.terminating_ids().is_empty(),
            "a declined notice means no drain window to wait out"
        );
        assert!(!manager.instances.contains_key(&allocation.id));
        Ok(())
    }

    #[test]
    fn stop_causes_are_replaced_wholesale_not_merged() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        manager.set_stop_causes(HashMap::from([(1, StopCause::Drain), (2, StopCause::Stop)]));
        // The manager retracted the notice for 1 and changed 2's cause. The
        // worker mirrors the manager's current set rather than accumulating,
        // which is what keeps the map bounded with no worker-side expiry.
        manager.set_stop_causes(HashMap::from([(2, StopCause::Update)]));

        assert_eq!(manager.stop_cause_for(1), None, "retracted notice is gone");
        assert_eq!(manager.stop_cause_for(2), Some(StopCause::Update));
        Ok(())
    }

    #[test]
    fn reconcile_returns_new_allocations_for_launch() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);

        let ta = TaskAllocation {
            task_volumes: String::new(),
            allocation_id: 9,
            job_id: "job".into(),
            job_config_id: "job-1".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "exec-task".into(),
            task_runtime: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: String::new(),
            self_terminate_after_seconds: 0,
            task_artifacts: String::new(),
            task_resources: String::new(),

            network: String::new(),
            task_connect: String::new(),
            ..Default::default()
        };
        let new = manager.reconcile_allocations(std::slice::from_ref(&ta));
        assert_eq!(new.len(), 1);
        assert_eq!(new[0].allocation_id, 9);

        // Already-running allocations are not returned again.
        manager.instances.insert(9, TaskInstance::default());
        let new = manager.reconcile_allocations(std::slice::from_ref(&ta));
        assert!(new.is_empty());
        Ok(())
    }

    #[test]
    fn task_connect_round_trips_through_the_wire() -> R<()> {
        // The declared `connect` block and the loopback ports it was assigned
        // travel on different fields (task_connect / network.upstreams) and
        // must both survive the manager -> worker hop: the forwarder syncs
        // listeners from the pair.
        let connect: crate::structs::ConnectConfig = serde_json::from_value(json!({
            "upstreams": [
                { "service": "db" },
                { "service": "cache", "localPort": 6379, "balance": "least_conn" },
            ]
        }))?;
        let task = TaskConfig {
            config: json!({ "command": ["true"] }),
            connect: Some(connect.clone()),
            ..TaskConfig::new("api", "exec")
        };
        let placed = Allocation {
            network: Some(crate::structs::AllocNetwork {
                address: Some("10.0.0.4".into()),
                ports: Vec::new(),
                upstreams: vec![
                    crate::structs::AllocUpstream {
                        service: "db".into(),
                        local_port: 20003,
                        balance: "round_robin".into(),
                    },
                    crate::structs::AllocUpstream {
                        service: "cache".into(),
                        local_port: 6379,
                        balance: "least_conn".into(),
                    },
                ],
            }),
            ..alloc()
        };

        let wire = crate::manager::dispatcher::build_task_allocation(&task, &placed, None, &[])?;
        let (parsed_alloc, parsed_task) = task_from_allocation(&wire)?;
        assert_eq!(parsed_task.connect, Some(connect));
        let network = parsed_alloc.network.context("network lost on the wire")?;
        assert_eq!(
            network.upstream("db").map(|upstream| upstream.local_port),
            Some(20003)
        );
        assert_eq!(
            network
                .upstream("cache")
                .map(|upstream| upstream.balance.as_str()),
            Some("least_conn")
        );

        // A task without upstreams comes back with no connect block at all.
        let plain = TaskConfig {
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("api", "exec")
        };
        let wire = crate::manager::dispatcher::build_task_allocation(&plain, &alloc(), None, &[])?;
        assert_eq!(wire.task_connect, "");
        let (_, parsed_task) = task_from_allocation(&wire)?;
        assert_eq!(parsed_task.connect, None);
        Ok(())
    }

    #[test]
    fn staging_cleans_stale_dirs_from_interrupted_staging() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);

        // Simulate a worker that died mid-staging: leftover content (and no
        // staging manifest) exists, but the allocation is not registered in
        // this (restarted) manager.
        let stale = temp_dir
            .path()
            .join("allocs")
            .join("11")
            .join("exec-task")
            .join("leftover");
        std::fs::create_dir_all(stale.parent().unwrap())?;
        std::fs::write(&stale, "partial")?;

        let source_dir = TempDir::new()?;
        let source = source_dir.path().join("tool.bin");
        std::fs::write(&source, "fresh bytes")?;
        let mut artifact = ArtifactSpec::from_source(format!("file://{}", source.display()));
        artifact.name = Some("tool".into());
        artifact.unpack = crate::structs::ArtifactUnpack::File;
        let ta = TaskAllocation {
            task_volumes: String::new(),
            allocation_id: 11,
            job_id: "job".into(),
            job_config_id: "job-1".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "exec-task".into(),
            task_runtime: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: String::new(),
            self_terminate_after_seconds: 0,
            task_artifacts: serde_json::to_string(&[artifact])?,
            task_resources: String::new(),
            network: String::new(),
            task_connect: String::new(),
            ..Default::default()
        };
        let outcome = manager.begin_task_allocation(&ta)?;
        let job = match outcome {
            BeginOutcome::Staging(job) => job,
            BeginOutcome::Started(..) => panic!("artifact task must stage"),
        };

        // Without a manifest, fresh staging clears the leftovers and fetches.
        let staged = job.stager.stage_task_artifacts(
            job.allocation.id,
            &job.task_dir,
            &job.task.artifacts,
            &job.board,
        )?;
        assert!(!stale.exists(), "stale staging leftovers must be removed");
        assert_eq!(staged.len(), 1);
        assert!(job.task_dir.join("tool.bin").is_file());
        Ok(())
    }

    #[test]
    fn begin_task_allocation_with_artifacts_registers_staging() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);

        let artifacts = vec![ArtifactSpec::from_source("https://example.com/tool.tar.gz")];
        let ta = TaskAllocation {
            task_volumes: String::new(),
            allocation_id: 11,
            job_id: "job".into(),
            job_config_id: "job-1".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "exec-task".into(),
            task_runtime: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: String::new(),
            self_terminate_after_seconds: 0,
            task_artifacts: serde_json::to_string(&artifacts)?,
            task_resources: String::new(),

            network: String::new(),
            task_connect: String::new(),
            ..Default::default()
        };

        let outcome = manager.begin_task_allocation(&ta)?;
        let job = match outcome {
            BeginOutcome::Staging(job) => job,
            BeginOutcome::Started(..) => panic!("artifact task must stage"),
        };
        assert_eq!(job.task.artifacts.len(), 1);
        assert!(manager.is_allocation_active(11));

        let health = manager.allocation_health_snapshot();
        let entry = health
            .iter()
            .find(|h| h.allocation_id == 11)
            .expect("health");
        assert_eq!(entry.status, "staging artifacts");
        let states = entry.artifact_states.as_deref().expect("artifact states");
        assert!(states.contains("\"state\":\"pending\""), "{states}");
        Ok(())
    }

    #[test]
    fn start_staged_allocation_interpolates_and_injects_env() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);

        let allocation = alloc();
        let mut spec = ArtifactSpec::from_source("https://example.com/tool.tar.gz");
        spec.name = Some("tool".into());
        let task = TaskConfig {
            config: json!({
                "command": ["true"],
                "env": {"TOOL_HOME": "${artifact.tool}"},
            }),
            artifacts: vec![spec],
            ..TaskConfig::new("exec-task", "exec")
        };

        // Register as staging first (what begin_task_allocation does).
        let instance = manager.prepare_instance(&allocation, &task)?;
        let task_dir = instance.task_dir.clone().unwrap();
        manager.instances.insert(allocation.id, instance);
        manager
            .allocations
            .insert(allocation.id, allocation.clone());
        manager.task_configs.insert(allocation.id, task.clone());

        let staged_dir = task_dir.join("tool");
        std::fs::create_dir_all(&staged_dir)?;
        let staged = vec![StagedArtifact {
            name: Some("tool".into()),
            digest: Some(format!("sha256:{}", "a".repeat(64))),
            bytes: 10,
            payload: crate::artifacts::StagedPayload::Tree {
                dir: staged_dir.clone(),
            },
        }];
        manager.start_staged_allocation(&allocation, &task, staged)?;

        let instance = manager.instances.get(&allocation.id).expect("instance");
        assert_eq!(
            instance
                .extra_env
                .get("LEGION_ARTIFACT_TOOL")
                .map(String::as_str),
            Some(staged_dir.to_str().unwrap())
        );
        // The stored (started) task config is interpolated.
        let stored = manager.task_configs.get(&allocation.id).expect("task");
        assert_eq!(
            stored.config["env"]["TOOL_HOME"],
            json!(staged_dir.display().to_string())
        );
        Ok(())
    }

    #[test]
    fn allocation_health_snapshot_sorted() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        manager.set_allocation_health(9, true, "healthy");
        manager.set_allocation_health(3, false, "unhealthy");

        let snapshot = manager.allocation_health_snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].allocation_id, 3);
        assert!(!snapshot[0].healthy);
        assert_eq!(snapshot[1].allocation_id, 9);
        assert!(snapshot[1].healthy);

        Ok(())
    }

    #[test]
    fn spawn_health_monitor_without_checks_marks_allocation_healthy() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let manager = Arc::new(Mutex::new(WorkerTaskManager::with_state_store(state_store)));
        let allocation = alloc();
        let task = TaskConfig::new("noop-task", "noop");

        spawn_health_monitor(manager.clone(), allocation.clone(), task);

        let guard = manager.lock().expect("task manager");
        let health = guard
            .allocation_health
            .get(&allocation.id)
            .context("missing allocation health")?;
        assert!(health.healthy);
        assert_eq!(health.status, "healthy (no checks configured)");
        assert!(!guard.health_monitors.contains_key(&allocation.id));

        Ok(())
    }

    fn insert_running_allocation(
        manager: &mut WorkerTaskManager,
        allocation_id: i64,
        window: Option<Duration>,
    ) {
        let allocation = Allocation {
            id: allocation_id,
            ..alloc()
        };
        manager
            .instances
            .insert(allocation_id, TaskInstance::default());
        manager.allocations.insert(allocation_id, allocation);
        manager
            .task_configs
            .insert(allocation_id, TaskConfig::new("noop-task", "noop"));
        if let Some(window) = window {
            manager.self_fence_windows.insert(allocation_id, window);
        }
    }

    #[test]
    fn fence_candidates_respects_per_allocation_windows() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        // Stateless (no window), short window, long window.
        insert_running_allocation(&mut manager, 1, None);
        insert_running_allocation(&mut manager, 2, Some(Duration::from_secs(30)));
        insert_running_allocation(&mut manager, 3, Some(Duration::from_secs(90)));

        // Under every window: nothing to fence.
        assert!(manager.fence_candidates(Duration::from_secs(10)).is_empty());
        // Past 30s but not 90s: only allocation 2.
        assert_eq!(
            manager.fence_candidates(Duration::from_secs(45)),
            vec![(2, Duration::from_secs(30))]
        );
        // Exactly at the window is NOT past it.
        assert_eq!(
            manager.fence_candidates(Duration::from_secs(30)),
            Vec::new()
        );
        // Past both windows: stateless allocation 1 is still never fenced.
        assert_eq!(
            manager.fence_candidates(Duration::from_secs(120)),
            vec![(2, Duration::from_secs(30)), (3, Duration::from_secs(90)),]
        );
        Ok(())
    }

    #[test]
    fn self_fence_kills_only_expired_stateful_allocations_and_reports_them() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);

        insert_running_allocation(&mut manager, 1, None); // stateless
        insert_running_allocation(&mut manager, 2, Some(Duration::from_secs(30)));
        insert_running_allocation(&mut manager, 3, Some(Duration::from_secs(300)));

        let fenced = manager.self_fence_expired_allocations(Duration::from_secs(60));
        assert_eq!(fenced, vec![2]);

        // The fenced allocation is gone and reported as failed; the others
        // keep running untouched.
        assert!(!manager.allocations.contains_key(&2));
        assert!(!manager.instances.contains_key(&2));
        assert!(manager.allocations.contains_key(&1));
        assert!(manager.allocations.contains_key(&3));
        let failures = manager.drain_failures();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].allocation_id, 2);
        assert!(
            failures[0].reason.contains("self-fenced")
                && failures[0].reason.contains("lost manager contact"),
            "{}",
            failures[0].reason
        );

        // Idempotent: a second sweep finds nothing left to fence.
        assert!(manager
            .self_fence_expired_allocations(Duration::from_secs(60))
            .is_empty());
        Ok(())
    }

    #[test]
    fn self_fence_window_persists_and_restores_across_restart() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_path = temp_dir.path().join("worker_state.json");

        let mut manager =
            WorkerTaskManager::with_state_store(WorkerStateStore::new(state_path.clone()));
        manager.load_builtin_plugins();
        let task = TaskConfig {
            config: json!({"command": ["sleep", "5"]}),
            ..TaskConfig::new("exec-task", "exec")
        };
        manager.run_allocation(&alloc(), &task)?;
        manager.set_self_fence_window(alloc().id, Duration::from_secs(45));

        let mut restarted = WorkerTaskManager::with_state_store(WorkerStateStore::new(state_path));
        restarted.load_builtin_plugins();
        let restored = restarted.restore_allocations()?;
        assert_eq!(restored.len(), 1);
        assert_eq!(
            restarted.fence_candidates(Duration::from_secs(60)),
            vec![(alloc().id, Duration::from_secs(45))]
        );

        // Cleanup the spawned sleep.
        if let Some(pid) = restarted.allocation_pid(alloc().id) {
            let mut system = sysinfo::System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
            if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
                process.kill();
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn worker_restores_allocations_and_monitors() -> R<()> {
        let temp_dir = TempDir::new()?;
        let state_path = temp_dir.path().join("worker_state.json");

        let state_store = WorkerStateStore::new(state_path.clone());
        let mut manager = WorkerTaskManager::with_state_store(state_store);
        manager.load_builtin_plugins();

        let allocation = alloc();
        let task = TaskConfig {
            config: json!({"command": ["sleep", "5"]}),
            checks: vec![TaskCheck {
                check_type: TaskCheckType::Health,
                name: Some("heartbeat".into()),
                kind: TaskCheckKind::Exec,
                command: Some(vec!["true".into()]),
                path: None,
                port: None,
                every: Duration::from_millis(50),
                timeout: Duration::from_millis(50),
                unhealthy_after_count: 3,
                boot_grace_time: None,
                unhealthy_after_count_timespan: None,
            }],
            ..TaskConfig::new("exec-task", "exec")
        };

        manager.run_allocation(&allocation, &task)?;

        thread::sleep(Duration::from_millis(50));

        let mut restarted_manager =
            WorkerTaskManager::with_state_store(WorkerStateStore::new(state_path));
        restarted_manager.load_builtin_plugins();

        let restored = restarted_manager.restore_allocations()?;
        assert_eq!(restored.len(), 1);

        let manager = Arc::new(Mutex::new(restarted_manager));
        let (restored_alloc, restored_task) = restored.into_iter().next().unwrap();
        spawn_health_monitor(manager.clone(), restored_alloc, restored_task);

        {
            let guard = manager.lock().expect("task manager");
            assert!(guard.health_monitors.contains_key(&allocation.id));
            let instance = guard.instances.get(&allocation.id).expect("instance");
            assert!(
                instance.task_dir.is_some(),
                "restored instance keeps task dir"
            );
        }

        let pid = {
            let guard = manager.lock().expect("task manager");
            guard
                .instances
                .get(&allocation.id)
                .and_then(|instance| instance.pid)
                .context("pid missing")?
        };

        let mut system = sysinfo::System::new();
        system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
        if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
            process.kill();
        }

        Ok(())
    }
}
