use crate::artifacts::{ArtifactStager, ArtifactStatusBoard, CredentialSource, StagedArtifact};
use crate::prelude::*;
use crate::structs::{
    substitute_artifact_tokens, Allocation, AllocationHealth, TaskAllocation, TaskConfig,
    TaskOnUnhealthyConfig, WorkerArtifactFetchConfigFull,
};
use crate::task_plugins::{TaskPluginRegistry, TaskRuntime};
use crate::worker::log_sink::{default_sinks, LogResource, LogSink};
use crate::worker::logs::{read_allocation_logs, CapturedStream};
use crate::worker::state::{pid_is_running, PersistedAllocation, WorkerStateStore};
use crate::worker::task_health::{spawn_health_monitor, HealthMonitorHandle};
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
    runtimes: HashMap<i64, TaskRuntime>,
    allocations: HashMap<i64, Allocation>,
    task_configs: HashMap<i64, TaskConfig>,
    health_monitors: HashMap<i64, HealthMonitorHandle>,
    allocation_health: HashMap<i64, AllocationHealth>,
    failed: Vec<FailedAllocationReport>,
    // Per-allocation self-fence windows (stateful tasks only; see
    // `TaskAllocation.self_terminate_after_seconds`). The watchdog kills any
    // allocation whose window is exceeded by the time since the last
    // successful manager heartbeat.
    self_fence_windows: HashMap<i64, std::time::Duration>,
    state_store: WorkerStateStore,
    /// Root of per-allocation directories: `<state dir>/allocs/<alloc_id>/<task>/`.
    alloc_root: PathBuf,
    artifact_status: ArtifactStatusBoard,
    stager: Arc<ArtifactStager>,
    log_sinks: Vec<Box<dyn LogSink>>,
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
        Self {
            registry: TaskPluginRegistry::new(),
            runtimes: HashMap::new(),
            allocations: HashMap::new(),
            task_configs: HashMap::new(),
            health_monitors: HashMap::new(),
            allocation_health: HashMap::new(),
            failed: Vec::new(),
            self_fence_windows: HashMap::new(),
            state_store,
            alloc_root,
            artifact_status: ArtifactStatusBoard::new(),
            stager: Arc::new(ArtifactStager::new(
                WorkerArtifactFetchConfigFull::default(),
                Box::new(crate::artifacts::AnonymousCredentials),
            )),
            log_sinks: default_sinks(),
        }
    }

    /// Build the [`LogResource`] describing an allocation's captured logs.
    fn log_resource(&self, allocation: &Allocation, task: &TaskConfig) -> LogResource {
        LogResource {
            allocation_id: allocation.id,
            job_id: allocation.job_id.clone(),
            node_id: allocation.node_id.clone(),
            task_name: task.name.clone(),
            task_kind: task.kind.clone(),
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
        if let Err(err) = self.registry.load_plugin("noop", true) {
            debug!(?err, "Noop task plugin not available");
        }
    }

    pub fn readiness_checks(&self) -> Map<String, Value> {
        self.registry.readiness_checks()
    }

    pub fn allocations_snapshot(&self) -> Vec<i64> {
        let mut allocations = self.runtimes.keys().copied().collect::<Vec<_>>();
        allocations.sort_unstable();
        allocations
    }

    /// Whether this worker currently runs the allocation (used by identity
    /// refresh loops to stop once the allocation is gone).
    pub fn has_allocation(&self, allocation_id: i64) -> bool {
        self.runtimes.contains_key(&allocation_id)
    }

    /// The tracked process id for an allocation, if any.
    pub fn allocation_pid(&self, allocation_id: i64) -> Option<u32> {
        self.runtimes
            .get(&allocation_id)
            .and_then(|runtime| runtime.pid)
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

    /// Deliver a Unix signal (e.g. "SIGHUP") to the allocation's process —
    /// the `changeMode: signal` pathway for refreshed identity tokens.
    pub fn signal_allocation(&self, allocation_id: i64, signal: &str) -> R<()> {
        let pid = self
            .allocation_pid(allocation_id)
            .context("Allocation has no tracked pid to signal")?;
        send_signal(pid, signal)
            .with_context(|| format!("Failed to deliver {signal} to allocation {allocation_id}"))?;
        info!(allocation_id, signal, pid, "Signaled allocation process");
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

    pub fn drain_failures(&mut self) -> Vec<FailedAllocationReport> {
        std::mem::take(&mut self.failed)
    }

    fn alloc_dir(&self, allocation_id: i64) -> PathBuf {
        self.alloc_root.join(allocation_id.to_string())
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

    /// Creates the allocation + task directories and returns a runtime
    /// pre-wired with dirs and the `LEGION_ALLOC_DIR`/`LEGION_TASK_DIR` env.
    fn prepare_runtime(&self, allocation: &Allocation, task: &TaskConfig) -> R<TaskRuntime> {
        let alloc_dir = self.alloc_dir(allocation.id);
        let task_dir = alloc_dir.join(&task.name);
        std::fs::create_dir_all(&task_dir)
            .with_context(|| format!("create task directory {}", task_dir.display()))?;
        // Owner-only: the alloc dir is the isolation boundary between
        // allocations sharing a node — other tasks must not be able to reach
        // this allocation's artifacts, injected secrets, or output.
        crate::util::restrict_dir_to_owner(&alloc_dir)?;
        crate::util::restrict_dir_to_owner(&task_dir)?;
        let mut runtime = TaskRuntime::default();
        runtime
            .extra_env
            .insert("LEGION_ALLOC_DIR".into(), alloc_dir.display().to_string());
        runtime
            .extra_env
            .insert("LEGION_TASK_DIR".into(), task_dir.display().to_string());
        runtime.alloc_dir = Some(alloc_dir);
        runtime.task_dir = Some(task_dir);
        Ok(runtime)
    }

    pub fn record_failure(&mut self, allocation_id: i64, reason: impl Into<String>) {
        if let Some(monitor) = self.health_monitors.remove(&allocation_id) {
            monitor.shutdown();
        }
        if let (Some(allocation), Some(task)) = (
            self.allocations.get(&allocation_id),
            self.task_configs.get(&allocation_id),
        ) {
            if let Some(plugin) = self.registry.plugin(&task.kind) {
                if let Some(runtime) = self.runtimes.get_mut(&allocation_id) {
                    if let Err(err) = plugin.destroy(allocation, task, runtime) {
                        warn!(
                            allocation_id,
                            task_name = %task.name,
                            error = ?err,
                            "Failed to destroy allocation runtime"
                        );
                    }
                }
            }
        }
        self.runtimes.remove(&allocation_id);
        self.allocations.remove(&allocation_id);
        self.task_configs.remove(&allocation_id);
        self.allocation_health.remove(&allocation_id);
        self.self_fence_windows.remove(&allocation_id);
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
    pub fn reconcile_allocations(&mut self, allocations: &[TaskAllocation]) -> Vec<TaskAllocation> {
        let desired_ids = allocations
            .iter()
            .map(|allocation| allocation.allocation_id)
            .collect::<std::collections::HashSet<_>>();
        let existing_ids = self.runtimes.keys().copied().collect::<Vec<_>>();
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

        allocations
            .iter()
            .filter(|allocation| !self.runtimes.contains_key(&allocation.allocation_id))
            .cloned()
            .collect()
    }

    fn stop_allocation(&mut self, allocation_id: i64) -> R<()> {
        self.stop_allocation_inner(allocation_id, true)
    }

    /// Stops an allocation's process and forgets it. `remove_dirs: false`
    /// keeps the task directory in place so an immediate relaunch of the
    /// same allocation (in-place re-dispatch) can reuse already-staged
    /// artifacts via the staging manifest.
    fn stop_allocation_inner(&mut self, allocation_id: i64, remove_dirs: bool) -> R<()> {
        if let Some(monitor) = self.health_monitors.remove(&allocation_id) {
            monitor.shutdown();
        }

        let allocation = self.allocations.get(&allocation_id).cloned();
        let task = self.task_configs.get(&allocation_id).cloned();
        if let (Some(allocation), Some(task)) = (allocation.as_ref(), task.as_ref()) {
            if let Some(plugin) = self.registry.plugin(&task.kind) {
                if let Some(runtime) = self.runtimes.get_mut(&allocation_id) {
                    if let Err(err) = plugin.stop(allocation, task, runtime) {
                        warn!(
                            allocation_id,
                            task_name = %task.name,
                            error = ?err,
                            "Failed to stop allocation runtime"
                        );
                    }
                    if let Err(err) = plugin.destroy(allocation, task, runtime) {
                        warn!(
                            allocation_id,
                            task_name = %task.name,
                            error = ?err,
                            "Failed to destroy allocation runtime"
                        );
                    }
                }
            }
        }

        self.runtimes.remove(&allocation_id);
        self.allocations.remove(&allocation_id);
        self.task_configs.remove(&allocation_id);
        self.allocation_health.remove(&allocation_id);
        self.self_fence_windows.remove(&allocation_id);
        self.artifact_status.remove(allocation_id);
        if remove_dirs {
            self.cleanup_alloc_dir(allocation_id);
        }
        self.persist_state();

        Ok(())
    }

    /// Whether an allocation is still wanted (used by staging threads to
    /// detect cancellation while they were fetching).
    pub fn is_allocation_active(&self, allocation_id: i64) -> bool {
        self.runtimes.contains_key(&allocation_id)
    }

    /// Synchronous run for tasks without artifacts (and the post-staging
    /// path). Creates dirs, runs `prepare` + `start`, registers the
    /// allocation.
    #[instrument(err, skip_all)]
    pub fn run_allocation(&mut self, allocation: &Allocation, task: &TaskConfig) -> R<()> {
        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            task_kind = %task.kind,
            "Received task allocation"
        );

        self.registry
            .load_plugin(&task.kind, true)
            .with_context(|| format!("Failed to load task plugin {}", task.kind))?;
        let runtime = self.prepare_runtime(allocation, task)?;
        self.runtimes.insert(allocation.id, runtime);
        self.start_prepared_allocation(allocation, task)
    }

    /// Runs `consume_artifact` hooks + `prepare` + `start` against the
    /// already-registered runtime, then records the allocation as started.
    fn start_prepared_allocation(&mut self, allocation: &Allocation, task: &TaskConfig) -> R<()> {
        let plugin = self
            .registry
            .plugin(&task.kind)
            .context("Task plugin not loaded")?;
        let log_dir = self.state_store.allocation_log_dir(allocation.id);
        let runtime = self
            .runtimes
            .get_mut(&allocation.id)
            .context("Task runtime missing")?;
        runtime.log_dir = Some(log_dir);

        let staged = runtime.staged_artifacts.clone();
        for artifact in &staged {
            plugin
                .consume_artifact(artifact, runtime)
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
        plugin.prepare(allocation, task, runtime)?;

        info!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Starting task allocation"
        );
        plugin.start(allocation, task, runtime)?;

        self.allocations.insert(allocation.id, allocation.clone());
        self.task_configs.insert(allocation.id, task.clone());
        self.set_allocation_health(allocation.id, false, "starting");
        self.notify_log_sinks_started(allocation, task);
        self.persist_state();

        Ok(())
    }

    /// Entry point for a new allocation from the manager. Tasks without
    /// artifacts start synchronously; artifact-bearing tasks are registered
    /// as `staging artifacts` and return a [`StagingJob`] the caller runs on
    /// a blocking thread (never under this lock — fetches can take minutes
    /// and heartbeats must keep flowing).
    pub fn begin_task_allocation(&mut self, allocation: &TaskAllocation) -> R<BeginOutcome> {
        let (alloc, task) = task_from_allocation(allocation)?;
        // A re-dispatch of an id we already run is an in-place update (for
        // example a config change reusing the allocation): stop the old
        // process but keep the task directory — when the artifact list is
        // unchanged, staging reuses the already-fetched content via the
        // staging manifest instead of refetching.
        if self.runtimes.contains_key(&alloc.id) {
            info!(
                allocation_id = alloc.id,
                task_name = %task.name,
                "Re-dispatched allocation; stopping previous instance before relaunch"
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
            task_kind = %task.kind,
            artifacts = task.artifacts.len(),
            "Received task allocation with artifacts"
        );
        self.registry
            .load_plugin(&task.kind, true)
            .with_context(|| format!("Failed to load task plugin {}", task.kind))?;
        // Defense in depth: admission already gates on supports_artifacts.
        let plugin = self
            .registry
            .plugin(&task.kind)
            .context("Task plugin not loaded")?;
        if !plugin.supports_artifacts() {
            bail!(
                "task kind {} does not support artifacts on this worker",
                task.kind
            );
        }

        // Cleanliness is the stager's job now: it reuses the task dir when a
        // valid staging manifest matches the artifact list, and clears it
        // before any fresh fetch (covering partial content left by a worker
        // that died mid-staging).
        let runtime = self.prepare_runtime(&alloc, &task)?;
        let task_dir = runtime
            .task_dir
            .clone()
            .context("task directory missing after prepare")?;
        self.runtimes.insert(alloc.id, runtime);
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
    /// `${artifact.<name>}` into the kind config, exposes
    /// `LEGION_ARTIFACT_<NAME>` env vars, and runs consume/prepare/start.
    pub fn start_staged_allocation(
        &mut self,
        allocation: &Allocation,
        task: &TaskConfig,
        staged: Vec<StagedArtifact>,
    ) -> R<()> {
        let mut artifact_paths: HashMap<String, String> = HashMap::new();
        let runtime = self
            .runtimes
            .get_mut(&allocation.id)
            .context("Task runtime missing for staged allocation")?;
        for artifact in &staged {
            if let Some(name) = artifact.name.as_deref() {
                let path = artifact.reference_path().display().to_string();
                let env_name = format!("LEGION_ARTIFACT_{}", name.to_uppercase().replace('-', "_"));
                runtime.extra_env.insert(env_name, path.clone());
                artifact_paths.insert(name.to_string(), path);
            }
        }
        runtime.staged_artifacts = staged;

        // Worker-side interpolation is dumb substitution over the declared
        // names; admission already rejected unknown references.
        let mut started_task = task.clone();
        substitute_artifact_tokens(&mut started_task.config, &artifact_paths);

        self.start_prepared_allocation(allocation, &started_task)
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
        let runtime = self
            .runtimes
            .get_mut(&allocation_id)
            .context("Task runtime missing for restart")?;
        let plugin = self
            .registry
            .plugin(&task.kind)
            .context("Task plugin missing for restart")?;

        plugin.stop(allocation, task, runtime)?;
        plugin.start(allocation, task, runtime)?;
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
            let mut runtime = TaskRuntime {
                pid: Some(pid),
                log_dir: Some(self.state_store.allocation_log_dir(record.allocation.id)),
                ..TaskRuntime::default()
            };
            runtime
                .extra_env
                .insert("LEGION_ALLOC_DIR".into(), alloc_dir.display().to_string());
            runtime
                .extra_env
                .insert("LEGION_TASK_DIR".into(), task_dir.display().to_string());
            runtime.alloc_dir = Some(alloc_dir);
            runtime.task_dir = Some(task_dir);
            self.runtimes.insert(record.allocation.id, runtime);
            self.allocations
                .insert(record.allocation.id, record.allocation.clone());
            self.task_configs
                .insert(record.allocation.id, record.task.clone());
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
                .runtimes
                .get(allocation_id)
                .and_then(|runtime| runtime.pid);
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
                self_terminate_after_secs: self
                    .self_fence_windows
                    .get(allocation_id)
                    .map(std::time::Duration::as_secs),
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

/// Resolve a signal name like "SIGHUP"/"HUP" and deliver it to `pid`.
#[cfg(unix)]
fn send_signal(pid: u32, signal: &str) -> R<()> {
    let signum = parse_signal(signal)?;
    // SAFETY: plain kill(2) with a validated signal number.
    let rc = unsafe { libc::kill(pid as libc::pid_t, signum) };
    if rc != 0 {
        bail!(
            "kill({pid}, {signal}) failed: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn send_signal(_pid: u32, signal: &str) -> R<()> {
    bail!("signal delivery ({signal}) is not supported on this platform");
}

#[cfg(unix)]
fn parse_signal(name: &str) -> R<libc::c_int> {
    let normalized = name.trim().to_ascii_uppercase();
    let stripped = normalized.strip_prefix("SIG").unwrap_or(&normalized);
    Ok(match stripped {
        "HUP" => libc::SIGHUP,
        "INT" => libc::SIGINT,
        "QUIT" => libc::SIGQUIT,
        "TERM" => libc::SIGTERM,
        "USR1" => libc::SIGUSR1,
        "USR2" => libc::SIGUSR2,
        "CONT" => libc::SIGCONT,
        "WINCH" => libc::SIGWINCH,
        other => bail!("unsupported signal {other:?}"),
    })
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

    let task = TaskConfig {
        config: task_config,
        checks,
        on_unhealthy,
        identities,
        artifacts,
        ..TaskConfig::new(&allocation.task_name, &allocation.task_kind)
    };
    let alloc = Allocation {
        id: allocation.allocation_id,
        status: "running".into(),
        resources: serde_json::json!({}),
        node_id: allocation.node_id.clone(),
        job_config_id: allocation.job_config_id.clone(),
        task_config_id: allocation.task_config_id,
        job_id: allocation.job_id.clone(),
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
        }
    }

    fn manager_in(temp_dir: &TempDir) -> WorkerTaskManager {
        let state_store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let mut manager = WorkerTaskManager::with_state_store(state_store);
        manager.load_builtin_plugins();
        manager
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

        let runtime = manager.runtimes.get(&5).expect("runtime");
        assert_eq!(runtime.task_dir.as_deref(), Some(task_dir.as_path()));
        assert_eq!(
            runtime.extra_env.get("LEGION_TASK_DIR").map(String::as_str),
            Some(task_dir.to_str().unwrap())
        );
        assert!(runtime.extra_env.contains_key("LEGION_ALLOC_DIR"));
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
        let _ = manager.reconcile_allocations(&[]);
        assert!(!alloc_dir.exists(), "alloc dir must be cleaned on stop");
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
            .runtimes
            .insert(allocation.id, TaskRuntime::default());
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
        assert!(!manager.runtimes.contains_key(&allocation.id));
        assert!(!manager.allocation_health.contains_key(&allocation.id));

        Ok(())
    }

    #[test]
    fn reconcile_returns_new_allocations_for_launch() -> R<()> {
        let temp_dir = TempDir::new()?;
        let mut manager = manager_in(&temp_dir);

        let ta = TaskAllocation {
            allocation_id: 9,
            job_id: "job".into(),
            job_config_id: "job-1".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "exec-task".into(),
            task_kind: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: String::new(),
            self_terminate_after_seconds: 0,
            task_artifacts: String::new(),
        };
        let new = manager.reconcile_allocations(std::slice::from_ref(&ta));
        assert_eq!(new.len(), 1);
        assert_eq!(new[0].allocation_id, 9);

        // Already-running allocations are not returned again.
        manager.runtimes.insert(9, TaskRuntime::default());
        let new = manager.reconcile_allocations(std::slice::from_ref(&ta));
        assert!(new.is_empty());
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
            allocation_id: 11,
            job_id: "job".into(),
            job_config_id: "job-1".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "exec-task".into(),
            task_kind: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: String::new(),
            self_terminate_after_seconds: 0,
            task_artifacts: serde_json::to_string(&[artifact])?,
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
            allocation_id: 11,
            job_id: "job".into(),
            job_config_id: "job-1".into(),
            node_id: "node-1".into(),
            task_config_id: 1,
            task_name: "exec-task".into(),
            task_kind: "exec".into(),
            task_config: json!({"command": ["true"]}).to_string(),
            task_checks: String::new(),
            task_on_unhealthy: String::new(),
            task_identities: String::new(),
            self_terminate_after_seconds: 0,
            task_artifacts: serde_json::to_string(&artifacts)?,
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
        let runtime = manager.prepare_runtime(&allocation, &task)?;
        let task_dir = runtime.task_dir.clone().unwrap();
        manager.runtimes.insert(allocation.id, runtime);
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

        let runtime = manager.runtimes.get(&allocation.id).expect("runtime");
        assert_eq!(
            runtime
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
        assert_eq!(snapshot[0].healthy, false);
        assert_eq!(snapshot[1].allocation_id, 9);
        assert_eq!(snapshot[1].healthy, true);

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
            .runtimes
            .insert(allocation_id, TaskRuntime::default());
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
        assert!(!manager.runtimes.contains_key(&2));
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
            let runtime = guard.runtimes.get(&allocation.id).expect("runtime");
            assert!(
                runtime.task_dir.is_some(),
                "restored runtime keeps task dir"
            );
        }

        let pid = {
            let guard = manager.lock().expect("task manager");
            guard
                .runtimes
                .get(&allocation.id)
                .and_then(|runtime| runtime.pid)
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
